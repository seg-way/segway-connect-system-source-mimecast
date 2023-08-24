from syslogng import LogSource
from syslogng import LogMessage

import os
from dotenv import load_dotenv

load_dotenv()

from json import loads
import gzip
import tempfile
import asyncio
import backoff
import aiofiles
import httpx
from datetime import timedelta, date, datetime
import furl


class LogSource(LogSource):
    """Provides a syslog-ng async source for Mimecast"""

    cancelled: bool = False

    def init(self, options):
        self._CLIENT_ID = os.environ["MIMECAST_CLIENT_ID"]
        self._CLIENT_SECRET = os.environ["MIMECAST_CLIENT_SECRET"]
        self._HOST = os.environ.get("MIMECAST_HOST", "api.services.mimecast.com")

        return True

    def run(self):
        """Simple Run method to create the loop"""
        asyncio.run(self.receive_batch())

    def backoff_hdlr(details):
        logger.warning(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "calling function {target} with args {args} and kwargs "
            "{kwargs}".format(**details)
        )

    @backoff.on_exception(backoff.expo, httpx.ReadTimeout)
    async def get_access_token(session):
        requestUrl = "https://api.services.mimecast.com/oauth/token"
        payload = f"client_id={MIMECAST_CLIENT_ID}&client_secret={MIMECAST_CLIENT_SECRET}&grant_type=client_credentials"
        requestHeaders = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            # "Accept-Encoding": "gzip",
        }

        response = await session.post(requestUrl, headers=requestHeaders, data=payload)
        response.raise_for_status()
        print(response.json())
        return response.json()

    @backoff.on_exception(backoff.expo, httpx.ReadTimeout, max_time=60, on_backoff=backoff_hdlr)
    async def get_next_batch(session, access_token, type, nextPage):
        if nextPage:
            requestUrl = f"https://api.services.mimecast.com/v1/siem/batch/events/cg?type={type}&pageSize=100&nextLink={nextPage}"
        else:
            requestUrl = f"https://api.services.mimecast.com/v1/siem/batch/events/cg?type={type}&pageSize=100"

        requestHeaders = {
            "Authorization": f"Bearer {access_token['access_token']}",
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        }
        response = await session.get(requestUrl, headers=requestHeaders)
        response.raise_for_status()

        return response.json()

    async def batch_to_events(session, link):
        chunk_size: int = 8192
        url = link["url"]

        # response = await session.get(url)
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmpfile = os.path.join(tmpdirname, "data.json.gz")
            async with aiofiles.open(tmpfile, mode="wb") as f:
                async with session.stream("GET", url) as response:
                    async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                        await f.write(chunk)
                    response.raise_for_status()

                # await f.flush()
            with gzip.GzipFile(mode="rb", filename=tmpfile) as j:
                count = 0
                for line in j:
                    count += 1
                    data = loads(line)
                    # print(data) #TODO: SEND DATA
                # print(f"found {count}")

    async def bulk(session, batch):
        for link in batch["value"]:
            tasks = []
            tasks.append(batch_to_events(session, link))
            await asyncio.gather(*tasks)

    async def receive_batch(self):
        params = {
            "limit": MAX_CHUNK_SIZE,
            "stream_type": EnterpriseEventsStreamType.ADMIN_LOGS,
            "stream_position": self.persist["stream_position"],
        }
        timeout = 5
        while not self.cancelled:
            # self.cancelled = True
            # try:
            box_response = self._get_events(params)
            entries = box_response["entries"]
            for entry in entries:
                event = EventStream.clean_event(entry)
                record_lmsg = LogMessage(orjson.dumps(event))
                self.post_message(record_lmsg)

            self.persist["stream_position"] = box_response["next_stream_position"]
            params["stream_position"] = box_response["next_stream_position"]
            logger.info(
                f"Posted count={len(entries)} next_stream_position={params['stream_position']}"
            )

    def backoff_hdlr_exp(details):
        logger.info(
            "Backing off {wait:0.1f} seconds after {tries} tries with args {args}".format(**details)
        )

    def backoff_hdlr_pred(details):
        logger.info(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "with args {args} result {value}".format(**details)
        )

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_time=300,
        on_backoff=backoff_hdlr_exp,
    )
    @backoff.on_predicate(
        backoff.expo,
        lambda x: x["entries"] == [],
        max_time=300,
        on_backoff=backoff_hdlr_pred,
    )
    def _get_events(self, params):
        box_response = self._client.make_request(
            "GET", self._client.get_url("events"), params=params, timeout=30
        )
        result = box_response.json()
        return result

    @staticmethod
    def clean_event(source_dict: dict):
        """
        Delete keys with the value ``None``  or ```` (empty) string in a dictionary, recursively.
        Remove empty list and dict objects

        This alters the input so you may wish to ``copy`` the dict first.
        """
        # For Python 3, write `list(d.items())`; `d.items()` won’t work
        # For Python 2, write `d.items()`; `d.iteritems()` won’t work
        for key, value in list(source_dict.items()):
            if value is None:
                del source_dict[key]
            elif isinstance(value, str) and value in ("", "None", "none"):
                del source_dict[key]
            elif isinstance(value, str):
                if value.endswith("\n"):
                    value = value.strip("\n")

                if value.startswith('{"'):
                    try:
                        value = orjson.loads(value)
                        EventStream.clean_event(value)
                        source_dict[key] = value
                    except orjson.JSONDecodeError:
                        pass
            elif isinstance(value, dict) and not value:
                del source_dict[key]
            elif isinstance(value, dict):
                EventStream.clean_event(value)
            elif isinstance(value, list) and not value:
                del source_dict[key]
        return source_dict  # For convenience
