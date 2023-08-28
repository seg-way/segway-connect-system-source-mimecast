import asyncio
import gzip
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta
from json import loads

import aiofiles
import backoff
import httpx
import orjson
import pytz
from dotenv import load_dotenv
from flatdict import FlatDict
from furl import furl
from pythonjsonlogger import jsonlogger

from segateway_source_mimecast._CleanEvent import CleanEvent


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now
        if log_record.get("level"):
            log_record["level"] = log_record["level"].upper()
        else:
            log_record["level"] = record.levelname

        log_record["threadName"] = record.threadName
        log_record["lineno"] = record.lineno
        log_record["module"] = record.module
        log_record["exc_info"] = record.exc_info
        log_record["exc_text"] = record.exc_text


# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logHandler = logging.StreamHandler()
formatter = CustomJsonFormatter()
logHandler.setFormatter(formatter)

logger.addHandler(logHandler)

load_dotenv()

try:
    from syslogng import LogMessage, LogSource, Persist

    syslogng = True
except ImportError:
    syslogng = False

    class LogSource:
        pass

    class LogMessage:
        pass


def _backoff_handler(details):
    logger.warning(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


class LogSourcePlugin(LogSource):
    """Provides a syslog-ng async source for Mimecast"""

    _cancelled: bool = False

    def init(self, options):
        """Syslog NG doesn't use the python init so any one time setup is done here"""

        self._CLIENT_ID = os.environ["MIMECAST_CLIENT_ID"]
        self._CLIENT_SECRET = os.environ["MIMECAST_CLIENT_SECRET"]
        self._HOST = os.environ.get("MIMECAST_HOST", "api.services.mimecast.com")
        self._TYPE = os.environ["MIMECAST_TYPE"]
        logger.info(f"Setup Collection for type {self._TYPE} from {self._HOST}")

        if syslogng:
            self.persist = Persist(f"mimecast_{self._TYPE}", defaults={"nextPage": ""})

        return True

    def run(self):
        """Called by Syslog-ng to start the function"""

        logger.info("Run called by syslog-ng")
        asyncio.run(self.run_async())

    def request_exit(self):
        """Called by syslog NG on exit"""

        logger.info("Exit called by syslog-ng")
        self.cancelled = True

    async def run_async(self):
        """Actual start of process"""

        async with httpx.AsyncClient(http2=True) as session:
            token = None
            nextPage = None
            if syslogng and self.persist["nextPage"] != "":
                nextPage = self.persist["nextPage"]

            thisPage = None

            # Tokens have an expiration point in seconds so we start
            # The loop with an expired token so we can auth
            expires = datetime.now() - timedelta(seconds=30)
            while not self._cancelled:
                logger.info(f"expires in {expires-datetime.now()}")
                if datetime.now() >= expires:
                    token = await self.get_access_token(session)
                    logger.debug(f"Auth Token {token}")
                    expires = (
                        datetime.now()
                        + timedelta(seconds=int(token["expires_in"]))
                        + timedelta(seconds=-30)
                    )

                # Keep up with current page if the nextPage does not change
                # We don't want to re-ingest the same data
                thisPage = nextPage

                # mimecast returns a list of batches which are signed urls
                # pointing to amazon s3 object in form of json.gz files
                batch_list = await self.get_next_batch(session, token, self._TYPE, nextPage)

                if "@nextPage" in batch_list:
                    nextPage = batch_list["@nextPage"]

                    if (
                        "value" in batch_list
                        and len(batch_list["value"]) > 0
                        and (nextPage != thisPage or not thisPage)
                    ):
                        tasks = []
                        for link in batch_list["value"]:
                            tasks.append(self.batch_to_events(session, link))
                        await asyncio.gather(*tasks)
                    elif nextPage == thisPage and thisPage:
                        logger.info("sleeping page did not move")
                        time.sleep(60)

                    if syslogng:
                        self.persist["nextPage"] = nextPage
                else:
                    logger.warning(f"sleeping no data on page {batch_list}")
                    time.sleep(60)

    def _get_url(self, path=None, args=None):
        return furl().set(
            scheme="https",
            host=self._HOST,
            path=path,
            args=args,
        )

    @backoff.on_exception(backoff.expo, httpx.ReadTimeout)
    async def get_access_token(self, session):
        """Get an auth token"""
        requestUrl = self._get_url(path="oauth/token").url
        # This payload looks like query string args but this is the format the api requires

        payload = "&".join(
            [
                "=".join(["client_id", self._CLIENT_ID]),
                "=".join(["client_secret", self._CLIENT_SECRET]),
                "=".join(["grant_type", "client_credentials"]),
            ]
        )
        # Note the accept encoding
        requestHeaders = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Accept-Encoding": "br;q=1.0, gzip;q=0.8, deflate;q=0.1",
        }
        logger.debug(f"Auth Attempt {requestUrl} {requestHeaders}")
        response = await session.post(requestUrl, headers=requestHeaders, data=payload)
        response.raise_for_status()
        logger.debug(f"Auth Response {response.json()}")
        return response.json()

    @backoff.on_exception(backoff.expo, httpx.ReadTimeout, max_time=60, on_backoff=_backoff_handler)
    @backoff.on_exception(
        backoff.expo, httpx.HTTPStatusError, max_time=60, on_backoff=_backoff_handler
    )
    async def get_next_batch(self, session, access_token, feed_type, nextPage):
        """Use the auth token to get a batch of urls to process"""
        request_furl = self._get_url(
            path="v1/siem/batch/events/cg",
            args={
                "pageSize": "100",
                "type": feed_type,
            },
        )

        if nextPage:
            request_furl.args["nextLink"] = nextPage

        requestHeaders = {
            "Authorization": f"Bearer {access_token['access_token']}",
            "Accept": "application/json",
            "Accept-Encoding": "br;q=1.0, gzip;q=0.8, deflate;q=0.1",
        }
        response = await session.get(request_furl.url, headers=requestHeaders)
        response.raise_for_status()

        # logger.debug(response.json())
        return response.json()

    @backoff.on_exception(backoff.expo, httpx.ReadTimeout, max_time=60, on_backoff=_backoff_handler)
    async def batch_to_events(self, session, link):
        chunk_size: int = 8192
        url = link["url"]
        f_url = furl(url).set(args=[])
        head, filename = os.path.split(str(f_url.path))
        logger.debug(f"collecting {filename} from {f_url.url}")

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
                    CleanEvent(data)
                    event_time = datetime.utcfromtimestamp(int(data["timestamp"]) / 1000).replace(
                        tzinfo=pytz.utc
                    )
                    logger.debug(
                        f"source={filename} line={count} parsedtime_stamp={event_time} data={data}"
                    )

                    if syslogng:
                        message = orjson.dumps(data)
                        single_event = LogMessage(message)
                        single_event.set_timestamp(event_time)
                        for field_key, field_value in FlatDict(data, delimiter=".").items():
                            if not field_key.startswith("_") and field_key not in ("timestamp"):
                                single_event[f".Vendor.{field_key}"] = field_value
                        # record_lmsg.update(FlatDict(data, delimiter='.'))
                        self.post_message(single_event)
            logger.info(f"Completed {count} from {filename}")


def main():
    source = LogSourcePlugin()
    source.init({})
    source.run()


if __name__ == "__main__":
    main()
