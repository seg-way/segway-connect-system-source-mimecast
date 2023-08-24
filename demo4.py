import os
from dotenv import load_dotenv

load_dotenv()
import shutil
import pprint
from json import loads
import gzip
import tempfile
import asyncio
import backoff
import aiofiles
import httpx
import time
from datetime import timedelta, date, datetime
import furl

MIMECAST_HOST = os.environ.get("MIMECAST_HOST", "api.services.mimecast.com")
MIMECAST_CLIENT_ID = os.environ["MIMECAST_CLIENT_ID"]
MIMECAST_CLIENT_SECRET = os.environ["MIMECAST_CLIENT_SECRET"]


def build_mimecast_auth_url():
    return (
        furl(MIMECAST_HOST)
        .set(
            protocol="https",
            path="/oauth/token",
            args={
                "client_id": MIMECAST_CLIENT_ID,
                "client_secret": MIMECAST_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        )
        .url
    )


def backoff_hdlr(details):
    print(
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
        requestUrl = (
            f"https://api.services.mimecast.com/v1/siem/batch/events/cg?type={type}&pageSize=100"
        )

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


async def main():
    async with httpx.AsyncClient(http2=True) as mimecast:
        token = None
        nextPage = None
        thisPage = None
        expires = datetime.now() - timedelta(seconds=30)
        while True:
            print(f"expires in {expires-datetime.now()} so {datetime.now() >= expires}")
            if datetime.now() >= expires:
                # lastAuth = date.today()
                token = await get_access_token(mimecast)
                print(token)
                expires = (
                    datetime.now()
                    + timedelta(seconds=int(token["expires_in"]))
                    + timedelta(seconds=-30)
                )

            thisPage = nextPage
            batch = await get_next_batch(mimecast, token, "process", nextPage)
            print("gotbatch")
            if "@nextPage" in batch:
                nextPage = batch["@nextPage"]

            if (
                "value" in batch
                and len(batch["value"]) > 0
                and (nextPage != thisPage or not thisPage)
            ):
                async with httpx.AsyncClient(http2=True) as aws:
                    await bulk(aws, batch)
            elif nextPage == thisPage and thisPage:
                print("sleeping page did not move")
                time.sleep(60)
            else:
                print(f"sleeping no data on page {batch}")
                time.sleep(60)


asyncio.run(main())
