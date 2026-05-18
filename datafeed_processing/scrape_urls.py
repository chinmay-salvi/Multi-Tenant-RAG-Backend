import asyncio
import json
import logging
import random
import re
import sys
from collections import deque, defaultdict
from typing import Dict
from urllib.parse import urlparse
from uuid import uuid4

import aiohttp
import tiktoken
from sqlalchemy import (
    update,
    select,
    Sequence,
    UUID,
    func,
    asc,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession

from config import DATABASE_URL, LOG_LEVEL, load_auth_cred_dict
from tables import (
    SysAuthCred,
    URLScrapingMessageQueue,
    MessageQueueStatusEnum,
    TempUrlDataFeed,
)

MIN_SCRAPER_TOKEN_BALANCE = 100000
SCRAPER_MAX_DEPTH = 2
MAX_CONCURRENT_TASKS = 25

logger = logging.getLogger(__name__)


def __setup_logging(log_level: str):
    log_level = getattr(logging, log_level.upper())
    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    root_logger.addHandler(stream_handler)
    logger.info("Set up logging with log level %s", log_level)


# List of common pagination patterns
pagination_patterns = (
    re.compile(r"page=\d+"),  # Matches 'page=1', 'page=34', etc.
    re.compile(r"p=\d+"),  # Matches 'p=1', 'p=34', etc.
    re.compile(r"pg=\d+"),  # Matches 'pg=1', 'pg=34', etc.
    re.compile(r"pageNum=\d+"),  # Matches 'pageNum=1', 'pageNum=34', etc.
    re.compile(r"offset=\d+"),  # Matches 'offset=10', 'offset=100', etc.
    re.compile(r"start=\d+"),  # Matches 'start=0', 'start=10', etc.
    re.compile(r"currentPage=\d+"),  # Matches 'currentPage=1', 'currentPage=34', etc.
)

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=25,  # Number of connections to keep open in the pool
    max_overflow=25,  # Number of connections that can be opened beyond the pool_size
    pool_recycle=600,  # Recycle connections after 1 hour
    pool_timeout=30,  # Raise an exception after 2 minutes if no connection is available from the pool
)

SessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=engine)


async def fetch_random_auth_cred(db: AsyncSession) -> Sequence[str] | None | str:
    result = await db.execute(
        select(SysAuthCred.authCred)
        .filter(
            SysAuthCred.type == "URL_SCRAPER",
            SysAuthCred.balance >= MIN_SCRAPER_TOKEN_BALANCE,
        )
        .order_by(func.random())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def update_api_key_balance(db: AsyncSession, auth_cred_str: str, balance: int):
    await db.execute(
        update(SysAuthCred)
        .where(SysAuthCred.authCred == auth_cred_str)
        .values(balance=balance)
    )


def num_tokens_from_string(string: str, model_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.encoding_for_model(model_name=model_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


def clean_url(url):
    parsed_url = urlparse(url)
    modified_url = parsed_url._replace(
        netloc=parsed_url.netloc.replace("www.", "", 1), scheme="", fragment=""
    )
    return modified_url.geturl()[2:]


def check_pagination(url):
    # Check each pattern in the list
    for pattern in pagination_patterns:
        if pattern.search(url):
            return True

    return False


async def get_url_data(user_id, group_id, main_url, page_url):
    headers = {
        "X-With-Links-Summary": "true",
        # "X-With-Images-Summary": "true",
        "Accept": "application/json",
        "X-With-Generated-Alt": "true",
    }

    async with aiohttp.ClientSession() as session:
        try:
            print("Fetching URL data with API KEY")

            async with SessionLocal() as db:
                auth_cred_str = await fetch_random_auth_cred(db)

                if not auth_cred_str:
                    raise Exception(
                        f"No api key found for scraping with balance above minimum."
                    )

                try:
                    creds = load_auth_cred_dict(auth_cred_str)
                    api_key = creds["API_KEY"]
                except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
                    raise Exception(
                        "Scraper credential must be JSON with an API_KEY field"
                    ) from e

                async with session.get(
                    f"https://r.jina.ai/{page_url}",
                    headers={**headers, "Authorization": f"Bearer {api_key}"},
                ) as response:
                    data = await response.json()

                    if data["code"] != 200 and data["status"] != 20000:
                        if data["name"] == "InsufficientBalanceError":
                            await update_api_key_balance(db, auth_cred_str, 0)
                            await db.commit()
                        raise Exception(data)

                try:
                    async with session.get(
                        f"https://embeddings-dashboard-api.jina.ai/api/v1/api_key/user?api_key={api_key}",
                        headers={**headers, "Authorization": f"Bearer {api_key}"},
                    ) as token_balance_response:
                        token_balance_data = await token_balance_response.json()
                        await update_api_key_balance(
                            db,
                            auth_cred_str,
                            token_balance_data["wallet"]["total_balance"],
                        )
                        await db.commit()

                except Exception as e:
                    print("Scraper API token balance update failed due to error:", e)
        except Exception as e:
            print(
                f"Fetching url {page_url} without API KEY as earlier fetch failed due to error: {e}"
            )
            try:
                async with session.get(
                    f"https://r.jina.ai/{page_url}", headers=headers
                ) as response:
                    data = await response.json()
                    if data["code"] != 200 and data["status"] != 20000:
                        raise Exception(data, page_url)
            except Exception as e2:
                print(
                    f"Unable to fetch URL again without API KEY: {page_url} -- due to error: {e2}"
                )
                raise

    url_data = {
        "urlId": uuid4(),
        "groupId": group_id,
        "pageTitle": data["data"]["title"],
        "url": data["data"]["url"],
        "mainURL": main_url,
        "content": data["data"]["content"],
        "tokenCount": num_tokens_from_string(
            data["data"]["content"], model_name="gpt-3.5-turbo"
        ),
        "userId": user_id,
    }

    return url_data, data["data"]["links"].values()


async def fetch_urls_recursive(main_url: str, user_id: UUID, group_id: UUID) -> Dict:
    urls_data = dict()
    urls_q = deque([clean_url(main_url)])
    depth = 0
    pagination_found = retries = False
    retried_urls = defaultdict(int)
    limit_errors = 10**9

    while depth < SCRAPER_MAX_DEPTH or pagination_found or retries:
        new_urls_set = set()
        pagination_found = retries = False

        while urls_q:
            tasks = []
            error_task_count = 0

            for _ in range(min(limit_errors, MAX_CONCURRENT_TASKS, len(urls_q))):
                page_url = urls_q.popleft()
                tasks.append(get_url_data(user_id, group_id, main_url, page_url))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:

                if isinstance(result, Exception):
                    key = clean_url(result.args[1])
                    print(f"Error: {result} -- retry count: {retried_urls[key]}")

                    if retried_urls[key] == 2:
                        continue

                    new_urls_set.add(key)
                    retried_urls[key] += 1
                    error_task_count += 1
                    retries = True
                else:
                    url_data, links = result
                    key = clean_url(url_data["url"])
                    urls_data[key] = url_data

                    if key in new_urls_set:
                        new_urls_set.remove(key)

                    for link in links:
                        if not link.startswith("http"):
                            continue

                        if check_pagination(link):
                            pagination_found = True
                        elif depth >= SCRAPER_MAX_DEPTH - 1:
                            continue

                        link = clean_url(link)

                        if link in retried_urls and retried_urls[link] == 2:
                            continue

                        if link not in urls_data:
                            new_urls_set.add(link)

            if error_task_count >= len(results) // 2:
                limit_errors = 5
                await asyncio.sleep(random.randint(60, 70))
            else:
                limit_errors = 10**9

        urls_q = deque(new_urls_set.difference(urls_data.keys()))
        depth += 1

    return urls_data


async def poll_and_process_scraping_messages(task_num):
    while True:
        # logger.info(f"Running Task No.: {task_num}")

        async with SessionLocal() as session:
            # Fetch the oldest pending message from the queue
            message = await session.scalar(
                select(URLScrapingMessageQueue)
                .filter(URLScrapingMessageQueue.messageStatus == "PENDING")
                .order_by(asc(URLScrapingMessageQueue.createdAt))
                .limit(1)
                .with_for_update(skip_locked=True)  # Lock the fetched row
            )

            if message:
                logger.info(
                    f"Scraping URLs for message with groupId: {message.groupId} and userId: {message.userId} in task no.: {task_num}"
                )
                # Set the message status to processing
                await session.execute(
                    update(URLScrapingMessageQueue)
                    .where(
                        URLScrapingMessageQueue.groupId == message.groupId,
                        URLScrapingMessageQueue.userId == message.userId,
                    )
                    .values(messageStatus=MessageQueueStatusEnum.PROCESSING)
                )
                await session.commit()
                await session.refresh(message)

                try:
                    # Process the message outside the transaction to avoid blocking
                    urls_data = await fetch_urls_recursive(
                        message.mainURL, message.userId, message.groupId
                    )

                    for page_url, url_data in urls_data.items():
                        session.add(TempUrlDataFeed(**url_data))

                    # set datafeed active to 1
                    message_status = MessageQueueStatusEnum.SUCCESS
                except:
                    message_status = MessageQueueStatusEnum.ERROR

                # After processing, mark the message status
                await session.execute(
                    update(URLScrapingMessageQueue)
                    .where(
                        URLScrapingMessageQueue.groupId == message.groupId,
                        URLScrapingMessageQueue.userId == message.userId,
                    )
                    .values(messageStatus=message_status)
                )
                await session.commit()

            # Sleep for some time before polling again
            await asyncio.sleep(5)


async def main():
    # Run multiple instances of poll_data_and_process concurrently
    tasks = [
        asyncio.create_task(poll_and_process_scraping_messages(task_num))
        for task_num in range(1, 2)
    ]
    await asyncio.gather(*tasks)


__setup_logging(LOG_LEVEL)
asyncio.run(main())
