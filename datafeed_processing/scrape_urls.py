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
from pgqueuer import Job


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
    """
    Crawls and scrapes URLs recursively up to the max depth (SCRAPER_MAX_DEPTH).

    This function does the following:
    1. Uses a breadth-first search (BFS) queue populated with URL targets.
    2. Runs batch requests concurrently (up to MAX_CONCURRENT_TASKS) to retrieve markdown data.
    3. Handles temporary server blocks or rate limits by dynamically adjusting concurrent limits.
    4. Identifies paginated links to dynamically support crawling paginated content.
    5. Deduplicates URLs to prevent recursive infinite loops.

    Args:
        main_url (str): The landing domain or specific URL target to crawl.
        user_id (UUID): The user ID triggering this crawl.
        group_id (UUID): The group ID grouping crawled URLs together.

    Returns:
        Dict: A mapping of cleaned URL strings to their parsed scraped metadata.
    """
    urls_data = dict()
    # BFS queue initialized with the main landing page URL
    urls_q = deque([clean_url(main_url)])
    depth = 0
    pagination_found = retries = False
    retried_urls = defaultdict(int)
    limit_errors = 10**9

    # BFS traversal loop restricted by depth and outstanding retries
    while depth < SCRAPER_MAX_DEPTH or pagination_found or retries:
        new_urls_set = set()
        pagination_found = retries = False

        while urls_q:
            tasks = []
            error_task_count = 0

            # Batch dynamic tasks to prevent excessive concurrency
            for _ in range(min(limit_errors, MAX_CONCURRENT_TASKS, len(urls_q))):
                page_url = urls_q.popleft()
                tasks.append(get_url_data(user_id, group_id, main_url, page_url))

            # Execute batch scrapes in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                # Handle error responses from third-party scraper API (Jina Reader)
                if isinstance(result, Exception):
                    key = clean_url(result.args[1])
                    print(f"Error: {result} -- retry count: {retried_urls[key]}")

                    # Limit retry attempts to 2 per URL
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

                    # Extract nested links for recursive indexing
                    for link in links:
                        if not link.startswith("http"):
                            continue

                        # Dynamic pagination validation check
                        if check_pagination(link):
                            pagination_found = True
                        elif depth >= SCRAPER_MAX_DEPTH - 1:
                            continue

                        link = clean_url(link)

                        if link in retried_urls and retried_urls[link] == 2:
                            continue

                        # Add new unvisited links to the traversal set
                        if link not in urls_data:
                            new_urls_set.add(link)

            # If rate-limiting blocks are detected, execute back-off cooldown
            if error_task_count >= len(results) // 2:
                limit_errors = 5
                await asyncio.sleep(random.randint(60, 70))
            else:
                limit_errors = 10**9

        # Populate queue with newly discovered and filtered URLs
        urls_q = deque(new_urls_set.difference(urls_data.keys()))
        depth += 1

    return urls_data


async def process_scraping_job(job: Job) -> None:
    """
    Processes a single url scraping job from PgQueuer.
    """
    import traceback
    from datetime import timedelta
    from pgqueuer import RetryRequested
    from sqlalchemy import text

    try:
        data = json.loads(job.payload.decode("utf-8"))
        group_id = UUID(data["groupId"])
        user_id = UUID(data["userId"])
        main_url = data["mainURL"]
        org_id = UUID(data["orgId"])
    except Exception as e:
        logger.error(f"Failed to parse payload for job {job.id}: {e}")
        # Delete invalid payload jobs immediately from active queue
        async with SessionLocal() as session:
            await session.execute(
                text("DELETE FROM pgqueuer WHERE id = :job_id"),
                {"job_id": job.id}
            )
            await session.commit()
        return

    logger.info(f"Picked scraping job {job.id} for mainURL: {main_url}")

    # Mark status as PROCESSING in URLScrapingMessageQueue
    async with SessionLocal() as session:
        await session.execute(
            update(URLScrapingMessageQueue)
            .where(
                URLScrapingMessageQueue.groupId == group_id,
                URLScrapingMessageQueue.userId == user_id,
            )
            .values(messageStatus=MessageQueueStatusEnum.PROCESSING)
        )
        await session.commit()

    try:
        # Run recursive scraper crawl
        urls_data = await fetch_urls_recursive(
            main_url, user_id, group_id
        )

        async with SessionLocal() as session:
            for page_url, url_data in urls_data.items():
                session.add(TempUrlDataFeed(**url_data))

            await session.execute(
                update(URLScrapingMessageQueue)
                .where(
                    URLScrapingMessageQueue.groupId == group_id,
                    URLScrapingMessageQueue.userId == user_id,
                )
                .values(messageStatus=MessageQueueStatusEnum.SUCCESS)
            )
            await session.commit()

        logger.info(f"Successfully completed scraping job {job.id}")

    except Exception as e:
        logger.exception(f"Error processing scraping job {job.id} on attempt {job.attempts}")
        
        # If we have reached the limit (3 retries / 3 attempts total), route to DLQ
        if job.attempts >= 3:
            async with SessionLocal() as session:
                await session.execute(
                    update(URLScrapingMessageQueue)
                    .where(
                        URLScrapingMessageQueue.groupId == group_id,
                        URLScrapingMessageQueue.userId == user_id,
                    )
                    .values(messageStatus=MessageQueueStatusEnum.ERROR)
                )
                await session.execute(
                    text(
                        """
                        INSERT INTO pgqueuer_dlq (original_job_id, channel, payload, error_message, traceback)
                        VALUES (:job_id, :channel, :payload, :error_message, :traceback)
                        """
                    ),
                    {
                        "job_id": job.id,
                        "channel": job.entrypoint,
                        "payload": job.payload,
                        "error_message": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )
                await session.execute(
                    text("DELETE FROM pgqueuer WHERE id = :job_id"),
                    {"job_id": job.id}
                )
                await session.commit()
            logger.error(f"Job {job.id} failed after 3 attempts. Moved to DLQ.")
        else:
            # Re-raise to trigger PgQueuer retry mechanism with backoff
            delay = timedelta(seconds=10 * (job.attempts + 1))
            raise RetryRequested(delay=delay)


async def main():
    import asyncpg
    from pgqueuer import PgQueuer
    from pgqueuer.db import AsyncpgPoolDriver
    from pgqueuer.models import Schedule
    from sqlalchemy import text

    dsn = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(dsn=dsn)
    driver = AsyncpgPoolDriver(pool)
    pgq = PgQueuer(driver)

    # Register consumer entrypoint
    pgq.entrypoint("url_scraping", concurrency_limit=1)(process_scraping_job)

    # Register pruning schedule
    @pgq.schedule("prune_successful_jobs", "0 0 * * *")
    async def prune_successful_jobs(schedule: Schedule) -> None:
        async with SessionLocal() as session:
            result = await session.execute(
                text(
                    "DELETE FROM pgqueuer WHERE status = 'successful' AND created < NOW() - INTERVAL '7 days'"
                )
            )
            await session.commit()
            logger.info(f"Scheduled pruning deleted {result.rowcount} successful jobs older than 7 days.")

    logger.info("Starting PgQueuer URL Scraper consumer loop...")
    await pgq.run()


__setup_logging(LOG_LEVEL)
asyncio.run(main())
