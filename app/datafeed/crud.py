from typing import Sequence, List
from uuid import UUID
from aiocache import cached, SimpleMemoryCache
from sqlalchemy import (
    select,
    delete,
    func,
    desc,
    asc,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.db.tables import (
    DataFeed,
    DataFeedTag,
    TempUrlDataFeed,
    DataFeedEmbeddingMessageQueue,
    MessageQueueStatusEnum,
    URLScrapingMessageQueue,
)


@cached(cache=SimpleMemoryCache, key=lambda *args, **kwargs: args[0], ttl=30)
async def count_token_utilization(org_id: UUID, db: AsyncSession) -> int:
    stmt = select(func.sum(DataFeed.tokenCount)).where(
        DataFeed.tokenCount != -1, DataFeed.activeStatus == 1, DataFeed.orgId == org_id
    )
    result = await db.execute(stmt)
    return result.scalar() or 0


async def fetch_datafeeds_with_tags(
    org_id: UUID, db: AsyncSession
) -> Sequence[DataFeed]:
    stmt = (
        select(DataFeed)
        .filter(DataFeed.orgId == org_id)
        .options(selectinload(DataFeed.dataFeedTags).selectinload(DataFeedTag.tag))
        .order_by(desc(DataFeed.createdDatetime))
    )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().fetchall()


async def fetch_datafeed_tags(
    data_feed_ids: List[UUID], db: AsyncSession
) -> Sequence[DataFeedTag]:
    stmt = select(DataFeedTag).filter(DataFeedTag.dataFeedId.in_(data_feed_ids))
    datafeed_tags = await db.execute(stmt)
    return datafeed_tags.scalars().fetchall()


async def fetch_datafeed_by_name(
    org_id: UUID, data_feed_name: str, db: AsyncSession
) -> DataFeed | None:
    stmt = (
        select(DataFeed)
        .filter(DataFeed.orgId == org_id, DataFeed.dataFeedName == data_feed_name)
        .options(selectinload(DataFeed.dataFeedTags).selectinload(DataFeedTag.tag))
    )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().first()


async def fetch_datafeeds_by_ids(
    org_id: UUID, data_feed_ids: List[UUID], processed_only: bool, db: AsyncSession
) -> Sequence[DataFeed]:
    if processed_only:
        stmt = (
            select(DataFeed)
            .filter(
                DataFeed.orgId == org_id,
                DataFeed.activeStatus == 1,
                DataFeed.dataFeedId.in_(data_feed_ids),
            )
            .order_by(asc(DataFeed.createdDatetime))
        )
    else:
        stmt = (
            select(DataFeed)
            .filter(
                DataFeed.orgId == org_id,
                DataFeed.dataFeedId.in_(data_feed_ids),
            )
            .order_by(asc(DataFeed.createdDatetime))
        )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().fetchall()


async def fetch_url_datafeed_by_ids(
    user_id: UUID, url_datafeed_ids: List[UUID], db: AsyncSession
) -> Sequence[TempUrlDataFeed]:
    stmt = select(TempUrlDataFeed).filter(
        TempUrlDataFeed.userId == user_id,
        TempUrlDataFeed.urlId.in_(url_datafeed_ids),
    )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().fetchall()


async def delete_url_datafeed_group(user_id: UUID, group_id: UUID, db: AsyncSession):
    stmt = delete(TempUrlDataFeed).filter(
        TempUrlDataFeed.userId == user_id, TempUrlDataFeed.groupId == group_id
    )
    await db.execute(stmt)


async def fetch_data_feed_embedding_message_directly(
    db: AsyncSession, datafeed_id: UUID
) -> DataFeedEmbeddingMessageQueue | None:
    stmt_tag = select(DataFeedEmbeddingMessageQueue).filter(
        DataFeedEmbeddingMessageQueue.dataFeedId == datafeed_id,
        DataFeedEmbeddingMessageQueue.messageStatus == MessageQueueStatusEnum.PENDING,
    )
    result_tag = await db.execute(stmt_tag)
    queue_message = result_tag.scalar_one_or_none()

    return queue_message


async def fetch_url_scraping_message(
    db: AsyncSession, group_id: UUID, user_id: UUID
) -> URLScrapingMessageQueue | None:
    stmt = select(URLScrapingMessageQueue).where(
        URLScrapingMessageQueue.groupId == group_id,
        URLScrapingMessageQueue.userId == user_id,
    )
    result = await db.execute(stmt)
    return result.scalars().first()


async def fetch_scraped_urls(
    db: AsyncSession, group_id: UUID, user_id: UUID
) -> Sequence[TempUrlDataFeed]:
    stmt = select(TempUrlDataFeed).where(
        TempUrlDataFeed.groupId == group_id,
        TempUrlDataFeed.userId == user_id,
    )
    result = await db.execute(stmt)
    return result.scalars().all()
