from typing import Sequence, List
from uuid import UUID
from fastapi import HTTPException
from sqlalchemy import (
    select,
    func,
    asc,
    desc,
    Row,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.db.tables import (
    Chatbot,
    ChatbotSupportTicketingCategories,
    ChatbotDataFeeds,
    DataFeed,
    DataFeedTag,
)


async def count_existing_chatbot(org_id: UUID, db: AsyncSession) -> int:
    stmt = select(func.count()).where(
        Chatbot.orgId == org_id, Chatbot.activeStatus == True
    )
    result = await db.execute(stmt)
    return result.scalar()


async def fetch_existing_chatbot(
    org_id: UUID,
    db: AsyncSession,
    chatbot_name: str = None,
    chatbot_id: UUID = None,
) -> Chatbot | None:
    if chatbot_id:
        stmt_tag = select(Chatbot).filter(
            Chatbot.chatbotId == chatbot_id,
            Chatbot.orgId == org_id,
            Chatbot.activeStatus == True,
        )
    else:
        stmt_tag = select(Chatbot).filter(
            Chatbot.chatbotName == chatbot_name,
            Chatbot.orgId == org_id,
            Chatbot.activeStatus == True,
        )
    result_tag = await db.execute(stmt_tag)
    existing_chatbot = result_tag.scalar_one_or_none()
    return existing_chatbot


async def fetch_chatbot_directly(
    db: AsyncSession, chatbot_id: UUID, ensure: bool = False
) -> Chatbot | None:
    stmt_tag = select(Chatbot).filter(
        Chatbot.chatbotId == chatbot_id,
        Chatbot.activeStatus == True,
    )
    result_tag = await db.execute(stmt_tag)
    chatbot = result_tag.scalar_one_or_none()

    if ensure and chatbot is None:
        raise HTTPException(status_code=404, detail="No such chatbot found")

    return chatbot


async def fetch_org_all_chatbots_id_name(
    org_id: UUID,
    db: AsyncSession,
) -> Sequence[Row[tuple[UUID, str]]]:
    stmt_tag = select(Chatbot.chatbotId, Chatbot.chatbotName).filter(
        Chatbot.orgId == org_id, Chatbot.activeStatus == True
    )
    result_tag = await db.execute(stmt_tag)
    all_active_chatbots_id_name = result_tag.fetchall()
    return all_active_chatbots_id_name


async def fetch_support_ticketing_categories_for_chatbot(
    chatbot_id: UUID, db: AsyncSession
) -> Sequence[Row[tuple[ChatbotSupportTicketingCategories]]]:
    stmt_tag = (
        select(ChatbotSupportTicketingCategories)
        .filter(ChatbotSupportTicketingCategories.chatbotId == chatbot_id)
        .order_by(
            asc(ChatbotSupportTicketingCategories.supportTicketingCategorySequence)
        )
    )

    result_tag = await db.execute(stmt_tag)
    return result_tag.fetchall()


async def fetch_chatbot_datafeeds(
    db: AsyncSession,
    chatbot_id: UUID = None,
    data_feed_ids: List[UUID] = None,
) -> Sequence[ChatbotDataFeeds]:
    if chatbot_id:
        stmt = select(ChatbotDataFeeds).filter(ChatbotDataFeeds.chatbotId == chatbot_id)
    else:
        stmt = select(ChatbotDataFeeds).filter(
            ChatbotDataFeeds.dataFeedId.in_(data_feed_ids)
        )
    chatbot_datafeeds = await db.execute(stmt)
    return chatbot_datafeeds.scalars().fetchall()


async def fetch_chatbots_datafeeds_support_ticketing_categories(
    org_id: UUID, db: AsyncSession
) -> Sequence[Chatbot]:
    query = (
        select(Chatbot)
        .filter(
            Chatbot.orgId == org_id, Chatbot.activeStatus == True
        )
        .options(
            selectinload(Chatbot.chatbotDataFeeds)
            .selectinload(ChatbotDataFeeds.datafeed)
            .selectinload(DataFeed.dataFeedTags)
            .selectinload(DataFeedTag.tag),
            selectinload(Chatbot.chatbotSupportTicketingCategories),
            selectinload(Chatbot.latestLeadForm),
        )
        .order_by(desc(Chatbot.chatbotUpdationDate))
    )

    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_chatbots_support_ticketing_categories(
    chatbot_id: UUID, db: AsyncSession
) -> Chatbot | None:
    query = (
        select(Chatbot)
        .filter(Chatbot.chatbotId == chatbot_id, Chatbot.activeStatus == True)
        .options(
            selectinload(Chatbot.chatbotSupportTicketingCategories),
            selectinload(Chatbot.latestLeadForm),
        )
    )

    result = await db.execute(query)
    return result.scalar()
