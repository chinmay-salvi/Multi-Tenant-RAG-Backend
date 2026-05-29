from datetime import datetime, MINYEAR, MAXYEAR
from typing import Sequence, List
from uuid import UUID
from aiocache import cached, SimpleMemoryCache
from sqlalchemy import (
    select,
    func,
    desc,
    asc,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload
from app.db.tables import Conversation, Message


@cached(cache=SimpleMemoryCache, key=lambda *args, **kwargs: args[0], ttl=300)
async def count_message_utilization(
    org_id: UUID, from_datetime: datetime, to_datetime: datetime, db: AsyncSession
) -> int:
    stmt = select(func.sum(Conversation.totalConversationMessage)).where(
        Conversation.createdAt.between(
            from_datetime.replace(tzinfo=None), to_datetime.replace(tzinfo=None)
        ),
        Conversation.orgId == org_id,
    )
    result = await db.execute(stmt)
    return result.scalar()


async def fetch_conversations(
    db: AsyncSession,
    org_id: UUID,
    chatbot_id: UUID = None,
    from_date: datetime = datetime(MINYEAR, 1, 1),
    to_date: datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999),
) -> Sequence[Conversation]:
    if chatbot_id:
        query = (
            select(Conversation)
            .filter(
                Conversation.orgId == org_id,
                Conversation.chatbotId == chatbot_id,
                Conversation.createdAt >= from_date,
                Conversation.createdAt <= to_date,
            )
            .order_by(desc(Conversation.createdAt))
        )
    else:
        query = (
            select(Conversation)
            .filter(
                Conversation.orgId == org_id,
                Conversation.createdAt >= from_date,
                Conversation.createdAt <= to_date,
            )
            .order_by(desc(Conversation.createdAt))
        )

    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_conversation(
    db: AsyncSession,
    conversation_id: UUID,
) -> Conversation | None:
    query = select(Conversation).filter(
        Conversation.conversationId == conversation_id,
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def fetch_messages(db: AsyncSession, conversation_id: UUID) -> Sequence[Message]:
    query = (
        select(Message)
        .filter(Message.conversationId == conversation_id)
        .order_by(asc(Message.createdAt))
    )

    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_conversation_with_messages(
    db: AsyncSession, conversation_id: UUID
) -> Conversation | None:
    """
    Fetch a conversation with its messages + messagesubprocesses
    return None if the conversation with the given id does not exist
    """
    stmt = (
        select(Conversation)
        .options(joinedload(Conversation.messages).subqueryload(Message.subProcesses))
        .where(Conversation.conversationId == conversation_id)
    )

    result = await db.execute(stmt)
    return result.scalars().first()


async def fetch_managed_conversation_with_messages(
    db: AsyncSession, managed_conversation_id: int
) -> Conversation | None:
    """
    Fetch a conversation with its messages + messagesubprocesses
    return None if the conversation with the given id does not exist
    """
    stmt = (
        select(Conversation)
        .options(joinedload(Conversation.messages).subqueryload(Message.subProcesses))
        .where(Conversation.managedConversationId == managed_conversation_id)
    )

    result = await db.execute(stmt)
    return result.scalars().first()


async def fetch_message_with_sub_processes(
    db: AsyncSession, message_id: str
) -> Message | None:
    """
    Fetch a message with its sub processes
    return None if the message with the given id does not exist
    """
    stmt = (
        select(Message)
        .options(joinedload(Message.subProcesses))
        .where(Message.messageId == message_id)
    )
    result = await db.execute(stmt)
    return result.scalars().first()
