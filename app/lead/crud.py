from datetime import datetime, MINYEAR, MAXYEAR
from typing import Sequence
from uuid import UUID
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.db.tables import Chatbot, Lead


async def fetch_chatbot_lead_form(
    db: AsyncSession,
    chatbot_id: UUID,
    org_id: UUID | None = None,
) -> Chatbot | None:
    stmt = (
        select(Chatbot)
        .filter(Chatbot.chatbotId == chatbot_id)
        .options(selectinload(Chatbot.latestLeadForm))
    )

    if org_id:
        stmt = stmt.filter(Chatbot.orgId == org_id)

    result = await db.execute(stmt)
    return result.scalars().first()


async def fetch_leads(
    db: AsyncSession,
    org_id: UUID,
    chatbot_id: UUID = None,
    from_date: datetime = datetime(MINYEAR, 1, 1),
    to_date: datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999),
) -> Sequence[Lead]:
    query = (
        select(Lead)
        .filter(
            Lead.orgId == org_id,
            Lead.chatbotId == chatbot_id,
            Lead.createdAt >= from_date,
            Lead.createdAt <= to_date,
        )
        .options(selectinload(Lead.leadForm))
        .order_by(desc(Lead.createdAt))
    )

    result = await db.execute(query)
    return result.scalars().fetchall()
