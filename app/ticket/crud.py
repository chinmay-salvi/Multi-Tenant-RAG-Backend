from typing import Sequence
from uuid import UUID
from sqlalchemy import select, asc, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.tables import Tickets


async def fetch_tickets(
    org_id: UUID, chatbot_id: UUID, db: AsyncSession
) -> Sequence[Tickets]:
    if chatbot_id:
        query = (
            select(Tickets)
            .filter(Tickets.orgId == org_id, Tickets.chatbotId == chatbot_id)
            .order_by(asc(Tickets.createdTime))
        )
    else:
        query = (
            select(Tickets)
            .filter(Tickets.orgId == org_id)
            .order_by(asc(Tickets.createdTime))
        )

    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_ticket(
    ticket_id: int, org_id: UUID, chatbot_id: UUID, db: AsyncSession
) -> Tickets | None:
    query = select(Tickets).filter(
        Tickets.ticketId == ticket_id,
        Tickets.orgId == org_id,
        Tickets.chatbotId == chatbot_id,
    )

    result = await db.execute(query)
    return result.scalar()


async def fetch_customer_tickets_email(
    customer_email: str, org_id: UUID, chatbot_id: UUID, db: AsyncSession
) -> Sequence[Tickets]:
    if chatbot_id:
        query = (
            select(Tickets)
            .filter(
                Tickets.email == customer_email,
                Tickets.orgId == org_id,
                Tickets.chatbotId == chatbot_id,
            )
            .order_by(desc(Tickets.createdTime))
        )
    else:
        query = (
            select(Tickets)
            .filter(Tickets.email == customer_email, Tickets.orgId == org_id)
            .order_by(desc(Tickets.createdTime))
        )

    result = await db.execute(query)
    return result.scalars().fetchall()
