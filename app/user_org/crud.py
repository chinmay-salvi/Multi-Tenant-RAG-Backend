from typing import Sequence, List
from uuid import UUID
from fastapi import HTTPException
from sqlalchemy import select, Row
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.tables import (
    Organization,
    OrganizationUser,
    Tag,
    AgentType,
    SysAuthCred,
)


async def fetch_existing_organization(
    org_id: UUID, db: AsyncSession, ensure: bool = False
):
    stmt = select(Organization).filter(Organization.orgId == org_id)
    result = await db.execute(stmt)
    organization = result.scalar_one_or_none()

    if ensure and organization is None:
        raise HTTPException(status_code=404, detail="Organization not found")

    return organization


async def fetch_existing_user(
    org_id: UUID, user_id: UUID, db: AsyncSession, ensure: bool = False
) -> OrganizationUser | None:
    query = select(OrganizationUser).filter(
        OrganizationUser.userId == user_id, OrganizationUser.orgId == org_id
    )
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    if ensure and user is None:
        raise HTTPException(status_code=404, detail="User not found")

    return user


async def fetch_existing_tag(
    tag: str, org_id: UUID, db: AsyncSession, ensure: bool = False
):
    stmt_tag = select(Tag).filter(Tag.tagName == tag, Tag.orgId == org_id)
    result_tag = await db.execute(stmt_tag)
    existing_tag = result_tag.scalar_one_or_none()

    if ensure and existing_tag is None:
        raise HTTPException(status_code=404, detail="Tag not found")

    return existing_tag


async def fetch_all_tags_for_org(
    org_id: UUID, db: AsyncSession
) -> Sequence[Row[tuple[UUID, str]]]:
    stmt_tag = select(Tag.tagId, Tag.tagName).filter(Tag.orgId == org_id)
    result_tag = await db.execute(stmt_tag)
    return result_tag.fetchall()


async def fetch_agent_type(db: AsyncSession, agent_type_name: str) -> AgentType | None:
    result = await db.execute(
        select(AgentType).where(AgentType.agentTypeName == agent_type_name)
    )
    agent_type = result.scalars().first()

    if not agent_type:
        db.add(AgentType(agentTypeName=agent_type_name))
        await db.commit()
        result = await db.execute(
            select(AgentType).where(AgentType.agentTypeName == agent_type_name)
        )
        agent_type = result.scalars().first()

    return agent_type


async def fetch_random_auth_cred(
    db: AsyncSession, auth_cred_type: str
) -> Sequence[str] | None | str:
    result = await db.execute(
        select(SysAuthCred.authCred).filter(SysAuthCred.type == auth_cred_type)
    )
    return result.scalars().all()
