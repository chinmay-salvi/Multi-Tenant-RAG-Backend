import logging
from uuid import UUID
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db
from app.auth import validate_user
from app.db.tables import Tag
from .schemas import TagsAdd
from .crud import (
    fetch_existing_user,
    fetch_existing_tag,
    fetch_all_tags_for_org,
)

logger = logging.getLogger(__name__)

user_org_router = APIRouter()


@user_org_router.get("/get_tag_data")
async def get_tag_data(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves all classification tags registered under a tenant organization.

    Args:
        userId (UUID): Requesting user ID.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: A JSON response containing the requesting userId and list of tag mappings.
    """
    # Assert identity mapping
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    # Fetch all tags associated with the tenant
    tags = await fetch_all_tags_for_org(org_id=token_payload["orgId"], db=db)

    return {
        "userId": token_payload["userId"],
        "tags": [{"tagId": tag.tagId, "tagName": tag.tagName} for tag in tags],
    }


@user_org_router.post("/add_tag")
async def add_tag(
    body: TagsAdd,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
) -> str:
    """
    Registers a new datafeed classification tag under the tenant organization.

    Enforces tag name uniqueness scoped per tenant to prevent collisions.

    Args:
        body (TagsAdd): Struct containing tag name parameters.
        token_payload (dict): Decoded and verified tenant token.
        db (AsyncSession): Active database session.

    Returns:
        str: Outcome confirmation message string.
    """
    # Ensure organization and user mapping exist inside the tenant DB
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    # Check if the tag name is already active for the organization
    existing_tag = await fetch_existing_tag(
        tag=body.tagName, org_id=token_payload["orgId"], db=db
    )

    if existing_tag:
        return "Tag already exists"

    try:
        # Create and persist the Tag row
        tag = Tag(
            tagName=body.tagName,
            orgId=token_payload["orgId"],
        )
        db.add(tag)
        await db.commit()
        await db.refresh(tag)
        return "Tag added successfully"
    except Exception as e:
        print("An unexpected error occurred:", e)
        return "An unexpected error occurred"
