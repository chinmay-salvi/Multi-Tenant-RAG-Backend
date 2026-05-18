import logging
from uuid import UUID
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app import schema
from app.api.crud_helper import (
    fetch_existing_user,
    fetch_existing_tag,
    fetch_all_tags_for_org,
)
from app.api.deps import get_db
from app.auth import validate_user
from app.db.tables import Tag


logger = logging.getLogger(__name__)

tags_router = APIRouter()


@tags_router.get("/get_tag_data")
async def get_tag_data(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    tags = await fetch_all_tags_for_org(org_id=token_payload["orgId"], db=db)

    return {
        "userId": token_payload["userId"],
        "tags": [{"tagId": tag.tagId, "tagName": tag.tagName} for tag in tags],
    }


@tags_router.post("/add_tag")
async def add_tag(
    body: schema.TagsAdd,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
) -> str:
    """
    Adds a new tag.
    :param body: Body containing tag details.
    :param token_payload: Token payload with user and organization information.
    :param db: Asynchronous database session.
    :return: Success message.
    """
    # Ensure organization exists using orgId from token_payload and also user exists using userId
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    # Check if the tag already exists for the given orgId and tagName
    existing_tag = await fetch_existing_tag(
        tag=body.tagName, org_id=token_payload["orgId"], db=db
    )

    if existing_tag:
        return "Tag already exists"

    try:
        # Add the new tag
        tag = Tag(
            tagName=body.tagName,
            orgId=token_payload["orgId"],
        )
        db.add(tag)
        await db.commit()
        await db.refresh(tag)
        return "Tag added successfully"
    except Exception as e:
        # Handle other exceptions
        print("An unexpected error occurred:", e)
        return "An unexpected error occurred"
