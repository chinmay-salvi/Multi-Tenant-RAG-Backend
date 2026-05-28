"""Async helpers to create tags and datafeeds for API tests.

Kept in a dedicated module so conversation, chatbot, and lead tests can import
helpers without circular imports through ``test_datafeed.py``.
"""

from __future__ import annotations

import uuid

from httpx import AsyncClient
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.tables import DataFeed
from tests.conftest import API_PREFIX, TEST_ORG_ID, TEST_USER_ID


async def create_tag(client: AsyncClient, tag_name: str) -> None:
    """Registers a tag for the test org via ``POST /add_tag``."""
    response = await client.post(
        f"{API_PREFIX}/add_tag",
        json={"userId": str(TEST_USER_ID), "tagName": tag_name},
    )
    assert response.status_code == 200


async def create_active_text_datafeed(
    client: AsyncClient, db: AsyncSession
) -> uuid.UUID:
    """Uploads a text datafeed and marks it active (simulates finished embedding).

    ``build_chatbot`` only accepts feeds with ``activeStatus == 1``.

    Args:
        client: HTTP client with auth overridden.
        db: Session used to flip ``activeStatus`` / ``tokenCount``.

    Returns:
        The new datafeed's UUID.
    """
    tag_name = f"active-feed-{uuid.uuid4().hex[:8]}"
    await create_tag(client, tag_name)

    upload = await client.post(
        f"{API_PREFIX}/text_data_upload",
        json={
            "userId": str(TEST_USER_ID),
            "text": "Processed feed for downstream tests.",
            "selectedTag": tag_name,
        },
    )
    assert upload.json()["success"] is True
    data_feed_id = uuid.UUID(upload.json()["dataFeedId"])

    # Workers would set this; tests shortcut so chatbot build accepts the feed.
    await db.execute(
        update(DataFeed)
        .where(
            DataFeed.dataFeedId == data_feed_id,
            DataFeed.orgId == TEST_ORG_ID,
        )
        .values(activeStatus=1, tokenCount=10)
    )
    await db.commit()
    return data_feed_id
