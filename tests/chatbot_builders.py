"""Async helpers to create chatbots for API tests."""

from __future__ import annotations

import uuid

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import API_PREFIX, TEST_USER_ID
from tests.datafeed_builders import create_active_text_datafeed


async def create_chatbot_via_api(
    client: AsyncClient,
    db: AsyncSession,
    *,
    chatbot_name: str | None = None,
) -> uuid.UUID:
    """Builds a Support chatbot linked to one active text datafeed.

    Args:
        client: HTTP client with auth overridden.
        db: DB session for activating a datafeed before build.
        chatbot_name: Optional stable name; defaults to a unique ``pytest-`` prefix.

    Returns:
        New ``chatbotId`` from ``POST /build_chatbot``.
    """
    data_feed_id = await create_active_text_datafeed(client, db)
    name = chatbot_name or f"pytest-bot-{uuid.uuid4().hex[:8]}"
    response = await client.post(
        f"{API_PREFIX}/build_chatbot",
        json={
            "userId": str(TEST_USER_ID),
            "chatbotName": name,
            "chatbotIntroMessages": ["Test intro."],
            "dataFeedIds": [str(data_feed_id)],
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    return uuid.UUID(str(body["chatbotId"]))
