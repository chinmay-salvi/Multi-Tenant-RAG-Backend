"""Tests for chatbot build and listing endpoints under ``/api/v1``."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import API_PREFIX, TEST_USER_ID
from tests.datafeed_builders import create_active_text_datafeed


@pytest.mark.asyncio
async def test_build_chatbot(client: AsyncClient, db: AsyncSession) -> None:
    """Verifies ``POST /build_chatbot`` creates an agent linked to active datafeeds.

    Vector-node updates are mocked; the test asserts API success and a new id.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.
        db: Async SQLAlchemy session used to activate a datafeed before build.

    Asserts:
        HTTP 200, ``success`` is true, and ``chatbotId`` is non-null.
    """
    data_feed_id = await create_active_text_datafeed(client, db)
    chatbot_name = f"pytest-bot-{uuid.uuid4().hex[:8]}"

    response = await client.post(
        f"{API_PREFIX}/build_chatbot",
        json={
            "userId": str(TEST_USER_ID),
            "chatbotName": chatbot_name,
            "chatbotIntroMessages": ["Hi, I am a test bot."],
            "dataFeedIds": [str(data_feed_id)],
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["chatbotId"] is not None


@pytest.mark.asyncio
async def test_build_chatbot_duplicate_name(
    client: AsyncClient, db: AsyncSession
) -> None:
    """Verifies ``POST /build_chatbot`` rejects duplicate names within an org.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.
        db: Async SQLAlchemy session used to activate a datafeed before build.

    Asserts:
        First build succeeds; second returns ``success`` false with an exists message.
    """
    data_feed_id = await create_active_text_datafeed(client, db)
    chatbot_name = f"dup-bot-{uuid.uuid4().hex[:8]}"
    payload = {
        "userId": str(TEST_USER_ID),
        "chatbotName": chatbot_name,
        "chatbotIntroMessages": ["Hello"],
        "dataFeedIds": [str(data_feed_id)],
    }

    first = await client.post(f"{API_PREFIX}/build_chatbot", json=payload)
    assert first.json()["success"] is True

    second = await client.post(f"{API_PREFIX}/build_chatbot", json=payload)
    assert second.status_code == 200
    assert second.json()["success"] is False
    assert "already exists" in second.json()["message"].lower()


@pytest.mark.asyncio
async def test_fetch_chatbots_list(client: AsyncClient, db: AsyncSession) -> None:
    """Verifies ``GET /fetch_chatbots_list`` includes a newly built chatbot.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.
        db: Async SQLAlchemy session used to activate a datafeed before build.

    Asserts:
        HTTP 200 and the built chatbot name appears in the ``chatbots`` pairs.
    """
    data_feed_id = await create_active_text_datafeed(client, db)
    chatbot_name = f"listed-bot-{uuid.uuid4().hex[:8]}"
    await client.post(
        f"{API_PREFIX}/build_chatbot",
        json={
            "userId": str(TEST_USER_ID),
            "chatbotName": chatbot_name,
            "chatbotIntroMessages": ["Listed bot"],
            "dataFeedIds": [str(data_feed_id)],
        },
    )

    response = await client.get(
        f"{API_PREFIX}/fetch_chatbots_list?userId={TEST_USER_ID}"
    )
    assert response.status_code == 200
    # Each entry is [chatbotId, chatbotName], not a dict.
    names = {pair[1] for pair in response.json()["chatbots"]}
    assert chatbot_name in names
