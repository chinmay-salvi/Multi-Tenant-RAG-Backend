"""Tests for conversation list/detail endpoints under ``/api/v1``."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.tables import Chatbot, Conversation, Message, MessageRoleEnum, MessageStatusEnum
from tests.chatbot_builders import create_chatbot_via_api
from tests.conftest import API_PREFIX, TEST_ORG_ID, TEST_USER_ID


async def _seed_conversation(
    db: AsyncSession,
    *,
    chatbot_id: uuid.UUID,
    user_message: str,
    bot_message: str,
    total_messages: int = 2,
) -> uuid.UUID:
    """Creates a conversation row for test assertions.

    Args:
        db: Async SQLAlchemy session.
        chatbot_id: Chatbot to bind the conversation to.
        user_message: First user utterance snapshot.
        bot_message: First assistant utterance snapshot.
        total_messages: Value to persist in ``totalConversationMessage``.

    Returns:
        UUID of the new conversation.
    """
    conversation_id = uuid.uuid4()
    db.add(
        Conversation(
            conversationId=conversation_id,
            chatbotId=chatbot_id,
            orgId=TEST_ORG_ID,
            userMessage=user_message,
            botMessage=bot_message,
            totalConversationMessage=total_messages,
            createdAt=datetime.utcnow() - timedelta(minutes=1),
            updatedAt=datetime.utcnow(),
        )
    )
    await db.commit()
    return conversation_id


@pytest.mark.asyncio
async def test_get_conversations_returns_rows(client: AsyncClient, db: AsyncSession) -> None:
    """Verifies ``GET /get_conversations`` returns conversations for a chatbot.

    Args:
        client: HTTP client with auth override.
        db: DB session used to seed conversation rows.

    Asserts:
        Response is successful and includes seeded conversations with total count.
    """
    chatbot_id = await create_chatbot_via_api(client, db)
    await _seed_conversation(
        db,
        chatbot_id=chatbot_id,
        user_message="Hello",
        bot_message="Hi there!",
        total_messages=2,
    )
    await _seed_conversation(
        db,
        chatbot_id=chatbot_id,
        user_message="Need pricing",
        bot_message="Here is pricing.",
        total_messages=3,
    )

    response = await client.get(
        f"{API_PREFIX}/get_conversations?userId={TEST_USER_ID}&chatbotId={chatbot_id}"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["message"].startswith("Successfully fetched conversations")
    assert len(body["conversations"]) >= 2
    assert body["totalMessages"] >= 5


@pytest.mark.asyncio
async def test_get_conversation_public_chatbot_returns_messages(
    client: AsyncClient, db: AsyncSession
) -> None:
    """Verifies ``GET /conversation`` returns message list for a public chatbot.

    Args:
        client: HTTP client with auth override.
        db: DB session used to toggle chatbot visibility and seed messages.

    Asserts:
        Response includes both user and assistant messages in persisted order.
    """
    chatbot_id = await create_chatbot_via_api(client, db)
    await db.execute(
        update(Chatbot)
        .where(Chatbot.chatbotId == chatbot_id, Chatbot.orgId == TEST_ORG_ID)
        .values(isPublic=True)
    )
    await db.commit()

    conversation_id = await _seed_conversation(
        db,
        chatbot_id=chatbot_id,
        user_message="Help me",
        bot_message="Sure",
        total_messages=2,
    )
    db.add_all(
        [
            Message(
                messageId=uuid.uuid4(),
                conversationId=conversation_id,
                content="Help me",
                role=MessageRoleEnum.user,
                status=MessageStatusEnum.SUCCESS,
                orgId=TEST_ORG_ID,
                createdAt=datetime.utcnow() - timedelta(seconds=5),
                updatedAt=datetime.utcnow() - timedelta(seconds=5),
            ),
            Message(
                messageId=uuid.uuid4(),
                conversationId=conversation_id,
                content="Sure, what do you need?",
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.SUCCESS,
                orgId=TEST_ORG_ID,
                createdAt=datetime.utcnow(),
                updatedAt=datetime.utcnow(),
            ),
        ]
    )
    await db.commit()

    response = await client.get(
        f"{API_PREFIX}/conversation?conversationId={conversation_id}&chatbotId={chatbot_id}"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["conversationId"] == str(conversation_id)
    assert len(body["messages"]) == 2
    assert body["messages"][0]["role"] == MessageRoleEnum.user.value
    assert body["messages"][1]["role"] == MessageRoleEnum.assistant.value


@pytest.mark.asyncio
async def test_get_conversation_returns_404_for_missing_conversation(
    client: AsyncClient, db: AsyncSession
) -> None:
    """Verifies ``GET /conversation`` returns 404 when conversation does not exist.

    Args:
        client: HTTP client with auth override.
        db: DB session used to create and expose a public chatbot.

    Asserts:
        HTTP 404 with ``Conversation not found`` detail.
    """
    chatbot_id = await create_chatbot_via_api(client, db)
    await db.execute(
        update(Chatbot)
        .where(Chatbot.chatbotId == chatbot_id, Chatbot.orgId == TEST_ORG_ID)
        .values(isPublic=True)
    )
    await db.commit()

    response = await client.get(
        f"{API_PREFIX}/conversation?conversationId={uuid.uuid4()}&chatbotId={chatbot_id}"
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Conversation not found"
