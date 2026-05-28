"""Tests for lead form and lead capture endpoints under ``/api/v1``."""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.tables import Conversation
from tests.chatbot_builders import create_chatbot_via_api
from tests.conftest import API_PREFIX, TEST_ORG_ID, TEST_USER_ID


async def _seed_conversation(db: AsyncSession, chatbot_id: uuid.UUID) -> uuid.UUID:
    """Creates a minimal conversation row for lead linkage.

    Args:
        db: Async SQLAlchemy session.
        chatbot_id: Parent chatbot id for the conversation.

    Returns:
        UUID of the newly inserted conversation row.
    """
    conversation_id = uuid.uuid4()
    db.add(
        Conversation(
            conversationId=conversation_id,
            chatbotId=chatbot_id,
            orgId=TEST_ORG_ID,
            userMessage="Interested in demo",
            botMessage="Please share your details.",
            totalConversationMessage=2,
            createdAt=datetime.utcnow(),
            updatedAt=datetime.utcnow(),
        )
    )
    await db.commit()
    return conversation_id


@pytest.mark.asyncio
async def test_save_lead_form_success(client: AsyncClient, db: AsyncSession) -> None:
    """Verifies ``POST /save_lead_form`` enables and stores a lead form template.

    Args:
        client: HTTP client with auth override.
        db: DB session used to create a chatbot before the API call.

    Asserts:
        API returns success when keys/labels/inputTypes are aligned.
    """
    chatbot_id = await create_chatbot_via_api(client, db)
    response = await client.post(
        f"{API_PREFIX}/save_lead_form",
        json={
            "userId": str(TEST_USER_ID),
            "chatbotId": str(chatbot_id),
            "isLeadFormEnabled": True,
            "leadFormTitle": "Contact details",
            "leadFormKeys": ["name", "email"],
            "leadFormLabels": ["Name", "Email"],
            "leadFormInputTypes": ["text", "email"],
            "frequencyHours": 24,
            "maxShowLimit": 3,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["message"] == "Lead form saved successfully."


@pytest.mark.asyncio
async def test_save_lead_success(client: AsyncClient, db: AsyncSession) -> None:
    """Verifies ``POST /save_lead`` stores submitted lead values for a conversation.

    Args:
        client: HTTP client with auth override.
        db: DB session used to create chatbot and conversation fixtures.

    Asserts:
        API returns success for payload matching the saved lead form schema.
    """
    chatbot_id = await create_chatbot_via_api(client, db)
    await client.post(
        f"{API_PREFIX}/save_lead_form",
        json={
            "userId": str(TEST_USER_ID),
            "chatbotId": str(chatbot_id),
            "isLeadFormEnabled": True,
            "leadFormTitle": "Contact details",
            "leadFormKeys": ["name", "email"],
            "leadFormLabels": ["Name", "Email"],
            "leadFormInputTypes": ["text", "email"],
            "frequencyHours": 24,
            "maxShowLimit": 3,
        },
    )
    conversation_id = await _seed_conversation(db, chatbot_id)

    response = await client.post(
        f"{API_PREFIX}/save_lead",
        json={
            "chatbotId": str(chatbot_id),
            "conversationId": str(conversation_id),
            "leadFormKeys": ["name", "email"],
            "leadFormLabels": ["Name", "Email"],
            "leadFormValues": ["Chinmay", "chinmay@example.com"],
        },
    )
    assert response.status_code == 200
    assert response.json()["success"] is True


@pytest.mark.asyncio
async def test_retrieve_leads_returns_saved_data(
    client: AsyncClient, db: AsyncSession
) -> None:
    """Verifies ``GET /retrieve_leads`` returns lead rows for the chatbot.

    Args:
        client: HTTP client with auth override.
        db: DB session used to seed chatbot, lead form, and lead submission.

    Asserts:
        Response has at least one lead with expected keys and values ordering.
    """
    chatbot_id = await create_chatbot_via_api(client, db)
    await client.post(
        f"{API_PREFIX}/save_lead_form",
        json={
            "userId": str(TEST_USER_ID),
            "chatbotId": str(chatbot_id),
            "isLeadFormEnabled": True,
            "leadFormTitle": "Contact details",
            "leadFormKeys": ["name", "email"],
            "leadFormLabels": ["Name", "Email"],
            "leadFormInputTypes": ["text", "email"],
            "frequencyHours": 24,
            "maxShowLimit": 3,
        },
    )
    conversation_id = await _seed_conversation(db, chatbot_id)
    await client.post(
        f"{API_PREFIX}/save_lead",
        json={
            "chatbotId": str(chatbot_id),
            "conversationId": str(conversation_id),
            "leadFormKeys": ["name", "email"],
            "leadFormLabels": ["Name", "Email"],
            "leadFormValues": ["Alice", "alice@example.com"],
        },
    )

    response = await client.get(
        f"{API_PREFIX}/retrieve_leads?userId={TEST_USER_ID}&chatbotId={chatbot_id}"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert len(body["leads"]) >= 1
    assert body["leads"][0]["leadKeys"] == ["name", "email"]
    assert body["leads"][0]["leadValues"] == ["Alice", "alice@example.com"]
