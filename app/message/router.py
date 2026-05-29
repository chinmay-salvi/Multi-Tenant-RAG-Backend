import asyncio
import datetime
import logging
import re
from collections import OrderedDict
from uuid import UUID, uuid4

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request, Query, Header
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from app.core.deps import get_db
from app.auth import validate_user
from app.db.tables import (
    Message,
    MessageRoleEnum,
    MessageStatusEnum,
    MessageSubProcess,
    MessageSubProcessStatusEnum,
    Conversation,
)
from app.payments.plans_helper import fetch_cached_plan_data, check_datetime_validity
from app.user_org.crud import fetch_existing_user
from app.chatbot.crud import fetch_chatbot_directly
from app.chat.response_generation import (
    handle_chat_message,
    get_chat_engine,
    get_chat_engine_for_managed_backend,
)

from . import schemas
from .crud import (
    fetch_conversations,
    fetch_conversation,
    fetch_messages,
    fetch_conversation_with_messages,
    fetch_managed_conversation_with_messages,
    count_message_utilization,
)
from .managed_backend_helper import notify_managed_backend, change_conversation_status
from .services import (
    generate_chat_response_stream,
    handle_managed_inbox_webhook,
)

logger = logging.getLogger(__name__)

message_router = APIRouter()


@message_router.get("/get_conversations", response_model=schemas.GetConversationsResponse)
async def get_conversations(
    userId: UUID,
    chatbotId: UUID = None,
    fromDate: datetime.datetime = datetime.datetime(datetime.MINYEAR, 1, 1),
    toDate: datetime.datetime = datetime.datetime(datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999),
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetches a list of conversations for a tenant's chatbot within a given timeframe.

    Validates:
    1. Chatbot existence and organizational ownership.
    2. Requesting user belongs to the tenant organization.
    """
    _ = await fetch_chatbot_directly(chatbot_id=chatbotId, db=db, ensure=True)

    try:
        logger.info("Fetching existing user")
        # Validate that requesting user is mapped inside the tenant database records
        _ = await fetch_existing_user(
            org_id=token_payload["orgId"], user_id=token_payload["userId"], db=db
        )

        # Remove timezone constraints to perform timezone-agnostic query filtering
        from_date = fromDate.replace(tzinfo=None) if fromDate.tzinfo else fromDate
        to_date = toDate.replace(tzinfo=None) if toDate.tzinfo else toDate

        logger.info("Fetching conversations")
        # Query database matching date constraints, org constraints, and chatbot filters
        conversations = await fetch_conversations(
            db=db,
            org_id=token_payload["orgId"],
            chatbot_id=chatbotId,
            from_date=from_date,
            to_date=to_date,
        )

        response_conversation = []
        total_conversation_messages = 0

        # Construct serialized Pydantic responses
        for conversation in conversations:
            response_conversation.append(
                schemas.ConversationResponse(
                    chatbotId=conversation.chatbotId,
                    conversationId=conversation.conversationId,
                    dateTime=conversation.createdAt.strftime("%d %B %Y,%I:%M %p"),
                    userMessage=conversation.userMessage,
                    botMessage=conversation.botMessage,
                    totalConversationMessage=conversation.totalConversationMessage,
                )
            )
            total_conversation_messages += conversation.totalConversationMessage

        return schemas.GetConversationsResponse(
            conversations=response_conversation,
            totalMessages=total_conversation_messages,
            success=True,
            message="Successfully fetched conversations associated with provided chatbot.",
        )

    except Exception as e:
        logger.error(f"Error fetching conversations: {e}")
        return schemas.GetConversationsResponse(
            success=False,
            message="Unexpected error occurred while fetching conversations associated with provided chatbot.",
        )


@message_router.get("/conversation")
async def get_conversation(
    conversationId: UUID,
    chatbotId: UUID,
    request: Request,
    authorization: str = Header(None),
    userId: UUID = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves the complete message history detail of a specific conversation.
    """
    chatbot = await fetch_chatbot_directly(db=db, chatbot_id=chatbotId, ensure=True)

    # Secure private endpoints from unauthorized visitors
    if not chatbot.isPublic:
        if authorization and userId:
            _ = await validate_user(request, authorization)
        else:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the main conversation row
    conversation = await fetch_conversation(conversation_id=conversationId, db=db)

    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Fetch all sequential message records associated with the conversation ID
    messages = await fetch_messages(conversation_id=conversationId, db=db)

    return {
        "conversationId": conversation.conversationId,
        "createdAt": conversation.createdAt,
        "updatedAt": conversation.updatedAt,
        "messages": [
            {
                "messageId": message.messageId,
                "createdAt": message.createdAt,
                "updatedAt": message.updatedAt,
                "content": message.content,
                "role": message.role,
                "status": message.status,
            }
            for message in messages
        ],
    }


@message_router.get("/get_messages")
async def get_messages(
    userId: UUID,
    conversationId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves the sequential message history associated with a specific conversation ID.
    """
    # Enforce organizational member verification
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"], user_id=token_payload["userId"], db=db
    )

    # Fetch targeted conversation record from relational DB
    conversation = await fetch_conversation(conversation_id=conversationId, db=db)

    if conversation:
        # Retrieve message log records associated with the conversation
        messages = await fetch_messages(conversation_id=conversationId, db=db)

        return {
            "conversationId": conversationId,
            "userId": userId,
            "chatbotId": conversation.chatbotId,
            "messages": [
                {
                    "role": message.role,
                    "message": message.content,
                    "timestamp": message.createdAt,
                }
                for message in messages
            ],
        }

    return {"conversationId": None, "userId": userId, "chatbotId": None}


@message_router.get("/message")
async def message_conversation(
    request: Request,
    conversationId: UUID,
    chatbotId: UUID,
    userMessageString: str,
    authorization: str | None = None,
    userId: UUID = None,
    db: AsyncSession = Depends(get_db),
) -> EventSourceResponse:
    """
    Handles client messages to a chatbot conversation, generating an SSE (Server-Sent Events) stream.
    """
    chatbot = await fetch_chatbot_directly(db=db, chatbot_id=chatbotId, ensure=True)

    # Secure private endpoints from unauthorized visitors
    if not chatbot.isPublic:
        if authorization and userId:
            _ = await validate_user(request, authorization)
        else:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch organization limits and active quota metrics
    plan_data = await fetch_cached_plan_data(chatbot.orgId)
    error_message = None

    if plan_data:
        # Enforce limits checks for trial organization tier
        if not plan_data.get("razorpay_subscriptions", []) and plan_data.get(
            "user_trial", []
        ):
            start_datetime = plan_data["user_trial"][0]["trial_start"]
            end_datetime = plan_data["user_trial"][0]["trial_end"]

            # Confirm trial subscription window validity
            if check_datetime_validity(start_datetime, end_datetime):
                utilized_messages = await count_message_utilization(
                    chatbot.orgId, start_datetime, end_datetime, db
                )

                if utilized_messages >= int(
                    plan_data["user_trial"][0]["notes"]["max_message"]
                ):
                    error_message = "Message limit exceeded please upgrade."
            else:
                error_message = "Trial period over, please subscribe."
    else:
        raise HTTPException(status_code=404, detail="No plan found.")

    # Yield plan error message chunks immediately if quota is exceeded
    if error_message:

        async def event_publisher():
            yield schemas.Message(
                conversationId=conversationId,
                messageId=uuid4(),
                content=error_message,
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.ERROR,
            ).model_dump_json()

        return EventSourceResponse(event_publisher())

    return await generate_chat_response_stream(
        db=db,
        conversation_id=conversationId,
        chatbot=chatbot,
        user_message_string=userMessageString,
    )


@message_router.post("/managed_conversation_message")
async def managed_conversation_message(
    request: Request,
    bot_id: UUID = Query(None),
    bot_token: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Receives incoming webhook events and AI suggestion queries from the managed inbox panel.
    """
    try:
        body = await request.json()
    except Exception as e:
        logger.error(f"Error parsing request body: {e}")
        raise HTTPException(status_code=400, detail="Bad Request")

    return await handle_managed_inbox_webhook(
        db=db,
        bot_id=bot_id,
        bot_token=bot_token,
        body=body,
    )
