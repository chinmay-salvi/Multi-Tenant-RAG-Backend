import logging
from datetime import datetime, MINYEAR, MAXYEAR
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.crud_helper import (
    fetch_existing_user,
    fetch_conversations,
    fetch_conversation,
    fetch_messages,
    fetch_chatbot_directly,
)
from app.api.deps import get_db
from app.auth import validate_user
from app.schema import GetConversationsResponse, ConversationResponse

logger = logging.getLogger(__name__)

conversation_router = APIRouter()


@conversation_router.get("/get_conversations", response_model=GetConversationsResponse)
async def get_conversations(
    userId: UUID,
    chatbotId: UUID = None,
    fromDate: datetime = datetime(MINYEAR, 1, 1),
    toDate: datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999),
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetches a list of conversations for a tenant's chatbot within a given timeframe.

    Validates:
    1. Chatbot existence and organizational ownership.
    2. Requesting user belongs to the tenant organization.

    Args:
        userId (UUID): Requesting user ID.
        chatbotId (UUID, optional): Filter by chatbot ID.
        fromDate (datetime, optional): Timeframe beginning filter.
        toDate (datetime, optional): Timeframe end filter.
        token_payload (dict): Decoded and verified tenant JWT payload.
        db (AsyncSession): SQLAlchemy active database session.

    Returns:
        GetConversationsResponse: Pydantic model containing conversation arrays and aggregate metadata.
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
                ConversationResponse(
                    chatbotId=conversation.chatbotId,
                    conversationId=conversation.conversationId,
                    dateTime=conversation.createdAt.strftime("%d %B %Y,%I:%M %p"),
                    userMessage=conversation.userMessage,
                    botMessage=conversation.botMessage,
                    totalConversationMessage=conversation.totalConversationMessage,
                )
            )
            total_conversation_messages += conversation.totalConversationMessage

        return GetConversationsResponse(
            conversations=response_conversation,
            totalMessages=total_conversation_messages,
            success=True,
            message="Successfully fetched conversations associated with provided chatbot.",
        )

    except Exception as e:
        logger.error(f"Error fetching conversations: {e}")
        return GetConversationsResponse(
            success=False,
            message="Unexpected error occurred while fetching conversations associated with provided chatbot.",
        )


@conversation_router.get("/conversation")
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

    Executes:
    1. Public/Private chatbot verification checks.
    2. Identity validation queries.
    3. Retrieval of all messages in sequence.

    Args:
        conversationId (UUID): Target conversation ID.
        chatbotId (UUID): Target chatbot ID.
        request (Request): Active HTTP request object.
        authorization (str, optional): HTTP Bearer token checked for private chatbots.
        userId (UUID, optional): User ID checked for private chatbots.
        db (AsyncSession): SQLAlchemy active database session.

    Returns:
        dict: A JSON response containing conversation details and sequential message arrays.

    Raises:
        HTTPException:
            - 401 Unauthorized: If private credentials validation fails.
            - 404 Not Found: If conversation record is missing.
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
