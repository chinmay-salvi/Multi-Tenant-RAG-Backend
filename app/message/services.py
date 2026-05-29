import asyncio
import datetime
import logging
import re
from collections import OrderedDict
from typing import List
from uuid import UUID, uuid4

import anyio
from fastapi import HTTPException
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from app.db.tables import (
    Chatbot,
    Message,
    MessageRoleEnum,
    MessageStatusEnum,
    MessageSubProcess,
    MessageSubProcessStatusEnum,
    Conversation,
)
from app.chat.response_generation import (
    handle_chat_message,
    get_chat_engine,
    get_chat_engine_for_managed_backend,
)
from . import schemas
from .crud import (
    fetch_conversation_with_messages,
    fetch_managed_conversation_with_messages,
)
from .managed_backend_helper import notify_managed_backend, change_conversation_status

logger = logging.getLogger(__name__)


async def generate_chat_response_stream(
    db: AsyncSession,
    conversation_id: UUID,
    chatbot: Chatbot,
    user_message_string: str,
) -> EventSourceResponse:
    """
    Coordinates SSE response streaming for a chatbot conversation, handling event logging
    and database persistence of user and assistant messages.
    """
    # Retrieve conversation metadata and prior message history
    conversation = await fetch_conversation_with_messages(db, conversation_id)

    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    company_do = chatbot.companyDo
    chatbot_for = chatbot.chatbotFor
    hallucination_fixer = chatbot.hallucinationFixer
    business_contact_details = chatbot.businessContactDetails

    if len(conversation.messages) == 0:
        conversation.messages = [
            Message(
                conversationId=conversation_id,
                content=intro_message,
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.SUCCESS,
                orgId=conversation.orgId,
                subProcesses=[],
            )
            for intro_message in chatbot.chatbotIntroMessages
        ]
        conversation.userMessage = user_message_string
        await db.commit()
        await db.refresh(conversation)

    user_message = Message(
        createdAt=datetime.datetime.utcnow(),
        updatedAt=datetime.datetime.utcnow(),
        conversationId=conversation_id,
        content=user_message_string,
        role=MessageRoleEnum.user,
        status=MessageStatusEnum.SUCCESS,
        orgId=conversation.orgId,
    )

    send_chan, recv_chan = anyio.create_memory_object_stream(100)

    async def event_publisher():
        async with send_chan:
            task = asyncio.create_task(
                handle_chat_message(
                    schemas.Conversation.model_validate(conversation),
                    user_message_string,
                    company_do,
                    chatbot_for,
                    hallucination_fixer,
                    business_contact_details,
                    send_chan,
                )
            )
            assistant_message_id = str(uuid4())
            assistant_message = Message(
                createdAt=datetime.datetime.utcnow(),
                messageId=assistant_message_id,
                conversationId=conversation_id,
                content="",
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.PENDING,
                orgId=conversation.orgId,
                subProcesses=[],
            )
            complete_assistant_message = ""
            event_id_to_sub_process = OrderedDict()

            try:
                async for message_obj in recv_chan:

                    if isinstance(message_obj, schemas.StreamedMessage):
                        assistant_message.content = message_obj.content
                        complete_assistant_message += message_obj.content
                        yield schemas.Message.model_validate(assistant_message).model_dump_json()

                    elif isinstance(message_obj, schemas.StreamedMessageSubProcess):
                        if message_obj.event_id in event_id_to_sub_process:
                            created_at = event_id_to_sub_process[
                                message_obj.event_id
                            ].createdAt
                        else:
                            created_at = datetime.datetime.utcnow()

                        sub_process = MessageSubProcess(
                            createdAt=created_at,
                            messageId=assistant_message_id,
                            source=message_obj.source,
                            metadataMap=message_obj.metadata_map,
                            status=(
                                MessageSubProcessStatusEnum.FINISHED
                                if message_obj.has_ended
                                else MessageSubProcessStatusEnum.PENDING
                            ),
                        )
                        event_id_to_sub_process[message_obj.event_id] = sub_process

                        assistant_message.subProcesses = list(
                            event_id_to_sub_process.values()
                        )
                    else:
                        logger.error(
                            f"Unknown message object type: {type(message_obj)}"
                        )
                        continue

                await task

                if task.exception():
                    raise ValueError(
                        "handle_chat_message task failed"
                    ) from task.exception()

                assistant_message.status = MessageStatusEnum.SUCCESS
                conversation.totalConversationMessage += 2
            except Exception as e:
                print("Exception in message publisher", e)
                logger.error("Error in message publisher", exc_info=True)
                assistant_message.status = MessageStatusEnum.ERROR
                conversation.totalConversationMessage += 1

            assistant_message.content = ""
            final_message = schemas.Message.model_validate(assistant_message).model_dump_json()

            assistant_message.content = complete_assistant_message

            db.add(user_message)
            db.add(conversation)
            db.add(assistant_message)
            await db.commit()

            yield final_message

    return EventSourceResponse(event_publisher())


async def handle_managed_inbox_webhook(
    db: AsyncSession,
    bot_id: UUID,
    bot_token: str,
    body: dict,
) -> dict:
    """
    Coordinates AI suggestion queries, webhook parsing, conversation loading, pgvector context retrieval,
    relevance grading, human handoff triggers, and remote agent notifications.
    """
    chatbot = await fetch_chatbot_directly(db=db, chatbot_id=bot_id, ensure=True)

    # Check if the request contains 'ai_assist'
    if "ai_assist" in body:
        try:
            ai_assist_request = schemas.AiAssistRequest(**body)
        except ValidationError as val_err:
            logger.error(f"Validation error for AI assist: {val_err}")
            raise HTTPException(status_code=422, detail="Unprocessable Entity")

        if ai_assist_request.ai_assist:
            chat_history = [
                ChatMessage(
                    content=message["content"],
                    role=(
                        MessageRole.SYSTEM
                        if message["role"] == "system"
                        else (
                            MessageRole.USER
                            if message["role"] == "user"
                            else MessageRole.ASSISTANT
                        )
                    ),
                )
                for message in ai_assist_request.messages
                if message["content"].strip()
            ]

            last_message = chat_history.pop()

            chat_engine = await get_chat_engine(
                [],
                chat_history,
                chatbot.chatbotId,
                chatbot.orgId,
                chatbot.companyDo,
                chatbot.chatbotFor,
                chatbot.hallucinationFixer,
                chatbot.businessContactDetails,
            )

            try:
                assistant_message = await chat_engine.achat(last_message.content)
                return {"response": assistant_message.response}
            except Exception as e:
                logger.error(f"Error fetching AI assist suggestions: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        else:
            return {"message": "AI assist not enabled in payload"}

    # Process standard incoming chat webhook messages from managed backend
    try:
        message = schemas.ManagedBackendMessage(**body)
    except ValidationError as val_err:
        logger.error(f"Validation error: {val_err}")
        raise HTTPException(status_code=422, detail="Unprocessable Entity")

    if message.message_type == "incoming" and message.conversation["status"] != "open":
        conversation = await fetch_managed_conversation_with_messages(
            db, message.conversation["id"]
        )

        if conversation is None:
            conversation = Conversation(
                conversationId=uuid4(),
                chatbotId=bot_id,
                managedConversationId=message.conversation["id"],
                orgId=chatbot.orgId,
                messages=[],
                totalConversationMessage=0,
            )
            db.add(conversation)

        try:
            user_message = Message(
                createdAt=datetime.datetime.utcnow(),
                updatedAt=datetime.datetime.utcnow(),
                conversationId=conversation.conversationId,
                content=message.content,
                role=MessageRoleEnum.user,
                status=MessageStatusEnum.SUCCESS,
                orgId=conversation.orgId,
            )

            if len(conversation.messages) == 0:
                conversation.messages = [
                    Message(
                        conversationId=conversation.conversationId,
                        content=intro_message,
                        role=MessageRoleEnum.assistant,
                        status=MessageStatusEnum.SUCCESS,
                        orgId=conversation.orgId,
                        subProcesses=[],
                    )
                    for intro_message in chatbot.chatbotIntroMessages
                ]
                conversation.userMessage = message.content

            assistant_message = Message(
                createdAt=datetime.datetime.utcnow(),
                messageId=uuid4(),
                conversationId=conversation.conversationId,
                content="",
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.PENDING,
                orgId=conversation.orgId,
                subProcesses=[],
            )

            try:
                chat_history = [
                    ChatMessage(
                        content=m.content,
                        role=(
                            MessageRole.ASSISTANT
                            if m.role == MessageRoleEnum.assistant
                            else MessageRole.USER
                        ),
                    )
                    for m in conversation.messages
                    if m.content.strip()
                    and m.status == MessageStatusEnum.SUCCESS
                ]

                chat_engine = await get_chat_engine_for_managed_backend(
                    [],
                    chat_history,
                    conversation.chatbotId,
                    conversation.orgId,
                    chatbot.companyDo,
                    chatbot.chatbotFor,
                    chatbot.hallucinationFixer,
                    chatbot.businessContactDetails,
                    message.account["id"],
                    message.conversation["id"],
                    bot_token,
                )

                assistant_response = await chat_engine.achat(message.content)
                checked_response = await relevance_check_and_transfer(
                    message.account["id"],
                    message.conversation["id"],
                    bot_token,
                    message.content,
                    assistant_response.response,
                )
                assistant_message.content = checked_response

                notify_response = await notify_managed_backend(
                    message.account["id"],
                    message.conversation["id"],
                    checked_response,
                    bot_token,
                )
                assistant_message.status = MessageStatusEnum.SUCCESS
                conversation.totalConversationMessage += 2

            except Exception as e:
                logger.error(f"Error processing message response from chat engine: {e}")
                assistant_message.status = MessageStatusEnum.ERROR
                conversation.totalConversationMessage += 1
                notify_response = await notify_managed_backend(
                    message.account["id"],
                    message.conversation["id"],
                    "Sorry, something went wrong.Please try again",
                    bot_token,
                )

            db.add(user_message)
            db.add(conversation)
            db.add(assistant_message)

            await db.commit()
            await db.refresh(user_message)
            await db.refresh(conversation)
            await db.refresh(assistant_message)

            return notify_response

        except Exception as e:
            logger.error(f"Unexpected error seeding conversation records: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Not an incoming message or status is open"}


async def relevance_check_and_transfer(
    account_id, conversation_id, bot_token, query: str, response: str
) -> str:
    """
    Parses LLM outputs for relevance and human-transfer scoring, triggering handoff if needed.
    """
    relevance_match = re.search(r"\[RELEVANCE_SCORE: (0\.\d+|1\.00?)\]", response)
    transfer_match = re.search(
        r"\[TRANSFER_TO_HUMAN_SCORE: (0\.\d+|1\.00?)\]", response
    )

    if relevance_match and transfer_match:
        relevance_score = float(relevance_match.group(1))
        transfer_score = float(transfer_match.group(1))

        # Purge tracking annotations from the final text payload
        response = re.sub(r"\[RELEVANCE_SCORE: (0\.\d+|1\.00?)\]", "", response)
        response = re.sub(
            r"\[TRANSFER_TO_HUMAN_SCORE: (0\.\d+|1\.00?)\]", "", response
        ).strip()

        # Define thresholds for relevance and transfer scores
        relevance_threshold = 0.5  
        transfer_threshold = 0.7  

        # Trigger human agent handoff if the transfer score exceeds safety limits
        if transfer_score > transfer_threshold:
            human_transfer_response = await transfer_to_human_agent(
                account_id, conversation_id, bot_token
            )
            return f"{response}\n\n{human_transfer_response}"

    return response


async def transfer_to_human_agent(account_id, conversation_id, bot_token) -> str:
    """
    Triggers a live human agent handoff by altering the conversation status in the inbox panel.
    """
    logger.info("transfer_to_human_agent tool triggered")
    _ = await change_conversation_status(account_id, conversation_id, bot_token)
    return (
        "I'm transferring you to a human agent who will be able to assist you better."
    )
