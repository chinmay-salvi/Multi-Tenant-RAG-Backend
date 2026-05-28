import asyncio
import datetime
import logging
from collections import OrderedDict
from pprint import pprint
from uuid import UUID, uuid4

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse
from sqlalchemy.orm import selectinload


from app import schema
from app.api.crud_helper import (
    fetch_existing_user,
    fetch_messages,
    fetch_conversation,
    fetch_conversation_with_messages,
    fetch_managed_conversation_with_messages,
    fetch_chatbot_directly,
    count_message_utilization,
)
from app.api.deps import get_db
from sqlalchemy.future import select
from app.api.managed_backend_helper import notify_managed_backend
from app.api.plans_helper import fetch_cached_plan_data, check_datetime_validity
from app.auth import validate_user
from app.chat.response_generation import (
    handle_chat_message,
    get_chat_engine,
    get_chat_engine_for_managed_backend,
)
from app.db.tables import (
    Message,
    MessageRoleEnum,
    MessageStatusEnum,
    MessageSubProcess,
    MessageSubProcessStatusEnum,
    Conversation,
)
from app.api.managed_backend_helper import change_conversation_status
import re

logger = logging.getLogger(__name__)

message_router = APIRouter()


@message_router.get("/get_messages")
async def get_messages(
    userId: UUID,
    conversationId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves the sequential message history associated with a specific conversation ID.

    This function does the following:
    1. Validates that the requesting user exists within the organization matched in the JWT.
    2. Queries the database for the targeted conversation record.
    3. If found, fetches all active message rows (both user queries and assistant responses)
       ordered chronologically.
    4. Serializes message role, text content, and creation timestamps for client usage.

    Args:
        userId (UUID): The user ID requesting message history.
        conversationId (UUID): Associated conversation identifier.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: A response containing conversation metadata and list of message maps.
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

    The endpoint executes the following workflow:
    1. Directs a security lookup to confirm if the chatbot is public, otherwise asserts JWT authenticity.
    2. Retrieves tenant trial/subscription metadata from the cache and counts message utilization.
    3. Triggers plan guardrails to halt execution if message limits are exceeded.
    4. Fetches conversation history to pass as grounded context.
    5. Spawns an asynchronous background generator that yields real-time assistant responses.

    Args:
        request (Request): Active HTTP request object.
        conversationId (UUID): Target conversation ID.
        chatbotId (UUID): Associated Chatbot ID.
        userMessageString (str): The query string submitted by the user.
        authorization (str, optional): HTTP Bearer token checked for private chatbots.
        userId (UUID, optional): User ID checked for private chatbots.
        db (AsyncSession): SQLAlchemy active database session.

    Returns:
        EventSourceResponse: Real-time SSE stream sending JSON-serialized message chunks.
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
            yield schema.Message(
                conversationId=conversationId,
                messageId=uuid4(),
                content=error_message,
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.ERROR,
            ).json()

        return EventSourceResponse(event_publisher())

    # Retrieve conversation metadata and prior message history
    conversation = await fetch_conversation_with_messages(db, conversationId)

    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    company_do = chatbot.companyDo
    chatbot_for = chatbot.chatbotFor
    hallucination_fixer = chatbot.hallucinationFixer
    business_contact_details = chatbot.businessContactDetails

    if len(conversation.messages) == 0:
        conversation.messages = [
            Message(
                conversationId=conversationId,
                content=intro_message,
                role=MessageRoleEnum.assistant,
                status=MessageStatusEnum.SUCCESS,
                orgId=conversation.orgId,
                subProcesses=[],
            )
            for intro_message in chatbot.chatbotIntroMessages
        ]
        conversation.userMessage = userMessageString
        await db.commit()
        await db.refresh(conversation)

    user_message = Message(
        createdAt=datetime.datetime.utcnow(),
        updatedAt=datetime.datetime.utcnow(),
        conversationId=conversationId,
        content=userMessageString,
        role=MessageRoleEnum.user,
        status=MessageStatusEnum.SUCCESS,
        orgId=conversation.orgId,
    )

    send_chan, recv_chan = anyio.create_memory_object_stream(100)

    async def event_publisher():
        async with send_chan:
            task = asyncio.create_task(
                handle_chat_message(
                    schema.Conversation.model_validate(conversation),
                    userMessageString,
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
                conversationId=conversationId,
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

                    if isinstance(message_obj, schema.StreamedMessage):
                        assistant_message.content = message_obj.content
                        complete_assistant_message += message_obj.content
                        yield schema.Message.from_orm(assistant_message).json()

                    elif isinstance(message_obj, schema.StreamedMessageSubProcess):
                        if message_obj.event_id in event_id_to_sub_process:
                            created_at = event_id_to_sub_process[
                                message_obj.event_id
                            ].createdAt
                        else:
                            created_at = datetime.datetime.utcnow()

                        sub_process = MessageSubProcess(
                            # NOTE: By setting the created_at to the current time, we are
                            # no longer able to use the created_at field to determine the
                            # time at which the subprocess was inserted into the database.
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
            final_message = schema.Message.from_orm(assistant_message).json()

            assistant_message.content = complete_assistant_message

            db.add(user_message)
            db.add(conversation)
            db.add(assistant_message)
            await db.commit()

            yield final_message

            # result = await fetch_message_with_sub_processes(db, assistant_message_id)
            # if result:
            #     final_message = schema.Message.from_orm(result)
            # else:
            #     pass  # TODO: what if None is returned by fetch_message_with_sub_processes
            #
            # yield final_message.json()

    return EventSourceResponse(event_publisher())


@message_router.post("/managed_conversation_message")
async def managed_conversation_message(
    request: Request,
    bot_id: UUID = Query(None),
    bot_token: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Receives incoming webhook events and AI suggestion queries from the managed inbox panel.

    This dual-mode endpoint operates as follows:
    1. **AI Assist Mode (`ai_assist` payload exists)**:
       Acts as a prompt completion co-pilot for human agents. It builds an isolated chat engine
       instance from conversation history and outputs response suggestions directly to the agent's screen.
    2. **Standard Webhook Mode (`incoming` message type)**:
       Synchronizes user-to-agent exchanges with the relational database. If a new conversation starts,
       it automatically provisions metadata. It invokes the standard RAG chat pipeline to compute the
       AI response, performs relevance-checking guardrails to detect handoff requests, registers the
       message rows in the database, and dispatches the final response to the customer via `notify_managed_backend`.

    Args:
        request (Request): Active HTTP request object holding json body payload.
        bot_id (UUID): Target chatbot identifier parameter.
        bot_token (str): Secret authorization token used to communicate with the managed backend inbox service.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: Completion response payloads or callback confirmation payloads.

    Raises:
        HTTPException:
            - 422 Unprocessable Entity: If request validation against Pydantic schemas fails.
            - 400 Bad Request: If request JSON parsing fails.
            - 500 Internal Server Error: If RAG processing or DB operations raise exceptions.
    """
    try:
        body = await request.json()
        chatbot = await fetch_chatbot_directly(db=db, chatbot_id=bot_id, ensure=True)

        # Check if the request contains 'ai_assist'
        if "ai_assist" in body:
            ai_assist_request = schema.AiAssistRequest(**body)

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
                    print("assistant_message=======", assistant_message)
                    return {"response": assistant_message.response}

                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))
        else:
            message = schema.ManagedBackendMessage(**body)

    except ValidationError as e:
        print("Validation error:", e)
        # print("Request body:", await request.body())
        raise HTTPException(status_code=422, detail="Unprocessable Entity")

    except Exception as e:
        print("Error parsing request body:", e)
        # print("Request body:", await request.body())
        raise HTTPException(status_code=400, detail="Bad Request")

    print("Request coming")
    print("Message details:")
    # pprint(message.dict())  # Pretty print the message as a dictionary

    # print("Bot ID:", bot_id)
    # print("Bot Token:", bot_token)

    if message.message_type == "incoming" and message.conversation["status"] != "open":
        conversation = await fetch_managed_conversation_with_messages(
            db, message.conversation["id"]
        )
        print("=====message_type == incoming")

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
                print("conversation.messages zero")
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
                        content=message.content,
                        role=(
                            MessageRole.ASSISTANT
                            if message.role == MessageRoleEnum.assistant
                            else MessageRole.USER
                        ),
                    )
                    for message in conversation.messages
                    if message.content.strip()
                    and message.status == MessageStatusEnum.SUCCESS
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
                print("===================checked_response: ", checked_response)
                assistant_message.content = checked_response

                # assistant_message.content = assistant_response.response
                print("before notify_managed_backend ==============")
                notify_response = notify_managed_backend(
                    message.account["id"],
                    message.conversation["id"],
                    checked_response,
                    bot_token,
                )
                assistant_message.status = MessageStatusEnum.SUCCESS
                conversation.totalConversationMessage += 2

            except Exception as e:
                print("Error on 472", e)
                # notify_response = None
                assistant_message.status = MessageStatusEnum.ERROR
                conversation.totalConversationMessage += 1
                notify_response = notify_managed_backend(
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
            print("Error on 495", e)
            raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Not an incoming message or status is open"}


# async def relevance_check_and_transfer(
#     account_id, conversation_id, bot_token, query: str, response: str
# ) -> str:
#     # Extract relevance score
#     print("=============================response", response)
#     match = re.search(r"\[RELEVANCE_SCORE: (0\.\d+|1\.00?)\]", response)
#     if match:
#         relevance_score = float(match.group(1))
#         # Remove the relevance score from the response
#         response = re.sub(r"\[RELEVANCE_SCORE: (0\.\d+|1\.00?)\]", "", response).strip()
#
#         if relevance_score < 0.5:  # You can adjust this threshold as needed
#             human_transfer_response = await transfer_to_human_agent(
#                 account_id, conversation_id, bot_token
#             )
#             return f"{response}\n\n{human_transfer_response}"
#     return response


async def relevance_check_and_transfer(
    account_id, conversation_id, bot_token, query: str, response: str
) -> str:
    """
    Parses LLM outputs for relevance and human-transfer scoring, triggering handoff if needed.

    This function executes the following workflow:
    1. Uses regular expressions to extract `[RELEVANCE_SCORE: X]` and `[TRANSFER_TO_HUMAN_SCORE: Y]`
       from the generated RAG response text.
    2. Strips these tracking markers from the final message text so that customer-facing text is clean.
    3. Asserts the parsed `transfer_score` against designated thresholds.
    4. If the threshold is exceeded (e.g. `transfer_score > 0.7`), it triggers an asynchronous
       handoff request to a live support representative by calling `transfer_to_human_agent`.

    Args:
        account_id (str): The external account ID for the inbox widget.
        conversation_id (str): The active conversation identifier.
        bot_token (str): Secret credential for webhook dispatching.
        query (str): The raw text submitted by the customer.
        response (str): The raw text response computed by the LLM containing meta scores.

    Returns:
        str: Cleaned text response string optionally concatenated with handoff notifications.
    """
    # Extract structural relevance markers from LLM response text
    relevance_match = re.search(r"\[RELEVANCE_SCORE: (0\.\d+|1\.00?)\]", response)

    # Extract structural transfer markers from LLM response text
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

    Args:
        account_id (str): The external account ID for the inbox widget.
        conversation_id (str): The active conversation identifier.
        bot_token (str): Secret credential for webhook dispatching.

    Returns:
        str: A message stating that the conversation has been transferred to a human agent.
    """
    print("TOOL CALLED: transfer_to_human_agent")
    # Dispatch state alteration request to the remote inbox panel API
    response = change_conversation_status(account_id, conversation_id, bot_token)
    return (
        "I'm transferring you to a human agent who will be able to assist you better."
    )
