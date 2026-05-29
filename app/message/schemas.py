from pydantic import BaseModel, ConfigDict
from enum import Enum
from typing import List, Optional, Dict, Any, Union
from uuid import UUID
from datetime import datetime
from llama_index.core.callbacks.schema import EventPayload
from app.db.tables import (
    MessageRoleEnum,
    MessageStatusEnum,
    MessageSubProcessSourceEnum,
)


class SubProcessMetadataKeysEnum(str, Enum):
    SUB_QUESTION = EventPayload.SUB_QUESTION.value


SubProcessMetadataMap = Dict[Union[SubProcessMetadataKeysEnum, str], Any]


class StreamedMessage(BaseModel):
    content: str


class StreamedMessageSubProcess(BaseModel):
    source: MessageSubProcessSourceEnum
    has_ended: bool
    event_id: str
    metadata_map: SubProcessMetadataMap | None = None


class Message(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    conversationId: UUID
    messageId: UUID
    content: str
    role: MessageRoleEnum
    status: MessageStatusEnum
    createdAt: datetime | None = None


class Conversation(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    messages: List[Message]
    orgId: UUID
    chatbotId: UUID


class ConversationResponse(BaseModel):
    chatbotId: UUID
    conversationId: UUID
    dateTime: str
    userMessage: str | None
    botMessage: str | None
    totalConversationMessage: int
    rating: int | None = None


class GetConversationsResponse(BaseModel):
    message: str
    success: bool
    conversations: List[ConversationResponse] | None = None
    totalMessages: int | None = None


class ManagedBackendMessage(BaseModel):
    message_type: str
    content: str
    conversation: dict
    sender: dict
    account: dict


class AiAssistRequest(BaseModel):
    ai_assist: bool
    model: str
    messages: list
