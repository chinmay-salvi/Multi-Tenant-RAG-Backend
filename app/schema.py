"""
Pydantic Schemas for the API
"""

from pydantic import BaseModel, Field, validator, ConfigDict
from enum import Enum
from typing import List, Optional, Dict, Union, Any
from uuid import UUID
from datetime import datetime

# from llama_index.core.schema import BaseNode, NodeWithScore
from llama_index.core.callbacks.schema import EventPayload

# from llama_index.core.query_engine.sub_question_query_engine import (
#     SubQuestionAnswerPair,
# )
from app.db.tables import (
    MessageRoleEnum,
    MessageStatusEnum,
    MessageSubProcessSourceEnum,
    MessageSubProcessStatusEnum,
    DataFeedDataTypeEnum,
    DataFeedFileTypeEnum,
)

# later will be Union[QuestionAnswerPair, more to add later... ]
class SubProcessMetadataKeysEnum(str, Enum):
    SUB_QUESTION = EventPayload.SUB_QUESTION.value


# keeping the typing pretty loose here, in case there are changes to the metadata data formats.
SubProcessMetadataMap = Dict[Union[SubProcessMetadataKeysEnum, str], Any]


class StreamedMessage(BaseModel):
    content: str


class StreamedMessageSubProcess(BaseModel):
    source: MessageSubProcessSourceEnum
    has_ended: bool
    event_id: str
    metadata_map: SubProcessMetadataMap | None = None  # TODO: remove if not used


# ---------------------------------------------------------------------------------------------


class OrgAdd(BaseModel):
    # userId: UUID
    userId: str
    orgName: str


class TagsAdd(BaseModel):
    userId: UUID
    tagName: str


class TextDataUploadRequest(BaseModel):
    userId: UUID
    text: str
    selectedTag: str


class TextOrFileUploadResponse(BaseModel):
    message: str
    success: bool
    dataFeedId: UUID | None
    tagId: UUID | None


class ScrapeURLRequest(BaseModel):
    userId: UUID
    url: str


class ScrapeURLResponse(BaseModel):
    message: str
    success: bool
    userId: UUID
    groupId: UUID | None


class DataFeedURL(BaseModel):
    urlId: UUID
    groupId: UUID
    url: str
    tokenCount: int


class FetchURLResponse(BaseModel):
    message: str
    success: bool
    userId: UUID
    urlsData: List[DataFeedURL] | None


class ParseURLRequest(BaseModel):
    userId: UUID
    urls: List[DataFeedURL]
    tag: str


class TagResponse(BaseModel):
    tagId: UUID
    tagName: str


class DataFeedResponse(BaseModel):
    dataFeedId: UUID
    dataFeedName: str
    tokenCount: int
    dataType: DataFeedDataTypeEnum
    fileType: DataFeedFileTypeEnum | None
    createdDatetime: datetime
    updatedDatetime: datetime
    activeStatus: int
    dataFeedTags: List[TagResponse]


class GetDataFeedResponse(BaseModel):
    message: str
    success: bool
    totalTokensConsumed: int | None = None
    tokenLimit: int | None = None
    datafeeds: List[DataFeedResponse] | None = None


class DeleteDataFeedRequest(BaseModel):
    userId: UUID
    dataFeedIds: List[UUID]


class BuildChatbotRequest(BaseModel):
    userId: UUID
    chatbotName: str
    chatbotIntroMessages: List[str]
    dataFeedIds: List[UUID]


class DeleteChatbotRequest(BaseModel):
    userId: UUID
    chatbotId: UUID


class SaveChatbotDataFeedsRequest(BaseModel):
    userId: UUID
    chatbotId: UUID
    dataFeedIds: List[UUID]


class ChatbotWidgetLogoBase64Response(BaseModel):
    message: str
    success: bool
    chatbotId: UUID | None = None
    widgetLogoBase64: str | None = None


class TicketStatusChangeRequest(BaseModel):
    userId: UUID
    chatbotId: UUID
    ticketId: str
    status: str


class TicketUsingEmailRequest(BaseModel):
    userId: UUID
    chatbotId: UUID | None = None
    email: str


class TicketUsingEmailResponse(BaseModel):
    name: str
    email: str
    userProfilePicUrl: str
    userFirstContactDateTime: str
    userLanguage: str
    tickets: List


# class MessageSubProcess(Base):
#     model_config = ConfigDict(from_attributes=True)
#     messageId: UUID
#     source: MessageSubProcessSourceEnum
#     status: MessageSubProcessStatusEnum
#     metadataMap: Optional[SubProcessMetadataMap]


class Message(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    conversationId: UUID
    messageId: UUID
    content: str
    role: MessageRoleEnum
    status: MessageStatusEnum
    createdAt: datetime | None = None
    # subProcesses: List[MessageSubProcess]


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


class SaveLeadFormRequest(BaseModel):
    userId: UUID
    chatbotId: UUID
    isLeadFormEnabled: bool
    leadFormTitle: str | None = None
    leadFormKeys: List[str] | None = None
    leadFormLabels: List[str] | None = None
    leadFormInputTypes: List[str] | None = None
    frequencyHours: int | None = None
    maxShowLimit: int | None = None


class SaveLeadRequest(BaseModel):
    chatbotId: UUID
    conversationId: UUID
    leadFormKeys: List[str]
    leadFormLabels: List[str]
    leadFormValues: List[str]


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
