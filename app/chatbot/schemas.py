from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID


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
