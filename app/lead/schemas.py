from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID


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
