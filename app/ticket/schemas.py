from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID


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
