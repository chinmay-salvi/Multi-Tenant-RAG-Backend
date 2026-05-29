from pydantic import BaseModel
from uuid import UUID


class OrgAdd(BaseModel):
    userId: str
    orgName: str


class TagsAdd(BaseModel):
    userId: UUID
    tagName: str
