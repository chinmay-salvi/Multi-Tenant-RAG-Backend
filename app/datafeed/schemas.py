from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID
from datetime import datetime
from app.db.tables import DataFeedDataTypeEnum, DataFeedFileTypeEnum


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
