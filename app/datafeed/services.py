import logging
import os
from typing import List
from uuid import UUID, uuid4

import aiofiles
from fastapi import UploadFile, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.tables import (
    DataFeedDataTypeEnum,
    DataFeed,
    DataFeedTag,
    DataFeedFileTypeEnum,
    DataFeedEmbeddingMessageQueue,
    MessageQueueStatusEnum,
)
from app.user_org.crud import fetch_existing_tag
from app.payments.plans_helper import check_datafeed_token_limit
from app.utils.s3_helper import upload_file_to_s3
from .crud import (
    count_token_utilization,
    fetch_url_datafeed_by_ids,
    delete_url_datafeed_group,
)

from .schemas import TextOrFileUploadResponse

logger = logging.getLogger(__name__)


async def process_text_upload(
    db: AsyncSession,
    org_id: UUID,
    text: str,
    selected_tag: str,
) -> TextOrFileUploadResponse:
    """
    Coordinates S3 text storage, DB rows registration, and embedding queue loading for text uploads.
    """
    # Assert plan token limitations
    utilized_tokens = await count_token_utilization(org_id, db)
    message = check_datafeed_token_limit(org_id, utilized_tokens)

    if message:
        return TextOrFileUploadResponse(
            success=False,
            message=message,
            dataFeedId=None,
            tagId=None,
        )

    # Fetch or auto-create tag configurations
    existing_tag = await fetch_existing_tag(
        tag=selected_tag, org_id=org_id, db=db, ensure=True
    )

    tag_id = existing_tag.tagId
    data_feed_id = uuid4()

    # Save text data to a temporary file
    temp_filename = f"{data_feed_id}.txt"
    with open(temp_filename, "w") as temp_file:
        temp_file.write(text)

    try:
        # Upload the file to S3 using the async function
        s3_url = await upload_file_to_s3(
            uploaded_file_path=temp_filename,
            s3_key=f"{org_id}/datafeeds/{temp_filename}",
        )
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

    # Record datafeed row inside database
    db.add(
        DataFeed(
            dataFeedId=data_feed_id,
            dataFeedName=text[:20],
            dataType=DataFeedDataTypeEnum.Text,
            dataFeedURL=s3_url,
            orgId=org_id,
        )
    )
    db.add(DataFeedTag(dataFeedId=data_feed_id, tagId=tag_id))
    # Push job metadata to Embedding Queue for background processing
    db.add(
        DataFeedEmbeddingMessageQueue(
            dataFeedId=data_feed_id,
            dataFeedName=text[:20],
            messageStatus=MessageQueueStatusEnum.PENDING,
            dataType=DataFeedDataTypeEnum.Text,
            dataFeedURL=s3_url,
            orgId=org_id,
        )
    )
    await db.commit()

    return TextOrFileUploadResponse(
        success=True,
        message="Data feed along with provided tag uploaded successfully",
        dataFeedId=data_feed_id,
        tagId=tag_id,
    )


async def process_file_upload(
    db: AsyncSession,
    org_id: UUID,
    file: UploadFile,
    selected_tag: str,
) -> TextOrFileUploadResponse:
    """
    Coordinates binary file validation, temp storage, S3 upload, DB registration, and embedding queue triggers.
    """
    utilized_tokens = await count_token_utilization(org_id, db)
    message = check_datafeed_token_limit(org_id, utilized_tokens)

    if message:
        return TextOrFileUploadResponse(
            success=False,
            message=message,
            dataFeedId=None,
            tagId=None,
        )

    file_extension = file.filename.split(".")[-1].upper()

    # Map the file extension to the corresponding file type enum value
    if file_extension == "PDF":
        file_type = DataFeedFileTypeEnum.PDF
    elif file_extension == "DOCX":
        file_type = DataFeedFileTypeEnum.DOCX
    elif file_extension == "JSON":
        file_type = DataFeedFileTypeEnum.JSON
    elif file_extension == "TXT":
        file_type = DataFeedFileTypeEnum.TXT
    else:
        raise HTTPException(
            status_code=415, detail=f"Unsupported file type: {file_extension}"
        )

    existing_tag = await fetch_existing_tag(
        tag=selected_tag, org_id=org_id, db=db, ensure=True
    )

    tag_id = existing_tag.tagId
    data_feed_id = uuid4()

    # Save the uploaded file to a temporary location
    temp_filename = f"{data_feed_id}.{file_extension.lower()}"
    async with aiofiles.open(temp_filename, "wb") as temp_file:
        content = await file.read()
        await temp_file.write(content)

    try:
        # Upload the file to S3 using the async function
        s3_url = await upload_file_to_s3(
            uploaded_file_path=temp_filename,
            s3_key=f"{org_id}/datafeeds/{temp_filename}",
        )
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

    db.add(
        DataFeed(
            dataFeedId=data_feed_id,
            dataFeedName=file.filename,
            dataType=DataFeedDataTypeEnum.File,
            dataFeedURL=s3_url,
            fileType=file_type,
            orgId=org_id,
        )
    )
    db.add(DataFeedTag(dataFeedId=data_feed_id, tagId=tag_id))
    db.add(
        DataFeedEmbeddingMessageQueue(
            dataFeedId=data_feed_id,
            dataFeedName=file.filename,
            dataType=DataFeedDataTypeEnum.File,
            fileType=file_type,
            messageStatus=MessageQueueStatusEnum.PENDING,
            dataFeedURL=s3_url,
            orgId=org_id,
        )
    )
    await db.commit()

    return TextOrFileUploadResponse(
        success=True,
        message="Data feed and tag uploaded successfully",
        dataFeedId=data_feed_id,
        tagId=tag_id,
    )


async def process_urls_ingestion(
    db: AsyncSession,
    org_id: UUID,
    user_id: UUID,
    urls: List[dict],
    tag_name: str,
) -> dict:
    """
    Coordinates text extraction, temporary text file generation, S3 uploads, DB metadata persistence, and embedding queue triggers for URLs.
    """
    # Abort if no URLs were selected in the frontend widget
    if not urls:
        return {
            "message": "No URLs selected",
            "success": False,
        }

    # Fetch or auto-create tag configurations
    existing_tag = await fetch_existing_tag(
        tag=tag_name, org_id=org_id, db=db, ensure=True
    )

    existing_tag_id = existing_tag.tagId

    urls_group_id = None
    selected_urls_ids = []

    # Map the unique sub-IDs and capture the batch group ID
    for url_data in urls:
        urls_group_id = url_data.groupId
        selected_urls_ids.append(url_data.urlId)

    # Fetch temporary crawled page contents matching selected IDs
    selected_urls_data = await fetch_url_datafeed_by_ids(
        user_id=user_id, url_datafeed_ids=selected_urls_ids, db=db
    )

    if not selected_urls_data:
        return {
            "message": "No selected URLs found.",
            "success": False,
        }

    for selected_url_data in selected_urls_data:
        data_feed_id = uuid4()

        # Save extracted page text content to a temporary workspace
        temp_filename = f"{data_feed_id}.txt"
        with open(temp_filename, "w") as temp_file:
            temp_file.write(selected_url_data.content)

        try:
            # Upload the text file asynchronously to long-term S3 storage
            s3_url = await upload_file_to_s3(
                uploaded_file_path=temp_filename,
                s3_key=f"{org_id}/datafeeds/{temp_filename}",
            )
        finally:
            # Clean up the temporary workspace file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

        # Insert long-term DataFeed configuration record in database
        db.add(
            DataFeed(
                dataFeedId=data_feed_id,
                dataFeedName=selected_url_data.url,
                dataType=DataFeedDataTypeEnum.URL,
                mainURL=selected_url_data.mainURL,
                title=selected_url_data.pageTitle,
                dataFeedURL=s3_url,
                orgId=org_id,
            )
        )
        # Push indexing job into the embedding queue for background workers
        db.add(
            DataFeedEmbeddingMessageQueue(
                dataFeedId=data_feed_id,
                dataFeedName=selected_url_data.url,
                dataType=DataFeedDataTypeEnum.URL,
                mainURL=selected_url_data.mainURL,
                messageStatus=MessageQueueStatusEnum.PENDING,
                dataFeedURL=s3_url,
                orgId=org_id,
            )
        )
        # Map tag to the newly registered page feed
        db.add(DataFeedTag(dataFeedId=data_feed_id, tagId=existing_tag_id))

    # Purge the temporary crawled URL assets to release database storage
    await delete_url_datafeed_group(user_id=user_id, group_id=urls_group_id, db=db)

    await db.commit()

    return {
        "message": "Selected URLs are being processed.",
        "success": True,
    }
