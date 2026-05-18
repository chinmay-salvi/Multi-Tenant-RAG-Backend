import logging
import os
from uuid import UUID, uuid4

import aiofiles
from fastapi import APIRouter, Depends, UploadFile, Form, File, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.crud_helper import (
    fetch_existing_tag,
    fetch_existing_user,
    fetch_datafeeds_with_tags,
    fetch_datafeeds_by_ids,
    fetch_chatbot_datafeeds,
    fetch_datafeed_tags,
    fetch_data_feed_embedding_message_directly,
    fetch_url_datafeed_by_ids,
    delete_url_datafeed_group,
    fetch_url_scraping_message,
    fetch_scraped_urls,
    count_token_utilization,
)
from app.api.datafeed_helper import delete_nodes
from app.api.deps import get_db
from app.api.plans_helper import check_datafeed_token_limit, get_limits
from app.api.s3_helper import upload_file_to_s3
from app.auth import validate_user
from app.db.tables import (
    DataFeedDataTypeEnum,
    DataFeed,
    DataFeedTag,
    DataFeedFileTypeEnum,
    DeletedDataFeed,
    DataFeedEmbeddingMessageQueue,
    MessageQueueStatusEnum,
    URLScrapingMessageQueue,
)
from app.schema import (
    TextDataUploadRequest,
    TextOrFileUploadResponse,
    ScrapeURLRequest,
    ScrapeURLResponse,
    DataFeedURL,
    FetchURLResponse,
    ParseURLRequest,
    TagResponse,
    DataFeedResponse,
    GetDataFeedResponse,
    DeleteDataFeedRequest,
)

logger = logging.getLogger(__name__)

datafeed_router = APIRouter()


@datafeed_router.post("/text_data_upload", response_model=TextOrFileUploadResponse)
async def text_data_upload(
    body: TextDataUploadRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    utilized_tokens = await count_token_utilization(token_payload["orgId"], db)
    message = check_datafeed_token_limit(token_payload["orgId"], utilized_tokens)

    if message:
        return TextOrFileUploadResponse(
            success=False,
            message=message,
            dataFeedId=None,
            tagId=None,
        )

    existing_tag = await fetch_existing_tag(
        tag=body.selectedTag, org_id=token_payload["orgId"], db=db, ensure=True
    )

    tag_id = existing_tag.tagId
    data_feed_id = uuid4()

    # Save text data to a temporary file
    temp_filename = f"{data_feed_id}.txt"
    with open(temp_filename, "w") as temp_file:
        temp_file.write(body.text)

    # Upload the file to S3 using the async function
    s3_url = await upload_file_to_s3(
        uploaded_file_path=temp_filename,
        s3_key=f"{token_payload['orgId']}/datafeeds/{temp_filename}",
    )

    # Clean up the temporary file
    os.remove(temp_filename)

    db.add(
        DataFeed(
            dataFeedId=data_feed_id,
            dataFeedName=body.text[:20],
            dataType=DataFeedDataTypeEnum.Text,
            dataFeedURL=s3_url,
            orgId=token_payload["orgId"],
        )
    )
    db.add(DataFeedTag(dataFeedId=data_feed_id, tagId=tag_id))
    db.add(
        DataFeedEmbeddingMessageQueue(
            dataFeedId=data_feed_id,
            dataFeedName=body.text[:20],
            messageStatus=MessageQueueStatusEnum.PENDING,
            dataType=DataFeedDataTypeEnum.Text,
            dataFeedURL=s3_url,
            orgId=token_payload["orgId"],
        )
    )
    await db.commit()

    # Return a successful response
    return TextOrFileUploadResponse(
        success=True,
        message="Data feed along with provided tag uploaded successfully",
        dataFeedId=data_feed_id,
        tagId=tag_id,
    )


@datafeed_router.post("/upload_file", response_model=TextOrFileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    userId: UUID = Form(...),
    selectedTag: str = Form(),
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    if userId != UUID(token_payload["userId"]):
        raise HTTPException(status_code=401, detail="Unauthorized")

    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    utilized_tokens = await count_token_utilization(token_payload["orgId"], db)
    message = check_datafeed_token_limit(token_payload["orgId"], utilized_tokens)

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
        tag=selectedTag, org_id=token_payload["orgId"], db=db, ensure=True
    )

    tag_id = existing_tag.tagId
    data_feed_id = uuid4()

    # Save the uploaded file to a temporary location
    temp_filename = f"{data_feed_id}.{file_extension.lower()}"
    async with aiofiles.open(temp_filename, "wb") as temp_file:
        content = await file.read()
        await temp_file.write(content)

    # Upload the file to S3 using the async function
    s3_url = await upload_file_to_s3(
        uploaded_file_path=temp_filename,
        s3_key=f"{token_payload['orgId']}/datafeeds/{temp_filename}",
    )

    # Clean up the temporary file
    os.remove(temp_filename)

    db.add(
        DataFeed(
            dataFeedId=data_feed_id,
            dataFeedName=file.filename,
            dataType=DataFeedDataTypeEnum.File,
            dataFeedURL=s3_url,
            fileType=file_type,
            orgId=token_payload["orgId"],
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
            orgId=token_payload["orgId"],
        )
    )
    await db.commit()

    # Return a successful response
    return TextOrFileUploadResponse(
        success=True,
        message="Data feed and tag uploaded successfully",
        dataFeedId=data_feed_id,
        tagId=tag_id,
    )


@datafeed_router.post("/scrape_url")
async def scrape_url(
    body: ScrapeURLRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        _ = await fetch_existing_user(
            org_id=token_payload["orgId"],
            user_id=token_payload["userId"],
            db=db,
            ensure=True,
        )

        utilized_tokens = await count_token_utilization(token_payload["orgId"], db)
        message = check_datafeed_token_limit(token_payload["orgId"], utilized_tokens)

        if message:
            return ScrapeURLResponse(
                message=message,
                success=False,
                userId=body.userId,
                groupId=None,
            )

        group_id = uuid4()

        db.add(
            URLScrapingMessageQueue(
                groupId=group_id,
                userId=body.userId,
                messageStatus=MessageQueueStatusEnum.PENDING,
                mainURL=body.url,
                orgId=token_payload["orgId"],
            )
        )

        await db.commit()

        return ScrapeURLResponse(
            message="Created a job to scrape URLs",
            success=True,
            userId=body.userId,
            groupId=group_id,
        )
    except Exception as e:
        print("Error occurred while trying to add URL to queue.", e)
        return ScrapeURLResponse(
            message="Error has occurred.",
            success=False,
            userId=body.userId,
            groupId=None,
        )


@datafeed_router.get("/fetch_urls")
async def fetch_urls(
    userId: UUID,
    groupId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    scraping_message = await fetch_url_scraping_message(db, groupId, userId)

    urls_data = None
    success = False

    if scraping_message:
        if scraping_message.messageStatus == MessageQueueStatusEnum.SUCCESS:
            try:
                temp_url_datafeeds = await fetch_scraped_urls(db, groupId, userId)
                urls_data = [
                    DataFeedURL.model_validate(temp_url_datafeed.__dict__)
                    for temp_url_datafeed in temp_url_datafeeds
                ]
                message = "Successfully fetched URLs"
                success = True
            except:
                message = "Error has occurred while fetching URLs"
        elif scraping_message.messageStatus == MessageQueueStatusEnum.PENDING:
            message = "Yet to start scraping URLs"
            success = True
        elif scraping_message.messageStatus == MessageQueueStatusEnum.PROCESSING:
            message = "Scraping URLs in progress"
            success = True
        elif scraping_message.messageStatus == MessageQueueStatusEnum.ERROR:
            message = "Error occurred while scraping URLs"
        else:
            message = "Reached unexpected state while scraping URLs"
    else:
        message = "No URL found corresponding to provided groupId"

    return FetchURLResponse(
        message=message,
        success=success,
        userId=userId,
        urlsData=urls_data,
    )


@datafeed_router.post("/parse_urls")
async def parse_urls(
    body: ParseURLRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    if not body.urls:
        return {
            "message": "No URLs selected",
            "success": False,
        }

    existing_tag = await fetch_existing_tag(
        tag=body.tag, org_id=token_payload["orgId"], db=db, ensure=True
    )

    existing_tag_id = existing_tag.tagId

    urls_group_id = None
    selected_urls_ids = []

    for url_data in body.urls:
        urls_group_id = url_data.groupId
        selected_urls_ids.append(url_data.urlId)

    selected_urls_data = await fetch_url_datafeed_by_ids(
        user_id=body.userId, url_datafeed_ids=selected_urls_ids, db=db
    )

    if not selected_urls_data:
        return {
            "message": "No selected URLs found.",
            "success": False,
        }

    for selected_url_data in selected_urls_data:
        data_feed_id = uuid4()

        # Save text data to a temporary file
        temp_filename = f"{data_feed_id}.txt"
        with open(temp_filename, "w") as temp_file:
            temp_file.write(selected_url_data.content)

        # Upload the file to S3 using the async function
        s3_url = await upload_file_to_s3(
            uploaded_file_path=temp_filename,
            s3_key=f"{token_payload['orgId']}/datafeeds/{temp_filename}",
        )

        # Clean up the temporary file
        os.remove(temp_filename)

        db.add(
            DataFeed(
                dataFeedId=data_feed_id,
                dataFeedName=selected_url_data.url,
                dataType=DataFeedDataTypeEnum.URL,
                mainURL=selected_url_data.mainURL,
                title=selected_url_data.pageTitle,
                dataFeedURL=s3_url,
                orgId=token_payload["orgId"],
            )
        )
        db.add(
            DataFeedEmbeddingMessageQueue(
                dataFeedId=data_feed_id,
                dataFeedName=selected_url_data.url,
                dataType=DataFeedDataTypeEnum.URL,
                mainURL=selected_url_data.mainURL,
                messageStatus=MessageQueueStatusEnum.PENDING,
                dataFeedURL=s3_url,
                orgId=token_payload["orgId"],
            )
        )
        db.add(DataFeedTag(dataFeedId=data_feed_id, tagId=existing_tag_id))

    await delete_url_datafeed_group(user_id=body.userId, group_id=urls_group_id, db=db)

    await db.commit()

    return {
        "message": "Selected URLs are being processed.",
        "success": True,
    }


@datafeed_router.get("/get_data_feed", response_model=GetDataFeedResponse)
async def get_data_feed(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Validate the existing user
        _ = await fetch_existing_user(
            org_id=token_payload["orgId"],
            user_id=token_payload["userId"],
            db=db,
            ensure=True,
        )

        # Fetch data feeds
        data_feeds = await fetch_datafeeds_with_tags(
            org_id=token_payload["orgId"], db=db
        )

        _, token_limit = await get_limits(org_id=token_payload["orgId"])

        response_data_feeds = []
        token_total = 0

        for data_feed in data_feeds:
            if data_feed.tokenCount != -1:
                token_total += data_feed.tokenCount

            # Create a DataFeedResponse object
            response_data_feeds.append(DataFeedResponse(
                dataFeedId=data_feed.dataFeedId,
                dataFeedName=data_feed.dataFeedName,
                dataType=data_feed.dataType,
                fileType=data_feed.fileType,
                createdDatetime=data_feed.createdDatetime,
                updatedDatetime=data_feed.updatedDatetime,
                tokenCount=data_feed.tokenCount,
                activeStatus=data_feed.activeStatus,
                dataFeedTags=[
                    TagResponse(tagId=dft.tag.tagId, tagName=dft.tag.tagName)
                    for dft in data_feed.dataFeedTags
                ],
            ))

        # Return the list of data feeds and their associated tags in the response
        return GetDataFeedResponse(
            message="Datafeeds fetching successful.",
            success=True,
            datafeeds=response_data_feeds,
            totalTokensConsumed=token_total,
            tokenLimit=token_limit,
        )
    except Exception as e:
        logger.exception(
            f"An error occurred while processing the data feed request.\n{str(e)}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the data feed request.",
        )


@datafeed_router.post("/delete_data_feed")
async def delete_data_feed(
    body: DeleteDataFeedRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    # Check if datafeedIds list is empty
    if not body.dataFeedIds:
        raise HTTPException(status_code=400, detail="No data feed IDs provided.")

    # Fetch the data feeds by IDs
    data_feeds = await fetch_datafeeds_by_ids(
        org_id=token_payload["orgId"],
        data_feed_ids=body.dataFeedIds,
        processed_only=False,
        db=db,
    )

    # Check if data feeds exist
    if not data_feeds:
        raise HTTPException(status_code=404, detail="No such active data feeds found.")

    chatbot_datafeeds = await fetch_chatbot_datafeeds(
        data_feed_ids=body.dataFeedIds, db=db
    )

    datafeed_tags = await fetch_datafeed_tags(data_feed_ids=body.dataFeedIds, db=db)

    try:
        for data_feed in data_feeds:
            db.add(
                DeletedDataFeed(
                    dataFeedId=data_feed.dataFeedId,
                    dataType=data_feed.dataType,
                    fileType=data_feed.fileType,
                    dataFeedName=data_feed.dataFeedName,
                    mainURL=data_feed.mainURL,
                    dataFeedURL=data_feed.dataFeedURL,
                    createdDatetime=data_feed.createdDatetime,
                    updatedDatetime=data_feed.updatedDatetime,
                    tokenCount=data_feed.tokenCount,
                    orgId=data_feed.orgId,
                )
            )
            await db.delete(data_feed)

            queue_message = await fetch_data_feed_embedding_message_directly(
                db=db, datafeed_id=data_feed.dataFeedId
            )

            if queue_message:
                await db.delete(queue_message)

        for chatbot_datafeed in chatbot_datafeeds:
            await db.delete(chatbot_datafeed)

        for datafeed_tag in datafeed_tags:
            await db.delete(datafeed_tag)

        nodes_deletion_result = delete_nodes(token_payload["orgId"], body.dataFeedIds)

        if nodes_deletion_result:
            await db.commit()
            return {"message": "Data feeds deleted successfully."}

        return {"message": "Data feeds deletion failed."}
    except Exception as e:
        # Rollback changes if any error occurs
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to delete data feeds: {str(e)}"
        )
