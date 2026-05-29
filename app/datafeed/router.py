import logging
import os
from uuid import UUID, uuid4

import aiofiles
from fastapi import APIRouter, Depends, UploadFile, Form, File, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db
from app.payments.plans_helper import check_datafeed_token_limit, get_limits
from app.utils.s3_helper import upload_file_to_s3
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
from app.user_org.crud import fetch_existing_user, fetch_existing_tag

from app.chatbot.crud import fetch_chatbot_datafeeds

from .schemas import (
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
from .crud import (
    fetch_datafeeds_with_tags,
    fetch_datafeeds_by_ids,
    fetch_datafeed_tags,
    fetch_data_feed_embedding_message_directly,
    fetch_url_datafeed_by_ids,
    delete_url_datafeed_group,
    fetch_url_scraping_message,
    fetch_scraped_urls,
    count_token_utilization,
)
from .datafeed_helper import delete_nodes
from .services import (
    process_text_upload,
    process_file_upload,
    process_urls_ingestion,
)

logger = logging.getLogger(__name__)

datafeed_router = APIRouter()


@datafeed_router.post("/text_data_upload", response_model=TextOrFileUploadResponse)
async def text_data_upload(
    body: TextDataUploadRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Ingests dynamic text input as a grounding datafeed for RAG query processing.
    """
    # Assert identity mapping
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    return await process_text_upload(
        db=db,
        org_id=token_payload["orgId"],
        text=body.text,
        selected_tag=body.selectedTag,
    )


@datafeed_router.post("/upload_file", response_model=TextOrFileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    userId: UUID = Form(...),
    selectedTag: str = Form(),
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Ingests files (PDF, DOCX, JSON, TXT) as grounding datafeeds for RAG query processing.
    """
    if userId != UUID(token_payload["userId"]):
        raise HTTPException(status_code=401, detail="Unauthorized")

    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    return await process_file_upload(
        db=db,
        org_id=token_payload["orgId"],
        file=file,
        selected_tag=selectedTag,
    )


@datafeed_router.post("/scrape_url")
async def scrape_url(
    body: ScrapeURLRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Registers a background job to scrape a website domain and extract sub-pages.

    This function does the following:
    1. Asserts requesting user membership in the tenant organization.
    2. Runs quota checks to ensure the organization has not exceeded their RAG token usage limit.
    3. Provisions a new `groupId` UUID to represent this batch of scraped URLs.
    4. Pushes an indexing record to the URL Scraping Queue (`URLScrapingMessageQueue`)
       with status set to `PENDING`. This triggers background crawler workers to scrape pages.

    Args:
        body (ScrapeURLRequest): Struct containing target website url and requesting userId.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active database session.

    Returns:
        ScrapeURLResponse: Struct containing job placement status, userId, and the generated groupId.
    """
    try:
        # Verify requesting user exists in the tenant organization
        _ = await fetch_existing_user(
            org_id=token_payload["orgId"],
            user_id=token_payload["userId"],
            db=db,
            ensure=True,
        )

        # Enforce total token constraints for organization datafeeds
        utilized_tokens = await count_token_utilization(token_payload["orgId"], db)
        message = check_datafeed_token_limit(token_payload["orgId"], utilized_tokens)

        if message:
            return ScrapeURLResponse(
                message=message,
                success=False,
                userId=body.userId,
                groupId=None,
            )

        # Generate a unique batch group identifier for the scraping job
        group_id = uuid4()

        # Place the job in the scraping message queue for background workers
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
    """
    Checks the status of a URL scraping job and fetches scraped page URLs if available.

    This function does the following:
    1. Asserts requesting user membership in the tenant organization.
    2. Fetches the scraping job status from `URLScrapingMessageQueue`.
    3. Handles status transitions:
       - `PENDING`: Scraping has not yet started.
       - `PROCESSING`: Scraping is actively crawling pages.
       - `SUCCESS`: Scraping complete; queries and returns the collection of scraped sub-URLs.
       - `ERROR`: Scraping failed.

    Args:
        userId (UUID): The user ID requesting scraping status.
        groupId (UUID): The batch group ID generated when the scrape was registered.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        FetchURLResponse: Pydantic response containing status descriptions and list of discovered sub-URLs.
    """
    # Verify requesting user exists in the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    # Fetch the status row matching this specific batch jobId
    scraping_message = await fetch_url_scraping_message(db, groupId, userId)

    urls_data = None
    success = False

    if scraping_message:
        # Route logic according to background processing state
        if scraping_message.messageStatus == MessageQueueStatusEnum.SUCCESS:
            try:
                # Retrieve all scraped URLs belonging to this batch job
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
    """
    Ingests selected scraped URLs by uploading their text to S3 and queueing them for vector embedding.
    """
    # Verify requesting user exists in the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    return await process_urls_ingestion(
        db=db,
        org_id=token_payload["orgId"],
        user_id=body.userId,
        urls=[{"groupId": u.groupId, "urlId": u.urlId} for u in body.urls] if body.urls else [],
        tag_name=body.tag,
    )


@datafeed_router.get("/get_data_feed", response_model=GetDataFeedResponse)
async def get_data_feed(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves all active knowledge datafeeds and computes total token consumption for the tenant.

    This function does the following:
    1. Asserts requesting user membership in the tenant organization.
    2. Fetches all registered datafeed records along with their classification tags.
    3. Queries subscription tiers to fetch the organization's designated total token count limit.
    4. Aggregates processed token counts across all active datafeeds to compute `totalTokensConsumed`.

    Args:
        userId (UUID): Requesting user ID.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        GetDataFeedResponse: Pydantic response containing search status, list of datafeed profiles,
                             aggregated tokens consumed, and the subscription token limit.
    """
    try:
        # Validate the requesting user's organizational context
        _ = await fetch_existing_user(
            org_id=token_payload["orgId"],
            user_id=token_payload["userId"],
            db=db,
            ensure=True,
        )

        # Fetch all registered data feeds alongside their classification tags
        data_feeds = await fetch_datafeeds_with_tags(
            org_id=token_payload["orgId"], db=db
        )

        # Fetch limits allocated to the tenant organization under their active plan tier
        _, token_limit = await get_limits(org_id=token_payload["orgId"])

        response_data_feeds = []
        token_total = 0

        for data_feed in data_feeds:
            # Aggregate processed token counts, avoiding uninitialized default weights (-1)
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
    """
    Deletes specified knowledge datafeeds, archiving them and purging vector store mappings.

    This function executes the following workflow:
    1. Asserts requesting user membership in the tenant organization.
    2. Validates that the list of target datafeed IDs is not empty.
    3. Fetches all targeted datafeeds matching the IDs.
    4. Retrieves associated chatbot-to-datafeed relations and tag links.
    5. For each datafeed:
       - Archives its current metadata by copying it into a `DeletedDataFeed` record.
       - Deletes the original `DataFeed` row.
       - Deletes any unprocessed embedding queue items matching the ID from the queue table.
    6. Removes the relational links for chatbots and tags.
    7. Contacts the vector database service to purge pgvector nodes associated with these datafeeds
       by calling `delete_nodes`.

    Args:
        body (DeleteDataFeedRequest): Struct containing list of target dataFeedIds.
        token_payload (dict): Decoded and verified tenant JWT details.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: Success state confirmation string.
    """
    # Verify requesting user exists in the tenant organization
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

    # Fetch mapping relations linking knowledge feeds to active chatbots
    chatbot_datafeeds = await fetch_chatbot_datafeeds(
        data_feed_ids=body.dataFeedIds, db=db
    )

    # Fetch classification tags mapped to these knowledge feeds
    datafeed_tags = await fetch_datafeed_tags(data_feed_ids=body.dataFeedIds, db=db)

    try:
        for data_feed in data_feeds:
            # Archive the datafeed details to the historical DeletedDataFeed table
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

            # Purge pending embedding requests if the datafeed is deleted before processing
            queue_message = await fetch_data_feed_embedding_message_directly(
                db=db, datafeed_id=data_feed.dataFeedId
            )

            if queue_message:
                await db.delete(queue_message)

        # Purge association maps linking active chatbots to deleted feeds
        for chatbot_datafeed in chatbot_datafeeds:
            await db.delete(chatbot_datafeed)

        # Purge classification tag association maps
        for datafeed_tag in datafeed_tags:
            await db.delete(datafeed_tag)

        # Delete matching document vector nodes from the pgvector database
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
