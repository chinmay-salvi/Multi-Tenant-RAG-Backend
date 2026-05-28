import asyncio
import json
import logging
import os
import random
import sys
from typing import List, Tuple

import tiktoken
from aiobotocore.session import get_session
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document
from llama_index.embeddings.cloudflare_workersai import CloudflareEmbedding
from llama_index.vector_stores.postgres import PGVectorStore
from sqlalchemy import (
    asc,
    make_url,
    update,
    UUID,
    func,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.sql.expression import select

from config import (
    AWS_REGION,
    CLOUDFLARE_EMBEDDING_MODEL_DIMENSION,
    CLOUDFLARE_EMBEDDING_MODEL_NAME,
    DATABASE_URL,
    LOG_LEVEL,
    NODE_PARSER_CHUNK_OVERLAP,
    NODE_PARSER_CHUNK_SIZE,
    S3_BUCKET_NAME,
    VECTOR_STORE_TABLE_NAME,
    load_auth_cred_dict,
    s3_client_credentials,
)
from plans_helper import check_datafeed_token_limit
from tables import (
    SysAuthCred,
    DataFeedEmbeddingMessageQueue,
    MessageQueueStatusEnum,
    DataFeed,
)

logger = logging.getLogger(__name__)


def __setup_logging(log_level: str):
    log_level = getattr(logging, log_level.upper())
    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    root_logger.addHandler(stream_handler)
    logger.info("Set up logging with log level %s", log_level)


url = make_url(DATABASE_URL)

vector_store = PGVectorStore.from_params(
    database=url.database,
    host=url.host,
    password=url.password,
    port=url.port,
    user=url.username,
    table_name=VECTOR_STORE_TABLE_NAME,
    embed_dim=CLOUDFLARE_EMBEDDING_MODEL_DIMENSION,
)

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=3,  # Number of connections to keep open in the pool
    max_overflow=5,  # Number of connections that can be opened beyond the pool_size
    pool_recycle=1200,  # Recycle connections after 1 hour
    pool_timeout=60,  # Raise an exception after 2 minutes if no connection is available from the pool
)

SessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=engine)

node_parser = SentenceSplitter.from_defaults(
    chunk_size=NODE_PARSER_CHUNK_SIZE, chunk_overlap=NODE_PARSER_CHUNK_OVERLAP
)

lock = asyncio.Lock()
auth_creds_circular_array = None


class AuthCredsCircularArray:
    def __init__(self):
        self.data = []
        self.size = len(self.data)
        self.index = 0

    async def initialize(self):
        async with SessionLocal() as session:
            result = await session.execute(
                select(SysAuthCred.authCred).filter(
                    SysAuthCred.type == "CLOUDFLARE_EMBED"
                )
            )
            self.data = []
            for record in result.scalars().all():
                try:
                    self.data.append(load_auth_cred_dict(record))
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    logger.warning(
                        "Skipping invalid CLOUDFLARE_EMBED credential (expected JSON object): %s",
                        e,
                    )

        random.shuffle(self.data)
        self.size = len(self.data)
        self.index = 0

    def get_next(self):
        if self.size == 0:
            return None
        item = self.data[self.index]
        self.index = (self.index + 1) % self.size
        return item


def num_tokens_from_string(string: str, model_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.encoding_for_model(model_name=model_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


async def fetch_and_read_document_from_s3(
    org_id: UUID, datafeed_id: UUID, s3_url: str
) -> Tuple[List[Document], int]:
    # Create a new session for aiobotocore
    session = get_session()

    documents = []
    token_count = 0

    async with session.create_client(
        "s3",
        region_name=AWS_REGION,
        **s3_client_credentials(),
    ) as s3_client:
        # Download the file from S3
        s3_url_parts = s3_url.split("/")
        local_file_path = str(s3_url_parts[-1])

        response = await s3_client.get_object(
            Bucket=S3_BUCKET_NAME, Key="/".join(s3_url_parts[3:])
        )

        # response = await s3_client.download_file(bucket_name, key, local_file_path)

        # Save the downloaded file locally
        async with response["Body"] as stream:
            with open(local_file_path, "wb") as f:
                f.write(await stream.read())

        reader = SimpleDirectoryReader(
            input_files=[local_file_path],
            filename_as_id=True,
            file_metadata=lambda x: {
                "dataFeedId": str(datafeed_id),
                "orgID": str(org_id),
                "chatbotIds": [],
            },
        )
        documents = await reader.aload_data(show_progress=True)
        os.remove(local_file_path)

    for document in documents:
        document.excluded_embed_metadata_keys.extend(
            ("dataFeedId", "orgID", "chatbotIds")
        )
        document.excluded_llm_metadata_keys.extend(
            ("dataFeedId", "orgID", "chatbotIds")
        )
        token_count += num_tokens_from_string(document.text, model_name="gpt-3.5-turbo")

    return documents, token_count


async def fetch_and_index_documents(data_feed_message: DataFeedEmbeddingMessageQueue) -> Tuple[bool, int]:
    """
    Downloads an S3 document, parses it, generates semantic embeddings, and stores them in pgvector.

    This function does the following:
    1. Downloads the active document file from S3 to a local temporary location.
    2. Loads the text data using SimpleDirectoryReader.
    3. Calculates token counts to check subscription limits.
    4. Applies transformations: chunk splitting and BGE embedding generation.
    5. Inserts nodes (chunks + embeddings) into the PGVectorStore database table.

    Args:
        data_feed_message (DataFeedEmbeddingMessageQueue): The message queue record holding metadata.

    Returns:
        Tuple[bool, int]: (success_flag, calculated_token_count)
    """
    try:
        # Retrieve document from S3 and get text representation
        llama_index_docs, token_count = await fetch_and_read_document_from_s3(
            data_feed_message.orgId,
            data_feed_message.dataFeedId,
            s3_url=data_feed_message.dataFeedURL,
        )
        global auth_creds_circular_array

        # Dynamically rotate active Cloudflare embedding API credentials using an asyncio lock
        async with lock:
            cloudflare_embed_auth_creds = auth_creds_circular_array.get_next()

        embed_model = CloudflareEmbedding(
            model=CLOUDFLARE_EMBEDDING_MODEL_NAME, **cloudflare_embed_auth_creds
        )
        
        # Build ingestion pipeline with BGE text splitting & embedding generation
        pipeline = IngestionPipeline(
            transformations=[
                node_parser,
                embed_model,
            ],
        )
        
        # Parse and process embeddings concurrently using 3 background workers
        nodes = await pipeline.arun(documents=llama_index_docs, num_workers=3)
        
        # Store processed chunks directly in PostgreSQL pgvector tables
        index = VectorStoreIndex.from_vector_store(
            vector_store=vector_store, embed_model=embed_model
        )
        index.insert_nodes(nodes=nodes)
        
        return True, token_count
    except Exception as e:
        print("Exception:", e)
        return False, -1


async def poll_data_and_process_embedding_messages(task_num: int):
    """
    Background loop that continuously polls and processes pending datafeed ingestion tasks.

    Polled elements are secured via 'with_for_update(skip_locked=True)' to prevent multiple workers
    from processing the same task.

    Args:
        task_num (int): Identifier for this background worker thread.
    """
    while True:
        async with SessionLocal() as session:
            # Safely fetch the oldest pending queue message, skipping rows already locked by other threads
            message = await session.scalar(
                select(DataFeedEmbeddingMessageQueue)
                .filter(DataFeedEmbeddingMessageQueue.messageStatus == "PENDING")
                .order_by(asc(DataFeedEmbeddingMessageQueue.createdAt))
                .limit(1)
                .with_for_update(skip_locked=True)
            )

            if message:
                logger.info(
                    f"Processing Message: {message.messageId} in Task No.: {task_num}"
                )
                # Set message status to PROCESSING to signal active state
                await session.execute(
                    update(DataFeedEmbeddingMessageQueue)
                    .where(DataFeedEmbeddingMessageQueue.messageId == message.messageId)
                    .values(messageStatus="PROCESSING")
                )
                await session.commit()
                await session.refresh(message)

                # Fetch the organization's sum of active token counts
                utilized_tokens = (
                    await session.scalar(
                        select(func.sum(DataFeed.tokenCount)).where(
                            DataFeed.tokenCount != -1,
                            DataFeed.activeStatus == 1,
                            DataFeed.orgId == message.orgId,
                        )
                    )
                    or 0
                )

                error_message = check_datafeed_token_limit(
                    message.orgId, utilized_tokens
                )

                if error_message:
                    message_status = MessageQueueStatusEnum.ERROR
                    active_status = -2
                    token_count = -1
                else:
                    # Process the message outside the transaction to avoid blocking
                    indexing_result, token_count = await fetch_and_index_documents(
                        message
                    )

                    if indexing_result:
                        # set datafeed active to 1
                        message_status = MessageQueueStatusEnum.SUCCESS
                        active_status = 1
                    else:
                        message_status = MessageQueueStatusEnum.ERROR
                        active_status = -1

                # After processing, mark the message status
                await session.execute(
                    update(DataFeed)
                    .where(DataFeed.dataFeedId == message.dataFeedId)
                    .values(activeStatus=active_status, tokenCount=token_count)
                )
                await session.execute(
                    update(DataFeedEmbeddingMessageQueue)
                    .where(DataFeedEmbeddingMessageQueue.messageId == message.messageId)
                    .values(messageStatus=message_status)
                )
                await session.commit()

            # Sleep for some time before polling again
            await asyncio.sleep(5)


async def main():
    global auth_creds_circular_array

    if auth_creds_circular_array is None:
        auth_creds_circular_array = AuthCredsCircularArray()
        await auth_creds_circular_array.initialize()

    # Run multiple instances of poll_data_and_process concurrently
    tasks = [
        asyncio.create_task(poll_data_and_process_embedding_messages(task_num))
        for task_num in range(1, 4)
    ]
    await asyncio.gather(*tasks)


__setup_logging(LOG_LEVEL)
asyncio.run(main())
