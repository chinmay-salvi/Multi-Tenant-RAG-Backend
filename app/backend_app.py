import sys
import uvicorn
import logging
from llama_index.core.node_parser.text.utils import split_by_sentence_tokenizer
from app.api import api_router
from app.db.wait_for_db import check_database_connection, create_tables_if_not_exists
from app.core.config import PROJECT_NAME, LOG_LEVEL, API_PREFIX, WORKERS
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # first wait for DB to be connectable
    await check_database_connection()
    await create_tables_if_not_exists()
    try:
        # Some setup is required to initialize the llama-index sentence splitter
        split_by_sentence_tokenizer()
    except FileExistsError:
        # Sometimes seen in deployments, should be benign.
        logger.info("Tried to re-download NLTK files but already exists.")
    yield


app = FastAPI(
    title=PROJECT_NAME,
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router, prefix=API_PREFIX)



__setup_logging(LOG_LEVEL)


if __name__ == "__main__":
    uvicorn.run("backend_app:app", host="0.0.0.0", port=8000, workers=WORKERS)
