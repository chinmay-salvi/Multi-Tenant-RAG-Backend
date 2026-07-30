from llama_index.vector_stores.postgres import PGVectorStore
from sqlalchemy.engine import make_url
from app.core.config import (
    DATABASE_URL,
    VECTOR_STORE_TABLE_NAME,
    CLOUDFLARE_EMBEDDING_MODEL_DIMENSION,
)
from app.db.session import SessionLocal as AppSessionLocal, engine as app_engine
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

singleton_instance = None
did_run_setup = False


class CustomPGVectorStore(PGVectorStore):
    """
    Custom PGVectorStore that uses the same connection pool as the FastAPI app.
    """

    def _connect(self) -> None:
        self._engine = create_engine(self.connection_string)
        self._session = sessionmaker(self._engine)

        # Use our existing app engine and session so we can use the same connection pool
        self._async_engine = app_engine
        self._async_session = AppSessionLocal

    async def close(self) -> None:
        self._session.close_all()
        self._engine.dispose()

        await self._async_engine.dispose()

    def _create_tables_if_not_exists(self) -> None:
        pass

    def _create_extension(self) -> None:
        pass

    async def run_setup(self) -> None:
        global did_run_setup
        if did_run_setup:
            return
        self._initialize()
        async with self._async_session() as session:
            async with session.begin():
                statement = sqlalchemy.text("CREATE EXTENSION IF NOT EXISTS vector")
                await session.execute(statement)
                await session.commit()

        async with self._async_session() as session:
            async with session.begin():
                conn = await session.connection()
                await conn.run_sync(self._base.metadata.create_all)
                
                # Add a B-Tree index on orgId within the metadata JSONB column to prevent sequential scans
                # LlamaIndex prefixes the table name with 'data_'
                index_stmt = sqlalchemy.text(
                    f"CREATE INDEX IF NOT EXISTS idx_vector_store_orgid "
                    f"ON data_{VECTOR_STORE_TABLE_NAME} ((metadata_->>'orgId'))"
                )
                await conn.execute(index_stmt)
        did_run_setup = True


async def get_vector_store_singleton() -> CustomPGVectorStore:
    global singleton_instance

    if singleton_instance is not None:
        return singleton_instance

    url = make_url(DATABASE_URL)
    singleton_instance = CustomPGVectorStore.from_params(
        url.host,
        url.port or 5432,
        url.database,
        url.username,
        url.password,
        VECTOR_STORE_TABLE_NAME,
        embed_dim=CLOUDFLARE_EMBEDDING_MODEL_DIMENSION,
    )
    return singleton_instance
