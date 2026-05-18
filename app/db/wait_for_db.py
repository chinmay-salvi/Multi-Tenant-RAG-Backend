import asyncio
from app.db.pg_vector import get_vector_store_singleton
from app.db.session import SessionLocal, engine
from asyncpg.exceptions import InvalidCatalogNameError
from sqlalchemy import text, create_engine, make_url
from app.core.config import DATABASE_URL
from app.db.tables import Base


def create_database_if_not_exists():
    url = make_url(DATABASE_URL)
    database_name = str(url.database)
    url = url.set(drivername="postgresql", database="postgres")
    # Create an engine without the database name
    admin_engine = create_engine(url)

    # Obtain a connection from the engine
    with admin_engine.connect() as conn:
        # Execute the SQL command to create the database outside a transaction block
        try:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(f"CREATE DATABASE {database_name}"))
            conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'))
            print(f"Database 'backend_agents' created successfully.")
        except Exception as e:
            # If the database already exists, you may see an error
            # You can optionally ignore the error if it indicates that the database already exists
            print(f"An error occurred while creating the database: {e}")


async def create_tables_if_not_exists():
    """
    Create database tables based on the models defined in the Base class.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    vector_store = await get_vector_store_singleton()
    await vector_store.run_setup()


async def check_database_connection(
    max_attempts: int = 30, sleep_interval: int = 1
) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            async with SessionLocal() as db:
                await db.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'))
                await db.commit()
                await db.execute(text("SELECT 1"))
                print(f"Connected to the database on attempt {attempt}.")
                return
        except InvalidCatalogNameError:
            try:
                print(f"Database not found trying to create new one.")
                create_database_if_not_exists()
            except Exception as e:
                print(f"Attempted to create database. Error: {e}")

        except Exception as e:
            print(f"Attempt {attempt}: Database is not yet available. Error: {e}")
            if attempt == max_attempts:
                raise ValueError(
                    f"Couldn't connect to database after {max_attempts} attempts."
                ) from e
        await asyncio.sleep(sleep_interval)
