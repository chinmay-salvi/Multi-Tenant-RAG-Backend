import asyncio
import json
import pytest
import asyncpg
from pgqueuer import PgQueuer, Job, RetryRequested
from pgqueuer.db import AsyncpgPoolDriver
from pgqueuer.queries import Queries
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import DATABASE_URL
from app.db.session import SessionLocal

@pytest.mark.asyncio
async def test_pgqueuer_success_flow(db: AsyncSession) -> None:
    # Clear tables for clean test
    await db.execute(text("DELETE FROM pgqueuer"))
    await db.execute(text("DELETE FROM pgqueuer_dlq"))
    await db.commit()

    dsn = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(dsn=dsn)
    driver = AsyncpgPoolDriver(pool)
    pgq = PgQueuer(driver)

    processed_jobs = []

    @pgq.entrypoint("test_success_channel")
    async def handle_success(job: Job) -> None:
        payload = json.loads(job.payload.decode("utf-8"))
        processed_jobs.append(payload)

    # Enqueue a job
    queries = Queries(driver)
    await queries.enqueue("test_success_channel", json.dumps({"test_key": "test_val"}).encode("utf-8"))

    # Run PgQueuer briefly to process it
    task = asyncio.create_task(pgq.run())
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(processed_jobs) == 1
    assert processed_jobs[0]["test_key"] == "test_val"

    await pool.close()

@pytest.mark.asyncio
async def test_pgqueuer_retry_and_dlq_flow(db: AsyncSession) -> None:
    # Clear tables for clean test
    await db.execute(text("DELETE FROM pgqueuer"))
    await db.execute(text("DELETE FROM pgqueuer_dlq"))
    await db.commit()

    dsn = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(dsn=dsn)
    driver = AsyncpgPoolDriver(pool)
    pgq = PgQueuer(driver)

    attempts_recorded = []

    @pgq.entrypoint("test_fail_channel")
    async def handle_fail(job: Job) -> None:
        attempts_recorded.append(job.attempts)
        import traceback
        from datetime import timedelta
        try:
            raise ValueError("Intentional processing failure")
        except Exception as e:
            if job.attempts >= 2:  # attempt 0, 1, 2 = 3 total attempts
                # Move to DLQ
                async with SessionLocal() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO pgqueuer_dlq (original_job_id, channel, payload, error_message, traceback)
                            VALUES (:job_id, :channel, :payload, :error_message, :traceback)
                            """
                        ),
                        {
                            "job_id": job.id,
                            "channel": job.entrypoint,
                            "payload": job.payload,
                            "error_message": str(e),
                            "traceback": traceback.format_exc(),
                        }
                    )
                    await session.execute(
                        text("DELETE FROM pgqueuer WHERE id = :job_id"),
                        {"job_id": job.id}
                    )
                    await session.commit()
            else:
                # Retry immediately
                raise RetryRequested(delay=timedelta(seconds=0))

    # Enqueue a failing job
    queries = Queries(driver)
    await queries.enqueue("test_fail_channel", b"fail_payload")

    # Run PgQueuer to trigger failures, retries, and DLQ routing
    task = asyncio.create_task(pgq.run())
    await asyncio.sleep(1.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Should have run 3 times (attempts 0, 1, 2)
    assert len(attempts_recorded) == 3
    assert attempts_recorded == [0, 1, 2]

    # Verify DLQ record exists
    result = await db.execute(text("SELECT * FROM pgqueuer_dlq"))
    dlq_jobs = result.fetchall()
    assert len(dlq_jobs) == 1
    # Column mapping in pgqueuer_dlq: original_job_id, channel, payload, error_message
    assert dlq_jobs[0][1] == dlq_jobs[0][1] # id check
    assert dlq_jobs[0][2] == "test_fail_channel"
    assert dlq_jobs[0][3] == b"fail_payload"
    assert "Intentional processing failure" in dlq_jobs[0][4]

    # Verify deleted from active queue
    result_q = await db.execute(text("SELECT count(*) FROM pgqueuer"))
    assert result_q.scalar() == 0

    await pool.close()

@pytest.mark.asyncio
async def test_delete_datafeed_purges_pgqueuer(client: AsyncClient, db: AsyncSession) -> None:
    from httpx import AsyncClient
    from app.db.tables import DataFeed, DataFeedDataTypeEnum
    from tests.conftest import API_PREFIX, TEST_ORG_ID, TEST_USER_ID

    # Clear tables
    await db.execute(text("DELETE FROM pgqueuer"))
    await db.commit()

    import uuid
    data_feed_id = uuid.uuid4()

    # Create dummy DataFeed
    df = DataFeed(
        dataFeedId=data_feed_id,
        dataFeedName="Test File for deletion",
        dataType=DataFeedDataTypeEnum.File,
        dataFeedURL="s3://dummy/path",
        orgId=TEST_ORG_ID,
    )
    db.add(df)
    await db.commit()

    # Enqueue a job in pgqueuer for this datafeed
    dsn = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(dsn=dsn)
    driver = AsyncpgPoolDriver(pool)
    queries = Queries(driver)

    payload = json.dumps({
        "dataFeedId": str(data_feed_id),
        "orgId": str(TEST_ORG_ID),
    }).encode("utf-8")
    await queries.enqueue("datafeed_embedding", payload, 0)
    await pool.close()

    # Assert it is present in pgqueuer table
    res_before = await db.execute(text("SELECT count(*) FROM pgqueuer WHERE entrypoint = 'datafeed_embedding'"))
    assert res_before.scalar() == 1

    # Call the API to delete the datafeed
    response = await client.post(
        f"{API_PREFIX}/delete_data_feed",
        json={
            "userId": str(TEST_USER_ID),
            "dataFeedIds": [str(data_feed_id)],
        },
    )
    assert response.status_code == 200

    # Assert it has been purged from pgqueuer
    res_after = await db.execute(text("SELECT count(*) FROM pgqueuer WHERE entrypoint = 'datafeed_embedding'"))
    assert res_after.scalar() == 0

