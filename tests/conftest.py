"""
Pytest configuration and shared fixtures for API tests.

Design notes
------------
- **Real Postgres**: ``DATABASE_URL`` in ``app/core/config.py`` must point at a
  running instance (e.g. ``docker compose up -d db``). Tables are created once
  per test session.
- **No Supabase in tests**: ``validate_user`` is overridden to return a fixed
  tenant (``TEST_USER_ID`` / ``TEST_ORG_ID``). Plan lookups and S3 are monkeypatched
  so routes do not call external services.
- **Async engine + pytest-asyncio**: SQLAlchemy's async pool is tied to the event
  loop. We ``dispose()`` the engine after session bootstrap and after each test
  to avoid "Future attached to a different loop" from asyncpg.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import validate_user
from app.backend_app import app
from app.db.session import SessionLocal, engine
from app.db.tables import AgentType, Organization, OrganizationUser, User

# ---------------------------------------------------------------------------
# Stable tenant IDs (must match rows seeded in ``_bootstrap_database``).
# All authenticated routes see this org/user when ``validate_user`` is overridden.
# ---------------------------------------------------------------------------
TEST_USER_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
TEST_ORG_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
API_PREFIX = "/api/v1"


async def _override_validate_user() -> dict:
    """FastAPI dependency override: pretend JWT validation succeeded.

    The real ``validate_user`` calls Supabase; tests never configure it.
    """
    return {
        "userId": str(TEST_USER_ID),
        "orgId": str(TEST_ORG_ID),
        "userRole": "admin",
        "userCreateAt": datetime(2024, 1, 1, 0, 0, 0),
    }


def _noop_plan_check(*_args, **_kwargs) -> None:
    """Replacement for token/chatbot plan guards (no quota errors in tests)."""
    return None


@pytest.fixture(scope="session", autouse=True)
def _bootstrap_database() -> None:
    """Once per ``pytest`` process: wait for DB, create tables, seed minimal rows.

    Runs in a dedicated asyncio loop via ``asyncio.run``, then disposes the
    global engine so later tests attach to their own loop cleanly.
    """
    from app.db.wait_for_db import (
        check_database_connection,
        create_tables_if_not_exists,
    )

    async def setup() -> None:
        await check_database_connection()
        await create_tables_if_not_exists()

        async with SessionLocal() as db:
            if await db.get(Organization, TEST_ORG_ID) is None:
                db.add(Organization(orgId=TEST_ORG_ID))
            if await db.get(User, TEST_USER_ID) is None:
                db.add(User(userId=TEST_USER_ID))
            membership = await db.execute(
                select(OrganizationUser).where(
                    OrganizationUser.userId == TEST_USER_ID,
                    OrganizationUser.orgId == TEST_ORG_ID,
                )
            )
            if membership.scalar_one_or_none() is None:
                db.add(
                    OrganizationUser(
                        userId=TEST_USER_ID,
                        orgId=TEST_ORG_ID,
                        userRole="admin",
                    )
                )
            agent = await db.execute(
                select(AgentType).where(AgentType.agentTypeName == "Support")
            )
            if agent.scalar_one_or_none() is None:
                db.add(AgentType(agentTypeName="Support"))
            await db.commit()

        await engine.dispose()

    asyncio.run(setup())


@pytest.fixture(autouse=True)
async def _reset_connection_pool() -> AsyncGenerator[None, None]:
    """Per test: yield, then drop pooled connections (asyncpg loop safety)."""
    yield
    await engine.dispose()


@pytest.fixture(autouse=True)
def _mock_external_services(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch endpoints that would otherwise require Supabase, AWS, or vector DB.

    Targets are **import sites** inside route modules (where the name is bound),
    not only the defining module in ``plans_helper``.
    """
    monkeypatch.setattr(
        "app.datafeed.router.check_datafeed_token_limit",
        _noop_plan_check,
    )
    monkeypatch.setattr(
        "app.datafeed.services.check_datafeed_token_limit",
        _noop_plan_check,
    )
    monkeypatch.setattr(
        "app.chatbot.router.check_chatbot_limit",
        _noop_plan_check,
    )
    monkeypatch.setattr(
        "app.chatbot.services.check_chatbot_limit",
        _noop_plan_check,
    )
    monkeypatch.setattr(
        "app.payments.plans_helper.fetch_plan_data",
        lambda org_id: {
            "razorpay_subscriptions": [],
            "user_trial": [
                {
                    "trial_start": "2020-01-01T00:00:00",
                    "trial_end": "2099-12-31T23:59:59",
                    "notes": {
                        "max_datafeed_tokens": "999999",
                        "max_message": "999999",
                        "max_chatbot": "999",
                    },
                }
            ],
            "plans": [],
        },
    )

    async def _fake_get_limits(org_id: str) -> tuple[int, int]:
        return 10_000, 1_000_000

    monkeypatch.setattr("app.datafeed.router.get_limits", _fake_get_limits)

    async def _fake_s3_upload(uploaded_file_path: str, s3_key: str) -> str:
        return f"https://test-bucket.s3.amazonaws.com/{s3_key}"

    monkeypatch.setattr(
        "app.datafeed.router.upload_file_to_s3",
        _fake_s3_upload,
    )
    monkeypatch.setattr(
        "app.datafeed.services.upload_file_to_s3",
        _fake_s3_upload,
    )
    monkeypatch.setattr(
        "app.chatbot.router.upload_file_to_s3",
        _fake_s3_upload,
    )
    monkeypatch.setattr(
        "app.chatbot.services.upload_file_to_s3",
        _fake_s3_upload,
    )
    monkeypatch.setattr(
        "app.chatbot.router.modify_chatbot_id_in_nodes",
        lambda org_id, chatbot_id, remove_datafeeds, additional_datafeeds: True,
    )
    monkeypatch.setattr(
        "app.chatbot.services.modify_chatbot_id_in_nodes",
        lambda org_id, chatbot_id, remove_datafeeds, additional_datafeeds: True,
    )


@pytest.fixture
async def db() -> AsyncGenerator[AsyncSession, None]:
    """Short-lived async SQLAlchemy session for direct DB setup/assertions."""
    async with SessionLocal() as session:
        yield session


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """HTTPX async client wired to the FastAPI app in-process (no TCP).

    Installs the auth override for the duration of the test.
    """
    app.dependency_overrides[validate_user] = _override_validate_user
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def auth_query() -> str:
    """Query string fragment matching what ``validate_user`` expects on GET."""
    return f"userId={TEST_USER_ID}"


@pytest.fixture
def auth_body() -> dict:
    """JSON fragment with ``userId`` for POST bodies checked by ``validate_user``."""
    return {"userId": str(TEST_USER_ID)}
