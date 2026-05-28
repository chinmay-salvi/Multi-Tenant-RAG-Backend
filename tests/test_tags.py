"""Tests for tag management endpoints under ``/api/v1``."""

import uuid

import pytest
from httpx import AsyncClient

from tests.conftest import API_PREFIX, TEST_USER_ID


@pytest.mark.asyncio
async def test_add_tag_success(client: AsyncClient, auth_body: dict) -> None:
    """Verifies ``POST /add_tag`` persists a new tag for the test organization.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.
        auth_body: Request body fragment containing ``userId``.

    Asserts:
        HTTP 200 and the plain-text success message from the API.
    """
    tag_name = f"test-tag-{uuid.uuid4().hex[:8]}"
    response = await client.post(
        f"{API_PREFIX}/add_tag",
        json={**auth_body, "tagName": tag_name},
    )
    assert response.status_code == 200
    assert response.json() == "Tag added successfully"


@pytest.mark.asyncio
async def test_add_tag_duplicate(client: AsyncClient, auth_body: dict) -> None:
    """Verifies ``POST /add_tag`` is idempotent for duplicate tag names.

    The second request with the same ``tagName`` must not create another row.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.
        auth_body: Request body fragment containing ``userId``.

    Asserts:
        Both responses return HTTP 200; the second body indicates duplication.
    """
    tag_name = f"dup-tag-{uuid.uuid4().hex[:8]}"
    payload = {**auth_body, "tagName": tag_name}

    first = await client.post(f"{API_PREFIX}/add_tag", json=payload)
    assert first.status_code == 200

    second = await client.post(f"{API_PREFIX}/add_tag", json=payload)
    assert second.status_code == 200
    assert second.json() == "Tag already exists"


@pytest.mark.asyncio
async def test_get_tag_data_lists_tags(client: AsyncClient, auth_query: str) -> None:
    """Verifies ``GET /get_tag_data`` returns tags created for the organization.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.
        auth_query: Query string containing ``userId`` for the protected route.

    Asserts:
        HTTP 200, matching ``userId``, and the newly added tag name in ``tags``.
    """
    tag_name = f"listed-tag-{uuid.uuid4().hex[:8]}"
    await client.post(
        f"{API_PREFIX}/add_tag",
        json={"userId": str(TEST_USER_ID), "tagName": tag_name},
    )

    response = await client.get(f"{API_PREFIX}/get_tag_data?{auth_query}")
    assert response.status_code == 200
    body = response.json()
    assert body["userId"] == str(TEST_USER_ID)
    tag_names = {t["tagName"] for t in body["tags"]}
    assert tag_name in tag_names
