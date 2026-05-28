"""Tests for datafeed upload and listing endpoints under ``/api/v1``."""

import uuid

import pytest
from httpx import AsyncClient

from tests.conftest import API_PREFIX, TEST_USER_ID
from tests.datafeed_builders import create_tag


@pytest.mark.asyncio
async def test_text_data_upload(client: AsyncClient) -> None:
    """Verifies ``POST /text_data_upload`` stores text and links it to a tag.

    S3 upload is mocked in ``conftest``; this test only checks API and DB wiring.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.

    Asserts:
        HTTP 200, ``success`` is true, and both ``dataFeedId`` and ``tagId`` are set.
    """
    tag_name = f"feed-tag-{uuid.uuid4().hex[:8]}"
    await create_tag(client, tag_name)

    response = await client.post(
        f"{API_PREFIX}/text_data_upload",
        json={
            "userId": str(TEST_USER_ID),
            "text": "Hello from pytest — knowledge base snippet.",
            "selectedTag": tag_name,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["dataFeedId"] is not None
    assert body["tagId"] is not None


@pytest.mark.asyncio
async def test_get_data_feed_lists_upload(client: AsyncClient) -> None:
    """Verifies ``GET /get_data_feed`` includes a feed created via text upload.

    Plan limits are mocked so listing does not call Supabase.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.

    Asserts:
        HTTP 200, ``success`` is true, and the uploaded ``dataFeedId`` appears.
    """
    tag_name = f"list-feed-tag-{uuid.uuid4().hex[:8]}"
    await create_tag(client, tag_name)

    upload = await client.post(
        f"{API_PREFIX}/text_data_upload",
        json={
            "userId": str(TEST_USER_ID),
            "text": "Content for get_data_feed test.",
            "selectedTag": tag_name,
        },
    )
    assert upload.json()["success"] is True
    data_feed_id = upload.json()["dataFeedId"]

    response = await client.get(
        f"{API_PREFIX}/get_data_feed?userId={TEST_USER_ID}"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    feed_ids = {str(f["dataFeedId"]) for f in body["datafeeds"]}
    assert str(data_feed_id) in feed_ids


@pytest.mark.asyncio
async def test_text_data_upload_unknown_tag_fails(client: AsyncClient) -> None:
    """Verifies ``POST /text_data_upload`` returns 404 when the tag does not exist.

    Args:
        client: Async HTTP client with ``validate_user`` overridden.

    Asserts:
        HTTP 404 because ``fetch_existing_tag(..., ensure=True)`` raises.
    """
    response = await client.post(
        f"{API_PREFIX}/text_data_upload",
        json={
            "userId": str(TEST_USER_ID),
            "text": "orphan text",
            "selectedTag": f"no-such-tag-{uuid.uuid4().hex}",
        },
    )
    assert response.status_code == 404
