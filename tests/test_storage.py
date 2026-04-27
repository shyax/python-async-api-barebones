"""Round-trip tests for the SQLite storage layer."""

from __future__ import annotations

import pytest

from async_hardened_client.models import Request
from async_hardened_client.storage import (
    STATUS_IN_PROGRESS,
    STATUS_PENDING,
    Storage,
)


@pytest.fixture
async def storage(tmp_path):
    db = tmp_path / "ahc.db"
    async with Storage(db) as s:
        yield s


async def test_inflight_upsert_and_list(storage):
    req = Request(method="GET", url="https://example.com/a")
    await storage.upsert_inflight(req)

    rows = await storage.list_inflight()
    assert len(rows) == 1
    assert rows[0].idempotency_key == req.idempotency_key
    assert rows[0].status == STATUS_PENDING
    assert rows[0].retry_count == 0


async def test_inflight_upsert_is_idempotent(storage):
    req = Request(method="GET", url="https://example.com/a")
    await storage.upsert_inflight(req)
    await storage.upsert_inflight(req, status=STATUS_IN_PROGRESS)

    rows = await storage.list_inflight()
    assert len(rows) == 1
    assert rows[0].status == STATUS_IN_PROGRESS


async def test_increment_retry_returns_new_count(storage):
    req = Request(method="POST", url="https://example.com/b", body={"x": 1})
    await storage.upsert_inflight(req)

    count = await storage.increment_retry(req.idempotency_key, last_error="HTTP 503")
    assert count == 1
    count = await storage.increment_retry(req.idempotency_key, last_error="timeout")
    assert count == 2

    rows = await storage.list_inflight()
    assert rows[0].last_error == "timeout"
    assert rows[0].status == STATUS_PENDING


async def test_delete_inflight_removes_row(storage):
    req = Request(method="GET", url="https://example.com/c")
    await storage.upsert_inflight(req)
    await storage.delete_inflight(req.idempotency_key)
    assert await storage.list_inflight() == []


async def test_dead_letter_round_trip(storage):
    req = Request(method="GET", url="https://example.com/d")
    dlq_id = await storage.push_dead_letter(req, error="HTTP 400 bad request", retry_count=0)
    assert dlq_id > 0

    rows = await storage.list_dead_letter()
    assert len(rows) == 1
    assert rows[0].id == dlq_id
    assert rows[0].error == "HTTP 400 bad request"
    assert rows[0].request.url == req.url


async def test_dead_letter_purge(storage):
    for i in range(3):
        await storage.push_dead_letter(
            Request(method="GET", url=f"https://example.com/{i}"),
            error="boom",
            retry_count=5,
        )
    purged = await storage.purge_dead_letter()
    assert purged == 3
    assert await storage.list_dead_letter() == []


async def test_inflight_recovery_payload_roundtrip(storage):
    """Persisted requests must reconstruct exactly — this is what crash
    recovery depends on."""
    req = Request(
        method="POST",
        url="https://api.example.com/v1/widgets",
        headers={"X-Api-Key": "redacted"},
        params={"q": "spline"},
        body={"name": "alpha", "qty": 3},
        priority=5,
    )
    await storage.upsert_inflight(req)
    [row] = await storage.list_inflight()

    restored = row.request
    assert restored.method == req.method
    assert restored.url == req.url
    assert restored.headers == req.headers
    assert restored.params == req.params
    assert restored.body == req.body
    assert restored.priority == req.priority
    assert restored.idempotency_key == req.idempotency_key
