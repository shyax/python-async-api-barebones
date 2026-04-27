"""Crash-recovery integration tests.

The contract: a process that crashes (or is `kill -9`'d) before draining
the queue must, on the next start with the same SQLite path, replay any
requests that were not yet successfully completed. No silent loss.

We simulate a crash by populating the inflight table directly and then
booting a fresh client against the same DB path. The client's `start()`
calls `_recover_inflight()` which re-enqueues every row before the
worker pool is even spawned.
"""

from __future__ import annotations

import asyncio

import pytest

from async_hardened_client import (
    AsyncHardenedClient,
    CircuitBreakerConfig,
    ClientConfig,
    QueueConfig,
    RateLimitConfig,
    RetryPolicy,
)
from async_hardened_client.models import Request
from async_hardened_client.storage import Storage


def _config(db_path) -> ClientConfig:
    return ClientConfig(
        db_path=str(db_path),
        request_timeout=5.0,
        rate_limit=RateLimitConfig(rate_per_second=200.0, burst=50),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=10, failure_rate=0.9, rolling_window=20, open_seconds=0.2
        ),
        retry=RetryPolicy(max_retries=4, base_delay=0.02, max_delay=0.5, jitter=False),
        queue=QueueConfig(max_size=200, workers=4),
    )


async def test_recovery_re_enqueues_orphaned_requests(tmp_path, mock_server):
    """Seed inflight rows directly, then start a client against the same DB
    — recovery should replay them through to success."""
    db = tmp_path / "ahc.db"

    orphans = [
        Request(method="GET", url=f"{mock_server.base_url}/flaky", params={"recover": str(i)})
        for i in range(5)
    ]
    async with Storage(db) as s:
        for req in orphans:
            await s.upsert_inflight(req, retry_count=1, last_error="HTTP 503")
        rows_before = await s.list_inflight()
        assert len(rows_before) == 5

    seen: list[Request] = []

    async def hook(req, resp):
        seen.append(req)

    async with AsyncHardenedClient(_config(db)) as client:
        client.set_response_hook(hook)
        # Workers pick up recovered rows immediately at start(). drain()
        # also waits for any rescheduled retries to settle.
        await asyncio.wait_for(client.drain(), timeout=10.0)

    seen_keys = {r.idempotency_key for r in seen}
    expected_keys = {r.idempotency_key for r in orphans}
    assert seen_keys == expected_keys

    # Inflight table is now empty — recovery completed every orphan.
    async with Storage(db) as s:
        assert await s.list_inflight() == []


async def test_recovery_preserves_retry_count(tmp_path, mock_server):
    """A row recovered with retry_count=N should resume at attempt N — its
    remaining retry budget shrinks accordingly."""
    db = tmp_path / "ahc.db"

    # 100% failure rate so the recovered request will use up its budget.
    import aiohttp
    async with aiohttp.ClientSession() as s:
        await s.post(f"{mock_server.base_url}/admin/profile", json={"p_500": 1.0})

    req = Request(method="GET", url=f"{mock_server.base_url}/flaky", params={"r": "1"})
    async with Storage(db) as s:
        # Already used 3 of 4 retries when "crashed". Only 1 attempt left.
        await s.upsert_inflight(req, retry_count=3, last_error="HTTP 503")

    async with AsyncHardenedClient(_config(db)) as client:
        await asyncio.wait_for(client.drain(), timeout=10.0)

        snap = client.metrics()
        # Exactly 1 retry should have happened (attempt=3 → 4, then DLQ).
        assert snap["retries_performed"] == 1
        assert snap["dlq_pushes"] == 1

    async with Storage(db) as s:
        assert await s.list_inflight() == []
        dlq = await s.list_dead_letter()
        assert len(dlq) == 1
        # When recovery was queued, it began at attempt=3; it then failed
        # once more, raising it to attempt=4, the boundary at which the
        # request is escalated.
        assert dlq[0].retry_count == 4


async def test_recovery_is_idempotent_across_two_starts(tmp_path, mock_server):
    """Starting and stopping a client with no work should leave inflight
    untouched — recovery must not corrupt state."""
    db = tmp_path / "ahc.db"
    req = Request(method="GET", url=f"{mock_server.base_url}/flaky")

    async with Storage(db) as s:
        await s.upsert_inflight(req)

    # First boot drains the row to completion.
    async with AsyncHardenedClient(_config(db)) as client:
        await asyncio.wait_for(client._queue.join(), timeout=5.0)

    async with Storage(db) as s:
        assert await s.list_inflight() == []

    # Second boot has nothing to do; metrics show zero work performed.
    async with AsyncHardenedClient(_config(db)) as client:
        # No queue to join; but a quick sleep proves we didn't hang.
        await asyncio.sleep(0.05)
        snap = client.metrics()
        assert snap["requests_started"] == 0
