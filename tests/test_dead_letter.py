"""Tests for the dead-letter queue helpers and CLI behavior."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys

import aiohttp
import pytest

from async_hardened_client import (
    AsyncHardenedClient,
    CircuitBreakerConfig,
    ClientConfig,
    QueueConfig,
    RateLimitConfig,
    RetryPolicy,
)
from async_hardened_client.dead_letter import (
    list_entries,
    purge,
    replay_all,
    replay_entry,
)
from async_hardened_client.errors import DeadLetterError
from async_hardened_client.storage import Storage


def _config(db_path) -> ClientConfig:
    return ClientConfig(
        db_path=str(db_path),
        request_timeout=5.0,
        rate_limit=RateLimitConfig(rate_per_second=200.0, burst=50),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=20, failure_rate=0.99, rolling_window=20, open_seconds=0.1
        ),
        retry=RetryPolicy(max_retries=2, base_delay=0.01, max_delay=0.1, jitter=False),
        queue=QueueConfig(max_size=100, workers=4),
    )


async def test_replay_succeeds_when_upstream_recovers(tmp_path, mock_server):
    """The classic operator workflow: a request DLQ'd while the upstream
    was broken; once it's fixed, replay drains the DLQ."""
    db = tmp_path / "ahc.db"

    # Upstream is broken; the request will exhaust retries and DLQ.
    async with aiohttp.ClientSession() as s:
        await s.post(f"{mock_server.base_url}/admin/profile", json={"p_500": 1.0})

    async with AsyncHardenedClient(_config(db)) as client:
        with pytest.raises(DeadLetterError):
            await client.request("GET", f"{mock_server.base_url}/flaky", params={"r": "1"})

        async with Storage(db) as s:
            assert len(await s.list_dead_letter()) == 1

    # Upstream recovers.
    async with aiohttp.ClientSession() as s:
        await s.post(f"{mock_server.base_url}/admin/profile", json={"p_500": 0.0})

    # Operator runs `replay_all`. The replayed request now succeeds and the
    # DLQ row is removed.
    async with AsyncHardenedClient(_config(db)) as client:
        results = await replay_all(client)
        assert len(results) == 1
        assert results[0].succeeded is True

        async with Storage(db) as s:
            assert await s.list_dead_letter() == []


async def test_replay_leaves_row_when_upstream_still_broken(tmp_path, mock_server):
    """If replay itself fails, the row must remain — we never silently
    drop unrecovered work."""
    db = tmp_path / "ahc.db"

    async with aiohttp.ClientSession() as s:
        await s.post(f"{mock_server.base_url}/admin/profile", json={"p_500": 1.0})

    async with AsyncHardenedClient(_config(db)) as client:
        with pytest.raises(DeadLetterError):
            await client.request("GET", f"{mock_server.base_url}/flaky", params={"x": "1"})

    async with AsyncHardenedClient(_config(db)) as client:
        results = await replay_all(client)
        assert results[0].succeeded is False

    async with Storage(db) as s:
        assert len(await s.list_dead_letter()) == 1


async def test_purge_removes_all_rows(tmp_path):
    db = tmp_path / "ahc.db"
    from async_hardened_client.models import Request as Req

    async with Storage(db) as s:
        for i in range(5):
            await s.push_dead_letter(Req(method="GET", url=f"https://e/{i}"), error="boom", retry_count=3)
        assert len(await s.list_dead_letter()) == 5
        n = await purge(s)
        assert n == 5
        assert await s.list_dead_letter() == []


async def test_cli_list_and_purge(tmp_path):
    """Round-trip through the actual CLI. We seed a DLQ row, then invoke
    the CLI subcommands and parse stdout."""
    db = tmp_path / "ahc.db"
    from async_hardened_client.models import Request as Req

    async with Storage(db) as s:
        await s.push_dead_letter(
            Req(method="POST", url="https://example.com/api/x", body={"y": 1}),
            error="HTTP 502",
            retry_count=4,
        )

    def run_cli(*args: str) -> tuple[int, str, str]:
        proc = subprocess.run(
            [sys.executable, "-m", "async_hardened_client.cli", *args],
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout, proc.stderr

    code, out, err = run_cli("dlq", "list", "--db", str(db))
    assert code == 0, err
    rows = json.loads(out)
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/api/x"
    assert rows[0]["error"] == "HTTP 502"

    # Purge requires --yes
    code, _, err = run_cli("dlq", "purge", "--db", str(db))
    assert code == 1
    assert "refusing" in err

    code, out, _ = run_cli("dlq", "purge", "--db", str(db), "--yes")
    assert code == 0
    assert json.loads(out) == {"removed": 1}
