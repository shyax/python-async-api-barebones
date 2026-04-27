"""Load tests: many concurrent requests, verify the rate limiter holds."""

from __future__ import annotations

import asyncio
import time

import pytest

from async_hardened_client import (
    AsyncHardenedClient,
    CircuitBreakerConfig,
    ClientConfig,
    QueueConfig,
    RateLimitConfig,
    RetryPolicy,
)


@pytest.mark.parametrize("concurrency", [100, 200])
async def test_rate_limit_holds_under_high_concurrency(tmp_path, mock_server, concurrency):
    """Fire `concurrency` concurrent requests at a client whose rate limiter
    is set to 50rps with a small burst. The realized rate must not exceed
    the configured rate by more than a small slack factor.
    """
    cfg = ClientConfig(
        db_path=str(tmp_path / "load.db"),
        request_timeout=10.0,
        rate_limit=RateLimitConfig(rate_per_second=50.0, burst=5),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=999, failure_rate=1.0, rolling_window=concurrency,
            open_seconds=0.05,
        ),
        retry=RetryPolicy(max_retries=8, base_delay=0.01, max_delay=0.5, jitter=True),
        queue=QueueConfig(max_size=concurrency, workers=32),
    )
    async with AsyncHardenedClient(cfg) as client:
        start = time.monotonic()
        urls = [f"{mock_server.base_url}/flaky?n={i}" for i in range(concurrency)]
        results = await asyncio.gather(*(client.request("GET", u) for u in urls))
        elapsed = time.monotonic() - start

        assert len(results) == concurrency
        assert all(r.status_code == 200 for r in results)

        # 50 rps with burst 5: minimum elapsed ≈ (concurrency - 5) / 50.
        min_expected = (concurrency - 5) / 50.0
        # Generous upper bound — uvicorn loopback adds latency.
        max_expected = min_expected * 3.0 + 1.0
        assert elapsed >= min_expected * 0.85, (
            f"finished too fast: {elapsed:.2f}s < {min_expected:.2f}s — "
            "rate limit was bypassed"
        )
        assert elapsed <= max_expected, (
            f"too slow: {elapsed:.2f}s > {max_expected:.2f}s"
        )

        snap = client.metrics()
        assert snap["requests_succeeded"] == concurrency
        assert snap["requests_failed_terminal"] == 0


async def test_zero_drops_under_failure_storm(tmp_path, mock_server):
    """Mixed-mode: 50% 5xx + 30% 429 random failures + 100 concurrent
    requests. The contract is "zero dropped requests under simulated
    failures" — every caller must observe a 200 eventually."""
    import aiohttp

    async with aiohttp.ClientSession() as s:
        await s.post(
            f"{mock_server.base_url}/admin/profile",
            json={"p_500": 0.5, "p_429": 0.3, "seed": 11},
        )

    cfg = ClientConfig(
        db_path=str(tmp_path / "storm.db"),
        request_timeout=10.0,
        rate_limit=RateLimitConfig(rate_per_second=200.0, burst=20),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=50, failure_rate=0.95, rolling_window=40,
            open_seconds=0.1,
        ),
        retry=RetryPolicy(max_retries=15, base_delay=0.01, max_delay=0.2, jitter=True),
        queue=QueueConfig(max_size=200, workers=32),
    )
    async with AsyncHardenedClient(cfg) as client:
        urls = [f"{mock_server.base_url}/flaky?id={i}" for i in range(100)]
        results = await asyncio.gather(*(client.request("GET", u) for u in urls))

        assert all(r.status_code == 200 for r in results)
        snap = client.metrics()
        assert snap["requests_succeeded"] == 100
        assert snap["requests_failed_terminal"] == 0
        # Sanity: there must have been actual retries to recover from the
        # storm; otherwise the test isn't proving anything.
        assert snap["retries_performed"] > 50
