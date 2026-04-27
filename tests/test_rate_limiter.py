"""Behavior tests for the token-bucket rate limiter."""

from __future__ import annotations

import asyncio
import time

import pytest

from async_hardened_client.config import RateLimitConfig
from async_hardened_client.rate_limiter import HostRateLimiter, TokenBucket


async def test_burst_drains_immediately_then_refills():
    """A fresh bucket lets `burst` requests through with zero delay; the
    `burst+1`th must wait for refill."""
    bucket = TokenBucket(rate_per_second=10.0, burst=3)

    delays = [await bucket.acquire() for _ in range(3)]
    assert all(d == 0.0 for d in delays)

    start = time.monotonic()
    await bucket.acquire()
    elapsed = time.monotonic() - start
    # 1 token at 10/sec ≈ 0.1s; allow generous CI slack.
    assert 0.05 < elapsed < 0.5


async def test_steady_state_throughput_matches_rate():
    """Over many requests, the realized rate must converge on the configured
    rate. This is the SOW guarantee: "Rate limits never exceeded under load."
    """
    rate = 20.0
    bucket = TokenBucket(rate_per_second=rate, burst=5)

    start = time.monotonic()
    for _ in range(25):
        await bucket.acquire()
    elapsed = time.monotonic() - start

    # 25 reqs minus the 5-token burst = 20 reqs at 20/sec ≈ 1.0s minimum.
    # Allow 30% upper slack for scheduler jitter.
    assert elapsed >= 1.0 * 0.95
    assert elapsed <= 1.0 * 1.4


async def test_concurrent_acquires_are_serialized_correctly():
    """Many coroutines hitting the same bucket must collectively respect
    the rate, not each get their own quota."""
    rate = 50.0
    bucket = TokenBucket(rate_per_second=rate, burst=10)

    start = time.monotonic()
    await asyncio.gather(*(bucket.acquire() for _ in range(60)))
    elapsed = time.monotonic() - start

    # 60 - 10 burst = 50 paced at 50/sec ≈ 1.0s
    assert elapsed >= 0.95
    assert elapsed <= 1.5


async def test_request_larger_than_capacity_rejected():
    bucket = TokenBucket(rate_per_second=10.0, burst=2)
    with pytest.raises(ValueError):
        await bucket.acquire(count=5.0)


async def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_second=0, burst=1)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_second=1, burst=0)


async def test_host_limiter_isolates_buckets_per_host():
    """Bucket for example.com is independent of api.other.com — saturating
    one host does not throttle another."""
    limiter = HostRateLimiter(RateLimitConfig(rate_per_second=5.0, burst=2))

    # Drain example.com bucket
    for _ in range(2):
        d = await limiter.acquire("https://example.com/a")
        assert d == 0.0

    # Other host still has fresh tokens
    delay = await limiter.acquire("https://other.com/a")
    assert delay == 0.0


async def test_host_override_takes_effect():
    """Registering a per-host config replaces the default for that host
    without affecting other hosts."""
    limiter = HostRateLimiter(RateLimitConfig(rate_per_second=100.0, burst=100))
    limiter.set_override("slow.example.com", RateLimitConfig(rate_per_second=2.0, burst=1))

    # First call to slow host consumes the lone token.
    assert await limiter.acquire("https://slow.example.com/x") == 0.0

    start = time.monotonic()
    await limiter.acquire("https://slow.example.com/x")
    elapsed = time.monotonic() - start
    # Second call must wait ≈ 1/2 = 0.5s for refill
    assert elapsed >= 0.3
