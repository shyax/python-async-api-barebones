"""Tests for retry classification and backoff math."""

from __future__ import annotations

import asyncio
import random

import aiohttp
import pytest

from async_hardened_client.config import RetryPolicy
from async_hardened_client.errors import (
    NonRetryableError,
    RetryableError,
    is_retryable_exception,
    is_retryable_status,
)
from async_hardened_client.retry import (
    backoff_seconds,
    parse_retry_after,
    should_retry,
)


@pytest.mark.parametrize("status", [500, 502, 503, 504, 429])
def test_retryable_statuses(status):
    assert is_retryable_status(status) is True


@pytest.mark.parametrize("status", [200, 201, 301, 400, 401, 403, 404, 422])
def test_non_retryable_statuses(status):
    assert is_retryable_status(status) is False


def test_retryable_exception_classification():
    assert is_retryable_exception(asyncio.TimeoutError()) is True
    assert is_retryable_exception(RetryableError("transient")) is True

    # aiohttp client/server transport errors are transient
    assert is_retryable_exception(aiohttp.ClientConnectionError()) is True
    assert is_retryable_exception(aiohttp.ServerDisconnectedError()) is True

    # Permanent error markers are not retried
    assert is_retryable_exception(NonRetryableError("bad")) is False
    assert is_retryable_exception(ValueError("bad")) is False


def test_should_retry_caps_at_max_retries():
    policy = RetryPolicy(max_retries=3)
    assert should_retry(policy, attempt=0) is True
    assert should_retry(policy, attempt=2) is True
    assert should_retry(policy, attempt=3) is False


def test_backoff_grows_exponentially_until_cap():
    policy = RetryPolicy(max_retries=10, base_delay=1.0, max_delay=8.0, jitter=False)
    assert backoff_seconds(policy, 0) == 1.0
    assert backoff_seconds(policy, 1) == 2.0
    assert backoff_seconds(policy, 2) == 4.0
    assert backoff_seconds(policy, 3) == 8.0
    assert backoff_seconds(policy, 4) == 8.0  # capped


def test_backoff_jitter_stays_in_range():
    policy = RetryPolicy(max_retries=5, base_delay=1.0, max_delay=10.0, jitter=True)
    rng = random.Random(42)
    for attempt in range(5):
        target = min(policy.max_delay, policy.base_delay * (2 ** attempt))
        for _ in range(50):
            d = backoff_seconds(policy, attempt, rng=rng)
            assert 0.0 <= d <= target


def test_retry_after_header_overrides_policy():
    policy = RetryPolicy(max_retries=5, base_delay=1.0, max_delay=60.0)
    assert backoff_seconds(policy, 5, retry_after=2.5) == 2.5
    # Capped at policy max_delay so a misbehaving server cannot stall forever
    assert backoff_seconds(policy, 0, retry_after=999) == 60.0


def test_parse_retry_after_seconds_only():
    assert parse_retry_after("5") == 5.0
    assert parse_retry_after("  3.5 ") == 3.5
    assert parse_retry_after("0") == 0.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT") is None
