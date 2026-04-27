"""Error types and HTTP/exception classification.

Classification is the contract that drives the retry engine: anything that
returns `True` from `is_retryable` will be retried up to the policy cap;
anything else short-circuits straight to the dead-letter queue.
"""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp


class AsyncHardenedError(Exception):
    """Base class for client-raised errors."""


class CircuitOpenError(AsyncHardenedError):
    """Raised when the breaker rejects a request without attempting it."""

    def __init__(self, host: str, retry_after: float):
        super().__init__(f"circuit open for {host}, retry after {retry_after:.1f}s")
        self.host = host
        self.retry_after = retry_after


class RetryableError(AsyncHardenedError):
    """Wraps a transient failure that should be retried per policy."""

    def __init__(self, message: str, *, status_code: int | None = None, retry_after: float | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class NonRetryableError(AsyncHardenedError):
    """Wraps a permanent failure. Surfaced from the queue and persisted to DLQ."""

    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class DeadLetterError(NonRetryableError):
    """Signal that a request has exhausted retries and been moved to DLQ."""


def is_retryable_status(status_code: int) -> bool:
    """HTTP 5xx and 429 are retryable; other 4xx are permanent."""
    if status_code == 429:
        return True
    return 500 <= status_code < 600


def is_retryable_exception(exc: BaseException) -> bool:
    """Network-layer transient errors are retryable; everything else is not."""
    if isinstance(exc, RetryableError):
        return True
    if isinstance(exc, NonRetryableError):
        return False
    if isinstance(exc, asyncio.TimeoutError):
        return True
    if isinstance(exc, aiohttp.ClientConnectionError):
        return True
    if isinstance(exc, aiohttp.ServerDisconnectedError):
        return True
    if isinstance(exc, aiohttp.ClientPayloadError):
        return True
    return False
