"""In-flight deduplication registry.

When two coroutines submit the same request concurrently — same method,
URL, params, body — they end up with the same idempotency key. The
registry collapses them: the second caller does not enqueue, it simply
attaches a listener to the future that the first caller's request will
resolve. This both saves a real HTTP call and guarantees the two callers
observe the *same* result, which matters for non-idempotent semantics
implemented at the application layer.

The registry is a process-local map; restart recovery uses the SQLite
inflight table instead. Combined, the two layers ensure no duplicate
in-process work and no lost work across crashes.
"""

from __future__ import annotations

import asyncio
from typing import Generic, TypeVar

T = TypeVar("T")


class InFlightRegistry(Generic[T]):
    """Map of idempotency_key → asyncio.Future shared by all waiters."""

    def __init__(self):
        self._inflight: dict[str, asyncio.Future[T]] = {}
        self._lock = asyncio.Lock()
        # Counters exposed for observability — no locking on read; values
        # are simple integers so the worst case is a slightly stale read.
        self.dedup_hits = 0
        self.unique_requests = 0

    async def get_or_create(self, key: str) -> tuple[asyncio.Future[T], bool]:
        """Return (future, owner) for the given key.

        `owner` is True if this caller is responsible for executing the
        request and resolving the future. False means another caller owns
        execution and this caller should just await.
        """
        async with self._lock:
            existing = self._inflight.get(key)
            if existing is not None and not existing.done():
                self.dedup_hits += 1
                return existing, False
            future: asyncio.Future[T] = asyncio.get_running_loop().create_future()
            self._inflight[key] = future
            self.unique_requests += 1
            return future, True

    async def resolve(self, key: str, value: T) -> None:
        async with self._lock:
            future = self._inflight.pop(key, None)
        if future is not None and not future.done():
            future.set_result(value)

    async def fail(self, key: str, exc: BaseException) -> None:
        async with self._lock:
            future = self._inflight.pop(key, None)
        if future is not None and not future.done():
            future.set_exception(exc)

    async def discard(self, key: str) -> None:
        """Drop a key without resolving — used when a request enters the
        dead-letter queue and the future has already been failed elsewhere.
        """
        async with self._lock:
            self._inflight.pop(key, None)
