"""Bounded request queue with backpressure.

The queue itself is a thin wrapper over `asyncio.Queue` — its job is to
own queue depth tracking and provide a single place to read for the
metrics layer. The actual orchestration (dedup → breaker → rate-limiter
→ execute → retry/DLQ) lives in `client.py`.

Backpressure: `enqueue()` is awaitable and blocks when the queue is full,
which propagates pressure all the way back to the caller invoking
`client.request()`. This is the contract that prevents the client from
being a memory bomb under sustained overload.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from async_hardened_client.models import Request


@dataclass
class QueueItem:
    """A unit of work pulled by a worker.

    `attempt` is the zero-indexed retry attempt. The orchestrator increments
    it before re-enqueuing on a transient failure. The worker resolves the
    shared dedup future via `idempotency_key` once execution settles.
    """
    request: Request
    attempt: int = 0


class RequestQueue:
    """Bounded async FIFO with depth tracking."""

    def __init__(self, max_size: int):
        if max_size <= 0:
            raise ValueError("max_size must be > 0")
        self._q: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=max_size)
        self._max = max_size

    @property
    def depth(self) -> int:
        return self._q.qsize()

    @property
    def max_size(self) -> int:
        return self._max

    @property
    def is_full(self) -> bool:
        return self._q.full()

    async def enqueue(self, item: QueueItem) -> None:
        """Block until a slot is available — backpressure for callers."""
        await self._q.put(item)

    def try_enqueue(self, item: QueueItem) -> bool:
        """Non-blocking variant. Returns False when the queue is full."""
        try:
            self._q.put_nowait(item)
            return True
        except asyncio.QueueFull:
            return False

    async def dequeue(self) -> QueueItem:
        return await self._q.get()

    def task_done(self) -> None:
        self._q.task_done()

    async def join(self) -> None:
        await self._q.join()
