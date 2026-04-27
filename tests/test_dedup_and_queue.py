"""Tests for the in-flight dedup registry and request queue."""

from __future__ import annotations

import asyncio

import pytest

from async_hardened_client.dedup import InFlightRegistry
from async_hardened_client.models import Request
from async_hardened_client.queue import QueueItem, RequestQueue


# ---------- dedup ----------

async def test_first_caller_owns_the_future():
    reg = InFlightRegistry[int]()
    future, owner = await reg.get_or_create("key-a")
    assert owner is True
    assert reg.unique_requests == 1
    assert reg.dedup_hits == 0
    assert not future.done()


async def test_second_caller_attaches_to_same_future():
    reg = InFlightRegistry[int]()
    fut1, owner1 = await reg.get_or_create("key-a")
    fut2, owner2 = await reg.get_or_create("key-a")
    assert owner1 is True
    assert owner2 is False
    assert fut1 is fut2
    assert reg.dedup_hits == 1
    assert reg.unique_requests == 1


async def test_resolve_wakes_all_waiters_with_same_value():
    reg = InFlightRegistry[int]()
    fut, _ = await reg.get_or_create("k")
    fut2, _ = await reg.get_or_create("k")
    fut3, _ = await reg.get_or_create("k")
    assert fut is fut2 is fut3

    await reg.resolve("k", 42)
    assert (await fut) == 42
    assert (await fut2) == 42
    assert (await fut3) == 42


async def test_fail_propagates_exception_to_all_waiters():
    reg = InFlightRegistry[int]()
    fut1, _ = await reg.get_or_create("k")
    fut2, _ = await reg.get_or_create("k")

    err = RuntimeError("boom")
    await reg.fail("k", err)
    with pytest.raises(RuntimeError, match="boom"):
        await fut1
    with pytest.raises(RuntimeError, match="boom"):
        await fut2


async def test_resolved_key_is_evicted_so_next_call_is_fresh():
    reg = InFlightRegistry[int]()
    fut1, owner1 = await reg.get_or_create("k")
    await reg.resolve("k", 1)
    fut2, owner2 = await reg.get_or_create("k")
    assert owner2 is True
    assert fut1 is not fut2
    assert reg.unique_requests == 2


async def test_concurrent_dedup_under_burst_load():
    """50 coroutines hit the same key simultaneously — one should own,
    the rest should attach. Final result must be observed by all."""
    reg = InFlightRegistry[str]()

    async def caller():
        fut, owner = await reg.get_or_create("hot")
        if owner:
            await asyncio.sleep(0.05)
            await reg.resolve("hot", "done")
        return await fut

    results = await asyncio.gather(*(caller() for _ in range(50)))
    assert results == ["done"] * 50
    assert reg.unique_requests == 1
    assert reg.dedup_hits == 49


# ---------- queue ----------

async def test_queue_enforces_size():
    q = RequestQueue(max_size=2)
    req = Request(method="GET", url="https://e.com/")
    assert q.try_enqueue(QueueItem(req)) is True
    assert q.try_enqueue(QueueItem(req)) is True
    # Full now — non-blocking enqueue must return False
    assert q.try_enqueue(QueueItem(req)) is False
    assert q.is_full
    assert q.depth == 2


async def test_queue_blocking_enqueue_provides_backpressure():
    q = RequestQueue(max_size=1)
    req = Request(method="GET", url="https://e.com/")
    await q.enqueue(QueueItem(req))

    # Second enqueue must block until a worker dequeues
    enqueue_done = asyncio.Event()

    async def producer():
        await q.enqueue(QueueItem(req))
        enqueue_done.set()

    task = asyncio.create_task(producer())
    await asyncio.sleep(0.02)
    assert not enqueue_done.is_set()  # still blocked

    item = await q.dequeue()
    q.task_done()
    await asyncio.wait_for(enqueue_done.wait(), timeout=1.0)
    await task

    # Drain second item so test exits cleanly
    await q.dequeue()
    q.task_done()


async def test_queue_invalid_size_rejected():
    with pytest.raises(ValueError):
        RequestQueue(max_size=0)
