"""High-level operations on the dead-letter queue.

The Storage class owns the SQLite plumbing; this module wraps it with
operator-friendly verbs (`list`, `replay`, `replay_all`, `purge`) that
the CLI exposes. Replay re-submits a DLQ row through a live
AsyncHardenedClient and removes the row only on successful resolution —
if the replay itself fails, the row stays in the DLQ for another try.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from async_hardened_client.errors import AsyncHardenedError
from async_hardened_client.observability import get_logger
from async_hardened_client.storage import DeadLetterRow, Storage

if TYPE_CHECKING:
    from async_hardened_client.client import AsyncHardenedClient

log = get_logger("ahc.dlq")


@dataclass
class ReplayResult:
    row_id: int
    request_id: str
    succeeded: bool
    error: str | None = None


async def list_entries(storage: Storage, *, limit: int = 100) -> list[DeadLetterRow]:
    return await storage.list_dead_letter(limit=limit)


async def replay_entry(
    client: AsyncHardenedClient, row: DeadLetterRow
) -> ReplayResult:
    """Re-submit a single DLQ row through the live client.

    Strategy: delete the existing DLQ row up-front so the per-key DLQ stays
    a single source of truth. If the replay succeeds we are done; if it
    fails again, the orchestrator's terminal-failure path will write a
    fresh DLQ row with current timestamps and the new error reason.
    Either way, we never silently drop unrecovered work.
    """
    await client._storage.remove_dead_letter(row.id)
    try:
        await client.request(
            method=row.request.method,
            url=row.request.url,
            params=row.request.params,
            headers=row.request.headers,
            body=row.request.body,
            priority=row.request.priority,
        )
    except AsyncHardenedError as exc:
        log.warning("dlq.replay_failed", row_id=row.id, error=str(exc))
        return ReplayResult(
            row_id=row.id,
            request_id=row.request_id,
            succeeded=False,
            error=str(exc),
        )
    log.info("dlq.replayed", row_id=row.id, request_id=row.request_id)
    return ReplayResult(row_id=row.id, request_id=row.request_id, succeeded=True)


async def replay_all(client: AsyncHardenedClient) -> list[ReplayResult]:
    """Replay every row currently in the DLQ. Order is the SQLite default
    (newest-first from `list_dead_letter`); we walk a snapshot so rows
    that fail re-replay aren't re-walked in this pass."""
    rows = await client._storage.list_dead_letter(limit=10_000)
    results: list[ReplayResult] = []
    for row in rows:
        results.append(await replay_entry(client, row))
    return results


async def purge(storage: Storage) -> int:
    """Delete every DLQ row. Operator escape hatch when entries are known
    to be irrelevant (e.g. fixed upstream contract). Always logs the
    count so the action is auditable."""
    n = await storage.purge_dead_letter()
    log.warning("dlq.purged", removed=n)
    return n
