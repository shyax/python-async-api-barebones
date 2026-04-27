"""SQLite-backed persistence for inflight tracking and the dead-letter queue.

Two tables back the resilience layer:

* `inflight_requests` — rows for every request currently in the queue, in
  retry, or actively executing. Rows are inserted on enqueue, updated on
  retry, and deleted on successful completion or move-to-DLQ. The contents
  drive crash recovery: on startup we re-enqueue every row found here.

* `dead_letter_queue` — rows for terminally-failed requests. Each row keeps
  the full original payload plus the failure reason and attempt count, so
  operators can inspect or replay through the CLI.

We use `aiosqlite` so all access is non-blocking. The connection is opened
once per `Storage` instance and protected by an `asyncio.Lock` to serialize
writes — sqlite supports a single writer at a time, and this matches the
single-process, single-event-loop design.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiosqlite

from async_hardened_client.models import Request

_SCHEMA = """
CREATE TABLE IF NOT EXISTS inflight_requests (
    idempotency_key TEXT PRIMARY KEY,
    request_id      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inflight_status ON inflight_requests(status);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT NOT NULL,
    request_id       TEXT NOT NULL,
    payload          TEXT NOT NULL,
    error            TEXT NOT NULL,
    retry_count      INTEGER NOT NULL,
    first_seen       REAL NOT NULL,
    last_attempt     REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dlq_key ON dead_letter_queue(idempotency_key);
"""


# Inflight statuses. `pending` covers freshly enqueued and queued-for-retry
# alike; `in_progress` is set the moment a worker picks the row up.
STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"


@dataclass(frozen=True)
class InflightRow:
    idempotency_key: str
    request_id: str
    request: Request
    status: str
    retry_count: int
    last_error: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class DeadLetterRow:
    id: int
    idempotency_key: str
    request_id: str
    request: Request
    error: str
    retry_count: int
    first_seen: float
    last_attempt: float


def _now() -> float:
    return time.time()


class Storage:
    """Async SQLite wrapper. Thread-safety is provided by the event loop;
    inter-coroutine write serialization is handled by `_write_lock`."""

    def __init__(self, db_path: str | Path):
        self._db_path = str(db_path)
        self._conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def open(self) -> None:
        if self._conn is not None:
            return
        self._conn = await aiosqlite.connect(self._db_path)
        # WAL gives us readers-don't-block-writers; perfectly matched to a
        # single writer + many readers from the worker pool.
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> Storage:
        await self.open()
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.close()

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Storage not opened — call .open() first")
        return self._conn

    # ------------------------------------------------------------------
    # inflight_requests
    # ------------------------------------------------------------------

    async def upsert_inflight(
        self,
        request: Request,
        *,
        status: str = STATUS_PENDING,
        retry_count: int = 0,
        last_error: str | None = None,
    ) -> None:
        """Insert or update an inflight row keyed by idempotency_key.

        Concurrent duplicate requests collapse to the same row — the dedup
        layer ensures only one of them actually enters the worker pool.
        """
        payload = json.dumps(request.to_storage())
        now = _now()
        async with self._write_lock:
            await self.conn.execute(
                """
                INSERT INTO inflight_requests
                    (idempotency_key, request_id, payload, status, retry_count, last_error, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    status      = excluded.status,
                    retry_count = excluded.retry_count,
                    last_error  = excluded.last_error,
                    updated_at  = excluded.updated_at
                """,
                (
                    request.idempotency_key,
                    request.request_id,
                    payload,
                    status,
                    retry_count,
                    last_error,
                    now,
                    now,
                ),
            )
            await self.conn.commit()

    async def mark_inflight_status(
        self, idempotency_key: str, status: str, *, last_error: str | None = None
    ) -> None:
        async with self._write_lock:
            await self.conn.execute(
                """
                UPDATE inflight_requests
                SET status = ?, last_error = COALESCE(?, last_error), updated_at = ?
                WHERE idempotency_key = ?
                """,
                (status, last_error, _now(), idempotency_key),
            )
            await self.conn.commit()

    async def increment_retry(self, idempotency_key: str, *, last_error: str) -> int:
        """Bump the retry counter and return the new value."""
        async with self._write_lock:
            await self.conn.execute(
                """
                UPDATE inflight_requests
                SET retry_count = retry_count + 1,
                    status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE idempotency_key = ?
                """,
                (STATUS_PENDING, last_error, _now(), idempotency_key),
            )
            await self.conn.commit()
        async with self.conn.execute(
            "SELECT retry_count FROM inflight_requests WHERE idempotency_key = ?",
            (idempotency_key,),
        ) as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    async def delete_inflight(self, idempotency_key: str) -> None:
        async with self._write_lock:
            await self.conn.execute(
                "DELETE FROM inflight_requests WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            await self.conn.commit()

    async def list_inflight(self) -> list[InflightRow]:
        async with self.conn.execute(
            """
            SELECT idempotency_key, request_id, payload, status, retry_count,
                   last_error, created_at, updated_at
            FROM inflight_requests
            ORDER BY created_at ASC
            """
        ) as cur:
            rows = await cur.fetchall()
        return [
            InflightRow(
                idempotency_key=row[0],
                request_id=row[1],
                request=Request.from_storage(json.loads(row[2])),
                status=row[3],
                retry_count=int(row[4]),
                last_error=row[5],
                created_at=float(row[6]),
                updated_at=float(row[7]),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # dead_letter_queue
    # ------------------------------------------------------------------

    async def push_dead_letter(
        self, request: Request, *, error: str, retry_count: int
    ) -> int:
        payload = json.dumps(request.to_storage())
        now = _now()
        async with self._write_lock:
            cursor = await self.conn.execute(
                """
                INSERT INTO dead_letter_queue
                    (idempotency_key, request_id, payload, error, retry_count, first_seen, last_attempt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.idempotency_key,
                    request.request_id,
                    payload,
                    error,
                    retry_count,
                    now,
                    now,
                ),
            )
            await self.conn.commit()
            return int(cursor.lastrowid or 0)

    async def list_dead_letter(self, limit: int = 100) -> list[DeadLetterRow]:
        async with self.conn.execute(
            """
            SELECT id, idempotency_key, request_id, payload, error, retry_count,
                   first_seen, last_attempt
            FROM dead_letter_queue
            ORDER BY last_attempt DESC
            LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [
            DeadLetterRow(
                id=int(row[0]),
                idempotency_key=row[1],
                request_id=row[2],
                request=Request.from_storage(json.loads(row[3])),
                error=row[4],
                retry_count=int(row[5]),
                first_seen=float(row[6]),
                last_attempt=float(row[7]),
            )
            for row in rows
        ]

    async def remove_dead_letter(self, dlq_id: int) -> None:
        async with self._write_lock:
            await self.conn.execute(
                "DELETE FROM dead_letter_queue WHERE id = ?", (dlq_id,)
            )
            await self.conn.commit()

    async def purge_dead_letter(self) -> int:
        async with self._write_lock:
            cursor = await self.conn.execute("DELETE FROM dead_letter_queue")
            await self.conn.commit()
            return cursor.rowcount or 0
