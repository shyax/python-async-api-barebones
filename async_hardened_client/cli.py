"""Command-line entry point for operating the dead-letter queue.

Usage:
    ahc dlq list   [--db PATH] [--limit N]
    ahc dlq replay [--db PATH] [--id ID]
    ahc dlq purge  [--db PATH] [--yes]
    ahc metrics    [--db PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from async_hardened_client.client import AsyncHardenedClient
from async_hardened_client.config import ClientConfig
from async_hardened_client.dead_letter import (
    list_entries,
    purge,
    replay_all,
    replay_entry,
)
from async_hardened_client.observability import configure_logging
from async_hardened_client.storage import Storage


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ahc")
    sub = parser.add_subparsers(dest="cmd", required=True)

    dlq = sub.add_parser("dlq", help="Inspect and operate on the dead-letter queue")
    dlq_sub = dlq.add_subparsers(dest="dlq_cmd", required=True)

    p_list = dlq_sub.add_parser("list")
    p_list.add_argument("--db", default="ahc_state.db")
    p_list.add_argument("--limit", type=int, default=100)

    p_replay = dlq_sub.add_parser("replay")
    p_replay.add_argument("--db", default="ahc_state.db")
    p_replay.add_argument(
        "--id", type=int, default=None, help="Replay a single row id (default: all)"
    )

    p_purge = dlq_sub.add_parser("purge")
    p_purge.add_argument("--db", default="ahc_state.db")
    p_purge.add_argument("--yes", action="store_true", help="Skip confirmation")

    p_metrics = sub.add_parser("metrics", help="Print a metrics snapshot")
    p_metrics.add_argument("--db", default="ahc_state.db")
    return parser


async def _cmd_list(args: argparse.Namespace) -> int:
    async with Storage(args.db) as s:
        rows = await list_entries(s, limit=args.limit)
        out = [
            {
                "id": r.id,
                "request_id": r.request_id,
                "method": r.request.method,
                "url": r.request.url,
                "error": r.error,
                "retry_count": r.retry_count,
                "first_seen": r.first_seen,
                "last_attempt": r.last_attempt,
            }
            for r in rows
        ]
        print(json.dumps(out, indent=2, default=str))
    return 0


async def _cmd_replay(args: argparse.Namespace) -> int:
    cfg = ClientConfig(db_path=args.db)
    async with AsyncHardenedClient(cfg) as client:
        if args.id is not None:
            rows = await client._storage.list_dead_letter(limit=10_000)
            target = next((r for r in rows if r.id == args.id), None)
            if target is None:
                print(f"no DLQ row with id {args.id}", file=sys.stderr)
                return 1
            result = await replay_entry(client, target)
            print(json.dumps(result.__dict__, default=str))
            return 0 if result.succeeded else 1
        results = await replay_all(client)
        print(
            json.dumps(
                {
                    "replayed": sum(1 for r in results if r.succeeded),
                    "still_failing": sum(1 for r in results if not r.succeeded),
                    "details": [r.__dict__ for r in results],
                },
                indent=2,
                default=str,
            )
        )
        return 0 if all(r.succeeded for r in results) else 2


async def _cmd_purge(args: argparse.Namespace) -> int:
    if not args.yes:
        print("refusing to purge without --yes", file=sys.stderr)
        return 1
    async with Storage(args.db) as s:
        n = await purge(s)
        print(json.dumps({"removed": n}))
    return 0


async def _cmd_metrics(args: argparse.Namespace) -> int:
    cfg = ClientConfig(db_path=args.db)
    async with AsyncHardenedClient(cfg) as client:
        # Best-effort snapshot. The DB-backed counts come from storage,
        # the in-memory counts are zero on a fresh boot.
        snap = client.metrics()
        rows = await client._storage.list_inflight()
        dlq = await client._storage.list_dead_letter(limit=10_000)
        snap["inflight_persisted"] = len(rows)
        snap["dlq_persisted"] = len(dlq)
        print(json.dumps(snap, indent=2, default=str))
    return 0


async def _async_main(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_logging()
    if args.cmd == "dlq":
        if args.dlq_cmd == "list":
            return await _cmd_list(args)
        if args.dlq_cmd == "replay":
            return await _cmd_replay(args)
        if args.dlq_cmd == "purge":
            return await _cmd_purge(args)
    if args.cmd == "metrics":
        return await _cmd_metrics(args)
    parser.error(f"unknown command: {args.cmd}")
    return 2


def main(argv: list[str] | None = None) -> int:
    code = asyncio.run(_async_main(argv if argv is not None else sys.argv[1:]))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
