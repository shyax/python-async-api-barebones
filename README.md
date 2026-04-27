# async-hardened-client

Production-grade async HTTP client built on `aiohttp` with a consistent
resilience layer: per-host rate limiting, circuit breaking, request
deduplication, classified retries, a SQLite-backed dead-letter queue, and
crash recovery across process restarts.

Designed for autonomous Python automation platforms (launchd-managed Mac
mini, k8s daemons) that integrate with rate-limited, intermittently flaky
upstream APIs and cannot afford to silently drop requests on restart.

## What it gives you

| Concern | Without this library | With this library |
| --- | --- | --- |
| Rate limits | hand-rolled per-agent | per-host token bucket, configurable per endpoint |
| Transient 5xx / network errors | uncaught, dropped | classified, retried with full-jitter backoff |
| 429 / Retry-After | ignored | honored verbatim, capped by policy max |
| Sustained upstream failure | retry storms forever | breaker opens, requests short-circuit |
| Concurrent identical requests | N HTTP calls | one HTTP call, N callers attached to a shared future |
| Process crash mid-request | request lost | persisted to SQLite, replayed on next boot |
| Unrecoverable failures | swallowed | dead-letter queue with a CLI to inspect/replay |
| Observability | print() debugging | structured JSON logs + metrics snapshot |

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the component diagram and
the request lifecycle, and [`RUNBOOK.md`](./RUNBOOK.md) for operational
procedures (DLQ replay, recovery, reading metrics).

## Layout

```
async_hardened_client/    core library
  client.py               orchestrator (the public AsyncHardenedClient)
  models.py               Request, Response
  config.py               ClientConfig + per-component configs
  errors.py               error taxonomy and HTTP/exception classification
  rate_limiter.py         per-host token bucket
  circuit_breaker.py      per-host CLOSED/OPEN/HALF_OPEN state machine
  retry.py                full-jitter backoff math
  queue.py                bounded asyncio queue with backpressure
  dedup.py                in-flight registry collapsing duplicates
  storage.py              aiosqlite persistence for inflight + DLQ
  recovery is built into client.start() — see ARCHITECTURE.md
  dead_letter.py          high-level DLQ ops (list, replay, purge)
  cli.py                  `ahc` command-line tool
  observability.py        structlog + Metrics dataclass
mock_server/              FastAPI failure-simulation server
  app.py                  configurable 429/5xx/latency/rate-limit knobs
tests/
  test_*.py               unit tests for each component
  test_integration_*.py   end-to-end tests against the mock server
  test_recovery.py        crash-recovery scenarios
  test_load.py            100/200 concurrent + failure-storm load tests
```

## Requirements

- Python 3.11+
- Runtime: `aiohttp`, `aiosqlite`, `structlog`
- Dev/mock: `fastapi`, `uvicorn`, `pytest`, `pytest-asyncio`, `httpx`

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[mock,test]'
```

## Quickstart

```python
import asyncio
from async_hardened_client import AsyncHardenedClient, ClientConfig

async def main():
    config = ClientConfig(db_path="ahc_state.db")
    async with AsyncHardenedClient(config) as client:
        response = await client.request(
            "GET",
            "https://api.sam.gov/opportunities/v2/search",
            params={"limit": 25},
        )
        print(response.status_code, response.data)

asyncio.run(main())
```

That single block gives you, by default:

- A shared `aiohttp.ClientSession` with a connection-pooled TCP connector
- A per-host token bucket sized 10 rps with burst 20
- A circuit breaker that opens after 5 consecutive failures or 50%
  failure rate over the last 20 outcomes
- 5 retries with full-jitter backoff capped at 30s
- 1000-deep request queue with 16 workers and backpressure on `request()`
- SQLite-backed inflight tracking — if the process is killed mid-flight,
  the next boot replays orphaned requests automatically
- A dead-letter queue for terminal failures, inspectable via `ahc dlq list`

Tune any of those via `ClientConfig` — every knob is documented in
[`config.py`](./async_hardened_client/config.py).

## Operating the dead-letter queue

```bash
ahc dlq list   --db ahc_state.db
ahc dlq replay --db ahc_state.db          # all rows
ahc dlq replay --db ahc_state.db --id 7   # one row
ahc dlq purge  --db ahc_state.db --yes
ahc metrics    --db ahc_state.db          # snapshot incl. inflight count
```

Logs go to stderr as one JSON object per line. Stdout is reserved for
machine-readable output (the JSON the commands print) so pipelines and
shell jq filters work cleanly.

## Running the test suite

```bash
pytest -q
```

71 tests, ~18s total. Coverage:

- 7 — SQLite storage round-trips
- 7 — token-bucket rate limiter (incl. 60-coroutine concurrency)
- 10 — circuit breaker state transitions
- 19 — retry classification + backoff math
- 9 — dedup registry + bounded queue
- 4 — basic integration (GET/POST/concurrent/metrics)
- 5 — resilience integration (5xx-recovery, 4xx-DLQ, dedup-collapse, strict
  rate-limit)
- 3 — crash recovery (orphan replay, retry-count preservation, idempotent
  no-op restart)
- 4 — DLQ helpers + CLI subprocess
- 3 — load (100, 200 concurrent, 50%-fail/30%-throttle storm)

## Running the failure-simulation server standalone

```bash
python -m mock_server.app
# 127.0.0.1:8765 — POST /admin/profile to flip the failure profile
# GET /flaky to be on the receiving end of the orchestrator
```

This is what the integration tests run uvicorn against in-process. You
can target it manually for ad-hoc demos.

## License

MIT — see `LICENSE`.
