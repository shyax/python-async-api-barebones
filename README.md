# async-hardened-client

Production-grade async HTTP client built on `aiohttp` with a consistent
resilience layer: per-host rate limiting, circuit breaking, request
deduplication, classified retries, a SQLite-backed dead-letter queue, and
crash recovery across process restarts.

> Status: under active construction — components are landing
> commit-by-commit.

## Layout

```
async_hardened_client/      core library
mock_server/                FastAPI failure-simulation server
tests/                      unit, integration, and load tests
```

## Requirements

- Python 3.11+
- aiohttp, aiosqlite, structlog (runtime)
- fastapi, uvicorn, httpx, pytest, pytest-asyncio (dev/mock)

## Quickstart (preview)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[mock,test]'
pytest -q
```

Full usage, architecture, and operations runbook will be published with the
final milestone.
