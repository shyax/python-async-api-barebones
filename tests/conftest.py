"""Shared fixtures: mock server lifecycle and a pre-wired client."""

from __future__ import annotations

import asyncio
import contextlib
import socket

import pytest
import uvicorn

from async_hardened_client import (
    AsyncHardenedClient,
    CircuitBreakerConfig,
    ClientConfig,
    QueueConfig,
    RateLimitConfig,
    RetryPolicy,
)
from mock_server import FailureProfile, build_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _UvicornInProcess:
    """Run uvicorn on a background task so the same event loop drives both
    sides — fastest path to a real HTTP loopback in tests."""

    def __init__(self, app, port: int):
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
        self._server = uvicorn.Server(config)
        self._task: asyncio.Task | None = None
        self._port = port

    async def start(self) -> None:
        self._task = asyncio.create_task(self._server.serve())
        # Wait for the server to flip its `started` flag.
        for _ in range(200):
            if self._server.started:
                return
            await asyncio.sleep(0.02)
        raise RuntimeError("uvicorn did not start in time")

    async def stop(self) -> None:
        self._server.should_exit = True
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(self._task, timeout=5.0)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._port}"


@pytest.fixture
async def mock_server():
    port = _free_port()
    app = build_app(FailureProfile())
    server = _UvicornInProcess(app, port)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
async def client(tmp_path, mock_server):
    """Standard test client: small queue, tight rate limit, fast retry."""
    config = ClientConfig(
        db_path=str(tmp_path / "ahc.db"),
        request_timeout=5.0,
        rate_limit=RateLimitConfig(rate_per_second=200.0, burst=50),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=10, failure_rate=0.9, rolling_window=20, open_seconds=0.2
        ),
        retry=RetryPolicy(max_retries=4, base_delay=0.02, max_delay=0.5, jitter=False),
        queue=QueueConfig(max_size=200, workers=8),
    )
    async with AsyncHardenedClient(config) as c:
        yield c
