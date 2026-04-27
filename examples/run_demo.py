"""End-to-end demo: spin up the failure simulator, run a mixed workload
through the client, print the metrics snapshot.

Run with:

    python examples/run_demo.py

You should see roughly:

    * a burst of `request.success` lines on stderr
    * a few `request.retry_scheduled` lines for the simulated 5xx
    * a final metrics block on stdout showing nonzero retries and 100%
      success rate
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket

import aiohttp
import uvicorn

from async_hardened_client import (
    AsyncHardenedClient,
    CircuitBreakerConfig,
    ClientConfig,
    QueueConfig,
    RateLimitConfig,
    RetryPolicy,
    configure_logging,
)
from mock_server import build_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _run_server(app, port: int):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="off")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    return server, task


async def main() -> None:
    configure_logging()
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    app = build_app()
    server, server_task = await _run_server(app, port)

    # Configure the simulator: 30% 5xx, 10% 429.
    async with aiohttp.ClientSession() as s:
        await s.post(f"{base}/admin/profile", json={"p_500": 0.3, "p_429": 0.1, "seed": 7})

    cfg = ClientConfig(
        db_path=":memory:",
        rate_limit=RateLimitConfig(rate_per_second=100.0, burst=10),
        circuit_breaker=CircuitBreakerConfig(failure_threshold=20, failure_rate=0.99),
        retry=RetryPolicy(max_retries=8, base_delay=0.01, max_delay=0.5),
        queue=QueueConfig(max_size=200, workers=16),
    )

    async with AsyncHardenedClient(cfg) as client:
        urls = [f"{base}/flaky?i={i}" for i in range(50)]
        responses = await asyncio.gather(*(client.request("GET", u) for u in urls))

        # Demonstrate dedup: 20 concurrent identical requests collapse to 1.
        dedup_results = await asyncio.gather(
            *(client.request("GET", f"{base}/flaky", params={"unique": "x"}) for _ in range(20))
        )
        assert all(r is dedup_results[0] for r in dedup_results)

        snapshot = client.metrics()
    print(json.dumps(snapshot, indent=2))

    server.should_exit = True
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(server_task, timeout=5.0)


if __name__ == "__main__":
    asyncio.run(main())
