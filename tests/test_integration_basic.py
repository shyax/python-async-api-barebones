"""Basic integration tests: client ↔ mock server happy path."""

from __future__ import annotations

import asyncio

import pytest


async def test_simple_get_succeeds(client, mock_server):
    resp = await client.request("GET", f"{mock_server.base_url}/flaky", params={"a": "1"})
    assert resp.status_code == 200
    assert resp.data["ok"] is True
    assert resp.data["query"] == {"a": "1"}
    assert resp.retries == 0


async def test_post_with_json_body(client, mock_server):
    resp = await client.request(
        "POST", f"{mock_server.base_url}/flaky", body={"name": "alpha", "n": 7}
    )
    assert resp.status_code == 200
    assert resp.data["body"] == {"name": "alpha", "n": 7}


async def test_concurrent_requests_resolve_independently(client, mock_server):
    urls = [f"{mock_server.base_url}/flaky?i={i}" for i in range(20)]
    results = await asyncio.gather(*(client.request("GET", u) for u in urls))
    statuses = [r.status_code for r in results]
    assert all(s == 200 for s in statuses)


async def test_metrics_capture_outcome(client, mock_server):
    for _ in range(5):
        await client.request("GET", f"{mock_server.base_url}/flaky")
    snap = client.metrics()
    assert snap["requests_started"] == 5
    assert snap["requests_succeeded"] == 5
    assert snap["success_rate"] == pytest.approx(1.0)
