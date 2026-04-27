"""Configurable failure-simulation server.

Endpoints:

  GET  /healthz                      always-200 liveness
  GET  /echo                         200 with the parsed request as JSON
  GET  /flaky                        respects the active FailureProfile
  POST /flaky                        same, accepts a body
  GET  /metrics                      counts of issued response codes
  POST /admin/profile                replace the active FailureProfile
  POST /admin/reset                  reset counters and ratelimits

The "flaky" endpoint is the integration-test surface. The active
FailureProfile decides:

* what fraction of requests get a 429 (rate-limit) or 5xx (server error)
* whether to inject artificial latency before responding
* a hard token-bucket rate limit (independent of the random 429s),
  emulating a server that strictly enforces a quota

Every test fixture flips the profile to a deterministic seed so tests
are stable.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, HTTPException, Request as FastAPIRequest, Response as FastAPIResponse
from fastapi.responses import JSONResponse


@dataclass
class FailureProfile:
    """Knobs the test sets to drive the mock's behavior."""

    p_429: float = 0.0
    p_500: float = 0.0
    latency_seconds: float = 0.0
    # Hard token-bucket: server enforces this regardless of the random p_429.
    enforce_rate_per_second: float | None = None
    enforce_burst: int = 5
    seed: int | None = 0
    # When set, the first N requests succeed deterministically before random
    # failures kick in — useful for "warm up then fail" scenarios.
    succeed_first: int = 0


@dataclass
class _ServerState:
    profile: FailureProfile = field(default_factory=FailureProfile)
    rng: random.Random = field(default_factory=random.Random)
    counters: dict[int, int] = field(default_factory=dict)
    request_count: int = 0
    bucket_tokens: float = 0.0
    bucket_last_refill: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def reset_for_profile(self, profile: FailureProfile) -> None:
        self.profile = profile
        self.rng = random.Random(profile.seed)
        self.counters.clear()
        self.request_count = 0
        self.bucket_tokens = float(profile.enforce_burst)
        self.bucket_last_refill = time.monotonic()


def build_app(profile: FailureProfile | None = None) -> FastAPI:
    state = _ServerState()
    state.reset_for_profile(profile or FailureProfile())

    app = FastAPI(title="ahc-mock", version="0.1")
    app.state.simulator = state

    async def _check_rate_limit() -> None:
        if state.profile.enforce_rate_per_second is None:
            return
        async with state.lock:
            now = time.monotonic()
            elapsed = now - state.bucket_last_refill
            state.bucket_tokens = min(
                float(state.profile.enforce_burst),
                state.bucket_tokens + elapsed * state.profile.enforce_rate_per_second,
            )
            state.bucket_last_refill = now
            if state.bucket_tokens < 1.0:
                deficit = 1.0 - state.bucket_tokens
                wait = deficit / state.profile.enforce_rate_per_second
                state.counters[429] = state.counters.get(429, 0) + 1
                raise HTTPException(
                    status_code=429,
                    detail="rate limited",
                    headers={"Retry-After": f"{wait:.3f}"},
                )
            state.bucket_tokens -= 1.0

    async def _maybe_random_failure() -> tuple[int, dict[str, Any]] | None:
        async with state.lock:
            state.request_count += 1
            count = state.request_count
            if count <= state.profile.succeed_first:
                return None
            if state.profile.latency_seconds > 0:
                await_seconds = state.profile.latency_seconds
            else:
                await_seconds = 0.0
            roll_429 = state.rng.random()
            roll_500 = state.rng.random()
        if await_seconds > 0:
            await asyncio.sleep(await_seconds)
        if roll_429 < state.profile.p_429:
            state.counters[429] = state.counters.get(429, 0) + 1
            return 429, {"detail": "throttled"}
        if roll_500 < state.profile.p_500:
            code = state.rng.choice([500, 502, 503])
            state.counters[code] = state.counters.get(code, 0) + 1
            return code, {"detail": "server error"}
        return None

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/echo")
    async def echo(request: FastAPIRequest) -> dict[str, Any]:
        params = dict(request.query_params)
        return {"path": request.url.path, "query": params}

    @app.api_route("/flaky", methods=["GET", "POST", "PUT", "DELETE"])
    async def flaky(request: FastAPIRequest) -> Any:
        await _check_rate_limit()
        failure = await _maybe_random_failure()
        if failure is not None:
            status, body = failure
            return JSONResponse(content=body, status_code=status)
        body = None
        if request.method in ("POST", "PUT"):
            try:
                body = await request.json()
            except Exception:
                body = (await request.body()).decode("utf-8", errors="replace")
        state.counters[200] = state.counters.get(200, 0) + 1
        return {
            "ok": True,
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.query_params),
            "body": body,
            "n": state.request_count,
        }

    @app.get("/metrics")
    async def metrics() -> dict[str, Any]:
        return {"counters": dict(state.counters), "request_count": state.request_count}

    @app.post("/admin/profile")
    async def set_profile(profile: dict[str, Any]) -> dict[str, str]:
        new = FailureProfile(
            p_429=float(profile.get("p_429", 0.0)),
            p_500=float(profile.get("p_500", 0.0)),
            latency_seconds=float(profile.get("latency_seconds", 0.0)),
            enforce_rate_per_second=(
                float(profile["enforce_rate_per_second"])
                if profile.get("enforce_rate_per_second") is not None
                else None
            ),
            enforce_burst=int(profile.get("enforce_burst", 5)),
            seed=profile.get("seed", 0),
            succeed_first=int(profile.get("succeed_first", 0)),
        )
        state.reset_for_profile(new)
        return {"status": "applied"}

    @app.post("/admin/reset")
    async def reset() -> dict[str, str]:
        state.reset_for_profile(state.profile)
        return {"status": "reset"}

    return app


def main() -> None:
    """Console entry point: `python -m mock_server.app`."""
    import uvicorn

    uvicorn.run(build_app(), host="127.0.0.1", port=8765, log_level="info")


if __name__ == "__main__":
    main()
