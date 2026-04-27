"""Per-host circuit breaker.

States:

    CLOSED      — requests flow normally; the breaker tracks outcomes.
    OPEN        — requests are short-circuited with CircuitOpenError until
                  `open_seconds` elapses since opening.
    HALF_OPEN   — after the cool-down, allow up to `half_open_max_probes`
                  trial requests through. A success closes the breaker; a
                  single failure re-opens it for another `open_seconds`.

Triggers (CLOSED → OPEN):
- Consecutive failures exceed `failure_threshold`, OR
- Failure rate over the last `rolling_window` outcomes exceeds `failure_rate`
  (only evaluated once the window is full).

The breaker is non-blocking — `before_call()` either returns or raises
CircuitOpenError, and never awaits.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from enum import Enum
from urllib.parse import urlparse

from async_hardened_client.config import CircuitBreakerConfig
from async_hardened_client.errors import CircuitOpenError


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """A breaker for a single host."""

    def __init__(self, host: str, config: CircuitBreakerConfig, *, time_func=time.monotonic):
        self._host = host
        self._cfg = config
        self._time = time_func
        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float = 0.0
        self._inflight_probes = 0
        self._outcomes: deque[bool] = deque(maxlen=config.rolling_window)
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def host(self) -> str:
        return self._host

    async def before_call(self) -> None:
        """Gate-keep an outgoing request. Raises CircuitOpenError when blocked.

        Transitions OPEN → HALF_OPEN automatically once the cool-down has
        passed, and accounts for the probe slot before returning.
        """
        async with self._lock:
            now = self._time()
            if self._state == CircuitState.OPEN:
                if now - self._opened_at >= self._cfg.open_seconds:
                    self._state = CircuitState.HALF_OPEN
                    self._inflight_probes = 0
                else:
                    retry_after = self._cfg.open_seconds - (now - self._opened_at)
                    raise CircuitOpenError(self._host, retry_after)
            if self._state == CircuitState.HALF_OPEN:
                if self._inflight_probes >= self._cfg.half_open_max_probes:
                    # Probe slot full; reject without consuming a slot. The
                    # call will be retried via the normal retry pipeline.
                    raise CircuitOpenError(self._host, 0.0)
                self._inflight_probes += 1

    async def on_success(self) -> None:
        async with self._lock:
            self._consecutive_failures = 0
            self._outcomes.append(True)
            if self._state == CircuitState.HALF_OPEN:
                # First successful probe closes the breaker.
                self._state = CircuitState.CLOSED
                self._inflight_probes = 0
                self._outcomes.clear()

    async def on_failure(self) -> None:
        async with self._lock:
            self._consecutive_failures += 1
            self._outcomes.append(False)
            if self._state == CircuitState.HALF_OPEN:
                # A failed probe re-opens the breaker for another cool-down.
                self._open_now()
                return
            if self._state == CircuitState.CLOSED:
                if self._consecutive_failures >= self._cfg.failure_threshold:
                    self._open_now()
                    return
                if (
                    len(self._outcomes) == self._cfg.rolling_window
                    and self._failure_rate() >= self._cfg.failure_rate
                ):
                    self._open_now()

    def _failure_rate(self) -> float:
        if not self._outcomes:
            return 0.0
        failures = sum(1 for ok in self._outcomes if not ok)
        return failures / len(self._outcomes)

    def _open_now(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._time()
        self._inflight_probes = 0


class HostCircuitBreakers:
    """Lazily-instantiated per-host breaker registry."""

    def __init__(self, config: CircuitBreakerConfig):
        self._cfg = config
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def host_of(url: str) -> str:
        return urlparse(url).netloc.lower() or url

    async def for_url(self, url: str) -> CircuitBreaker:
        host = self.host_of(url)
        breaker = self._breakers.get(host)
        if breaker is None:
            async with self._lock:
                breaker = self._breakers.get(host)
                if breaker is None:
                    breaker = CircuitBreaker(host, self._cfg)
                    self._breakers[host] = breaker
        return breaker

    def snapshot(self) -> dict[str, CircuitState]:
        """Cheap state read for observability — no locking, may be slightly
        stale, never blocks."""
        return {host: br.state for host, br in self._breakers.items()}
