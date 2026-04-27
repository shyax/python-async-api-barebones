"""Token-bucket rate limiter, per host.

Each host gets its own bucket sized by `RateLimitConfig.burst` and refilled
at `rate_per_second` tokens. Acquiring a token blocks (asynchronously)
until enough capacity is available, so requests are *delayed*, never
*dropped* — that is the contract the queue relies on.

The implementation is loop-safe: a lock serializes the read-modify-write
of the token count, and a `wait_for` is computed analytically rather than
busy-looping. Multiple coroutines waiting on the same bucket wake in FIFO
order via the lock fairness.
"""

from __future__ import annotations

import asyncio
import time
from urllib.parse import urlparse

from async_hardened_client.config import RateLimitConfig


class TokenBucket:
    """Single token bucket. Capacity is `burst`, refill is `rate` tokens/sec."""

    def __init__(self, rate_per_second: float, burst: int, *, time_func=time.monotonic):
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        if burst <= 0:
            raise ValueError("burst must be > 0")
        self._rate = float(rate_per_second)
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._last = time_func()
        self._time = time_func
        self._lock = asyncio.Lock()

    @property
    def tokens(self) -> float:
        """Current token count, refilled to wallclock. Read-only snapshot."""
        return min(self._capacity, self._tokens + (self._time() - self._last) * self._rate)

    def _refill(self) -> None:
        now = self._time()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now

    async def acquire(self, count: float = 1.0) -> float:
        """Wait until `count` tokens are available, then consume them.

        Returns the wall-time delay the caller experienced (0 on a free
        acquire). The delay is exposed so the orchestrator can log when
        rate limits are actually slowing requests down.
        """
        if count > self._capacity:
            raise ValueError(f"requested {count} tokens > bucket capacity {self._capacity}")
        delay_total = 0.0
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= count:
                    self._tokens -= count
                    return delay_total
                # Compute exactly how long until enough tokens accrue.
                deficit = count - self._tokens
                wait = deficit / self._rate
            delay_total += wait
            await asyncio.sleep(wait)


class HostRateLimiter:
    """Lazily-instantiated per-host TokenBucket registry.

    A single shared default config is used for every host the client touches
    unless a host-specific override is registered via `set_override`. This
    matches the SOW requirement of "configurable per endpoint" without
    forcing callers to enumerate every host up front.
    """

    def __init__(self, default: RateLimitConfig):
        self._default = default
        self._overrides: dict[str, RateLimitConfig] = {}
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    def set_override(self, host: str, config: RateLimitConfig) -> None:
        self._overrides[host] = config
        # If a bucket already exists for this host, drop it so the next
        # acquire builds one with the new config.
        self._buckets.pop(host, None)

    @staticmethod
    def host_of(url: str) -> str:
        parsed = urlparse(url)
        return parsed.netloc.lower() or url

    async def acquire(self, url: str) -> float:
        host = self.host_of(url)
        bucket = self._buckets.get(host)
        if bucket is None:
            async with self._lock:
                bucket = self._buckets.get(host)
                if bucket is None:
                    cfg = self._overrides.get(host, self._default)
                    bucket = TokenBucket(cfg.rate_per_second, cfg.burst)
                    self._buckets[host] = bucket
        return await bucket.acquire()
