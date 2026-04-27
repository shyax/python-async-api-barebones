"""Configuration dataclasses for the hardened client.

All behavior is config-driven: rate limits, retry policy, breaker thresholds,
queue size, and storage location are set once at construction and never mutated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class RateLimitConfig:
    """Token-bucket parameters for a single host.

    `rate_per_second` is the steady-state refill rate; `burst` is the bucket
    capacity, allowing short bursts above the steady rate.
    """

    rate_per_second: float = 10.0
    burst: int = 20


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Thresholds for the per-host circuit breaker."""

    failure_threshold: int = 5
    failure_rate: float = 0.5
    rolling_window: int = 20
    open_seconds: float = 30.0
    half_open_max_probes: int = 1


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff retry policy.

    Backoff is `min(base * 2**attempt, cap)` plus a uniform jitter in
    [0, base * 2**attempt]. 429 responses honor `Retry-After` when present.
    """

    max_retries: int = 5
    base_delay: float = 0.25
    max_delay: float = 30.0
    jitter: bool = True


@dataclass(frozen=True)
class QueueConfig:
    """Sizing for the bounded request queue and worker pool."""

    max_size: int = 1000
    workers: int = 16


@dataclass(frozen=True)
class ClientConfig:
    """Top-level client configuration.

    `db_path` is the SQLite file backing inflight, retry, and dead-letter
    state. Use `:memory:` for ephemeral test runs.
    """

    db_path: str | Path = "ahc_state.db"
    request_timeout: float = 15.0
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    queue: QueueConfig = field(default_factory=QueueConfig)
    user_agent: str = "async-hardened-client/0.1"
