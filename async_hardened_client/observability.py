"""Structured logging and in-process metrics.

Logging uses `structlog` configured for JSON output, so every log line is a
machine-readable record with stable field names. The contract is that any
log message in the resilience layer always carries `event` (a short verb
phrase like `request.success`, `breaker.opened`, `dlq.push`) plus the
request_id when relevant — operators can grep one stream and pivot by
either dimension.

Metrics live in a `Metrics` class instead of a global registry to keep the
client safely re-instantiable in tests. Each AsyncHardenedClient owns one
Metrics. The values are simple counters and gauges; no histogram support
because everything we'd want there (latency percentiles) is better
collected externally and we want this layer to stay dependency-free.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from typing import Any

import structlog


def configure_logging(level: int = logging.INFO, *, force: bool = False) -> None:
    """Initialize structlog for JSON output. Idempotent unless `force`."""
    if structlog.is_configured() and not force:
        return
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=level,
        force=True,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = "async_hardened_client") -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


@dataclass
class Metrics:
    """In-process counters and gauges. Read at any time for a snapshot."""

    requests_started: int = 0
    requests_succeeded: int = 0
    requests_failed_retryable: int = 0
    requests_failed_terminal: int = 0
    retries_performed: int = 0
    dedup_hits: int = 0
    dlq_pushes: int = 0
    rate_limit_waits: int = 0
    rate_limit_wait_seconds_total: float = 0.0
    breaker_short_circuits: int = 0
    breaker_state: dict[str, str] = field(default_factory=dict)
    queue_depth: int = 0
    queue_max_size: int = 0

    def snapshot(self) -> dict[str, Any]:
        """Plain-dict view, safe to JSON-serialize for /metrics or logs."""
        return {
            "requests_started": self.requests_started,
            "requests_succeeded": self.requests_succeeded,
            "requests_failed_retryable": self.requests_failed_retryable,
            "requests_failed_terminal": self.requests_failed_terminal,
            "retries_performed": self.retries_performed,
            "dedup_hits": self.dedup_hits,
            "dlq_pushes": self.dlq_pushes,
            "rate_limit_waits": self.rate_limit_waits,
            "rate_limit_wait_seconds_total": round(self.rate_limit_wait_seconds_total, 4),
            "breaker_short_circuits": self.breaker_short_circuits,
            "breaker_state": dict(self.breaker_state),
            "queue_depth": self.queue_depth,
            "queue_max_size": self.queue_max_size,
            "success_rate": (
                self.requests_succeeded / self.requests_started
                if self.requests_started
                else 0.0
            ),
        }
