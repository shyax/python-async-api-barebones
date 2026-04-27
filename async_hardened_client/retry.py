"""Retry policy: classification + backoff math.

The retry engine itself is just a pure function: given a `RetryPolicy` and
an `attempt` number, what delay should the orchestrator wait before the
next attempt? We deliberately keep scheduling out of this module so the
orchestrator can decide whether to re-enqueue, sleep inline, or escalate
to the dead-letter queue based on the wider context.

Backoff is the canonical "full jitter" strategy from the AWS Architecture
Blog: `delay = random.uniform(0, min(cap, base * 2**attempt))`. Without
jitter, a fleet of clients that fail in lock-step retries in lock-step too
and stampedes the recovering API. The jitter spreads them out.

429 responses honor the `Retry-After` header verbatim when set, since the
upstream API has told us exactly when to come back.
"""

from __future__ import annotations

import random

from async_hardened_client.config import RetryPolicy


def should_retry(policy: RetryPolicy, attempt: int) -> bool:
    """`attempt` is the count of *previous* attempts; the first call has
    attempt=0 and is not yet a "retry"."""
    return attempt < policy.max_retries


def backoff_seconds(
    policy: RetryPolicy,
    attempt: int,
    *,
    retry_after: float | None = None,
    rng: random.Random | None = None,
) -> float:
    """Compute the wait before attempt N+1.

    `attempt` is the zero-indexed count of failures so far; attempt=0
    schedules the first retry. `retry_after` overrides the policy when
    upstream provides a Retry-After value (capped at max_delay so a
    misbehaving server cannot stall us indefinitely).
    """
    if retry_after is not None and retry_after > 0:
        return min(retry_after, policy.max_delay)

    cap = max(0.0, policy.max_delay)
    base = max(0.0, policy.base_delay)
    # 2**attempt grows fast; clamp before the random draw so jitter does
    # not amplify already-capped delays.
    target = min(cap, base * (2 ** attempt))
    if not policy.jitter:
        return target
    rng = rng or random
    return rng.uniform(0.0, target)


def parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP `Retry-After` header value (seconds form only — the
    HTTP-date form is rare in API responses and we treat it as missing)."""
    if not value:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return max(0.0, seconds)
