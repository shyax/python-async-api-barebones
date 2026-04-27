# Runbook

Operational procedures for the async-hardened-client. Assumes the client
is embedded in a daemon (e.g. an `agent.py` invoked via `launchd`) and
the SQLite state file is on local disk at `ahc_state.db`.

## Quick reference

| Symptom | First thing to check | Fix |
| --- | --- | --- |
| Daemon spamming retries | breaker state in `ahc metrics` | wait for cool-down or raise `failure_threshold` |
| Steady stream of `request.dead_letter` | upstream returning 4xx? | `ahc dlq list` to inspect, fix caller, then `ahc dlq replay` |
| Daemon hangs on submit | queue full (`queue_depth == queue_max_size`) | raise `QueueConfig.max_size` or `workers`, or shed load |
| Daemon restarts and "loses" work | nothing — recovery is automatic | check stderr for `client.recovery` log on next boot |
| Realized rate above configured | not possible without a bug | report; see `tests/test_rate_limiter.py` for invariants |

## Reading the logs

Every line is a single JSON object on stderr:

```
{"event": "request.success", "status": 200, "attempt": 0, "request_id": "...", "level": "info", "timestamp": "..."}
```

Stable `event` values:

| Event | Level | When |
| --- | --- | --- |
| `request.submitted` | info | caller invoked `request()` |
| `request.dedup_attached` | debug | duplicate caller hooked onto an existing future |
| `request.success` | info | 2xx/3xx response, future resolved |
| `request.retry_scheduled` | info | retryable failure, re-enqueue after `delay_seconds` |
| `request.dead_letter` | warning | terminal failure, row written to DLQ |
| `breaker.short_circuit` | warning | breaker open or half-open slot full, request rejected |
| `breaker.opened` (implicit) | — | inferred from `breaker_state` in metrics |
| `ratelimit.waited` | debug | non-zero rate-limiter delay |
| `client.recovery` | info | inflight rows replayed at startup |
| `dlq.replayed` / `dlq.replay_failed` / `dlq.purged` | info/warning | CLI operations |

Filter with `jq`:

```bash
launchctl ... 2>&1 | jq -c 'select(.event == "request.dead_letter")'
```

## DLQ workflow

### Inspect

```bash
ahc dlq list --db ahc_state.db
```

Output is a JSON array, newest first:

```json
[
  {
    "id": 7,
    "request_id": "abc123...",
    "method": "GET",
    "url": "https://api.example.com/v1/widgets",
    "error": "HTTP 502",
    "retry_count": 4,
    "first_seen": "2026-04-28T10:14:01Z",
    "last_attempt": "2026-04-28T10:14:32Z"
  }
]
```

### Replay

After confirming the upstream has recovered (or your code change is
deployed):

```bash
# Replay everything
ahc dlq replay --db ahc_state.db

# Replay one row
ahc dlq replay --db ahc_state.db --id 7
```

The CLI exits 0 if every replay succeeded, 2 if some are still failing.
Failed replays *stay* in the DLQ — the row is updated in place with the
new error reason and timestamp, so the DLQ never grows duplicates for
the same idempotency key.

### Purge

Operator escape hatch. Use only when the DLQ entries are known to be
irrelevant (e.g. the API contract changed and old payloads will never
succeed again):

```bash
ahc dlq purge --db ahc_state.db --yes
```

The `--yes` flag is mandatory; without it the command refuses. Always
look at `ahc dlq list` first.

## Crash recovery

There is no manual procedure — recovery runs automatically on every
`AsyncHardenedClient.start()`. Verify it worked by looking for this on
boot:

```
{"event": "client.recovery", "recovered": <n>, "level": "info", ...}
```

`n > 0` means rows were replayed; the workers will drain them within
milliseconds of starting. If `n == 0` either there was no orphaned work
or `ahc_state.db` was deleted between runs.

If the daemon crashes repeatedly and the inflight table grows
unboundedly, the most likely culprit is a bug *outside* this library
(e.g. an exception in your `response_hook` or in code that wraps
`request()`). Inspect:

```bash
ahc metrics --db ahc_state.db
# look at "inflight_persisted" — should be ~0 in steady state
```

## Shutdown

`async with AsyncHardenedClient(...)` calls `stop()` on exit, which:

1. Sets the stopping event so newly-rescheduled retries don't fire.
2. Calls `drain()` — waits for the queue and every pending retry-sleep
   task to settle.
3. Cancels workers and closes the aiohttp session.
4. Closes the SQLite connection.

Steps 1 and 2 give the "no dropped requests on graceful exit" guarantee.
Hard `kill -9` skips this entirely; that's the path crash recovery
covers.

## Tuning

`ClientConfig` knobs and when to change them:

| Field | Default | Raise when | Lower when |
| --- | --- | --- | --- |
| `rate_limit.rate_per_second` | 10.0 | upstream allows more | seeing 429s after retries |
| `rate_limit.burst` | 20 | bursty workloads tolerate it | upstream is strict |
| `circuit_breaker.failure_threshold` | 5 | upstream is noisy but recovers | want faster fail-fast |
| `circuit_breaker.open_seconds` | 30.0 | upstream takes longer to recover | upstream recovers fast |
| `retry.max_retries` | 5 | high-stakes work | latency-sensitive |
| `retry.max_delay` | 30.0 | OK to wait longer | tight SLA |
| `queue.max_size` | 1000 | bursty submitters | want stricter backpressure |
| `queue.workers` | 16 | upstream can handle parallelism | upstream throttles per-connection |

After any change, run `pytest -q` against the mock server before
deploying — the tests assert behavior under failure storms and rate
limits, not just happy paths.

## Adding per-host overrides

```python
from async_hardened_client.config import RateLimitConfig

async with AsyncHardenedClient(config) as client:
    client._rate_limiter.set_override(
        "api.sam.gov", RateLimitConfig(rate_per_second=5.0, burst=10)
    )
```

(Promoted to a public method in the next iteration; `_rate_limiter` is
intentionally accessible today.)

## Reading metrics

```bash
ahc metrics --db ahc_state.db
```

Output:

```json
{
  "requests_started": 0,
  "requests_succeeded": 0,
  "requests_failed_retryable": 0,
  "requests_failed_terminal": 0,
  "retries_performed": 0,
  "dedup_hits": 0,
  "dlq_pushes": 0,
  "rate_limit_waits": 0,
  "rate_limit_wait_seconds_total": 0.0,
  "breaker_short_circuits": 0,
  "breaker_state": {},
  "queue_depth": 0,
  "queue_max_size": 1000,
  "success_rate": 0.0,
  "inflight_persisted": 0,
  "dlq_persisted": 0
}
```

Things to watch:

- `success_rate` < 0.95 sustained → upstream degraded, retry budget
  thin, or breaker thrashing.
- `dlq_persisted` > 0 → operator action needed (inspect, fix, replay).
- `queue_depth` near `queue_max_size` → submitters are outpacing
  workers; consider raising worker count or sharding by host.
- `breaker_state` map — any host stuck in `open` for long stretches
  means that upstream is genuinely broken; page if it's load-bearing.
