# Architecture

## Goals

The client is a single in-process resilience layer for outbound HTTP
calls. It targets a deployment shape that's common in autonomous Python
automation: one or more long-running `asyncio` agents on a single host,
managed by `launchd` or `systemd`, calling rate-limited public APIs that
fail intermittently. The five hardening domains from the SOW map to five
components:

| Domain | Component |
| --- | --- |
| Async architecture | `client.py` (`AsyncHardenedClient`) |
| Rate limiting + backoff | `rate_limiter.py`, `retry.py`, `circuit_breaker.py` |
| Request queue + dedup | `queue.py`, `dedup.py` |
| Crash recovery | `storage.py` + `client._recover_inflight()` |
| Error standardization | `errors.py` + `client._handle_retry_or_dlq()` |

## Component diagram

```
                        ┌──────────────────────────┐
       caller ─────────▶│   AsyncHardenedClient    │
                        │       .request()         │
                        └────┬─────────────────────┘
                             │
                             ▼
                  ┌─────────────────────────┐
                  │   InFlightRegistry      │  process-local dedup
                  │   key: idempotency_key  │  (sha256 of method+url+
                  │   value: shared Future  │   params+body)
                  └─────────┬───────────────┘
                  duplicate │     │ owner
                            ▼     ▼
                          await  ┌──────────────────┐
                          shared │ Storage          │  SQLite, WAL
                          Future │ inflight upsert  │  recovery point
                                 └────────┬─────────┘
                                          ▼
                                 ┌──────────────────┐
                                 │ RequestQueue     │  asyncio.Queue
                                 │ (bounded)        │  backpressure here
                                 └────────┬─────────┘
                                          ▼
            ┌──────────────────────── workers (N) ────────────────────────┐
            │                                                              │
            ▼                                                              ▼
   ┌─────────────────┐                                          ┌──────────────────┐
   │ CircuitBreaker  │ — OPEN → CircuitOpenError → retry path   │ ...              │
   │ before_call()   │                                          │                  │
   └────────┬────────┘                                          └──────────────────┘
            ▼
   ┌─────────────────┐
   │ RateLimiter     │ — token bucket per host
   │ acquire()       │   delays caller, never drops
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ aiohttp session │ — connection pooled, TTL'd DNS cache
   │ .request()      │
   └────────┬────────┘
            ▼
        classify
   ┌────────┴────────┬─────────────────┐
   ▼                 ▼                 ▼
 success         retryable         non-retryable / exhausted
   │                 │                 │
   ▼                 ▼                 ▼
on_success       backoff          Storage.push_dead_letter
delete inflight  Storage.increment_retry      ↓
resolve future   re-enqueue after delay    fail future with DeadLetterError
```

## The request lifecycle in detail

### 1. Submission (caller coroutine)

`request()` builds an immutable `Request`, computes its `idempotency_key`
(SHA-256 over canonicalized method, URL, params, body), and consults the
`InFlightRegistry`:

- If a future already exists for this key, the caller becomes a *waiter*
  and simply awaits it. The dedup hits counter increments. No HTTP call,
  no queue entry, no storage row.
- Otherwise, the caller is the *owner* of a fresh future, persists the
  request to `inflight_requests`, and enqueues a `QueueItem`.

The caller's `await` resolves on the same future the worker will eventually
resolve.

### 2. Worker execution

A worker pulls a `QueueItem` and runs:

1. `breaker.before_call()` — raises `CircuitOpenError` if the breaker is
   OPEN (or the HALF_OPEN probe slot is full). The caller does *not* see
   this directly; the orchestrator routes it back into the retry path.
2. `rate_limiter.acquire()` — awaits until a token is available. If the
   wait is non-zero, the rate-limit-wait metric increments.
3. `aiohttp.session.request()` — single shared session, single shared
   connector. Body-parsing returns `(data, headers)`.

### 3. Outcome classification

Statuses 2xx/3xx → success. Statuses 5xx and 429 → `RetryableError`. Other
4xx → `NonRetryableError`. Network/timeout exceptions → either retryable
(via `is_retryable_exception`) or wrapped non-retryable.

### 4. Success path

- `breaker.on_success()` — closes the breaker if it was HALF_OPEN.
- `Storage.delete_inflight()` — drops the persistence row.
- `dedup.resolve()` — wakes every caller waiting on this key with the
  same `Response` object.
- Optional `response_hook` is invoked (errors swallowed and logged).

### 5. Retry path

- `breaker.on_failure()` — *unless* the failure is HTTP 429, which we
  treat as healthy backpressure rather than a fault.
- If `attempt < max_retries`, compute `backoff_seconds(...)` (with jitter
  and Retry-After respected) and schedule re-enqueue via
  `asyncio.create_task` after the delay. The reschedule task is tracked
  so `drain()`/`stop()` can wait for it.
- `Storage.increment_retry()` — bumps the retry_count and records the
  last error reason.

### 6. Terminal failure path

- `Storage.push_dead_letter()` — writes a row to `dead_letter_queue`
  with the full payload, the error reason, and the attempt count.
- `Storage.delete_inflight()` — removes the inflight row (the work has
  exited the active set).
- `dedup.fail()` — every caller waiting on this key receives a
  `DeadLetterError` (or the original `NonRetryableError`).

## Crash recovery

`AsyncHardenedClient.start()` calls `_recover_inflight()` *before* spawning
the worker pool. It reads every row in `inflight_requests` and re-enqueues
each one with its persisted `retry_count`. Workers pick them up the moment
they come online, so recovered work begins draining within milliseconds of
process start.

Three properties make this safe:

1. **Persist before enqueue.** A request is in `inflight_requests` before
   it ever enters the in-process queue. A crash between insert and
   enqueue still produces a recovery row.
2. **Idempotency keys collapse duplicates.** If a recovered request is
   submitted again by a freshly-restarted agent, the dedup registry hands
   the second submitter the same future the worker is already racing to
   resolve.
3. **Successful completion deletes the row.** Recovery cannot re-execute
   a request that already succeeded — its row is gone.

## Why per-host

Rate limits and circuit-breaker state are per-API. Saturating SAM.gov
must not throttle USASpending.gov, and a SAM.gov outage should not open
the breaker for USASpending.gov. Both registries (`HostRateLimiter`,
`HostCircuitBreakers`) lazily build a bucket/breaker the first time a
URL with that netloc is requested. Per-host overrides can be installed
at runtime for hosts with non-default budgets.

## Why full-jitter

A fleet of clients that fail in lock-step will retry in lock-step too
unless their backoff is randomized. The AWS Architecture Blog's "full
jitter" formula — `random.uniform(0, min(cap, base * 2**attempt))` — is
the canonical fix. The cap is applied before the random draw so jitter
cannot amplify already-capped delays.

## Why bound the queue

An unbounded queue under sustained overload is a memory leak by another
name. The bounded `asyncio.Queue` propagates pressure back to the caller:
`request()` blocks on `enqueue()` once the queue is full. This is the
contract that makes the client safe to embed in a long-running daemon.

## What's deliberately out of scope

- **Distributed coordination.** The client is single-process. A
  redis-backed queue and rate limiter are listed in the PRD as a future
  extension; the storage layer is shaped to make that swap a localized
  change.
- **Authentication.** Header injection is a caller responsibility — pass
  any `Authorization`/`X-Api-Key` headers via `headers=`.
- **Streaming responses.** The client reads the full body before
  resolving the future. APIs that return long-lived streams should use
  `aiohttp` directly.
- **Synchronous facade.** Pure async. No `run_until_complete` wrapper.
