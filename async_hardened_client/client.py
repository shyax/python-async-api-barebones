"""AsyncHardenedClient — the orchestrator that wires every resilience
primitive into a single request lifecycle.

Lifecycle of a request, end to end:

    request()                                         (caller coroutine)
        ├─ build Request, hash idempotency_key
        ├─ InFlightRegistry.get_or_create(key)
        │     └─ if not owner: await shared future, return
        ├─ Storage.upsert_inflight()                  (recovery point)
        ├─ RequestQueue.enqueue()                     (backpressure here)
        └─ await shared future

    worker (one of N coroutines pulling from the queue)
        ├─ CircuitBreaker.before_call()               (CircuitOpenError ⇒ retry)
        ├─ RateLimiter.acquire()                      (delay until token)
        ├─ aiohttp.ClientSession.request()
        ├─ classify outcome
        │     ├─ 2xx/3xx → success: resolve future, delete inflight row
        │     ├─ retryable status / transient exc:
        │     │       ├─ if attempts left:
        │     │       │       schedule re-enqueue after backoff,
        │     │       │       increment storage retry_count
        │     │       └─ else: push to DLQ, fail future
        │     └─ non-retryable: push to DLQ, fail future
        └─ task_done()

The session is a single, long-lived `aiohttp.ClientSession` with a shared
TCP connector — connection pooling is automatic, no per-request session
construction. Everything runs inside one event loop; no threads, no
synchronous calls.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

import aiohttp

from async_hardened_client.circuit_breaker import HostCircuitBreakers
from async_hardened_client.config import ClientConfig
from async_hardened_client.dedup import InFlightRegistry
from async_hardened_client.errors import (
    CircuitOpenError,
    DeadLetterError,
    NonRetryableError,
    RetryableError,
    is_retryable_exception,
    is_retryable_status,
)
from async_hardened_client.models import Request, Response
from async_hardened_client.observability import Metrics, get_logger
from async_hardened_client.queue import QueueItem, RequestQueue
from async_hardened_client.rate_limiter import HostRateLimiter
from async_hardened_client.retry import (
    backoff_seconds,
    parse_retry_after,
    should_retry,
)
from async_hardened_client.storage import Storage

log = get_logger()


# ---- Body parsing helpers --------------------------------------------------

async def _read_response(resp: aiohttp.ClientResponse) -> tuple[Any, dict[str, str]]:
    """Read the body once; if it parses as JSON, return the parsed object,
    otherwise return the raw text. Headers are returned alongside as a
    plain dict for easy persistence."""
    raw = await resp.read()
    text = raw.decode("utf-8", errors="replace") if raw else ""
    try:
        data: Any = json.loads(text) if text else None
    except json.JSONDecodeError:
        data = text
    return data, dict(resp.headers)


# ---- Client ----------------------------------------------------------------

ResponseHook = Callable[[Request, Response], Awaitable[None]]


class AsyncHardenedClient:
    """Single-process async HTTP client with the full resilience stack.

    Use as an async context manager so the underlying session, queue, and
    storage are torn down in the right order:

        async with AsyncHardenedClient(config) as client:
            response = await client.request("GET", "https://api.example.com/v1/x")

    The constructor never starts a session; `start()` (or `__aenter__`) is
    the actual init point. `stop()` drains pending work and closes the
    session.
    """

    def __init__(self, config: ClientConfig | None = None):
        self._cfg = config or ClientConfig()
        self._session: aiohttp.ClientSession | None = None
        self._storage = Storage(self._cfg.db_path)
        self._queue = RequestQueue(max_size=self._cfg.queue.max_size)
        self._dedup: InFlightRegistry[Response] = InFlightRegistry()
        self._rate_limiter = HostRateLimiter(self._cfg.rate_limit)
        self._breakers = HostCircuitBreakers(self._cfg.circuit_breaker)
        self._workers: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()
        self._metrics = Metrics(queue_max_size=self._cfg.queue.max_size)
        self._response_hook: ResponseHook | None = None

    # ----- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        if self._session is not None:
            return
        await self._storage.open()
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=self._cfg.request_timeout)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": self._cfg.user_agent},
        )
        # Recovery: re-enqueue anything left behind by a previous run before
        # workers come online so they pick it up immediately.
        recovered = await self._recover_inflight()
        if recovered:
            log.info("client.recovery", recovered=recovered)
        self._stopping.clear()
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"ahc-worker-{i}")
            for i in range(self._cfg.queue.workers)
        ]

    async def stop(self) -> None:
        if self._session is None:
            return
        self._stopping.set()
        # Drain queued work before shutting down — this is what gives us
        # the "no dropped requests" guarantee on graceful exit.
        await self._queue.join()
        for w in self._workers:
            w.cancel()
        for w in self._workers:
            try:
                await w
            except (asyncio.CancelledError, Exception):
                pass
        self._workers.clear()
        await self._session.close()
        self._session = None
        await self._storage.close()

    async def __aenter__(self) -> AsyncHardenedClient:
        await self.start()
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.stop()

    # ----- public API -----------------------------------------------------

    def set_response_hook(self, hook: ResponseHook | None) -> None:
        """Optional async callback invoked after every successful response
        — useful for tests and bespoke instrumentation. Errors raised inside
        the hook are logged and swallowed so they cannot poison normal flow.
        """
        self._response_hook = hook

    def metrics(self) -> dict[str, Any]:
        self._metrics.queue_depth = self._queue.depth
        self._metrics.dedup_hits = self._dedup.dedup_hits
        self._metrics.breaker_state = {
            host: state.value for host, state in self._breakers.snapshot().items()
        }
        return self._metrics.snapshot()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: Any = None,
        priority: int = 0,
    ) -> Response:
        """Submit a request. Awaits the eventual Response or raises a
        terminal error (DeadLetterError / NonRetryableError).
        """
        if self._session is None:
            raise RuntimeError("client not started — use `async with` or call .start()")
        req = Request(
            method=method.upper(),
            url=url,
            params=params or {},
            headers=headers or {},
            body=body,
            priority=priority,
        )
        return await self._submit(req)

    # ----- internals ------------------------------------------------------

    async def _submit(self, req: Request) -> Response:
        future, owner = await self._dedup.get_or_create(req.idempotency_key)
        if not owner:
            log.debug("request.dedup_attached", request_id=req.request_id, key=req.idempotency_key[:12])
            return await future

        # Persist before enqueue. If the process crashes between enqueue and
        # successful execution, recovery will find this row and re-submit.
        await self._storage.upsert_inflight(req)
        self._metrics.requests_started += 1
        log.info("request.submitted", request_id=req.request_id, method=req.method, url=req.url)
        await self._queue.enqueue(QueueItem(req, attempt=0))
        return await future

    async def _recover_inflight(self) -> int:
        """Re-enqueue any rows left in inflight from a prior run."""
        rows = await self._storage.list_inflight()
        for row in rows:
            # Recovered items get a fresh dedup future so concurrent
            # callers can attach if they replay the same logical request.
            future, owner = await self._dedup.get_or_create(row.idempotency_key)
            if owner:
                self._metrics.requests_started += 1
                await self._queue.enqueue(
                    QueueItem(row.request, attempt=row.retry_count)
                )
            # If owner is False, another in-process caller already owns it,
            # which means this is the same loop's attempt to recover state
            # it just wrote. The owning path already enqueued; do nothing.
            _ = future
        return len(rows)

    async def _worker(self, worker_id: int) -> None:
        log.debug("worker.start", worker_id=worker_id)
        try:
            while True:
                item = await self._queue.dequeue()
                try:
                    await self._handle(item)
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            log.debug("worker.cancelled", worker_id=worker_id)
            raise

    async def _handle(self, item: QueueItem) -> None:
        req = item.request
        breaker = await self._breakers.for_url(req.url)
        try:
            await breaker.before_call()
        except CircuitOpenError as e:
            self._metrics.breaker_short_circuits += 1
            log.warning(
                "breaker.short_circuit",
                request_id=req.request_id,
                host=e.host,
                retry_after=round(e.retry_after, 2),
                attempt=item.attempt,
            )
            await self._handle_retry_or_dlq(item, e, retry_after=e.retry_after)
            return

        wait = await self._rate_limiter.acquire(req.url)
        if wait > 0:
            self._metrics.rate_limit_waits += 1
            self._metrics.rate_limit_wait_seconds_total += wait
            log.debug(
                "ratelimit.waited",
                request_id=req.request_id,
                host=self._rate_limiter.host_of(req.url),
                wait_seconds=round(wait, 3),
            )

        try:
            response = await self._execute(req, item.attempt)
        except (RetryableError, NonRetryableError) as exc:
            await breaker.on_failure()
            await self._handle_retry_or_dlq(item, exc)
            return
        except Exception as exc:  # transport-level transient
            if is_retryable_exception(exc):
                await breaker.on_failure()
                wrapped = RetryableError(str(exc) or exc.__class__.__name__)
                await self._handle_retry_or_dlq(item, wrapped)
                return
            await breaker.on_failure()
            wrapped_nr = NonRetryableError(str(exc) or exc.__class__.__name__)
            await self._handle_retry_or_dlq(item, wrapped_nr)
            return

        await breaker.on_success()
        self._metrics.requests_succeeded += 1
        await self._storage.delete_inflight(req.idempotency_key)
        log.info(
            "request.success",
            request_id=req.request_id,
            status=response.status_code,
            attempt=item.attempt,
        )
        if self._response_hook is not None:
            try:
                await self._response_hook(req, response)
            except Exception as exc:
                log.error("response_hook.error", request_id=req.request_id, error=str(exc))
        await self._dedup.resolve(req.idempotency_key, response)

    async def _execute(self, req: Request, attempt: int) -> Response:
        assert self._session is not None
        async with self._session.request(
            method=req.method,
            url=req.url,
            params=req.params or None,
            headers=req.headers or None,
            json=req.body if isinstance(req.body, (dict, list)) else None,
            data=req.body if isinstance(req.body, (str, bytes)) else None,
        ) as resp:
            data, headers = await _read_response(resp)
            if 200 <= resp.status < 400:
                return Response(
                    status_code=resp.status,
                    data=data,
                    headers=headers,
                    retries=attempt,
                    request_id=req.request_id,
                )

            retry_after = parse_retry_after(headers.get("Retry-After"))
            message = f"HTTP {resp.status}"
            if is_retryable_status(resp.status):
                raise RetryableError(message, status_code=resp.status, retry_after=retry_after)
            raise NonRetryableError(message, status_code=resp.status, body=data)

    async def _handle_retry_or_dlq(
        self,
        item: QueueItem,
        exc: BaseException,
        *,
        retry_after: float | None = None,
    ) -> None:
        req = item.request
        retryable = isinstance(exc, RetryableError) or isinstance(exc, CircuitOpenError)
        if retryable and should_retry(self._cfg.retry, item.attempt):
            self._metrics.requests_failed_retryable += 1
            self._metrics.retries_performed += 1
            new_attempt = item.attempt + 1
            inferred_retry_after = (
                retry_after
                if retry_after is not None
                else getattr(exc, "retry_after", None)
            )
            delay = backoff_seconds(
                self._cfg.retry, item.attempt, retry_after=inferred_retry_after
            )
            await self._storage.increment_retry(req.idempotency_key, last_error=str(exc))
            log.info(
                "request.retry_scheduled",
                request_id=req.request_id,
                attempt=new_attempt,
                delay_seconds=round(delay, 3),
                error=str(exc),
            )
            asyncio.create_task(self._reschedule(item, new_attempt, delay))
            return

        # Terminal: dead-letter and fail the future.
        self._metrics.requests_failed_terminal += 1
        self._metrics.dlq_pushes += 1
        dlq_id = await self._storage.push_dead_letter(
            req, error=str(exc), retry_count=item.attempt
        )
        await self._storage.delete_inflight(req.idempotency_key)
        log.warning(
            "request.dead_letter",
            request_id=req.request_id,
            dlq_id=dlq_id,
            attempts=item.attempt,
            error=str(exc),
        )
        terminal: BaseException = exc if isinstance(exc, NonRetryableError) else DeadLetterError(str(exc))
        await self._dedup.fail(req.idempotency_key, terminal)

    async def _reschedule(self, item: QueueItem, new_attempt: int, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        if self._stopping.is_set():
            return
        await self._queue.enqueue(QueueItem(item.request, attempt=new_attempt))


@asynccontextmanager
async def hardened_client(config: ClientConfig | None = None) -> AsyncIterator[AsyncHardenedClient]:
    """Convenience context manager for one-off scripts."""
    client = AsyncHardenedClient(config)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()
