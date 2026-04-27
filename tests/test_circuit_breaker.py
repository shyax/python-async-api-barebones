"""State-machine tests for the circuit breaker."""

from __future__ import annotations

import pytest

from async_hardened_client.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    HostCircuitBreakers,
)
from async_hardened_client.config import CircuitBreakerConfig
from async_hardened_client.errors import CircuitOpenError


class FakeClock:
    """Deterministic monotonic time for the breaker tests."""

    def __init__(self):
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def make_breaker(**overrides):
    cfg = CircuitBreakerConfig(
        failure_threshold=overrides.pop("failure_threshold", 3),
        failure_rate=overrides.pop("failure_rate", 0.5),
        rolling_window=overrides.pop("rolling_window", 10),
        open_seconds=overrides.pop("open_seconds", 5.0),
        half_open_max_probes=overrides.pop("half_open_max_probes", 1),
    )
    clock = FakeClock()
    return CircuitBreaker("host", cfg, time_func=clock), clock


async def test_starts_closed():
    breaker, _ = make_breaker()
    assert breaker.state == CircuitState.CLOSED
    await breaker.before_call()  # closed: passes


async def test_opens_after_consecutive_failures():
    breaker, _ = make_breaker(failure_threshold=3)

    for _ in range(2):
        await breaker.before_call()
        await breaker.on_failure()
    assert breaker.state == CircuitState.CLOSED

    await breaker.before_call()
    await breaker.on_failure()
    assert breaker.state == CircuitState.OPEN


async def test_open_blocks_calls_with_retry_after():
    breaker, clock = make_breaker(failure_threshold=1, open_seconds=10.0)
    await breaker.before_call()
    await breaker.on_failure()
    assert breaker.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError) as exc_info:
        await breaker.before_call()
    assert exc_info.value.retry_after == pytest.approx(10.0, abs=0.01)
    clock.advance(3.0)
    with pytest.raises(CircuitOpenError) as exc_info:
        await breaker.before_call()
    assert exc_info.value.retry_after == pytest.approx(7.0, abs=0.01)


async def test_transitions_to_half_open_after_cooldown():
    breaker, clock = make_breaker(failure_threshold=1, open_seconds=5.0)
    await breaker.before_call()
    await breaker.on_failure()
    clock.advance(5.0)

    await breaker.before_call()  # consumes the probe slot
    assert breaker.state == CircuitState.HALF_OPEN


async def test_half_open_success_closes_breaker():
    breaker, clock = make_breaker(failure_threshold=1, open_seconds=5.0)
    await breaker.before_call()
    await breaker.on_failure()
    clock.advance(5.0)

    await breaker.before_call()
    await breaker.on_success()
    assert breaker.state == CircuitState.CLOSED
    # Closed breaker accepts new traffic immediately.
    await breaker.before_call()


async def test_half_open_failure_reopens_breaker():
    breaker, clock = make_breaker(failure_threshold=1, open_seconds=5.0)
    await breaker.before_call()
    await breaker.on_failure()
    clock.advance(5.0)

    await breaker.before_call()
    await breaker.on_failure()
    assert breaker.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        await breaker.before_call()


async def test_half_open_caps_concurrent_probes():
    breaker, clock = make_breaker(
        failure_threshold=1, open_seconds=5.0, half_open_max_probes=1
    )
    await breaker.before_call()
    await breaker.on_failure()
    clock.advance(5.0)

    await breaker.before_call()  # probe 1
    with pytest.raises(CircuitOpenError):
        await breaker.before_call()  # probe slot full
    assert breaker.state == CircuitState.HALF_OPEN


async def test_failure_rate_trigger_with_full_window():
    breaker, _ = make_breaker(
        failure_threshold=999,  # disable consecutive trigger
        failure_rate=0.5,
        rolling_window=4,
    )
    # 2 successes, 2 failures = 50% rate, exactly at threshold.
    await breaker.before_call(); await breaker.on_success()
    await breaker.before_call(); await breaker.on_success()
    await breaker.before_call(); await breaker.on_failure()
    assert breaker.state == CircuitState.CLOSED  # window not yet full of mixed signal
    await breaker.before_call(); await breaker.on_failure()
    assert breaker.state == CircuitState.OPEN


async def test_partial_window_does_not_trigger_rate():
    """Until the rolling window fills, the failure-rate check stays quiet —
    otherwise a single early failure would open the breaker on a brand-new
    host."""
    breaker, _ = make_breaker(
        failure_threshold=999, failure_rate=0.1, rolling_window=20
    )
    await breaker.before_call(); await breaker.on_failure()
    await breaker.before_call(); await breaker.on_failure()
    assert breaker.state == CircuitState.CLOSED


async def test_host_registry_isolates_breakers_per_host():
    registry = HostCircuitBreakers(
        CircuitBreakerConfig(failure_threshold=1, open_seconds=5.0)
    )
    a = await registry.for_url("https://api-a.example.com/v1/x")
    b = await registry.for_url("https://api-b.example.com/v1/x")

    await a.before_call(); await a.on_failure()
    assert a.state == CircuitState.OPEN
    assert b.state == CircuitState.CLOSED  # unaffected
    await b.before_call()  # still flows
