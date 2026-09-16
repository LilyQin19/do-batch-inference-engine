from __future__ import annotations

import random

from batchengine.core.retry import CircuitBreaker, RetryBudget, full_jitter_delay


def test_full_jitter_bounds() -> None:
    rng = random.Random(0)
    for attempt in range(10):
        delay = full_jitter_delay(attempt, base=0.5, cap=30.0, rng=rng)
        assert 0.0 <= delay <= min(30.0, 0.5 * (2**attempt))


def test_full_jitter_respects_cap() -> None:
    rng = random.Random(0)
    delay = full_jitter_delay(20, base=0.5, cap=30.0, rng=rng)
    assert delay <= 30.0


def test_full_jitter_decorrelates_cohort() -> None:
    """Plain exponential backoff keeps a cohort's retries synchronized on
    every wave; full jitter should spread them. Sampling many independent
    rngs (one per "worker") at the same attempt number should produce a
    wide spread of delays, not a single repeated value.
    """
    delays = [full_jitter_delay(3, rng=random.Random(seed)) for seed in range(50)]
    assert len(set(round(d, 4) for d in delays)) > 10


def test_retry_budget_caps_at_fraction() -> None:
    budget = RetryBudget(total_items=100, fraction=0.20)
    assert budget.limit == 20
    for _ in range(20):
        assert budget.try_consume()
    assert not budget.try_consume()
    assert budget.remaining == 0


def test_retry_budget_grows_as_total_items_grows() -> None:
    """The scheduler grows total_items as streaming ingest discovers more
    rows; the budget limit should track that live."""
    budget = RetryBudget(total_items=10, fraction=0.5)
    assert budget.limit == 5
    budget.total_items = 40
    assert budget.limit == 20


def test_circuit_breaker_opens_after_majority_failures() -> None:
    clock = {"t": 0.0}
    breaker = CircuitBreaker(window=20, threshold=0.5, cooldown_s=30.0, clock=lambda: clock["t"])
    for _ in range(11):
        breaker.record(is_failure=True)
    for _ in range(9):
        breaker.record(is_failure=False)
    assert breaker.state == "open"
    assert not breaker.allow_request()


def test_circuit_breaker_half_open_then_closes_on_success() -> None:
    clock = {"t": 0.0}
    breaker = CircuitBreaker(window=20, threshold=0.5, cooldown_s=30.0, clock=lambda: clock["t"])
    for _ in range(15):
        breaker.record(is_failure=True)
    assert breaker.state == "open"

    clock["t"] = 30.0
    assert breaker.allow_request()  # transitions to half_open, allows one probe
    assert breaker.state == "half_open"
    assert not breaker.allow_request()  # no second concurrent probe

    breaker.record(is_failure=False)
    assert breaker.state == "closed"
    assert breaker.allow_request()


def test_circuit_breaker_half_open_reopens_on_probe_failure() -> None:
    clock = {"t": 0.0}
    breaker = CircuitBreaker(window=20, threshold=0.5, cooldown_s=30.0, clock=lambda: clock["t"])
    for _ in range(15):
        breaker.record(is_failure=True)
    clock["t"] = 30.0
    assert breaker.allow_request()
    breaker.record(is_failure=True)
    assert breaker.state == "open"
    assert not breaker.allow_request()
