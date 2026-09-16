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


def test_half_open_probe_that_never_reports_does_not_deadlock() -> None:
    """A probe that goes out (allow_request() -> True during half-open) but
    whose caller never calls record() -- e.g. it crashed, or hit one of
    process_item's abort/cancel early returns before reaching the provider
    call -- must not wedge the breaker open-forever. Without probe_timeout_s,
    `_probe_in_flight` stays True permanently and allow_request() returns
    False on every subsequent call, hanging the job (§ defect 2).
    """
    clock = {"t": 0.0}
    breaker = CircuitBreaker(
        window=20, threshold=0.5, cooldown_s=30.0, probe_timeout_s=10.0, clock=lambda: clock["t"]
    )
    for _ in range(15):
        breaker.record(is_failure=True)
    assert breaker.state == "open"

    clock["t"] = 30.0
    assert breaker.allow_request()  # issues the probe; _probe_in_flight = True
    assert breaker.state == "half_open"
    assert not breaker.allow_request()  # still in flight, not yet stale -- no second probe

    # The probe never calls record(). `_opened_at` is the original
    # closed->open transition time (0.0 here, since the clock never
    # advanced during the failure-recording loop above), so the probe is
    # considered stale once `cooldown_s + probe_timeout_s` (= 40.0) has
    # elapsed since then -- i.e. `probe_timeout_s` (10.0) after half-open
    # began at t=30.0. Just before that, the breaker must still refuse
    # (otherwise the timeout isn't doing anything).
    clock["t"] = 39.9
    assert not breaker.allow_request()

    # Once the full cooldown_s + probe_timeout_s budget has elapsed, the
    # stale probe must be cleared and a fresh one allowed -- this is the
    # line that prevents the permanent hang.
    clock["t"] = 40.1
    assert breaker.allow_request(), "a stale probe must not deadlock allow_request() forever"


def test_circuit_breaker_outcomes_bounded_by_window() -> None:
    """`_outcomes` is a deque(maxlen=window) -- confirms it self-trims
    instead of growing unboundedly over a long-running job.
    """
    breaker = CircuitBreaker(window=5, threshold=0.99)
    for _ in range(100):
        breaker.record(is_failure=False)
    assert len(breaker._outcomes) == 5
