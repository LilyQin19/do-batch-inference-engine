from __future__ import annotations

import random

import pytest

from batchengine.core.ratelimit import AdaptiveController, TokenBucket


class FakeClock:
    """A controllable monotonic clock so backoff/refill tests run instantly
    instead of depending on wall-clock sleeps (§7 requirement)."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class FakeSleeper:
    """Replaces asyncio.sleep: instead of actually waiting, advances the
    fake clock by the requested amount and returns immediately."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.total_slept = 0.0

    async def __call__(self, seconds: float) -> None:
        self.total_slept += seconds
        self.clock.advance(seconds)


def test_bucket_refills_over_time() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, capacity=10.0, clock=clock)
    bucket._tokens = 0.0
    clock.advance(0.5)
    bucket._refill()
    assert bucket._tokens == pytest.approx(5.0)


def test_bucket_never_exceeds_capacity() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, capacity=5.0, clock=clock)
    clock.advance(100.0)
    bucket._refill()
    assert bucket._tokens == 5.0


@pytest.mark.asyncio
async def test_bucket_acquire_waits_when_empty() -> None:
    clock = FakeClock()
    sleeper = FakeSleeper(clock)
    bucket = TokenBucket(rate=2.0, capacity=1.0, clock=clock)
    await bucket.acquire(sleep=sleeper)  # consumes the initial token
    await bucket.acquire(sleep=sleeper)  # must wait for refill
    assert sleeper.total_slept > 0


def test_aimd_decrease_on_throttle() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, capacity=10.0, clock=clock)
    controller = AdaptiveController(bucket, min_rate=1.0, max_rate=20.0, clock=clock)
    controller.record_throttle()
    assert bucket.rate == pytest.approx(7.5)


def test_aimd_increase_after_success_streak() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, capacity=10.0, clock=clock)
    controller = AdaptiveController(
        bucket, min_rate=1.0, max_rate=20.0, clock=clock, success_streak_for_increase=5
    )
    for _ in range(5):
        controller.record_success(latency_s=1.0)
    assert bucket.rate == pytest.approx(10.5)


def test_cooldown_prevents_multiplicative_collapse_under_concurrent_429s() -> None:
    """The single most important AIMD bug this system must not have: a burst
    of N concurrent 429s (e.g. 20 workers all hitting one momentary overage)
    must not divide the rate by 0.75**N. Without a cooldown window, 20
    concurrent throttles would collapse a rate of 10 down to ~0.03 -- this
    test pins the cooldown behavior that prevents that collapse.
    """
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, capacity=10.0, clock=clock)
    controller = AdaptiveController(
        bucket, min_rate=0.1, max_rate=20.0, clock=clock, cooldown_s=5.0
    )

    for _ in range(20):
        controller.record_throttle()  # all "simultaneous" -- clock doesn't advance

    # Only the first throttle in the cooldown window should have taken effect.
    assert bucket.rate == pytest.approx(7.5)
    assert controller.throttle_events_429 == 20

    clock.advance(5.0)
    controller.record_throttle()
    assert bucket.rate == pytest.approx(7.5 * 0.75)


def test_rate_never_drops_below_min() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=1.0, capacity=1.0, clock=clock)
    controller = AdaptiveController(
        bucket, min_rate=0.5, max_rate=20.0, clock=clock, cooldown_s=0.0
    )
    for _ in range(10):
        clock.advance(1.0)
        controller.record_throttle()
    assert bucket.rate >= 0.5


def test_rate_never_exceeds_max() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=19.9, capacity=20.0, clock=clock)
    controller = AdaptiveController(
        bucket, min_rate=1.0, max_rate=20.0, clock=clock, success_streak_for_increase=1
    )
    for _ in range(10):
        controller.record_success(latency_s=1.0)
    assert bucket.rate <= 20.0


@pytest.mark.asyncio
async def test_reset_header_pause_then_jitter_resume() -> None:
    clock = FakeClock()
    epoch = {"now": 1000.0}

    def now_epoch() -> float:
        return epoch["now"]

    sleeper = FakeSleeper(clock)
    bucket = TokenBucket(rate=10.0, capacity=10.0, clock=clock)
    controller = AdaptiveController(
        bucket, min_rate=1.0, max_rate=20.0, clock=clock, sleep=sleeper, rng=random.Random(1)
    )

    async def advancing_sleep(seconds: float) -> None:
        epoch["now"] += seconds
        await sleeper(seconds)

    controller._sleep = advancing_sleep
    await controller.honor_reset_header(1010.0, now_epoch_fn=now_epoch)
    # Hard pause of 10s plus <=250ms jitter.
    assert 10.0 <= sleeper.total_slept <= 10.25


@pytest.mark.asyncio
async def test_reset_header_jitter_spreads_workers() -> None:
    """Distinct rng seeds per worker should produce distinct resume offsets
    -- proof the thundering-herd jitter actually varies across workers."""
    epoch = 1000.0

    offsets = set()
    for seed in range(5):
        sleeper = FakeSleeper(FakeClock())
        bucket = TokenBucket(rate=10.0, capacity=10.0)
        controller = AdaptiveController(
            bucket, min_rate=1.0, max_rate=20.0, sleep=sleeper, rng=random.Random(seed)
        )
        await controller.honor_reset_header(epoch, now_epoch_fn=lambda: epoch)
        offsets.add(round(sleeper.total_slept, 6))
    assert len(offsets) > 1


def test_littles_law_optimal_concurrency() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=2.0, capacity=2.0, clock=clock)
    controller = AdaptiveController(bucket, min_rate=0.1, max_rate=5.0, clock=clock)
    for _ in range(3):
        controller.record_success(latency_s=3.0)
    # L = lambda * W = 2 req/s * 3s = 6
    assert controller.littles_law_optimal_concurrency() == 6
