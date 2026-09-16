"""TokenBucket (mechanism) + AIMD AdaptiveController (policy) pacing workers
against the account's shared RPM quota (§6.1).

A semaphore bounds *concurrency*; it says nothing about *rate*. Two workers
each capable of 10 req/s will happily produce 20 req/s and a 429 storm. The
token bucket is what actually paces requests/second, independent of how many
workers are running -- see docs/decisions.md for the semaphore-vs-bucket
writeup.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


class TokenBucket:
    """Classic token bucket. Uses monotonic time only -- a wall-clock jump
    (NTP step, DST, VM pause) must never cause a burst of stale tokens or a
    frozen bucket, which time.time() would risk.
    """

    def __init__(
        self, rate: float, capacity: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.rate = rate
        self.capacity = capacity
        self._clock = clock
        self._tokens = capacity
        self._last_refill = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last_refill)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    async def acquire(self, sleep: Callable[[float], Awaitable[None]] | None = None) -> None:
        """Block until one token is available, then consume it."""
        _sleep = sleep or asyncio.sleep
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                deficit = 1.0 - self._tokens
                wait_s = deficit / self.rate if self.rate > 0 else 0.05
            await _sleep(max(wait_s, 0.001))

    def set_rate(self, rate: float) -> None:
        self._refill()
        self.rate = max(rate, 1e-6)


@dataclass(slots=True)
class BackpressureSnapshot:
    current_rate_limit_rps: float
    configured_max_rps: float
    throttle_events_429: int
    retries_issued: int
    circuit_state: str
    observed_mean_latency_s: float
    littles_law_optimal_concurrency: int


class AdaptiveController:
    """AIMD wrapper around a TokenBucket, driven by observed 429s and
    rate-limit response headers (§1.3, §6.1).
    """

    def __init__(
        self,
        bucket: TokenBucket,
        min_rate: float,
        max_rate: float,
        decrease_factor: float = 0.75,
        increase_step: float = 0.5,
        success_streak_for_increase: int = 50,
        cooldown_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.bucket = bucket
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.decrease_factor = decrease_factor
        self.increase_step = increase_step
        self.success_streak_for_increase = success_streak_for_increase
        self.cooldown_s = cooldown_s
        self._clock = clock
        self._sleep = sleep or asyncio.sleep
        self._rng = rng or random.Random()

        self._consecutive_successes = 0
        self._last_decrease_at = -float("inf")
        self.throttle_events_429 = 0
        self.retries_issued = 0

        self._latencies: deque[float] = deque(maxlen=200)

    def record_success(self, latency_s: float) -> None:
        self._latencies.append(latency_s)
        self._consecutive_successes += 1
        if self._consecutive_successes >= self.success_streak_for_increase:
            self.bucket.set_rate(min(self.max_rate, self.bucket.rate + self.increase_step))
            self._consecutive_successes = 0

    def record_throttle(self) -> None:
        """Called on every 429, but the rate only actually decreases once per
        cooldown window (§6.1). Without this cooldown, a burst of N concurrent
        429s from a single momentary overage would divide the rate by
        0.75**N in one instant -- collapsing throughput far more than the
        upstream ever asked for. This is the AIMD bug most candidates miss;
        see tests/unit/test_ratelimit.py::test_cooldown_prevents_multiplicative_collapse.
        """
        self.throttle_events_429 += 1
        self._consecutive_successes = 0
        now = self._clock()
        if now - self._last_decrease_at >= self.cooldown_s:
            self.bucket.set_rate(max(self.min_rate, self.bucket.rate * self.decrease_factor))
            self._last_decrease_at = now

    def record_retry_issued(self) -> None:
        self.retries_issued += 1

    async def honor_reset_header(
        self, reset_epoch: float, now_epoch_fn: Callable[[], float] = time.time
    ) -> None:
        """Hard-pause until `reset_epoch`, then resume with per-worker jitter.

        `x-ratelimit-reset-requests` is a forward-refill projection shared by
        every worker (§1.3), not a fixed window edge -- if every worker wakes
        at exactly that instant they collide immediately, recreating the
        throttle they just paused for. The uniform(0, 250ms) jitter spreads
        the resume across workers instead.
        """
        if reset_epoch <= 0:
            return
        now = now_epoch_fn()
        wait_s = max(0.0, reset_epoch - now)
        if wait_s > 0:
            await self._sleep(wait_s)
        await self._sleep(self._rng.uniform(0, 0.25))

    def mean_latency(self) -> float:
        if not self._latencies:
            return 0.0
        return sum(self._latencies) / len(self._latencies)

    def littles_law_optimal_concurrency(self) -> int:
        """L = lambda * W: at the current paced rate and observed mean
        latency, how many requests must be in flight to just saturate the
        rate limiter. See §1.2 -- this is the number a hardcoded concurrency
        can never get right across varying prompt lengths and model warmth.
        """
        import math

        mean_lat = self.mean_latency()
        if mean_lat <= 0:
            return 1
        return max(1, math.ceil(self.bucket.rate * mean_lat))

    def snapshot(self, circuit_state: str) -> BackpressureSnapshot:
        return BackpressureSnapshot(
            current_rate_limit_rps=round(self.bucket.rate, 4),
            configured_max_rps=self.max_rate,
            throttle_events_429=self.throttle_events_429,
            retries_issued=self.retries_issued,
            circuit_state=circuit_state,
            observed_mean_latency_s=round(self.mean_latency(), 4),
            littles_law_optimal_concurrency=self.littles_law_optimal_concurrency(),
        )
