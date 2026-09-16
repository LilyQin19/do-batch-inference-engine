"""Property test (§7): the token bucket must never let more than
`rate * window + capacity` tokens out over any sliding time window, no
matter how the caller's acquire pattern is shaped.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from batchengine.core.ratelimit import TokenBucket

pytestmark = pytest.mark.asyncio


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


async def _fake_sleep(clock: FakeClock, seconds: float) -> None:
    clock.advance(seconds)


@settings(max_examples=40, deadline=None)
@given(
    rate=st.floats(min_value=0.5, max_value=20.0),
    capacity=st.floats(min_value=1.0, max_value=20.0),
    n_requests=st.integers(min_value=5, max_value=60),
)
async def test_bucket_never_exceeds_rate_over_any_window(
    rate: float, capacity: float, n_requests: int
) -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=rate, capacity=capacity, clock=clock)
    sleep = lambda s: _fake_sleep(clock, s)  # noqa: E731

    grant_times: list[float] = []
    for _ in range(n_requests):
        await bucket.acquire(sleep=sleep)
        grant_times.append(clock.now)

    # For every window [t, t+W], the number of grants inside it must not
    # exceed what the bucket could have accumulated: rate*W + capacity
    # (the +capacity accounts for burst -- tokens saved up before the window).
    for window in (1.0, 5.0, 10.0):
        for start in grant_times:
            count_in_window = sum(1 for t in grant_times if start <= t < start + window)
            max_allowed = rate * window + capacity + 1e-6
            assert count_in_window <= max_allowed, (
                f"{count_in_window} grants in a {window}s window exceeds "
                f"rate*window+capacity={max_allowed} (rate={rate}, capacity={capacity})"
            )
