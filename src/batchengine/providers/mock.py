"""Deterministic, seeded, zero-network chaos provider.

This is what makes the entire test suite hermetic (§6.5) and what lets us
*prove* backpressure and failure-classification behavior instead of asserting
it by inspection. Built before the live provider on purpose: every other
component is developed and tested against this.
"""

from __future__ import annotations

import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

from batchengine.providers.base import ProviderResponse, ProviderTransportError


@dataclass(slots=True)
class MockProviderConfig:
    seed: int = 0
    p_429: float = 0.0
    p_500: float = 0.0
    p_400: float = 0.0
    p_timeout: float = 0.0
    latency_mean: float = 0.05
    latency_jitter: float = 0.02
    # If set, calls beyond this rate in a rolling 60s window get a real 429
    # with correct x-ratelimit-* headers, simulating the account quota.
    hard_rpm_ceiling: int | None = None
    # After this many total calls, every subsequent call returns `fatal_status`.
    # Used to test that fatal_auth/fatal_billing abort within a handful of
    # requests rather than after the whole batch is burned.
    fail_after_n: int | None = None
    fatal_status: int = 402
    malformed_response_rate: float = 0.0
    # When True, the provider actually awaits its simulated latency. Off by
    # default so unit/integration tests covering thousands of calls stay fast;
    # turn on for tests that specifically assert on observed latency/W.
    simulate_delay: bool = False
    cost_per_1m_input: float = 0.05
    cost_per_1m_output: float = 0.45


class MockProvider:
    """Implements InferenceProvider. See MockProviderConfig for knobs."""

    def __init__(self, config: MockProviderConfig, clock: Callable[[], float] = time.monotonic) -> None:
        self._cfg = config
        self._rng = random.Random(config.seed)
        self._clock = clock
        self._call_count = 0
        self._request_timestamps: deque[float] = deque()

    def _prune_window(self, now: float) -> None:
        window_start = now - 60.0
        while self._request_timestamps and self._request_timestamps[0] < window_start:
            self._request_timestamps.popleft()

    async def complete(self, prompt: str, model: str, max_tokens: int) -> ProviderResponse:
        self._call_count += 1
        now = self._clock()

        if self._cfg.simulate_delay:
            import asyncio

            latency = max(0.0, self._rng.gauss(self._cfg.latency_mean, self._cfg.latency_jitter))
            await asyncio.sleep(latency)

        if self._cfg.fail_after_n is not None and self._call_count > self._cfg.fail_after_n:
            return ProviderResponse(
                status_code=self._cfg.fatal_status,
                text="mock: fatal condition injected",
                headers={},
            )

        if self._cfg.hard_rpm_ceiling is not None:
            self._prune_window(now)
            if len(self._request_timestamps) >= self._cfg.hard_rpm_ceiling:
                reset_at = self._request_timestamps[0] + 60.0
                return ProviderResponse(
                    status_code=429,
                    text="mock: rate limit exceeded",
                    headers=self._headers(
                        remaining_requests=0,
                        reset_requests=reset_at,
                    ),
                )
            self._request_timestamps.append(now)

        if self._rng.random() < self._cfg.p_timeout:
            raise ProviderTransportError("mock: simulated timeout")

        if self._rng.random() < self._cfg.p_429:
            return ProviderResponse(
                status_code=429,
                text="mock: simulated throttle",
                headers=self._headers(remaining_requests=0, reset_requests=now + 1.0),
            )

        if self._rng.random() < self._cfg.p_500:
            return ProviderResponse(status_code=500, text="mock: simulated server error", headers=self._headers())

        if self._rng.random() < self._cfg.p_400:
            return ProviderResponse(
                status_code=400, text="mock: simulated invalid request", headers=self._headers()
            )

        input_tokens = max(1, len(prompt) // 4)
        output_tokens = min(max_tokens, max(1, self._rng.randint(max_tokens // 2, max_tokens)))
        malformed = self._rng.random() < self._cfg.malformed_response_rate

        return ProviderResponse(
            status_code=200,
            text="" if malformed else f"mock completion for: {prompt[:32]!r}",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            malformed=malformed,
            headers=self._headers(remaining_requests=self._remaining(now)),
        )

    def _remaining(self, now: float) -> int:
        if self._cfg.hard_rpm_ceiling is None:
            return 999
        self._prune_window(now)
        return max(0, self._cfg.hard_rpm_ceiling - len(self._request_timestamps))

    def _headers(self, remaining_requests: int = 999, reset_requests: float = 0.0) -> dict[str, str]:
        limit = self._cfg.hard_rpm_ceiling or 120
        return {
            "x-ratelimit-limit-requests": str(limit),
            "x-ratelimit-remaining-requests": str(remaining_requests),
            "x-ratelimit-reset-requests": str(int(reset_requests)),
            "x-ratelimit-limit-tokens-per-minute": "500000",
            "x-ratelimit-remaining-tokens-per-minute": "500000",
            "x-ratelimit-reset-tokens-per-minute": "0",
        }

    def cost_per_1m_input(self) -> float:
        return self._cfg.cost_per_1m_input

    def cost_per_1m_output(self) -> float:
        return self._cfg.cost_per_1m_output
