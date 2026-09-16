"""Failure classification (§6.2 taxonomy), full-jitter backoff, a global retry
budget, and a circuit breaker. These four pieces together are what let the
worker pool survive a real 429 storm without either hammering the upstream or
silently dropping rows.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field

from batchengine.core.models import FailureClass
from batchengine.providers.base import ProviderResponse, ProviderTransportError

# §6.2, verbatim. http status -> (FailureClass, max_attempts).
# max_attempts counts total tries (including the first), so "max 5 attempts"
# for transient means up to 4 retries after the initial call.
_STATUS_TABLE: dict[int, tuple[FailureClass, int]] = {
    408: (FailureClass.TRANSIENT, 5),
    429: (FailureClass.THROTTLED, 8),
    500: (FailureClass.TRANSIENT, 5),
    502: (FailureClass.TRANSIENT, 5),
    503: (FailureClass.TRANSIENT, 5),
    504: (FailureClass.TRANSIENT, 5),
    400: (FailureClass.INVALID_INPUT, 1),
    422: (FailureClass.INVALID_INPUT, 1),
    413: (FailureClass.INVALID_INPUT, 1),
    401: (FailureClass.FATAL_AUTH, 1),
    403: (FailureClass.FATAL_AUTH, 1),
    402: (FailureClass.FATAL_BILLING, 1),
}

_DEFAULT_MAX_ATTEMPTS = 5


def classify(response: ProviderResponse | None, exc: Exception | None = None) -> tuple[FailureClass, int]:
    """Map a provider outcome to (FailureClass, max_attempts_for_this_class).

    Exactly one of `response`/`exc` should be given: `exc` for a transport
    failure that never produced an HTTP response, `response` otherwise.
    """
    if exc is not None:
        if isinstance(exc, ProviderTransportError):
            return FailureClass.TRANSIENT, _DEFAULT_MAX_ATTEMPTS
        raise exc

    assert response is not None
    if response.status_code == 200:
        if response.malformed:
            # "malformed/unparseable response -> retry once, then terminal":
            # modeled as transient with a tight attempt cap of 2.
            return FailureClass.TRANSIENT, 2
        return FailureClass.SUCCESS, 1

    if response.status_code in _STATUS_TABLE:
        return _STATUS_TABLE[response.status_code]

    # Unknown status: treat conservatively as transient rather than silently
    # dropping the row or crashing the worker.
    return FailureClass.TRANSIENT, _DEFAULT_MAX_ATTEMPTS


def full_jitter_delay(attempt: int, base: float = 0.5, cap: float = 30.0, rng: random.Random | None = None) -> float:
    """AWS "full jitter": sleep = uniform(0, min(cap, base * 2**attempt)).

    Plain exponential backoff keeps a retry cohort's wake-up times
    correlated -- every worker that failed on the same upstream blip retries
    at the same instant on every subsequent wave, so the herd never
    decorrelates. Full jitter samples uniformly under the exponential
    envelope, so the *first* retry already spreads the cohort out, and it
    stays spread on every later wave instead of re-synchronizing.
    """
    rng = rng or random
    ceiling = min(cap, base * (2**attempt))
    return rng.uniform(0, ceiling)


@dataclass(slots=True)
class RetryBudget:
    """Caps total retries at a fraction of total requests for the job.
    Without this, a struggling upstream gets hit with retry-amplified load
    on top of whatever is already causing it to struggle.
    """

    total_items: int
    fraction: float = 0.20
    _used: int = 0

    @property
    def limit(self) -> int:
        return max(1, int(self.total_items * self.fraction))

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self._used)

    def try_consume(self) -> bool:
        if self._used >= self.limit:
            return False
        self._used += 1
        return True


@dataclass(slots=True)
class CircuitBreaker:
    """Trip open after >50% of the last `window` requests are 5xx/transient.
    While open, callers should fail fast instead of dispatching -- half-open
    lets exactly one probe through after `cooldown_s`.
    """

    window: int = 100
    threshold: float = 0.5
    cooldown_s: float = 30.0
    clock: object = field(default=time.monotonic)
    _outcomes: list[bool] = field(default_factory=list)  # True = failure
    _state: str = "closed"  # closed | open | half_open
    _opened_at: float = 0.0
    _probe_in_flight: bool = False

    def record(self, is_failure: bool) -> None:
        self._outcomes.append(is_failure)
        if len(self._outcomes) > self.window:
            self._outcomes.pop(0)
        if self._state == "half_open":
            self._probe_in_flight = False
            if is_failure:
                self._state = "open"
                self._opened_at = self.clock()  # type: ignore[operator]
            else:
                self._state = "closed"
                self._outcomes.clear()
            return
        if self._state == "closed" and len(self._outcomes) >= min(self.window, 10):
            failure_rate = sum(self._outcomes) / len(self._outcomes)
            if failure_rate > self.threshold:
                self._state = "open"
                self._opened_at = self.clock()  # type: ignore[operator]

    def allow_request(self) -> bool:
        now = self.clock()  # type: ignore[operator]
        if self._state == "open":
            if now - self._opened_at >= self.cooldown_s:
                self._state = "half_open"
                self._probe_in_flight = True
                return True
            return False
        if self._state == "half_open":
            return not self._probe_in_flight
        return True

    @property
    def state(self) -> str:
        return self._state
