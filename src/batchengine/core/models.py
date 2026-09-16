"""Domain types shared across ingest, scheduling, retry, and the sink.

Kept dependency-free of FastAPI/pydantic-settings so core logic can be unit
tested without spinning up the app.
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any


class FailureClass(enum.StrEnum):
    """The §6.2 taxonomy. Every non-success outcome maps to exactly one of these."""

    SUCCESS = "success"
    THROTTLED = "throttled"
    TRANSIENT = "transient"
    INVALID_INPUT = "invalid_input"
    FATAL_AUTH = "fatal_auth"
    FATAL_BILLING = "fatal_billing"
    TRANSIENT_EXHAUSTED = "transient_exhausted"
    CANCELLED = "cancelled"

    @property
    def is_fatal(self) -> bool:
        """Fatal classes abort the whole job rather than failing one row."""
        return self in (FailureClass.FATAL_AUTH, FailureClass.FATAL_BILLING)

    @property
    def is_retryable(self) -> bool:
        return self in (FailureClass.THROTTLED, FailureClass.TRANSIENT)


class JobStatus(enum.StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class PromptItem:
    """One row of the input batch. `id` defaults to its ingest offset if the
    input row doesn't carry its own id -- offsets alone aren't stable across
    reruns of a corrected file, so an explicit id is preferred when present.
    """

    id: str
    prompt: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class UsageDelta:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass(slots=True)
class RowResult:
    item_id: str
    response_text: str
    usage: UsageDelta
    latency_s: float
    attempt: int


@dataclass(slots=True)
class RowError:
    item_id: str
    failure_class: FailureClass
    message: str
    attempt: int
    http_status: int | None = None


@dataclass(slots=True)
class JobConfig:
    input_path: str
    model: str
    max_tokens: int
    concurrency: int | None  # None -> auto-size via Little's Law
    webhook_url: str | None = None
    max_job_spend_usd: float = 0.25
    # Ingestion stops (not fails) once this many items have been read.
    # Set by the route layer for live-provider jobs per the §12.2 safety
    # rail; None for mock-provider jobs, which have no such ceiling.
    max_items: int | None = None


@dataclass(slots=True)
class JobCounts:
    ingested: int = 0
    succeeded: int = 0
    failed: int = 0
    in_flight: int = 0
    pending: int = 0

    def conserved(self) -> bool:
        """The prime-directive invariant: every ingested item is accounted for
        exactly once across success + failure, once ingestion is complete and
        nothing remains in flight or pending.
        """
        return self.succeeded + self.failed == self.ingested


@dataclass(slots=True)
class JobTiming:
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None

    def elapsed_s(self) -> float:
        end = self.finished_at if self.finished_at is not None else time.time()
        return end - self.started_at


@dataclass(slots=True)
class UsageTotals:
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_usd: float = 0.0


@dataclass(slots=True)
class JobRecord:
    """Everything `GET /job/{id}/status` needs, plus enough to persist and
    resume a job. `backpressure` and `errors_by_class` are kept as plain
    dicts (rather than importing ratelimit types here) so this module stays
    the dependency-free base that everything else imports.
    """

    job_id: str
    config: JobConfig
    status: JobStatus = JobStatus.QUEUED
    submitted_at: float = field(default_factory=time.time)
    counts: JobCounts = field(default_factory=JobCounts)
    timing: JobTiming = field(default_factory=JobTiming)
    usage: UsageTotals = field(default_factory=UsageTotals)
    backpressure: dict[str, Any] = field(default_factory=dict)
    errors_by_class: dict[str, int] = field(default_factory=dict)
    result_path: str = ""
    abort_reason: str | None = None
