"""Pydantic request/response models for the §5 API contract."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class JobSubmitRequest(BaseModel):
    input_path: str
    model: str | None = None
    concurrency: int | None = Field(default=None, ge=1, le=256)
    max_tokens: int | None = Field(default=None, ge=1, le=4096)
    webhook_url: str | None = None


class JobSubmitResponse(BaseModel):
    job_id: str
    status: str
    submitted_at: str


class JobCountsResponse(BaseModel):
    ingested: int
    succeeded: int
    failed: int
    in_flight: int
    pending: int


class JobTimingResponse(BaseModel):
    started_at: str
    elapsed_s: float
    eta_s: float | None
    throughput_rps: float


class JobBackpressureResponse(BaseModel):
    current_rate_limit_rps: float
    configured_max_rps: float
    throttle_events_429: int
    retries_issued: int
    retry_budget_remaining: int
    circuit_state: str
    observed_mean_latency_s: float
    littles_law_optimal_concurrency: int


class JobUsageResponse(BaseModel):
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float


class JobStatusResponse(BaseModel):
    job_id: str
    status: Literal["queued", "running", "succeeded", "partial", "failed", "cancelled"]
    counts: JobCountsResponse
    progress_pct: float
    timing: JobTimingResponse
    backpressure: JobBackpressureResponse
    usage: JobUsageResponse
    errors_by_class: dict[str, int]
    abort_reason: str | None = None


class JobCancelResponse(BaseModel):
    job_id: str
    status: str
