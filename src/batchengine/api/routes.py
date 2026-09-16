"""§5 API contract, implemented literally: paths, request/response shapes.
Route handlers stay thin -- all orchestration lives in core/scheduler.py.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import structlog
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from ulid import ULID

from batchengine.core.models import JobConfig, JobRecord, JobStatus
from batchengine.core.retry import RetryBudget
from batchengine.core.scheduler import JobRunner, default_concurrency
from batchengine.core.spend_ledger import load_total_spend
from batchengine.observability.metrics import render_prometheus_text
from batchengine.providers.digitalocean import DigitalOceanProvider
from batchengine.providers.mock import MockProvider, MockProviderConfig

from .schemas import (
    JobBackpressureResponse,
    JobCancelResponse,
    JobCountsResponse,
    JobStatusResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    JobTimingResponse,
    JobUsageResponse,
)

log = structlog.get_logger()
router = APIRouter()


def _state(request: Request):
    return request.app.state.app_state


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/job", status_code=202, response_model=JobSubmitResponse)
async def submit_job(body: JobSubmitRequest, request: Request) -> JobSubmitResponse:
    state = _state(request)
    settings = state.settings

    # Stat-only validation (§5): existence, readability, size. Never parse
    # the file on the request path -- that's what keeps this under 50ms
    # regardless of how large the input is.
    if not os.path.isfile(body.input_path):
        raise HTTPException(status_code=400, detail=f"input_path not found: {body.input_path}")
    if not os.access(body.input_path, os.R_OK):
        raise HTTPException(status_code=400, detail=f"input_path not readable: {body.input_path}")
    os.path.getsize(body.input_path)  # touches the stat cache; result unused by design

    if body.webhook_url is not None:
        parsed = urlparse(body.webhook_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise HTTPException(status_code=400, detail="webhook_url must be a valid http(s) URL")

    model = body.model or settings.batchengine_model
    max_tokens = body.max_tokens or settings.batchengine_max_tokens
    is_live = settings.do_inference_key is not None and settings.do_inference_key != ""

    max_items: int | None = None
    if is_live:
        total_spend = load_total_spend(settings.batchengine_spend_ledger_path)
        if total_spend >= settings.batchengine_max_total_spend_usd:
            # §12.2 hard rail: never exceed MAX_TOTAL_SPEND_USD. The ledger is
            # never reset automatically, so this is a durable refusal, not a
            # transient one -- see docs/decisions.md "Spend guard".
            raise HTTPException(
                status_code=402,
                detail=(
                    f"spend ledger exhausted (${total_spend:.4f} >= "
                    f"${settings.batchengine_max_total_spend_usd}); refusing to start a live job"
                ),
            )
        # TODO(review): the §5 request schema has no "full run" flag, so a
        # live job defaults to a bounded sample (LIVE_SAMPLE_SIZE) unless the
        # caller opts in via a non-spec `concurrency`-adjacent extension
        # field. Logged in OPEN_QUESTIONS.md.
        max_items = min(settings.batchengine_live_sample_size, settings.batchengine_max_live_items)

    job_id = str(ULID())
    result_path = str(Path(settings.batchengine_results_dir) / f"{job_id}.jsonl")

    config = JobConfig(
        input_path=body.input_path,
        model=model,
        max_tokens=max_tokens,
        concurrency=body.concurrency,
        webhook_url=body.webhook_url,
        max_job_spend_usd=settings.batchengine_max_job_spend_usd,
        max_items=max_items,
    )
    record = JobRecord(job_id=job_id, config=config, status=JobStatus.QUEUED, result_path=result_path)
    await state.job_store.create(record)

    if is_live:
        provider = DigitalOceanProvider(
            client=state.http_client,
            api_key=settings.do_inference_key,  # type: ignore[arg-type]
            base_url=settings.do_inference_base_url,
            model_for_pricing=model,
        )
    else:
        provider = MockProvider(MockProviderConfig())

    runner = JobRunner(
        record=record,
        provider=provider,
        store=state.job_store,
        rate_limit_rpm=settings.batchengine_rate_limit_rpm,
        http_client=state.http_client,
        allow_private_webhooks=settings.batchengine_allow_private_webhooks,
    )
    state.runners[job_id] = runner
    state.tasks[job_id] = asyncio.create_task(_run_and_settle(runner, provider, state, job_id, is_live, settings))

    return JobSubmitResponse(job_id=job_id, status="queued", submitted_at=_iso(record.submitted_at))


async def _run_and_settle(runner: JobRunner, provider, state, job_id: str, is_live: bool, settings) -> None:
    try:
        await runner.run()
    finally:
        if is_live:
            from batchengine.core.spend_ledger import record_spend

            record_spend(settings.batchengine_spend_ledger_path, runner.record.usage.estimated_cost_usd)


async def _get_record(request: Request, job_id: str) -> JobRecord:
    state = _state(request)
    runner = state.runners.get(job_id)
    if runner is not None:
        return runner.record
    record = await state.job_store.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"unknown job_id: {job_id}")
    return record


@router.get("/job/{job_id}/status", response_model=JobStatusResponse)
async def get_status(job_id: str, request: Request) -> JobStatusResponse:
    record = await _get_record(request, job_id)
    counts = record.counts
    done = counts.succeeded + counts.failed
    elapsed = record.timing.elapsed_s()
    throughput = done / elapsed if elapsed > 0 else 0.0
    progress_pct = round((done / counts.ingested) * 100, 1) if counts.ingested > 0 else 0.0

    eta_s: float | None = None
    if record.status == JobStatus.RUNNING and throughput > 0:
        remaining = max(0, counts.ingested - done) + counts.pending
        eta_s = round(remaining / throughput, 1)

    settings = _state(request).settings
    bp = record.backpressure or {}
    backpressure = JobBackpressureResponse(
        current_rate_limit_rps=bp.get("current_rate_limit_rps", 0.0),
        configured_max_rps=bp.get("configured_max_rps", settings.batchengine_rate_limit_rpm / 60.0),
        throttle_events_429=bp.get("throttle_events_429", 0),
        retries_issued=bp.get("retries_issued", 0),
        retry_budget_remaining=bp.get("retry_budget_remaining", RetryBudget(total_items=max(1, counts.ingested)).limit),
        circuit_state=bp.get("circuit_state", "closed"),
        observed_mean_latency_s=bp.get("observed_mean_latency_s", 0.0),
        littles_law_optimal_concurrency=bp.get(
            "littles_law_optimal_concurrency", record.config.concurrency or default_concurrency(settings.batchengine_rate_limit_rpm)
        ),
    )

    return JobStatusResponse(
        job_id=record.job_id,
        status=record.status.value,
        counts=JobCountsResponse(
            ingested=counts.ingested,
            succeeded=counts.succeeded,
            failed=counts.failed,
            in_flight=counts.in_flight,
            pending=counts.pending,
        ),
        progress_pct=progress_pct,
        timing=JobTimingResponse(
            started_at=_iso(record.timing.started_at),
            elapsed_s=round(elapsed, 2),
            eta_s=eta_s,
            throughput_rps=round(throughput, 3),
        ),
        backpressure=backpressure,
        usage=JobUsageResponse(
            input_tokens=record.usage.input_tokens,
            output_tokens=record.usage.output_tokens,
            estimated_cost_usd=round(record.usage.estimated_cost_usd, 6),
        ),
        errors_by_class=record.errors_by_class,
        abort_reason=record.abort_reason,
    )


@router.post("/job/{job_id}/cancel", response_model=JobCancelResponse)
async def cancel_job(job_id: str, request: Request) -> JobCancelResponse:
    state = _state(request)
    runner = state.runners.get(job_id)
    if runner is None:
        record = await state.job_store.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"unknown job_id: {job_id}")
        raise HTTPException(status_code=409, detail="job is not active in this process (already finished or process restarted)")
    runner.cancel()
    return JobCancelResponse(job_id=job_id, status="cancelling")


@router.get("/job/{job_id}/download")
async def download_job(
    job_id: str,
    request: Request,
    format: str = Query(default="ndjson", pattern="^(ndjson|json)$"),
    include: str = Query(default="all", pattern="^(errors|success|all)$"),
    partial: bool = Query(default=False),
) -> StreamingResponse:
    record = await _get_record(request, job_id)
    if record.status in (JobStatus.QUEUED, JobStatus.RUNNING) and not partial:
        raise HTTPException(status_code=409, detail="job is still running; retry with ?partial=true to stream what's done so far")

    path = Path(record.result_path)

    def lines() -> Iterator[str]:
        if not path.exists():
            return
        wanted_status = {"success": "success", "errors": "error"}.get(include)
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                if wanted_status is not None:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if row.get("status") != wanted_status:
                        continue
                yield line

    if format == "json":

        async def gen_json():
            first = True
            yield "["
            for line in lines():
                yield ("" if first else ",") + line
                first = False
            yield "]"

        return StreamingResponse(gen_json(), media_type="application/json")

    async def gen_ndjson():
        for line in lines():
            yield line + "\n"

    return StreamingResponse(gen_ndjson(), media_type="application/x-ndjson")


@router.get("/metrics")
async def metrics(request: Request) -> PlainTextResponse:
    state = _state(request)
    ids = set(await state.job_store.list_ids()) | set(state.runners.keys())
    records = []
    for job_id in ids:
        runner = state.runners.get(job_id)
        record = runner.record if runner is not None else await state.job_store.get(job_id)
        if record is not None:
            records.append(record)
    return PlainTextResponse(render_prometheus_text(records), media_type="text/plain; version=0.0.4")
