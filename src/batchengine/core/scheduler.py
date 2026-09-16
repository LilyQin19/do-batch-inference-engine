"""Job orchestration and lifecycle (§3). Wires ingest -> bounded queue ->
rate limiter -> worker pool -> sink -> JobStore into one asyncio task per job.
This module has no HTTP awareness at all -- the API layer only creates a
JobRunner and calls `.run()` as a background task.
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import time

import httpx
import structlog

from batchengine.core.ingest import stream_items
from batchengine.core.models import JobRecord, JobStatus, PromptItem, RowError
from batchengine.core.ratelimit import AdaptiveController, TokenBucket
from batchengine.core.retry import CircuitBreaker, RetryBudget
from batchengine.core.sink import ResultSink
from batchengine.core.worker import SHUTDOWN, WorkerContext, worker_loop
from batchengine.providers.base import InferenceProvider
from batchengine.store.base import JobStore

log = structlog.get_logger()

# A hardcoded concurrency is wrong at every observed latency except the one
# it was tuned for (§1.2). This is only the *starting* guess, used before any
# latency has been observed; AdaptiveController.littles_law_optimal_concurrency
# reports the number the run actually converges toward.
_DEFAULT_ASSUMED_LATENCY_S = 2.0
_QUEUE_MULTIPLIER = 4
_STATUS_PERSIST_EVERY = 25
_OVERPROVISION_WARN_FACTOR = 2.0


def default_concurrency(rate_limit_rpm: int) -> int:
    rate_rps = rate_limit_rpm / 60.0
    return max(2, math.ceil(rate_rps * _DEFAULT_ASSUMED_LATENCY_S))


class JobRunner:
    """One instance per job; `.run()` is scheduled with `asyncio.create_task`
    by the route handler and awaited to completion in the background.
    """

    def __init__(
        self,
        record: JobRecord,
        provider: InferenceProvider,
        store: JobStore,
        rate_limit_rpm: int,
        http_client: httpx.AsyncClient | None = None,
        webhook_secret: str = "",
        allow_private_webhooks: bool = False,
        circuit_cooldown_s: float = 30.0,
    ) -> None:
        self.record = record
        self.provider = provider
        self.store = store
        self.rate_limit_rpm = rate_limit_rpm
        self.circuit_cooldown_s = circuit_cooldown_s
        self.cancel_event = asyncio.Event()
        self._http_client = http_client
        self._webhook_secret = webhook_secret
        self._allow_private_webhooks = allow_private_webhooks

    def cancel(self) -> None:
        self.cancel_event.set()

    async def run(self) -> None:
        record = self.record
        config = record.config
        concurrency = config.concurrency or default_concurrency(self.rate_limit_rpm)

        record.status = JobStatus.RUNNING
        await self.store.save(record)

        queue: asyncio.Queue[object] = asyncio.Queue(maxsize=concurrency * _QUEUE_MULTIPLIER)
        rate_rps = self.rate_limit_rpm / 60.0
        bucket = TokenBucket(rate=rate_rps, capacity=max(2.0, rate_rps))
        controller = AdaptiveController(
            bucket=bucket, min_rate=max(0.05, rate_rps * 0.1), max_rate=rate_rps * 1.5
        )
        circuit_breaker = CircuitBreaker(cooldown_s=self.circuit_cooldown_s)
        retry_budget = RetryBudget(
            total_items=1
        )  # grown as items are ingested; see _ingest_and_dispatch

        sink = ResultSink(record.result_path)
        await sink.start()

        abort_event = asyncio.Event()

        async def on_spend_check() -> None:
            """§6.7: recomputed every 50 completed items, comparing both
            actual cost-so-far and a linear cost projection against the
            per-job cap. Projection uses `ingested-so-far` as the total-items
            estimate since the input is still streaming in -- it undercounts
            early in a run, which is conservative (it can only trip the
            guard *later* than the true total would), not permissive.
            """
            done = record.counts.succeeded + record.counts.failed
            if done == 0 or done % 50 != 0:
                return
            cost_so_far = (
                record.usage.input_tokens / 1_000_000 * self.provider.cost_per_1m_input()
                + record.usage.output_tokens / 1_000_000 * self.provider.cost_per_1m_output()
            )
            items_total_estimate = max(record.counts.ingested, done)
            projected = (cost_so_far / done) * items_total_estimate
            if cost_so_far >= config.max_job_spend_usd or projected >= config.max_job_spend_usd:
                if not abort_event.is_set():
                    log.error(
                        "job.spend_guard_tripped",
                        job_id=record.job_id,
                        cost_so_far_usd=round(cost_so_far, 4),
                        projected_usd=round(projected, 4),
                        cap_usd=config.max_job_spend_usd,
                    )
                    worker_ctx.abort_reason.append(
                        f"spend guard: cost ${cost_so_far:.4f} / projected ${projected:.4f} "
                        f">= cap ${config.max_job_spend_usd}"
                    )
                abort_event.set()

        worker_ctx = WorkerContext(
            job_id=record.job_id,
            queue=queue,
            provider=self.provider,
            controller=controller,
            circuit_breaker=circuit_breaker,
            retry_budget=retry_budget,
            sink=sink,
            counts=record.counts,
            usage=record.usage,
            errors_by_class=record.errors_by_class,
            config=config,
            abort_event=abort_event,
            cancel_event=self.cancel_event,
            on_spend_check=on_spend_check,
        )

        workers = [asyncio.create_task(worker_loop(worker_ctx)) for _ in range(concurrency)]

        try:
            await self._ingest_and_dispatch(
                queue, retry_budget, record, abort_event, controller, circuit_breaker
            )
        finally:
            for _ in workers:
                await queue.put(SHUTDOWN)
            await asyncio.gather(*workers)
            await sink.close()

        optimal = controller.littles_law_optimal_concurrency()
        if optimal > 0 and concurrency > optimal * _OVERPROVISION_WARN_FACTOR:
            log.warning(
                "job.overprovisioned_concurrency",
                job_id=record.job_id,
                configured_concurrency=concurrency,
                littles_law_optimal_concurrency=optimal,
            )

        record.timing.finished_at = time.time()
        record.backpressure = dataclasses.asdict(controller.snapshot(circuit_breaker.state))
        record.backpressure["retry_budget_remaining"] = retry_budget.remaining
        record.usage.estimated_cost_usd = (
            record.usage.input_tokens / 1_000_000 * self.provider.cost_per_1m_input()
            + record.usage.output_tokens / 1_000_000 * self.provider.cost_per_1m_output()
        )
        record.status = self._final_status(abort_event, worker_ctx)
        if abort_event.is_set() and worker_ctx.abort_reason:
            record.abort_reason = worker_ctx.abort_reason[0]

        assert record.counts.conserved(), (
            f"conservation invariant violated: {record.counts.succeeded=} "
            f"{record.counts.failed=} {record.counts.ingested=}"
        )
        await self.store.save(record)
        await self._deliver_webhook_if_configured()

    async def _deliver_webhook_if_configured(self) -> None:
        record = self.record
        if not record.config.webhook_url or self._http_client is None:
            return
        from batchengine.extensions.webhook import WebhookSSRFError, deliver_webhook

        payload: dict[str, object] = {
            "job_id": record.job_id,
            "status": record.status.value,
            "counts": dataclasses.asdict(record.counts),
            "download_url": f"/job/{record.job_id}/download",
        }
        try:
            await deliver_webhook(
                self._http_client,
                record.config.webhook_url,
                payload,
                self._webhook_secret,
                allow_private=self._allow_private_webhooks,
            )
        except WebhookSSRFError as exc:
            log.error("job.webhook_rejected", job_id=record.job_id, reason=str(exc))

    def _final_status(self, abort_event: asyncio.Event, worker_ctx: WorkerContext) -> JobStatus:
        counts = self.record.counts
        if self.cancel_event.is_set():
            return JobStatus.CANCELLED
        if abort_event.is_set():
            return JobStatus.FAILED
        if counts.ingested == 0:
            return JobStatus.SUCCEEDED
        if counts.failed == 0:
            return JobStatus.SUCCEEDED
        if counts.succeeded == 0:
            return JobStatus.FAILED
        return JobStatus.PARTIAL

    async def _ingest_and_dispatch(
        self,
        queue: asyncio.Queue[object],
        retry_budget: RetryBudget,
        record: JobRecord,
        abort_event: asyncio.Event,
        controller: AdaptiveController,
        circuit_breaker: CircuitBreaker,
    ) -> None:
        gen = stream_items(record.config.input_path)
        try:
            async for parsed in gen:
                if self.cancel_event.is_set() or abort_event.is_set():
                    break
                assert isinstance(parsed, (PromptItem, RowError))
                record.counts.ingested += 1
                record.counts.pending += 1
                retry_budget.total_items = record.counts.ingested
                await queue.put(parsed)
                if record.counts.ingested % _STATUS_PERSIST_EVERY == 0:
                    record.backpressure = dataclasses.asdict(
                        controller.snapshot(circuit_breaker.state)
                    )
                    record.backpressure["retry_budget_remaining"] = retry_budget.remaining
                    await self.store.save(record)
                max_items = record.config.max_items
                if max_items is not None and record.counts.ingested >= max_items:
                    log.info("job.live_item_cap_reached", job_id=record.job_id, cap=max_items)
                    break
        finally:
            await gen.aclose()
