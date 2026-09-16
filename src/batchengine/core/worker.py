"""The worker loop: acquire a rate-limit token, call the provider, classify
the outcome, retry or terminate per §6.2. N of these run concurrently; the
token bucket -- not the worker count -- is what actually paces request rate
(§1.2). A worker that loses its race for a token simply awaits longer; it
never bypasses pacing to "make progress".
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import structlog

from batchengine.core.models import (
    FailureClass,
    JobConfig,
    JobCounts,
    PromptItem,
    RowError,
    RowResult,
    UsageDelta,
    UsageTotals,
)
from batchengine.core.ratelimit import AdaptiveController
from batchengine.core.retry import CircuitBreaker, RetryBudget, classify, full_jitter_delay
from batchengine.core.sink import ResultSink
from batchengine.providers.base import InferenceProvider, ProviderTransportError

log = structlog.get_logger()

# Sentinel telling a worker there is no more input; one is enqueued per worker.
SHUTDOWN = object()


@dataclass(slots=True)
class WorkerContext:
    job_id: str
    queue: asyncio.Queue[object]
    provider: InferenceProvider
    controller: AdaptiveController
    circuit_breaker: CircuitBreaker
    retry_budget: RetryBudget
    sink: ResultSink
    counts: JobCounts
    usage: UsageTotals
    errors_by_class: dict[str, int]
    config: JobConfig
    abort_event: asyncio.Event
    cancel_event: asyncio.Event
    on_spend_check: Callable[[], Awaitable[None]]
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    rng: random.Random = field(default_factory=random.Random)
    abort_reason: list[str] = field(default_factory=list)  # single-slot box
    abort_class: list[FailureClass] = field(default_factory=list)  # single-slot box


def _bump_error_class(ctx: WorkerContext, failure_class: FailureClass) -> None:
    ctx.errors_by_class[failure_class.value] = ctx.errors_by_class.get(failure_class.value, 0) + 1


async def _emit_error(
    ctx: WorkerContext,
    item_id: str,
    failure_class: FailureClass,
    message: str,
    attempt: int,
    http_status: int | None,
) -> None:
    await ctx.sink.submit(
        RowError(
            item_id=item_id,
            failure_class=failure_class,
            message=message,
            attempt=attempt,
            http_status=http_status,
        )
    )
    ctx.counts.failed += 1
    ctx.counts.pending -= 1
    _bump_error_class(ctx, failure_class)


async def process_item(ctx: WorkerContext, item: PromptItem) -> None:
    """Drive one prompt through acquire -> call -> classify -> retry/emit.
    Returning normally always means the item was terminally resolved
    (success or a terminal RowError) -- this is what the conservation
    invariant depends on.
    """
    attempt = 0
    while True:
        if ctx.cancel_event.is_set():
            await _emit_error(
                ctx,
                item.id,
                FailureClass.CANCELLED,
                "job cancelled before dispatch",
                attempt or 1,
                None,
            )
            return

        if ctx.abort_event.is_set():
            reason = ctx.abort_reason[0] if ctx.abort_reason else "job aborted"
            klass = ctx.abort_class[0] if ctx.abort_class else FailureClass.FATAL_AUTH
            await _emit_error(ctx, item.id, klass, reason, attempt or 1, None)
            return

        if not ctx.circuit_breaker.allow_request():
            await ctx.sleep(0.25)
            continue

        attempt += 1
        await ctx.controller.bucket.acquire(sleep=ctx.sleep)
        ctx.counts.in_flight += 1
        start = ctx.clock()
        response = None
        exc: ProviderTransportError | None = None
        try:
            response = await ctx.provider.complete(
                item.prompt, ctx.config.model, ctx.config.max_tokens
            )
        except asyncio.CancelledError:
            raise
        except ProviderTransportError as e:
            exc = e
        except Exception as e:  # noqa: BLE001 -- see comment below
            # A provider bug (anything not modeled as ProviderTransportError
            # or a classified HTTP response) must still resolve this item to
            # a terminal outcome. Letting it propagate would kill this
            # worker task without ever emitting a RowResult/RowError --
            # exactly how the conservation invariant
            # (succeeded + failed == ingested) gets silently violated. Wrap
            # it as a transport error so it flows through the normal
            # transient-retry path unchanged.
            log.exception(
                "worker.unexpected_provider_exception",
                job_id=ctx.job_id,
                item_id=item.id,
            )
            exc = ProviderTransportError(f"unexpected error: {e!r}")
        finally:
            ctx.counts.in_flight -= 1
        latency = ctx.clock() - start

        failure_class, max_attempts = classify(response, exc)
        http_status = response.status_code if response is not None else None

        # Per-attempt observability (§6.8): every attempt is logged with its
        # outcome and latency, not just the item's final one -- this is what
        # lets p50/p95 latency under normal vs. throttled conditions, and
        # the literal rate-limit headers on a real 429, be reconstructed
        # from logs after a live run instead of needing bespoke
        # instrumentation each time. Never logs prompt content.
        log.info(
            "job.attempt",
            job_id=ctx.job_id,
            item_id=item.id,
            attempt=attempt,
            latency_s=round(latency, 4),
            http_status=http_status,
            failure_class=failure_class.value,
            rate_limit_headers=(response.headers if response is not None else None),
        )

        breaker_failure = failure_class in (
            FailureClass.TRANSIENT,
            FailureClass.FATAL_AUTH,
            FailureClass.FATAL_BILLING,
        )
        ctx.circuit_breaker.record(breaker_failure)

        if failure_class == FailureClass.SUCCESS:
            assert response is not None
            ctx.controller.record_success(latency)
            usage = UsageDelta(
                input_tokens=response.input_tokens, output_tokens=response.output_tokens
            )
            ctx.usage.input_tokens += usage.input_tokens
            ctx.usage.output_tokens += usage.output_tokens
            await ctx.sink.submit(
                RowResult(
                    item_id=item.id,
                    response_text=response.text,
                    usage=usage,
                    latency_s=latency,
                    attempt=attempt,
                )
            )
            ctx.counts.succeeded += 1
            ctx.counts.pending -= 1
            await ctx.on_spend_check()
            return

        if failure_class.is_fatal:
            ctx.abort_reason.append(f"{failure_class.value} (HTTP {http_status})")
            ctx.abort_class.append(failure_class)
            ctx.abort_event.set()
            log.error(
                "job.fatal_abort",
                job_id=ctx.job_id,
                failure_class=failure_class.value,
                http_status=http_status,
            )
            await _emit_error(
                ctx, item.id, failure_class, f"fatal: HTTP {http_status}", attempt, http_status
            )
            return

        if failure_class == FailureClass.THROTTLED:
            ctx.controller.record_throttle()
            reset_header = (
                (response.headers or {}).get("x-ratelimit-reset-requests") if response else None
            )
            if reset_header:
                try:
                    reset_epoch = float(reset_header)
                except ValueError:
                    reset_epoch = 0.0
                if reset_epoch > 0:
                    await ctx.controller.honor_reset_header(reset_epoch)

        if attempt >= max_attempts:
            final_class = (
                FailureClass.TRANSIENT_EXHAUSTED
                if failure_class in (FailureClass.THROTTLED, FailureClass.TRANSIENT)
                else failure_class
            )
            await _emit_error(
                ctx, item.id, final_class, "max attempts exhausted", attempt, http_status
            )
            return

        if not ctx.retry_budget.try_consume():
            await _emit_error(
                ctx,
                item.id,
                FailureClass.TRANSIENT_EXHAUSTED,
                "retry budget exhausted",
                attempt,
                http_status,
            )
            return

        ctx.controller.record_retry_issued()
        delay = full_jitter_delay(attempt, rng=ctx.rng)
        await ctx.sleep(delay)
        # loop: retry the same item


async def worker_loop(ctx: WorkerContext) -> None:
    while True:
        item = await ctx.queue.get()
        try:
            if item is SHUTDOWN:
                return
            if isinstance(item, RowError):
                # Already-terminal from ingest (e.g. malformed row) -- pass through.
                await ctx.sink.submit(item)
                ctx.counts.failed += 1
                ctx.counts.pending -= 1
                _bump_error_class(ctx, item.failure_class)
                continue
            assert isinstance(item, PromptItem)
            try:
                await process_item(ctx, item)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Last-resort safety net. process_item is designed to
                # always resolve an item to a terminal outcome itself (see
                # its own try/except around the provider call above); this
                # only fires if some *other*, currently-unknown bug still
                # lets an exception escape before that item's counts were
                # mutated. Emitting a terminal RowError here rather than
                # letting the exception kill this worker task is what keeps
                # the conservation invariant true even against a defect we
                # haven't found yet -- the alternative is a queue item that
                # is never resolved and never task_done()'d.
                log.exception(
                    "worker.process_item_unexpected_exception",
                    job_id=ctx.job_id,
                    item_id=item.id,
                )
                await _emit_error(
                    ctx,
                    item.id,
                    FailureClass.TRANSIENT_EXHAUSTED,
                    "unexpected exception escaped process_item",
                    1,
                    None,
                )
        finally:
            ctx.queue.task_done()
