"""Direct, fast unit coverage of the worker loop's exception-safety
behavior (see the review-flagged defect: anything but
`ProviderTransportError` used to kill the worker task without ever
resolving the item). These hit `process_item`/`worker_loop` directly with
fakes instead of going through the full HTTP+scheduler stack, so they run
in milliseconds and pin the exact behavior the integration-level
`p_unexpected_exception` chaos case only proves indirectly.
"""

from __future__ import annotations

import asyncio

import pytest

from batchengine.core.models import (
    FailureClass,
    JobConfig,
    JobCounts,
    PromptItem,
    RowError,
    RowResult,
    UsageTotals,
)
from batchengine.core.ratelimit import AdaptiveController, TokenBucket
from batchengine.core.retry import CircuitBreaker, RetryBudget
from batchengine.core.worker import SHUTDOWN, WorkerContext, process_item, worker_loop


class _FakeSink:
    def __init__(self) -> None:
        self.submitted: list[RowResult | RowError] = []

    async def submit(self, item: RowResult | RowError) -> None:
        self.submitted.append(item)


class _RaisingProvider:
    """Raises whatever it's told to, every call -- deterministic chaos
    without needing MockProvider's seeded randomness.
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.calls = 0

    async def complete(self, prompt: str, model: str, max_tokens: int) -> object:
        self.calls += 1
        raise self._exc

    def cost_per_1m_input(self) -> float:
        return 0.0

    def cost_per_1m_output(self) -> float:
        return 0.0


async def _noop_spend_check() -> None:
    return None


async def _fast_sleep(_seconds: float) -> None:
    return None


def _make_ctx(provider: object, **overrides: object) -> WorkerContext:
    bucket = TokenBucket(rate=1000.0, capacity=1000.0)
    controller = AdaptiveController(bucket, min_rate=1.0, max_rate=1000.0)
    kwargs: dict[str, object] = dict(
        job_id="job-1",
        queue=asyncio.Queue(),
        provider=provider,
        controller=controller,
        circuit_breaker=CircuitBreaker(),
        retry_budget=RetryBudget(total_items=10, fraction=1.0),
        sink=_FakeSink(),
        counts=JobCounts(),
        usage=UsageTotals(),
        errors_by_class={},
        config=JobConfig(input_path="x", model="m", max_tokens=8, concurrency=1),
        abort_event=asyncio.Event(),
        cancel_event=asyncio.Event(),
        on_spend_check=_noop_spend_check,
        sleep=_fast_sleep,
    )
    kwargs.update(overrides)
    return WorkerContext(**kwargs)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_process_item_survives_unexpected_exception_from_provider() -> None:
    """A plain RuntimeError from the provider (not ProviderTransportError,
    not a modeled HTTP response) must not propagate out of process_item --
    it must be converted to a transient failure and, once attempts are
    exhausted, resolve to a terminal RowError.
    """
    provider = _RaisingProvider(RuntimeError("boom, not a ProviderTransportError"))
    ctx = _make_ctx(provider)
    ctx.counts.pending = 1  # the scheduler always increments this before enqueueing
    item = PromptItem(id="item-1", prompt="hello")

    await process_item(ctx, item)  # must return normally, never raise

    assert ctx.counts.in_flight == 0
    assert ctx.counts.succeeded == 0
    assert ctx.counts.failed == 1
    assert ctx.counts.pending == 0
    assert provider.calls == 5  # TRANSIENT's default max_attempts
    [emitted] = ctx.sink.submitted  # type: ignore[attr-defined]
    assert isinstance(emitted, RowError)
    assert emitted.failure_class == FailureClass.TRANSIENT_EXHAUSTED
    assert emitted.item_id == "item-1"


@pytest.mark.asyncio
async def test_process_item_in_flight_decremented_even_on_exception() -> None:
    """in_flight must never leak even if something above classify() throws
    -- this is why the decrement lives in a finally, not after the try.
    """
    provider = _RaisingProvider(RuntimeError("boom"))
    ctx = _make_ctx(provider)
    item = PromptItem(id="item-1", prompt="hello")

    await process_item(ctx, item)

    assert ctx.counts.in_flight == 0


@pytest.mark.asyncio
async def test_process_item_cancelled_error_still_propagates() -> None:
    """CancelledError must never be swallowed and converted into a retry --
    that would make the job un-cancellable.
    """
    provider = _RaisingProvider(asyncio.CancelledError())
    ctx = _make_ctx(provider)
    item = PromptItem(id="item-1", prompt="hello")

    with pytest.raises(asyncio.CancelledError):
        await process_item(ctx, item)


@pytest.mark.asyncio
async def test_worker_loop_emits_terminal_error_if_process_item_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Last-resort safety net: even if some other, unknown bug lets an
    exception escape process_item entirely, worker_loop must still resolve
    the item to a terminal RowError and still call task_done() -- not kill
    the worker task and leave the item (and the queue) unresolved.
    """
    import batchengine.core.worker as worker_module

    async def _always_raises(ctx: WorkerContext, item: PromptItem) -> None:
        raise ValueError("simulated bug that process_item's own net didn't catch")

    monkeypatch.setattr(worker_module, "process_item", _always_raises)

    ctx = _make_ctx(_RaisingProvider(RuntimeError("unused")))
    item = PromptItem(id="item-1", prompt="hello")
    await ctx.queue.put(item)
    await ctx.queue.put(SHUTDOWN)

    await asyncio.wait_for(worker_loop(ctx), timeout=5.0)

    assert ctx.counts.failed == 1
    [emitted] = ctx.sink.submitted  # type: ignore[attr-defined]
    assert isinstance(emitted, RowError)
    # queue.get() was called exactly twice (item, SHUTDOWN) and task_done()
    # exactly twice -- join() would hang forever if either call were missed.
    await asyncio.wait_for(ctx.queue.join(), timeout=5.0)


@pytest.mark.asyncio
async def test_worker_loop_task_done_called_for_row_error_from_ingest() -> None:
    """A RowError handed straight from ingest (malformed row) must also be
    task_done()'d -- covers the non-PromptItem branch of the same try/finally.
    """
    ctx = _make_ctx(_RaisingProvider(RuntimeError("unused")))
    error = RowError(
        item_id="bad-1", failure_class=FailureClass.INVALID_INPUT, message="x", attempt=1
    )
    await ctx.queue.put(error)
    await ctx.queue.put(SHUTDOWN)

    await asyncio.wait_for(worker_loop(ctx), timeout=5.0)

    assert ctx.counts.failed == 1
    await asyncio.wait_for(ctx.queue.join(), timeout=5.0)
