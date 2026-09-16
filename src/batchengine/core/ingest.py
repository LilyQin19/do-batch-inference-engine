"""Streaming input reader (§6.3). The whole memory-boundedness story starts
here: `ijson.items()` yields one row at a time from disk, so `POST /job`
never holds N rows in memory regardless of whether N is 1,000 or 500,000.
`json.load()` is banned in this module on purpose -- see docs/decisions.md.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path

import ijson

from batchengine.core.models import FailureClass, PromptItem, RowError

_PEEK_BYTES = 64


def _sniff_is_json_array(path: Path) -> bool:
    with open(path, "rb") as f:
        chunk = f.read(_PEEK_BYTES)
    for byte in chunk:
        ch = chr(byte)
        if ch.isspace():
            continue
        return ch == "["
    return False  # empty file: treat as (empty) JSONL


def _row_to_item(raw: object, offset: int) -> PromptItem | RowError:
    if not isinstance(raw, dict) or "prompt" not in raw or not isinstance(raw["prompt"], str):
        return RowError(
            item_id=str(raw.get("id"))
            if isinstance(raw, dict) and "id" in raw
            else f"offset-{offset}",
            failure_class=FailureClass.INVALID_INPUT,
            message="row missing a string 'prompt' field",
            attempt=1,
        )
    item_id = str(raw.get("id", f"offset-{offset}"))
    return PromptItem(id=item_id, prompt=raw["prompt"], raw=raw)


def iter_items_sync(path: str | Path) -> Iterator[PromptItem | RowError]:
    """Synchronous generator -- the actual streaming parse. Kept separate
    from the async wrapper below so it's trivially unit-testable without an
    event loop.
    """
    p = Path(path)
    is_array = _sniff_is_json_array(p)

    if is_array:
        with open(p, "rb") as f:
            offset = 0
            # A JSON-syntax error here aborts the whole parse -- that's a
            # property of JSON (one malformed brace corrupts the document
            # structure, not just one row), not a memory-boundedness gap.
            # JSONL mode below tolerates row-level corruption because each
            # line is an independent JSON document.
            for raw in ijson.items(f, "item"):
                yield _row_to_item(raw, offset)
                offset += 1
    else:
        with open(p, encoding="utf-8") as f:
            for offset, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    yield RowError(
                        item_id=f"line-{offset}",
                        failure_class=FailureClass.INVALID_INPUT,
                        message=f"malformed JSON line: {exc}",
                        attempt=1,
                    )
                    continue
                yield _row_to_item(raw, offset)


async def stream_items(path: str | Path) -> AsyncGenerator[PromptItem | RowError, None]:
    """Async bridge over `iter_items_sync`. Runs the sync/blocking ijson
    parse in a worker thread and forwards results through a small bounded
    queue, so the event loop is never blocked on file I/O and the producer
    thread is itself backpressured by a slow consumer (queue.put blocks the
    thread once the queue is full).
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[object] = asyncio.Queue(maxsize=64)
    sentinel = object()
    error_box: list[BaseException] = []
    # Cooperative stop signal for the producer thread. Without this, an
    # early consumer exit (job cancelled/aborted partway through a large
    # file) still leaves the producer blindly iterating and cross-thread
    # round-tripping *every remaining row* -- for a 1,000-2,000 row file
    # aborted near the start, that's ~1,000+ `run_coroutine_threadsafe(...)
    # .result()` round-trips that buy nothing, since nothing is consuming
    # them anymore. Each round-trip is cheap in isolation, but under
    # coverage instrumentation on a CPU-constrained runner they compound
    # into minutes -- the suspected cause of an intermittent multi-minute
    # CI hang localized to exactly the two tests that cancel/abort a
    # large file (see BUILD_LOG.md). Checked once per row, so at most one
    # more row is attempted after the signal is set.
    stop = threading.Event()

    def producer() -> None:
        try:
            for result in iter_items_sync(path):
                if stop.is_set():
                    return
                asyncio.run_coroutine_threadsafe(queue.put(result), loop).result()
        except BaseException as exc:  # noqa: BLE001 -- forwarded to the consumer below
            error_box.append(exc)
        finally:
            asyncio.run_coroutine_threadsafe(queue.put(sentinel), loop).result()

    future = loop.run_in_executor(None, producer)
    try:
        while True:
            result = await queue.get()
            if result is sentinel:
                break
            yield result  # type: ignore[misc]
    finally:
        # If the consumer stops early, tell the producer to stop reading
        # more rows, then drain whatever's already in the queue so a
        # producer blocked inside queue.put() (waiting for space) can
        # finish instead of leaking a thread.
        stop.set()
        while not future.done():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0)
        await future
    if error_box:
        raise error_box[0]
