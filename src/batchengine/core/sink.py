"""Single-writer append-only JSONL sink (§6.4). This is the third leg of the
O(concurrency) memory story: results are written to disk as they arrive and
only counters are kept in memory -- never a results list (that path alone
costs ~750MB at N=500,000; see §6.6 and docs/scaling.md).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from batchengine.core.models import RowError, RowResult

_FLUSH_EVERY_ROWS = 100
_FLUSH_EVERY_SECONDS = 2.0


def _result_line(result: RowResult) -> dict[str, object]:
    return {
        "item_id": result.item_id,
        "status": "success",
        "response_text": result.response_text,
        "input_tokens": result.usage.input_tokens,
        "output_tokens": result.usage.output_tokens,
        "latency_s": result.latency_s,
        "attempt": result.attempt,
    }


def _error_line(error: RowError) -> dict[str, object]:
    return {
        "item_id": error.item_id,
        "status": "error",
        "failure_class": error.failure_class.value,
        "message": error.message,
        "attempt": error.attempt,
        "http_status": error.http_status,
    }


@dataclass(slots=True)
class ReplayState:
    succeeded: int = 0
    failed: int = 0
    seen_item_ids: set[str] | None = None


def replay(path: str | Path) -> ReplayState:
    """Rebuild counts (and, optionally, the set of already-processed item
    ids) by reading the existing JSONL result file. Used on process restart
    so a job resumes from the last completed offset instead of redoing --
    or worse, losing track of -- already-finished work.
    """
    p = Path(path)
    state = ReplayState(seen_item_ids=set())
    if not p.exists():
        return state
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue  # last line may be a torn write from a crash; ignore it
            assert state.seen_item_ids is not None
            state.seen_item_ids.add(str(row.get("item_id")))
            if row.get("status") == "success":
                state.succeeded += 1
            else:
                state.failed += 1
    return state


class ResultSink:
    """Owns the one file handle for a job's result file. Everything writes
    through `submit()`; a single background task does the actual I/O so
    concurrent workers never race on the file.
    """

    def __init__(self, path: str | Path, clock: "type[time]" = time) -> None:
        self.path = Path(path)
        self._queue: asyncio.Queue[RowResult | RowError | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._clock = clock
        self._fh: TextIO | None = None

    async def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        self._task = asyncio.create_task(self._run())

    async def submit(self, item: RowResult | RowError) -> None:
        await self._queue.put(item)

    async def close(self) -> None:
        await self._queue.put(None)
        if self._task is not None:
            await self._task
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()

    async def _run(self) -> None:
        assert self._fh is not None
        unflushed = 0
        last_flush = self._clock.monotonic()
        while True:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=_FLUSH_EVERY_SECONDS)
            except asyncio.TimeoutError:
                item = "TIMEOUT"  # type: ignore[assignment]

            if item is None:
                self._flush()
                return

            if item != "TIMEOUT":
                line = _result_line(item) if isinstance(item, RowResult) else _error_line(item)
                self._fh.write(json.dumps(line) + "\n")
                unflushed += 1

            now = self._clock.monotonic()
            if unflushed >= _FLUSH_EVERY_ROWS or (unflushed > 0 and now - last_flush >= _FLUSH_EVERY_SECONDS):
                self._flush()
                unflushed = 0
                last_flush = now

    def _flush(self) -> None:
        assert self._fh is not None
        self._fh.flush()
        os.fsync(self._fh.fileno())
