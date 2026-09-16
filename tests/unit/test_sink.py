from __future__ import annotations

import json
from pathlib import Path

import pytest

from batchengine.core.models import FailureClass, RowError, RowResult, UsageDelta
from batchengine.core.sink import ResultSink, replay


@pytest.mark.asyncio
async def test_sink_appends_success_and_error_rows(tmp_path: Path) -> None:
    path = tmp_path / "results.jsonl"
    sink = ResultSink(path)
    await sink.start()
    await sink.submit(
        RowResult(item_id="a", response_text="hi", usage=UsageDelta(1, 2), latency_s=0.1, attempt=1)
    )
    await sink.submit(
        RowError(item_id="b", failure_class=FailureClass.INVALID_INPUT, message="bad", attempt=1)
    )
    await sink.close()

    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    row_a = json.loads(lines[0])
    row_b = json.loads(lines[1])
    assert row_a["item_id"] == "a" and row_a["status"] == "success"
    assert row_b["item_id"] == "b" and row_b["status"] == "error"


@pytest.mark.asyncio
async def test_sink_flushes_before_close_even_under_batch_threshold(tmp_path: Path) -> None:
    path = tmp_path / "results.jsonl"
    sink = ResultSink(path)
    await sink.start()
    for i in range(5):  # far below the 100-row flush batch
        await sink.submit(
            RowResult(
                item_id=str(i), response_text="x", usage=UsageDelta(1, 1), latency_s=0.01, attempt=1
            )
        )
    await sink.close()
    assert len(path.read_text().strip().splitlines()) == 5


@pytest.mark.asyncio
async def test_replay_rebuilds_counts_from_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "results.jsonl"
    sink = ResultSink(path)
    await sink.start()
    await sink.submit(
        RowResult(item_id="a", response_text="x", usage=UsageDelta(1, 1), latency_s=0.01, attempt=1)
    )
    await sink.submit(
        RowResult(item_id="b", response_text="x", usage=UsageDelta(1, 1), latency_s=0.01, attempt=1)
    )
    await sink.submit(
        RowError(
            item_id="c", failure_class=FailureClass.TRANSIENT_EXHAUSTED, message="x", attempt=5
        )
    )
    await sink.close()

    state = replay(path)
    assert state.succeeded == 2
    assert state.failed == 1
    assert state.seen_item_ids == {"a", "b", "c"}


def test_replay_on_missing_file_returns_empty_state(tmp_path: Path) -> None:
    state = replay(tmp_path / "does_not_exist.jsonl")
    assert state.succeeded == 0
    assert state.failed == 0


@pytest.mark.asyncio
async def test_replay_tolerates_torn_last_line(tmp_path: Path) -> None:
    path = tmp_path / "results.jsonl"
    sink = ResultSink(path)
    await sink.start()
    await sink.submit(
        RowResult(item_id="a", response_text="x", usage=UsageDelta(1, 1), latency_s=0.01, attempt=1)
    )
    await sink.close()
    with open(path, "a", encoding="utf-8") as f:
        f.write('{"item_id": "torn", "status": "suc')  # simulate a crash mid-write

    state = replay(path)
    assert state.succeeded == 1
    assert state.failed == 0
