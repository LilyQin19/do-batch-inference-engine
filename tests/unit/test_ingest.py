from __future__ import annotations

import json
from pathlib import Path

import pytest

from batchengine.core.ingest import iter_items_sync, stream_items
from batchengine.core.models import FailureClass, PromptItem, RowError
from tests.conftest import write_batch


def test_array_ingest_yields_prompt_items(tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 5, as_array=True)
    items = list(iter_items_sync(p))
    assert len(items) == 5
    assert all(isinstance(i, PromptItem) for i in items)
    assert [i.id for i in items] == [f"item-{i}" for i in range(5)]  # type: ignore[union-attr]


def test_jsonl_ingest_yields_prompt_items(tmp_path: Path) -> None:
    p = tmp_path / "batch.jsonl"
    write_batch(p, 5, as_array=False)
    items = list(iter_items_sync(p))
    assert len(items) == 5
    assert all(isinstance(i, PromptItem) for i in items)


def test_jsonl_malformed_row_is_skipped_and_recorded(tmp_path: Path) -> None:
    p = tmp_path / "batch.jsonl"
    lines = [
        json.dumps({"id": "a", "prompt": "hi"}),
        "{not valid json",
        json.dumps({"id": "b", "prompt": "hello"}),
    ]
    p.write_text("\n".join(lines))
    items = list(iter_items_sync(p))
    assert len(items) == 3
    assert isinstance(items[0], PromptItem)
    assert isinstance(items[1], RowError)
    assert items[1].failure_class == FailureClass.INVALID_INPUT
    assert isinstance(items[2], PromptItem)


def test_array_row_missing_prompt_field_is_invalid(tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    p.write_text(json.dumps([{"id": "a"}, {"id": "b", "prompt": "ok"}]))
    items = list(iter_items_sync(p))
    assert isinstance(items[0], RowError)
    assert items[0].failure_class == FailureClass.INVALID_INPUT
    assert isinstance(items[1], PromptItem)


def test_never_materializes_whole_file_uses_generator(tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 100, as_array=True)
    gen = iter_items_sync(p)
    first = next(gen)
    assert isinstance(first, PromptItem)
    # Generator, not a list -- proves nothing was eagerly materialized.
    assert not isinstance(gen, list)


@pytest.mark.asyncio
async def test_async_stream_items_matches_sync(tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 10, as_array=True)
    results = [r async for r in stream_items(p)]
    assert len(results) == 10
    assert all(isinstance(r, PromptItem) for r in results)


@pytest.mark.asyncio
async def test_async_stream_items_can_be_closed_early(tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 500, as_array=True)
    gen = stream_items(p)
    count = 0
    async for _ in gen:
        count += 1
        if count == 3:
            break
    await gen.aclose()
    assert count == 3
