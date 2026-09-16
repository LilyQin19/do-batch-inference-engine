"""Proves /job/{id}/download is wired as a StreamingResponse (no
content-length precomputed -- FastAPI only omits that header for a response
whose body is an iterator, never for a materialized string/bytes body) and
correctly reproduces a large result set. httpx's in-process ASGITransport
does not preserve real wire-level TCP chunk framing, so it cannot prove
"arrives in multiple packets" the way a live server would; the RSS-vs-N
memory claim itself is measured out-of-process by scripts/memory_probe.py
and reported in the README (§6.6).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from batchengine.core.models import RowResult, UsageDelta
from batchengine.core.sink import ResultSink


async def _seed_result_file(path: Path, n: int) -> None:
    sink = ResultSink(path)
    await sink.start()
    for i in range(n):
        await sink.submit(
            RowResult(
                item_id=f"item-{i}",
                response_text="x" * 200,
                usage=UsageDelta(10, 10),
                latency_s=0.01,
                attempt=1,
            )
        )
    await sink.close()


@pytest.mark.asyncio
async def test_download_streams_in_multiple_chunks_without_content_length(
    app_client_factory, tmp_path: Path
) -> None:
    from batchengine.core.models import JobConfig, JobRecord, JobStatus

    n = 20_000
    result_path = tmp_path / "results" / "seeded.jsonl"
    await _seed_result_file(result_path, n)

    async with app_client_factory() as client:
        record = JobRecord(
            job_id="seeded",
            config=JobConfig(input_path="unused", model="mock", max_tokens=8, concurrency=1),
            status=JobStatus.SUCCEEDED,
            result_path=str(result_path),
        )
        record.counts.ingested = record.counts.succeeded = n
        await client.app_state.job_store.create(record)

        async with client.stream("GET", "/job/seeded/download") as resp:
            assert resp.status_code == 200
            assert "content-length" not in resp.headers  # streamed, not precomputed
            buf = ""
            async for chunk in resp.aiter_text():
                buf += chunk
            row_count = len([line for line in buf.splitlines() if line])

    assert row_count == n
