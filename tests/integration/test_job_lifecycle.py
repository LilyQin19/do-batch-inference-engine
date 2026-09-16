from __future__ import annotations

import json
import time
from pathlib import Path

import httpx
import pytest

from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal


@pytest.mark.asyncio
async def test_submit_returns_202_under_50ms(app_client: httpx.AsyncClient, tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 1000)  # stat-only validation: size must not matter

    start = time.perf_counter()
    resp = await app_client.post("/job", json={"input_path": str(p)})
    elapsed_ms = (time.perf_counter() - start) * 1000

    assert resp.status_code == 202
    assert elapsed_ms < 50, f"POST /job took {elapsed_ms:.1f}ms, expected <50ms"
    body = resp.json()
    assert body["status"] == "queued"
    assert "job_id" in body


@pytest.mark.asyncio
async def test_submit_rejects_missing_file(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.post("/job", json={"input_path": "/does/not/exist.json"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_full_lifecycle_submit_poll_download(
    app_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 20)

    resp = await app_client.post("/job", json={"input_path": str(p), "concurrency": 4})
    job_id = resp.json()["job_id"]

    final = await wait_for_terminal(app_client, job_id)
    assert final["status"] == "succeeded"
    assert final["counts"]["ingested"] == 20
    assert final["counts"]["succeeded"] == 20
    assert final["counts"]["failed"] == 0
    assert final["progress_pct"] == 100.0
    assert "backpressure" in final and "usage" in final

    download = await app_client.get(f"/job/{job_id}/download")
    assert download.status_code == 200
    lines = [line for line in download.text.strip().splitlines() if line]
    assert len(lines) == 20
    rows = [json.loads(line) for line in lines]
    assert all(r["status"] == "success" for r in rows)


@pytest.mark.asyncio
async def test_download_json_array_format(app_client: httpx.AsyncClient, tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 5)
    resp = await app_client.post("/job", json={"input_path": str(p)})
    job_id = resp.json()["job_id"]
    await wait_for_terminal(app_client, job_id)

    download = await app_client.get(f"/job/{job_id}/download", params={"format": "json"})
    parsed = json.loads(download.text)
    assert isinstance(parsed, list)
    assert len(parsed) == 5


@pytest.mark.asyncio
async def test_download_running_job_returns_409_without_partial(
    app_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 5000)
    resp = await app_client.post("/job", json={"input_path": str(p)})
    job_id = resp.json()["job_id"]

    download = await app_client.get(f"/job/{job_id}/download")
    assert download.status_code == 409

    partial = await app_client.get(f"/job/{job_id}/download", params={"partial": "true"})
    assert partial.status_code == 200


@pytest.mark.asyncio
async def test_healthz(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_unknown_job_id_404s(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.get("/job/nonexistent/status")
    assert resp.status_code == 404
