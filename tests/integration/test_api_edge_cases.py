from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal


@pytest.mark.asyncio
async def test_submit_rejects_unreadable_webhook_url(
    app_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 3)
    resp = await app_client.post("/job", json={"input_path": str(p), "webhook_url": "not-a-url"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_cancel_unknown_job_404s(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.post("/job/does-not-exist/cancel")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_download_unknown_job_404s(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.get("/job/does-not-exist/download")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_metrics_endpoint_reports_job(app_client: httpx.AsyncClient, tmp_path: Path) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 3)
    resp = await app_client.post("/job", json={"input_path": str(p)})
    job_id = resp.json()["job_id"]
    await wait_for_terminal(app_client, job_id)

    metrics = await app_client.get("/metrics")
    assert metrics.status_code == 200
    assert f'job_id="{job_id}"' in metrics.text
    assert "batchengine_job_ingested_total" in metrics.text


@pytest.mark.asyncio
async def test_cancel_after_job_already_finished_returns_409_or_ok(
    app_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    p = tmp_path / "batch.json"
    write_batch(p, 2)
    resp = await app_client.post("/job", json={"input_path": str(p)})
    job_id = resp.json()["job_id"]
    await wait_for_terminal(app_client, job_id)

    cancel_resp = await app_client.post(f"/job/{job_id}/cancel")
    # The runner is still tracked in-process even after finishing, so cancel
    # is accepted but has no effect on an already-terminal job.
    assert cancel_resp.status_code == 200
    status = await app_client.get(f"/job/{job_id}/status")
    assert status.json()["status"] in ("succeeded", "partial", "failed", "cancelled")
