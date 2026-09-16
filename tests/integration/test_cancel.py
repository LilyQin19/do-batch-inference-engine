from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal, wait_until


@pytest.mark.asyncio
async def test_cancel_drains_gracefully_with_conservation_intact(
    app_client_factory, tmp_path: Path
) -> None:
    n = 2000
    p = tmp_path / "batch.json"
    write_batch(p, n)

    async with app_client_factory(
        rate_limit_rpm=600
    ) as client:  # slow enough to still be running when we cancel
        client.app_state.mock_provider_factory = lambda: MockProvider(MockProviderConfig(seed=3))
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 8})
        job_id = resp.json()["job_id"]

        await wait_until(lambda: _synchronous_status(client, job_id) == "running", timeout=5.0)

        cancel_resp = await client.post(f"/job/{job_id}/cancel")
        assert cancel_resp.status_code == 200

        final = await wait_for_terminal(client, job_id, timeout=15.0)

    assert final["status"] == "cancelled"
    assert final["counts"]["succeeded"] + final["counts"]["failed"] == final["counts"]["ingested"]
    assert (
        final["counts"]["ingested"] < n
    )  # proves it actually stopped early, not just finished naturally
    assert final["counts"]["in_flight"] == 0
    assert final["counts"]["pending"] == 0


def _synchronous_status(client, job_id: str) -> str | None:
    runner = client.app_state.runners.get(job_id)
    return runner.record.status.value if runner else None


@pytest.mark.asyncio
async def test_no_orphaned_tasks_after_cancel(app_client_factory, tmp_path: Path) -> None:
    n = 500
    p = tmp_path / "batch.json"
    write_batch(p, n)

    tasks_before = len(asyncio.all_tasks())

    async with app_client_factory(rate_limit_rpm=600) as client:
        client.app_state.mock_provider_factory = lambda: MockProvider(MockProviderConfig(seed=4))
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 8})
        job_id = resp.json()["job_id"]
        await wait_until(lambda: _synchronous_status(client, job_id) == "running", timeout=5.0)
        await client.post(f"/job/{job_id}/cancel")
        await wait_for_terminal(client, job_id, timeout=15.0)
        # Give the event loop one tick to let worker tasks fully unwind.
        await asyncio.sleep(0)

    tasks_after = len(asyncio.all_tasks())
    assert tasks_after <= tasks_before + 1  # the test's own task, no leaked workers
