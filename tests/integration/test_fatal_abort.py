"""§6.2's most-missed case: 401/402 must abort the whole job within a
handful of requests, never after burning the full batch."""

from __future__ import annotations

from pathlib import Path

import pytest

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal


@pytest.mark.asyncio
async def test_401_aborts_job_within_a_handful_of_requests(
    app_client_factory, tmp_path: Path
) -> None:
    n = 1000
    p = tmp_path / "batch.json"
    write_batch(p, n)

    async with app_client_factory(rate_limit_rpm=6000) as client:
        client.app_state.mock_provider_factory = lambda: MockProvider(
            MockProviderConfig(seed=1, fail_after_n=5, fatal_status=401)
        )
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 4})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=15.0)

    assert final["status"] == "failed"
    assert final["counts"]["succeeded"] + final["counts"]["failed"] == final["counts"]["ingested"]
    # The whole point: nowhere near all 1000 items should have been dispatched.
    assert final["counts"]["ingested"] < 100
    assert final["errors_by_class"].get("fatal_auth", 0) > 0


@pytest.mark.asyncio
async def test_402_billing_aborts_job(app_client_factory, tmp_path: Path) -> None:
    n = 1000
    p = tmp_path / "batch.json"
    write_batch(p, n)

    async with app_client_factory(rate_limit_rpm=6000) as client:
        client.app_state.mock_provider_factory = lambda: MockProvider(
            MockProviderConfig(seed=2, fail_after_n=3, fatal_status=402)
        )
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 4})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=15.0)

    assert final["status"] == "failed"
    assert final["counts"]["ingested"] < 100
    assert final["errors_by_class"].get("fatal_billing", 0) > 0
    assert final["abort_reason"] is not None and "fatal_billing" in final["abort_reason"]
