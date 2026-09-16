from __future__ import annotations

import json
from pathlib import Path

import pytest

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal


@pytest.mark.asyncio
async def test_mixed_400_and_500_partitions_success_and_error(
    app_client_factory, tmp_path: Path
) -> None:
    n = 50
    p = tmp_path / "batch.json"
    write_batch(p, n)

    async with app_client_factory(rate_limit_rpm=6000) as client:
        # p_400 is terminal (invalid_input), p_500 is retried up to 5 attempts
        # then transient_exhausted if it never succeeds. With p_500 this low,
        # essentially all 500s eventually succeed on retry.
        client.app_state.mock_provider_factory = lambda: MockProvider(
            MockProviderConfig(seed=99, p_400=0.15, p_500=0.05)
        )
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 8})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=30.0)

        assert final["status"] in ("partial", "succeeded")
        assert final["counts"]["succeeded"] + final["counts"]["failed"] == n
        assert (
            final["counts"]["failed"] > 0
        )  # p_400=0.15 over 50 items should produce some terminal errors
        assert final["errors_by_class"].get("invalid_input", 0) > 0

        download = await client.get(f"/job/{job_id}/download", params={"include": "errors"})
        error_rows = [json.loads(line) for line in download.text.strip().splitlines() if line]
        assert len(error_rows) == final["counts"]["failed"]
        assert all(r["status"] == "error" for r in error_rows)
        assert all(r["failure_class"] for r in error_rows)

        success_dl = await client.get(f"/job/{job_id}/download", params={"include": "success"})
        success_rows = [json.loads(line) for line in success_dl.text.strip().splitlines() if line]
        assert len(success_rows) == final["counts"]["succeeded"]
