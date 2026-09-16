"""The crown jewel (§7). Under every chaos configuration the mock provider
can produce, `succeeded + failed == ingested` must hold, and every item id
must appear exactly once in the output file. If this test suite has value
beyond all others, it's this one.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal

_CHAOS_CONFIGS = [
    MockProviderConfig(seed=1, p_429=0.0, p_500=0.0, p_400=0.0),
    MockProviderConfig(seed=2, p_429=0.4, p_500=0.0, p_400=0.0),
    MockProviderConfig(seed=3, p_429=0.0, p_500=0.4, p_400=0.0),
    MockProviderConfig(seed=4, p_429=0.0, p_500=0.0, p_400=0.4),
    MockProviderConfig(seed=5, p_429=0.2, p_500=0.2, p_400=0.1, malformed_response_rate=0.1),
    MockProviderConfig(seed=6, p_429=0.3, p_500=0.3, p_400=0.0, p_timeout=0.1),
    MockProviderConfig(seed=7, hard_rpm_ceiling=20, window_s=1.0),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("chaos", _CHAOS_CONFIGS, ids=lambda c: f"seed{c.seed}")
async def test_conservation_holds_under_every_chaos_configuration(
    app_client_factory, tmp_path: Path, chaos: MockProviderConfig
) -> None:
    n = 60
    p = tmp_path / f"batch_{chaos.seed}.json"
    write_batch(p, n)

    async with app_client_factory(rate_limit_rpm=6000) as client:
        client.app_state.mock_provider_factory = lambda cfg=chaos: MockProvider(cfg)
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 8})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=30.0)

        assert (
            final["counts"]["succeeded"] + final["counts"]["failed"] == final["counts"]["ingested"]
        )
        assert final["counts"]["ingested"] == n
        assert final["counts"]["in_flight"] == 0
        assert final["counts"]["pending"] == 0

        download = await client.get(f"/job/{job_id}/download")
        rows = [json.loads(line) for line in download.text.strip().splitlines() if line]
        assert len(rows) == n
        ids_seen = [r["item_id"] for r in rows]
        assert len(ids_seen) == len(set(ids_seen)), "every item id must appear exactly once"
        assert set(ids_seen) == {f"item-{i}" for i in range(n)}
