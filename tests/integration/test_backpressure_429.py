from __future__ import annotations

from pathlib import Path

import pytest

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal


@pytest.mark.asyncio
async def test_hard_rpm_ceiling_conservation_and_rate_convergence(
    app_client_factory, tmp_path: Path
) -> None:
    """Client configured well above the mock's hard 429 ceiling: this must
    produce real 429s, retries, and the AIMD controller backing off toward
    the ceiling. "Zero dropped items" means conservation holds -- every item
    lands in succeeded or failed -- not that every item necessarily
    succeeds: the global retry budget (§6.2) is designed to let some items
    exhaust into `transient_exhausted` under a sustained storm rather than
    retry-amplify a struggling upstream forever.
    """
    n = 60
    ceiling = 20
    p = tmp_path / "batch.json"
    write_batch(p, n)

    async with app_client_factory(
        rate_limit_rpm=6000
    ) as client:  # client configured far above the ceiling
        client.app_state.mock_provider_factory = lambda: MockProvider(
            MockProviderConfig(seed=42, hard_rpm_ceiling=ceiling, window_s=1.0)
        )
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 16})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=30.0)

    assert final["counts"]["succeeded"] + final["counts"]["failed"] == n  # nothing dropped
    assert final["counts"]["succeeded"] > 0
    assert final["backpressure"]["throttle_events_429"] > 0
    assert final["backpressure"]["retries_issued"] > 0
    # AIMD should have adapted the rate down from its 6000rpm=100rps start.
    assert final["backpressure"]["current_rate_limit_rps"] < 100.0
    if final["counts"]["failed"] > 0:
        assert final["errors_by_class"].get("transient_exhausted", 0) == final["counts"]["failed"]


@pytest.mark.asyncio
async def test_littles_law_optimal_concurrency_reported(app_client_factory, tmp_path: Path) -> None:
    n = 40
    p = tmp_path / "batch.json"
    write_batch(p, n)

    async with app_client_factory(rate_limit_rpm=6000) as client:
        client.app_state.mock_provider_factory = lambda: MockProvider(
            MockProviderConfig(
                seed=7,
                hard_rpm_ceiling=10,
                window_s=1.0,
                simulate_delay=True,
                latency_mean=0.05,
                latency_jitter=0.01,
            )
        )
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 32})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=30.0)

    assert final["counts"]["succeeded"] + final["counts"]["failed"] == n
    # configured concurrency (32) should be well above what Little's Law says
    # is actually needed at the converged rate/latency -- the over-provisioning
    # warning path (scheduler.py) is exercised whenever this ratio exceeds 2x.
    assert final["backpressure"]["littles_law_optimal_concurrency"] >= 1
    assert final["backpressure"]["littles_law_optimal_concurrency"] < 32
