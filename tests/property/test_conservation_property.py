"""Property test (§7): the conservation invariant across randomized chaos
parameters, not just the handful of fixed configurations in
tests/integration/test_conservation.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal

pytestmark = pytest.mark.asyncio


@settings(
    max_examples=15, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    p_429=st.floats(min_value=0.0, max_value=0.5),
    p_500=st.floats(min_value=0.0, max_value=0.5),
    p_400=st.floats(min_value=0.0, max_value=0.3),
    p_timeout=st.floats(min_value=0.0, max_value=0.2),
    malformed=st.floats(min_value=0.0, max_value=0.2),
    seed=st.integers(min_value=0, max_value=10_000),
)
async def test_conservation_holds_for_arbitrary_chaos_mix(
    app_client_factory,
    tmp_path: Path,
    p_429: float,
    p_500: float,
    p_400: float,
    p_timeout: float,
    malformed: float,
    seed: int,
) -> None:
    n = 25
    p = tmp_path / f"batch_{seed}.json"
    write_batch(p, n)

    cfg = MockProviderConfig(
        seed=seed,
        p_429=p_429,
        p_500=p_500,
        p_400=p_400,
        p_timeout=p_timeout,
        malformed_response_rate=malformed,
    )

    async with app_client_factory(rate_limit_rpm=12000) as client:
        client.app_state.mock_provider_factory = lambda cfg=cfg: MockProvider(cfg)
        resp = await client.post("/job", json={"input_path": str(p), "concurrency": 8})
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=30.0)

        assert final["counts"]["ingested"] == n
        assert final["counts"]["succeeded"] + final["counts"]["failed"] == n
        assert final["counts"]["in_flight"] == 0
        assert final["counts"]["pending"] == 0
