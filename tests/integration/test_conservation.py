"""The crown jewel (§7). Under every chaos configuration the mock provider
can produce, `succeeded + failed == ingested` must hold, and every item id
must appear exactly once in the output file. If this test suite has value
beyond all others, it's this one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from batchengine.providers.mock import MockProvider, MockProviderConfig
from tests.conftest import write_batch
from tests.integration.conftest import wait_for_terminal


@dataclass(slots=True)
class ConservationCase:
    chaos: MockProviderConfig
    n: int = 60
    rate_limit_rpm: int = 6000
    concurrency: int = 8
    timeout_s: float = 30.0


_CASES = [
    ConservationCase(MockProviderConfig(seed=1, p_429=0.0, p_500=0.0, p_400=0.0)),
    ConservationCase(MockProviderConfig(seed=2, p_429=0.4, p_500=0.0, p_400=0.0)),
    ConservationCase(MockProviderConfig(seed=3, p_429=0.0, p_500=0.4, p_400=0.0)),
    ConservationCase(MockProviderConfig(seed=4, p_429=0.0, p_500=0.0, p_400=0.4)),
    ConservationCase(
        MockProviderConfig(seed=5, p_429=0.2, p_500=0.2, p_400=0.1, malformed_response_rate=0.1)
    ),
    ConservationCase(MockProviderConfig(seed=6, p_429=0.3, p_500=0.3, p_400=0.0, p_timeout=0.1)),
    ConservationCase(MockProviderConfig(seed=7, hard_rpm_ceiling=20, window_s=1.0)),
    # An arbitrary, unmodeled exception from the provider (a bug, not a
    # classified HTTP/transport failure) must still resolve every item to a
    # terminal outcome -- this is the case that caught the critical defect
    # where anything but ProviderTransportError killed the worker task and
    # silently violated conservation.
    ConservationCase(MockProviderConfig(seed=8, p_unexpected_exception=0.15), timeout_s=45.0),
    # Scale: N=1,000 rather than 60 -- most of the configs above are small
    # enough that a single unlucky ordering wouldn't be representative.
    ConservationCase(
        MockProviderConfig(seed=9, p_429=0.1, p_500=0.1, p_400=0.05),
        n=1_000,
        concurrency=16,
        timeout_s=60.0,
    ),
    # Realistic rate limit (120 RPM, the real Tier 1 quota) combined with
    # chaos, so backpressure and conservation are exercised together --
    # every other case above sets rate_limit_rpm=6000, which disables the
    # client-side limiter as a practical constraint.
    ConservationCase(
        MockProviderConfig(seed=10, p_429=0.15, p_500=0.1),
        n=30,
        rate_limit_rpm=120,
        concurrency=6,
        timeout_s=45.0,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", _CASES, ids=lambda c: f"seed{c.chaos.seed}_n{c.n}_rpm{c.rate_limit_rpm}"
)
async def test_conservation_holds_under_every_chaos_configuration(
    app_client_factory, tmp_path: Path, case: ConservationCase
) -> None:
    p = tmp_path / f"batch_{case.chaos.seed}.json"
    write_batch(p, case.n)

    async with app_client_factory(rate_limit_rpm=case.rate_limit_rpm) as client:
        client.app_state.mock_provider_factory = lambda cfg=case.chaos: MockProvider(cfg)
        resp = await client.post(
            "/job", json={"input_path": str(p), "concurrency": case.concurrency}
        )
        job_id = resp.json()["job_id"]
        final = await wait_for_terminal(client, job_id, timeout=case.timeout_s)

        assert (
            final["counts"]["succeeded"] + final["counts"]["failed"] == final["counts"]["ingested"]
        )
        assert final["counts"]["ingested"] == case.n
        assert final["counts"]["in_flight"] == 0
        assert final["counts"]["pending"] == 0

        download = await client.get(f"/job/{job_id}/download")
        rows = [json.loads(line) for line in download.text.strip().splitlines() if line]
        assert len(rows) == case.n
        ids_seen = [r["item_id"] for r in rows]
        assert len(ids_seen) == len(set(ids_seen)), "every item id must appear exactly once"
        assert set(ids_seen) == {f"item-{i}" for i in range(case.n)}
