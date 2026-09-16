from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio

from batchengine.main import create_app


def write_batch(path: Path, n: int, as_array: bool = True) -> None:
    rows = [{"id": f"item-{i}", "prompt": f"prompt number {i}"} for i in range(n)]
    if as_array:
        path.write_text(json.dumps(rows))
    else:
        path.write_text("\n".join(json.dumps(r) for r in rows))


@pytest.fixture
def sample_batch(tmp_path: Path) -> Path:
    p = tmp_path / "batch.json"
    write_batch(p, 20)
    return p


_TERMINAL = {"succeeded", "partial", "failed", "cancelled"}


async def wait_for_terminal(
    client: httpx.AsyncClient, job_id: str, timeout: float = 20.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = await client.get(f"/job/{job_id}/status")
        body = resp.json()
        if body["status"] in _TERMINAL:
            return body
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} did not reach a terminal state within {timeout}s")


async def wait_until(
    predicate: Callable[[], Any], timeout: float = 10.0, interval: float = 0.02
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


@pytest_asyncio.fixture
async def app_client_factory(tmp_path: Path, monkeypatch):
    """Factory so individual tests can control env-derived settings (e.g. a
    higher client-side rate limit than the mock's hard_rpm_ceiling, to force
    throttling) before the app's lifespan reads them. Shared across
    integration and property tests -- both drive the real HTTP surface.
    """
    monkeypatch.delenv("DO_INFERENCE_KEY", raising=False)
    monkeypatch.setenv("BATCHENGINE_DB_PATH", str(tmp_path / "jobs.db"))
    monkeypatch.setenv("BATCHENGINE_RESULTS_DIR", str(tmp_path / "results"))
    monkeypatch.setenv("BATCHENGINE_SPEND_LEDGER_PATH", str(tmp_path / "ledger.json"))
    monkeypatch.setenv(
        "BATCHENGINE_RATE_LIMIT_RPM", "6000"
    )  # fast by default; tests override to force throttling
    monkeypatch.setenv(
        "BATCHENGINE_CIRCUIT_COOLDOWN_S", "0.2"
    )  # keep chaos tests from waiting out a real 30s cooldown

    @asynccontextmanager
    async def _factory(**env_overrides: object) -> AsyncIterator[httpx.AsyncClient]:
        for key, value in env_overrides.items():
            monkeypatch.setenv(key.upper(), str(value))
        app = create_app()
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                client.app_state = app.state.app_state  # type: ignore[attr-defined]
                yield client

    return _factory


@pytest_asyncio.fixture
async def app_client(app_client_factory) -> AsyncIterator[httpx.AsyncClient]:
    async with app_client_factory() as client:
        yield client
