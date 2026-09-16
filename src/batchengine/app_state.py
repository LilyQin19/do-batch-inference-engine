"""Process-wide app state, split out from main.py so api/routes.py can
import the type without a main.py <-> routes.py import cycle (main.py
imports the router; routes.py needs the state's shape).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx

from batchengine.config import Settings
from batchengine.core.scheduler import JobRunner
from batchengine.providers.base import InferenceProvider
from batchengine.store.sqlite import SqliteJobStore


class AppState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.http_client: httpx.AsyncClient | None = None
        self.job_store: SqliteJobStore | None = None
        self.runners: dict[str, JobRunner] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}
        # Test seam only: lets the integration/property suite drive the
        # mock provider's chaos knobs (p_429, hard_rpm_ceiling, ...) through
        # the real HTTP API instead of a bare MockProviderConfig(). Never
        # set outside tests -- a real deployment has no code path that sets it.
        self.mock_provider_factory: Callable[[], InferenceProvider] | None = None
