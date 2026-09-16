"""App factory + lifespan. Owns the process-wide httpx.AsyncClient and the
SQLite job store -- both created once here and threaded through app.state,
per the "one shared client per process" rule (§13).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI

from batchengine.api.routes import router
from batchengine.config import Settings, get_settings
from batchengine.core.scheduler import JobRunner
from batchengine.observability.logging import configure_logging
from batchengine.store.sqlite import SqliteJobStore


class AppState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.http_client: httpx.AsyncClient | None = None
        self.job_store: SqliteJobStore | None = None
        self.runners: dict[str, JobRunner] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging()
    state = AppState(settings)
    state.http_client = httpx.AsyncClient(http2=True, timeout=60.0)
    state.job_store = SqliteJobStore(settings.batchengine_db_path)
    await state.job_store.init()
    app.state.app_state = state
    try:
        yield
    finally:
        for task in list(state.tasks.values()):
            if not task.done():
                task.cancel()
        await asyncio.gather(*state.tasks.values(), return_exceptions=True)
        await state.http_client.aclose()


def create_app() -> FastAPI:
    app = FastAPI(title="Batch Inference Engine", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
