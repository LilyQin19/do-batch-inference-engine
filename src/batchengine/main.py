"""App factory + lifespan. Owns the process-wide httpx.AsyncClient and the
SQLite job store -- both created once here and threaded through app.state,
per the "one shared client per process" rule (§13).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from batchengine.api.routes import router
from batchengine.app_state import AppState
from batchengine.config import get_settings
from batchengine.observability.logging import configure_logging
from batchengine.store.sqlite import SqliteJobStore


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
