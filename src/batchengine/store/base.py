"""JobStore Protocol. The scheduler talks to this, not to SQLite or a dict
directly, so `MemoryJobStore` can back the test suite and `SqliteJobStore`
can back the running app without either leaking into the scheduler.
"""

from __future__ import annotations

from typing import Protocol

from batchengine.core.models import JobRecord


class JobStore(Protocol):
    async def create(self, record: JobRecord) -> None: ...

    async def get(self, job_id: str) -> JobRecord | None: ...

    async def save(self, record: JobRecord) -> None:
        """Upsert the full record. Called on status transitions and
        periodically during a run -- not on every single row, which would
        turn the store into the bottleneck.
        """
        ...

    async def list_ids(self) -> list[str]: ...
