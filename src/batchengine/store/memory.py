"""In-memory JobStore. Used by the unit/integration test suite so tests never
touch a filesystem-backed database.
"""

from __future__ import annotations

from batchengine.core.models import JobRecord


class MemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}

    async def create(self, record: JobRecord) -> None:
        self._jobs[record.job_id] = record

    async def get(self, job_id: str) -> JobRecord | None:
        return self._jobs.get(job_id)

    async def save(self, record: JobRecord) -> None:
        self._jobs[record.job_id] = record

    async def list_ids(self) -> list[str]:
        return list(self._jobs.keys())
