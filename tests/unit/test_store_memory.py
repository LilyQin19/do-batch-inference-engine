from __future__ import annotations

import pytest

from batchengine.core.models import JobConfig, JobRecord, JobStatus
from batchengine.store.memory import MemoryJobStore


@pytest.mark.asyncio
async def test_memory_store_create_get_save_list() -> None:
    store = MemoryJobStore()
    record = JobRecord(
        job_id="a",
        config=JobConfig(input_path="x", model="m", max_tokens=8, concurrency=1),
        status=JobStatus.QUEUED,
    )
    await store.create(record)

    fetched = await store.get("a")
    assert fetched is not None and fetched.job_id == "a"

    fetched.status = JobStatus.RUNNING
    await store.save(fetched)
    refetched = await store.get("a")
    assert refetched is not None and refetched.status == JobStatus.RUNNING

    assert await store.list_ids() == ["a"]
    assert await store.get("missing") is None
