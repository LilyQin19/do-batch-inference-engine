"""SQLite-backed JobStore. Gives the app durable job tracking across process
restarts (§2) without requiring an external database for a take-home-scale
service. A job's authoritative progress still lives in the JSONL result file
(§6.4/replay) -- this store persists metadata/status/counters so
`GET /job/{id}/status` survives a restart too.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import aiosqlite

from batchengine.core.models import (
    JobConfig,
    JobCounts,
    JobRecord,
    JobStatus,
    JobTiming,
    UsageTotals,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
"""


def _record_to_json(record: JobRecord) -> str:
    payload = {
        "job_id": record.job_id,
        "config": dataclasses.asdict(record.config),
        "status": record.status.value,
        "submitted_at": record.submitted_at,
        "counts": dataclasses.asdict(record.counts),
        "timing": dataclasses.asdict(record.timing),
        "usage": dataclasses.asdict(record.usage),
        "backpressure": record.backpressure,
        "errors_by_class": record.errors_by_class,
        "result_path": record.result_path,
        "abort_reason": record.abort_reason,
    }
    return json.dumps(payload)


def _record_from_json(raw: str) -> JobRecord:
    d = json.loads(raw)
    return JobRecord(
        job_id=d["job_id"],
        config=JobConfig(**d["config"]),
        status=JobStatus(d["status"]),
        submitted_at=d["submitted_at"],
        counts=JobCounts(**d["counts"]),
        timing=JobTiming(**d["timing"]),
        usage=UsageTotals(**d["usage"]),
        backpressure=d["backpressure"],
        errors_by_class=d["errors_by_class"],
        result_path=d["result_path"],
        abort_reason=d["abort_reason"],
    )


class SqliteJobStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    async def init(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(_SCHEMA)
            await db.commit()

    async def create(self, record: JobRecord) -> None:
        await self.save(record)

    async def get(self, job_id: str) -> JobRecord | None:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT data FROM jobs WHERE job_id = ?", (job_id,))
            row = await cursor.fetchone()
            if row is None:
                return None
            return _record_from_json(row[0])

    async def save(self, record: JobRecord) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO jobs (job_id, data) VALUES (?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET data = excluded.data",
                (record.job_id, _record_to_json(record)),
            )
            await db.commit()

    async def list_ids(self) -> list[str]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT job_id FROM jobs")
            rows = await cursor.fetchall()
            return [r[0] for r in rows]
