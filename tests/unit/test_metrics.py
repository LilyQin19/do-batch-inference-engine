from __future__ import annotations

from batchengine.core.models import JobConfig, JobRecord, JobStatus
from batchengine.observability.metrics import render_prometheus_text


def test_render_prometheus_text_includes_job_metrics() -> None:
    record = JobRecord(
        job_id="job-1",
        config=JobConfig(input_path="x", model="m", max_tokens=8, concurrency=1),
        status=JobStatus.SUCCEEDED,
    )
    record.counts.ingested = 10
    record.counts.succeeded = 9
    record.counts.failed = 1
    record.backpressure = {
        "throttle_events_429": 2,
        "retries_issued": 3,
        "current_rate_limit_rps": 1.5,
    }
    record.usage.estimated_cost_usd = 0.01

    text = render_prometheus_text([record])

    assert 'batchengine_job_ingested_total{job_id="job-1",status="succeeded"} 10' in text
    assert 'batchengine_job_succeeded_total{job_id="job-1",status="succeeded"} 9' in text
    assert 'batchengine_job_failed_total{job_id="job-1",status="succeeded"} 1' in text
    assert 'batchengine_job_throttle_events_total{job_id="job-1",status="succeeded"} 2' in text
    assert "# HELP batchengine_job_estimated_cost_usd" in text
    assert "# TYPE batchengine_job_estimated_cost_usd gauge" in text


def test_render_prometheus_text_empty_records() -> None:
    text = render_prometheus_text([])
    assert "# HELP" in text
    assert "job_id=" not in text
