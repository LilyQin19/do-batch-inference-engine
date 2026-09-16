"""Hand-rolled Prometheus text exposition (no `prometheus_client` dependency
needed for a handful of gauges). Renders straight from JobStore records, so
`/metrics` reflects the same counters `/job/{id}/status` does -- one source
of truth, two views.
"""

from __future__ import annotations

from batchengine.core.models import JobRecord

_HELP = {
    "batchengine_job_ingested_total": "Items ingested for a job.",
    "batchengine_job_succeeded_total": "Items that completed successfully.",
    "batchengine_job_failed_total": "Items that terminally failed.",
    "batchengine_job_in_flight": "Items currently in flight to the provider.",
    "batchengine_job_throttle_events_total": "429 responses observed.",
    "batchengine_job_retries_issued_total": "Retries issued.",
    "batchengine_job_current_rate_limit_rps": "Current AIMD-controlled request rate.",
    "batchengine_job_estimated_cost_usd": "Estimated spend for a job so far.",
}

_TYPE = {
    "batchengine_job_ingested_total": "counter",
    "batchengine_job_succeeded_total": "counter",
    "batchengine_job_failed_total": "counter",
    "batchengine_job_in_flight": "gauge",
    "batchengine_job_throttle_events_total": "counter",
    "batchengine_job_retries_issued_total": "counter",
    "batchengine_job_current_rate_limit_rps": "gauge",
    "batchengine_job_estimated_cost_usd": "gauge",
}


def render_prometheus_text(records: list[JobRecord]) -> str:
    lines: list[str] = []
    for metric, help_text in _HELP.items():
        lines.append(f"# HELP {metric} {help_text}")
        lines.append(f"# TYPE {metric} {_TYPE[metric]}")
        for record in records:
            label = f'job_id="{record.job_id}",status="{record.status.value}"'
            value = _value_for(metric, record)
            lines.append(f"{metric}{{{label}}} {value}")
    return "\n".join(lines) + "\n"


def _value_for(metric: str, record: JobRecord) -> float:
    bp = record.backpressure or {}
    values: dict[str, float] = {
        "batchengine_job_ingested_total": record.counts.ingested,
        "batchengine_job_succeeded_total": record.counts.succeeded,
        "batchengine_job_failed_total": record.counts.failed,
        "batchengine_job_in_flight": record.counts.in_flight,
        "batchengine_job_throttle_events_total": bp.get("throttle_events_429", 0),
        "batchengine_job_retries_issued_total": bp.get("retries_issued", 0),
        "batchengine_job_current_rate_limit_rps": bp.get("current_rate_limit_rps", 0.0),
        "batchengine_job_estimated_cost_usd": record.usage.estimated_cost_usd,
    }
    return float(values[metric])
