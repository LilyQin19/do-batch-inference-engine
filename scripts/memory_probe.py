#!/usr/bin/env python
"""Measures RSS at N = 1K / 10K / 100K / 500K against the mock provider with
zero API calls (§1.5, §6.6). This is the *measured* half of the memory
argument -- the README pairs this output with the *predicted* decomposition
table so a reviewer can compare claim against observation.

Hard rail (§12.2 / instructions.md §1.5): this script must NEVER make a live
API call. It only ever constructs MockProvider. There is no code path here
that reads DO_INFERENCE_KEY.
"""

from __future__ import annotations

import asyncio
import gc
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import psutil

from batchengine.core.models import JobConfig, JobRecord, JobStatus
from batchengine.core.scheduler import JobRunner
from batchengine.providers.mock import MockProvider, MockProviderConfig
from batchengine.store.memory import MemoryJobStore

_SIZES = [1_000, 10_000, 100_000, 500_000]
_CONCURRENCY = 16


def _write_batch_streaming(path: Path, n: int) -> None:
    """Writes the input file line by line -- never builds an in-memory list
    of N rows, so the *generator* side of this script doesn't undermine the
    thing it's trying to measure.
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write("[")
        for i in range(n):
            if i:
                f.write(",")
            f.write(json.dumps({"id": f"item-{i}", "prompt": f"prompt number {i}"}))
        f.write("]")


@dataclass(slots=True)
class ProbeResult:
    n: int
    baseline_rss_mb: float
    peak_rss_mb: float
    final_rss_mb: float
    delta_mb: float
    elapsed_s: float
    throughput_rps: float


async def _run_one(n: int, tmp_dir: Path) -> ProbeResult:
    batch_path = tmp_dir / f"batch_{n}.json"
    result_path = tmp_dir / f"results_{n}.jsonl"
    _write_batch_streaming(batch_path, n)

    process = psutil.Process()
    gc.collect()
    baseline_rss = process.memory_info().rss / 1_048_576

    peak_rss = baseline_rss
    stop_sampling = False

    async def sample_rss() -> None:
        nonlocal peak_rss
        while not stop_sampling:
            peak_rss = max(peak_rss, process.memory_info().rss / 1_048_576)
            await asyncio.sleep(0.05)

    config = JobConfig(
        input_path=str(batch_path),
        model="mock",
        max_tokens=64,
        concurrency=_CONCURRENCY,
        max_job_spend_usd=1_000_000.0,  # spend guard is tested elsewhere; irrelevant here
    )
    record = JobRecord(
        job_id=f"probe-{n}", config=config, status=JobStatus.QUEUED, result_path=str(result_path)
    )
    store = MemoryJobStore()
    await store.create(record)
    provider = MockProvider(MockProviderConfig(seed=0, latency_mean=0.0, latency_jitter=0.0))
    runner = JobRunner(record=record, provider=provider, store=store, rate_limit_rpm=10_000_000)

    sampler = asyncio.create_task(sample_rss())
    start = time.perf_counter()
    await runner.run()
    elapsed = time.perf_counter() - start
    stop_sampling = True
    await sampler

    gc.collect()
    final_rss = process.memory_info().rss / 1_048_576

    assert record.counts.succeeded == n, (
        f"expected all {n} to succeed against the clean mock provider"
    )

    batch_path.unlink(missing_ok=True)
    result_path.unlink(missing_ok=True)

    return ProbeResult(
        n=n,
        baseline_rss_mb=round(baseline_rss, 1),
        peak_rss_mb=round(peak_rss, 1),
        final_rss_mb=round(final_rss, 1),
        delta_mb=round(peak_rss - baseline_rss, 1),
        elapsed_s=round(elapsed, 2),
        throughput_rps=round(n / elapsed, 1) if elapsed > 0 else 0.0,
    )


async def main() -> None:
    sizes = _SIZES
    if len(sys.argv) > 1:
        sizes = [int(x) for x in sys.argv[1:]]

    tmp_dir = Path("data") / "_memory_probe_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for n in sizes:
        print(f"probing N={n}...", file=sys.stderr)
        result = await _run_one(n, tmp_dir)
        results.append(result)
        print(
            f"  peak RSS delta over baseline: {result.delta_mb} MB, "
            f"{result.throughput_rps} items/s",
            file=sys.stderr,
        )

    out_path = Path("docs") / "memory_probe_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps([asdict(r) for r in results], indent=2))

    print("\n| N | Baseline RSS | Peak RSS | Delta | Throughput |")
    print("|---|---|---|---|---|")
    for r in results:
        print(
            f"| {r.n:,} | {r.baseline_rss_mb} MB | {r.peak_rss_mb} MB | "
            f"{r.delta_mb} MB | {r.throughput_rps} items/s |"
        )

    tmp_dir.rmdir()


if __name__ == "__main__":
    asyncio.run(main())
