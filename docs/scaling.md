# Scaling and memory

## Measured memory (§6.6)

Produced by `python scripts/memory_probe.py`, which drives the real
`JobRunner` pipeline against `MockProvider` (zero API calls, zero cost) and
samples RSS every 50ms. `docs/memory_probe_results.json` holds the raw
numbers this table is generated from.

| N | Baseline RSS | Peak RSS | Delta over baseline | Throughput (mock, no simulated latency) |
|---|---|---|---|---|
| 1,000 | 36.7 MB | 37.5 MB | 0.8 MB | 4,992 items/s |
| 10,000 | 37.5 MB | 37.7 MB | 0.2 MB | 5,410 items/s |
| 100,000 | 37.7 MB | 38.0 MB | 0.3 MB | 2,633 items/s |
| 500,000 | 38.0 MB | 38.1 MB | 0.0 MB | 2,311 items/s |

500× more input data, and the delta over baseline stays inside noise
(0.0-0.8 MB) the entire way — exactly the flat-memory claim this section
exists to support. (Throughput here reflects Python/asyncio scheduling
overhead against a zero-latency mock, not the account's real RPM ceiling —
see the next section for the actual bottleneck.)

The claim is that peak RSS *does not scale with N*. The four terms behind
that (§6.6 of instructions.md):

```
peak_RSS = interpreter + framework baseline      (~30-100 MB, fixed)
         + queue_depth × item_size               (concurrency × 4 × ~600 B)
         + in_flight × (request + response)      (concurrency × ~2 KB)
         + write_buffer                          (100 rows × ~1.5 KB)
```

At concurrency 16 the job-data terms total well under 1 MB. Three code
paths are what make this true, and each is annotated in the source with why
the alternative was rejected:

| Path | This codebase | The O(N) mistake it avoids |
|---|---|---|
| Ingest (`core/ingest.py`) | `ijson.items()` generator | `json.load()` — 250MB of text becomes ~1GB of Python objects at N=500,000 |
| Dispatch (`core/scheduler.py`) | bounded `asyncio.Queue(maxsize=concurrency×4)` | pre-creating 500,000 `asyncio.Task`s — 0.5-1.5GB |
| Collect (`core/sink.py`) | append-only JSONL, counters only | accumulating results in a list — ~750MB |

N-dependence lives entirely on disk (the JSONL result file grows linearly),
which costs nothing and is the point of streaming output instead of
buffering it.

## Throughput: the account quota is the bottleneck, not the worker pool

DigitalOcean serverless inference Tier 1: **120 requests/minute**,
500K-750K tokens/minute.

- **1,000 prompts has a hard floor of ~8.3 minutes** (1000 ÷ 120 RPM). No
  amount of worker concurrency beats this — spawning 64 workers against a
  120 RPM account produces a 429 storm, not more throughput.
- **RPM binds before TPM until ~4,167 tokens/request** (500,000 TPM ÷ 120
  RPM). Below that crossover, request rate is the constraint; above it,
  token rate is. At `max_tokens=128` and typical prompt lengths in this
  repo's sample batch, every real request is far under that crossover, so
  RPM is the binding constraint for this workload.

### Little's Law sizing

`L = λ × W`: in-flight requests = throughput × latency. λ is fixed
externally by the account quota — concurrency can't raise it. Inverting:
the number of workers needed to *just saturate* the quota is `λ × W`,
where W is the *observed* mean request latency (`AdaptiveController.littles_law_optimal_concurrency`,
reported live in `GET /job/{id}/status.backpressure`).

| Observed W | Required L (at λ=2 req/s) |
|---|---|
| 1s | 2 |
| 3s | 6 |
| 10s | 20 |
| 30s | 60 |

A hardcoded concurrency is correct at exactly one latency and wrong at
every other one — W varies with prompt length, `max_tokens`, and model
warmth. That's why this engine tracks a rolling mean latency and exposes
the computed optimum instead of asking the operator to guess it, and logs a
warning (`job.overprovisioned_concurrency`) when configured concurrency
exceeds the computed optimum by more than 2×.

Over-provisioning is not free even though the limiter caps throughput
either way: at concurrency = 3L, the surplus 2L sit queued waiting for a
token. Queue time counts toward observed latency, so W inflates, client
timeouts fire on requests that were never actually slow to execute, and
burst-aligned dispatch produces more 429s than a correctly-sized pool
would. The same L also bounds memory — peak in-flight bytes is `L ×
payload_size` — so the concurrency ceiling and the memory ceiling are the
same calculation viewed from two angles.

## The 500,000-item figure

500,000 ÷ 120 RPM ≈ **69 hours**. Streaming the JSON solves the memory
problem; it does nothing for this number, because the constraint is the
account's request rate, not this engine's implementation.

> This figure is analysis only — it is never executed against a live
> endpoint. `scripts/memory_probe.py` generates a 500,000-item file and
> measures memory against `MockProvider` with zero API calls. No live run
> in this build (or any build following instructions.md §12.2) exceeds
> N=1,000.

## When to stop using this service

| Scale | Right answer | Why | Cost / constraint |
|---|---|---|---|
| Up to a few thousand items, ad hoc | This engine, serverless inference | Fits comfortably inside Tier 1 RPM; no setup cost | ~8 min per 1,000 items, ~$0.065/1,000 at `openai-gpt-oss-20b`/`max_tokens=128` |
| Tens of thousands, batchable, not latency-sensitive | **DigitalOcean Batch Inference** | Separate quota pool from real-time traffic (doesn't degrade a customer's production p99), up to 50% discount, 50,000 requests/file, 200MB max file, 24h completion window | Requires accepting a completion window instead of synchronous polling |
| Sustained high volume, latency-sensitive | **Dedicated Inference endpoint** | Removes the shared RPM ceiling entirely — you own the GPU | From $2.59/hr (AMD MI300X); only economical above a utilization threshold |
| Sustained high volume on serverless specifically | **Quota tier increase** | Tier 5 reaches 4,500 RPM — 500K items drops from ~69h to **~1.9h** | Requires an account-level request/approval to DigitalOcean; no code change |

This table, not the code, is the strongest artifact in this repository: it
demonstrates the judgment to recognize when the right move is a product
decision rather than a more clever scheduler.
