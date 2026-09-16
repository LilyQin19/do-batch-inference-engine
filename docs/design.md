# Batch Inference Engine — Architecture & Design

**Version 1.0 · September 2026**

A REST service that executes large prompt batches against rate-limited LLM
inference endpoints without losing work, exhausting memory, or overwhelming the
upstream provider.

---

## 1. Specification

### 1.1 Problem

A caller submits a file containing thousands of prompts and needs every one of
them evaluated against a live inference endpoint. Three properties of that
environment drive the entire design:

- **The upstream is rate-limited and not under our control.** DigitalOcean
  serverless inference allows 120 requests/minute on a Tier 1 account. Exceeding
  it produces HTTP 429, not additional throughput.
- **Individual rows fail independently.** A malformed prompt, a transient 5xx,
  or a context-length rejection must not terminate the batch.
- **The input does not fit a naive in-memory model.** At 500,000 rows, loading
  the file and accumulating results costs 2–3 GB and exhausts a small instance.

### 1.2 Functional requirements

| ID | Requirement |
|---|---|
| F1 | Ingest a local batch file and return a job ID immediately; execution proceeds in the background |
| F2 | Distribute work across a bounded worker pool invoking live inference endpoints |
| F3 | Absorb upstream 429 backpressure with exponential backoff and jitter, without dropping items |
| F4 | Isolate individual row failures; aggregate successes |
| F5 | Expose progress via `GET /job/{id}/status` |
| F6 | Expose compiled results via `GET /job/{id}/download` |

### 1.3 Non-functional requirements

| ID | Requirement | Target |
|---|---|---|
| N1 | Submission latency | `POST /job` returns in < 50 ms regardless of input size |
| N2 | Memory boundedness | Peak RSS independent of N |
| N3 | Work conservation | `succeeded + failed == ingested`, always |
| N4 | Cost control | Hard per-job and cross-run spend caps |
| N5 | Testability | Full suite runs with zero network and zero credentials |

### 1.4 Non-goals

Deliberately out of scope, with the reasoning:

| Non-goal | Why | What would change |
|---|---|---|
| Distributed execution | Single process is sufficient at the 120 RPM ceiling; the quota, not CPU, is the bottleneck | Shared queue (Redis/SQS), distributed rate limiter, leader election |
| Exactly-once delivery | Requires idempotency keys the inference API does not offer | Provider-side dedup or a two-phase commit against the result store |
| Authentication / multi-tenancy | Single-operator tool; auth belongs at the ingress | Per-tenant quota accounting and isolated spend ledgers |
| Job prioritization / fairness | One job at a time is the observed usage | Priority queue and weighted scheduling across jobs |

---

## 2. Design goals

Three properties are load-bearing. Everything else is support structure.

**G1 — Flow control.** The service must saturate the upstream quota without
exceeding it, and must degrade gracefully when the upstream pushes back.
Concurrency and request rate are treated as distinct quantities with distinct
mechanisms.

**G2 — Memory boundedness.** Peak memory must be a function of concurrency, not
of input size. This is provable, measured, and tested — not asserted.

**G3 — Failure semantics.** Every outcome maps to exactly one class, each class
has one defined action, and no path exists by which an ingested item fails to
reach a terminal state.

### Guiding principles

- **Bound every queue.** An unbounded buffer is a memory leak with good manners.
- **Stream, never accumulate.** Input streams in; results stream out; only
  counters live in memory.
- **Classify before reacting.** A 429 and a 400 are not both "an error."
- **Make invariants testable.** G3 is a single assertion checked under chaos,
  not a paragraph in a README.
- **Fail fast on fatal, isolate on local.** Bad credentials stop the job in one
  request; a bad row stops only itself.

---

## 3. High-level architecture

![architecture flow](diagrams/flow.svg)

One asyncio task per job walks four zones. The API layer has no knowledge of
execution; the scheduler has no knowledge of HTTP.

### 3.1 Component inventory

| Zone | Component | Module | Responsibility |
|---|---|---|---|
| — | API Layer | `api/routes.py` | Endpoint surface, request validation, job handoff |
| 1 | Ingest | `core/ingest.py` | Streaming file parse, row validation |
| — | Scheduler | `core/scheduler.py` | Job lifecycle, component wiring, final status |
| 2 | Bounded Queue | `asyncio.Queue` | Backpressure boundary; bounds memory |
| 2 | Rate Limiter | `core/ratelimit.py` | Token bucket + AIMD adaptation |
| 3 | Worker Pool | `core/worker.py` | Acquire → call → classify → retry or emit |
| 3 | Retry & Classify | `core/retry.py` | Failure taxonomy, backoff, budget, circuit breaker |
| 3 | Providers | `providers/` | Live DigitalOcean client; deterministic mock |
| 4 | Result Sink | `core/sink.py` | Single-writer append-only JSONL |
| 4 | Job Store | `store/` | Status, counts, usage; memory + SQLite |
| — | Spend Guard | `core/spend_ledger.py` | Per-job and cross-run cost caps |
| — | Extensions | `extensions/` | Spaces checkpointing, completion webhook |

### 3.2 Control flow

```
POST /job
  → stat-only validation (never opens the file)
  → persist JobRecord, create_task(JobRunner.run), return 202 + job_id

JobRunner.run
  → spawn N workers
  → stream input → bounded queue          [backpressure boundary 1]
  → workers: token acquire                [backpressure boundary 2]
           → provider call
           → classify
           → retry (full jitter) or emit terminal outcome
  → sink writes each outcome to JSONL
  → on drain: compute final status, assert conservation, persist, webhook
```

### 3.3 Item lifecycle

![item path](diagrams/item_path.svg)

### 3.4 Job lifecycle

![job lifecycle](diagrams/lifecycle.svg)

Terminal status is derived, not assigned: `cancelled` if cancellation was
requested, `failed` if a fatal class aborted the job or nothing succeeded,
`succeeded` if nothing failed, otherwise `partial`.

---

## 4. Component design

### 4.1 API Layer — `api/routes.py`

**Responsibility.** Translate HTTP into job operations. Contains no execution
logic.

| Endpoint | Behavior |
|---|---|
| `POST /job` | Validates by `stat()` only — existence, readability, size. Generates a ULID, persists a `QUEUED` record, schedules `JobRunner.run()` as a background task, returns `202`. |
| `GET /job/{id}/status` | Counts, progress, timing, backpressure telemetry, token usage and cost, errors by class. |
| `GET /job/{id}/download` | Streams the JSONL result file. `?format=json` frames a JSON array incrementally. `409` while running unless `?partial=true`. |
| `POST /job/{id}/cancel` | Sets a cancel event; in-flight items drain to terminal state. |
| `GET /healthz`, `GET /metrics` | Liveness and Prometheus-format metrics. |

**Key design.** The 50 ms submission target (N1) is met by never opening the
input file on the request path. Parsing is deferred entirely to the background
task, which is also what makes submission latency independent of input size.

### 4.2 Ingest — `core/ingest.py`

**Responsibility.** Yield one row at a time from disk.

**Key design.** Format is sniffed from the first non-whitespace byte: `[`
selects `ijson.items()` incremental array parsing, anything else selects
line-delimited JSON. `json.load()` does not appear in this module — that is the
first leg of G2.

The synchronous parse runs in a worker thread, bridged to the event loop through
a bounded queue of 64. Two consequences: the event loop never blocks on file
I/O, and the producer thread is itself backpressured when the consumer falls
behind, because `queue.put` blocks the thread once full.

**Failure behavior.** A row missing a string `prompt` becomes a terminal
`invalid_input` error and the stream continues. In JSONL mode a malformed line is
similarly isolated, because each line is an independent document. In array mode
a JSON syntax error aborts the parse — a property of the format, not a design
gap: one bad brace corrupts document structure, not just one row.

**Cleanup.** If the consumer stops early — cancellation, fatal abort — the
producer thread may be blocked in `put`. The generator's `finally` drains the
queue so the thread can exit rather than leak.

### 4.3 Scheduler — `core/scheduler.py`

**Responsibility.** Own the job lifecycle and wire every other component
together. No HTTP awareness.

**Key design.** Constructs the queue at `concurrency × 4`, the token bucket at
the account rate, the AIMD controller, circuit breaker, retry budget, and sink;
spawns N workers; runs ingestion; then drains.

Initial concurrency, when not specified, is `ceil(rate_rps × 2.0)` — an assumed
2-second latency used only until real latency is observed. The converged figure
is reported live as `littles_law_optimal_concurrency`, and a warning is logged
when configured concurrency exceeds it by more than 2×.

**Drain sequence.** One `SHUTDOWN` sentinel per worker, then
`gather(*workers, return_exceptions=True)`, then `sink.close()`. The
`return_exceptions=True` is load-bearing: without it, the first worker to raise
makes `gather` raise, `sink.close()` is skipped, the file handle leaks, and the
job never reaches terminal status.

**Invariant enforcement.** Before persisting the final record, the scheduler
asserts `counts.conserved()`. G3 is checked in production code, not only in
tests.

### 4.4 Rate Limiter — `core/ratelimit.py`

**Responsibility.** Pace requests per second against the account quota, and
adapt when the upstream signals pressure.

**Mechanism — `TokenBucket`.** Continuous refill on a monotonic clock.
`time.monotonic` rather than `time.time` because an NTP step or VM pause must
never mint a burst of stale tokens or freeze the bucket.

**Policy — `AdaptiveController` (AIMD).**

| Signal | Action |
|---|---|
| 429 received | `rate ×= 0.75`, then a 5-second cooldown during which further 429s do not decrease again |
| 50 consecutive successes | `rate += 0.5`, capped at `max_rate` |
| `x-ratelimit-reset-requests` present | Hard-pause until that epoch, then resume with `uniform(0, 250 ms)` per-worker jitter |

**The cooldown is the critical detail.** Without it, N concurrent 429s arising
from a single momentary overage multiply the rate by `0.75^N` in one instant —
twenty simultaneous throttles collapse a rate of 10 to roughly 0.03. That is a
self-inflicted outage, not a controlled backoff. The cooldown treats a burst of
simultaneous signals as one signal.

**Why a bucket and not a semaphore.** A semaphore bounds how many requests are
in flight; it says nothing about how many complete per second. Two workers
finishing in 100 ms produce 20 req/s and breach a 2 req/s quota immediately
regardless of semaphore size. Concurrency and rate are different quantities —
related by Little's Law, `L = λ × W` — and need different mechanisms. The queue
bounds `L` for memory; the bucket bounds `λ` for quota.

### 4.5 Worker Pool — `core/worker.py`

**Responsibility.** Drive one item through acquire → call → classify → retry or
emit. Returning normally always means the item reached a terminal state.

**Loop structure.** Check cancel and abort events; check circuit breaker; acquire
a token; call the provider; classify; then success, fatal, retry, or exhaustion.

**Exception containment.** The provider call catches `ProviderTransportError`,
re-raises `asyncio.CancelledError` so cooperative cancellation still works, and
converts any other exception into a transient failure. The in-flight counter is
decremented in a `finally`. `worker_loop` wraps `process_item` so that even an
unexpected exception emits a terminal `RowError` and calls `task_done()`.

This containment is what makes G3 structural rather than incidental. Without it,
one unanticipated exception loses a row, leaks a counter, skips `task_done`, and
hangs the job — and no conservation test would catch it, because a mock provider
only raises the exceptions the code already expects.

### 4.6 Retry & Classification — `core/retry.py`

**Taxonomy.** Every outcome maps to exactly one class with one action:

| Condition | Class | Action | Max attempts |
|---|---|---|---|
| 200 | `success` | Record result | 1 |
| 429 | `throttled` | Retry; AIMD decrease; honor reset header | 8 |
| 408, 500, 502, 503, 504 | `transient` | Retry with full jitter | 5 |
| Timeout / connection error | `transient` | Retry with full jitter | 5 |
| 400, 422, 413 | `invalid_input` | Terminal; record row error | 1 |
| 401, 403 | `fatal_auth` | **Abort entire job** | 1 |
| 402 | `fatal_billing` | **Abort entire job** | 1 |
| 200 with unparseable body | `transient` | Retry once, then terminal | 2 |
| Unknown status | `transient` | Retry conservatively | 5 |

The two fatal classes matter disproportionately. Serverless inference is
prepaid: a `402` means the balance is exhausted, and retrying 1,000 rows five
times each against an empty balance is exactly the waste this service exists to
prevent. Both abort within a single request.

**Backoff — full jitter.** `sleep = uniform(0, min(cap, base × 2^attempt))`,
base 0.5 s, cap 30 s. Plain exponential backoff keeps a retry cohort's wake times
correlated: twenty workers throttled by the same blip retry at the same instant
on wave one, collide, and collide again on every subsequent wave. Sampling
uniformly under the exponential envelope decorrelates the cohort on the first
retry and keeps it spread.

**Retry budget.** Total retries are capped at 20% of ingested items. The
denominator grows as ingestion streams, so early items cannot retry without
limit while the file is still being read. Beyond the cap, remaining failures
terminate as `transient_exhausted`. This prevents retry amplification against an
upstream that is already struggling.

**Circuit breaker.** Opens when more than 50% of the last 100 outcomes are
failures. After a cooldown it admits exactly one half-open probe. A probe
timeout clears the in-flight flag if that probe never reports, so the breaker
cannot deadlock a job.

### 4.7 Result Sink — `core/sink.py`

**Responsibility.** Persist every terminal outcome exactly once, without holding
results in memory.

**Key design.** A single background task owns the only file handle; workers
submit through a queue, so concurrent writers never race. Rows append as JSONL,
flushed and `fsync`ed every 100 rows or 2 seconds — batching amortizes the sync
cost while bounding loss to at most one window.

This is the third leg of G2: accumulating `RowResult` objects in a list would
cost roughly 750 MB at N = 500,000. Writing through means the only in-memory
record of completed work is a handful of integers.

**Recovery.** `replay()` rebuilds counts and the set of processed item IDs from
the existing file on restart. A torn final line from a crash is skipped rather
than treated as corruption.

### 4.8 Job Store — `store/`

**Responsibility.** Hold job status durably enough to survive process restart.

In-memory for speed, mirrored to SQLite via `aiosqlite`. Status is persisted
every 25 ingested items and at every state transition, so a restarted process
can serve status and download for a job it did not run.

### 4.9 Providers — `providers/`

**Interface.** A `Protocol` with `complete()`, `cost_per_1m_input()`, and
`cost_per_1m_output()`. Selection is by environment: the live DigitalOcean
client when `DO_INFERENCE_KEY` is set, the mock otherwise. CI never sets it, so
the test suite cannot select the live path — that is how N5 is enforced
structurally rather than by convention.

**`digitalocean.py`.** OpenAI-compatible calls to
`https://inference.do-ai.run/v1/chat/completions` over a shared pooled
`httpx.AsyncClient`. Parses rate-limit headers and token usage from each
response. Does not assume `content` is a string: reasoning models return
`content: null` with text in `reasoning_content`, and an unhandled `None` here
is precisely the unanticipated exception that G3 depends on containing. See
`docs/model-selection.md`.

**`mock.py`.** Seeded and deterministic, parameterized by `p_429`, `p_500`,
`p_400`, `p_timeout`, `malformed_response_rate`, `p_unexpected_exception`, and a
`hard_rpm_ceiling` that returns real 429s with correct headers above a
configured rate. This component makes the entire test suite hermetic and is what
lets backpressure be *demonstrated* rather than claimed.

### 4.10 Spend Guard — `core/spend_ledger.py`

Two caps, because one is insufficient. `MAX_JOB_SPEND_USD` is recomputed every
50 completed items against both actual and linearly projected cost, and aborts
the job on breach. `MAX_TOTAL_SPEND_USD` is a committed ledger accumulating
across runs, checked before a live job starts, never reset automatically. A
per-job cap alone protects against one runaway job but not against many small
test runs draining a prepaid balance.

Cost is computed from **actual** tokens reported in each response, never
estimated from prompt length — chat-template overhead varies roughly 5× between
models on identical input.

### 4.11 Extensions — `extensions/`

**Spaces checkpointing.** Streams completed result chunks to S3-compatible
DigitalOcean Spaces so a crash preserves finished work. Feature-flagged; tests
stub the client.

**Completion webhook.** Posts final status and counts to a registered URL,
signed with HMAC-SHA256 in `X-Signature`, retried three times with full jitter.
The target URL is validated against private, loopback, and link-local ranges to
prevent SSRF.

---

## 5. Cross-cutting design

### 5.1 Memory model

Peak RSS decomposes into four terms, none containing N:

```
peak_RSS = interpreter + framework baseline    (~37 MB, fixed)
         + queue_depth × item_size             (concurrency × 4 × ~600 B)
         + in_flight × (request + response)    (concurrency × ~2 KB)
         + write_buffer                        (100 rows × ~1.5 KB)
```

Measured, driving the real pipeline against the mock provider:

| N | Peak RSS | Delta over baseline |
|---|---|---|
| 1,000 | 37.5 MB | 0.8 MB |
| 10,000 | 37.7 MB | 0.2 MB |
| 100,000 | 38.0 MB | 0.3 MB |
| 500,000 | 38.1 MB | 0.0 MB |

A 500× increase in input produces no measurable increase in memory.
N-dependence lives on disk, where the result file grows linearly and costs
nothing.

### 5.2 Concurrency model

Single-threaded asyncio, plus one worker thread for the blocking file parse.
This has a useful property: any function containing no `await` executes
atomically with respect to other coroutines. `TokenBucket._refill` relies on
this — it mutates shared state without holding the lock, and is safe because it
never yields.

Concurrency is sized by Little's Law, `L = λ × W`. Because λ is fixed externally
by the quota, the equation is inverted to solve for L — the number of in-flight
requests needed to just saturate the quota:

| Observed latency W | Required concurrency L (λ = 2 req/s) |
|---|---|
| 1 s | 2 |
| 3 s | 6 |
| 10 s | 20 |
| 30 s | 60 |

A hardcoded concurrency is correct at exactly one latency. Over-provisioning is
not free even though the limiter caps throughput regardless: surplus workers
queue for tokens, queue time inflates observed latency, and client timeouts fire
on requests that were never slow.

### 5.3 Delivery semantics

**At-least-once, bounded by the retry budget.**

A completed item is written to the JSONL sink before its counter increments, so
a crash cannot lose an already-recorded result — at worst it loses up to one
fsync window (100 rows or 2 seconds).

Items in flight when the process dies are neither recorded nor retried
automatically; on restart, `replay()` reconstructs which IDs already completed,
and re-submitting the job re-runs only the remainder. Re-running an item that
had in fact completed upstream would pay for that inference twice. Exactly-once
would require an idempotency key the inference API does not expose, which is why
it appears in the non-goals.

### 5.4 Observability

Structured JSON logs carry `job_id` and `item_id` on every line; prompt content
and key material are never logged. `GET /job/{id}/status` exposes live
backpressure telemetry — current paced rate, 429 count, retries issued, retry
budget remaining, circuit state, observed mean latency, and the computed
Little's Law optimum — which makes the control loop inspectable rather than
opaque.

---

## 6. Scale thresholds

The 120 RPM quota, not this engine, is the binding constraint. 1,000 items has a
floor of ~8.3 minutes; 500,000 items would take ~69 hours. Streaming solves
memory; it does nothing for that number.

| Scale | Right answer | Why |
|---|---|---|
| Up to a few thousand, ad hoc | This engine on serverless inference | Fits inside Tier 1; no setup cost |
| Tens of thousands, batchable | **DigitalOcean Batch Inference** | Separate quota pool, up to 50% discount, isolated from production p99 |
| Sustained, latency-sensitive | **Dedicated Inference endpoint** | Removes the shared RPM ceiling; from $2.59/hr |
| Sustained on serverless | **Quota tier increase** | Tier 5 reaches 4,500 RPM — 500K drops to ~1.9 hours |

Recognizing when the correct answer is a product decision rather than a more
clever scheduler is itself part of the design.

---

## 7. References

| Document | Contents |
|---|---|
| `docs/decisions.md` | Full ADR log — every choice with its rejected alternative |
| `docs/scaling.md` | Memory measurements and throughput analysis |
| `docs/model-selection.md` | Model choice; why the specified model is unavailable |
| `docs/observed_throttling.md` | Live rate-limit behavior observations |
| `README.md` | Quickstart and overview |
