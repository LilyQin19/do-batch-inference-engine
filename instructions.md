# instructions.md — Batch Inference Engine REST API Service

**Build spec for Claude Code.** Read this file completely before writing any code.

**Author/owner:** Lily Qin (github.com/LilyQin19)
**Context:** DigitalOcean Forward Deployed Engineer (AI/ML) take-home. The finished repo will be
reviewed live, line by line, by a DigitalOcean engineer in a 45-minute session. Every design
decision must be defensible out loud. Prefer a smaller, fully-understood, fully-tested system over
a larger one with unexplained parts.

---

## 0. Prime directive

The graders are not testing whether you can call an LLM API in a loop. They are testing three
things, in this order of weight:

1. **Flow control** — a bounded worker pool that behaves correctly against a rate limit it does not
   control, and *maximizes completion velocity without dropping elements*.
2. **Memory boundedness** — demonstrable `O(concurrency)` memory, not `O(N)`, with measured numbers.
3. **Failure semantics** — a precise taxonomy of retryable vs. terminal, with zero lost rows.

Everything else (job IDs, status endpoints, Spaces, webhooks) is table stakes. Do not let polish on
table stakes crowd out depth on these three.

**Non-negotiable invariant, asserted in tests:**
```
succeeded_count + failed_count == items_ingested
```
Every input item appears exactly once across the success set and the error set. Always. Under 429
storms, under 500s, under cancellation, under malformed input.

---

## 1. Key research findings — read before designing

These were verified against DigitalOcean's live documentation on 2026-09-15. They change the design.

### 1.1 The model named in the spec is not available on Serverless Inference

The project statement says to use `meta-llama-3-8b-instruct`. DigitalOcean's model catalog lists
`llama3-8b-instruct` as **dedicated-inference only** — it is not offered through the serverless
endpoint.

**Action:** Default to `openai-gpt-oss-20b` ($0.05/1M input, $0.45/1M output — cheapest serverless
option). Make the model fully configurable. Add a `## Product findings` section to the README
documenting this discrepancy, the substitution, and the cost comparison. Do not silently work
around it — surfacing it is a scored behavior for this role.

Serverless-available low-cost options, for the README's cost table:

| Model ID | $/1M input | $/1M output | Est. cost, 1,000 prompts* |
|---|---|---|---|
| `openai-gpt-oss-20b` | $0.05 | $0.45 | ~$0.10 |
| `openai-gpt-oss-120b` | $0.06 | $0.39 | ~$0.09 |
| `gemma-4-31B-it` | $0.18 | $0.50 | ~$0.13 |
| `llama-4-maverick` | $0.20 | $0.696 | ~$0.17 |
| `ministral-3-14B` | $0.20 | $0.20 | ~$0.07 |

\* assuming ~150 input / ~200 output tokens per prompt. Compute these in code, don't hardcode.

### 1.2 The bottleneck is the account quota, not the worker pool

DigitalOcean serverless inference Tier 1 quota: **120 requests/minute**, 500K–750K tokens/minute.

Consequences that must appear in the code and the docs:

- **1,000 prompts has a hard floor of ~8.3 minutes** (1000 ÷ 120 RPM). No amount of concurrency
  beats this. Spawning 64 workers produces a 429 storm, not throughput.
- **Correct concurrency comes from Little's Law:** `L = λ × W`, where L = in-flight requests,
  λ = throughput, W = per-request latency.

  The important move is *which variable you solve for*. λ is fixed externally at 2 req/s by the
  account quota — you cannot raise it by adding workers. So invert and solve for L: how many
  requests must be in flight to just saturate the quota? One worker completes `1/W` req/s, so you
  need `λ / (1/W)` = `λ × W` workers.

  | Observed W | Required L |
  |---|---|
  | 1s | 2 |
  | 3s | 6 |
  | 10s | 20 |
  | 30s | 60 |

  W varies with prompt length, `max_tokens`, and model warmth — so **a hardcoded concurrency is
  wrong at every latency except the one it was tuned for.** The engine must maintain a rolling mean
  of observed latency and expose `littles_law_optimal_concurrency = ceil(rate × mean_latency)`.

  Two second-order consequences to implement and comment:
  - **Over-provisioning is not free.** At concurrency = 3L, the surplus 2L sit in queue. Queue time
    counts toward W, so measured latency inflates, client timeouts fire on requests that were never
    slow, and burst alignment produces 429s. Throughput does not improve — the limiter caps it.
  - **The same L bounds memory.** Peak in-flight bytes = L × payload size. The concurrency ceiling
    and the memory ceiling are the same calculation.
- **RPM binds before TPM** until ~4,167 tokens/request (500,000 TPM ÷ 120 RPM). Below that
  threshold, request rate is the constraint; above it, token rate is. State this crossover
  explicitly in `docs/scaling.md`.

### 1.3 Rate-limit response headers

Serverless inference returns quota headers. Use them — do not backoff blindly.

- `x-ratelimit-limit-requests`, `x-ratelimit-remaining-requests`, `x-ratelimit-reset-requests`
- Same triplet for `-tokens-per-minute` and `-tokens-per-day`
- `x-ratelimit-reset-*` is a **Unix epoch timestamp** and a *forward refill projection*, not a
  fixed-window boundary. Value `0` means capacity was sufficient.

**Design implication:** because it is a continuous-refill projection shared by all workers, waking
every worker at exactly that timestamp creates a thundering herd. Honor the header as a floor, then
add per-worker jitter on top. Call this out in a code comment — it is a review talking point.

### 1.4 Serverless inference is prepaid

If the account balance hits $0, all requests fail. This means **HTTP 402 is a fatal, whole-job
failure class** — not a per-row error and not retryable. Burning 1,000 rows × 5 retries against an
empty balance is exactly the kind of thing a Forward Deployed Engineer is supposed to prevent.

### 1.5 At 500,000 items the right answer is to change products

500,000 ÷ 120 RPM = **~69 hours**. Streaming the JSON solves memory; it does not solve this.

> **This figure is analysis only. It is never executed.** `scripts/memory_probe.py` generates a
> 500,000-item *file* and measures ingest/steady-state RSS against the **mock provider with zero
> API calls**. Claude Code must never launch a live inference run above N = 1,000. See §12.2.

`docs/scaling.md` must state that beyond roughly 50K items the correct engineering answer is:

1. **DigitalOcean Batch Inference** — separate quota pool, 50,000 requests per file, 200 MB max
   file, 24-hour completion window, up to 50% discount, and isolated from real-time traffic so it
   doesn't degrade a customer's production p99.
2. **Dedicated Inference endpoint** — from $2.59/hr (AMD MI300X), removes the shared RPM ceiling.
3. **Quota tier increase** — Tier 5 reaches 4,500 RPM, which brings 500K down to ~1.9 hours.

Include a decision table with the crossover points. This is the single strongest section in the
whole deliverable: it shows product judgment, not just code.

---

## 2. Stack

Fixed. Do not substitute without asking.

| Concern | Choice | Why (be ready to defend) |
|---|---|---|
| Language | Python 3.12 | Matches the AI/ML tooling ecosystem; asyncio is the right concurrency model for an I/O-bound fan-out |
| API | FastAPI | ASGI-native, Pydantic validation, `StreamingResponse` built in |
| HTTP client | `httpx.AsyncClient` | Connection pooling + HTTP/2; one shared client for the process |
| Streaming JSON | `ijson` | Incremental parse; the whole point of the memory story |
| Job state | in-memory + `aiosqlite` | Survives process restart; demonstrates durable job tracking |
| Tests | `pytest`, `pytest-asyncio`, `respx`, `hypothesis` | `respx` mocks httpx at transport level — deterministic 429/500 injection with zero network |
| Logging | `structlog` (JSON) | Machine-readable; include `job_id` + `item_id` in every line |
| Lint/type | `ruff`, `mypy --strict` | Enforced in CI |
| Container | Docker (python:3.12-slim) | |
| CI | GitHub Actions | Must be green on push; must need no secrets |

**Hard rule:** the entire test suite runs with zero network access and zero API keys. Live
DigitalOcean calls are opt-in via environment variable only.

---

## 3. Architecture

```
POST /job
  │  (stat file only — never read it on the request path)
  │  generate ULID, persist job=queued, asyncio.create_task(runner), return 202
  │  target: < 50 ms
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ BACKGROUND RUNNER (one asyncio task per job)                        │
│                                                                      │
│  Ingest (ijson streaming generator)                                 │
│      │  yields PromptItem one at a time — never materializes list   │
│      ▼                                                               │
│  asyncio.Queue(maxsize = concurrency × 4)   ◄── BACKPRESSURE #1     │
│      │  producer blocks when full; this is what bounds memory        │
│      ▼                                                               │
│  TokenBucket + AIMD controller             ◄── BACKPRESSURE #2      │
│      │  paces against the 120 RPM account quota                      │
│      ▼                                                               │
│  Worker pool (N bounded asyncio tasks)                              │
│      │  each: acquire token → call provider → classify → retry/emit  │
│      │  shared httpx.AsyncClient (pooled, HTTP/2)                    │
│      ▼                                                               │
│  Result queue → single writer task                                  │
│      │  append-only JSONL to disk, fsync batched                     │
│      │  counters only in memory — never a results list               │
│      ▼                                                               │
│  JobStore (memory + SQLite): counts, rates, cost, concurrency        │
└─────────────────────────────────────────────────────────────────────┘
      │                              │
      ▼                              ▼
GET /job/{id}/status          GET /job/{id}/download
  counters + live metrics       StreamingResponse over the JSONL
```

Produce this as `docs/diagrams/flow.mmd` (Mermaid) **and** render to `flow.svg`, committed. The
spec explicitly requires an architecture flow diagram covering: ingestion track, scatter pool
segmentation, backpressure throttling layer, gather collection layer. Label those four zones
visibly on the diagram.

---

## 4. Repository layout

```
do-batch-inference-engine/
├── README.md                    ← primary graded artifact, see §9
├── instructions.md              ← this file, committed (shows process)
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
├── Makefile                     ← make dev / test / lint / run / demo
├── .env.example
├── .github/workflows/ci.yml
├── docs/
│   ├── architecture.md
│   ├── scaling.md               ← memory + throughput analysis, §1.5
│   ├── decisions.md             ← short ADR log, one paragraph per decision
│   └── diagrams/flow.mmd, flow.svg
├── src/batchengine/
│   ├── main.py                  app factory, lifespan (httpx client, sqlite)
│   ├── config.py                pydantic-settings
│   ├── api/routes.py            exact endpoints from §5
│   ├── api/schemas.py
│   ├── core/
│   │   ├── models.py            PromptItem, RowResult, RowError, FailureClass, JobState
│   │   ├── ingest.py            streaming reader
│   │   ├── scheduler.py         job orchestration + lifecycle
│   │   ├── worker.py            worker loop
│   │   ├── ratelimit.py         TokenBucket + AdaptiveController
│   │   ├── retry.py             backoff policy + classify()
│   │   └── sink.py              JSONL writer + checkpointing
│   ├── providers/
│   │   ├── base.py              InferenceProvider Protocol
│   │   ├── digitalocean.py      live client
│   │   └── mock.py              deterministic chaos provider
│   ├── store/  base.py, memory.py, sqlite.py
│   ├── observability/  metrics.py, logging.py
│   └── extensions/  spaces.py, webhook.py
├── scripts/
│   ├── generate_batch.py        builds data/sample_batch.json (1,000 items)
│   ├── memory_probe.py          measures RSS at N = 1K / 10K / 100K / 500K
│   └── loadtest.py
├── data/sample_batch.json
└── tests/  unit/, integration/, property/
```

---

## 5. API contract

Implement these paths **literally as written in the project statement** — graders check exact
strings. Aliases may be added, originals may not be renamed.

### `POST /job`
```jsonc
// request
{
  "input_path": "data/sample_batch.json",   // required
  "model": "openai-gpt-oss-20b",            // optional, falls back to config
  "concurrency": 8,                          // optional, falls back to auto-sizing
  "max_tokens": 256,
  "webhook_url": null                        // optional, §8.2
}
// 202 Accepted
{ "job_id": "01J...", "status": "queued", "submitted_at": "..." }
```
Validation on the request path is **stat-only** — existence, readability, size. Never parse the
file. Must return in under 50 ms; assert this in a test.

### `GET /job/{id}/status`
```jsonc
{
  "job_id": "01J...",
  "status": "running",            // queued|running|succeeded|partial|failed|cancelled
  "counts": { "ingested": 1000, "succeeded": 812, "failed": 7, "in_flight": 6, "pending": 175 },
  "progress_pct": 81.9,
  "timing": { "started_at": "...", "elapsed_s": 241.3, "eta_s": 52.1, "throughput_rps": 3.4 },
  "backpressure": {
    "current_rate_limit_rps": 1.8,
    "configured_max_rps": 2.0,
    "throttle_events_429": 23,
    "retries_issued": 31,
    "retry_budget_remaining": 169,
    "circuit_state": "closed",
    "observed_mean_latency_s": 2.9,
    "littles_law_optimal_concurrency": 6
  },
  "usage": { "input_tokens": 121800, "output_tokens": 162400, "estimated_cost_usd": 0.0792 },
  "errors_by_class": { "invalid_input": 5, "transient_exhausted": 2 }
}
```
The `backpressure` and `usage` blocks are the differentiators. Most submissions will return a
percentage and nothing else.

### `GET /job/{id}/download`
Default `application/x-ndjson`, streamed line by line from disk.
`?format=json` streams a JSON array with manual `[`, `,`, `]` framing — never materialize it.
`?include=errors|success|all` (default `all`).
Return `409` if the job is still running unless `?partial=true`.

### Extras
`GET /healthz`, `GET /metrics` (Prometheus text), `POST /job/{id}/cancel` (graceful drain).

---

## 6. Core component specs

### 6.1 `ratelimit.py` — TokenBucket + AdaptiveController

Token bucket: `rate` (tokens/sec), `capacity` (burst), monotonic-clock refill, async `acquire()`.
Use `time.monotonic()`, never `time.time()` — wall-clock jumps must not corrupt pacing.

AIMD controller wrapping it:
- **On 429:** `rate = max(min_rate, rate × 0.75)`, then enter a cooldown window (default 5 s) during
  which further 429s do *not* trigger another decrease. Without the cooldown, a burst of 20
  concurrent 429s from one overage collapses the rate 20×. This is the most likely bug; write a
  test for it specifically.
- **On sustained success:** after 50 consecutive successes, `rate = min(max_rate, rate + 0.5)`.
- **On `x-ratelimit-reset-requests`:** hard-pause until that epoch, then resume with per-worker
  jitter of `uniform(0, 250ms)` to avoid the thundering herd described in §1.3.

Auto-sizing: track a rolling mean of observed latency; expose
`littles_law_optimal_concurrency = ceil(rate × mean_latency)`. Log when configured concurrency
exceeds it by more than 2× — that's a misconfiguration worth surfacing to an operator.

### 6.2 `retry.py` — classification and backoff

**Full jitter:** `sleep = uniform(0, min(cap, base × 2**attempt))` with `base=0.5s`, `cap=30s`.
Be ready to explain *why* full jitter beats plain exponential: plain exponential keeps the retry
cohort synchronized, so the herd re-collides on every wave; full jitter decorrelates it. This is a
near-certain review question.

**Global retry budget:** retries capped at 20% of total requests for the job. When exhausted, stop
retrying and fail remaining rows as `transient_exhausted`. Prevents retry-amplification collapse of
an already-struggling upstream.

**Circuit breaker:** if >50% of the last 100 requests are 5xx, open for 30 s, then half-open with a
single probe.

**Failure taxonomy** — implement as an enum, table-drive the tests:

| Condition | Class | Action |
|---|---|---|
| 200 | `success` | record result |
| 429 | `throttled` | retry; AIMD down; honor reset header; separate higher attempt cap (8) |
| 408, 500, 502, 503, 504 | `transient` | retry, full jitter, max 5 attempts |
| timeout / connect error | `transient` | retry |
| 400, 422 | `invalid_input` | **terminal** — record row error, never retry |
| 413 / context-length exceeded | `invalid_input` | **terminal** |
| 401, 403 | `fatal_auth` | **abort entire job** — fail fast |
| 402 | `fatal_billing` | **abort entire job** — prepaid balance exhausted (§1.4) |
| malformed/unparseable response | `transient` | retry once, then terminal |

The two `fatal_*` classes are the ones most candidates miss. Make sure they abort the job within
one request rather than after 1,000 × 5 wasted calls, and test that explicitly.

### 6.3 `ingest.py` — streaming reader

`ijson.items(fileobj, 'item')` yielding `PromptItem` one at a time. Support both a JSON array and
JSONL (sniff the first non-whitespace byte). Skip-and-record malformed items as `invalid_input`
rather than aborting the stream — a corrupt row must not kill the job. Never call `json.load()`.

### 6.4 `sink.py` — result writer

Single writer task consuming a result queue. Append-only JSONL, `fsync` every 100 rows or 2 s.
Maintains counters only. On restart, replay the JSONL to rebuild counts and resume from the last
completed offset.

### 6.5 `providers/mock.py` — the chaos provider

Configurable, seeded, deterministic:
`p_429`, `p_500`, `p_400`, `latency_mean`, `latency_jitter`, `hard_rpm_ceiling` (returns real 429s
above it with correct `x-ratelimit-*` headers), `fail_after_n`, `malformed_response_rate`.

This is what makes the whole test suite hermetic and what lets you *prove* backpressure works. Build
it early — it is more valuable than the live provider.

### 6.6 The memory argument — `O(concurrency)`, not `O(N)`

Peak RSS decomposes into four terms. **None of them contain N:**

```
peak_RSS = interpreter + framework baseline      (~100 MB, fixed)
         + queue_depth × item_size               (concurrency × 4 × ~600 B)
         + in_flight × (request + response)      (concurrency × ~2 KB)
         + write_buffer                          (100 rows × ~1.5 KB)
```

At concurrency 8 the job-data terms total roughly **185 KB**. RSS therefore stays flat at
~100–180 MB for N = 1,000 and for N = 500,000 alike. N-dependence lives on disk, where the JSONL
result file grows linearly and costs nothing.

Exactly three code paths produce this property. Each needs a comment saying so:

| Path | Correct | The O(N) mistake it avoids |
|---|---|---|
| Ingest | `ijson.items()` generator | `json.load()` — 250 MB of text → ~1 GB of objects at 500K |
| Dispatch | bounded `asyncio.Queue(maxsize=concurrency×4)` | pre-creating 500K Tasks → 0.5–1.5 GB |
| Collect | append-only JSONL sink, counters only | results accumulated in a list → ~750 MB |

Combined, the naive version reaches 2–3 GB at 500K and OOMs on any small droplet. Put this table in
the README next to the *measured* numbers from `scripts/memory_probe.py`. Measured beats claimed.

### 6.7 Spend guard — hard requirement

Live inference costs real prepaid balance. The engine must be structurally incapable of overspending.

- `MAX_JOB_SPEND_USD` (default `0.25`) — the scheduler tracks actual token cost as results arrive
  and aborts the job the moment actual-or-projected cost crosses this. Projected cost =
  `(cost_so_far / items_done) × items_total`, recomputed every 50 items.
- `MAX_TOTAL_SPEND_USD` (default `1.00`) — a committed `.spend_ledger.json` accumulates cost across
  every live run. The provider refuses to start a job when the ledger is exhausted. Never reset it
  automatically. **This is the binding constraint and it is deliberately tight:** at ~$0.065 per
  full 1,000-item run it allows ~15 runs, well above the ~5 this build needs.
- `max_tokens` defaults to `128`, not 256. Output tokens dominate cost (~9× input at `gpt-oss-20b`).
- Live runs default to `LIVE_SAMPLE_SIZE=50`. Exactly one full 1,000-item live run is performed, for
  the README, and its output is committed to `docs/sample_run.json`.
- The mock provider has no network path at all, so the test suite cannot spend money by construction.

Budget context: a full 1,000-prompt run at `max_tokens=128` on `openai-gpt-oss-20b` costs about
**$0.065**. The $5 prepaid credit is ~75 full runs. Cost is not the risk; an unbounded retry loop
is. These guards exist for that.

Platform-side backstop (verified in the control panel, 2026-09-15): serverless inference is
**prepaid-only** — requests draw down the prepaid balance and access is suspended at $0. With
auto-reload disabled, inference cannot bill a payment method at all. Our ledger is the inner cap;
the prepaid model is the outer one.

### 6.8 Deliberate over-rate observation run (M5)

Once the live provider works, perform one short, intentional experiment: run ~200 items with the
client limiter configured to roughly **4× the 120 RPM quota** to induce genuine throttling. Capture
and commit to `docs/observed_throttling.md`:

- the actual HTTP status and body returned on throttle
- the literal `x-ratelimit-limit-requests`, `x-ratelimit-remaining-requests`, and
  `x-ratelimit-reset-requests` values observed
- whether `x-ratelimit-reset-requests` behaves as the continuous-refill projection the docs
  describe, or as a fixed window boundary
- the observed latency distribution (p50/p95) under normal versus throttled conditions
- whether the AIMD controller converged, and how long convergence took

Cost: a few cents. Value: firsthand, citable evidence about how DigitalOcean's own limiter behaves,
and a direct demonstration that the backpressure design was validated against reality rather than
assumed. Document any place where observed behavior contradicts §1.3 — a documented contradiction is
a stronger artifact than agreement.

---

## 7. Testing

Coverage gate: 85%. Every test must be deterministic (seed everything, no real sleeps longer than
necessary — use a controllable clock in the rate limiter so backoff tests run instantly).

**Unit**
- `test_ratelimit.py` — refill math; burst; AIMD decrease; **cooldown prevents multiplicative
  collapse under concurrent 429s**; reset-header pause; jitter spread
- `test_retry.py` — full-jitter bounds; attempt caps; retry budget exhaustion; circuit breaker
  open → half-open → closed
- `test_classify.py` — table-driven over the entire §6.2 taxonomy
- `test_ingest.py` — array vs JSONL; malformed row skipped and recorded; never loads whole file
- `test_sink.py` — append correctness; fsync batching; replay-on-restart

**Integration** (full app via `httpx.ASGITransport`, mock provider)
- `test_job_lifecycle.py` — submit → 202 in <50 ms → poll → succeeded → download
- `test_backpressure_429.py` — mock with `hard_rpm_ceiling=120`; assert **zero dropped items**,
  429s were retried, observed rate converged near the ceiling, concurrency adapted down
- `test_partial_failure.py` — mixed 400s/500s; successes aggregated; errors isolated with reasons
- `test_conservation.py` — **the crown jewel.** Under every chaos configuration:
  `succeeded + failed == ingested`, and every item id appears exactly once
- `test_fatal_abort.py` — 401 and 402 abort within a handful of requests, not thousands
- `test_download_streaming.py` — response is chunked; peak RSS during download does not scale with
  result-set size
- `test_cancel.py` — graceful drain, no orphaned tasks

**Property** (`hypothesis`)
- Conservation invariant across randomized chaos parameters
- Rate limiter never exceeds configured rate over any sliding window

---

## 8. Extensions

Build only after §§3–7 are complete and CI is green.

### 8.1 Spaces checkpointing
S3-compatible via `boto3` / `aioboto3` against `https://{region}.digitaloceanspaces.com`.
Multipart-upload result chunks progressively so a crash mid-job preserves completed work. On
restart, resume from the last uploaded part. Keep it behind a feature flag; tests use `moto` or a
stub — never a live bucket in CI.

### 8.2 Webhook
`POST` to the registered URL on terminal state. Payload: job id, final status, counts, download URL.
Sign with HMAC-SHA256 in an `X-Signature` header. Retry 3× with full jitter. **Validate the URL
against SSRF** — reject private/link-local/loopback ranges unless explicitly allowed by config.
Mentioning SSRF unprompted is a strong security signal.

---

## 9. README — the primary graded artifact

Required sections, in order:

1. **What this is** — two sentences.
2. **Quickstart** — copy-pasteable, against the 1,000-prompt template, works with zero credentials
   (mock provider), then the live-mode variant. Must actually work from a clean clone; verify it.
3. **Architecture** — embedded `flow.svg` + prose walking the four zones.
4. **Design decisions** — token bucket vs. semaphore; full jitter vs. exponential; AIMD cooldown;
   queue-as-backpressure; JSONL sink vs. in-memory list. One short paragraph each, each naming the
   alternative that was rejected and why.
5. **Failure taxonomy** — the §6.2 table verbatim.
6. **Scaling and memory** — *measured* RSS table from `scripts/memory_probe.py` at N = 1K / 10K /
   100K / 500K, showing flat memory. Then the throughput analysis: 120 RPM ceiling, Little's Law
   sizing, the 4,167-token RPM/TPM crossover, and the 69-hour figure for 500K.
7. **Product findings** — the `meta-llama-3-8b-instruct` serverless availability gap (§1.1), the
   prepaid-balance 402 case, and the reset-header thundering-herd note.
8. **When to stop using this service** — §1.5 decision table: Batch Inference vs. Dedicated
   Inference vs. quota increase, with crossover points and costs.
9. **Cost model** — per-1,000-prompt and per-500K figures across the candidate models.
10. **Testing** — how to run, what each suite proves.
11. **What I'd do with more time** — honest, specific, short.

Section 8 is the one that separates a Staff submission from a Principal one. Do not cut it.

---

## 10. CI

`.github/workflows/ci.yml`, on push and PR, matrix Python 3.11 + 3.12:

1. `ruff check .` and `ruff format --check .`
2. `mypy --strict src/`
3. `pytest --cov=src/batchengine --cov-fail-under=85`
4. `docker build .`
5. Upload coverage as an artifact

**No secrets.** If CI needs a key, the provider abstraction is wrong.

---

## 11. Milestones — run continuously, do not wait for approval

**Operating mode: autonomous overnight.** Lily reviews the finished work in the morning. Do not stop
at gates to ask permission. Complete M1 → M5 in order, committing at each boundary, and write the
morning review packet described in §12.3.

A milestone's gate condition is a **self-check**, not a human checkpoint: verify it, record the
result in `BUILD_LOG.md`, and continue.

| # | Scope | Self-check gate | Est. |
|---|---|---|---|
| **M1** | Repo skeleton, config, models, mock provider, streaming ingest, CI pipeline | CI green; ingest test passes; `POST /job` returns 202 in <50 ms | ~2h |
| **M2** | Scheduler, bounded queue, worker pool, JSONL sink, status + download | End-to-end job completes against mock; **conservation test passes** | ~3h |
| **M3** | Token bucket, AIMD, full-jitter retry, full failure taxonomy, circuit breaker | Backpressure + fatal-abort tests pass; zero dropped items under 429 storm | ~3h |
| **M4** | Metrics + cost accounting, spend guard, memory probe, README, diagram, scaling doc | Measured memory table produced; README complete | ~3h |
| **M5** | Live DO provider run, Spaces checkpointing, webhook, polish, final CI | All green; one recorded live run if a key is present | ~2h |

**Note the M4/M5 reordering versus the original plan.** Everything that does not require a
DigitalOcean credential now happens first, so a missing or broken key at 2 a.m. costs the polish
work rather than the core deliverable.

Hard deadline: **end of day Wednesday, September 16, 2026.** If time compresses, cut in this order:
Spaces checkpointing → webhook → live run. **Never cut** the README scaling section (§9.6/9.8) or
the conservation test. Those two carry the most review weight.

---

## 12. Autonomous operation rules

These govern the unattended overnight run. The quality bar in §0 does not relax — only the
check-in cadence changes.

### 12.1 Never block on a human

If something in this spec is ambiguous, wrong, or under-specified:

1. Choose the **lowest-risk, most conventional** option.
2. Append an entry to `OPEN_QUESTIONS.md`: what was ambiguous, what was chosen, what the
   alternative was, and how hard it would be to reverse.
3. Mark the code with `# TODO(review): <one line>`.
4. Continue.

Never idle waiting for a reply. An unreviewed reasonable choice is recoverable in the morning; a
stalled build at 3 a.m. is not.

If a gate self-check fails, fix it. If still stuck after ~30 minutes of genuine attempts: stub the
smallest possible piece, mark it `@pytest.mark.xfail(reason=...)` or `# TODO(review)`, log it
prominently in `BUILD_LOG.md` under **BLOCKED**, and move to the next milestone. Do not let one
failure consume the night.

### 12.2 Hard safety rails — no exceptions

- **Never** run a live inference job above **N = 1,000** items.
- **Never** exceed `MAX_TOTAL_SPEND_USD` ($3.00). Check the ledger before every live job.
- **Never** run live inference at all if `DO_INFERENCE_KEY` is absent — skip M5's live portion,
  log it, and finish everything else. This is an expected, non-blocking outcome.
- **Never** commit `.env`, a key, a token, or any real credential. Verify `.gitignore` before the
  first commit and re-verify before the last.
- **Never** log prompt content or key material.
- **Never** force-push or rewrite history.
- The full test suite must remain runnable with zero network and zero credentials at all times.

### 12.3 Morning review packet — produce all four

Lily reads these first thing. Write them as you go, not at the end.

1. **`BUILD_LOG.md`** — chronological, one section per milestone: what was built, gate self-check
   result, elapsed time, anything surprising. A **BLOCKED** section at the top if anything is
   stubbed or xfailed, so it cannot be missed.
2. **`OPEN_QUESTIONS.md`** — every judgment call made without her, per §12.1. Ordered by how much
   it would cost to reverse, most expensive first.
3. **`docs/decisions.md`** — the ADR log. One paragraph per design decision: what was chosen, what
   was rejected, why. This is the raw material for her interview prep, so write it for a reader who
   will have to defend it out loud, not for a reader who already agrees.
4. **`STATUS.md`** — a single-screen summary: milestones complete, test count, coverage %, CI
   status, live-run status, total spend, and the top three things needing her attention.

### 12.4 Commit discipline

Commit at every milestone boundary and at any meaningful sub-step. Real messages describing the
*why*. The commit history is reviewed alongside the code — a single squashed dump reads as
AI-generated and undercuts the authorship story. Conventional-commits style is fine.

### 12.5 Where the spec is wrong

This document was written from documentation research, not from running the code. Some of it will be
wrong. When reality contradicts it, **follow reality and record the contradiction** in
`OPEN_QUESTIONS.md` under a `SPEC-CORRECTION` heading. Getting a working, honest system matters more
than conforming to these instructions. Flagging the correction is itself a scored behavior.

---

## 13. Constraints and style

- Type hints everywhere; `mypy --strict` clean.
- Docstrings on every public function stating *why*, not what.
- No global mutable state except the app lifespan container.
- One shared `httpx.AsyncClient` per process, created in lifespan, explicitly closed.
- Structured JSON logs with `job_id` and `item_id` on every line.
- Never log prompt content or API keys.
- Config via environment with `.env.example` committed and `.env` gitignored.
- Commit in logical increments with real messages — the commit history is part of what gets
  reviewed. No single "initial commit" dump.
- Anything non-obvious gets a comment explaining the tradeoff, aimed at the reviewer.

---

## 14. Environment setup Lily performs (not Claude Code)

1. Create/sign in to a DigitalOcean account.
2. Add a **prepaid serverless inference balance** — $5 is roughly 50 full 1,000-prompt runs.
3. Gradient AI → Serverless Inference → **Create model access key** (`sk-do-...`).
4. Put it in `.env` as `DO_INFERENCE_KEY=...`. Confirm `.env` is gitignored before the first commit.
5. Optional, for §8.1: create a Spaces bucket and generate Spaces access keys.
6. Smoke test:
   ```bash
   curl -X POST https://inference.do-ai.run/v1/chat/completions \
     -H "Authorization: Bearer $DO_INFERENCE_KEY" \
     -H "Content-Type: application/json" \
     -d '{"model":"openai-gpt-oss-20b","messages":[{"role":"user","content":"ping"}],"max_tokens":8}'
   ```

Never commit a key. Never paste a key into chat.
