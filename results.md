# Results

Evidence index. Every claim this project makes, with what proves it — a
test, a measurement, or a specific artifact. No narrative; see
[`docs/design.md`](docs/design.md) for the design rationale and
[`BUILD_LOG.md`](BUILD_LOG.md) for how the build proceeded. Anything not
directly measured is marked **not measured** rather than estimated.

## 1. Requirements traceability

Requirement IDs match `docs/design.md` §1.2/§1.3.

| ID | Requirement | Evidence |
|---|---|---|
| F1 | Ingest a file, return a job ID immediately, execute in background | `tests/integration/test_job_lifecycle.py::test_submit_returns_202_under_50ms` (202 returned before any parsing); `test_full_lifecycle_submit_poll_download` (job progresses after the response is sent) |
| F2 | Distribute work across a bounded worker pool against a live endpoint | `core/scheduler.py::JobRunner.run` spawns N workers against one shared queue; `tests/integration/test_conservation.py` runs at `concurrency` up to 16; **live**: `docs/sample_run.json` (1,000 items, concurrency 4, real endpoint) |
| F3 | Absorb 429 backpressure with backoff+jitter, no dropped items | `tests/unit/test_retry.py` (full-jitter bounds, decorrelation); `tests/integration/test_backpressure_429.py` (mock `hard_rpm_ceiling`); **live**: `docs/observed_throttling.md` (52 real 429s in the throttled run, all retried; 11 real 429s in the 1,000-item run, all retried — zero dropped in either) |
| F4 | Isolate row failures, aggregate successes | `tests/unit/test_classify.py` (table-driven over the full §6.2 taxonomy); `tests/integration/test_partial_failure.py` |
| F5 | Expose progress via `GET /job/{id}/status` | `tests/integration/test_job_lifecycle.py`, `test_api_edge_cases.py`; **live**: every poll in `docs/sample_run.json`'s run |
| F6 | Expose results via `GET /job/{id}/download` | `tests/integration/test_download_streaming.py` (20,000-row stream, no `content-length`); `test_job_lifecycle.py` (ndjson + json array framing) |
| N1 | `POST /job` < 50ms regardless of input size | `test_submit_returns_202_under_50ms` — asserts on a 1,000-row file; measured, not assumed |
| N2 | Peak RSS independent of N | `scripts/memory_probe.py` → `docs/memory_probe_results.json` (see §4 below); measured at N=1K/10K/100K/500K against the mock provider |
| N3 | `succeeded + failed == ingested`, always | `tests/integration/test_conservation.py` (10 fixed chaos configs incl. N=1,000 and real-120RPM+chaos); `tests/property/test_conservation_property.py` (15 hypothesis-generated chaos mixes); **live**: `docs/sample_run.json` (1000+0=1000) and `docs/observed_throttling.md` (188+12=200) |
| N4 | Hard per-job and cross-run spend caps | `core/spend_ledger.py` unit-tested (`tests/unit/test_spend_ledger.py`) for the ledger arithmetic itself. **Gap, noted honestly**: the per-job guard's actual trip-and-abort path (`scheduler.py::on_spend_check`) and the pre-flight 402 refusal in `api/routes.py` (`if is_live: ... total_spend >= max_total_spend_usd`) have **no automated test** — the refusal branch only runs when `is_live=True`, which the zero-credential test suite (N5) never sets. Live-verified only, informally, this session: the pre-flight check ran (and passed, ledger well under cap) before both the 1,000-item run and the throttling experiment. |
| N5 | Full suite: zero network, zero credentials | CI (`.github/workflows/ci.yml`) never sets `DO_INFERENCE_KEY`. **Three incidents, all fixed this session** (full writeups in `BUILD_LOG.md`): (1) `monkeypatch.delenv("DO_INFERENCE_KEY")` didn't stop `pydantic-settings` reading a real key out of `.env` (a lower-priority but still-consulted source) — fixed to `monkeypatch.setenv(..., "")`. (2) `test_webhook.py` made a real, unmocked DNS lookup (`socket.getaddrinfo("example.com", ...)`, not interceptable by `respx`) — fixed with a monkeypatched fake resolver. (3) every HTTP-driven test runs real `asyncio.sleep`-based retry backoff with no ceiling on cumulative delay under an adversarial chaos config — fixed by adding configurable `retry_base_s`/`retry_cap_s` and setting them to sub-100ms in the test fixture. (2) and (3) together are the suspected cause of two separate >1hr `test (3.12)` CI hangs; a `timeout-minutes: 10` backstop was also added to the workflow. Verified: full suite with `.env` present creates zero new ledger files; full suite runtime dropped from ~93-96s to 80.29s after fix (3). |

## 2. Live run results

Full record: [`docs/sample_run.json`](docs/sample_run.json). Model
`mistral-3-14B`, `max_tokens=128`, real 120 RPM account quota, default
(auto-sized) concurrency of 4.

| Metric | Value |
|---|---|
| Ingested / succeeded / failed | 1000 / 1000 / 0 |
| Elapsed | 442.85s (~7.4 min) |
| Throughput | 2.258 req/s |
| Throttle events (429) / retries issued | 11 / 11 (all recovered) |
| Retry budget remaining at end | 189 / 200 (limit = 20% of 1,000) |
| Observed mean latency | 0.809s |
| Little's-Law optimal concurrency (converged) | 2 |
| Input tokens / output tokens | 15,337 / 76,567 |
| **Actual cost** | **$0.018381** |

**Estimated vs. actual cost, and why actual came in lower:**

| Estimate source | Basis | $/1,000 items | vs. actual |
|---|---|---|---|
| README/`docs/scaling.md` generic table | ~150 input / ~200 output tokens/prompt, applied uniformly to every catalog model | $0.07 | actual is 3.8× lower |
| `docs/model-selection.md` | live single-prompt measurement: 15 input + 94 output tokens | $0.022 | actual is 1.2× lower |
| **Actual (this run)** | 1,000-item average: 15.3 input + 76.6 output tokens/item | **$0.0184** | — |

Both gaps trace to the same cause: `mistral-3-14B`'s real chat-template
overhead (~15 tokens) is far below the README's generic 150-token
assumption, and this run's completions averaged 76.6 output tokens/item —
below even `model-selection.md`'s single-sample measurement of 94, and
well under the 128 cap (no truncation occurred).

**Notable finding — 11 throttles with zero configured overshoot.** See
`docs/decisions.md`'s "AIMD headroom probing vs. a rate ceiling matched
exactly to the configured quota" entry: `AdaptiveController.max_rate =
rate_rps × 1.5` deliberately permits climbing 50% past the configured RPM,
which is exactly what happened here.

## 3. Observed throttling results (§6.8)

Full record: [`docs/observed_throttling.md`](docs/observed_throttling.md).
200-item run, client paced at 480 RPM (4× the 120 RPM quota), concurrency 32.

| Metric | Value |
|---|---|
| Ingested / succeeded / failed | 200 / 188 / 12 (`transient_exhausted`) |
| Throttle events (429) | 52 |
| Retries issued / retry budget limit | 40 / 40 (budget fully exhausted) |
| Elapsed | 31.93s |
| Latency — successful requests, p50 / p95 | 0.848s / 1.399s |
| Latency — 429 responses themselves, p50 / p95 | 0.256s / 0.297s |
| AIMD rate: start → end | 8.0 rps → 4.0 rps (did not reach the true ~2.0 rps before the run ended) |

**Contradiction found vs. a pre-recorded manual baseline** (flagged per
instructions.md §12.5 — see the doc for full detail): observed
`x-ratelimit-limit-tokens-per-minute` was **750000**, not the baseline's
500000; observed `x-ratelimit-limit-tokens-per-day` was **8000000**, not
6000000; `x-ratelimit-remaining-tokens-per-day` **decremented** across
calls (800000 → 799260 over 100 requests), contradicting the baseline's
"pinned, not decrementing" observation. Only `x-ratelimit-limit-requests:
120` matched.

`x-ratelimit-reset-requests` confirmed as a **continuous forward-refill
projection** (advances second-by-second with wall-clock time across 52
throttled attempts), not a fixed per-minute window boundary — consistent
with §1.3's description, with the added nuance that integer-second
truncation can make it read up to ~1s stale.

## 4. Measured memory

Full record: [`docs/memory_probe_results.json`](docs/memory_probe_results.json),
produced by `scripts/memory_probe.py` against the mock provider (zero API
calls, per the §12.2 hard rail on live-run size).

| N | Baseline RSS | Peak RSS | Delta over baseline | Throughput |
|---|---|---|---|---|
| 1,000 | 36.7 MB | 37.5 MB | 0.8 MB | 4,992 items/s |
| 10,000 | 37.5 MB | 37.7 MB | 0.2 MB | 5,410 items/s |
| 100,000 | 37.7 MB | 38.0 MB | 0.3 MB | 2,633 items/s |
| 500,000 | 38.0 MB | 38.1 MB | 0.0 MB | 2,311 items/s |

500× increase in N; delta stays inside noise (0.0–0.8 MB) the entire way.

## 5. Test suite summary

| Category | Count | Notes |
|---|---|---|
| Unit (`tests/unit/`) | 81 | Fakes/respx only, no network |
| Integration (`tests/integration/`) | 30 | Full app via `httpx.ASGITransport`, mock provider |
| Property (`tests/property/`) | 2 test functions | `hypothesis`-driven: 15 generated chaos mixes (conservation) + 40 generated rate/capacity/pattern combinations (token bucket bound) — i.e. many more than 2 actual cases exercised |
| **Total** | **113** | 0 failing, 0 skipped |
| Coverage | **96%** | Gate is 85% (`pytest --cov`, `--cov-fail-under=85`) |
| `ruff check .` | Clean | 0 issues |
| `ruff format --check .` | Clean | 0 files would reformat |
| `mypy --strict src/` | Clean | 0 errors across 30 source files |
| CI (`.github/workflows/ci.yml`) | See below | 3.11/3.12 matrix: ruff, mypy, pytest+coverage, docker build |

## 6. Cost model — actual measured, per 1,000 items, by model

Computed by `scripts/cost_table.py` from `providers/digitalocean.py::MODEL_PRICING`
(the catalog's advertised $/1M token rates) — **not** independently
live-measured for every model in this table; only `mistral-3-14B` has a
live-measured actual run (§2 above). The generic-assumption estimates
below use ~150 input / ~200 output tokens/prompt for comparability across
models, which §2 shows overstates cost for at least `mistral-3-14B` by
~4×.

| Model | $/1M in | $/1M out | Generic-assumption est., 1,000 items | Live-measured actual, 1,000 items |
|---|---|---|---|---|
| `mistral-3-14B` (default) | $0.20 | $0.20 | $0.07 | **$0.0184** (measured, §2) |
| `openai-gpt-oss-120b` | $0.06 | $0.39 | $0.09 | not measured |
| `openai-gpt-oss-20b` | $0.05 | $0.45 | $0.10 | not measured (known to return null `content` at `max_tokens=128` — see `docs/model-selection.md`; a live cost figure at this budget would not reflect usable output) |
| `gemma-4-31B-it` | $0.18 | $0.50 | $0.13 | not measured |
| `llama-4-maverick` | $0.20 | $0.696 | $0.17 | not measured |

## 7. Live spend accounting

Persistent ledger: `.spend_ledger.json` (gitignored, not committed —
figures recorded here instead). Total across every real API call made
during this project's development, across all sessions: **$0.034948**,
against a `MAX_TOTAL_SPEND_USD` cap of $1.00. Breakdown:

| Source | Amount |
|---|---|
| Smoke test (3 items) before the full run | $0.0000668 |
| Full 1,000-item live run (§2) | $0.018381 |
| Test-suite contamination incident (§1, N5) — recovered and added to the ledger for accurate accounting, not separately re-verifiable per-call | $0.0112304 |
| Throttling-experiment 200-item run (§3) | $0.003369 |
| Normal-condition 100-item comparison run (§3) | $0.001801 |
| Direct-probe follow-up (uninstrumented, estimated) | $0.0001 |
