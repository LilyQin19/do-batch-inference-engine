# Status

Single-screen summary. Read this first.

## Milestones

| # | Scope | Status |
|---|---|---|
| M1 | Repo skeleton, config, core models, mock provider, streaming ingest, CI pipeline | **Done** |
| M2 | Scheduler, bounded queue, worker pool, JSONL sink, status + download | **Done** |
| M3 | Token bucket, AIMD, full-jitter retry, full failure taxonomy, circuit breaker | **Done** |
| M4 | Metrics + cost accounting, spend guard, memory probe, README, diagram, scaling doc | **Done** |
| M5 | Live DO provider, Spaces checkpointing, webhook, polish, final CI | **Partial — see BLOCKED below** |

## Tests / coverage / CI

- **113 tests passing**, 0 failing, 0 skipped.
- **96% line coverage** (`pytest --cov`), gate is 85%.
- `ruff check .` — clean. `ruff format --check .` — clean.
- `mypy --strict src/` — clean, 0 errors across 30 source files.
- `.github/workflows/ci.yml` runs all of the above on a 3.11/3.12 matrix
  plus `docker build .`, with zero secrets required anywhere in the
  pipeline. **Not verified locally** — Docker Desktop's engine was not
  running in this environment (see BLOCKED). The Dockerfile is a
  straightforward `python:3.12-slim` + `pip install .` build with no
  unusual steps; risk of a CI-only failure is low but unconfirmed.

## Post-review fixes (read this section if you're re-checking after a review)

A review found three real defects and asked for expanded design docs.
All fixed and documented in detail in `BUILD_LOG.md`'s "Post-review
fixes" section; summary:

1. **Conservation was not actually guaranteed (critical).**
   `worker.py::process_item` only caught `ProviderTransportError` — any
   other exception killed the worker task, leaked `in_flight`, skipped
   `task_done()`, and stopped the job from ever reaching a terminal
   state. Every existing test passed only because the mock provider never
   raised anything else. Fixed with explicit `CancelledError` re-raise +
   broad-exception-to-transient conversion + `finally`-based cleanup in
   `process_item`, a second outer safety net in `worker_loop`, and
   `return_exceptions=True` on the scheduler's `gather`. A new chaos knob
   (`MockProviderConfig.p_unexpected_exception`) reproduces this, and the
   new test was confirmed to fail against the old code and pass against
   the fix — not just written and assumed correct.
2. **Circuit breaker could deadlock the job.** A half-open probe that
   never called `record()` (crash, or an abort/cancel early return) left
   `allow_request()` returning `False` forever. Fixed with a
   `probe_timeout_s` that clears a stale probe and lets a fresh one
   through; same before/after verification as #1.
3. **Conservation was only tested at N=60 with the rate limiter
   effectively disabled** in 6 of 7 cases. Added an N=1,000 case and a
   case combining the real 120 RPM quota with simultaneous chaos.

Plus: a `docs/decisions.md` entry on per-item dispatch vs. the spec's
literal chunk-partitioning wording, two new rendered diagrams (job
lifecycle state machine; single-item sequence diagram), and "Delivery
semantics" + "Non-goals" sections in `docs/architecture.md` — including an
explicit, code-verified statement that this system is at-least-once while
the process is alive and *not* automatically resumed across a restart
(a resubmit after a crash re-pays for already-completed items against a
live provider).

**Also this pass:** `docs/model-selection.md` — written by you directly,
from a live model-catalog check with a real key, outside this session —
found that `openai-gpt-oss-20b` (the prior default) is a reasoning model
that returns `content: null` at this service's default `max_tokens=128`.
Flagged it back to you before acting on its claims (they didn't match the
committed code at the time); you confirmed it's authoritative, so the
codebase was reconciled to match: default model is now `mistral-3-14B`
(`config.py`, `.env.example`), `providers/digitalocean.py` now treats
non-string `content` as malformed instead of a silent false success, and
`README.md`/`docs/scaling.md`/`docs/decisions.md` were updated to match.
A second file you added, `docs/design.md` (a full architecture writeup),
didn't contradict anything and was committed as-is — see BUILD_LOG.md for
a note that it overlaps `docs/architecture.md` and may be worth merging
later.

## Live run status

**No live inference call was made by this session.** `DO_INFERENCE_KEY`
was never provisioned inside it (expected — Lily performs environment
setup per §14 of instructions.md). One narrower thing *did* happen live,
outside this session: you ran a model-catalog check with a real key (see
above and `docs/model-selection.md`) — that is reflected in code. The two
bigger M5 deliverables are still outstanding:

- `docs/sample_run.json` (the one full 1,000-item live run instructions.md
  asks for) — **not produced**.
- `docs/observed_throttling.md` (§6.8's deliberate over-rate experiment) —
  written as a pending checklist with the exact command to run once a key
  exists, but **not executed**.
- The live provider (`providers/digitalocean.py`) is fully implemented and
  unit-tested against `respx`-mocked HTTP responses (including the
  null-content case your catalog check surfaced), but this session has
  never made a real network call through it.

This is the single biggest thing needing your attention: with
`DO_INFERENCE_KEY` in `.env`, run `make demo` against `data/sample_batch.json`
for the one full 1,000-item run, and the §6.8 experiment per
`docs/observed_throttling.md`, then fill in both docs.

## Total spend

**$0.00.** No live inference call was made, so no cost was incurred and
`.spend_ledger.json` was never created. `MAX_TOTAL_SPEND_USD` ($1.00
default per instructions.md) has not been touched.

## BLOCKED

- **Docker build not locally verified** (Docker Desktop engine unavailable
  in this environment). CI will build it on push; if it fails there, the
  Dockerfile is the first place to look — it's simple enough that a
  failure would likely be an environment/base-image issue rather than an
  application one.
- **Everything under "Live run status" above** — blocked on
  `DO_INFERENCE_KEY`, which is expected/non-blocking per §12.2.

## Top three things needing your attention

1. **Provision `DO_INFERENCE_KEY` and run the one full 1,000-item live
   job + the §6.8 throttling experiment**, then fill in
   `docs/sample_run.json` and `docs/observed_throttling.md`. Everything
   else in the spec that depends on a live key funnels through this one
   step.
2. **Review `OPEN_QUESTIONS.md` item #1** (the server-side live-item cap)
   before your interview — it's the judgment call most likely to come up,
   since it's a field added beyond the literal §5 schema in service of the
   §12.2 safety rail.
3. **Confirm the Docker build succeeds in CI** on first push — it was
   never run locally in this environment.

## What's *not* blocked, for contrast

Every test-suite-verifiable claim in this repo — the conservation
invariant, the failure taxonomy, the AIMD cooldown behavior, the flat
memory scaling from N=1,000 to N=500,000 — is backed by a passing test or
a measured number produced in this session, not by an assertion in prose.
