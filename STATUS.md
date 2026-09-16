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

- **101 tests passing**, 0 failing, 0 skipped.
- **96% line coverage** (`pytest --cov`), gate is 85%.
- `ruff check .` — clean. `ruff format --check .` — clean.
- `mypy --strict src/` — clean, 0 errors across 30 source files.
- `.github/workflows/ci.yml` runs all of the above on a 3.11/3.12 matrix
  plus `docker build .`, with zero secrets required anywhere in the
  pipeline. **Not verified locally** — Docker Desktop's engine was not
  running in this environment (see BLOCKED). The Dockerfile is a
  straightforward `python:3.12-slim` + `pip install .` build with no
  unusual steps; risk of a CI-only failure is low but unconfirmed.

## Live run status

**No live inference call was made in this build.** `DO_INFERENCE_KEY` was
never provisioned (expected — Lily performs environment setup per §14 of
instructions.md). Consequently:

- `docs/sample_run.json` (the one full 1,000-item live run instructions.md
  asks for) — **not produced**.
- `docs/observed_throttling.md` (§6.8's deliberate over-rate experiment) —
  written as a pending checklist with the exact command to run once a key
  exists, but **not executed**.
- The live provider (`providers/digitalocean.py`) is fully implemented and
  unit-tested against `respx`-mocked HTTP responses, but has never made a
  real network call.

This is the single biggest thing needing your attention: once
`DO_INFERENCE_KEY` is in `.env`, run `make demo` against `data/sample_batch.json`
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
