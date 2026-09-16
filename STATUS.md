# Status

Single-screen summary. Read this first.

## Milestones

| # | Scope | Status |
|---|---|---|
| M1 | Repo skeleton, config, core models, mock provider, streaming ingest, CI pipeline | **Done** |
| M2 | Scheduler, bounded queue, worker pool, JSONL sink, status + download | **Done** |
| M3 | Token bucket, AIMD, full-jitter retry, full failure taxonomy, circuit breaker | **Done** |
| M4 | Metrics + cost accounting, spend guard, memory probe, README, diagram, scaling doc | **Done** |
| M5 | Live DO provider, Spaces checkpointing, webhook, polish, final CI | **Done** — live run + throttling experiment both complete |

## Tests / coverage / CI

- **114 tests passing**, 0 failing, 0 skipped.
- **96% line coverage** (`pytest --cov`), gate is 85%.
- `ruff check .` — clean. `ruff format --check .` — clean.
- `mypy --strict src/` — clean, 0 errors across 30 source files.
- `.github/workflows/ci.yml` runs all of the above on a 3.11/3.12 matrix
  plus `docker build .`, with zero secrets required anywhere in the
  pipeline. **The CI hang is fixed and understood, not just "currently
  green."** The real root cause: `core/ingest.py`'s producer thread kept
  iterating an *entire remaining large file* after an early
  cancel/abort, cross-thread round-tripping every leftover row; under
  `pytest-cov`'s tracing overhead on GitHub's 2-vCPU runner this
  compounded into minutes, and a `concurrent.futures` `atexit` hook then
  blocked the whole process from exiting until that thread finally
  finished — invisible to a per-test timeout because the hang was in
  interpreter shutdown, not inside any test. Fixed with a cooperative
  stop signal (one `threading.Event`, checked once per row) and covered
  by a new regression test
  (`tests/unit/test_ingest.py::test_producer_stops_promptly_on_early_close_of_a_large_file`,
  confirmed to fail against the pre-fix code first). Three earlier fixes
  (dotenv key fallback, unmocked DNS, unbounded retry-backoff sleep)
  were each real bugs worth fixing but were not, in the end, the cause
  of the hang — full retrospective, including why each apparent "clean
  run" after those fixes was coincidental timing rather than resolution,
  in `BUILD_LOG.md`'s "FOURTH INCIDENT" section. `timeout-minutes: 10`
  and `pytest-timeout --timeout=60` stay as backstops against a future
  regression. One cosmetic, non-blocking warning (an `aiosqlite`
  background-thread teardown race, caught by pytest as a warning, never
  failing a build) remains logged as `OPEN_QUESTIONS.md` item #6, not
  chased further.

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

## Live run status — both done

**`DO_INFERENCE_KEY` was provisioned and both M5 live deliverables are
complete.**

- **The one full 1,000-item live run**: 1000/1000 succeeded, $0.018381
  actual cost, 11 real 429s with zero configured overshoot (a genuine,
  unscripted AIMD-hunting finding — see `docs/decisions.md`). Recorded
  verbatim in `docs/sample_run.json`; summarized in `results.md` §2.
- **The §6.8 over-rate throttling experiment**: run at 4× the configured
  quota (480 RPM, 200 items), producing 52 real 429s and a genuine
  `partial` outcome (188 succeeded, 12 `transient_exhausted` — the retry
  budget exhausted before AIMD converged). **Found and documented a real
  contradiction against a pre-recorded manual baseline** (observed
  token-per-minute/token-per-day limits and the remaining-tokens-per-day
  decrement behavior all differ from the baseline — see
  `docs/observed_throttling.md` for the full comparison). Confirmed
  `x-ratelimit-reset-requests` behaves as the continuous forward-refill
  projection the docs describe.
- `.env` reverted to `.env.example`'s defaults (`RATE_LIMIT_RPM=120`,
  `LIVE_SAMPLE_SIZE=50`) once both experiments finished.

**Three incidents during this work, all found, fixed, and documented**
(full writeups in `BUILD_LOG.md`): (1) the test fixture's
`DO_INFERENCE_KEY` isolation had a real gap (`monkeypatch.delenv` doesn't
block `pydantic-settings`'s dotenv fallback) that let ~11 test-suite runs
make real live calls once `.env` existed, for ~$0.0112 not originally
recorded in the persistent ledger — fixed and verified. (2) A test made a
real, unmocked DNS lookup, and (3) every HTTP-driven test ran real,
uncapped retry-backoff sleeps under the actual scheduler — together the
suspected cause of two separate >1hr CI hangs on the `test (3.12)` matrix
leg (see "Tests / coverage / CI" above). Both fixed, verified locally
(113 passed, 96% coverage, suite runtime down to 80.29s), and a
`timeout-minutes: 10` backstop added to CI so a future regression like
either fails fast instead of silently hanging.

## Total spend

**$0.034948**, against a `MAX_TOTAL_SPEND_USD` cap of $1.00. Full
breakdown in `results.md` §7 (the live run, the throttling experiment and
its comparison run, a smoke test, and the recovered test-contamination
amount above). Nowhere close to the cap.

## BLOCKED

Nothing currently. (Docker was never verified locally in this
environment, but is now confirmed green on CI — see above.)

## Top three things needing your attention

1. **Review the two genuine findings from this session's live work**:
   the AIMD-hunting-with-zero-overshoot finding (`docs/decisions.md`) and
   the manual-baseline contradiction on token-limit headers
   (`docs/observed_throttling.md`) — both are the kind of thing likely to
   come up in a design review, and both are backed by real data, not
   speculation.
2. **Review `OPEN_QUESTIONS.md` item #1** (the server-side live-item cap)
   before your interview — it's the judgment call most likely to come up,
   since it's a field added beyond the literal §5 schema in service of the
   §12.2 safety rail.
3. **`results.md` §1 notes two real, honestly-marked test-coverage gaps**:
   the spend guard's trip-and-abort path and the pre-flight
   ledger-exhausted 402 refusal have no automated test (structurally
   unreachable in the zero-credential test suite) — only live-verified
   informally this session. Worth a look if you want that path covered
   before the review.

## What's *not* blocked, for contrast

Every test-suite-verifiable claim in this repo — the conservation
invariant, the failure taxonomy, the AIMD cooldown behavior, the flat
memory scaling from N=1,000 to N=500,000 — is backed by a passing test or
a measured number produced in this session, not by an assertion in prose.
