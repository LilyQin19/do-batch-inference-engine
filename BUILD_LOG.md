# Build log

Chronological. Written as the build progressed, not reconstructed after
the fact.

## BLOCKED (read this first)

1. **Docker Desktop's engine was not running in this environment**, so
   `docker build .` was never executed locally. The CI step
   (`.github/workflows/ci.yml` job `docker`) will run it on push. The
   Dockerfile is a plain `python:3.12-slim` + `pip install .`, so this is
   low-risk, but genuinely unconfirmed.
2. **No `DO_INFERENCE_KEY` was provisioned inside this session**, so this
   session never made a live inference call itself, and M5's two big
   live-run deliverables (one full 1,000-item run + the §6.8 over-rate
   experiment) are still not done. This is an expected, non-blocking
   outcome per instructions.md §12.2 — see STATUS.md for exactly what's
   pending.
   **Narrower update:** a live model-catalog check (`GET .../v1/models`
   against the real endpoint) *was* performed with a real key, outside
   this session, by Lily directly — see `docs/model-selection.md`. That
   check is what corrected the default model to `mistral-3-14B` (§ below)
   and is reflected in code and tests. It does not cover the two
   deliverables above.

Nothing else was stubbed, xfailed, or left half-implemented.

## M1 — Repo skeleton, config, core models, mock provider, streaming ingest

**Gate self-check:** CI-equivalent checks green locally (ruff, mypy
--strict, pytest); ingest tests pass; `POST /job` verified <50ms in a
dedicated test. **Result: pass.**

Built `core/models.py` (dependency-free domain types), `core/ingest.py`
(ijson streaming, array/JSONL sniffing, malformed-row skip-and-record),
`core/ratelimit.py` (TokenBucket + AIMD), `core/retry.py` (classification
table, full jitter, retry budget, circuit breaker), `core/sink.py` (JSONL
writer + replay), and `providers/mock.py` (the seeded chaos provider). 48
unit tests, all green, before any HTTP surface existed — the intent being
that flow-control and failure-taxonomy logic (§0's top two priorities)
should be provably correct in isolation first.

**Surprising:** the AIMD cooldown test
(`test_cooldown_prevents_multiplicative_collapse_under_concurrent_429s`)
caught nothing on the first write — the implementation had the cooldown
window from the start. Worth noting because instructions.md calls this
"the most likely bug," and it would have been easy to skip the dedicated
test on the assumption the design was obviously right.

## M2 + M3 — Scheduler, worker pool, full failure taxonomy, backpressure

Built together rather than sequentially: `core/scheduler.py` (JobRunner:
ingest -> queue -> rate limiter -> worker pool -> sink -> JobStore) and
`core/worker.py` (the per-item acquire/call/classify/retry/emit loop)
didn't stabilize independently of the integration tests that exercise them
end-to-end — several design details only surfaced once
`test_conservation.py` and `test_backpressure_429.py` existed to pin
behavior against.

**Gate self-check:** end-to-end job completes against the mock provider;
conservation test passes. **Result: pass** (101 tests total by the end of
this phase, including the property-based conservation test across
randomized chaos parameters).

**Surprising, and worth flagging explicitly:** the first version of
`test_backpressure_429.py` asserted that *every* item eventually succeeds
under a hard rate ceiling. That's wrong given this design's own retry
budget (20% of items, by design, to prevent retry-amplification) — under a
sustained enough throttle storm, some items are *supposed* to end up
`transient_exhausted` rather than retry forever. Fixed the test to assert
the actual invariant (conservation holds; nothing is silently dropped)
rather than an invented stronger one. Left as a note here because it's a
good illustration of the difference between "the test is red" meaning
"the code is wrong" versus "the test's premise is wrong" — this was the
latter.

**Also surprising:** `x-ratelimit-reset-requests` needing to be on the same
time axis (wall-clock) as `AdaptiveController.honor_reset_header`'s
`now_epoch_fn` was not obvious until a test using `MockProvider`'s default
`time.monotonic()`-based clock produced a nonsensical wait duration.
Fixed by defaulting `MockProvider`'s clock to `time.time` and documenting
why in a comment (`providers/mock.py`) — mixing monotonic and epoch clocks
across the header contract is an easy, silent bug.

## M4 — Observability, spend guard, memory probe, docs

**Gate self-check:** measured memory table produced; README complete.
**Result: pass.**

`scripts/memory_probe.py` measured peak RSS delta of 0.0–0.8 MB across
N=1,000/10,000/100,000/500,000 against the mock provider (zero API calls,
per the hard rail in §1.5/§12.2) — see `docs/memory_probe_results.json`
and `docs/scaling.md`. The 500,000-item run took ~3.6 minutes of wall time
(pure Python/asyncio scheduling overhead, not I/O) and confirmed the flat
line the O(concurrency) argument predicts.

Coverage was 84% after M1–M3's tests (just under the 85% gate) because
`extensions/webhook.py`, `extensions/spaces.py`, `providers/digitalocean.py`,
`observability/metrics.py`, `store/memory.py`, and `core/spend_ledger.py`
had no dedicated tests yet — all exercised HTTP surfaces indirectly, not
directly. Added targeted unit tests for each (webhook SSRF validation +
signing + respx-mocked delivery, the live provider via respx, the metrics
renderer, the in-memory store, the spend ledger) to reach 96%.

`ruff format .` reformatted 28 files in one pass after `ruff check --fix`
handled the auto-fixable lint issues (mostly line length in tests) — run
early and often in a real session rather than saved for the end, since
letting lint debt compound across dozens of files makes the eventual pass
noisier to review.

`mypy --strict` needed a real refactor, not just annotations: `AppState`
was originally defined in `main.py`, which `api/routes.py` needed to
import for a typed return value, but `main.py` also imports the router
from `api/routes.py` — a genuine import cycle. Split `AppState` into its
own `app_state.py` module. This is the kind of thing `mypy --strict` is
good for catching: the untyped version worked at runtime with no cycle
error (Python resolves it lazily inside functions), but strict typing
forced surfacing the layering problem explicitly.

## M5 — Live provider, extensions, polish

`providers/digitalocean.py` implemented completely (OpenAI-compatible
chat-completions call, rate-limit header extraction, malformed-response
detection) and unit-tested entirely through `respx` at the transport
level — zero real network. `extensions/webhook.py` (HMAC-SHA256 signing,
SSRF guard via DNS resolution + private/loopback/link-local rejection,
full-jitter retry) and `extensions/spaces.py` (multipart checkpointing,
behind a feature flag) implemented per §8.

**Gate self-check:** "all green; one recorded live run if a key is
present." Everything except the live run is green. The live run did not
happen — no key was provisioned. See BLOCKED above and STATUS.md.

## Post-review fixes — three defects, one design-doc pass

A review of the M1–M5 state above found three real defects and asked for
expanded design documentation. All four addressed in this pass.

### Defect 1 (critical): conservation invariant was not actually guaranteed

`worker.py::process_item` caught only `ProviderTransportError` around the
provider call. Any *other* exception (a plain bug in a provider
implementation, not a modeled failure) propagated out of `process_item`
and killed the worker task: the item was never emitted to the sink,
`counts.in_flight` leaked (the decrement happened after the try block,
never reached), `queue.task_done()` never fired, and
`scheduler.py`'s `asyncio.gather(*workers)` (no `return_exceptions=True`)
re-raised, which meant `sink.close()` never ran and the job never reached
a terminal status. **Every existing conservation test passed only because
`MockProvider` was never coded to raise anything but `ProviderTransportError`**
— the test suite had a blind spot exactly matching the mock's own
limitations, not the real failure surface.

Fixed by: catching `asyncio.CancelledError` and re-raising it explicitly;
catching broad `Exception` around the provider call and converting it into
a `ProviderTransportError` (so it flows through the existing
transient-retry path rather than needing a second parallel path); moving
the `in_flight` decrement into a `finally`; adding a second, outer
try/except/finally in `worker_loop` so even an exception that somehow
still escapes `process_item` still emits a terminal `RowError` and still
calls `task_done()`; adding `return_exceptions=True` to the `gather` call
in `scheduler.py` (with the worker results logged) as the last backstop.
Removed the now-dead `raise exc` branch in `retry.py::classify` and
replaced it with an `assert isinstance(exc, ProviderTransportError)` —
worker.py's new contract guarantees `classify()` never receives anything
else, so this documents the contract instead of silently relying on it.

Added `MockProviderConfig.p_unexpected_exception` (raises a plain
`RuntimeError`) and a corresponding case in
`tests/integration/test_conservation.py`. **Verified in both directions**:
ran the new test against the pre-fix code first — it failed exactly as
predicted, with the job hanging and `wait_for_terminal` timing out after
45s, because the worker task silently died — then reran it after the fix,
where it passes. Also added five focused unit tests
(`tests/unit/test_worker.py`) that hit `process_item`/`worker_loop`
directly with fakes rather than the full HTTP+scheduler stack, so the
exact exception-safety behavior is pinned in milliseconds, not just proven
indirectly through a slow integration case.

### Defect 2: circuit breaker could deadlock the job

`CircuitBreaker.allow_request()` sets `_probe_in_flight = True` on the
open->half-open transition. If that probe's caller never calls `record()`
— possible via the crash path above, or via the abort/cancel early-returns
in `process_item`, or any future bug that skips it — the flag never
clears, `allow_request()` returns `False` forever, and every worker spins
on `sleep(0.25); continue` with no attempt counted and no budget consumed.
The job hangs permanently.

Fixed by adding `probe_timeout_s` (default 30.0): if half-open and
`now - _opened_at > cooldown_s + probe_timeout_s`, the stale probe is
cleared and a fresh one is allowed through on the next call. Verified the
same way as Defect 1 — temporarily disabled the new check, confirmed the
new deadlock test failed, restored it, confirmed it passes. Also changed
`_outcomes` from a `list` with manual `pop(0)` to a
`collections.deque(maxlen=window)` while in there, since the manual
trimming was an O(n) shift on every single `record()` call for no reason.

### Defect 3: conservation only exercised at N=60, limiter effectively off

All seven original chaos configs used `n=60` and `rate_limit_rpm=6000` —
six of the seven never touched the client-side rate limiter as a
practical constraint. Restructured the test into a small `ConservationCase`
dataclass so each case can set its own `n`/`rate_limit_rpm`/`concurrency`,
and added: one case at `n=1,000`; one combining the *real* Tier 1 quota
(`rate_limit_rpm=120`) with simultaneous 429/500 chaos, so backpressure
and conservation are actually exercised together rather than in isolation.

### Design documentation expansion

Added to `docs/decisions.md`: a full entry on per-item dispatch through a
shared queue vs. the project statement's literal "partition into chunks"
wording, naming the deviation explicitly, arguing the straggler-imbalance
case for a self-balancing shared queue, and being honest about what
chunking would have bought (simpler chunk-level checkpointing/retry
granularity) that this design gives up.

Added two new rendered diagrams: `docs/diagrams/lifecycle.mmd` (a job's
`queued -> running -> {succeeded,partial,failed,cancelled}` state machine,
annotated with which component writes each transition and exactly what's
durable at each point) and `docs/diagrams/item_path.mmd` (a sequence
diagram tracing one prompt end-to-end from `POST /job` through both
backpressure points to `status`/`download`), both rendered to SVG via the
same `@mermaid-js/mermaid-cli` pipeline as the original flow diagram.

Added "Delivery semantics" and "Non-goals" sections to
`docs/architecture.md`. The delivery-semantics section says plainly that
this system is at-least-once *while the process is alive* and not even
consistently that *across a restart* — a crash mid-job loses whatever was
in flight, nothing currently re-drives an interrupted job automatically,
and a resubmit-after-crash re-pays for items that had already completed
against a live provider, because there is no dedup against the existing
JSONL result file today. This was written by reading what the code
actually does, not by describing what would be nice — in particular, the
"no automatic resume" fact was confirmed by grepping for callers of
`core/sink.py::replay()` and finding none in the running-app code path
(only in its own tests).

**Gate self-check (this pass):** all three defects reproduced against
pre-fix code and confirmed fixed (Defects 1 and 2 verified in both
directions as described above; Defect 3 is a test-coverage gap, not a
code defect, so "before/after" doesn't apply — the new cases simply pass).
**Result: pass.**

## Post-review, part 2 — model-selection correction (docs/model-selection.md)

Mid-session, `docs/model-selection.md` appeared in the working tree,
written by Lily directly (not by Claude Code) from a live model-catalog
check she ran herself with a real `DO_INFERENCE_KEY` against
`GET .../v1/models`. Flagged it back to her before acting on it, since its
claims (a live call happened; `providers/digitalocean.py` already handles
null content) didn't match anything in the actual diff at the time. She
confirmed it's hers and authoritative, so the codebase was brought into
line with it rather than left contradicting a committed doc:

- **`openai-gpt-oss-20b` is a reasoning model.** Queried live at
  `max_tokens=128`, it returns `content: null` with the token budget spent
  on a `reasoning_content` field instead. Cheaper per-token pricing on a
  model that returns no usable output at the budget this service actually
  pays for isn't actually cheaper — see `docs/model-selection.md`'s live
  cost/token comparison.
- **Default model changed to `mistral-3-14B`** (`config.py`,
  `.env.example`) — the cheapest model that returns real content at this
  service's default token budget. `openai-gpt-oss-20b` remains fully
  configurable, including for callers who raise `max_tokens` enough to
  leave room for real content after reasoning.
- **`providers/digitalocean.py::complete()` now treats non-string
  `content` as a malformed response** rather than passing `None` through
  as a false success — the exact "unhandled exception loses a row" shape
  Defect 1 above was about, except here it wouldn't even have raised; it
  would have silently recorded a null result as a success. Covered by a
  new respx-mocked unit test.
- **`MODEL_PRICING`'s `ministral-3-14B` entry (a research-stage guess at
  a catalog id from the very first build) corrected to `mistral-3-14B`**
  (the actual catalog id, per live verification) — pricing figures
  unchanged, only the id string.
- Propagated the corrected model name and finding through
  `README.md`, `docs/scaling.md`, and a new `docs/decisions.md` entry
  ("Default model: `mistral-3-14B`, not the cheapest-on-paper option").

A second file, `docs/design.md` — a full, polished architecture writeup
covering the same ground as `docs/architecture.md`/`docs/decisions.md`/
`docs/scaling.md` in one document — also appeared, also from Lily
directly. Unlike the first file, nothing in it contradicted the repo's
actual state (it was written after the fixes above, so it already
reflects them), so it didn't need the same flag-and-reconcile treatment.
Committed as-is; note for the morning read that it substantially
overlaps `docs/architecture.md` in scope and may be worth consolidating
later rather than maintaining both long-term.

**Gate self-check:** full suite rerun after the reconciliation — pass, 96%
coverage maintained, ruff/format/mypy --strict clean.

## Live runs, throttling experiment, doc consolidation

With a real `DO_INFERENCE_KEY` provisioned:

**One full 1,000-item live run** (`data/sample_batch.json`,
`mistral-3-14B`, real 120 RPM quota, default concurrency 4): 1000/1000
succeeded, 0 failed, 442.85s, $0.018381 actual cost vs. a ~$0.07
generic-assumption estimate and a ~$0.022 measured-token estimate — both
overshoot because this model's real chat-template overhead (~15 input
tokens/item) is far below generic assumptions, and completions averaged
~76.6 output tokens, under the 128 cap. Recorded verbatim in
`docs/sample_run.json`. **Notable, unscripted finding:** 11 real 429s
occurred with *zero* configured overshoot, because
`AdaptiveController.max_rate = rate_rps × 1.5` deliberately allows the
AIMD additive-increase phase to climb 50% past the configured quota — see
the new `docs/decisions.md` ADR entry ("AIMD headroom probing vs. a rate
ceiling matched exactly to the configured quota") and the matching
`docs/design.md` §4.4 note. Decision: keep the current behavior
(`max_rate = rate_rps` would eliminate the hunting but also the headroom
discovery it exists for); observed-and-explained beats silently tuned away.

**INCIDENT, found and fixed mid-session:** immediately after adding
per-attempt observability logging to `worker.py` (needed for the
throttling experiment below), a routine test run revealed that
`tests/conftest.py`'s `monkeypatch.delenv("DO_INFERENCE_KEY")` does not
stop `pydantic-settings` from reading a real key straight out of an
actual `.env` file — `delenv` only clears the process environment, and
the dotenv file is a lower-priority but still-consulted settings source
that a *missing* var still falls through to. Once `.env` existed (for the
live run above), this let roughly 11 test-suite job runs make real API
calls against the live endpoint instead of the mock provider, for a
combined ~$0.0112 that was never recorded in the persistent
`.spend_ledger.json` (each test's ledger path is itself sandboxed to a
throwaway `tmp_path`). Found by noticing test runs that should have taken
milliseconds were instead taking real wall-clock seconds and producing
response text that looked like genuine model output rather than the
mock's synthetic strings. Fixed the fixture to
`monkeypatch.setenv("DO_INFERENCE_KEY", "")` (an explicitly-set env var
does outrank the dotenv file), verified with `.env` still present that a
full suite run creates zero new ledger files anywhere, and reconciled the
persistent ledger to include the recovered spend so the running total
stays honest ($0.018448 → $0.029678 at that point). This is the kind of
thing N5 ("zero network, zero credentials") exists to prevent, and it
happened anyway the first time a real `.env` file entered the picture —
worth remembering that `env_file=` settings sources need the same
"explicitly set, don't delete" treatment as any other fallback-prone
config source.

**§6.8 over-rate throttling experiment**, run twice: a normal-condition
comparison (100 items, real 120 RPM) and the throttled condition proper
(200 items, client paced at 480 RPM / 4×, concurrency 32). Throttled run:
188/200 succeeded, 12 `transient_exhausted` (retry budget of 40 fully
exhausted before AIMD walked the rate down to the true limit — a real,
observed interaction between the 20% retry budget and a large sustained
overshoot, distinct from the healthier hunting-but-recovering pattern in
the 1,000-item run). 52 real 429s captured with full headers via the new
per-attempt log line. **Contradiction found against a pre-recorded manual
baseline**, flagged explicitly per §12.5 rather than silently
reconciled: observed `x-ratelimit-limit-tokens-per-minute` was 750000 (not
the baseline's 500000), `x-ratelimit-limit-tokens-per-day` was 8000000
(not 6000000), and `x-ratelimit-remaining-tokens-per-day` visibly
decremented across calls (not "pinned" as the baseline claimed). Only
`x-ratelimit-limit-requests: 120` matched. `x-ratelimit-reset-requests`
confirmed as a continuous forward-refill projection (advances
second-by-second with wall-clock time), not a fixed window, matching
§1.3 — with the added nuance that integer-second truncation can make it
read up to ~1s stale relative to the client's own clock. Full writeup:
`docs/observed_throttling.md`.

**Documentation consolidation.** `docs/architecture.md` and
`docs/design.md` had grown to overlap substantially after the previous
session's work. Per instruction, folded architecture.md's unique content
(the per-item-dispatch-vs-chunking paragraph, the job-lifecycle "no
separate aborted status" nuance, the one-item-lifecycle walkthrough) into
design.md without restructuring it, then replaced architecture.md with a
one-line pointer (kept, rather than deleted, since several other docs
link to it by path). While folding content in, caught and fixed a real
accuracy gap in design.md itself: §5.3 claimed restart deduplicates
against `replay()` and only re-runs the remainder, which is false — no
code path calls `replay()` outside its own tests, confirmed by grep.
Flagged this back before touching design.md's prose (per the instruction
not to restructure it); corrected in place once confirmed. Also swapped
the README's diagram embeds from the Mermaid-rendered `flow.svg`/
`lifecycle.svg` (CSS-class-styled, renders as solid black boxes outside
GitHub) to the hand-authored `architecture.svg`/`job-lifecycle.svg`/
`item-path.svg` (explicit presentation attributes, portable), added a
"Project structure" section (annotated directory tree) to the README,
and linked `docs/design.md` prominently at the top of the README as the
primary design-review artifact.

**`results.md` created** at the repo root: a factual evidence index
(requirements traceability for every F/N requirement in design.md, live
run and throttling results, the measured memory table, test suite
summary, and the cost model) with two gaps marked honestly rather than
glossed over: the spend guard's actual trip-and-abort path and the
pre-flight ledger-exhausted 402 refusal have no automated test (the
`is_live` branch is structurally unreachable in the zero-credential test
suite), and per-model cost is only live-measured for `mistral-3-14B` —
the other four catalog entries' costs remain generic-assumption estimates.

**Gate self-check:** full suite (113 tests) green, 96% coverage, ruff/
format/mypy --strict clean, after all of the above. `.env` reverted to
`.env.example`'s defaults (`RATE_LIMIT_RPM=120`, `LIVE_SAMPLE_SIZE=50`)
once both live experiments finished.

## SECOND INCIDENT: a real DNS lookup in the "zero-network" test suite, causing a >1hr CI hang

Checking the CI run for the previous push (per this session's final
instruction to confirm it went green) found the `test (3.12)` matrix leg
stuck `in_progress` on the `pytest with coverage` step for **over an
hour**, while `test (3.11)` had passed the identical step in 2m5s.

Root cause: `tests/unit/test_webhook.py::test_validate_allows_public_hostname`
called `validate_webhook_url("https://example.com/hook")` with nothing
mocked. `webhook.py::validate_webhook_url` calls `socket.getaddrinfo()`
directly (not through `httpx`, so `respx` mocking elsewhere in the suite
can't intercept it) to resolve the hostname and check it's not
private/loopback/link-local — for `example.com` that's a **real DNS
query** over the actual network. Two other tests
(`test_deliver_webhook_succeeds_on_2xx`,
`test_deliver_webhook_retries_then_gives_up_on_persistent_failure`) hit
the same unmocked call on their way into `deliver_webhook()`, before ever
reaching the `respx`-mocked HTTP layer. `socket.getaddrinfo` has no
built-in timeout; on a runner with any DNS hiccup at that moment, it can
hang far longer than any test reasonably should, and apparently did.

This is the second real N5 ("zero network") violation found this
session, after the `DO_INFERENCE_KEY`/dotenv-fallback incident above —
both were genuine gaps in "the suite runs with zero network," not just
theoretical risks, and both were only caught by symptoms (contaminated
test timing/output; a stuck CI job) rather than by a static check that
would have caught either up front. **Worth a lasting takeaway**: neither
`monkeypatch.delenv()` nor "this test doesn't call `httpx` directly" is
sufficient to guarantee no network I/O — a lower-priority settings
source and a raw `socket` call are both invisible to the obvious checks.

Fixed: added a `_fake_public_getaddrinfo` monkeypatch (returns a fixed
public IP, `93.184.216.34`, without any real lookup) and applied it to
all three affected tests. Verified: `tests/unit/test_webhook.py` passes
in under 4 seconds; full suite rerun, 113 passed, 96% coverage
maintained. Cancelled the stuck CI run (`gh run cancel`) rather than wait
out its timeout, since the fix needed to ship in the next push anyway.

## Final state

113 tests passing, 96% coverage (gate is 85%), ruff/ruff-format/mypy
--strict all clean, six rendered diagrams (flow, lifecycle, item-path as
Mermaid/GitHub-only; architecture, job-lifecycle, item-path as
hand-authored/portable), one full 1,000-item live run and one deliberate
throttling experiment both recorded with real data (including one
genuine, unscripted contradiction of a prior manual observation), a
factual evidence index (`results.md`), and all four morning-review
documents kept current. Two real "zero network/credentials" hygiene
incidents found and fixed in this session (dotenv key fallback; unmocked
DNS lookup) — both documented above rather than fixed silently. Total
real live spend to date: $0.034948 against a $1.00 cap. Committed in
logical increments with real messages, per §12.4.
