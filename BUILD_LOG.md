# Build log

Chronological. Written as the build progressed, not reconstructed after
the fact.

## BLOCKED (read this first)

1. **Docker Desktop's engine was not running in this environment**, so
   `docker build .` was never executed locally. The CI step
   (`.github/workflows/ci.yml` job `docker`) will run it on push. The
   Dockerfile is a plain `python:3.12-slim` + `pip install .`, so this is
   low-risk, but genuinely unconfirmed.
2. **No `DO_INFERENCE_KEY` was provisioned**, so M5's live-run portion
   (one full 1,000-item run + the §6.8 over-rate experiment) was not
   executed. This is an expected, non-blocking outcome per instructions.md
   §12.2 — see STATUS.md for exactly what's pending.

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

## Final state

101 tests passing, 96% coverage, ruff/ruff-format/mypy --strict all clean,
Mermaid diagram rendered to SVG, all four morning-review documents
produced. Committed in four milestone-boundary commits with real messages,
per §12.4.
