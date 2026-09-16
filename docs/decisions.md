# Decisions (ADR log)

Written for a reader who will defend these out loud in a 45-minute review,
not for a reader who already agrees. Each entry: what was chosen, what was
rejected, and why.

## Token bucket vs. semaphore for rate limiting

**Chosen:** a `TokenBucket` (continuous refill, monotonic clock) wrapped in
an AIMD `AdaptiveController`, separate from the `asyncio.Queue` that bounds
concurrency.

**Rejected:** an `asyncio.Semaphore(concurrency)` alone.

**Why:** a semaphore bounds how many requests can be *in flight at once*; it
says nothing about how many can complete *per second*. Two workers each
capable of finishing a request in 100ms produce 20 req/s, which blows
through a 120 RPM (2 req/s) account quota in well under a second regardless
of how small the semaphore is. Concurrency and rate are different physical
quantities (Little's Law: `L = λ × W`) and need two different mechanisms.
The queue bounds `L` for memory purposes; the bucket bounds `λ` for quota
purposes.

## Full jitter vs. exponential backoff

**Chosen:** full jitter — `sleep = uniform(0, min(cap, base × 2^attempt))`.

**Rejected:** plain exponential backoff (`sleep = base × 2^attempt`).

**Why:** plain exponential backoff keeps a retry cohort's wake times
correlated. If 20 workers all get throttled by the same momentary overage,
they all retry after exactly the same delay on wave 1, collide again, back
off to the same delay on wave 2, and collide again — the herd never
decorrelates. Sampling uniformly under the exponential envelope spreads the
cohort out starting on the very first retry, and keeps it spread on every
subsequent wave. This is a near-certain review question and is called out
directly in `core/retry.py::full_jitter_delay`.

## AIMD cooldown window

**Chosen:** on a 429, decrease the rate by 25% (`rate *= 0.75`), then ignore
further 429s for a 5-second cooldown before allowing another decrease.

**Rejected:** decreasing on every 429 unconditionally.

**Why:** without a cooldown, N *concurrent* 429s from a single momentary
account-side overage divide the rate by `0.75^N` in one instant — 20
concurrent throttles would collapse a rate of 10 down to roughly 0.03,
which is a self-inflicted outage, not a controlled backoff. The cooldown
treats a burst of simultaneous throttle signals as one signal. This is
covered directly by
`tests/unit/test_ratelimit.py::test_cooldown_prevents_multiplicative_collapse_under_concurrent_429s`
and is, per instructions.md, "the most likely bug" in this class of system.

## `x-ratelimit-reset-requests` as a floor, not a fixed window

**Chosen:** treat the header as a forward-refill projection — hard-pause
until that instant, then resume with `uniform(0, 250ms)` per-worker jitter.

**Rejected:** waking every worker at exactly the reset timestamp.

**Why:** the header is shared across every worker hitting the same account.
Waking all of them at the identical instant just recreates the exact
throttle they were pausing for, one instant later. This mirrors the full
jitter reasoning above and is called out in `core/ratelimit.py::AdaptiveController.honor_reset_header`.

## Per-item dispatch through a shared queue vs. static chunk partitioning

**Chosen:** every prompt is dispatched individually through one shared
`asyncio.Queue`, consumed by a fixed pool of worker tasks (§3, Zone 3). No
item is ever assigned to a specific worker in advance.

**Rejected:** the literal reading of the project statement, which says to
"partition the 1,000 prompt records into concurrent execution chunks" --
i.e. split the input into N contiguous chunks up front and hand one chunk
to each of N workers to process sequentially. This is a deliberate,
acknowledged deviation from the spec's literal wording, not an oversight.

**Why:** static chunking suffers from straggler imbalance. If prompt length,
`max_tokens`, or upstream latency varies across the batch (they always do
in practice), a chunk that happens to contain the slow prompts holds the
job open long after every other worker has drained its chunk and gone
idle -- the job's wall-clock time is bounded by its *worst* chunk, not by
the average. A shared queue is self-balancing: a worker that finishes fast
immediately pulls the next available item regardless of which "chunk" it
would have belonged to, so the whole pool drains at roughly the same rate
work actually completes -- this is exactly the work-stealing pattern that
avoids the straggler problem, achieved here for free by not partitioning
in the first place rather than by adding an explicit steal step.

**Honest tradeoff, not just a win:** chunking has a real advantage this
design gives up. A chunk is a natural checkpoint unit -- "chunk 3 of 10 is
done" is a simple, coarse-grained resumption point, and a chunk that fails
outright can be retried as a whole unit with clear boundaries. The shared
queue instead pushes checkpointing and retry down to per-item granularity
(the JSONL sink's replay-on-restart, §6.4, and per-item retry in
`core/worker.py`), which is finer-grained and arguably more precise, but
it means there's no single artifact that answers "how far did chunk 3
get" -- only "which item ids have terminal outcomes so far." For a system
whose top-priority requirement (§0) is flow control and per-item failure
semantics, per-item granularity is the right trade; a system whose
priority was coarse-grained resumability over a flaky, chunk-shaped
upstream might reasonably choose the opposite.

See `docs/architecture.md`'s Zone 3 section for where this plays out in
the running system, and `tests/integration/test_conservation.py` for the
per-item accounting this depends on.

## Queue-as-backpressure vs. an unbounded work list

**Chosen:** `asyncio.Queue(maxsize=concurrency × 4)` between ingestion and
the worker pool; the producer (ingestion) blocks on `queue.put()` once full.

**Rejected:** reading the whole input into a list before dispatching any
requests.

**Why:** this is the load-bearing decision behind the O(concurrency) memory
claim (§6.6). A bounded queue means ingestion can never run more than a
small, fixed multiple of `concurrency` ahead of processing, so memory stays
flat whether N is 1,000 or 500,000. The `×4` multiplier gives workers a
little slack to pull from without starving, without meaningfully changing
the memory bound.

## JSONL sink vs. in-memory results list

**Chosen:** append-only JSONL to disk, single writer task, batched
`fsync`, counters kept in memory.

**Rejected:** accumulating `RowResult`/`RowError` objects in a Python list
and serializing at the end.

**Why:** the second path is the other half of the O(N) memory story a naive
implementation falls into — roughly 750MB of accumulated result objects at
N=500,000 (§6.6). Writing through as results arrive means the only thing
this service ever holds in memory about *completed* work is a handful of
integer counters, regardless of N. It also means `GET /job/{id}/download`
can stream the file back without ever holding the full result set in
memory a second time, and a crash mid-job doesn't lose already-completed
rows.

## Spend guard: two caps, not one

**Chosen:** a per-job cap (`MAX_JOB_SPEND_USD`, checked every 50 items
against both actual and linearly-projected cost) *and* a cross-run,
committed ledger cap (`MAX_TOTAL_SPEND_USD`) that is never reset
automatically and is checked before a live job is even allowed to start.

**Rejected:** a single per-job cap with no cross-run memory.

**Why:** a per-job cap alone protects against one runaway job but not
against many small "just testing" runs draining a prepaid balance over a
session. Serverless inference is prepaid (§1.4) — a `402` at $0 balance is
a hard, whole-job failure, not a retryable one — so the ledger is a durable
outer bound and the per-job check is a fast-acting inner one. See
`core/spend_ledger.py` and `core/scheduler.py::on_spend_check`.

## Live-run item cap: an added field, not a spec rename

**Chosen:** `JobConfig.max_items` (server-side only, not part of the public
request schema) caps a *live*-provider job at `min(LIVE_SAMPLE_SIZE,
MAX_LIVE_ITEMS)` unless nothing overrides it; the mock provider has no such
cap. This is logged as a judgment call in `OPEN_QUESTIONS.md` since the §5
request schema, taken literally, has no field for a caller to request a
smaller or larger live run.

**Rejected:** trusting the client-declared `concurrency`/item count, or
silently allowing a live job to run against the full input file.

**Why:** §12.2's hard safety rail ("never run a live inference job above
N=1,000 items") has to be enforced somewhere that doesn't depend on the
caller's honesty, and the request-path validation is stat-only (existence/
readability/size — never parses the file to count rows). Capping ingestion
itself, inside the scheduler, is the one place that's true regardless of
what the caller asked for.

## Default model: `mistral-3-14B`, not the cheapest-on-paper option

**Chosen:** `mistral-3-14B` as `BATCHENGINE_MODEL`'s default.

**Rejected:** `openai-gpt-oss-20b`, despite it pricing out cheaper per
token on the serverless catalog.

**Why:** verified live against the model-access-key endpoint,
`openai-gpt-oss-20b` is a reasoning model — at this service's default
`max_tokens=128`, the entire token budget goes to a `reasoning_content`
field and the actual `content` field comes back `null`. Cheaper-per-token
pricing on a model that returns no usable output at the budget you're
actually paying for isn't actually cheaper. Full research trail,
including the live-verified pricing/token-count table and the correction
of `ministral-3-14B` (a research-stage guess) to the catalog's actual
`mistral-3-14B`, is in `docs/model-selection.md`. `providers/digitalocean.py`
now also treats non-string `content` as a malformed response rather than
passing `None` through as a false success, since that's the concrete bug
this finding would otherwise have caused.
