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
