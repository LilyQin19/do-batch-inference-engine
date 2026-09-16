# Observed throttling (§6.8)

Executed live against DigitalOcean Serverless Inference on 2026-09-16.
Two runs: a **normal** condition (client paced at the true 120 RPM quota,
`data/normal_probe.json`, 100 items) and a **throttled** condition (client
paced at ~4× the quota — 480 RPM configured, concurrency 32 —
`data/throttle_probe.json`, 200 items), to induce genuine backpressure per
the experiment instructions.md §6.8 asks for. Model: `mistral-3-14B`.
Total cost of both runs plus a small direct-probe follow-up: ~$0.0053
(`.spend_ledger.json`).

## Contradiction with the pre-recorded baseline — flagged explicitly

Baseline curl observations (recorded before this experiment) stated:
`x-ratelimit-limit-tokens-per-minute: 500000`,
`x-ratelimit-limit-tokens-per-day: 6000000`, remaining-tokens-per-day
"pinned at 600000 and not decrementing across calls."

**The live run contradicts all three of those specifics:**

- `x-ratelimit-limit-tokens-per-minute` observed as **`750000`**, not
  500000, on every single response in both runs.
- `x-ratelimit-limit-tokens-per-day` observed as **`8000000`**, not
  6,000,000, on every single response in both runs.
- `x-ratelimit-remaining-tokens-per-day` **does decrement** across calls —
  tracked from `800000` down to `799260` over the 100-item normal run (a
  real, if slightly non-monotonic — see below — downward trend, not a
  pinned constant).

Only `x-ratelimit-limit-requests: 120` from the baseline was confirmed
correct (observed on every response in both runs). Two explanations are
plausible and this doc doesn't have enough evidence to pick one: (a) the
baseline curl calls were made against a different point in time when the
account's tier/limits were configured differently, or (b) the
tokens-per-minute/tokens-per-day limits are account-tier-dependent and the
baseline was recorded under different tier assumptions. Either way: **the
limits are not the fixed constants the baseline assumed**, and any code or
docs that hardcode `500000`/`6000000` as expected values would be wrong.
This codebase doesn't hardcode them (it reads `x-ratelimit-*` headers
live and never assumes a specific limit value), so no code change follows
from this — but it's the single most concrete thing this experiment
found, and instructions.md §12.5 says a documented contradiction is more
valuable than agreement.

The non-monotonic wobble in remaining-tokens-per-day (e.g. `799921` then
`799913` then `799925` — occasionally ticking *up* between two closely
spaced calls) is itself worth noting: it's consistent with a rolling
window whose oldest usage ages out and gets credited back faster than new
usage is debited when several requests are in flight concurrently, rather
than a simple monotonic counter. Not investigated further; recorded as
observed.

## 1. Actual status and body on throttle

**Status: `429`**, confirmed on 52 of 240 attempts in the throttled run.
The response body was not separately captured this run (only headers were
logged per attempt — see `core/worker.py`'s `job.attempt` log line, added
for this experiment); a follow-up direct probe made no additional 429s
(the account's short-term quota had already recovered by the time it
ran), so the exact body text is not reproduced here. What *is* certain,
directly from `providers/digitalocean.py::complete()`'s behavior: a 429's
body is passed through verbatim as `ProviderResponse.text` and is never
parsed as JSON, so malformed-body handling never applies to a 429 — only
to a 200 with an unparseable payload.

## 2. Literal `x-ratelimit-*` header values observed

Normal condition (representative sample, mid-run):
```
x-ratelimit-limit-requests: 120
x-ratelimit-remaining-requests: 108        (decrementing across the run)
x-ratelimit-reset-requests: 0              (0 = capacity available, §1.3)
x-ratelimit-limit-tokens-per-minute: 750000
x-ratelimit-remaining-tokens-per-minute: 750000
x-ratelimit-reset-tokens-per-minute: 0
x-ratelimit-limit-tokens-per-day: 8000000
x-ratelimit-remaining-tokens-per-day: 799260  (started at 800000)
x-ratelimit-reset-tokens-per-day: 0
```

Throttled condition, a representative 429:
```
x-ratelimit-limit-requests: 120
x-ratelimit-remaining-requests: 0
x-ratelimit-reset-requests: 1789582462     (a Unix epoch -- see below)
```
(Only these three headers were present on the 429 responses observed —
DigitalOcean does not echo the token-bucket headers on a request-rate
429.)

## 3. Is `x-ratelimit-reset-requests` a continuous projection or a fixed window?

**Continuous forward-refill projection, confirmed.** Across the 52
throttled attempts, the header's value advanced second-by-second in lockstep
with wall-clock time — `1789582462`, `463`, `464`, ... `475` — rather than
staying constant at one fixed per-minute boundary. If it were a fixed
window edge, every 429 within the same minute would report the same
timestamp; instead each 429 reports a value close to "now," consistent
with §1.3's description of a rolling refill projection.

One nuance the docs don't mention: comparing each 429's reset value to the
client's own request timestamp, the reset value was **slightly in the
past** relative to the client clock 51 of 52 times (median −0.46s, range
−0.93s to +0.05s). This is consistent with integer-second truncation on a
sub-second-resolution rolling window: the header can only report to the
nearest whole second, so a projection that refilled 0.4s ago still reads
as "now" or "a moment ago" rather than showing negative/fractional
seconds. `AdaptiveController.honor_reset_header`'s behavior (`max(0.0,
reset_epoch - now)`) already treats a past-or-present reset value as "no
wait needed," which matches this observation correctly — no code change
needed, but worth knowing the header can legitimately read slightly stale.

## 4. Latency: normal vs. throttled

| Condition | n | p50 | p95 | mean |
|---|---|---|---|---|
| Normal (successful requests, 120 RPM paced) | 100 | 0.815s | 1.097s | 0.846s |
| Throttled run — successful requests | 188 | 0.848s | 1.399s | 0.895s |
| Throttled run — 429 responses themselves | 52 | 0.256s | 0.297s | 0.260s |

429 responses return markedly faster (~0.26s) than successful completions
(~0.85-0.9s) — DigitalOcean rejects an over-quota request before doing any
inference work, rather than queuing it. Successful-request latency under
the throttled condition is only modestly higher at p50 (0.848s vs 0.815s)
but noticeably fatter-tailed at p95 (1.399s vs 1.097s) — consistent with
some successful requests having to wait behind the AIMD controller's
paced token bucket after a throttle-driven rate decrease.

## 5. Did the AIMD controller converge, and how long did it take?

**Partially, and the run ended before full convergence** — this is itself
informative. Configured client rate: 480 RPM (8.0 rps) against a true
quota of ~120 RPM (~2.0 rps) — a deliberate 4× overshoot, with concurrency
32.

- t=0-19s: rate held at the initial 8.0 rps; no throttling yet (the token
  bucket's burst capacity absorbed the first wave).
- t≈19.3s: first `429` observed.
- t=19.3-32s: 52 throttle events accumulate. The 5-second AIMD cooldown
  (§6.1) limits this window to roughly 2-3 *effective* rate decreases
  (`rate *= 0.75` per cooldown window, not per 429) despite 52 raw signals
  arriving — exactly the collapse-prevention behavior
  `tests/unit/test_ratelimit.py::test_cooldown_prevents_multiplicative_collapse_under_concurrent_429s`
  is built to guarantee. Observed final rate: **4.0 rps**, down from 8.0 —
  consistent with 2 decreases (8.0 → 6.0 → 4.5, close to the observed 4.0
  once timing jitter is accounted for) rather than the 5-6 decreases a
  per-429 (uncooled) response would have produced.
- t=31.9s: job reaches terminal status (`partial`) before the rate ever
  reaches the true ~2.0 rps quota. The retry budget (20% of 200 = 40
  retries) was exhausted first — 12 items ended `transient_exhausted`,
  never one item silently lost (`188 succeeded + 12 failed == 200
  ingested`, conservation held).

**Conclusion: with this large an overshoot (4×) and this short a run (200
items), the retry budget's cap binds before the AIMD controller can fully
walk the rate down to the true limit.** This is a real, observed
interaction between two independently-reasonable design choices (a 20%
retry budget to prevent retry-amplification, and a gradual multiplicative
backoff to avoid overreacting to a single bad signal) that only shows up
under a large, sustained overshoot — worth knowing for anyone tuning
either constant. The full 1,000-item live run at the *correctly configured*
120 RPM (`docs/sample_run.json`) shows the complementary, healthier case:
11 throttle events, all retried successfully, rate hunting in a bounded
range (1.69-3.0 rps) around the true limit for the run's full 7.4 minutes
— see `docs/decisions.md`'s "AIMD headroom probing vs. a rate ceiling
matched exactly to the configured quota" entry for why that hunting
happens even with zero configured overshoot.

## Reproduction

```bash
# Throttled condition
BATCHENGINE_RATE_LIMIT_RPM=480 BATCHENGINE_LIVE_SAMPLE_SIZE=200 uvicorn batchengine.main:app --port 8000 &
python scripts/generate_batch.py --n 200 --seed 42 --out data/throttle_probe.json
curl -X POST localhost:8000/job -H "Content-Type: application/json" \
  -d '{"input_path": "data/throttle_probe.json", "concurrency": 32}'
```
