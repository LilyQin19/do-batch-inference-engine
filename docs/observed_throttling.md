# Observed throttling (§6.8) — PENDING

Not executed in this build: it requires a live, funded `DO_INFERENCE_KEY`,
and none was provisioned (see STATUS.md and `OPEN_QUESTIONS.md`). Per §12.2
of instructions.md, no live inference call was made without one.

## What this experiment is, so it can be run later

Once a key is available, run ~200 items with the client-side rate limiter
configured to roughly 4× the account's real RPM quota
(`BATCHENGINE_RATE_LIMIT_RPM` set to ~480 against a 120 RPM account) to
deliberately induce throttling, then record here:

- [ ] The actual HTTP status and response body DigitalOcean returns on
      throttle (confirm it's a plain `429` with a JSON error body, as
      assumed by `core/retry.py`'s classification table).
- [ ] The literal `x-ratelimit-limit-requests`, `x-ratelimit-remaining-requests`,
      and `x-ratelimit-reset-requests` values observed on both a throttled
      and a non-throttled response.
- [ ] Whether `x-ratelimit-reset-requests` behaves as the continuous
      forward-refill projection assumed in `docs/decisions.md` and
      `core/ratelimit.py::AdaptiveController.honor_reset_header`, or as a
      fixed window boundary. **If it's a fixed window, `honor_reset_header`'s
      "hard pause until that instant" logic is still correct; only the
      framing in the docs/comments would need correcting.**
- [ ] p50/p95 latency under normal vs. throttled conditions.
- [ ] Whether the AIMD controller converges (rate stabilizes near the
      effective ceiling) and how many requests/seconds that took.
- [ ] Any place observed behavior contradicts §1.3 of instructions.md —
      to be logged here as a `SPEC-CORRECTION`, per §12.5, since a
      documented contradiction between assumption and reality is a
      stronger artifact than silent agreement.

Command, once a key exists (does not exceed N=1,000 — see §12.2):

```bash
BATCHENGINE_RATE_LIMIT_RPM=480 uvicorn batchengine.main:app --port 8000 &
python scripts/generate_batch.py --n 200 --out data/throttle_probe.json
curl -X POST localhost:8000/job -H "Content-Type: application/json" \
  -d '{"input_path": "data/throttle_probe.json", "concurrency": 32}'
```
