# Open questions

Judgment calls made without a human checkpoint, per instructions.md §12.1.
Ordered by how expensive each would be to reverse, most expensive first.

## 1. Server-side item cap for live-provider jobs (`JobConfig.max_items`)

**What was ambiguous:** §12.2 says "never run a live inference job above
N=1,000 items," but `POST /job`'s request-path validation is stat-only
(existence/readability/size -- never parses the file, by design, to stay
under 50ms). There is no way to know N from the file alone without parsing
it, and the §5 request schema has no field for a caller to declare or
request a specific run size.

**What was chosen:** when a live provider is selected (i.e.
`DO_INFERENCE_KEY` is set), the scheduler caps ingestion server-side at
`min(LIVE_SAMPLE_SIZE, MAX_LIVE_ITEMS)` (defaults 50 and 1000) regardless
of the actual input file size, via `JobConfig.max_items` -- a field added
to the internal config, not exposed on the public request schema. The mock
provider has no such cap.

**Alternative rejected:** trusting a client-declared count, or adding a
"confirm you want a full run" field to the public request schema. Rejected
because the safety rail needs to hold even against a caller who doesn't
know or doesn't respect it -- it isn't a spec field to honor, it's a rail
that has to be true regardless of the request.

**Cost to reverse:** low. It's one field and one comparison in
`core/scheduler.py::_ingest_and_dispatch`; removing the cap or adding a
proper opt-in field to the schema is a small, localized change. Flagged
first anyway because it's the safety-rail-adjacent decision most likely to
come up in review.

## 2. Progress percentage during streaming ingestion

**What was ambiguous:** `progress_pct` in `GET /job/{id}/status` is
naturally `(succeeded+failed)/ingested`, but `ingested` keeps growing while
a job is still reading the input file -- so "50%" while running means "50%
of what's been read so far," not "50% of the eventual total," which isn't
knowable until ingestion finishes (§6.3's whole point is never
pre-scanning the file to find N).

**What was chosen:** report `(succeeded+failed)/ingested` as-is, and let it
read as an honest (if slightly optimistic-looking) number that becomes
exact once the job stops running. Documented in a comment at
`api/routes.py::get_status`.

**Alternative rejected:** stat-counting newlines/items in the file
up-front to estimate N before dispatch. Rejected because it reintroduces
exactly the kind of full-file-touch on (or near) the request path that
streaming ingestion exists to avoid, for a cosmetic percentage.

**Cost to reverse:** low -- purely a status-endpoint computation, not a
persisted or load-bearing decision.

## 3. Retry budget denominator during streaming ingestion

**What was ambiguous:** `RetryBudget(total_items, fraction=0.20)` needs a
total item count, but that total isn't known until ingestion finishes.

**What was chosen:** `RetryBudget.total_items` is grown live as each item
is ingested (`core/scheduler.py::_ingest_and_dispatch`), so the retry
budget's limit tracks the *running* ingested count rather than a fixed
final total.

**Alternative rejected:** deferring all retry-budget enforcement until
ingestion completes. Rejected because it would let early items retry
without limit while the file is still streaming in, which is exactly the
retry-amplification scenario the budget exists to prevent.

**Cost to reverse:** low -- confined to one field's update site; covered
directly by `tests/unit/test_retry.py::test_retry_budget_grows_as_total_items_grows`.

## 4. Circuit breaker cooldown made configurable for tests

**What was ambiguous:** §6.2 specifies a 30-second open/cooldown for the
circuit breaker, which is correct for production but makes any
HTTP-driven test that trips the breaker (high `p_500` chaos configs) wait
out 30 real seconds per trip.

**What was chosen:** `BATCHENGINE_CIRCUIT_COOLDOWN_S` (default 30.0,
matching §6.2) was added as a setting, and the test fixture
(`tests/conftest.py::app_client_factory`) overrides it to 0.2s so
chaos-heavy integration/property tests run in seconds, not minutes. The
breaker's *logic* (open -> half-open -> closed transitions) is verified
independently with a fully-controlled fake clock in
`tests/unit/test_retry.py`, so shortening the cooldown for HTTP-level tests
doesn't weaken what's actually being proven.

**Alternative rejected:** leaving the cooldown hardcoded at 30s and letting
affected tests simply run slowly. Rejected purely on practicality --
several such tests would have made the full suite take many extra minutes
for no additional coverage.

**Cost to reverse:** trivial -- delete the env var and the setting
reverts to a hardcoded 30.0.

## 5. Provider selection is automatic, not a request field

**What was ambiguous:** nothing in the §5 request schema lets a caller
choose mock vs. live explicitly.

**What was chosen:** `POST /job` selects the live DigitalOcean provider iff
`DO_INFERENCE_KEY` is set in the environment, and the mock provider
otherwise. This is also *why* the test suite needs zero credentials by
construction: CI never sets that variable, so it can never accidentally
select the live path.

**Alternative rejected:** an explicit `"provider": "mock"|"live"` request
field. Rejected as unnecessary surface area for a distinction that should
be an operator/environment concern, not a per-request one -- and adding it
would have been a schema addition beyond what was asked.

**Cost to reverse:** low -- confined to `api/routes.py::submit_job`.

## SPEC-CORRECTIONS (§12.5)

None identified that required deviating from instructions.md's design. The
one place reality could plausibly contradict the spec --
`x-ratelimit-reset-requests`'s exact semantics (continuous projection vs.
fixed window) -- was never tested against a live endpoint in this build (no
key was provisioned), so it's recorded as *pending verification*, not as a
correction, in `docs/observed_throttling.md`.
