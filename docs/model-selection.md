# Model selection

**Decision: the default inference model is `mistral-3-14B`.**

The project specification names `meta-llama-3-8b-instruct`. That model is not
available on DigitalOcean Serverless Inference. This document records how that
was established, what was chosen instead, and why.

## The specification is stale

The project statement says workers should "prioritize cost-efficient
configurations (e.g., `meta-llama-3-8b-instruct`) for high-throughput testing."

Three independent checks against the live API say that model cannot be used:

1. **DigitalOcean's model catalog** lists `llama3-8b-instruct` as available on
   *dedicated* inference only. The serverless column is empty.
2. **`GET https://inference.do-ai.run/v1/models`**, queried with a live model
   access key, returns 73 model IDs. Neither `llama3-8b-instruct` nor
   `meta-llama-3-8b-instruct` appears among them.
3. The only Meta model in the serverless catalog is `llama-4-maverick`.

This was verified against the running service, not inferred from documentation.

The reasonable conclusion is that the project statement predates a model
catalog change. The instruction that carries forward is the *requirement* —
"prioritize cost-efficient configurations" — not the specific model name, which
is an example that has since expired.

## Candidates actually available

Measured against the live endpoint with an identical prompt
("In two sentences, explain the origin of the word quarantine."), `max_tokens=128`:

| Model | $/1M in | $/1M out | Prompt tok | Completion tok | Cost / 1,000 items | Usable output |
|---|---|---|---|---|---|---|
| **`mistral-3-14B`** | $0.20 | $0.20 | 15 | 94 | **$0.022** | Yes |
| `openai-gpt-oss-20b` | $0.05 | $0.45 | 79 | 128 (capped) | $0.062 | **No — see below** |
| `llama-4-maverick` | $0.20 | $0.696 | ~15 | ~94 | ~$0.068 | Yes |

### Why not `openai-gpt-oss-20b`, despite the cheapest input pricing

It is a **reasoning model**. Verified live: it returns

```json
"content": null,
"reasoning_content": "The user wants: ...",
"finish_reason": "length"
```

At `max_tokens=128` the entire budget is consumed by reasoning and `content` is
`null`. Getting usable output would require a substantially higher token cap,
which eliminates the pricing advantage — output tokens cost 2.25× more than
`mistral-3-14B` and would be spent largely on reasoning traces this workload
discards.

This finding has a second consequence, recorded here because it caused a code
change: **any provider client must not assume `content` is a string.** Reading
`choices[0].message.content` directly returns `None` on reasoning models, and an
unhandled exception in the provider call path is exactly the condition that
loses a row and breaks the conservation invariant. See
`providers/digitalocean.py` and its null-content unit test.

### Why not `llama-4-maverick`, despite being the closest to the spec's intent

It is the only Meta model on serverless, so it is the nearest substitute if the
specification's intent was "use the Meta model." It costs roughly **3× more per
1,000 items**, driven entirely by output pricing ($0.696 vs $0.20 per 1M).

Given that the specification's stated *requirement* is cost efficiency and the
model name was an illustrative example, the cheaper model is the better reading.
`llama-4-maverick` is fully supported — set `BATCHENGINE_MODEL=llama-4-maverick`
— and is the right choice if Meta-family output is a hard constraint.

## A note on token accounting

The same prompt string produced **79 prompt tokens on `openai-gpt-oss-20b` and
15 on `mistral-3-14B`** — a 5× difference arising from chat-template overhead,
not from the prompt itself.

Any cost model built on estimated token counts will therefore be wrong by a
large factor when the model changes. This engine counts **actual** tokens from
each response's `usage` block rather than estimating from prompt length, which
is why the cost figures in `GET /job/{id}/status` remain accurate across model
substitutions.

## Summary

| | |
|---|---|
| Specified | `meta-llama-3-8b-instruct` — not available on serverless inference |
| Chosen | `mistral-3-14B` — $0.022 per 1,000 items, non-reasoning, real content |
| Alternative | `llama-4-maverick` — nearest Meta model, ~3× the cost |
| Rejected | `openai-gpt-oss-20b` — reasoning model, returns `content: null` |
| Configurable via | `BATCHENGINE_MODEL`, or the `model` field on `POST /job` |
