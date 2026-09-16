#!/usr/bin/env python
"""Computes the serverless-model cost table for the README instead of
hand-typing it (instructions.md §1.1: "Compute these in code, don't
hardcode."). Reads pricing from providers/digitalocean.py::MODEL_PRICING so
there is exactly one source of truth for $/1M tokens.
"""

from __future__ import annotations

from batchengine.providers.digitalocean import MODEL_PRICING

_AVG_INPUT_TOKENS = 150
_AVG_OUTPUT_TOKENS = 200


def cost_for_n(
    n_prompts: int,
    cost_in: float,
    cost_out: float,
    avg_in: int = _AVG_INPUT_TOKENS,
    avg_out: int = _AVG_OUTPUT_TOKENS,
) -> float:
    return n_prompts * (avg_in / 1_000_000 * cost_in + avg_out / 1_000_000 * cost_out)


def main() -> None:
    print(f"assuming ~{_AVG_INPUT_TOKENS} input / ~{_AVG_OUTPUT_TOKENS} output tokens per prompt\n")
    print(
        "| Model ID | $/1M input | $/1M output | Est. cost, 1,000 prompts | "
        "Est. cost, 500,000 prompts |"
    )
    print("|---|---|---|---|---|")
    for model, (cost_in, cost_out) in sorted(
        MODEL_PRICING.items(), key=lambda kv: cost_for_n(1000, *kv[1])
    ):
        cost_1k = cost_for_n(1_000, cost_in, cost_out)
        cost_500k = cost_for_n(500_000, cost_in, cost_out)
        print(f"| `{model}` | ${cost_in} | ${cost_out} | ~${cost_1k:.2f} | ~${cost_500k:,.2f} |")


if __name__ == "__main__":
    main()
