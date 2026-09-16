#!/usr/bin/env python
"""Generates data/sample_batch.json: a 1,000-row prompt batch for the
Quickstart and for the one full live run recorded in docs/sample_run.json.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

_TOPICS = [
    "the history of the printing press",
    "how tide pools form",
    "why sourdough starters need feeding",
    "the physics of soap bubbles",
    "the migration patterns of monarch butterflies",
    "how suspension bridges distribute load",
    "the origin of the word 'quarantine'",
    "why leaves change color in autumn",
    "how noise-cancelling headphones work",
    "the etiquette of tea ceremonies",
]


def build_rows(n: int, seed: int) -> list[dict[str, str]]:
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        topic = rng.choice(_TOPICS)
        rows.append({"id": f"item-{i:05d}", "prompt": f"In two sentences, explain {topic}."})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--out", type=Path, default=Path("data/sample_batch.json"))
    args = parser.parse_args()

    rows = build_rows(args.n, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(rows, indent=2))
    print(f"wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
