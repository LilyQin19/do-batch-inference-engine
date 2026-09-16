"""Committed cross-run spend ledger (§6.7). This is the outer spend cap: it
accumulates cost across every live run this repo has ever made, persists in
a file that is never reset automatically, and is checked *before* a live job
is allowed to start. `on_spend_check` in scheduler.py is the inner, per-job
cap; this is the one that survives a restart.

Only ever touched for live-provider jobs -- the mock provider has no network
path, so it can never add to this file, and the test suite never imports it
in a way that would create one.
"""

from __future__ import annotations

import json
from pathlib import Path


def load_total_spend(path: str | Path) -> float:
    p = Path(path)
    if not p.exists():
        return 0.0
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return float(data.get("total_spend_usd", 0.0))
    except (json.JSONDecodeError, ValueError, TypeError):
        return 0.0


def record_spend(path: str | Path, delta_usd: float) -> float:
    p = Path(path)
    total = load_total_spend(p) + max(0.0, delta_usd)
    p.write_text(json.dumps({"total_spend_usd": total}), encoding="utf-8")
    return total
