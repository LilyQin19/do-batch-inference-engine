from __future__ import annotations

import json
from pathlib import Path

import pytest


def write_batch(path: Path, n: int, as_array: bool = True) -> None:
    rows = [{"id": f"item-{i}", "prompt": f"prompt number {i}"} for i in range(n)]
    if as_array:
        path.write_text(json.dumps(rows))
    else:
        path.write_text("\n".join(json.dumps(r) for r in rows))


@pytest.fixture
def sample_batch(tmp_path: Path) -> Path:
    p = tmp_path / "batch.json"
    write_batch(p, 20)
    return p
