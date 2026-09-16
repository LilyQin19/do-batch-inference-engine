from __future__ import annotations

from pathlib import Path

import pytest

from batchengine.core.spend_ledger import load_total_spend, record_spend


def test_load_missing_ledger_returns_zero(tmp_path: Path) -> None:
    assert load_total_spend(tmp_path / "nope.json") == 0.0


def test_record_spend_accumulates(tmp_path: Path) -> None:
    path = tmp_path / "ledger.json"
    record_spend(path, 0.05)
    total = record_spend(path, 0.03)
    assert total == pytest.approx(0.08)
    assert load_total_spend(path) == pytest.approx(0.08)


def test_record_spend_ignores_negative_delta(tmp_path: Path) -> None:
    path = tmp_path / "ledger.json"
    record_spend(path, 0.10)
    total = record_spend(path, -5.0)
    assert total == pytest.approx(0.10)


def test_load_tolerates_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "ledger.json"
    path.write_text("not json")
    assert load_total_spend(path) == 0.0
