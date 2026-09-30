"""The mypy ratchet's comparison (Phase 266). The mypy run itself is CI's
step; this pins the rule it applies to the counts."""

from __future__ import annotations

import importlib.util
from collections import Counter
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "mypy_ratchet", Path(__file__).resolve().parent.parent / "scripts" / "mypy_ratchet.py"
)
assert _spec and _spec.loader
ratchet = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ratchet)


def test_a_module_may_not_carry_more_than_its_baseline() -> None:
    worse, better = ratchet.compare(Counter({"a.py": 3}), {"a.py": 2})
    assert worse == ["a.py: 2 → 3"] and better == []


def test_a_module_outside_the_baseline_must_carry_none() -> None:
    worse, _ = ratchet.compare(Counter({"new.py": 1}), {})
    assert worse == ["new.py: 1 (new — must be 0)"]


def test_an_improvement_is_reported_so_the_baseline_can_lock_it_in() -> None:
    worse, better = ratchet.compare(Counter({"a.py": 1}), {"a.py": 2, "gone.py": 1})
    assert worse == [] and better == ["a.py: 2 → 1", "gone.py: 1 → 0"]


def test_the_error_pattern_reads_mypy_lines_and_nothing_else() -> None:
    assert ratchet._ERROR.match("opencheck/app.py:58: error: Incompatible types  [assignment]")
    assert ratchet._ERROR.match("opencheck/x.py:3:7: error: Name 'y' is not defined  [name-defined]")
    assert not ratchet._ERROR.match("opencheck/app.py:58: note: See https://mypy.rtfd.io")
    assert not ratchet._ERROR.match("Found 135 errors in 53 files (checked 273 source files)")


def test_the_committed_baseline_is_well_formed() -> None:
    baseline = ratchet.load_baseline()
    assert baseline, "backend/mypy-baseline.json is missing or empty"
    assert all(k.startswith("opencheck/") and v > 0 for k, v in baseline.items())
