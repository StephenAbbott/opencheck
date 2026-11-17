"""A source's reading version moves whenever its mapper's output does (the
ledger ticket, 17 Nov 2026). The pinned bundles are real Golden Copy mirror
bundles of 1 Nov 2026 (GLEIF data, CC0)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from opencheck.bods.reading_versions import READING_VERSIONS, reading_digest, reading_version

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import pin_reading_versions as pins  # noqa: E402

PINNED = json.loads((pins.PINS_DIR / "pins.json").read_text())


def test_every_series_source_is_pinned_and_nothing_else():
    assert set(PINNED) == set(READING_VERSIONS)
    for source_id in READING_VERSIONS:
        assert (pins.PINS_DIR / f"{source_id}.json").exists()


@pytest.mark.parametrize("source_id", sorted(READING_VERSIONS))
def test_the_mapper_writes_what_its_reading_version_was_pinned_at(source_id):
    pin = PINNED[source_id]
    digest = reading_digest(pins.mapped(source_id))
    if pin["version"] == reading_version(source_id):
        assert digest == pin["digest"], (
            f"map_{source_id} now writes different recordDetails for the pinned bundles. "
            f"That is a change of reading: bump READING_VERSIONS[{source_id!r}] in "
            "opencheck/bods/reading_versions.py, then run "
            "`uv run python scripts/pin_reading_versions.py`. Without the bump the monthly "
            "series publishes the change as the source's own."
        )
    else:
        pytest.fail(
            f"READING_VERSIONS[{source_id!r}] is {reading_version(source_id)} but the pin is "
            f"for version {pin['version']}: run `uv run python scripts/pin_reading_versions.py`"
        )


def test_the_pinned_bundles_exercise_the_mapper():
    statements = pins.mapped("gleif")
    kinds = {s["recordType"] for s in statements}
    assert {"entity", "relationship", "person"} <= kinds
    assert len(statements) >= 12


def test_the_digest_ignores_dates_of_reading_and_notes():
    statements = pins.mapped("gleif")
    moved = [
        {**s, "statementDate": "2031-01-01", "annotations": [], "source": {}} for s in statements
    ]
    assert reading_digest(moved) == reading_digest(statements)
    changed = [dict(s) for s in statements]
    changed[0] = {**changed[0], "recordDetails": {**changed[0]["recordDetails"], "name": "Other"}}
    assert reading_digest(changed) != reading_digest(statements)


def test_a_source_without_a_version_is_not_ready_for_a_series():
    with pytest.raises(KeyError):
        reading_version("companies_house")
