"""The monthly BODS series build (``scripts/build_bods_series.py``): the
edition's date, the ledger stage across two editions, and the release notes.
The mapping stage is the Estonia dataset's own code path and is covered
there; here its output files are written by hand."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

from opencheck.bods.statements import make_entity_statement, make_relationship_statement

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import build_bods_series as series  # noqa: E402


def _mirror(path: Path, publish: str) -> Path:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.executemany(
        "INSERT INTO meta VALUES (?, ?)",
        [("source_publish_datetime", publish), ("built_at", "2026-11-01T13:55:55+00:00"),
         ("detail_zdict", "x")],
    )
    conn.commit()
    conn.close()
    return path


def _entity(lei: str, name: str) -> dict[str, Any]:
    return make_entity_statement(
        source_id="gleif", local_id=lei, name=name, statement_date="2026-10-20",
        identifiers=[{"scheme": "XI-LEI", "id": lei}],
    )


def _edge(child: dict[str, Any], parent: dict[str, Any]) -> dict[str, Any]:
    return make_relationship_statement(
        source_id="gleif", local_id=f"{child['recordId']}:direct-parent",
        subject_statement_id=child["recordId"], interested_party_statement_id=parent["recordId"],
        interested_party_type="entity",
        interests=[{"type": "otherInfluenceOrControl", "beneficialOwnershipOrControl": False}],
        statement_date="2026-10-20",
    )


def _work(work: Path, parties: list[dict], relationships: list[dict], selected: int) -> None:
    work.mkdir(parents=True, exist_ok=True)
    for kind, rows in (("parties", parties), ("relationships", relationships)):
        with gzip.open(work / f"{kind}-0.jsonl.gz", "wt", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
    (work / "map.json").write_text(
        json.dumps({"shards": 1, "counts": {"leis": len(parties), "selected": selected}})
    )


def _args(tmp: Path, mirror: Path, out: str, **kw: Any) -> argparse.Namespace:
    base = {
        "entity_pages_db": mirror, "ledger": tmp / "ledger.sqlite", "out_dir": tmp / out,
        "work_dir": tmp / out / "work", "edition": None, "limit": None, "part_statements": 2,
    }
    return argparse.Namespace(**{**base, **kw})


def _statements(out_dir: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for part in manifest["parts"]:
        data = (out_dir / part["file"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == part["sha256"]
        assert len(data) == part["bytes"]
        rows += [json.loads(line) for line in gzip.decompress(data).decode().splitlines()]
    return rows


def test_the_edition_is_the_first_of_the_month_of_the_golden_copy_publish(tmp_path):
    first = series.mirror_meta(_mirror(tmp_path / "a.sqlite", "2026-12-01 08:00:00"))
    assert "detail_zdict" not in first
    assert series.edition_for(first, None) == "2026-12-01"
    mid = series.mirror_meta(_mirror(tmp_path / "b.sqlite", "2026-12-14 16:00:00"))
    with pytest.raises(SystemExit, match="not of the first"):
        series.edition_for(mid, None)
    assert series.edition_for(mid, "2026-12-14") == "2026-12-14"


def test_two_editions_through_the_ledger_stage(tmp_path, monkeypatch):
    monkeypatch.setattr(series, "MIN_FULL_LEIS", 3)
    child, parent, other = _entity("C" * 20, "Child Ltd"), _entity("P" * 20, "Parent plc"), _entity("O" * 20, "Other SA")
    rel = _edge(child, parent)

    nov = _mirror(tmp_path / "nov.sqlite", "2026-11-01 08:00:00")
    args = _args(tmp_path, nov, "nov")
    _work(args.work_dir, [child, parent, other], [rel], selected=3)
    manifest = series.stage_ledger(args)
    assert manifest["edition"] == "2026-11-01"
    assert manifest["previous_edition"] is None
    assert manifest["partial"] is False
    assert manifest["statements"] == {"new": 4}
    assert manifest["licence"] == "CC0-1.0"
    assert manifest["sources"]["gleif"]["reading_version"] == 1
    assert [p["statements"] for p in manifest["parts"]] == [2, 2]
    assert manifest["parts"][0]["file"] == "opencheck-bods-gleif-2026-11-01-01.jsonl.gz"
    rows = _statements(args.out_dir, manifest)
    assert [r["recordType"] for r in rows] == ["entity", "entity", "entity", "relationship"]
    assert {r["recordStatus"] for r in rows} == {"new"}
    assert json.loads((args.out_dir / "manifest.json").read_text()) == manifest

    # December: the child is renamed, the parent edge is gone, Other is unchanged.
    dec = _mirror(tmp_path / "dec.sqlite", "2026-12-01 08:00:00")
    args = _args(tmp_path, dec, "dec")
    _work(args.work_dir, [_entity("C" * 20, "Child Holdings Ltd"), parent, other], [], selected=3)
    manifest = series.stage_ledger(args)
    assert manifest["previous_edition"] == "2026-11-01"
    assert manifest["statements"] == {"closed": 1, "unchanged": 2, "updated": 1}
    rows = _statements(args.out_dir, manifest)
    assert sorted((r["recordType"], r["recordStatus"]) for r in rows) == [
        ("entity", "updated"), ("relationship", "closed"),
    ]
    assert all(r["publicationDetails"]["publicationDate"] == "2026-12-01" for r in rows)
    assert manifest["live_records"] == {"gleif|entity": 3}
    readme = (args.out_dir / "README.md").read_text()
    assert "only what changed since 2026-11-01" in readme
    assert "Updated: 1" in readme and "Closed: 1" in readme


def test_a_trial_over_part_of_the_golden_copy_closes_nothing(tmp_path):
    child, parent = _entity("C" * 20, "Child Ltd"), _entity("P" * 20, "Parent plc")
    nov = _mirror(tmp_path / "nov.sqlite", "2026-11-01 08:00:00")
    args = _args(tmp_path, nov, "nov", limit=2)
    _work(args.work_dir, [child, parent], [_edge(child, parent)], selected=2)
    assert series.stage_ledger(args)["partial"] is True
    args = _args(tmp_path, nov, "again", limit=1, edition="2026-11-02")
    _work(args.work_dir, [child], [], selected=1)
    manifest = series.stage_ledger(args)
    assert manifest["statements"] == {"unchanged": 1}
    assert manifest["parts"] == []
    assert "Partial edition" in (args.out_dir / "README.md").read_text()


def test_an_edition_whose_edges_name_missing_parties_is_not_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(series, "MIN_FULL_LEIS", 1)
    monkeypatch.setattr(series, "MAX_DANGLING", 0)
    child, parent = _entity("C" * 20, "Child Ltd"), _entity("P" * 20, "Parent plc")
    nov = _mirror(tmp_path / "nov.sqlite", "2026-11-01 08:00:00")
    args = _args(tmp_path, nov, "nov")
    _work(args.work_dir, [child], [_edge(child, parent)], selected=1)
    with pytest.raises(SystemExit, match="name no live record"):
        series.stage_ledger(args)
    from opencheck.bods.publication_ledger import PublicationLedger

    assert PublicationLedger(args.ledger).editions() == []


def test_the_release_notes_never_state_ownership():
    manifest = {
        "edition": "2026-11-01", "previous_edition": None, "partial": False,
        "statements": {"new": 10}, "statements_published": 10,
        "sources": {"gleif": {"golden_copy_publish": "2026-11-01 08:00:00", "leis_in_mirror": 3}},
    }
    text = series.render_readme(manifest)
    assert "first edition" in text
    assert "accounting consolidation, not shareholding" in text
    for word in ("owns ", "owned by", "shareholder of"):
        assert word not in text


def test_a_refreshed_mirror_needs_a_named_edition(tmp_path) -> None:
    """A mirror brought forward by a delta is not the Golden Copy of the first
    of a month, so the preview must say which day it is — before any download."""
    with pytest.raises(SystemExit, match="needs --edition"):
        series.refresh_mirror(tmp_path / "entity_pages.sqlite", None)
