"""The daily BODS file and the ``/changes/*`` files — ``daily_changes_file``.

Driven through ``mirror_refresh.apply_delta`` with the mirror fixtures, like
``test_daily_changes``: the delta names three LEIs, the hook grades them and
writes the day's file from the mirror, and the routes serve it.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from pathlib import Path

import pytest

from opencheck import daily_changes as dc
from opencheck import daily_changes_file as dcf
from opencheck import mirror_refresh as mr
from tests.test_daily_changes import DAY, PUBLISH, _client, env  # noqa: F401 — fixture
from tests.test_gleif_mirror_store import EASY, TOP
from tests.test_mirror_refresh import NEW_LEI, _delta_publish


def _read(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def _leis(stmt: dict) -> set[str]:
    return {
        i.get("id")
        for i in (stmt.get("recordDetails") or {}).get("identifiers") or []
        if i.get("scheme") == "XI-LEI"
    }


def test_the_delta_writes_the_days_bods_file_beside_the_store(env: Path, tmp_path: Path) -> None:  # noqa: F811
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    path = tmp_path / "changes" / f"{DAY}.jsonl.gz"
    assert path.exists()
    statements = _read(path)
    ids = [s["statementId"] for s in statements]
    assert len(ids) == len(set(ids))
    entities = [s for s in statements if s["recordType"] == "entity"]
    named = set().union(*(_leis(s) for s in entities))
    # Every band is in the file: the new LEI and the note-only change too.
    assert {NEW_LEI, EASY, TOP} <= named
    # Parties come before any relationship, and every reference resolves.
    kinds = [s["recordType"] for s in statements]
    first_rel = kinds.index("relationship") if "relationship" in kinds else len(kinds)
    assert all(k != "relationship" for k in kinds[:first_rel])
    assert all(k == "relationship" for k in kinds[first_rel:])
    records = {s["recordId"] for s in statements if s["recordType"] != "relationship"}
    for rel in statements[first_rel:]:
        for side in ("subject", "interestedParty"):
            ref = rel["recordDetails"].get(side)
            if isinstance(ref, str):
                assert ref in records
    # No child edges: each statement here is the LEI's own, or a parent's.
    assert all(s["source"]["opencheckSourceId"] == "gleif" for s in statements)
    # Rebuilt whole: the same delta again gives the same bytes.
    before = path.read_bytes()
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    assert path.read_bytes() == before


def test_the_file_and_csv_routes(env: Path, tmp_path: Path) -> None:  # noqa: F811
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    http = _client()
    index = http.get("/daily-changes").json()
    (held,) = index["files"]
    assert held["day"] == DAY and held["bods"] == f"/changes/{DAY}.jsonl.gz"
    assert held["statements"] and held["leis"] == 3

    got = http.get(f"/changes/{DAY}.jsonl.gz")
    assert got.status_code == 200
    assert got.headers["content-type"] == "application/gzip"
    assert "max-age=86400" in got.headers["cache-control"]
    assert gzip.decompress(got.content).count(b"\n") == held["statements"]

    latest = http.get("/changes/latest.jsonl.gz", follow_redirects=False)
    assert latest.status_code == 302
    assert latest.headers["location"] == f"/changes/{DAY}.jsonl.gz"

    sheet = http.get(f"/changes/{DAY}.csv")
    assert sheet.status_code == 200 and sheet.headers["content-type"].startswith("text/csv")
    rows = list(csv.DictReader(io.StringIO(sheet.text)))
    assert tuple(rows[0]) == dcf.CSV_COLUMNS
    assert {r["lei"] for r in rows} == {NEW_LEI, EASY, TOP}

    # A refusal is never kept by the edge: the paths end in static extensions.
    for missing in ("/changes/2026-01-01.jsonl.gz", "/changes/whatever.zip"):
        refused = http.get(missing)
        assert refused.status_code == 404
        assert refused.headers["cache-control"] == "no-store"


def test_the_file_routes_answer_503_when_the_feature_is_off(
    env: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    from opencheck.config import get_settings

    monkeypatch.delenv("OPENCHECK_DAILY_CHANGES_DB_FILE")
    get_settings.cache_clear()
    dc.reset_for_tests()
    off = _client().get("/changes/latest.jsonl.gz")
    assert off.status_code == 503 and off.headers["cache-control"] == "no-store"


def test_latest_before_any_file_is_a_404_the_edge_does_not_keep(env: Path) -> None:  # noqa: F811
    got = _client().get("/changes/latest.jsonl.gz", follow_redirects=False)
    assert got.status_code == 404 and got.headers["cache-control"] == "no-store"


def test_files_are_pruned_with_the_days_the_store_drops(env: Path, tmp_path: Path) -> None:  # noqa: F811
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    stale = tmp_path / "changes" / "2026-01-01.jsonl.gz"
    stale.write_bytes(b"")
    assert dcf.prune(d["day"] for d in dc.get_store().days()) == 1
    assert not stale.exists()
    assert (tmp_path / "changes" / f"{DAY}.jsonl.gz").exists()


def test_an_opensanctions_row_in_the_csv_carries_its_id_and_no_name() -> None:
    rows = [
        {
            "lei": EASY, "name": "EASY POWER", "jurisdiction": "GB", "source": "opensanctions",
            "band": "headline", "reasons": ["os_topic_added"], "changes": [{"topics_added": ["sanction"]}],
            "as_of": "20261113065434-idv",
            "ref": {"opensanctions_id": "NK-abc", "url": "https://www.opensanctions.org/entities/NK-abc/"},
        }
    ]
    (row,) = csv.DictReader(io.StringIO(dcf.csv_bytes(rows, DAY).decode()))
    assert row["opensanctions_id"] == "NK-abc" and row["fields_changed"] == "topics_added"


def test_a_removed_parent_is_published_as_a_closed_record(env: Path) -> None:  # noqa: F811
    from opencheck.bods.statements import _stable_id

    gone = "5493000000000000GONE"
    stmts = dcf.statements_for(EASY, [("direct", gone), ("ultimate", gone)])
    closed = [s for s in stmts if s.get("recordStatus") == "closed"]
    assert len(closed) == 2
    for rel, kind in zip(closed, ("direct", "ultimate"), strict=True):
        assert rel["recordId"] == _stable_id(
            "gleif", "relationship-record", f"{EASY}:{kind}-parent:{gone}"
        )
        assert rel["recordDetails"]["interestedParty"] == _stable_id("gleif", "entity", gone)
        assert "endDate" not in rel["recordDetails"]["interests"][0]
    # A parent the LEI still has is never closed.
    held = [
        s for s in dcf.statements_for(EASY)
        if s["recordType"] == "relationship" and s.get("recordStatus") != "closed"
    ]
    for rel in held:
        parent = next(
            s for s in dcf.statements_for(EASY)
            if s.get("recordId") == rel["recordDetails"]["interestedParty"]
        )
        lei = dcf._lei_of(parent)
        assert not [
            s for s in dcf.statements_for(EASY, [("direct", lei)])
            if s.get("recordStatus") == "closed" and s["recordId"] == rel["recordId"]
        ]


def test_removed_parents_reads_the_parent_fields_only() -> None:
    rows = [
        {"lei": EASY, "changes": [
            {"field": "direct_parent_lei", "old": "AAA", "new": "BBB"},
            {"field": "legal_name", "old": "X", "new": "Y"},
        ]},
        {"lei": EASY, "changes": [{"field": "direct_parent_lei", "old": "AAA", "new": None}]},
        {"lei": TOP, "changes": [{"field": "ultimate_parent_lei", "old": None, "new": "CCC"}]},
    ]
    assert dcf.removed_parents(rows) == {EASY: [("direct", "AAA")]}


def test_the_page_words_every_band_and_reason_the_backend_has() -> None:
    import re

    ts = (Path(__file__).resolve().parents[2] / "frontend/src/lib/dailyChanges.ts").read_text()

    def block(name: str) -> dict[str, str]:
        body = re.search(rf"export const {name}[^=]*= \{{(.*?)\n\}};", ts, re.S).group(1)
        return dict(re.findall(r'^\s*(\w+): "([^"]*)",', body, re.M))

    assert block("BAND_LABELS") == dc.BAND_WORDS
    assert set(block("REASON_WORDS")) == set(dc.REASON_BAND)
