"""The daily changes list (shadow) — ``opencheck/daily_changes.py``.

Graders are pure and tested on plain dicts; the GLEIF hook is driven through
``mirror_refresh.apply_delta`` with the mirror fixtures; the OpenSanctions side
through a fake client, so nothing here touches the network.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import daily_changes as dc
from opencheck import entity_pages as ep
from opencheck import mirror_refresh as mr
from opencheck import mirrorstats
from opencheck.config import get_settings
from tests.test_gleif_mirror_store import EASY, TOP
from tests.test_mirror_refresh import NEW_LEI, _build_mirror, _delta_publish

PUBLISH = "2026-09-08 08:00:00"
DAY = "2026-09-08"


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db = _build_mirror(tmp_path / "entity_pages.sqlite", tmp_path)
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_DAILY_CHANGES_DB_FILE", str(tmp_path / "daily_changes.sqlite"))
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_URL", raising=False)
    monkeypatch.delenv("OPENCHECK_WATCHLIST_DB_FILE", raising=False)
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_DAILY_CHANGES_INTERVAL_S", "0")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    mr.reset_for_tests()
    mirrorstats.reset_for_tests()
    dc.reset_for_tests()
    yield db
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    mr.reset_for_tests()
    mirrorstats.reset_for_tests()
    dc.reset_for_tests()


def _facts(**over: Any) -> dict[str, Any]:
    base = {
        "legal_name": "ACME LTD", "entity_status": "ACTIVE", "registration_status": "ISSUED",
        "jurisdiction": "GB", "legal_form": "H0PO", "successors": [],
        "direct_parent_lei": None, "ultimate_parent_lei": None,
        "direct_exception": None, "ultimate_exception": None, "category": "GENERAL",
        "registered_as": "0001", "corporate_events": [],
    }
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# The vocabulary
# ---------------------------------------------------------------------------


def test_every_reason_has_a_known_band_and_every_band_has_words() -> None:
    assert set(dc.REASON_BAND.values()) <= set(dc.BANDS)
    assert set(dc.BAND_WORDS) == set(dc.BANDS)
    assert set(dc.DEFAULT_BANDS) < set(dc.BANDS)
    assert dc.NOTE not in dc.DEFAULT_BANDS and dc.NEW not in dc.DEFAULT_BANDS


def test_every_gleif_material_field_is_graded_by_name() -> None:
    """A field added to the watchlist's material set must be given a reason
    here, not fall through to ``event_other``."""
    from opencheck.watchlist import GLEIF_MATERIAL_FIELDS

    graded = set(dc._FIELD_REASON) | {"entity_status", "corporate_events", "direct_children"}
    assert set(GLEIF_MATERIAL_FIELDS) <= graded


# ---------------------------------------------------------------------------
# GLEIF grading
# ---------------------------------------------------------------------------


def test_renewal_churn_is_no_row() -> None:
    assert dc.grade_gleif(_facts(), _facts()) is None


def test_a_first_time_lei_is_new_and_a_vanished_one_is_nothing() -> None:
    assert dc.grade_gleif(None, _facts())["band"] == dc.NEW
    assert dc.grade_gleif(_facts(), None) is None


def test_a_parent_change_heads_the_list() -> None:
    row = dc.grade_gleif(_facts(), _facts(direct_parent_lei="5493001KJTIIGC8Y1R12"))
    assert row["band"] == dc.HEADLINE and row["reasons"] == ["parent_changed"]
    assert row["changes"] == [
        {"field": "direct_parent_lei", "old": None, "new": "5493001KJTIIGC8Y1R12"}
    ]


def test_a_reporting_exception_or_a_rekeyed_register_number_is_a_note() -> None:
    """The two that made the watchlist's recheck tier too loud site-wide:
    ~1,500 exception moves a day and a 1,700-row re-keying on 13 Nov 2026."""
    assert dc.grade_gleif(_facts(), _facts(direct_exception="NON_CONSOLIDATING"))["band"] == dc.NOTE
    assert dc.grade_gleif(_facts(), _facts(registered_as="00001"))["band"] == dc.NOTE
    assert dc.grade_gleif(_facts(), _facts(registration_status="LAPSED"))["band"] == dc.NOTE


def test_a_rename_is_a_record_change_and_the_worst_reason_sets_the_band() -> None:
    assert dc.grade_gleif(_facts(), _facts(legal_name="ACME PLC"))["band"] == dc.RECORD
    row = dc.grade_gleif(
        _facts(), _facts(legal_name="ACME PLC", ultimate_parent_lei="5493001KJTIIGC8Y1R12")
    )
    assert row["band"] == dc.HEADLINE
    assert row["reasons"] == ["parent_changed", "legal_name"]


def test_an_ending_on_an_lei_that_had_already_lapsed_is_set_apart() -> None:
    live = dc.grade_gleif(_facts(), _facts(entity_status="INACTIVE", registration_status="RETIRED"))
    assert live["band"] == dc.CEASED
    lapsed = dc.grade_gleif(
        _facts(registration_status="LAPSED"),
        _facts(entity_status="INACTIVE", registration_status="RETIRED"),
    )
    assert lapsed["band"] == dc.CEASED_LAPSED
    # ...but an ownership change on a lapsed LEI still heads the list.
    both = dc.grade_gleif(
        _facts(registration_status="LAPSED"),
        _facts(entity_status="INACTIVE", successors=["5493001KJTIIGC8Y1R12 (NEWCO)"]),
    )
    assert both["band"] == dc.HEADLINE


def test_events_are_graded_by_type_and_status() -> None:
    def ev(etype: str, status: str) -> dict[str, str]:
        return {"type": etype, "status": status, "effective": "2026-11-01", "recorded": "2026-11-12"}

    assert dc.event_reason(ev("MERGERS_AND_ACQUISITIONS", "COMPLETED")) == "merger_event"
    assert dc.event_reason(ev("DISSOLUTION", "COMPLETED")) == "terminal_event"
    assert dc.event_reason(ev("DISSOLUTION", "IN_PROGRESS")) == "terminal_event_in_progress"
    assert dc.event_reason(ev("CHANGE_LEGAL_FORM", "COMPLETED")) == "event_completed"
    assert dc.event_reason(ev("DISSOLUTION", "WITHDRAWN_CANCELLED")) == "event_other"
    old = [ev("DISSOLUTION", "IN_PROGRESS")]
    new = [ev("DISSOLUTION", "IN_PROGRESS"), ev("DISSOLUTION", "COMPLETED")]
    row = dc.grade_gleif(_facts(corporate_events=old), _facts(corporate_events=new))
    assert row["band"] == dc.CEASED and row["changes"][0]["new"] == [new[1]]
    # An event dropped from the record is not a change.
    assert dc.grade_gleif(_facts(corporate_events=new), _facts(corporate_events=old)) is None


def test_a_withdrawn_successor_is_not_a_change_of_hands() -> None:
    row = dc.grade_gleif(_facts(successors=["X (Y)"]), _facts(successors=[]))
    assert row["band"] == dc.NOTE


def test_sole_proprietors_are_recognised_on_either_side() -> None:
    assert dc.is_sole_proprietor(_facts(category="SOLE_PROPRIETOR"), _facts())
    assert dc.is_sole_proprietor(None, _facts(category="sole_proprietor"))
    assert not dc.is_sole_proprietor(_facts(), None)


# ---------------------------------------------------------------------------
# OpenSanctions grading
# ---------------------------------------------------------------------------


def _os(entity_id: str, *, schema: str = "Company", lei: str | None = EASY,
        topics: list[str] | None = None, datasets: list[str] | None = None,
        name: str = "SECRET NAME LLC") -> dict[str, Any]:
    props: dict[str, Any] = {"name": [name], "topics": topics or []}
    if lei:
        props["leiCode"] = [lei]
    return {"id": entity_id, "schema": schema, "properties": props, "datasets": datasets or ["x"]}


def test_only_an_lei_bearing_organisation_is_remembered() -> None:
    assert dc.os_projection(_os("a", schema="Person")) is None
    assert dc.os_projection(_os("a", lei=None)) is None
    assert dc.os_projection(_os("a", lei="NOTANLEI")) is None
    assert dc.os_projection(_os("a", topics=["sanction"])) == {
        "leis": [EASY], "topics": ["sanction"], "datasets": ["x"],
    }
    # ...and never its name or any other property.
    assert "SECRET" not in json.dumps(dc.os_projection(_os("a")))


def test_os_grading() -> None:
    p = dc.os_projection
    plain, listed = p(_os("a")), p(_os("a", topics=["sanction"]))
    assert dc.grade_os("MOD", plain, plain) is None
    assert dc.grade_os("MOD", plain, listed)["reasons"] == ["os_topic_added"]
    assert dc.grade_os("MOD", plain, listed)["band"] == dc.HEADLINE
    assert dc.grade_os("MOD", listed, plain)["band"] == dc.RECORD
    assert dc.grade_os("ADD", None, listed)["reasons"] == ["os_new_listing"]
    assert dc.grade_os("ADD", None, plain) is None
    assert dc.grade_os("DEL", listed, None)["reasons"] == ["os_removed"]
    assert dc.grade_os("DEL", plain, None) is None
    # A topic that is not a risk topic (a company's sector tag) is not a change.
    assert dc.grade_os("MOD", plain, p(_os("a", topics=["corp.public"]))) is None


def test_a_new_dataset_is_a_listing_only_when_the_catalogue_says_so() -> None:
    p = dc.os_projection
    before, after = p(_os("a")), p(_os("a", datasets=["x", "us_ofac_sdn"]))
    lists = frozenset({"us_ofac_sdn"})
    assert dc.grade_os("MOD", before, after, list_datasets=lists)["reasons"] == ["os_list_added"]
    assert dc.grade_os("MOD", before, after, list_datasets=frozenset())["band"] == dc.RECORD
    # Catalogue unread: never guessed to be a listing.
    assert dc.grade_os("MOD", before, after)["reasons"] == ["os_dataset_added"]
    # An enrichment source joining is not a change.
    assert dc.grade_os("MOD", before, p(_os("a", datasets=["x", "ext_gleif"]))) is None


def test_counter_sanctions_lists_are_not_a_listing_tag() -> None:
    assert "list.sanction.counter" not in dc.OS_LIST_TAGS


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------


def _row(lei: str, band: str = dc.HEADLINE) -> dict[str, Any]:
    return {"lei": lei, "band": band, "reasons": ["parent_changed"], "changes": [], "name": "N"}


def test_a_delta_recorded_twice_counts_once(tmp_path: Path) -> None:
    store = dc.DailyChangesStore(tmp_path / "d.sqlite")
    counts = {"named": 10, "churn": 9, "not_a_key": 5}
    assert store.record(DAY, "gleif", PUBLISH, [_row(EASY)], counts) == 1
    assert store.record(DAY, "gleif", PUBLISH, [_row(EASY)], counts) == 0
    (day,) = store.days()
    assert day["bands"] == {"headline": 1}
    assert day["sources"] == {"gleif": {"headline": 1, "named": 10, "churn": 9}}
    # A second publish on the same day adds to it.
    store.record(DAY, "gleif", "2026-09-08 16:00:00", [_row(EASY), _row(TOP, dc.NOTE)], {"named": 2})
    (day,) = store.days()
    assert day["bands"] == {"headline": 2, "note": 1} and day["sources"]["gleif"]["named"] == 12


def test_rows_are_served_most_notable_first_and_by_band(tmp_path: Path) -> None:
    store = dc.DailyChangesStore(tmp_path / "d.sqlite")
    store.record(DAY, "gleif", PUBLISH, [_row(TOP, dc.NOTE), _row(EASY, dc.RECORD), _row(NEW_LEI)], {})
    assert [r["band"] for r in store.day(DAY, dc.BANDS)] == ["headline", "record", "note"]
    assert [r["lei"] for r in store.day(DAY, dc.DEFAULT_BANDS)] == [NEW_LEI, EASY]
    assert store.day(DAY, ["nonsense"]) == []


def test_old_days_are_pruned(tmp_path: Path) -> None:
    store = dc.DailyChangesStore(tmp_path / "d.sqlite")
    store.record("2026-08-01", "gleif", "a", [_row(EASY)], {"named": 1})
    store.record(DAY, "gleif", "b", [_row(EASY)], {"named": 1})
    assert store.prune(30, today="2026-09-10") == 1
    assert [d["day"] for d in store.days()] == [DAY]
    assert store.prune(0) == 0


# ---------------------------------------------------------------------------
# The GLEIF hook, through apply_delta
# ---------------------------------------------------------------------------


def test_a_gleif_delta_is_graded_from_the_mirror_before_and_after(env: Path, tmp_path: Path) -> None:
    counts = mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    assert set(counts) == {"entities", "relationships", "exceptions", "compressed"}
    store = dc.get_store()
    rows = {r["lei"]: r for r in store.day(DAY, dc.BANDS)}
    assert rows[NEW_LEI]["band"] == dc.NEW and rows[NEW_LEI]["name"] == "Mirror Newcomer AS"
    assert "legal_name" in rows[EASY]["reasons"]
    assert rows[EASY]["name"] == "EASY POWER (RENAMED)" and rows[EASY]["source"] == "gleif"
    name_change = next(c for c in rows[EASY]["changes"] if c["field"] == "legal_name")
    assert name_change["new"] == "EASY POWER (RENAMED)" and name_change["old"]
    assert rows[TOP]["band"] == dc.NOTE and rows[TOP]["reasons"] == ["reporting_exception"]
    (day,) = store.days()
    assert day["sources"]["gleif"]["named"] == 3
    # The same delta again: nothing differs any more, and nothing is added.
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    assert len(store.day(DAY, dc.BANDS)) == 3


def test_the_hook_is_inert_when_the_feature_is_off(
    env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OPENCHECK_DAILY_CHANGES_DB_FILE")
    get_settings.cache_clear()
    dc.reset_for_tests()
    assert dc.capture_before([EASY]) is None
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    assert not (tmp_path / "daily_changes.sqlite").exists()
    assert dc.stats() == {"enabled": False}


def test_a_failure_in_the_hook_does_not_fail_the_refresh(
    env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("no")

    monkeypatch.setattr(dc, "_facts", boom)
    counts = mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    assert counts["entities"] == 2
    assert "RuntimeError" in dc.stats()["last_error"]


def test_a_sole_proprietor_is_counted_and_left_out(env: Path) -> None:
    before = {EASY: _facts(category="SOLE_PROPRIETOR", legal_name="A PERSON")}
    result = dc.on_gleif_delta(before, PUBLISH)
    assert result["stored"] == 0
    (day,) = dc.get_store().days()
    assert day["sources"]["gleif"] == {"named": 1, "sole_proprietor": 1}


# ---------------------------------------------------------------------------
# OpenSanctions, through a fake client
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, status: int = 200, payload: Any = None, lines: list[str] | None = None) -> None:
        self.status_code = status
        self._payload = payload
        self._lines = lines or []

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._payload

    def iter_lines(self) -> Any:
        return iter(self._lines)


class _Client:
    def __init__(self) -> None:
        self.versions: list[str] = []
        self.full: dict[str, list[dict[str, Any]]] = {}
        self.deltas: dict[str, list[dict[str, Any]]] = {}
        self.catalog: Any = {"datasets": [{"name": "us_ofac_sdn", "tags": ["list.sanction"]}]}
        self.calls: list[str] = []

    def get(self, url: str, timeout: float = 0) -> _Resp:
        self.calls.append(url)
        if url == dc.OS_VERSIONS_URL:
            return _Resp(payload={"items": list(self.versions)})
        if url == dc.OS_CATALOG_URL:
            return _Resp(payload=self.catalog) if self.catalog else _Resp(status=500)
        raise AssertionError(url)

    @contextmanager
    def stream(self, method: str, url: str, timeout: float = 0) -> Any:
        self.calls.append(url)
        for version, entities in self.full.items():
            if url == dc.OS_FULL_URL.format(version=version):
                yield _Resp(lines=[json.dumps(e) for e in entities])
                return
        for version, records in self.deltas.items():
            if url == dc.OS_DELTA_URL.format(version=version):
                yield _Resp(lines=[json.dumps(r) for r in records])
                return
        yield _Resp(status=404)


V1, V2, V3 = "20260908060000-aaa", "20260908120000-bbb", "20260909060000-ccc"


def test_the_first_run_takes_a_baseline_and_stores_no_rows(env: Path) -> None:
    client = _Client()
    client.versions = [V1]
    client.full[V1] = [_os("os-easy"), _os("os-person", schema="Person"), _os("os-nolei", lei=None)]
    result = dc.opensanctions_tick(client)
    store = dc.get_store()
    assert result["seeded"] == 1 and store.os_count() == 1
    assert store.get_meta("opensanctions_version") == V1
    assert store.days() == []
    # Nothing new: nothing read.
    client.calls.clear()
    assert dc.opensanctions_tick(client)["versions"] == 0
    assert client.calls == [dc.OS_VERSIONS_URL]


def test_a_new_sanctions_topic_on_an_lei_heads_the_list_as_a_reference(env: Path) -> None:
    client = _Client()
    client.versions = [V1]
    client.full[V1] = [_os("os-easy")]
    dc.opensanctions_tick(client)
    client.versions = [V1, V2, V3]
    client.deltas[V2] = [
        {"op": "MOD", "entity": _os("os-easy", topics=["sanction"], datasets=["x", "us_ofac_sdn"])},
        {"op": "MOD", "entity": _os("os-person", schema="Person", topics=["sanction"])},
        {"op": "ADD", "entity": _os("os-unknown", lei="529900T8BM49AURSDO55", topics=["debarment"])},
    ]
    client.deltas[V3] = [{"op": "MOD", "entity": _os("os-easy", datasets=["x", "us_ofac_sdn"])}]
    result = dc.opensanctions_tick(client)
    assert result["versions"] == 2 and result["stored"] == 2
    store = dc.get_store()
    (row,) = store.day("2026-09-08", dc.BANDS)
    assert row["lei"] == EASY and row["band"] == dc.HEADLINE and row["source"] == "opensanctions"
    assert row["reasons"] == ["os_list_added", "os_topic_added"]
    assert row["ref"] == {
        "opensanctions_id": "os-easy", "url": "https://www.opensanctions.org/entities/os-easy/",
    }
    # References only: OpenSanctions' own name for the entity is never stored;
    # the name on the row is GLEIF's.
    assert "SECRET" not in json.dumps(row) and row["name"]
    (later,) = store.day("2026-09-09", dc.BANDS)
    assert later["band"] == dc.RECORD and later["reasons"] == ["os_topic_removed"]
    counts = {d["day"]: d["sources"]["opensanctions"] for d in store.days()}
    assert counts["2026-09-08"]["not_an_lei_here"] == 1
    assert store.get_meta("opensanctions_version") == V3


def test_a_version_that_cannot_be_read_is_a_gap_and_a_new_baseline(env: Path) -> None:
    client = _Client()
    client.versions = [V1]
    client.full[V1] = [_os("os-easy")]
    dc.opensanctions_tick(client)
    client.versions = [V1, V2, V3]  # V2's delta answers 404
    client.full[V3] = [_os("os-easy", topics=["sanction"])]
    result = dc.opensanctions_tick(client)
    store = dc.get_store()
    assert result["gaps"] == 1 and store.get_meta("opensanctions_version") == V3
    assert store.os_get("os-easy")["topics"] == ["sanction"]
    assert store.day("2026-09-08", dc.BANDS) == []
    assert store.days()[0]["sources"]["opensanctions"]["gap"] == 1


def test_a_failed_tick_is_said_and_never_raises(env: Path) -> None:
    class Broken:
        def get(self, url: str, timeout: float = 0) -> Any:
            raise RuntimeError("down")

    assert dc.opensanctions_tick(Broken()) == {"versions": 0, "stored": 0}
    assert "RuntimeError" in dc.stats()["os_last_error"]


# ---------------------------------------------------------------------------
# The routes and /signalstats
# ---------------------------------------------------------------------------


def _client() -> TestClient:
    from opencheck.app import app

    return TestClient(app)


def test_the_routes_serve_the_day(env: Path, tmp_path: Path) -> None:
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    http = _client()
    index = http.get("/daily-changes").json()
    assert index["available"] and index["days"][0]["day"] == DAY
    assert [b["band"] for b in index["bands"]] == list(dc.BANDS)
    default = http.get(f"/daily-changes/{DAY}.json")
    assert default.status_code == 200
    body = default.json()
    assert body["complete"] is True and body["bands_served"] == list(dc.DEFAULT_BANDS)
    assert {r["band"] for r in body["rows"]} <= set(dc.DEFAULT_BANDS)
    assert "CC BY-NC" in body["opensanctions_notice"]
    assert "max-age" in default.headers["cache-control"]
    everything = http.get(f"/daily-changes/{DAY}.json?band=all").json()
    assert {r["lei"] for r in everything["rows"]} == {NEW_LEI, EASY, TOP}
    notes = http.get(f"/daily-changes/{DAY}.json?band=note").json()
    assert [r["lei"] for r in notes["rows"]] == [TOP]
    # An empty day is an answer, not a 404.
    empty = http.get("/daily-changes/2026-01-01.json")
    assert empty.status_code == 200 and empty.json()["rows"] == []
    assert http.get(f"/daily-changes/{DAY}.json?band=loud").status_code == 400
    assert http.get("/daily-changes/yesterday.json").status_code == 400


def test_the_routes_answer_503_when_the_feature_is_off(
    env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OPENCHECK_DAILY_CHANGES_DB_FILE")
    get_settings.cache_clear()
    dc.reset_for_tests()
    http = _client()
    assert http.get("/daily-changes").status_code == 503
    assert http.get(f"/daily-changes/{DAY}.json").status_code == 503


def test_signalstats_carries_counts_and_never_an_lei_or_a_name(env: Path, tmp_path: Path) -> None:
    mr.apply_delta(env, _delta_publish(tmp_path, PUBLISH), "LastDay")
    section = _client().get("/signalstats").json()["daily_changes"]
    assert section["enabled"] and section["days"][0]["bands"]
    text = json.dumps(section)
    for secret in (EASY, TOP, NEW_LEI, "EASY POWER", "Newcomer"):
        assert secret not in text
