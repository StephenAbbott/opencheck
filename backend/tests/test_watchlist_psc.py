"""The watchlist's PSC tier — the Companies House stream as a push trigger
(pKYC stage E, Route 1).

Pinned here: what the stream hands the watcher (each event's outcome and
kind, never a name); how a watch's Companies House number is found; the
hook that maps a batch to watched LEIs and queues one re-run per company,
folding a day's filings into it; the gap catch-up; the entry a ``psc``
re-run writes even when nothing else moved, graded recheck; the watch row's
"Register changes" line (Q5); the counters; and the words on both sides.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import psc_stream
from opencheck import watchlist as wl
from opencheck import watchlist_materiality as materiality
from opencheck import watchlist_psc as psc
from opencheck.psc_stream import Touched
from opencheck.routers.watch import _entry_content, _entry_title
from tests.test_psc_stream import _event, _lines, _run_until, _scripted, store_path  # noqa: F401, F811 — fixtures
from tests.test_watchlist import EASY, _watch, client, env  # noqa: F401, F811 — fixtures

REPO = Path(__file__).resolve().parents[2]
EASY_NUMBER = "02999029"  # the Companies House number GLEIF files for the fixture's EASY POWER


@pytest.fixture(autouse=True)
def _fresh_index() -> None:
    psc.reset_index_for_tests()
    psc_stream.reset_state_for_tests()


@pytest.fixture
def uk(client: TestClient) -> dict:  # noqa: F811
    """A list watching EASY, whose GLEIF record files a Companies House
    number (dropped leading zero, as filers write it)."""
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    with store._conn() as conn:
        conn.execute(
            "UPDATE watches SET gleif_facts_json = ? WHERE lei = ?",
            (json.dumps({"registered_at": "RA000585", "registered_as": "2999029"}), EASY),
        )
    psc.reset_index_for_tests()
    return {"store": store, "token": token}


def _touched(company: str, pid: str, outcome: str = "added", kind: str = "individual") -> Touched:
    return Touched(company, pid, outcome, kind)


# ---------------------------------------------------------------------------
# What the stream hands over
# ---------------------------------------------------------------------------


def test_the_kind_of_an_event_is_read_from_its_data_or_its_path() -> None:
    assert (
        psc_stream.psc_kind_of({"data": {"kind": "individual-person-with-significant-control"}})
        == "individual"
    )
    assert (
        psc_stream.psc_kind_of(
            {"data": {"kind": "corporate-entity-person-with-significant-control"}}
        )
        == "corporate"
    )
    assert (
        psc_stream.psc_kind_of({"data": {"kind": "legal-person-person-with-significant-control"}})
        == "legal_person"
    )
    assert (
        psc_stream.psc_kind_of({"data": {"kind": "super-secure-person-with-significant-control"}})
        == "withheld"
    )
    # A deleted event carries no data: the path says.
    assert (
        psc_stream.psc_kind_of(
            {"resource_uri": "/company/1/persons-with-significant-control/legal-person/x"}
        )
        == "legal_person"
    )
    assert (
        psc_stream.psc_kind_of(
            {"resource_uri": "/company/1/persons-with-significant-control-statements/x"}
        )
        == "statement"
    )
    assert psc_stream.psc_kind_of({}) == "unknown"
    assert set(psc.PSC_KIND_WORDS) == set(psc.PSC_KIND_PLURALS) == set(psc_stream.PSC_KINDS)
    assert set(psc.PSC_OUTCOME_WORDS) == set(psc_stream.PSC_OUTCOMES)


def test_apply_events_says_what_each_event_did_without_a_name(store_path: Path) -> None:  # noqa: F811
    import sqlite3

    conn = sqlite3.connect(store_path)
    conn.row_factory = sqlite3.Row
    applied = psc_stream.apply_events(
        conn,
        [
            _event("00000001", "p1", timepoint=1),  # new
            _event("00000001", "p1", timepoint=2, name="Renamed Owner"),  # an edit of the same item
            _event("00000001", "p1", timepoint=3, ceased_on="2026-09-09"),
            _event("00000002", "p9", timepoint=4, ev_type="deleted"),
            {
                "resource_uri": "/company/00000003/x",
                "event": {"type": "changed"},
            },  # unkeyed: skipped
        ],
    )
    conn.close()
    assert [(t.company_number, t.psc_id, t.outcome, t.kind) for t in applied.touched] == [
        ("00000001", "p1", "added", "individual"),
        ("00000001", "p1", "changed", "individual"),
        ("00000001", "p1", "ceased", "individual"),
        ("00000002", "p9", "deleted", "individual"),
    ]
    assert applied.skipped == 1
    assert not any(hasattr(t, "name") for t in applied.touched)


# ---------------------------------------------------------------------------
# Which watches the stream reaches
# ---------------------------------------------------------------------------


def test_a_watchs_number_comes_from_gleif_then_the_baseline() -> None:
    for ra in ("RA000585", "RA000586", "RA000587"):
        assert (
            psc.watch_ch_number({"gleif_facts": {"registered_at": ra, "registered_as": "2999029"}})
            == EASY_NUMBER
        )
    assert (
        psc.watch_ch_number(
            {"gleif_facts": {"registered_at": "RA000585", "registered_as": "SC12345"}}
        )
        == "SC012345"
    )
    assert (
        psc.watch_ch_number(
            {
                "gleif_facts": {"registered_at": "RA000181", "registered_as": "10421629"},
                "snapshot": {"identifiers": {"GB-COH": "00102498"}},
            }
        )
        == "00102498"
    )
    assert (
        psc.watch_ch_number(
            {"gleif_facts": {"registered_at": "RA000181", "registered_as": "10421629"}}
        )
        is None
    )
    assert psc.watch_ch_number({}) is None


def test_the_watch_row_says_which_register_feed_follows_it() -> None:
    uk_row = {"gleif_facts": {"registered_at": "RA000585", "registered_as": "2999029"}}
    assert psc.register_tier_of(uk_row) == {
        "followed": True,
        "register": "companies_house",
        "reason": None,
        "words": "Companies House PSC stream, live",
    }
    ee_row = {"gleif_facts": {"registered_at": "RA000181", "registered_as": "10421629"}}
    assert psc.register_tier_of(ee_row)["register"] == "ariregister"
    no_feed = psc.register_tier_of(
        {"gleif_facts": {"registered_at": "RA000472", "registered_as": "923609016"}}
    )
    assert no_feed == {
        "followed": False,
        "register": None,
        "reason": "register_not_covered",
        "words": psc.REGISTER_NOT_FOLLOWED_WORDS["register_not_covered"],
    }
    no_number = psc.register_tier_of(
        {"gleif_facts": {"registered_at": "RA999999", "registered_as": None}}
    )
    assert no_number["reason"] == "no_register_number"


def test_the_index_is_rebuilt_when_the_watches_change(uk: dict) -> None:
    store = uk["store"]
    assert psc.watched_ch_numbers(store) == {EASY_NUMBER: [EASY]}
    # A second list watching the same company: the stamp (count) moves, so
    # the index is re-read even inside the TTL.
    th2 = wl.token_hash("second")
    with store._conn() as conn:
        conn.execute(
            "INSERT INTO lists (token_hash, created_at, last_seen_at) VALUES (?, ?, ?)",
            (th2, "2026-10-19T00:00:00Z", "2026-10-19T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO watches (token_hash, lei, added_at, gleif_facts_json) VALUES (?, ?, ?, ?)",
            (
                th2,
                EASY,
                "2026-10-19T00:00:01Z",
                json.dumps({"registered_at": "RA000585", "registered_as": "2999029"}),
            ),
        )
    assert psc.watched_ch_numbers(store) == {EASY_NUMBER: [EASY]}


# ---------------------------------------------------------------------------
# The hook
# ---------------------------------------------------------------------------


def test_a_filing_on_a_watched_company_queues_one_counts_only_rerun(uk: dict) -> None:
    store = uk["store"]
    out = psc.on_psc_events(
        [
            _touched(EASY_NUMBER, "itemA", "added", "individual"),
            _touched(EASY_NUMBER, "itemB", "ceased", "individual"),
            _touched(EASY_NUMBER, "itemC", "changed", "corporate"),
            _touched("00000009", "other", "added", "individual"),  # not watched
        ],
        "2026-10-19T09:15:00",
    )
    assert out == {"events": 4, "matched": 1, "queued": 1, "coalesced": 0}
    (item,) = store.take_pending(10)
    assert item["lei"] == EASY and item["tier"] == wl.TIER_PSC
    assert item["trigger"] == {
        "tier": "psc",
        "register": "companies_house",
        "company_number": EASY_NUMBER,
        "events": 3,
        "kinds": {"individual": {"added": 1, "ceased": 1}, "corporate": {"changed": 1}},
        "psc_ids": ["itemA", "itemB", "itemC"],
        "first_at": "2026-10-19T09:15:00",
        "last_at": "2026-10-19T09:15:00",
    }
    assert "name" not in json.dumps(item["trigger"]).lower()
    st = wl.state()
    assert st["psc_events_seen"] == 4 and st["psc_matched"] == 1 and st["psc_queued"] == 1


def test_a_second_filing_the_same_day_is_folded_into_the_pending_rerun(uk: dict) -> None:
    store = uk["store"]
    psc.on_psc_events([_touched(EASY_NUMBER, "itemA", "added")], "2026-10-19T09:15:00")
    out = psc.on_psc_events(
        [_touched(EASY_NUMBER, "itemA", "changed"), _touched(EASY_NUMBER, "itemD", "added")],
        "2026-10-19T14:02:00",
    )
    assert out == {"events": 2, "matched": 1, "queued": 0, "coalesced": 1}
    (item,) = store.take_pending(10)
    assert item["trigger"]["events"] == 3
    assert item["trigger"]["kinds"] == {"individual": {"added": 2, "changed": 1}}
    assert item["trigger"]["psc_ids"] == ["itemA", "itemD"]
    assert (
        item["trigger"]["first_at"] == "2026-10-19T09:15:00"
        and item["trigger"]["last_at"] == "2026-10-19T14:02:00"
    )
    assert item["trigger"]["coalesced"] == 1
    assert wl.state()["psc_coalesced"] == 1


def test_a_psc_trigger_replaces_a_pending_catch_up(uk: dict) -> None:
    store = uk["store"]
    assert store.enqueue(EASY, wl.TIER_CATCHUP, {"tier": "catch_up", "reason": "aged_out"})
    psc.on_psc_events([_touched(EASY_NUMBER, "itemA")], None)
    (item,) = store.take_pending(10)
    assert item["tier"] == wl.TIER_PSC


def test_nothing_watched_or_no_store_is_a_quiet_no_op(
    env: Path,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert psc.on_psc_events([_touched("00000001", "p1")], None) == {
        "events": 1,
        "matched": 0,
        "queued": 0,
        "coalesced": 0,
    }
    monkeypatch.setattr(wl, "get_store", lambda: None)
    assert psc.on_psc_events([_touched("00000001", "p1")], None)["queued"] == 0


def test_the_hook_never_raises(uk: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("the store is locked")

    monkeypatch.setattr(psc, "watched_ch_numbers", boom)
    assert psc.on_psc_events([_touched(EASY_NUMBER, "itemA")], None)["queued"] == 0
    assert psc.on_psc_gap("2026-10-19T09:00:00Z")["queued"] == 0


async def test_the_stream_hands_a_committed_batch_to_the_hook(
    store_path: Path, uk: dict, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:  # noqa: F811
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    seen: list[tuple[list[Touched], str | None]] = []
    monkeypatch.setattr(
        psc, "on_psc_events", lambda touched, at=None: seen.append((touched, at)) or {}
    )
    gaps: list[str] = []
    monkeypatch.setattr(psc, "on_psc_gap", lambda since, tp=None: gaps.append(since) or {})
    source = _scripted(
        _lines(_event("00000001", "p1", timepoint=101, published_at="2026-09-08T14:00:00"))
    )
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 1 and bool(seen))
    ((touched, at),) = seen
    assert at == "2026-09-08T14:00:00"
    assert [(t.company_number, t.outcome) for t in touched] == [("00000001", "added")]
    # A fresh seed has no cursor, so the first connection resumed live: a gap.
    assert gaps and psc_stream.state()["gap"] is True


# ---------------------------------------------------------------------------
# Gaps
# ---------------------------------------------------------------------------


def test_a_live_resume_queues_a_catch_up_of_every_watched_uk_company_once(uk: dict) -> None:
    store = uk["store"]
    assert psc.on_psc_gap("2026-10-19T09:00:00Z", 123) == {"queued": 1}
    assert psc.on_psc_gap("2026-10-19T09:00:00Z", 123) == {
        "queued": 0
    }  # the same gap, answered once
    (item,) = store.take_pending(10)
    assert item["tier"] == wl.TIER_CATCHUP
    assert item["trigger"] == {
        "tier": "catch_up",
        "reason": "register_gap",
        "register": "companies_house",
        "after": None,
        "before": "2026-10-19T09:00:00Z",
    }
    (gap,) = psc.psc_gaps(store)
    assert (
        gap["since"] == "2026-10-19T09:00:00Z" and gap["timepoint"] == 123 and gap["watched"] == 1
    )
    assert wl.state()["psc_gaps"] == 1
    assert _entry_content(
        {
            "tier": "catch_up",
            "trigger": item["trigger"],
            "changes": [],
            "checked": [],
            "degraded": [],
            "lei": EASY,
        }
    ).splitlines()[0] == (
        "The Companies House PSC stream was rejoined live at 2026-10-19T09:00:00Z, so filings before that "
        "were not seen; every watched UK company was re-checked."
    )


# ---------------------------------------------------------------------------
# The entry
# ---------------------------------------------------------------------------


async def test_a_psc_rerun_writes_an_entry_even_when_nothing_else_moved(uk: dict) -> None:
    store = uk["store"]
    facts, _watermark = wl._mirror_facts(EASY)
    with store._conn() as conn:
        conn.execute(
            "UPDATE watches SET gleif_facts_json = ? WHERE lei = ?", (json.dumps(facts), EASY)
        )
    trigger = {
        "tier": "psc",
        "register": "companies_house",
        "company_number": EASY_NUMBER,
        "events": 2,
        "kinds": {"individual": {"added": 1, "ceased": 1}},
        "psc_ids": ["itemA", "itemB"],
        "first_at": "2026-10-19T09:15:00",
        "last_at": "2026-10-19T09:15:00",
    }
    result = await wl.rerun(EASY, wl.TIER_PSC, trigger)
    assert result["entries"] == 1 and result["changes"] == []
    th = wl.token_hash(uk["token"])
    (entry,) = store.entries(th)
    assert entry["tier"] == "psc" and entry["trigger"] == trigger
    assert entry["materiality"] == "recheck"
    assert (
        _entry_title(entry)
        == "Recheck the source — EASY POWER: Companies House PSC register changed"
    )
    content = _entry_content(entry)
    assert content.splitlines()[:2] == [
        f"Companies House recorded 2 changes to this company's people with significant control on 2026-10-19 (company number {EASY_NUMBER}).",
        " - 2 persons: 1 added, 1 ceased.",
    ]
    assert "A full re-run found nothing else that differs from the last check." in content


def test_the_trigger_grades_recheck_and_an_empty_one_note() -> None:
    assert materiality.psc_trigger_tier({"kinds": {"corporate": {"changed": 1}}}) == "recheck"
    assert materiality.psc_trigger_tier({"kinds": {}}) == "note"
    assert (
        materiality.tier_for_entry([], tier="psc", trigger={"kinds": {"individual": {"added": 1}}})
        == "recheck"
    )


def test_the_feed_words_every_kind_and_outcome() -> None:
    lines = psc.psc_change_lines(
        {
            "kinds": {
                "individual": {"added": 1, "changed": 2, "ceased": 1, "deleted": 1},
                "corporate": {"added": 1},
                "legal_person": {"ceased": 2},
                "withheld": {"changed": 1},
                "statement": {"added": 1},
                "unknown": {"deleted": 3},
            }
        }
    )
    assert lines == [
        "5 persons: 1 added, 2 changed, 1 ceased, 1 removed.",
        "1 corporate entity: 1 added.",
        "2 legal persons: 2 ceased.",
        "1 person whose details are withheld: 1 changed.",
        "1 PSC statement: 1 added.",
        "3 PSC items: 3 removed.",
    ]


# ---------------------------------------------------------------------------
# The page and the counters
# ---------------------------------------------------------------------------


def test_the_payload_carries_the_register_tier_line_and_the_psc_block(
    uk: dict, client: TestClient  # noqa: F811
) -> None:  # noqa: F811
    r = client.get(f"/watch/{uk['token']}")
    assert r.status_code == 200
    body = r.json()
    (watch,) = body["watches"]
    assert watch["register_tier"] == {
        "followed": True,
        "register": "companies_house",
        "reason": None,
        "words": "Companies House PSC stream, live",
    }
    assert set(body["tiers"]["psc"]) == {
        "available",
        "connected",
        "last_published_at",
        "gap",
        "matched",
        "queued",
        "last_match_at",
        "gaps",
    }
    assert body["tiers"]["psc"]["available"] is False  # no stream in the suite


def test_watchstats_counts_watched_leis_by_registration_authority(
    uk: dict, client: TestClient  # noqa: F811
) -> None:  # noqa: F811
    r = client.get("/watchstats")
    assert r.status_code == 200
    body = r.json()
    assert body["watched"]["watched_by_registration_authority"] == {"RA000585": 1}
    for key in ("psc_events_seen", "psc_matched", "psc_queued", "psc_coalesced", "psc_gaps"):
        assert key in body["watcher"]
    assert EASY not in json.dumps(body)


def test_the_page_carries_the_same_words() -> None:
    text = (REPO / "frontend/src/lib/watchlist.ts").read_text()

    def table(name: str) -> dict[str, str]:
        body = text[text.index(f"const {name}") :]
        body = body[body.index("{") + 1 : body.index("};")]
        return dict(re.findall(r'(\w+):\s*"([^"]*)"', body))

    assert table("PSC_KIND_WORDS") == psc.PSC_KIND_WORDS
    assert table("PSC_KIND_PLURALS") == psc.PSC_KIND_PLURALS
    assert table("PSC_OUTCOME_WORDS") == psc.PSC_OUTCOME_WORDS
    assert table("REGISTER_NOT_FOLLOWED_WORDS") == psc.REGISTER_NOT_FOLLOWED_WORDS
    assert table("REGISTER_FOLLOWED_WORDS") == psc.REGISTER_FOLLOWED_WORDS
    assert '"psc"' in text[text.index("export type Tier") : text.index("export type Tier") + 200]


def test_docs_name_the_tier() -> None:
    doc = (REPO / "docs/watchlist.md").read_text()
    assert "Companies House PSC stream" in doc
    assert re.search(r"Register changes: not\s+followed", doc)
