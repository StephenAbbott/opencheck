"""pKYC stage C — when OpenCheck last looked (``opencheck/watchlist_review.py``),
and the stale-list rule's notice-then-delete.

Stephen's decisions on the stage C ticket (17 Oct 2026): one instance-wide
review threshold of 90 days measured from the last full re-run; due-ness is a
state (the feed subtitle counts it, no entry per watch); the stale rule writes
one notice entry at 90 days and deletes 30 days later, the same for every
list; the 404 names both numbers; the new ``review`` tier is a note about the
list, not about a company.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import watchlist as wl
from opencheck import watchlist_materiality as materiality
from opencheck import watchlist_review as review
from opencheck.routers.watch import TIER_SENTENCE, render_atom
from tests.test_gleif_mirror_store import EASY, PARENT
from tests.test_watchlist import _watch, client, env  # noqa: F401 - fixtures

ATOM = "{http://www.w3.org/2005/Atom}"
NOW = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "watchlist.ts"
DOC = Path(__file__).resolve().parents[2] / "docs" / "watchlist.md"


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _watch_row(*, added_days: int, run_days: int | None, checked: list[dict] | None = None, degraded: list[dict] | None = None) -> dict:
    added = NOW - timedelta(days=added_days)
    last = NOW - timedelta(days=run_days) if run_days is not None else added
    return {
        "lei": EASY,
        "added_at": _iso(added),
        "last_checked_at": _iso(last),
        "snapshot": {"checked": checked or [], "degraded_sources": degraded or []},
    }


# ---- the review line ---------------------------------------------------------


def test_a_fresh_run_with_every_source_live() -> None:
    run = NOW - timedelta(days=3)
    row = _watch_row(
        added_days=40, run_days=3,
        checked=[
            {"source_id": "gleif", "liveness": "live", "retrieved_at": _iso(run)},
            {"source_id": "companies_house", "liveness": "live", "retrieved_at": _iso(run - timedelta(hours=2))},
        ],
    )
    r = review.review_of(row, now=NOW, due_days=90)
    assert r["due"] is False and r["last_run_days"] == 3 and r["never_rerun"] is False
    assert r["sources_total"] == 2 and r["sources_from_older_copy"] == [] and r["sources_not_reached"] == []
    assert r["sentence"] == "Last fully re-run 3 days ago; every source answered live."


def test_older_copies_and_a_source_not_reached_are_named_and_due_asks_for_a_recheck() -> None:
    run = NOW - timedelta(days=112)
    row = _watch_row(
        added_days=200, run_days=112,
        checked=[
            {"source_id": "gleif", "liveness": "live", "retrieved_at": _iso(run)},
            {"source_id": "companies_house", "liveness": "cached", "retrieved_at": "2026-06-03T09:00:00Z"},
            {"source_id": "ariregister", "liveness": "snapshot", "retrieved_at": "2026-06-20T00:00:00Z"},
            {"source_id": "wikidata", "liveness": "stub", "retrieved_at": None},
        ],
        degraded=[{"source_id": "kvk", "check": "source_fetch"}, {"source_id": "kvk", "check": "x"}],
    )
    r = review.review_of(row, now=NOW, due_days=90, label=lambda s: {"companies_house": "UK Companies House", "kvk": "KvK"}.get(s, s))
    assert r["due"] is True and r["last_run_days"] == 112
    assert r["sources_total"] == 3  # the stub is not a source that answered
    assert [o["source_id"] for o in r["sources_from_older_copy"]] == ["companies_house", "ariregister"]
    assert r["sources_not_reached"] == ["kvk"]
    assert r["sentence"] == (
        "Last fully re-run 112 days ago; 2 of 3 sources answered from an older copy "
        "(UK Companies House 3 Jun 2026 and ariregister 20 Jun 2026); KvK not reached. Re-check by hand to refresh."
    )


def test_a_watch_never_re_run_says_so_and_asks_to_confirm() -> None:
    row = _watch_row(added_days=112, run_days=None, checked=[{"source_id": "gleif", "liveness": "live", "retrieved_at": "2026-06-28T12:00:00Z"}])
    r = review.review_of(row, now=NOW, due_days=90)
    assert r["never_rerun"] is True and r["due"] is True
    assert r["sentence"] == (
        "Baseline taken 112 days ago and never re-run — no delta has named this company since. Re-check by hand to confirm."
    )


def test_the_due_boundary_and_a_disabled_threshold() -> None:
    assert review.review_of(_watch_row(added_days=100, run_days=89), now=NOW, due_days=90)["due"] is False
    assert review.review_of(_watch_row(added_days=100, run_days=90), now=NOW, due_days=90)["due"] is True
    assert review.review_of(_watch_row(added_days=400, run_days=300), now=NOW, due_days=0)["due"] is False
    today = review.review_of(_watch_row(added_days=100, run_days=0), now=NOW, due_days=90)
    assert today["sentence"].startswith("Last fully re-run today")


def test_list_sentence_and_rule_words() -> None:
    assert review.list_due_sentence(0, 5, 90) is None
    assert review.list_due_sentence(3, 12, 90) == "3 of 12 companies have not been fully re-run in 90 days."
    assert review.stale_rule_sentence(90, 30) == (
        "A list that nobody opens or polls for 90 days is warned in its feed, and deleted 30 days later."
    )


# ---- the stale-list rule -----------------------------------------------------


def _age(store: wl.WatchlistStore, token: str, *, seen_days: int, notice_days: int | None = None) -> None:
    with store._conn() as conn:
        conn.execute(
            "UPDATE lists SET last_seen_at = ?, expiry_notice_at = ? WHERE token_hash = ?",
            (
                _iso(NOW - timedelta(days=seen_days)),
                _iso(NOW - timedelta(days=notice_days)) if notice_days is not None else None,
                wl.token_hash(token),
            ),
        )


def test_notice_then_grace_then_delete(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    keep = _watch(client, PARENT)["token"]
    store = wl.get_store()
    assert store is not None
    th = wl.token_hash(token)
    _age(store, token, seen_days=91)
    # Day 91: one notice, nothing deleted.
    assert store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW) == {"noticed": 1, "deleted": 0}
    entries = store.entries(th)
    assert len(entries) == 1
    note = entries[0]
    assert note["tier"] == "review" and note["lei"] == "" and note["changes"] == []
    assert note["trigger"]["reason"] == "list_expiring"
    assert note["trigger"]["delete_after"].startswith("2026-11-17")
    assert note["materiality"] == "note" and note["materiality_rules"] == materiality.MATERIALITY_RULES
    # The same tick again writes no second notice, and the grace has not run.
    assert store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW + timedelta(days=10)) == {"noticed": 0, "deleted": 0}
    # Day 121: deleted, with its watches and the log; the other list untouched.
    assert store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW + timedelta(days=31)) == {"noticed": 0, "deleted": 1}
    assert store.entries(th) == [] and store.watched_leis() == {PARENT}
    assert client.get(f"/watch/{keep}").status_code == 200
    r = client.get(f"/watch/{token}")
    assert r.status_code == 404
    assert r.json()["detail"].endswith(review.stale_rule_sentence(90, 30))
    assert store.review_counts(due_days=90, now=NOW)["lists_deleted_total"] == 1


def test_a_visit_during_the_grace_withdraws_the_notice_and_keeps_the_log(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    th = wl.token_hash(token)
    _age(store, token, seen_days=95)
    store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW)
    assert store.review_counts(due_days=90, now=NOW)["lists_expiring"] == 1
    page = client.get(f"/watch/{token}")  # the owner comes back
    assert page.status_code == 200
    assert page.json()["entries"][0]["tier"] == "review"  # the warning is in the log
    assert store.review_counts(due_days=90, now=NOW)["lists_expiring"] == 0
    # Nothing is deleted on the day the grace would have run.
    assert store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW + timedelta(days=31)) == {"noticed": 0, "deleted": 0}
    assert store.watched_leis() == {EASY}
    # The feed renders the notice as an entry about the list, linked to the page.
    r = client.get(f"/watch/{token}.atom")
    root = ET.fromstring(r.text)
    entry = root.find(f"{ATOM}entry")
    assert entry is not None
    assert entry.findtext(f"{ATOM}title") == "This list: nobody has opened it for a while — it will be deleted unless opened"
    assert "90 days" in (entry.findtext(f"{ATOM}content") or "")
    assert "/watchlist?token=" in entry.find(f"{ATOM}link").get("href")  # type: ignore[union-attr]


def test_an_empty_list_gets_no_notice(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    client.delete(f"/watch/{token}/items/{EASY}")
    _age(store, token, seen_days=100)
    assert store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW) == {"noticed": 0, "deleted": 0}
    assert store.prune_empty_lists(older_than_s=0) == 1


def test_the_patch_route_keeps_a_list_alive(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    _age(store, token, seen_days=95, notice_days=2)
    assert client.patch(f"/watch/{token}/items/{EASY}", json={"rationale": "Key supplier."}).status_code == 200
    with store._conn() as conn:
        row = conn.execute("SELECT last_seen_at, expiry_notice_at FROM lists").fetchone()
    assert row[0] > _iso(NOW - timedelta(days=1)) and row[1] is None


# ---- the page, the feed and the stats -----------------------------------------


def test_the_payload_carries_the_review_and_the_feed_subtitle_counts_it(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    with store._conn() as conn:
        conn.execute("UPDATE watches SET last_checked_at = ?, snapshot_at = ?", (_iso(NOW - timedelta(days=200)),) * 2)
    page = client.get(f"/watch/{token}").json()
    rv = page["watches"][0]["review"]
    assert rv["due"] is True and rv["last_run_days"] >= 199 and rv["due_days"] == 90
    assert rv["sentence"].startswith("Last fully re-run")
    assert page["review"] == {
        "due": 1, "total": 1, "due_days": 90,
        "sentence": "1 of 1 companies have not been fully re-run in 90 days.",
    }
    root = ET.fromstring(client.get(f"/watch/{token}.atom").text)
    assert root.findtext(f"{ATOM}subtitle", "").startswith("1 watched company, 1 due a review.")
    stats = client.get("/watchstats").json()["watched"]
    assert stats["watches_review_due"] == 1 and stats["lists_expiring"] == 0 and stats["lists_deleted_total"] == 0
    assert EASY not in json.dumps(stats)


def test_a_fresh_list_is_not_due_and_the_subtitle_says_nothing_of_it(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    page = client.get(f"/watch/{token}").json()
    assert page["review"]["due"] == 0 and page["review"]["sentence"] is None
    assert page["watches"][0]["review"]["due"] is False
    root = ET.fromstring(client.get(f"/watch/{token}.atom").text)
    assert "due a review" not in root.findtext(f"{ATOM}subtitle", "")


def test_the_review_tier_is_in_the_vocabulary_and_grades_note() -> None:
    assert wl.TIER_REVIEW in wl.TIERS and wl.TIER_REVIEW in TIER_SENTENCE
    assert materiality.tier_for_entry([{"kind": "jurisdiction"}], tier="review", trigger={}) == "note"
    xml = render_atom(
        feed_id="urn:x", self_url="https://api/x.atom", page_url="https://app/watchlist?token=t", title="t",
        watches=[], entries=[], review_due=2,
    )
    ET.fromstring(xml)
    assert "0 watched companies, 2 due a review." in xml


# ---- the frontend and the doc carry the same words ----------------------------


def test_frontend_mirrors_the_review_words_and_the_tier() -> None:
    ts = FRONTEND.read_text(encoding="utf-8")
    block = re.search(r"export const REVIEW_WORDS[^{]*\{(.*?)\};", ts, re.S)
    assert block, "lib/watchlist.ts must export REVIEW_WORDS"
    words = dict(re.findall(r'(\w+):\s*"([^"]*)"', block.group(1)))
    assert words == review.REVIEW_WORDS
    tier_type = re.search(r"export type Tier = ([^;]+);", ts)
    assert tier_type and set(re.findall(r'"(\w+)"', tier_type.group(1))) == set(wl.TIERS)


def test_the_doc_states_the_rule_and_the_section() -> None:
    doc = DOC.read_text(encoding="utf-8")
    assert "## Periodic review: when OpenCheck last looked" in doc
    assert review.stale_rule_sentence(90, 30) in " ".join(doc.split())  # the doc wraps lines
    assert "OPENCHECK_WATCHLIST_REVIEW_DAYS" in doc and "OPENCHECK_WATCHLIST_DELETE_GRACE_DAYS" in doc
