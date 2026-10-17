"""pKYC stage B — what the watcher expected (``opencheck/watchlist_context.py``).

Stephen's decisions on the stage B ticket (17 Oct 2026): jurisdiction and one
parent LEI are the only expectations; a baseline that already disagrees is
said on the watch row and writes no entry; a contradicted expectation leaves
the materiality tier alone; the rationale is one line about the company with
e-mail, foreign links and personal-code-shaped digit runs refused; a pinned
report must be of the same LEI on this instance; a met expectation is said
("as your watch expected"); the entry is the record of the day and is never
rewritten when the context changes.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import saved_reports as sr
from opencheck import watchlist as wl
from opencheck import watchlist_context as ctx
from opencheck import watchlist_materiality as materiality
from opencheck.config import get_settings
from opencheck.routers.lookup import LookupResponse
from opencheck.routers.watch import render_atom
from tests.test_gleif_mirror_store import EASY, TOP
from tests.test_watchlist import _resp, _watch, client, env  # noqa: F401 - fixtures

ATOM = "{http://www.w3.org/2005/Atom}"
OTHER = "5493001KJTIIGC8Y1R12"  # a valid LEI that is not EASY's parent
FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "watchlist.ts"
DOC = Path(__file__).resolve().parents[2] / "docs" / "watchlist.md"


# ---- validation --------------------------------------------------------------


def test_jurisdiction_is_an_iso_code_upper_cased() -> None:
    assert ctx.validate_jurisdiction(" gb ") == "GB"
    assert ctx.validate_jurisdiction("us-de") == "US-DE"
    for bad in ("", "GBR", "XX", "US-ZZ", "Q159", 7):
        with pytest.raises(ctx.ContextError) as e:
            ctx.validate_jurisdiction(bad)
        assert e.value.field == "expected_jurisdiction"


def test_parent_is_an_lei_by_shape_and_check_digits() -> None:
    assert ctx.validate_parent_lei(f" {TOP.lower()} ") == TOP
    for bad in ("2138000000000000T17", "not-an-lei", "", None):
        with pytest.raises(ctx.ContextError) as e:
            ctx.validate_parent_lei(bad)
        assert e.value.field == "expected_parent_lei"


@pytest.mark.parametrize(
    "text",
    [
        "x" * (ctx.RATIONALE_MAX + 1),
        "",
        "   ",
        "Supplier contact is jane@example.com",
        "See https://example.com/dossier",
        "Owner's code 38001085718",
        "Call 020 7946 0958 ext 12345",  # eleven digits with spaces
    ],
)
def test_rationale_refuses_personal_data_shapes_and_length(text: str) -> None:
    with pytest.raises(ctx.ContextError) as e:
        ctx.validate_rationale(text)
    assert e.value.field == "rationale"


def test_rationale_is_one_line_and_keeps_an_opencheck_report_link() -> None:
    assert ctx.validate_rationale("Key supplier\nsince 2019.  Onboarded by procurement.") == (
        "Key supplier since 2019. Onboarded by procurement."
    )
    kept = "Onboarding record: https://opencheck.world/report/AbCdEfGhIjKlMnOpQrStUv."
    assert ctx.validate_rationale(kept) == kept
    # Ten digits are a company number, not a personal code.
    assert ctx.validate_rationale("CVR 1234567890 on file") == "CVR 1234567890 on file"


def test_unknown_field_and_clearing(env: Path) -> None:  # noqa: F811
    with pytest.raises(ctx.ContextError):
        ctx.validate({"colour": "red"}, EASY, report_store=None)
    assert ctx.validate({"rationale": None, "expected_jurisdiction": None}, EASY, report_store=None) == {
        "rationale": None,
        "expected_jurisdiction": None,
    }


def _saved_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lei: str) -> str:
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_DB_FILE", str(tmp_path / "saved.sqlite"))
    get_settings.cache_clear()
    sr.reset_for_tests()
    store = sr.get_store()
    assert store is not None
    rid = sr.new_report_id()
    data = sr.canonical_bytes({"schema": sr.SCHEMA, "lei": lei, "events": []})
    store.insert(
        report_id=rid, lei=lei, legal_name="EASY POWER", data=data,
        saved_at=datetime(2026, 10, 12, 9, 0, tzinfo=UTC), manage_token="m" * 32,
    )
    return rid


def test_a_pinned_report_must_exist_be_unexpired_and_name_the_same_lei(
    env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    with pytest.raises(ctx.ContextError) as e:
        ctx.validate_report("AbCdEfGhIjKlMnOpQrStUv", EASY, store=None)
    assert "not enabled" in e.value.message
    rid = _saved_report(tmp_path, monkeypatch, OTHER)
    store = sr.get_store()
    with pytest.raises(ctx.ContextError) as e:
        ctx.validate_report(rid, EASY, store=store)
    assert OTHER in e.value.message
    with pytest.raises(ctx.ContextError):
        ctx.validate_report("nope", EASY, store=store)
    with pytest.raises(ctx.ContextError):
        ctx.validate_report(sr.new_report_id(), EASY, store=store)
    mine = _saved_report(tmp_path, monkeypatch, EASY)
    assert ctx.validate_report(mine, EASY, store=sr.get_store())["report_id"] == mine


# ---- the comparison ----------------------------------------------------------


def _row(**context: str) -> dict:
    return {"context": context, "gleif_facts": {"jurisdiction": "GR", "ultimate_parent_lei": TOP}, "jurisdiction": "GR"}


def test_an_ultimate_parent_move_is_compared_both_ways() -> None:
    row = _row(expected_parent_lei=TOP)
    away = {"kind": "gleif_field", "field": "ultimate_parent_lei", "old": TOP, "new": OTHER}
    back = {"kind": "gleif_field", "field": "ultimate_parent_lei", "old": OTHER, "new": TOP}
    [a, b] = ctx.annotate([away, back], row)
    assert a["expectation"] == {"field": "ultimate_parent_lei", "expected": TOP, "met": False}
    assert b["expectation"] == {"field": "ultimate_parent_lei", "expected": TOP, "met": True}
    assert "expectation" not in away  # copies, never the caller's dicts


def test_a_direct_parent_is_compared_only_when_the_expected_lei_is_on_either_side() -> None:
    row = _row(expected_parent_lei=TOP)
    named = {"kind": "gleif_field", "field": "direct_parent_lei", "old": TOP, "new": OTHER}
    unrelated = {"kind": "gleif_field", "field": "direct_parent_lei", "old": OTHER, "new": None}
    [a, b] = ctx.annotate([named, unrelated], row)
    assert a["expectation"]["met"] is False
    assert "expectation" not in b


def test_jurisdiction_from_either_diff_and_a_country_is_met_by_its_subdivisions() -> None:
    row = _row(expected_jurisdiction="US")
    gleif = {"kind": "gleif_field", "field": "jurisdiction", "old": "US-DE", "new": "US-NY"}
    snap = {"kind": "jurisdiction", "old": "US", "new": "KY"}
    [a, b] = ctx.annotate([gleif, snap], row)
    assert a["expectation"] == {"field": "jurisdiction", "expected": "US", "met": True}
    assert b["expectation"] == {"field": "jurisdiction", "expected": "US", "met": False}
    assert ctx.jurisdiction_met("US", "US-DE") is False  # a subdivision expectation is exact


def test_a_change_with_nothing_to_compare_carries_no_key() -> None:
    row = _row(expected_jurisdiction="GR", expected_parent_lei=TOP)
    changes = [
        {"kind": "legal_name", "old": "A", "new": "B"},
        {"kind": "signal_new", "code": "SANCTIONED"},
        {"kind": "gleif_field", "field": "legal_form", "old": "x", "new": "y"},
    ]
    assert all("expectation" not in c for c in ctx.annotate(changes, row))
    assert ctx.annotate(changes, {"context": {}}) == changes
    assert ctx.contradicted(changes) is False


def test_baseline_disagrees_reads_the_facts_and_never_a_missing_value() -> None:
    assert ctx.baseline_disagrees(_row(expected_jurisdiction="GB", expected_parent_lei=OTHER)) == [
        {"field": "jurisdiction", "expected": "GB", "actual": "GR"},
        {"field": "ultimate_parent_lei", "expected": OTHER, "actual": TOP},
    ]
    assert ctx.baseline_disagrees(_row(expected_jurisdiction="GR", expected_parent_lei=TOP)) == []
    bare = {"context": {"expected_parent_lei": OTHER}, "gleif_facts": {"ultimate_parent_lei": None}}
    assert ctx.baseline_disagrees(bare) == []  # GLEIF names no parent: not a disagreement
    assert ctx.has_context(bare) and not ctx.has_context({"context": {"rationale": None}})


def test_the_feed_clause() -> None:
    assert ctx.expectation_clause(None) == ""
    assert ctx.expectation_clause({"met": True}) == " — as your watch expected."
    assert ctx.expectation_clause({"met": False, "expected": TOP}) == f" — your watch expected {TOP}."


# ---- the store and the routes ------------------------------------------------


def test_migration_4_adds_the_columns_and_a_pre_b_row_says_nothing(env: Path) -> None:  # noqa: F811
    store = wl.get_store()
    assert store is not None
    assert store.schema_version == 4
    assert [m.version for m in wl.MIGRATIONS] == [1, 2, 3, 4]
    assert wl.MIGRATIONS[3].steps[0].startswith("ALTER TABLE watches ADD COLUMN expected_jurisdiction")


def test_patch_sets_clears_and_leaves_alone(client: TestClient, env: Path) -> None:  # noqa: F811
    data = _watch(client, EASY)
    token = data["token"]
    assert data["watch"]["context"]["set_at"] is None
    r = client.patch(f"/watch/{token}/items/{EASY}", json={"expected_jurisdiction": "gb", "rationale": "Key supplier."})
    assert r.status_code == 200, r.text
    w = r.json()["watch"]
    assert w["context"]["expected_jurisdiction"] == "GB"
    assert w["context"]["rationale"] == "Key supplier."
    assert w["context"]["expected_parent_lei"] is None and w["context"]["set_at"]
    # The baseline is Greek: said at once on the watch row, with no entry.
    assert w["context"]["baseline_disagrees"] == [{"field": "jurisdiction", "expected": "GB", "actual": "GR"}]
    assert r.json()["entries"] == []
    # A field left out is left alone; null clears.
    r = client.patch(f"/watch/{token}/items/{EASY}", json={"expected_parent_lei": TOP.lower()})
    w = r.json()["watch"]
    assert w["context"]["expected_jurisdiction"] == "GB" and w["context"]["expected_parent_lei"] == TOP
    assert w["context"]["expected_parent_name"]  # resolved from the mirror on read
    assert w["context"]["baseline_disagrees"] == [{"field": "jurisdiction", "expected": "GB", "actual": "GR"}]
    r = client.patch(f"/watch/{token}/items/{EASY}", json={"expected_jurisdiction": None})
    assert r.json()["watch"]["context"]["expected_jurisdiction"] is None
    assert r.json()["watch"]["context"]["baseline_disagrees"] == []
    # The list payload carries the same block.
    page = client.get(f"/watch/{token}").json()
    assert page["watches"][0]["context"]["rationale"] == "Key supplier."


def test_patch_refuses_naming_the_field(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    r = client.patch(f"/watch/{token}/items/{EASY}", json={"rationale": "mail me at a@b.co"})
    assert r.status_code == 422 and r.json()["detail"]["field"] == "rationale"
    r = client.patch(f"/watch/{token}/items/{EASY}", json={"expected_parent_lei": "nope"})
    assert r.status_code == 422 and r.json()["detail"]["field"] == "expected_parent_lei"
    r = client.patch(f"/watch/{token}/items/{EASY}", json={"colour": "red"})
    assert r.status_code == 422
    r = client.patch(f"/watch/{token}/items/{OTHER}", json={"rationale": "x"})
    assert r.status_code == 404
    assert client.patch(f"/watch/nope/items/{EASY}", json={"rationale": "x"}).status_code == 404
    # Nothing stuck.
    assert client.get(f"/watch/{token}").json()["watches"][0]["context"]["rationale"] is None


def test_the_add_call_pins_a_saved_report_of_the_same_lei(
    client: TestClient, env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    rid = _saved_report(tmp_path, monkeypatch, EASY)
    r = client.post("/watch/items", json={"lei": EASY, "context": {"onboarding_report_id": rid}})
    assert r.status_code == 201, r.text
    c = r.json()["watch"]["context"]
    assert c["onboarding_report_id"] == rid
    assert c["onboarding_report"]["available"] is True
    assert c["onboarding_report"]["saved_at"].startswith("2026-10-12")
    assert c["onboarding_report"]["report_path"] == f"/report/{rid}"
    # A report of another company is refused and the watch is still added.
    other = _saved_report(tmp_path, monkeypatch, OTHER)
    r = client.patch(f"/watch/{r.json()['token']}/items/{EASY}", json={"onboarding_report_id": other})
    assert r.status_code == 422 and r.json()["detail"]["field"] == "onboarding_report_id"


def test_context_dies_with_the_watch(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    client.patch(f"/watch/{token}/items/{EASY}", json={"rationale": "Key supplier."})
    client.delete(f"/watch/{token}/items/{EASY}")
    data = _watch(client, EASY, token)
    assert data["watch"]["context"]["rationale"] is None


@pytest.mark.asyncio
async def test_a_rerun_compares_with_the_context_and_keeps_the_record(
    client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    token = _watch(client, EASY)["token"]
    th = wl.token_hash(token)
    store = wl.get_store()
    assert store is not None
    client.patch(f"/watch/{token}/items/{EASY}", json={"expected_parent_lei": TOP, "expected_jurisdiction": "GR"})

    async def _moved(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        return _resp(lei, jurisdiction="KY")

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _moved)
    # GLEIF's ultimate parent moved too: feed the facts a re-run would read.
    facts = dict(store.get_watch(th, EASY)["gleif_facts"])
    facts["ultimate_parent_lei"] = OTHER

    monkeypatch.setattr(wl, "_mirror_facts", lambda lei: (facts, "2026-10-17 00:00:00"))
    result = await wl.rerun(EASY, wl.TIER_MANUAL, {"tier": "manual"}, only_token_hash=th)
    assert result["entries"] == 1
    entry = store.entries(th)[0]
    by = {(c["kind"], c.get("field")): c for c in entry["changes"]}
    j = by[("jurisdiction", None)]
    assert j["expectation"] == {"field": "jurisdiction", "expected": "GR", "met": False}
    assert j["materiality"] == "escalate"  # the tier is the table's, unchanged by the contradiction
    parent = by[("gleif_field", "ultimate_parent_lei")]
    assert parent["expectation"] == {"field": "ultimate_parent_lei", "expected": TOP, "met": False}
    assert parent["materiality"] == "escalate"
    assert ctx.contradicted(entry["changes"])
    # Editing the context afterwards does not touch the entry.
    client.patch(f"/watch/{token}/items/{EASY}", json={"expected_jurisdiction": "KY"})
    again = store.entries(th)[0]
    assert again["changes"] == entry["changes"]
    # The feed says it, as a clause and as a category.
    xml = render_atom(
        feed_id="urn:x", self_url="https://api/x.atom", page_url="https://app/x",
        title="t", watches=store.watches(th), entries=store.entries(th),
    )
    assert "Jurisdiction: GR → KY — your watch expected GR. [escalate]" in xml
    assert f'<category scheme="{ctx.ATOM_SCHEME}" term="contradicted"' in xml
    import xml.etree.ElementTree as ET

    ET.fromstring(xml)
    stats = client.get("/watchstats").json()
    assert stats["watched"]["watches_with_context"] == 1
    assert stats["watched"]["expectations_contradicted"] == 1
    assert "KY" not in json.dumps(stats) and EASY not in json.dumps(stats)


def test_watchstats_counts_are_zero_without_context(client: TestClient, env: Path) -> None:  # noqa: F811
    _watch(client, EASY)
    stats = client.get("/watchstats").json()["watched"]
    assert stats["watches_with_context"] == 0 and stats["expectations_contradicted"] == 0


def test_a_contradiction_never_moves_the_materiality_tier() -> None:
    change = {"kind": "gleif_field", "field": "legal_form", "old": "a", "new": "b"}
    graded = materiality.tier_for_change(change)
    annotated = ctx.annotate([{**change, "expectation": {"field": "x", "expected": "y", "met": False}}], {"context": {}})[0]
    assert materiality.tier_for_change(annotated) == graded


# ---- the frontend and the doc carry the same words ----------------------------


def test_frontend_mirrors_the_expectation_words_and_fields() -> None:
    ts = FRONTEND.read_text(encoding="utf-8")
    block = re.search(r"export const EXPECTATION_WORDS[^{]*\{(.*?)\};", ts, re.S)
    assert block, "lib/watchlist.ts must export EXPECTATION_WORDS"
    words = dict(re.findall(r'(\w+):\s*"([^"]*)"', block.group(1)))
    assert words == ctx.EXPECTATION_WORDS
    for field in ctx.CONTEXT_FIELDS:
        assert re.search(rf"\b{field}\??:", ts), field
    assert re.search(rf"RATIONALE_MAX\s*=\s*{ctx.RATIONALE_MAX}\b", ts)


def test_the_doc_states_the_fields_and_the_cap() -> None:
    doc = DOC.read_text(encoding="utf-8")
    section = doc.split("## What the watcher expected", 1)
    assert len(section) == 2, "docs/watchlist.md needs the stage B section"
    body = section[1]
    for field in ctx.CONTEXT_FIELDS:
        assert f"`{field}`" in body, field
    assert str(ctx.RATIONALE_MAX) in body
    assert ctx.EXPECTATION_WORDS["met"] in body
