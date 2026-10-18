"""pKYC stage A — the materiality tier on a watchlist entry.

The three tables in ``watchlist_materiality`` are a closed vocabulary graded
once at write time. These tests pin that every kind, field and code the
watchlist can name has a row (or a rule), that the rules Stephen decided on
17 Oct 2026 hold, that the frontend's words and ``docs/watchlist.md`` say the
same thing, and that an entry written before grading reads as graded today.
"""

from __future__ import annotations

import importlib
import json
import re
import sqlite3
from pathlib import Path

import pytest

from opencheck import consistency, watchlist as wl
from opencheck import watchlist_materiality as m
from opencheck.bods import gleif_events
from opencheck.routers import watch as rw

REPO = Path(__file__).resolve().parents[2]
_TS = REPO / "frontend" / "src" / "lib" / "watchlist.ts"
_DOC = REPO / "docs" / "watchlist.md"

# The enumeration ``test_signal_label_coverage`` uses, plus the consistency
# observation that module list does not cover.
_CODE_MODULES = ("opencheck.risk", "opencheck.cross_check", "opencheck.securities")


def _backend_codes() -> set[str]:
    codes: set[str] = set()
    for name in _CODE_MODULES:
        mod = importlib.import_module(name)
        codes |= {n for n, v in vars(mod).items() if n.isupper() and isinstance(v, str) and v == n}
    return codes | {consistency.RECORD_CONSISTENCY}


# ---------------------------------------------------------------------------
# Coverage: the tables name everything the diff can
# ---------------------------------------------------------------------------


def test_every_change_kind_is_graded() -> None:
    graded_by_rule = {"gleif_field", "register_status", "signal_new"}
    for kind in wl.CHANGE_KINDS:
        assert kind in m.KIND_TIER or kind in graded_by_rule, kind
        assert m.tier_for_change({"kind": kind}) in m.TIERS
    assert not set(m.KIND_TIER) - set(wl.CHANGE_KINDS), "KIND_TIER names a kind the diff cannot produce"


def test_gleif_field_table_matches_the_feeds_field_words() -> None:
    assert set(m.GLEIF_FIELD_TIER) == set(rw.FIELD_WORDS)


def test_every_emittable_signal_code_has_a_tier_and_no_phantom_rows() -> None:
    codes = _backend_codes()
    assert codes <= set(m.SIGNAL_TIER), sorted(codes - set(m.SIGNAL_TIER))
    assert set(m.SIGNAL_TIER) <= codes, sorted(set(m.SIGNAL_TIER) - codes)


def test_register_trigger_table_names_the_registers_lists_fields_and_flags() -> None:
    assert set(m.REGISTER_TRIGGER_TIER) == set(wl.ARIREGISTER_LISTS) | set(wl.ARIREGISTER_FIELDS) | set(
        wl.ARIREGISTER_FLAGS
    )


def test_every_table_value_is_a_tier() -> None:
    for table in (m.KIND_TIER, m.GLEIF_FIELD_TIER, m.SIGNAL_TIER, m.REGISTER_TRIGGER_TIER):
        assert set(table.values()) <= set(m.TIERS)
    assert set(m.TIER_LABELS) == set(m.TIERS) == set(m.TIER_SENTENCES)


def test_event_recheck_types_are_disjoint_from_terminal_and_excluded() -> None:
    assert not m.EVENT_RECHECK_TYPES & gleif_events.TERMINAL_TYPES
    assert not m.EVENT_RECHECK_TYPES & gleif_events.EXCLUDED_TYPES


# ---------------------------------------------------------------------------
# The decisions of 17 Oct 2026
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "tier"),
    [
        ({"kind": "signal_new", "code": "SANCTIONED"}, "escalate"),
        ({"kind": "signal_new", "code": "RELATED_PEP"}, "recheck"),
        ({"kind": "signal_new", "code": "OFFSHORE_LEAKS"}, "note"),
        ({"kind": "signal_new", "code": "STATE_CONTROLLED"}, "note"),
        ({"kind": "signal_new", "code": "SANCTIONS_OWNERSHIP_TEST_POSSIBLE"}, "note"),
        ({"kind": "signal_new", "code": "RECORD_CONSISTENCY"}, "note"),
        # a code the table does not know is recheck, never silent
        ({"kind": "signal_new", "code": "A_FUTURE_CODE"}, "recheck"),
        # Q1: an unchecked signal is note only, whatever the code
        ({"kind": "signal_unchecked", "code": "SANCTIONED"}, "note"),
        ({"kind": "signal_retired", "code": "SANCTIONED"}, "note"),
        ({"kind": "context_new", "code": "NON_EU_JURISDICTION"}, "note"),
        ({"kind": "coverage_changed"}, "note"),
        ({"kind": "coverage_unchecked"}, "note"),
        ({"kind": "verdict"}, "note"),
        ({"kind": "founding_date"}, "note"),
        ({"kind": "identifier", "scheme": "GB-COH"}, "recheck"),
        ({"kind": "legal_name"}, "recheck"),
        ({"kind": "legal_form"}, "recheck"),
        # Q5: redomiciliation is escalate
        ({"kind": "jurisdiction"}, "escalate"),
        ({"kind": "dissolution_date"}, "escalate"),
        ({"kind": "register_status", "new": {"liveness": "terminal"}}, "escalate"),
        ({"kind": "register_status", "new": {"liveness": "pending"}}, "recheck"),
        ({"kind": "register_status", "new": {"liveness": "live"}}, "recheck"),
        # a kind the diff starts naming later is recheck, never silent
        ({"kind": "something_new"}, "recheck"),
    ],
)
def test_change_kinds_grade_as_decided(change: dict, tier: str) -> None:
    assert m.tier_for_change(change) == tier


@pytest.mark.parametrize(
    ("field", "new", "tier"),
    [
        ("direct_parent_lei", "X", "escalate"),
        ("ultimate_parent_lei", "X", "escalate"),
        ("successors", "X", "escalate"),
        ("expiration_date", "2026-01-01", "escalate"),
        ("jurisdiction", "LU", "escalate"),
        ("entity_status", "INACTIVE", "escalate"),
        ("entity_status", "ACTIVE", "recheck"),
        ("legal_name", "X", "recheck"),
        ("legal_address_country", "LU", "recheck"),
        ("registered_as", "123", "recheck"),
        ("direct_exception", "NON_PUBLIC", "recheck"),
        # Q6: subsidiaries below the subject are note
        ("direct_children", ["A", "B"], "note"),
        ("registration_status", "LAPSED", "note"),
        ("conformity_flag", "NON_CONFORMING", "note"),
        ("a_field_read_later", "x", "recheck"),
    ],
)
def test_gleif_fields_grade_as_decided(field: str, new, tier: str) -> None:
    assert m.tier_for_change({"kind": "gleif_field", "field": field, "old": None, "new": new}) == tier


@pytest.mark.parametrize(
    ("etype", "status", "tier"),
    [
        ("DISSOLUTION", "COMPLETED", "escalate"),
        ("MERGERS_AND_ACQUISITIONS", "COMPLETED", "escalate"),
        # Q2: in progress is one step lower
        ("DISSOLUTION", "IN_PROGRESS", "recheck"),
        ("CHANGE_LEGAL_NAME", "COMPLETED", "recheck"),
        ("CHANGE_LEGAL_NAME", "IN_PROGRESS", "note"),
        ("VOLUNTARY_ARRANGEMENT", "COMPLETED", "recheck"),
        # withdrawn: the event did not happen
        ("DISSOLUTION", "WITHDRAWN_CANCELLED", "note"),
        # an unknown status reads as in progress, never as completed
        ("DISSOLUTION", "SOMETHING_ELSE", "recheck"),
        ("DISSOLUTION", "", "recheck"),
        # an unknown type is recheck, never silent
        ("A_NEW_TYPE", "COMPLETED", "recheck"),
    ],
)
def test_legal_entity_events_grade_by_type_and_status(etype: str, status: str, tier: str) -> None:
    assert m.event_tier({"type": etype, "status": status}) == tier


def test_a_corporate_events_change_takes_its_worst_event() -> None:
    change = {
        "kind": "gleif_field",
        "field": "corporate_events",
        "old": None,
        "new": [
            {"type": "CHANGE_LEGAL_NAME", "status": "COMPLETED"},
            {"type": "LIQUIDATION", "status": "IN_PROGRESS"},
        ],
    }
    assert m.tier_for_change(change) == "recheck"
    change["new"].append({"type": "DISSOLUTION", "status": "COMPLETED"})
    assert m.tier_for_change(change) == "escalate"
    assert m.tier_for_change({"kind": "gleif_field", "field": "corporate_events", "new": None}) == "note"


def test_register_trigger_grades_as_decided() -> None:
    assert m.register_trigger_tier({"changes": {"shareholders": [1, 2]}}) == "recheck"
    assert m.register_trigger_tier({"changes": {"beneficial_owners": [2, 2]}}) == "recheck"
    assert m.register_trigger_tier({"changes": {"officers": [3, 4]}}) == "note"
    assert m.register_trigger_tier({"changes": {"supervisory_board": [3, 4], "added": True}}) == "note"
    assert m.register_trigger_tier({"changes": {"removed": True}}) == "escalate"
    assert m.register_trigger_tier({"changes": {"status": ["R", "L"]}}) == "recheck"
    assert m.register_trigger_tier({"changes": {}}) == "note"
    assert m.register_trigger_tier(None) == "note"
    # a list the tier starts reading later is recheck, never silent
    assert m.register_trigger_tier({"changes": {"a_new_list": [0, 1]}}) == "recheck"


# ---------------------------------------------------------------------------
# The entry
# ---------------------------------------------------------------------------


def test_the_entry_tier_is_the_worst_of_its_changes() -> None:
    changes = [{"kind": "coverage_changed"}, {"kind": "legal_name"}, {"kind": "signal_new", "code": "SANCTIONED"}]
    assert m.tier_for_entry(changes) == "escalate"
    assert m.tier_for_entry(changes[:2]) == "recheck"
    assert m.tier_for_entry(changes[:1]) == "note"


def test_an_entry_with_no_changes_is_note_unless_the_register_trigger_says_otherwise() -> None:
    assert m.tier_for_entry([]) == "note"
    assert m.tier_for_entry([], tier="opensanctions", trigger={"op": "ADD"}) == "note"
    assert m.tier_for_entry([], tier="register", trigger={"changes": {"shareholders": [1, 2]}}) == "recheck"
    # a non-register entry never reads a register trigger
    assert m.tier_for_entry([], tier="gleif", trigger={"changes": {"removed": True}}) == "note"


def test_grade_copies_the_changes_and_marks_each() -> None:
    changes = [{"kind": "coverage_changed"}, {"kind": "signal_new", "code": "PEP"}]
    tier, graded = m.grade(changes)
    assert tier == "recheck"
    assert [c["materiality"] for c in graded] == ["note", "recheck"]
    assert "materiality" not in changes[0], "the caller's list was mutated"


def test_is_material_is_recheck_and_escalate() -> None:
    assert [m.is_material(t) for t in m.TIERS] == [False, True, True]


# ---------------------------------------------------------------------------
# Stored once, kept; pre-grading rows graded on read
# ---------------------------------------------------------------------------


def test_add_entry_stores_the_tier_and_the_table_version(tmp_path: Path) -> None:
    store = wl.WatchlistStore(tmp_path / "w.sqlite", wl.Caps(10, 10))
    th = "h" * 64
    with sqlite3.connect(store.path) as conn:
        conn.execute("INSERT INTO lists (token_hash, created_at, last_seen_at) VALUES (?, '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')", (th,))
    lei = "5493001KJTIIGC8Y1R12"
    store.add_entry(
        th, lei, tier="gleif", trigger={}, changes=[{"kind": "coverage_changed"}, {"kind": "jurisdiction"}],
        checked=[], degraded=[], legal_name="X",
    )
    entry = store.entries(th)[0]
    assert entry["materiality"] == "escalate"
    assert entry["materiality_rules"] == m.MATERIALITY_RULES
    assert [c["materiality"] for c in entry["changes"]] == ["note", "escalate"]
    with sqlite3.connect(store.path) as conn:
        row = conn.execute("SELECT materiality, materiality_rules FROM entries").fetchone()
    assert row == ("escalate", m.MATERIALITY_RULES)
    assert store.entries_by_materiality() == {"note": 0, "recheck": 0, "escalate": 1}


def test_a_stored_grade_is_kept_even_if_the_table_would_now_say_otherwise(tmp_path: Path, monkeypatch) -> None:
    store = wl.WatchlistStore(tmp_path / "w.sqlite", wl.Caps(10, 10))
    th = "h" * 64
    with sqlite3.connect(store.path) as conn:
        conn.execute("INSERT INTO lists (token_hash, created_at, last_seen_at) VALUES (?, '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')", (th,))
    store.add_entry(th, "5493001KJTIIGC8Y1R12", tier="gleif", trigger={}, changes=[{"kind": "legal_name"}],
                    checked=[], degraded=[], legal_name="X")
    monkeypatch.setitem(m.KIND_TIER, "legal_name", "escalate")
    assert store.entries(th)[0]["materiality"] == "recheck", "graded once, kept"


def test_the_feed_title_prefixes_only_the_two_material_tiers() -> None:
    base = {"lei": "X", "legal_name": "ACME", "tier": "gleif", "changes": [{"kind": "coverage_changed"}]}
    assert rw._entry_title({**base, "materiality": "note"}) == "ACME: 1 change"
    assert rw._entry_title({**base, "materiality": "recheck"}) == "Recheck the source — ACME: 1 change"
    assert rw._entry_title({**base, "materiality": "escalate"}) == "Escalate — ACME: 1 change"
    assert rw._entry_title(base) == "ACME: 1 change"  # no tier at all: unchanged


def test_the_feed_content_names_the_tier_and_flags_a_pre_grading_entry() -> None:
    entry = {
        "lei": "X", "legal_name": "ACME", "tier": "gleif", "trigger": {}, "checked": [], "degraded": [],
        "changes": [{"kind": "legal_name", "old": "A", "new": "B", "materiality": "recheck"}],
        "materiality": "recheck", "materiality_rules": None,
    }
    content = rw._entry_content(entry)
    assert "Materiality: Recheck the source — Something about the company's record or exposure moved; open the report and look. (graded under today's table; the entry predates grading)" in content
    assert "[recheck]" in content
    entry["materiality_rules"] = 1
    assert "predates grading" not in rw._entry_content(entry)


# ---------------------------------------------------------------------------
# The frontend and the doc say the same thing
# ---------------------------------------------------------------------------


def _ts_object(name: str) -> dict[str, str]:
    src = _TS.read_text(encoding="utf-8")
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    src = re.sub(r"(?m)//.*$", "", src)
    mm = re.search(rf"export const {name}[^=]*=\s*\{{(.*?)\}}", src, re.S)
    assert mm, name
    return dict(re.findall(r'(\w+):\s*"([^"]*)"', mm.group(1)))


def test_the_frontend_carries_the_same_tiers_labels_and_sentences() -> None:
    src = _TS.read_text(encoding="utf-8")
    assert 'export type Materiality = "note" | "recheck" | "escalate"' in src
    assert _ts_object("MATERIALITY_LABELS") == m.TIER_LABELS
    assert _ts_object("MATERIALITY_SENTENCES") == m.TIER_SENTENCES


def test_the_doc_states_every_row_of_the_three_tables() -> None:
    doc = _DOC.read_text(encoding="utf-8")
    section = doc.split("## Does it matter? The materiality tier", 1)[1].split("\n## ", 1)[0]
    rows = dict(re.findall(r"^\| `([^`]+)` \| (note|recheck|escalate)", section, flags=re.M))
    for table in (m.KIND_TIER, m.GLEIF_FIELD_TIER, m.SIGNAL_TIER, m.REGISTER_TRIGGER_TIER):
        for key, tier in table.items():
            assert rows.get(key) == tier, f"docs/watchlist.md says {rows.get(key)!r} for {key}, the module {tier!r}"
    assert f"MATERIALITY_RULES` is {m.MATERIALITY_RULES}" in section
