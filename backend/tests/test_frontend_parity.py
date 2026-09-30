"""Three frontend copies of backend rules, pinned by parsing the TypeScript
(Phase 266) — the ``test_ra_codes.py`` pattern.

Each pair below was "pinned" only by two parallel test files, one per side,
which pass independently while the two copies drift apart: nothing failed if a
seventh register started emitting history and ``HISTORY_SOURCES`` never heard
of it, or if one side learned a new legacy wrapper key. Behaviour cannot be
parsed out of TypeScript, but the tables it runs on can, and so can the few
expressions that decide it (the tier order, the ``<=`` that makes an end date
inclusive). These tests read the frontend file and compare it with the live
backend objects, so a change on either side alone fails here.

* ``frontend/src/lib/historyMode.ts`` ↔ ``opencheck/timeline/`` (Phase 190)
* ``frontend/src/lib/relationshipStatus.ts`` ↔ ``opencheck/bods/lifecycle.py`` (Phase 219)
* ``frontend/src/lib/bodsRefs.ts`` ↔ ``opencheck/bods/refs.py`` (Phase 210)
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from opencheck.bods import lifecycle, refs

_BACKEND = Path(__file__).resolve().parents[1]
_LIB = _BACKEND.parent / "frontend" / "src" / "lib"
_TIMELINE = _BACKEND / "opencheck" / "timeline"

pytestmark = pytest.mark.skipif(
    not _LIB.is_dir(), reason="frontend sources not present (backend-only checkout)"
)


def _ts(name: str) -> str:
    return (_LIB / name).read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"(?m)//.*$", "", src)


def _string_list(src: str, pattern: str) -> list[str]:
    """The string literals inside the first bracket group ``pattern`` opens."""
    m = re.search(pattern, _strip_comments(src), flags=re.S)
    assert m, f"pattern not found: {pattern}"
    return re.findall(r'"([^"]*)"', m.group(1))


# ---------------------------------------------------------------------------
# History: which registers emit change events
# ---------------------------------------------------------------------------

_NOT_EMITTERS = {"__init__", "assemble", "model", "service"}


def _backend_emitters() -> set[str]:
    return {p.stem for p in _TIMELINE.glob("*.py")} - _NOT_EMITTERS


def test_every_timeline_emitter_emits_under_its_own_module_name() -> None:
    """The emitter set below is read from file names, so a module must emit
    under its own name — a ``source_id`` that differs would slip past it."""
    for stem in _backend_emitters():
        src = (_TIMELINE / f"{stem}.py").read_text(encoding="utf-8")
        ids = set(re.findall(r'source_id="([a-z_]+)"', src))
        ids |= set(re.findall(r'^_SOURCE = "([a-z_]+)"', src, flags=re.M))
        assert ids == {stem}, f"timeline/{stem}.py emits {sorted(ids)}"


def test_history_sources_names_every_register_the_backend_emits_for() -> None:
    frontend = _string_list(_ts("historyMode.ts"), r"HISTORY_SOURCES\s*=\s*\[(.*?)\]")
    assert len(frontend) == len(set(frontend)), "duplicate in HISTORY_SOURCES"
    assert set(frontend) == _backend_emitters()


def test_every_history_source_has_a_label_and_a_record_link() -> None:
    src = _strip_comments(_ts("historyMode.ts"))
    labels = re.search(r"HISTORY_SOURCE_LABEL[^=]*=\s*\{(.*?)\};", src, flags=re.S)
    assert labels
    labelled = set(re.findall(r"^\s*([a-z_]+):", labels.group(1), flags=re.M))
    body = re.search(r"export function recordUrl\((.*?)\n\}", src, flags=re.S)
    assert body
    linked = set(re.findall(r'case "([a-z_]+)":', body.group(1)))
    emitters = _backend_emitters()
    assert emitters <= labelled, f"no label for {sorted(emitters - labelled)}"
    assert emitters <= linked, f"no recordUrl case for {sorted(emitters - linked)}"


def test_registry_numbers_only_name_history_sources() -> None:
    """``/history``'s ``registry_numbers`` keys are what ``recordUrl`` reads and
    ``silentRegisters`` names; a key the frontend does not know renders as a
    raw slug with no link."""
    tree = ast.parse((_TIMELINE / "service.py").read_text(encoding="utf-8"))
    keys: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Attribute) and t.attr == "registry_numbers" for t in node.targets)
        ):
            for const in ast.walk(node.value):
                if isinstance(const, ast.Tuple) and len(const.elts) == 2:
                    first = const.elts[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        keys.add(first.value)
    assert keys, "registry_numbers assignment not found in timeline/service.py"
    frontend = set(_string_list(_ts("historyMode.ts"), r"HISTORY_SOURCES\s*=\s*\[(.*?)\]"))
    assert keys <= frontend


# ---------------------------------------------------------------------------
# Ended relationships
# ---------------------------------------------------------------------------


def test_month_names_match() -> None:
    frontend = _string_list(_ts("relationshipStatus.ts"), r"const MONTHS\s*=\s*\[(.*?)\]")
    assert tuple(frontend) == lifecycle._MONTHS


def test_the_date_part_is_read_by_the_same_pattern() -> None:
    src = _strip_comments(_ts("relationshipStatus.ts"))
    m = re.search(r"value\.match\(/(.+?)/\)", src)
    assert m, "isoDay's pattern not found"
    assert m.group(1) == lifecycle._ISO_DAY.pattern


def test_a_record_is_closed_by_the_same_status() -> None:
    src = _strip_comments(_ts("relationshipStatus.ts"))
    assert re.search(r'recordStatus\s*===\s*"closed"', src)
    assert lifecycle.record_closed({"recordStatus": "closed"})
    assert not lifecycle.record_closed({"recordStatus": "updated"})


def test_an_end_date_counts_on_the_day_itself_on_both_sides() -> None:
    """``end <= asOf``: an interest ending today has ended. A ``<`` on one side
    would disagree for one day and pass every other test."""
    src = _strip_comments(_ts("relationshipStatus.ts"))
    body = re.search(r"export function interestEnded\((.*?)\n\}", src, flags=re.S)
    assert body and re.search(r"end\s*<=\s*asOf", body.group(1))
    assert lifecycle.interest_ended({"endDate": "2026-09-30"}, False, "2026-09-30")
    assert not lifecycle.interest_ended({"endDate": "2026-10-01"}, False, "2026-09-30")


# ---------------------------------------------------------------------------
# Party references
# ---------------------------------------------------------------------------


def test_the_same_record_types_are_parties() -> None:
    frontend = _string_list(_ts("bodsRefs.ts"), r"PARTY_TYPES\s*=\s*new Set\(\[(.*?)\]\)")
    assert set(frontend) == set(refs._PARTY_TYPES)


def test_the_same_legacy_wrappers_are_read_in_the_same_order() -> None:
    src = _ts("bodsRefs.ts")
    frontend = _string_list(src, r"export function partyRef.*?for \(const k of \[(.*?)\]\)")
    assert tuple(frontend) == refs._WRAPPED_KEYS


def test_the_tiers_are_merged_in_the_same_order() -> None:
    """statementId > recordId > declarationSubject — the order that stops the
    OECD's head-on-every-statement alias shadowing a real recordId."""
    src = _strip_comments(_ts("bodsRefs.ts"))
    m = re.search(r"new Map\(\[\s*\.\.\.(\w+),\s*\.\.\.(\w+),\s*\.\.\.(\w+)\s*\]\)", src)
    assert m and m.groups() == ("byDecl", "byRid", "bySid"), "tier merge order changed"
    stmts = [
        {"recordType": "entity", "statementId": "head", "recordId": "r-head", "declarationSubject": "r-head"},
        {"recordType": "entity", "statementId": "sub", "recordId": "r-sub", "declarationSubject": "r-head"},
    ]
    resolve = refs.resolver(stmts)
    assert resolve("r-head") == "head" and resolve("r-sub") == "sub"


def test_the_same_id_fallbacks_are_read() -> None:
    src = _strip_comments(_ts("bodsRefs.ts"))
    assert "s.statementId ?? s.statementID" in src
    assert "s.recordType ?? s.statementType" in src
    resolve = refs.resolver([{"statementType": "entityStatement", "statementID": "legacy"}])
    assert resolve("legacy") == "legacy"
