"""Phase 208 — the OECD-UNSD MEIP register as a BODS source.

The store is exercised against a small synthetic register built by the real
``scripts/build_meip.py`` (so the packing and the reading are tested as one
thing), plus the committed JSON fallback the Subsidiaries tab uses when the
SQLite asset is not on disk. The real 66 MB file is gitignored; the tests
that need it skip cleanly when it is absent.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from opencheck import meip as meip_mod
from opencheck.bods import map_meip
from opencheck.bods.validator import validate_shape
from opencheck.config import get_settings
from opencheck.findings import finding_meip
from opencheck.meip import meip_declared, reload_store, store
from opencheck.risk import assess_bundle
from opencheck.sources import REGISTRY

from tests.meip_fixture import HEAD_A, NOT_IN, SUB_A1, build, statements


@pytest.fixture
def fixture_statements() -> list[dict]:
    return statements()


@pytest.fixture
def fixture_db(tmp_path: Path, monkeypatch) -> Path:
    """A tiny register packed by the real build script, wired in as the store."""
    out, _, meta = build(tmp_path)
    assert meta["entities"] == "5" and meta["relationships"] == "3"
    monkeypatch.setenv("OPENCHECK_MEIP_DB_FILE", str(out))
    get_settings.cache_clear()
    reload_store()
    yield out
    reload_store()
    get_settings.cache_clear()


@pytest.fixture
def no_db(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("OPENCHECK_MEIP_DB_FILE", str(tmp_path / "absent.sqlite"))
    get_settings.cache_clear()
    reload_store()
    yield
    reload_store()
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------


def test_store_loads_and_covers(fixture_db) -> None:
    st = store()
    assert st.available and st.edition == "2024-12-31"
    assert st.covers(SUB_A1) and st.covers(HEAD_A.lower())
    assert not st.covers(NOT_IN) and not st.covers(None)
    assert st.group_size("meip-entity-1") == (2, 1)


def test_statements_pass_through_verbatim(fixture_db, fixture_statements) -> None:
    """What comes out of the store is byte-for-byte what the OECD published."""
    st = store()
    by_id = {s["statementId"]: s for s in fixture_statements}
    for rid in ("meip-entity-1", "meip-entity-2", "meip-entity-5"):
        got = st.entity_statement(rid)
        assert got == by_id[got["statementId"]], rid
    rels = st.relationships_for_subject("meip-entity-2")
    assert len(rels) == 1 and rels[0] == by_id[rels[0]["statementId"]]
    # The one annotation in the file rides through.
    assert st.entity_statement("meip-entity-5")["annotations"][0]["motivation"] == "commenting"


def test_bundle_for_a_member_carries_edge_and_head(fixture_db) -> None:
    raw = store().bundle_for_lei(SUB_A1)
    assert raw["source_id"] == "meip" and raw["edition"] == "2024-12-31"
    # Listed in two groups: both memberships, both edges, both heads.
    assert [r["group"]["name"] for r in raw["records"]] == ["ALPHA GROUP PLC", "BETA SA"]
    assert [r["hierarchy"] for r in raw["records"]] == ["Known", "Unknown"]
    assert raw["records"][0]["immediate_parent"] == "ALPHA GROUP PLC"
    assert raw["records"][0]["immediate_parent_record_id"] == "meip-entity-1"
    kinds = sorted(s["recordType"] for s in raw["bods_statements"])
    assert kinds == ["entity"] * 4 + ["relationship"] * 2
    assert validate_shape(raw["bods_statements"]) == []
    # Never the head's children.
    assert "meip-entity-3" not in {s["recordId"] for s in raw["bods_statements"]}


def test_bundle_for_a_head_is_its_own_statement_and_counts(fixture_db) -> None:
    raw = store().bundle_for_lei(HEAD_A)
    assert [r["mode"] for r in raw["records"]] == ["mne_head"]
    assert raw["records"][0]["subsidiaries_total"] == 2
    assert raw["records"][0]["subsidiaries_with_lei"] == 1
    assert [s["recordId"] for s in raw["bods_statements"]] == ["meip-entity-1"]


def test_bundle_none_outside_register(fixture_db) -> None:
    assert store().bundle_for_lei(NOT_IN) is None


# ---------------------------------------------------------------------------
# The adapter and the mapper
# ---------------------------------------------------------------------------


async def test_adapter_fetches_declares_snapshot_and_maps_through(fixture_db) -> None:
    from opencheck import provenance

    adapter = REGISTRY["meip"]
    assert adapter.covers_lei(SUB_A1) and not adapter.covers_lei(NOT_IN)
    with provenance.recording() as rec:
        raw = await adapter.fetch_by_lei(SUB_A1)
    assert rec.resolve().liveness == "snapshot"
    assert raw["lei"] == SUB_A1
    statements = list(map_meip(raw))
    assert statements == raw["bods_statements"], "the mapper is a passthrough"
    # deepen / retry path
    again = await adapter.fetch(SUB_A1)
    assert again["bods_statements"] == raw["bods_statements"]
    stub = await adapter.fetch(NOT_IN)
    assert stub["is_stub"] is True
    assert await adapter.fetch_by_lei(NOT_IN) is None


def test_adapter_info_reads_the_store(fixture_db) -> None:
    info = REGISTRY["meip"].info
    assert info.license == "OECD-Terms"
    assert info.live_available is True
    assert not info.is_national_register
    # Fixed text, never the store's edition — see the next test.
    assert "31 December 2024" in info.description


def test_without_the_file_the_source_covers_nothing(no_db) -> None:
    adapter = REGISTRY["meip"]
    assert not store().available
    assert not adapter.covers_lei("21380068P1DRHMJ8KU70")
    assert adapter.info.live_available is False


def test_description_is_the_same_with_and_without_the_file(fixture_db, monkeypatch) -> None:
    """The OKF drift check compares ``info.description`` with the committed
    concept in CI, where no store is on disk — the first Phase 208 push
    failed that job because the description read the edition from the file.
    Only ``live_available`` may depend on the file."""
    adapter = REGISTRY["meip"]
    with_file = adapter.info
    assert with_file.live_available is True
    monkeypatch.setenv("OPENCHECK_MEIP_DB_FILE", str(fixture_db.parent / "absent.sqlite"))
    get_settings.cache_clear()
    reload_store()
    without = adapter.info
    assert without.live_available is False
    assert without.description == with_file.description
    assert "31 December 2024" in without.description


# ---------------------------------------------------------------------------
# The finding sentence (rule 10: one exact string + the degraded form)
# ---------------------------------------------------------------------------


def test_finding_member_names_the_group_never_owned_by(fixture_db) -> None:
    raw = store().bundle_for_lei(SUB_A1)
    sentence = finding_meip(raw)
    assert sentence == (
        "Listed in 2 groups: ALPHA GROUP PLC and BETA SA, hierarchy known, "
        "register of 31 December 2024."
    )
    assert not re.search(r"\bown(s|ed|ership)?\b", sentence.lower())


def test_finding_single_membership_and_head(fixture_db) -> None:
    # A head record: the register's own subsidiary count, no hierarchy clause.
    raw = store().bundle_for_lei(HEAD_A)
    assert finding_meip(raw) == (
        "One of the 500 largest multinational enterprise groups, with 2 subsidiaries "
        "listed, register of 31 December 2024."
    )


def test_finding_degrades_on_null_fixture() -> None:
    assert finding_meip(None) is None
    assert finding_meip({"is_stub": True}) is None
    assert finding_meip({"records": []}) is None
    # No edition, no hierarchy: the membership clause alone.
    assert finding_meip({"records": [{"mode": "subsidiary", "group": {"name": "Gamma"}}]}) == (
        "Listed in the Gamma group."
    )


# ---------------------------------------------------------------------------
# Risk: the layer count never reads MEIP edges; jurisdictions still do
# ---------------------------------------------------------------------------


def test_meip_edges_do_not_count_as_ownership_layers(fixture_db) -> None:
    raw = store().bundle_for_lei(SUB_A1)
    signals = assess_bundle("meip", raw, raw["bods_statements"], hit_id=SUB_A1)
    codes = {s.code for s in signals}
    assert "COMPLEX_OWNERSHIP_LAYERS" not in codes
    assert "COMPLEX_CORPORATE_STRUCTURE" not in codes


def test_meip_entities_feed_jurisdiction_signals(fixture_db, monkeypatch) -> None:
    import opencheck.risk as risk

    # Put the Beta head's jurisdiction (FR) on the FATF grey list for the test:
    # the OECD's entity statement is a jurisdiction fact like any source's.
    monkeypatch.setattr(risk, "FATF_GREY_LIST_CODES", frozenset({"FR"}))
    raw = store().bundle_for_lei(SUB_A1)
    signals = assess_bundle("meip", raw, raw["bods_statements"], hit_id=SUB_A1)
    assert "FATF_GREY_LIST" in {s.code for s in signals}


# ---------------------------------------------------------------------------
# The Subsidiaries tab's list
# ---------------------------------------------------------------------------


def test_declared_from_store_lists_the_whole_group(fixture_db) -> None:
    decl = meip_declared(HEAD_A)
    assert decl["mode"] == "mne_head" and decl["complete"] is True
    assert decl["total"] == 2 and decl["with_lei"] == 1
    names = [r["name"] for r in decl["rows"]]
    assert names == ["ALPHA ONE LTD", "Alpha Two GmbH"]
    assert decl["rows"][0]["direct"] is True and decl["rows"][0]["lei"] == SUB_A1
    # The no-LEI row is listed, with the spreadsheet's immediate parent.
    assert decl["rows"][1]["lei"] is None and decl["rows"][1]["immediate_parent"] == "ALPHA ONE LTD"
    assert decl["rows"][1]["direct"] is False
    # A subsidiary lists its own direct children, resolved from the spreadsheet.
    sub = meip_declared(SUB_A1)
    assert sub["mode"] == "subsidiary" and sub["memberships"] == 2
    assert [r["name"] for r in sub["rows"]] == ["Alpha Two GmbH"]
    assert sub["total"] is None
    assert meip_declared(NOT_IN) is None and meip_declared(None) is None


def test_declared_falls_back_to_the_committed_subset(no_db) -> None:
    shell = "21380068P1DRHMJ8KU70"
    decl = meip_declared(shell)
    assert decl is not None and decl["mode"] == "mne_head"
    assert decl["complete"] is False
    assert decl["total"] > len(decl["rows"]), "the fallback says how much it is not showing"
    assert all(r["lei"] for r in decl["rows"]), "the fallback holds only LEI-carrying rows"
    # The register's own LEI count includes LEIs it lists twice; the fallback
    # table is keyed by LEI, so it can only hold each once.
    assert decl["with_lei"] >= len(decl["rows"])


# ---------------------------------------------------------------------------
# The real release asset, when it is on disk
# ---------------------------------------------------------------------------


@pytest.fixture
def real_db(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "data" / "meip.sqlite"
    if not path.exists():
        pytest.skip("data/meip.sqlite (the meip-bods-2024 release asset) is not on disk")
    monkeypatch.setenv("OPENCHECK_MEIP_DB_FILE", str(path))
    get_settings.cache_clear()
    reload_store()
    yield path
    reload_store()
    get_settings.cache_clear()


def test_real_register_matches_the_review_figures(real_db) -> None:
    st = store()
    assert st.meta["entities"] == "126658" and st.meta["relationships"] == "126158"
    assert st.meta["groups"] == "500"
    shell = "21380068P1DRHMJ8KU70"
    raw = st.bundle_for_lei(shell)
    assert raw["records"][0]["mode"] == "mne_head"
    assert raw["records"][0]["subsidiaries_total"] == 1865
    assert validate_shape(raw["bods_statements"]) == []
    # AXA Banque Financement: listed under AXA and under BNP Paribas (§5.1 of the review).
    axa = st.bundle_for_lei("969500YSZDXP9YY91J31")
    assert sorted(r["group"]["name"] for r in axa["records"]) == ["AXA", "BNP PARIBAS"]
    # VINCI: the register's head record is VINCI CONSTRUCTION GRANDS PROJETS
    # (§5.3 of the review) and VINCI SAS sits among its members without an
    # LEI. Shown as published — the head looks up as a head.
    vinci = st.bundle_for_lei("2138003N4C1X1IS7BN65")
    assert vinci["records"][0]["mode"] == "mne_head"
    assert vinci["records"][0]["name"] == "VINCI CONSTRUCTION GRANDS PROJETS"
    assert vinci["records"][0]["subsidiaries_total"] == 4357
    members = {e.name for e in st.group_children(vinci["records"][0]["record_id"])}
    assert "VINCI SAS" in members
    sentence = finding_meip(vinci)
    assert sentence.startswith("One of the 500 largest multinational enterprise groups")
    assert not re.search(r"\bown(s|ed|ership)?\b", sentence.lower())


def test_real_register_statements_validate(real_db) -> None:
    """libcovebods over the re-published statements of one real lookup."""
    pytest.importorskip("libcovebods")
    raw = store().bundle_for_lei("213800F4ETX85XLF5K47")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    path = real_db.parent / "_meip_test_bundle.json"
    path.write_text(json.dumps(raw["bods_statements"]), encoding="utf-8")
    try:
        reader = DataReader(str(path))
        errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
        assert errors == [], [e.json()["message"] for e in errors]
    finally:
        path.unlink(missing_ok=True)
    assert meip_mod.GROUP_COUNT == 500
