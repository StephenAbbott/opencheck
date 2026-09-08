"""Phase 185 — the declared-subsidiaries aggregate behind the Subsidiaries tab.

Covers the three per-source helpers, the aggregate's honesty flags
(``available`` vs ``covered`` vs an empty list), and the endpoint's LEI
validation. No network: GEM is stubbed at the index level, MEIP and EITI read
the committed data.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

import opencheck.sources.climatetrace as ct
from opencheck.meip import MEIP_MNE_HEADS, MEIP_SUBSIDIARIES, meip_declared
from opencheck.routers.subsidiaries import (
    DeclaredSubsidiariesResponse,
)
from opencheck.routers.subsidiaries import (
    declared_subsidiaries as declared_endpoint,
)
from opencheck.sources.climatetrace import gem_direct_subsidiaries
from opencheck.sources.eiti_assessment import declared_subsidiaries as eiti_declared
from opencheck.subsidiaries_declared import DECLARED_SOURCES, assemble_declared

SHELL = "21380068P1DRHMJ8KU70"  # MEIP head, EITI supporting company


@pytest.fixture
def gem_stub(monkeypatch):
    """A tiny GEM world: the subject owns two entities, one with a certified LEI."""
    lei_idx = {SHELL: "E1", "5493001KJTIIGC8Y1R12": "E2"}
    ent_idx = {
        "E1": {"Full Name": "Shell plc"},
        "E2": {
            "Full Name": "Shell Energy North America",
            "Global Legal Entity Identifier Index": "not found",
            "Headquarters Country": "USA",
        },
        "E3": {
            "Full Name": "Column-LEI Sub",
            "Global Legal Entity Identifier Index": "549300ABCDEFGHIJKLM1; 549300ZZZZZZZZZZZZZ2",
            "Headquarters Country": "NLD",
        },
        "E4": {"Full Name": "No-LEI Sub"},
    }
    rel = {
        "E1": [
            {"entity_id": "E2", "name": "Shell Energy North America", "percent": 100.0},
            {"entity_id": "E3", "name": "Column-LEI Sub", "percent": 50.0},
            {"entity_id": "E4", "name": "No-LEI Sub", "percent": None},
        ]
    }
    monkeypatch.setattr(ct, "_get_indexes", lambda: (lei_idx, ent_idx))
    monkeypatch.setattr(ct, "_get_relationship_indexes", lambda: (rel, {}))
    return lei_idx, ent_idx, rel


# ---------------------------------------------------------------------------
# MEIP
# ---------------------------------------------------------------------------


def test_meip_declared_none_outside_register() -> None:
    assert meip_declared("529900XXXXXXXXXXXX00") is None
    assert meip_declared(None) is None


def test_meip_declared_head_lists_group_with_lei_on_every_row() -> None:
    decl = meip_declared(SHELL)
    assert decl is not None and decl["mode"] == "mne_head"
    head = MEIP_MNE_HEADS[SHELL]
    assert decl["total"] == head["subsidiaries_total"]
    assert decl["rows"], "an MNE head lists its LEI-carrying subsidiaries"
    assert all(len(r["lei"]) == 20 for r in decl["rows"])
    assert SHELL not in {r["lei"] for r in decl["rows"]}, "never lists the subject itself"
    # Every row is in the same MNE group as the head.
    group = head["parent_mne"].casefold()
    assert all(MEIP_SUBSIDIARIES[r["lei"]]["parent_mne"].casefold() == group for r in decl["rows"])
    # Direct children (immediate parent = the head) sort first.
    flags = [r["direct"] for r in decl["rows"]]
    assert flags == sorted(flags, reverse=True)


def test_meip_declared_subsidiary_mode_lists_only_direct_children() -> None:
    # Find a subsidiary that is itself an immediate parent of another row.
    by_parent: dict[str, list[str]] = {}
    for lei, sub in MEIP_SUBSIDIARIES.items():
        ip = (sub.get("immediate_parent") or "").strip().casefold()
        if ip:
            by_parent.setdefault((sub["parent_mne"].casefold(), ip), []).append(lei)
    parent_lei = next(
        lei
        for lei, sub in MEIP_SUBSIDIARIES.items()
        if (sub["parent_mne"].casefold(), sub["name"].strip().casefold()) in by_parent
    )
    decl = meip_declared(parent_lei)
    assert decl is not None and decl["mode"] == "subsidiary"
    assert decl["total"] is None, "MEIP publishes no count for a subsidiary"
    assert decl["rows"] and all(r["direct"] for r in decl["rows"])
    assert parent_lei not in {r["lei"] for r in decl["rows"]}


# ---------------------------------------------------------------------------
# EITI
# ---------------------------------------------------------------------------


def test_eiti_declared_none_outside_snapshot() -> None:
    assert eiti_declared("529900XXXXXXXXXXXX00") is None


def test_eiti_declared_rows_carry_no_identifier() -> None:
    decl = eiti_declared(SHELL)
    assert decl is not None
    assert decl["rows"], "Shell declares a list"
    assert decl["assessment_year"]
    for row in decl["rows"]:
        assert set(row) == {"name", "country", "years"}, "no identifier field, by design"
        assert row["name"]


# ---------------------------------------------------------------------------
# GEM
# ---------------------------------------------------------------------------


def test_gem_direct_subsidiaries_none_when_not_a_gem_entity(gem_stub) -> None:
    assert gem_direct_subsidiaries("529900XXXXXXXXXXXX00") is None


def test_gem_direct_subsidiaries_unavailable_when_no_data(monkeypatch) -> None:
    monkeypatch.setattr(ct, "_get_indexes", lambda: ({}, {}))
    assert gem_direct_subsidiaries(SHELL) == {"available": False, "rows": []}


def test_gem_direct_subsidiaries_resolves_lei_from_index_then_column(gem_stub) -> None:
    decl = gem_direct_subsidiaries(SHELL)
    assert decl is not None and decl["available"] and decl["gem_id"] == "E1"
    by_id = {r["gem_id"]: r for r in decl["rows"]}
    # Certified/index mapping wins over the column ("not found").
    assert by_id["E2"]["lei"] == "5493001KJTIIGC8Y1R12"
    assert by_id["E2"]["country"] == "USA"
    # GEM's own column, first valid token of a semicolon list.
    assert by_id["E3"]["lei"] == "549300ABCDEFGHIJKLM1"
    # No LEI anywhere → None, never invented.
    assert by_id["E4"]["lei"] is None
    # Largest holding first, unknown percentages last.
    assert [r["gem_id"] for r in decl["rows"]] == ["E2", "E3", "E4"]


# ---------------------------------------------------------------------------
# The aggregate
# ---------------------------------------------------------------------------


def test_assemble_declared_keeps_sources_apart(gem_stub) -> None:
    res = asyncio.run(assemble_declared(SHELL.lower()))
    assert res["lei"] == SHELL
    assert [s["id"] for s in res["sources"]] == list(DECLARED_SOURCES)
    assert res["covered"] == 3
    meip, eiti, gem = res["sources"]
    assert meip["covered"] and meip["available"] and meip["total"] > meip["listed"]
    assert meip["with_lei"] == meip["listed"], "every MEIP row carries an LEI"
    assert eiti["covered"] and eiti["with_lei"] == 0, "EITI never supplies an LEI"
    assert eiti["total"] == eiti["listed"] == len(eiti["rows"])
    assert gem["covered"] and gem["listed"] == 3 and gem["with_lei"] == 2
    assert res["listed"] == meip["listed"] + eiti["listed"] + gem["listed"]
    assert res["with_lei"] == meip["with_lei"] + gem["with_lei"]
    # Every block carries its own "what this measures" sentence.
    assert all(s["measures"] and s["homepage"] for s in res["sources"])
    # Validates against the response model.
    DeclaredSubsidiariesResponse.model_validate(res)


def test_assemble_declared_not_covered_is_not_a_finding(gem_stub) -> None:
    res = asyncio.run(assemble_declared("529900XXXXXXXXXXXX00"))
    assert res["covered"] == 0 and res["listed"] == 0
    for s in res["sources"]:
        assert s["available"] is True
        assert s["covered"] is False
        assert s["reason"], "a not-covered block says why"
        assert s["rows"] == []


def test_assemble_declared_reports_unreadable_gem_without_blanking_the_rest(monkeypatch) -> None:
    monkeypatch.setattr(ct, "_get_indexes", lambda: ({}, {}))
    res = asyncio.run(assemble_declared(SHELL))
    gem = res["sources"][2]
    assert gem["available"] is False and gem["covered"] is False
    assert "not loaded" in gem["reason"]
    assert res["sources"][0]["covered"] and res["sources"][1]["covered"]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


def _call(lei: str):
    return asyncio.run(declared_endpoint(request=None, response=None, lei=lei))


def test_endpoint_rejects_malformed_lei() -> None:
    with pytest.raises(HTTPException) as exc:
        _call("not-a-lei")
    assert exc.value.status_code == 400


def test_endpoint_returns_the_aggregate(gem_stub) -> None:
    res = _call(SHELL)
    assert res["lei"] == SHELL and res["covered"] == 3
