"""Source lineage (Phase 150): which source republishes which, and how that
discounts "N sources agree".

The declarations are data, so most of these tests pin decisions rather than
logic: that OpenCorporates + Companies House is ONE observation, that GLEIF +
OpenSanctions is one, that an undeclared source is independent by default,
and that the frontend copy of the table is the generated one.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.reconcile import reconcile
from opencheck.sources import REGISTRY, SearchKind, SourceHit, lineage

FRONTEND_JSON = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "lineage.json"


# ---------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------


def test_every_declared_upstream_is_a_known_source() -> None:
    """A typo in a ``derived_from`` would silently declare nothing."""
    known = set(REGISTRY) | set(lineage.EXTRA_DERIVED)
    for source_id, upstream in lineage.lineage_table().items():
        assert source_id in known, source_id
        for up in upstream:
            assert up in known, f"{source_id} claims to derive from unknown {up!r}"
            assert up != source_id


def test_no_declaration_cycles() -> None:
    for source_id in lineage.lineage_table():
        assert source_id not in lineage.ancestors(source_id), source_id


def test_national_registers_marker_expands_to_every_register() -> None:
    registers = lineage.national_register_ids()
    assert "companies_house" in registers
    assert "cvr_denmark" in registers
    assert "opencorporates" not in registers
    assert "gleif" not in registers
    # OpenCorporates mirrors all of them, and the marker is expanded — a new
    # register adapter is covered without touching the OpenCorporates class.
    assert registers <= lineage.derived_from("opencorporates")


def test_the_decided_lineage() -> None:
    """The four aggregators, pinned. Change deliberately, with the reason."""
    assert lineage.derived_from("opensanctions") == {"gleif", "companies_house"}
    assert lineage.derived_from("openaleph") == {"companies_house", "gleif", "opensanctions"}
    assert lineage.derived_from("everypolitician") == {"opensanctions"}
    assert lineage.derived_from("bods_gleif") == {"gleif"}
    assert lineage.derived_from("bods_uk_psc") == {"companies_house"}


def test_original_sources_are_independent_by_default() -> None:
    for source_id in ("gleif", "companies_house", "wikidata", "cvr_denmark", "climatetrace"):
        assert lineage.derived_from(source_id) == frozenset()
        assert lineage.ancestors(source_id) == frozenset()
    assert lineage.independent("gleif", "wikidata")
    assert lineage.independent("companies_house", "cvr_denmark")
    # Unknown ids are original too — under-claim, never crash.
    assert lineage.independent("gleif", "some_future_adapter")


# ---------------------------------------------------------------------
# Independence
# ---------------------------------------------------------------------


def test_a_source_is_not_independent_of_itself() -> None:
    assert not lineage.independent("gleif", "gleif")


def test_derivative_is_not_independent_of_its_upstream() -> None:
    assert not lineage.independent("opencorporates", "companies_house")
    assert not lineage.independent("companies_house", "opencorporates")
    assert not lineage.independent("opensanctions", "gleif")
    assert not lineage.independent("everypolitician", "opensanctions")


def test_transitive_lineage_counts() -> None:
    # EveryPolitician → OpenSanctions → GLEIF.
    assert "gleif" in lineage.ancestors("everypolitician")
    assert not lineage.independent("everypolitician", "gleif")


def test_two_mirrors_of_the_same_register_are_not_independent() -> None:
    """OpenCorporates and OpenAleph both mirror Companies House; their
    agreement may be one register read twice, so it is not corroboration."""
    assert not lineage.independent("opencorporates", "openaleph")


@pytest.mark.parametrize(
    ("sources", "expected"),
    [
        # Novo Nordisk A/S, live export 2026-09-02: five sources, three origins.
        (
            ["gleif", "opensanctions", "wikidata", "opencorporates", "cvr_denmark"],
            ["cvr_denmark", "gleif", "wikidata"],
        ),
        # Shell plc, same day: OpenAleph and OpenCorporates both fold into CH.
        (
            ["gleif", "openaleph", "opencorporates", "wikidata", "companies_house"],
            ["companies_house", "gleif", "wikidata"],
        ),
        # The 22 Shell officers: Companies House twice is one observation.
        (["opencorporates", "companies_house"], ["companies_house"]),
        (["gleif", "opensanctions"], ["gleif"]),
        (["opensanctions", "everypolitician"], ["opensanctions"]),
        # Two derivatives, upstream absent: one origin between them.
        (["opencorporates", "openaleph"], ["openaleph"]),
        # A derivative alone still counts as one — it IS an observation.
        (["opencorporates"], ["opencorporates"]),
        ([], []),
        (["", "gleif", "gleif"], ["gleif"]),
    ],
)
def test_independent_sources(sources: list[str], expected: list[str]) -> None:
    assert lineage.independent_sources(sources) == expected
    assert lineage.independent_count(sources) == len(expected)


def test_ancestors_terminates_on_a_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """The declarations test forbids cycles; the walk must still not hang if
    one is ever introduced."""
    monkeypatch.setattr(
        lineage, "lineage_table", lambda: {"a": frozenset({"b"}), "b": frozenset({"a"})}
    )
    assert lineage.ancestors("a") == {"b"}
    assert not lineage.independent("a", "b")


# ---------------------------------------------------------------------
# Where it is consumed
# ---------------------------------------------------------------------


def _hit(source_id: str, **identifiers: str) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=f"{source_id}-1",
        kind=SearchKind.ENTITY,
        name=source_id,
        summary="",
        identifiers=identifiers,
        is_stub=False,
    )


def test_cross_source_link_reports_independent_count_but_still_links() -> None:
    """GLEIF + OpenSanctions sharing an LEI is a real bridge (the panel is
    provenance) but ONE independent origin (anything counting corroboration
    must read this field, not ``len(hits)``)."""
    lei = "213800LBDB8WB3QGVN21"
    links = reconcile([_hit("gleif", lei=lei), _hit("opensanctions", lei=lei)])
    assert len(links) == 1
    assert len(links[0].hits) == 2
    assert links[0].independent_source_count == 1
    assert links[0].to_dict()["independent_source_count"] == 1

    links = reconcile([_hit("gleif", lei=lei), _hit("opensanctions", lei=lei), _hit("wikidata", lei=lei)])
    assert links[0].independent_source_count == 2


def test_sources_endpoint_exposes_derived_from() -> None:
    body = TestClient(app).get("/sources").json()
    by_id = {s["id"]: s for s in body["sources"]}
    assert by_id["opencorporates"]["derived_from"]  # non-empty, sorted
    assert by_id["opencorporates"]["derived_from"] == sorted(by_id["opencorporates"]["derived_from"])
    assert "companies_house" in by_id["opencorporates"]["derived_from"]
    assert by_id["gleif"]["derived_from"] == []
    assert by_id["everypolitician"]["derived_from"] == ["opensanctions"]


def test_frontend_lineage_json_is_generated_and_current() -> None:
    """``frontend/src/lib/lineage.json`` is written by ``scripts/gen_lineage.py``
    from the same table; a stale copy would let the browser count corroboration
    differently from the API."""
    from opencheck.bods.mapper import SOURCE_NAMES

    assert FRONTEND_JSON.exists(), "run: uv run python scripts/gen_lineage.py"
    committed = FRONTEND_JSON.read_text(encoding="utf-8")
    assert committed == lineage.export_json(SOURCE_NAMES), "run scripts/gen_lineage.py"
    data = json.loads(committed)
    # Every derived source has a description so the network can join on it.
    for source_id in data["derived_from"]:
        assert source_id in data["descriptions"], source_id
    for source_id in ("gleif", "companies_house", "opensanctions", "wikidata"):
        assert source_id in data["descriptions"]
