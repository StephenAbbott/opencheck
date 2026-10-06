"""Phase 292 — cross-source search ranking.

The fixture reproduces the shape of ``opencheck_search("Metastar Invest")``
on 5 Oct 2026: fuzzy ``abr_australia`` and ``brreg`` rows arrive first because
those adapters come first in the registry, and the exact Companies House
match arrived at position 21.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from opencheck import search_rank
from opencheck.mcp import shaping
from opencheck.routers import search as search_router
from opencheck.sources import SearchKind, SourceHit


def _hit(source: str, hit_id: str, name: str, summary: str, **ids: str) -> SourceHit:
    return SourceHit(
        source_id=source,
        hit_id=hit_id,
        kind=SearchKind.ENTITY,
        name=name,
        summary=summary,
        identifiers=ids,
        raw={},
        is_stub=False,
    )


def _metastar_fixture() -> list[SourceHit]:
    """Registry order: abr_australia, brreg, companies_house, gleif."""
    hits = [
        _hit("abr_australia", f"5100000000{i}", n, f"AU-ABN 5100000000{i} · NSW 2000",
             au_abn=f"5100000000{i}")
        for i, n in enumerate(
            ["M GROW INVEST", "METRO INVEST PTY LTD", "MEGASTAR INVESTMENTS", "STAR INVEST"]
        )
    ]
    hits += [
        _hit("brreg", "923000001", ":-) INVEST AS", "NO-ORGNR 923000001 · AS · Oslo",
             no_orgnr="923000001"),
        _hit("brreg", "923000002", "METASTAR AS", "NO-ORGNR 923000002 · AS · Bergen",
             no_orgnr="923000002"),
        _hit("brreg", "923000003", "INVEST METASTAR AS",
             "NO-ORGNR 923000003 · AS · Oslo · in liquidation", no_orgnr="923000003"),
    ]
    hits += [
        _hit("companies_house", "OC999999", "METASTAR INVEST 2 LLP",
             "Company OC999999 · dissolved · London", gb_coh="OC999999"),
        _hit("companies_house", "OC346224", "METASTAR INVEST LLP",
             "Company OC346224 · active · London", gb_coh="OC346224"),
    ]
    hits += [
        _hit("gleif", "2549000METASTAR0001", "METASTAR INVESTMENTS LIMITED",
             "LEI 2549000METASTAR0001 · GB · active", lei="2549000METASTAR0001"),
    ]
    return hits


def test_metastar_exact_match_ranks_first_across_sources() -> None:
    ranked = search_rank.rank_hits("Metastar Invest", _metastar_fixture())
    assert ranked[0].name == "METASTAR INVEST LLP"


def test_rank_order_is_pinned_for_a_fixed_fixture() -> None:
    """The whole order, across four sources. A change to the ladder or to a
    tie-break moves a row and fails here."""
    ranked = search_rank.rank_hits("Metastar Invest", _metastar_fixture())
    assert [(h.source_id, h.name) for h in ranked] == [
        # same_name (legal form differs from the query's none)
        ("companies_house", "METASTAR INVEST LLP"),
        # all_tokens — live before in a terminal process before dissolved
        ("brreg", "INVEST METASTAR AS"),
        ("companies_house", "METASTAR INVEST 2 LLP"),
        # fuzzy — an LEI row first, then by whole-name similarity
        ("gleif", "METASTAR INVESTMENTS LIMITED"),
        ("abr_australia", "STAR INVEST"),
        ("abr_australia", "MEGASTAR INVESTMENTS"),
        ("brreg", "METASTAR AS"),
        ("abr_australia", "M GROW INVEST"),
        ("abr_australia", "METRO INVEST PTY LTD"),
        ("brreg", ":-) INVEST AS"),
    ]


def test_bentcard_import_puts_both_the_register_row_and_the_lei_row_on_top() -> None:
    hits = [
        _hit("abr_australia", "1", "BENT IMPORTS", "AU-ABN 1", au_abn="1"),
        _hit("brreg", "2", "IMPORT CARD AS", "NO-ORGNR 2", no_orgnr="2"),
        _hit("abr_australia", "3", "BENTLEY CARD IMPORTERS", "AU-ABN 3", au_abn="3"),
        _hit("companies_house", "09999999", "BENTCARD IMPORT LTD",
             "Company 09999999 · active", gb_coh="09999999"),
        _hit("gleif", "984500BENTCARD00001", "BENTCARD IMPORT LIMITED",
             "LEI 984500BENTCARD00001 · GB · active", lei="984500BENTCARD00001",
             gb_coh="09999999"),
    ]
    top3 = {h.source_id for h in search_rank.rank_hits("Bentcard Import", hits)[:3]}
    assert {"companies_house", "gleif"} <= top3


def test_exact_beats_same_name_and_lei_breaks_a_tie() -> None:
    hits = [
        _hit("companies_house", "1", "ACME TRADING LIMITED", "Company 1 · active", gb_coh="1"),
        _hit("brreg", "2", "ACME TRADING", "NO-ORGNR 2", no_orgnr="2"),
        _hit("gleif", "L", "ACME TRADING LTD", "LEI L · GB · active", lei="L"),
    ]
    ranked = search_rank.rank_hits("Acme Trading Ltd", hits)
    # "Ltd" ≡ "Limited" under org_comparable_name: both exact; the LEI row wins the tie.
    assert [h.source_id for h in ranked] == ["gleif", "companies_house", "brreg"]


@pytest.mark.parametrize(
    ("query", "name", "tier"),
    [
        ("Metastar Invest", "METASTAR INVEST LLP", "same_name"),
        ("Unilever PLC", "Unilever Public Limited Company", "exact"),
        ("Bentcard Import", "BENTCARD IMPORT AND EXPORT LIMITED", "all_tokens"),
        ("Gazprom Neft", "GAZPRM NEFT OOO", "distinctive_tokens"),
        # Query-directed, unlike the symmetric Phase 120 gate.
        ("Metastar Invest", ":-) INVEST AS", "fuzzy"),
        # Numeric discriminators must match exactly.
        ("Hornsea 1", "HORNSEA 2 LIMITED", "fuzzy"),
    ],
)
def test_match_tier_ladder(query: str, name: str, tier: str) -> None:
    assert search_rank.match_label(query, name) == tier


@pytest.mark.parametrize(
    ("summary", "rank"),
    [
        ("Company 1 · active · London", 0),
        ("NO-ORGNR 2 · AS · Oslo", 0),  # unstated ranks with live
        ("NO-ORGNR 2 · AS · Oslo · in liquidation", 1),
        ("Company 1 · liquidation", 1),
        ("Company 1 · dissolved", 2),
        ("LEI X · GB · inactive", 2),
        ("Company 1 · converted-closed", 2),
    ],
)
def test_status_rank_reads_whole_summary_segments(summary: str, rank: int) -> None:
    assert search_rank.status_rank(summary) == rank


def test_stubs_sink_to_the_bottom() -> None:
    stub = _hit("gleif", "S", "METASTAR INVEST LLP", "stub")
    stub = stub.model_copy(update={"is_stub": True})
    live = _hit("brreg", "1", "SOMETHING ELSE AS", "NO-ORGNR 1", no_orgnr="1")
    assert search_rank.rank_hits("Metastar Invest", [stub, live])[-1] is stub


def test_person_search_ranks_name_order_invariant_matches_first() -> None:
    def _p(source: str, name: str) -> SourceHit:
        return SourceHit(source_id=source, hit_id=name, kind=SearchKind.PERSON,
                         name=name, summary="", identifiers={}, raw={}, is_stub=False)

    hits = [_p("opensanctions", "John Smithson"), _p("companies_house", "SMITH, John")]
    ranked = search_rank.rank_hits("John Smith", hits, person=True)
    assert ranked[0].name == "SMITH, John"


async def test_search_impl_returns_hits_ranked_across_sources(monkeypatch) -> None:
    fixture = _metastar_fixture()

    async def _fake_run(q, kind):
        grouped: dict[str, list[SourceHit]] = {}
        for h in fixture:
            grouped.setdefault(h.source_id, []).append(h)
        return grouped, {}

    monkeypatch.setattr(search_router, "_run_adapters", _fake_run)
    resp = await search_router._search_impl("Metastar Invest", SearchKind.ENTITY)
    assert resp.hits[0].name == "METASTAR INVEST LLP"
    assert len(resp.hits) == len(fixture)  # ranked, never dropped


def test_shape_search_caps_and_says_so() -> None:
    ranked = search_rank.rank_hits("Metastar Invest", _metastar_fixture())
    payload = SimpleNamespace(query="Metastar Invest", kind=SearchKind.ENTITY, hits=ranked)
    out = shaping.shape_search(payload, limit=3)
    assert out["count"] == 3 and out["total"] == 10 and out["truncated"] is True
    assert "top 3 of 10" in out["hint"]
    first = out["candidates"][0]
    assert first["name"] == "METASTAR INVEST LLP"
    assert first["match"] == "same_name"
    assert first["lei"] is None
    assert first["identifiers"] == [{"scheme": "GB-COH", "id": "OC346224"}]

    full = shaping.shape_search(payload)
    assert full["count"] == 10 and full["truncated"] is False
    assert "top" not in full["hint"]


def test_shape_search_limit_is_clamped() -> None:
    payload = SimpleNamespace(query="x", kind=SearchKind.ENTITY, hits=_metastar_fixture())
    assert shaping.shape_search(payload, limit=0)["count"] == 1
    assert shaping.shape_search(payload, limit=10_000)["count"] == 10
