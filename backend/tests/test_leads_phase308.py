"""Phase 308 — leads for a retired LEI that names no successor.

Barrick Gold Inc. (``5493002CWGHR03YL8X75``) is the case: dissolved 26 Nov
2025, LEI retired, no successor named, no parent ever filed. The GLEIF
search shape is the live ``lei-records`` resource as read on 8 Oct 2026.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.testclient import TestClient

from opencheck import leads
from opencheck.app import app
from opencheck.entity_pages import EntityRow, RelationshipRow
from opencheck.gleif_throttle import GleifRateLimitedError
from opencheck.sources import REGISTRY
from opencheck.sources.gleif import GleifAdapter

BARRICK = "5493002CWGHR03YL8X75"
MINING = "0O4KBQCJZX82UKGCBV73"
NAME = "BARRICK GOLD INC."


def _item(
    lei: str,
    name: str,
    status: str = "ACTIVE",
    jurisdiction: str = "CA-BC",
    reg: str = "ISSUED",
    other: list[dict] | None = None,
) -> dict:
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "otherNames": other or [],
                "status": status,
                "jurisdiction": jurisdiction,
            },
            "registration": {"status": reg},
        },
    }


#: Barrick Mining Corporation as GLEIF files it: three former legal names.
MINING_OTHER = [
    {"name": "American Barrick Resources Corporation", "language": "en", "type": "PREVIOUS_LEGAL_NAME"},
    {"name": "Barrick Gold Corporation", "language": "en", "type": "PREVIOUS_LEGAL_NAME"},
    {"name": "SOCIETE MINIERE BARRICK", "language": "en", "type": "TRADING_OR_OPERATING_NAME"},
]


SEARCH = {
    "data": [
        _item(BARRICK, NAME, status="INACTIVE", jurisdiction="CA-ON", reg="RETIRED"),
        _item(MINING, "BARRICK MINING CORPORATION", other=MINING_OTHER),
        _item("549300AAAAAAAAAAAAA1", "BARRICK GOLD CORPORATION"),
        _item("549300EEEEEEEEEEEEE5", "BARRICK INTERNATIONAL HOLDINGS"),
        _item("549300BBBBBBBBBBBBB2", "Barrick Gold Inc."),
        _item("549300CCCCCCCCCCCCC3", "GOLD FIELDS LIMITED", jurisdiction="ZA"),
        _item("549300DDDDDDDDDDDDD4", "BARRICK GOLD INC.", status="INACTIVE"),
    ]
}


# --- ranking (pure) -------------------------------------------------------------------


def test_candidates_are_active_others_ranked_by_tier_and_always_name_only() -> None:
    out = leads.rank_candidates(NAME, SEARCH["data"], exclude=BARRICK)
    assert [c["lei"] for c in out] == [
        "549300BBBBBBBBBBBBB2",  # exact (case folds)
        MINING,  # every word, through its former legal name — GLEIF's order within a tier
        "549300AAAAAAAAAAAAA1",  # every word of the name
    ]
    assert [c["match"] for c in out] == ["exact", "all_tokens", "all_tokens"]
    assert all(c["name_only"] is True for c in out)
    # The subject itself and INACTIVE records never appear; a name sharing
    # one word (BARRICK INTERNATIONAL HOLDINGS, GOLD FIELDS) is fuzzy at best.
    leis = {c["lei"] for c in out}
    for absent in (BARRICK, "549300DDDDDDDDDDDDD4", "549300CCCCCCCCCCCCC3", "549300EEEEEEEEEEEEE5"):
        assert absent not in leis
    # The Barrick case: the renamed company is found through the name it had.
    assert out[1] == {
        "lei": MINING,
        "name": "BARRICK MINING CORPORATION",
        "jurisdiction": "CA-BC",
        "registration_status": "ISSUED",
        "match": "all_tokens",
        "matched_name": "Barrick Gold Corporation",
        "matched_name_type": "PREVIOUS_LEGAL_NAME",
        "name_only": True,
    }
    assert out[0]["matched_name"] is None and out[0]["matched_name_type"] is None


def test_the_list_is_capped_and_deduplicated() -> None:
    items = [_item(f"549300X{i:013d}", NAME) for i in range(8)] + [_item("549300X0000000000000", NAME)]
    out = leads.rank_candidates(NAME, items, exclude=BARRICK)
    assert len(out) == leads.MAX_CANDIDATES
    assert len({c["lei"] for c in out}) == leads.MAX_CANDIDATES


# --- the parent GLEIF last filed ----------------------------------------------------


class _Store:
    def __init__(self, rel: RelationshipRow | None, rows: dict[str, EntityRow]) -> None:
        self.rel, self.rows = rel, rows

    def relationship(self, lei: str, kind: str) -> RelationshipRow | None:
        assert kind == "direct"
        return self.rel

    def get(self, lei: str) -> EntityRow | None:
        return self.rows.get(lei)


def _row(lei: str, name: str, es: str, rs: str) -> EntityRow:
    return EntityRow(
        lei=lei, name=name, slug=name.lower(), entity_status=es, registration_status=rs,
        jurisdiction=None, legal_form=None, city=None, region=None, country=None,
        first_registered=None, last_updated=None, successor_lei=None,
        direct_parent_lei=None, ultimate_parent_lei=None, detail={},
    )


def test_a_retired_relationship_is_still_a_lead_and_says_it_is_retired() -> None:
    rel = RelationshipRow(
        child_lei=BARRICK, relationship_type="IS_DIRECTLY_CONSOLIDATED_BY", parent_lei=MINING,
        relationship_status="INACTIVE", registration_status="RETIRED",
        period_start=None, period_end=None, last_updated=None,
    )
    store = _Store(rel, {MINING: _row(MINING, "BARRICK MINING CORPORATION", "ACTIVE", "ISSUED")})
    assert leads.last_filed_parent(BARRICK, store) == {
        "lei": MINING,
        "name": "BARRICK MINING CORPORATION",
        "entity_status": "ACTIVE",
        "registration_status": "ISSUED",
        "relationship_status": "INACTIVE",
        "standing": False,
    }


def test_no_mirror_or_no_row_is_no_parent() -> None:
    assert leads.last_filed_parent(BARRICK, _Store(None, {})) is None
    import opencheck.entity_pages as ep

    original = ep.get_store
    ep.get_store = lambda: None  # type: ignore[assignment]
    try:
        assert leads.last_filed_parent(BARRICK) is None
    finally:
        ep.get_store = original  # type: ignore[assignment]


# --- the assembled answer -----------------------------------------------------------


@pytest.fixture
def gleif(monkeypatch):
    """Live mode on (``info`` is computed from settings on every read)."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield REGISTRY["gleif"]
    # Restore the environment BEFORE clearing, or the cleared cache is
    # refilled with live mode on and a later test's GLEIF call goes live.
    monkeypatch.undo()
    get_settings.cache_clear()


async def test_assemble_searches_once_and_never_asserts(monkeypatch, gleif) -> None:
    get = AsyncMock(return_value=SEARCH)
    monkeypatch.setattr(GleifAdapter, "_get", get)
    out = await leads.assemble_leads(BARRICK, NAME, store=_Store(None, {}))
    assert out["lei"] == BARRICK and out["searched_name"] == NAME
    assert out["parent"] is None
    assert [c["lei"] for c in out["candidates"]] == ["549300BBBBBBBBBBBBB2", MINING, "549300AAAAAAAAAAAAA1"]
    assert out["gleif_unavailable_reason"] is None
    assert out["note"] == leads.NOTE and "not a successor" in leads.NOTE
    get.assert_awaited_once()
    path = get.await_args.args[0]
    assert "filter[entity.status]=ACTIVE" in path and "BARRICK" in path


@pytest.mark.parametrize(
    "exc, reason",
    [
        (GleifRateLimitedError("reserved", reason="held_for_lookups"), "held_for_lookups"),
        (GleifRateLimitedError("busy", reason="rate_limited"), "rate_limited"),
        (httpx.ConnectError("down"), "unreachable"),
        (ValueError("not json"), "unreachable"),
    ],
)
async def test_a_failed_search_says_why_and_is_never_an_empty_answer(monkeypatch, gleif, exc, reason) -> None:
    monkeypatch.setattr(GleifAdapter, "_get", AsyncMock(side_effect=exc))
    out = await leads.assemble_leads(BARRICK, NAME, store=_Store(None, {}))
    assert out["candidates"] == []
    assert out["gleif_unavailable_reason"] == reason


async def test_no_name_means_no_search(monkeypatch, gleif) -> None:
    get = AsyncMock(return_value=SEARCH)
    monkeypatch.setattr(GleifAdapter, "_get", get)
    out = await leads.assemble_leads(BARRICK, "   ", store=_Store(None, {}))
    assert out["candidates"] == [] and out["searched_name"] is None
    get.assert_not_awaited()


# --- the route ----------------------------------------------------------------------


def test_route_answers_with_the_shape_and_rejects_a_bad_lei(monkeypatch, gleif) -> None:
    monkeypatch.setattr(GleifAdapter, "_get", AsyncMock(return_value=SEARCH))
    monkeypatch.setattr(leads, "last_filed_parent", lambda lei, store=None: None)
    with TestClient(app) as client:
        r = client.get("/leads", params={"lei": BARRICK, "name": NAME})
        assert r.status_code == 200, r.text
        body: dict[str, Any] = r.json()
        assert set(body) == {"lei", "searched_name", "parent", "candidates", "gleif_unavailable_reason", "note"}
        assert body["candidates"][0]["name_only"] is True
        assert r.headers["cache-control"] == "public, max-age=3600"
        assert client.get("/leads", params={"lei": "NOT-AN-LEI"}).status_code == 400
        # Check digits are enforced by a setting the suite leaves off; the
        # gate is `identifiers.lei_check_digit_error`, pinned in its own tests.


def test_the_route_is_a_counted_gleif_surface() -> None:
    from opencheck import gleifstats

    assert "leads" in gleifstats.ROUTES
    assert gleifstats.route_for_path("/leads") == "leads"
