"""Phase 293 — screening corporate officers as companies, and the Maersk fix.

* A company named only as an officer (a laundromat LLP's Belize designated
  member) is screened by the entity paths; without a country in common with
  the record it stays ``low`` in both ICIJ and OpenSanctions (Stephen, 6 Oct
  2026).

The Maersk verdict fix (option b) is in ``test_phase293_verdict_leads.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import related_targets
from opencheck.bods import map_companies_house
from opencheck.config import get_settings
from opencheck.cross_check import (
    RELATED_SANCTIONED,
    _collect_targets as os_collect,
    assess_cross_source_names,
    corroborating_attributes,
    match_confidence,
)
from opencheck.icij_check import _collect_targets as icij_collect, _gate
from opencheck.sources import REGISTRY, SearchKind, SourceHit

# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _metastar_bods() -> list[dict[str, Any]]:
    member = {
        "officer_role": "corporate-llp-designated-member",
        "appointed_on": "2009-06-08",
        "address": {"premises": "35", "address_line_1": "Barrack Road",
                    "locality": "Belize City", "country": "Belize"},
    }
    items = []
    for name, number, oid in (("ADVANCE DEVELOPMENTS LIMITED", "36265", "A1"),
                              ("CORPORATE SOLUTIONS LIMITED", "36269", "C1")):
        items.append({
            **member, "name": name,
            "identification": {
                "identification_type": "non-eea",
                "legal_authority": "INTERNATIONAL BUSINESS COMPANIES ACT 1990, BELIZE",
                "place_registered": "REGISTRAR OF INTERNATIONAL BUSINESS COMPANIES",
                "registration_number": number,
            },
            "links": {"officer": {"appointments": f"/officers/{oid}/appointments"}},
        })
    items.append({"name": "SMITH, Jane", "officer_role": "llp-designated-member",
                  "appointed_on": "2010-01-01"})
    return list(map_companies_house({
        "source_id": "companies_house", "company_number": "OC346224",
        "profile": {"company_name": "METASTAR INVEST LLP", "type": "llp"},
        "officers": {"items": items}, "pscs": {"items": []}, "related_companies": {},
    }))


def _subject_id(bods: list[dict[str, Any]]) -> str:
    return next(s["statementId"] for s in bods if s["recordDetails"].get("name") == "METASTAR INVEST LLP")


# ---------------------------------------------------------------------
# Which parties are corporate officers
# ---------------------------------------------------------------------


def test_officer_only_entities_are_the_corporate_members_not_the_people() -> None:
    bods = _metastar_bods()
    officers = related_targets.officer_only_entity_ids(bods)
    names_ = {s["recordDetails"]["name"] for s in bods if s["statementId"] in officers}
    assert names_ == {"ADVANCE DEVELOPMENTS LIMITED", "CORPORATE SOLUTIONS LIMITED"}


def test_an_entity_that_also_owns_is_not_a_corporate_officer() -> None:
    def rel(sid, ip, interest):
        return {"statementId": sid, "recordType": "relationship", "recordDetails": {
            "subject": "s", "interestedParty": ip,
            "interests": [{"type": interest, "directOrIndirect": "direct"}]}}
    bods = [
        {"statementId": "s", "recordType": "entity", "recordDetails": {"name": "S"}},
        {"statementId": "e", "recordType": "entity", "recordDetails": {"name": "E"}},
        rel("r1", "e", "seniorManagingOfficial"),
        rel("r2", "e", "shareholding"),
    ]
    assert related_targets.officer_only_entity_ids(bods) == set()


# ---------------------------------------------------------------------
# ICIJ
# ---------------------------------------------------------------------


def test_icij_targets_carry_the_officer_flag_and_their_country() -> None:
    bods = _metastar_bods()
    targets = icij_collect(bods, exclude={_subject_id(bods)})
    corp = {t["name"]: t for t in targets if t["kind"] == "entity"}
    assert set(corp) == {"ADVANCE DEVELOPMENTS LIMITED", "CORPORATE SOLUTIONS LIMITED"}
    for t in corp.values():
        assert t["officer_entity"] is True
        assert t["countries"] == ["BZ"]  # jurisdiction and correspondence address
    person = next(t for t in targets if t["kind"] == "person")
    assert person["name"] == "SMITH, Jane"


def test_icij_corporate_officer_without_a_country_in_common_is_low() -> None:
    target = {"kind": "entity", "statement_id": "e", "name": "CORPORATE SOLUTIONS LIMITED",
              "countries": ["BZ"], "officer_entity": True}
    keep, confidence, evidence = _gate(target, dataset="Panama Papers", collection="",
                                       node={"country_codes": []}, details_answered=True)
    assert keep and confidence == "low"
    assert "corporate officer with no country in common: capped at low" in evidence["gates"]


def test_icij_corporate_officer_with_a_country_in_common_is_high() -> None:
    target = {"kind": "entity", "statement_id": "e", "name": "CORPORATE SOLUTIONS LIMITED",
              "countries": ["BZ"], "officer_entity": True}
    keep, confidence, _ = _gate(target, dataset="Panama Papers", collection="",
                                node={"country_codes": ["BZ"]}, details_answered=True)
    assert keep and confidence == "high"


def test_icij_corporate_officer_with_a_recorded_mismatch_is_dropped() -> None:
    target = {"kind": "entity", "statement_id": "e", "name": "CORPORATE SOLUTIONS LIMITED",
              "countries": ["BZ"], "officer_entity": True}
    keep, _, _ = _gate(target, dataset="Panama Papers", collection="",
                       node={"country_codes": ["PA"]}, details_answered=True)
    assert keep is False


def test_icij_other_entities_are_unchanged() -> None:
    target = {"kind": "entity", "statement_id": "e", "name": "HSBC HOLDINGS PLC",
              "countries": ["GB"], "officer_entity": False}
    _, confidence, _ = _gate(target, dataset="Paradise Papers", collection="Appleby",
                             node={"country_codes": []}, details_answered=True)
    assert confidence == "medium"


# ---------------------------------------------------------------------
# OpenSanctions
# ---------------------------------------------------------------------


def _os_company_hit(name: str, *, country: str | None, topics=("sanction",)) -> SourceHit:
    props: dict[str, Any] = {"name": [name], "topics": list(topics)}
    if country:
        props["country"] = [country]
    return SourceHit(
        source_id="opensanctions", hit_id="NK-corp", kind=SearchKind.ENTITY, name=name,
        summary="", identifiers={},
        raw={"id": "NK-corp", "schema": "Company", "properties": props, "topics": list(topics)},
        is_stub=False,
    )


def test_os_targets_carry_the_officer_flag_and_countries() -> None:
    bods = _metastar_bods()
    targets = os_collect(bods, exclude={_subject_id(bods)})
    corp = [t for t in targets if t["kind"] == "entity"]
    assert len(corp) == 2
    assert all(t["officer_entity"] and t["countries"] == ["bz"] for t in corp)
    # A person target carries neither.
    person = next(t for t in targets if t["kind"] == "person")
    assert "officer_entity" not in person


def test_os_country_corroborates_only_a_corporate_officer() -> None:
    officer = {"kind": "entity", "name": "X", "officer_entity": True, "countries": ["bz"]}
    plain = {"kind": "entity", "name": "X", "nationalities": ()}
    raw = {"properties": {"country": ["bz"]}}
    assert corroborating_attributes(officer, raw) == ("country",)
    assert corroborating_attributes(plain, raw) == ()
    assert corroborating_attributes(officer, {"properties": {"jurisdiction": ["bz"]}}) == ("country",)
    assert corroborating_attributes(officer, {"properties": {"country": ["pa"]}}) == ()


def test_os_confidence_for_a_corporate_officer() -> None:
    officer = {"kind": "entity", "name": "X", "officer_entity": True}
    assert match_confidence(officer, 1.0, ()) == "low"
    assert match_confidence(officer, 1.0, ("country",)) == "high"
    assert match_confidence({"kind": "entity", "name": "X"}, 1.0, ()) == "high"


@pytest.fixture
def _live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _Stub:
    def __init__(self, hits): self._hits = hits

    async def search(self, query: str, kind: SearchKind):
        return [h for h in self._hits if h.name.upper() == query.upper()]


async def test_os_screen_of_metastar_members(monkeypatch, _live) -> None:
    monkeypatch.setitem(REGISTRY, "opensanctions", _Stub([
        _os_company_hit("CORPORATE SOLUTIONS LIMITED", country=None),   # generic name, no country
        _os_company_hit("ADVANCE DEVELOPMENTS LIMITED", country="bz"),  # Belize agrees
    ]))
    monkeypatch.setitem(REGISTRY, "everypolitician", _Stub([]))
    bods = _metastar_bods()
    signals = await assess_cross_source_names(bods)
    by_name = {s.evidence["search_name"]: s for s in signals if s.code == RELATED_SANCTIONED}
    corp = by_name["CORPORATE SOLUTIONS LIMITED"]
    assert corp.confidence == "low"
    assert corp.evidence["name_match_only"] is True
    assert corp.summary.startswith("Possible name match only:")
    assert "no country in common" in corp.summary
    adv = by_name["ADVANCE DEVELOPMENTS LIMITED"]
    assert adv.confidence == "high"
    assert adv.evidence["corroboration"] == ["country"]
    assert "country also agrees" in adv.summary
