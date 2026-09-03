"""Tests for the Greek General Commercial Registry (ΓΕΜΗ) adapter and mapper.

Fixtures in ``tests/data/gemi_greece_live.json`` are **real responses**
captured from the live API on 2026-08-28 (long ``objective`` free text
trimmed), covering the three shapes that matter:

* ``ae_board`` — an ΑΕ (société anonyme): board members only, every
  ``percentage`` the literal ``"-"``. An ΑΕ's share register is not part of
  ΓΕΜΗ publicity, so this is the Greek regime, not missing data.
* ``ike_partners`` — an ΙΚΕ (private company): real partners at 70/30.
* ``ee_partners`` — an ΕΕ (limited partnership): general and limited
  partners, also 70/30.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck import degradation
from opencheck.bods.mapper import map_gemi_greece
from opencheck.findings import finding_gemi_greece
from opencheck.outbound_rate import CallBudget, budget_scope
from opencheck.sources.base import SearchKind
from opencheck.sources.gemi_greece import (
    CATEGORY_BOARD,
    CATEGORY_PARTNERS,
    GR_RA_CODE,
    GemiGreeceAdapter,
    codelist_entry,
    english_label,
    normalise_argemi,
    parse_percentage,
    status_is_active,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.gemi_greece import GemiGreeceBundle

_FIXTURES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "data" / "gemi_greece_live.json").read_text(encoding="utf-8")
)


def _bundle(key: str, **overrides: Any) -> dict[str, Any]:
    company = json.loads(json.dumps(_FIXTURES[key]))
    company.update(overrides.pop("company", {}))
    return {
        "source_id": "gemi_greece",
        "gr_argemi": str(company["arGemi"]),
        "company": company,
        "documents": None,
        "legal_name": "",
        "is_stub": False,
        **overrides,
    }


# ---------------------------------------------------------------------------
# Identifier normalisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        # GLEIF stores the Αριθμός ΓΕΜΗ zero-padded to 12 digits, and that
        # padded form is the citable identifier — it is preserved, not
        # stripped. The API accepts either form (both return byte-identical
        # responses, verified live 2026-08-28).
        ("003031801000", "003031801000"),
        ("3031801000", "3031801000"),
        (" 003031801000 ", "003031801000"),
        ("003.031.801.000", "003031801000"),
    ],
)
def test_normalise_argemi_preserves_padding(raw: str, expected: str) -> None:
    assert normalise_argemi(raw) == expected


@pytest.mark.parametrize("bad", ["", "ABC", "12345678901234", "31.1a", "GR123"])
def test_normalise_argemi_rejects_non_numeric(bad: str) -> None:
    with pytest.raises(ValueError):
        normalise_argemi(bad)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("70%", 70.0),
        ("30%", 30.0),
        ("50", 50.0),
        ("33,33%", 33.33),   # comma decimal separator
        ("-", None),         # every board member carries this
        ("", None),
        (None, None),
        ("0%", None),        # a zero holding is not a holding
        ("150%", None),
    ],
)
def test_parse_percentage(raw: Any, expected: float | None) -> None:
    assert parse_percentage(raw) == expected


# ---------------------------------------------------------------------------
# Codelists — the id-type trap
# ---------------------------------------------------------------------------


def test_codelist_joins_across_the_id_type_mismatch() -> None:
    """Codelists key on strings; company records embed integers.

    Getting this wrong is a silent total miss — every English label would
    quietly fall back to the Greek one — so it is pinned explicitly.
    """
    assert codelist_entry("companyStatuses", 3).get("descrEn") == "Active"
    assert codelist_entry("companyStatuses", "3").get("descrEn") == "Active"


def test_exactly_one_status_is_active() -> None:
    """Of the twelve ΓΕΜΗ statuses only id 3 (Ενεργή) is an operating one."""
    assert status_is_active({"id": 3}) is True
    assert status_is_active({"id": 17}) is False   # Διαγραφή / deletion
    assert status_is_active({"id": 99999}) is None  # unknown ≠ inactive
    assert status_is_active(None) is None


def test_english_label_falls_back_to_greek() -> None:
    assert english_label("legalTypes", {"id": 1, "descr": "ΑΕ"}) == "SA"
    assert english_label("legalTypes", {"id": 99999, "descr": "ΧΧΧ"}) == "ΧΧΧ"
    assert english_label("legalTypes", None) == ""


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", sorted(_FIXTURES))
def test_schema_accepts_live_payloads(key: str) -> None:
    model = validate_raw("gemi_greece", GemiGreeceBundle, _bundle(key))
    assert model.company is not None
    assert model.company.arGemi


def test_schema_keeps_undocumented_fields() -> None:
    """``phone`` and ``fax`` are returned live but are in no Swagger definition."""
    model = validate_raw(
        "gemi_greece",
        GemiGreeceBundle,
        {"gr_argemi": "1", "company": {"arGemi": "1", "phone": "210", "fax": "211"}},
    )
    assert model.company is not None
    assert model.company.model_extra["phone"] == "210"


def test_schema_requires_argemi() -> None:
    with pytest.raises(SourceSchemaError):
        validate_raw(
            "gemi_greece", GemiGreeceBundle, {"gr_argemi": "1", "company": {"afm": "1"}}
        )


def test_schema_accepts_null_prefecture() -> None:
    """``{"id": 0, "descr": null}`` is returned live for some companies."""
    validate_raw(
        "gemi_greece",
        GemiGreeceBundle,
        {"gr_argemi": "1", "company": {"arGemi": "1", "prefecture": {"id": 0, "descr": None}}},
    )


# ---------------------------------------------------------------------------
# BODS mapping
# ---------------------------------------------------------------------------


def _split(statements: list[dict[str, Any]]) -> tuple[list, list, list]:
    subjects = [s for s in statements if s["recordType"] == "entity"
                and s["recordDetails"].get("jurisdiction")]
    parties = [s for s in statements if s["recordType"] in ("entity", "person")
               and s not in subjects]
    rels = [s for s in statements if s["recordType"] == "relationship"]
    return subjects, parties, rels


@pytest.mark.parametrize("key", sorted(_FIXTURES))
def test_maps_one_subject_entity_per_company(key: str) -> None:
    subjects, _, rels = _split(list(map_gemi_greece(_bundle(key))))
    company = _FIXTURES[key]
    assert len(subjects) == 1
    details = subjects[0]["recordDetails"]
    assert details["name"] == company["coNameEl"]
    assert details["jurisdiction"] == {"name": "Greece", "code": "GR"}
    assert [i["scheme"] for i in details["identifiers"]] == ["GR-GEMI", "GR-AFM"]
    assert len(rels) == len(company["persons"])


@pytest.mark.parametrize("key", sorted(_FIXTURES))
def test_never_asserts_beneficial_ownership(key: str) -> None:
    """ΓΕΜΗ is a commercial register, not a BO regime — the canary.

    Greece's beneficial ownership register (Κεντρικό Μητρώο Πραγματικών
    Δικαιούχων) is separate and non-public. ``gemi_greece`` must therefore
    never appear in ``_BO_ASSERTING_SOURCES`` and no interest may carry the
    flag; BODS reads its absence as "not stated", which is the honest claim.
    """
    _, _, rels = _split(list(map_gemi_greece(_bundle(key))))
    for rel in rels:
        for interest in rel["recordDetails"]["interests"]:
            assert "beneficialOwnershipOrControl" not in interest


def test_gemi_is_not_a_bo_asserting_source() -> None:
    from opencheck.bods.mapper import source_may_assert_beneficial_ownership

    assert source_may_assert_beneficial_ownership("gemi_greece") is False


def test_ae_publishes_officers_but_no_owners() -> None:
    """An ΑΕ's share register is not part of ΓΕΜΗ publicity."""
    _, _, rels = _split(list(map_gemi_greece(_bundle("ae_board"))))
    interests = [i for r in rels for i in r["recordDetails"]["interests"]]
    assert {i["type"] for i in interests} == {"seniorManagingOfficial"}
    assert not any("share" in i for i in interests)


@pytest.mark.parametrize("key", ["ike_partners", "ee_partners"])
def test_partners_carry_percentage_holdings(key: str) -> None:
    _, _, rels = _split(list(map_gemi_greece(_bundle(key))))
    interests = [i for r in rels for i in r["recordDetails"]["interests"]]
    shares = sorted(i["share"]["exact"] for i in interests if "share" in i)
    assert shares == [30.0, 70.0]
    # Each partner is also a manager, so each holds two distinct interests —
    # a shareholding and a management role — not one flattened interest.
    assert {i["type"] for i in interests} == {"shareholding", "seniorManagingOfficial"}
    assert all(len(r["recordDetails"]["interests"]) == 2 for r in rels)


def test_future_dt_to_is_a_term_not_a_closure() -> None:
    """Board terms run to a fixed future date; that is not a closed record."""
    _, _, rels = _split(list(map_gemi_greece(_bundle("ae_board"))))
    assert all(r["dtTo"] for r in _FIXTURES["ae_board"]["persons"])
    assert {r["recordStatus"] for r in rels} == {"new"}


def test_past_dt_to_closes_the_record() -> None:
    bundle = _bundle("ike_partners")
    bundle["company"]["persons"][0]["dtTo"] = "2021-05-05"
    _, _, rels = _split(list(map_gemi_greece(bundle)))
    assert rels[0]["recordStatus"] == "closed"
    assert rels[1]["recordStatus"] == "new"
    assert any(
        i.get("endDate") == "2021-05-05" for i in rels[0]["recordDetails"]["interests"]
    )


def test_corporate_partner_becomes_an_entity_statement() -> None:
    bundle = _bundle("ike_partners")
    bundle["company"]["persons"][0] = {
        "personName": None,
        "businessName": "ΑΛΦΑ ΕΠΕ",
        "role": "Ομόρρυθμο Μέλος",
        "dtFrom": "2020-01-01",
        "dtTo": None,
        "percentage": "55%",
        "category": CATEGORY_PARTNERS,
    }
    _, parties, _ = _split(list(map_gemi_greece(bundle)))
    corporate = [p for p in parties if p["recordType"] == "entity"]
    assert len(corporate) == 1
    details = corporate[0]["recordDetails"]
    assert details["name"] == "ΑΛΦΑ ΕΠΕ"
    assert details["entityType"]["type"] == "legalEntity"
    # ομόρρυθμος (unlimited liability) vs ετερόρρυθμος is substantive and is
    # preserved rather than flattened to "partner".
    assert "General partner" in details["entityType"]["details"]


def test_unknown_category_is_recorded_not_dropped() -> None:
    bundle = _bundle("ike_partners")
    bundle["company"]["persons"] = [{
        "personName": "ΠΑΠΑΣ ΙΩΑΝΝΗΣ",
        "role": "Ρόλος Χ",
        "category": "Κάτι Άγνωστο",
        "dtFrom": "2020-01-01",
        "dtTo": None,
        "percentage": "-",
    }]
    _, _, rels = _split(list(map_gemi_greece(bundle)))
    interests = rels[0]["recordDetails"]["interests"]
    assert interests[0]["type"] == "otherInfluenceOrControl"
    assert interests[0]["details"] == "Ρόλος Χ"   # raw Greek survives


def test_dissolved_company_gets_a_dissolution_date() -> None:
    bundle = _bundle("ike_partners")
    bundle["company"]["status"] = {"id": 17, "descr": "Διαγραφή"}
    bundle["company"]["lastStatusChange"] = "2024-03-01"
    subjects, _, _ = _split(list(map_gemi_greece(bundle)))
    assert subjects[0]["recordDetails"]["dissolutionDate"] == "2024-03-01"


def test_active_company_has_no_dissolution_date() -> None:
    """``lastStatusChange`` on a live company is just the last edit."""
    subjects, _, _ = _split(list(map_gemi_greece(_bundle("ae_board"))))
    assert "dissolutionDate" not in subjects[0]["recordDetails"]


@pytest.mark.parametrize(
    "bundle",
    [
        {"gr_argemi": "1", "company": None},
        {"gr_argemi": "", "company": {}},
        {"gr_argemi": "1"},
    ],
)
def test_mapper_is_defensive(bundle: dict[str, Any]) -> None:
    assert list(map_gemi_greece(bundle)) == []


# ---------------------------------------------------------------------------
# Finding sentence
# ---------------------------------------------------------------------------


def test_finding_reports_partners_and_board() -> None:
    sentence = finding_gemi_greece(_bundle("ike_partners"))
    assert sentence is not None
    assert "70%" in sentence
    assert "partners" in sentence


def test_finding_never_says_owners_are_missing_for_an_ae() -> None:
    """An ΑΕ not publishing owners is Greek law, not a transparency failure."""
    sentence = finding_gemi_greece(_bundle("ae_board")) or ""
    assert "board member" in sentence
    for forbidden in ("no owner", "not found", "missing", "withheld", "none"):
        assert forbidden not in sentence.lower()


# ---------------------------------------------------------------------------
# Adapter — rate limiting, budget and degradation
# ---------------------------------------------------------------------------


def test_ra_code_and_deriver() -> None:
    adapter = GemiGreeceAdapter()
    assert GR_RA_CODE == "RA000685"
    deriver = adapter.lookup_derivers[0]
    assert GR_RA_CODE in deriver.ra_codes
    assert deriver.derived_key == "gr_argemi"
    assert deriver.normalise("003031801000") == "003031801000"


def test_info_requires_a_key() -> None:
    adapter = GemiGreeceAdapter()
    info = adapter.info
    assert info.requires_api_key is True
    assert info.is_national_register is True
    assert info.country == "GR"
    assert info.license == "ODC-BY-1.0"


def test_the_call_budget_fits_inside_the_source_timeout() -> None:
    """The budget's real ceiling is the clock, not ΓΕΜΗ's published quota.

    The token bucket serialises calls ``60 / _RATE_PER_MINUTE`` apart, and a
    source gets ``lookup_timeout_s`` wall-clock seconds inside a lookup. Spend
    the whole budget and the queueing alone must finish inside that, or the
    lookup reports a timeout — the very outcome the budget exists to replace
    with an honest degradation.

    So this is not a literal pin on 8 and 18; it is the relationship between
    them. Raising the budget without raising ``lookup_timeout_s`` fails here,
    with the arithmetic in the message.
    """
    from opencheck.sources.gemi_greece import (
        _LOOKUP_CALL_BUDGET,
        _RATE_PER_MINUTE,
    )

    interval = 60.0 / _RATE_PER_MINUTE
    worst_case_queueing = (_LOOKUP_CALL_BUDGET - 1) * interval
    timeout = GemiGreeceAdapter.lookup_timeout_s

    assert worst_case_queueing < timeout, (
        f"{_LOOKUP_CALL_BUDGET} calls paced {interval:.2f}s apart queue for "
        f"{worst_case_queueing:.1f}s, past the {timeout:.0f}s this source "
        "gets — raise lookup_timeout_s or lower the budget"
    )


def test_the_paced_rate_stays_under_the_published_quota() -> None:
    """ΓΕΜΗ publish 20/min (raised from 8 on 2026-09-03).

    Pacing at the published rate exactly leaves nothing for a retry or for
    clock skew against the server's own sliding window, which is why the
    original was 7 of 8. Keep at least that ~10% margin.
    """
    from opencheck.sources.gemi_greece import _RATE_PER_MINUTE

    published = 20.0
    assert _RATE_PER_MINUTE <= published * 0.95, (
        f"pacing at {_RATE_PER_MINUTE}/min leaves no headroom under ΓΕΜΗ's "
        f"published {published:.0f}/min"
    )


def test_call_budget_stops_at_the_limit() -> None:
    budget = CallBudget(limit=2)
    assert budget.take() is True
    assert budget.take() is True
    assert budget.take() is False
    assert budget.exhausted is True


def test_budget_exhaustion_degrades_rather_than_raising() -> None:
    """Over budget, the adapter records a degradation and returns a stub.

    The lookup must lose one source, not the whole request — and the reason
    has to reach the user rather than looking like an empty register.
    """
    adapter = GemiGreeceAdapter()
    settings = MagicMock(allow_live=True, gemi_api_key="test-key")
    with patch("opencheck.sources.gemi_greece.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=False), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch("opencheck.sources.gemi_greece.build_client") as client:
        client.return_value.__aenter__.return_value.get = AsyncMock()
        with degradation.recording() as recorded, budget_scope() as budgets:
            budgets["gemi_greece"] = CallBudget(limit=0)
            result = asyncio.run(adapter.fetch("3031801000"))

    assert result["company"] is None
    assert result["is_stub"] is True
    assert any(
        d.source_id == "gemi_greece" and d.reason == degradation.REASON_RATE_LIMITED
        for d in recorded
    )


def test_http_429_degrades_rather_than_raising() -> None:
    adapter = GemiGreeceAdapter()
    settings = MagicMock(allow_live=True, gemi_api_key="test-key")
    response = MagicMock(status_code=429, is_success=False)
    with patch("opencheck.sources.gemi_greece.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=False), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch("opencheck.sources.gemi_greece.build_client") as client:
        client.return_value.__aenter__.return_value.get = AsyncMock(return_value=response)
        with degradation.recording() as recorded:
            result = asyncio.run(adapter.fetch("3031801000"))

    assert result["company"] is None
    assert any(
        d.source_id == "gemi_greece" and d.reason == degradation.REASON_RATE_LIMITED
        for d in recorded
    )


def test_search_needs_no_follow_up_detail_call() -> None:
    """``/companies`` returns complete records, so search costs one request.

    Verified live 2026-08-28: ``searchResults[]`` elements carry ``persons``,
    ``capital`` and ``stocks``, not a summary. Pinned here because the whole
    request-budget arithmetic depends on it.
    """
    adapter = GemiGreeceAdapter()
    settings = MagicMock(allow_live=True, gemi_api_key="test-key")
    payload = {
        "searchMetadata": {"totalCount": 1, "resultsOffset": 0, "resultsSize": 1},
        "searchResults": [_FIXTURES["ike_partners"]],
    }
    response = MagicMock(status_code=200, is_success=True)
    response.json.return_value = payload
    get = AsyncMock(return_value=response)
    with patch("opencheck.sources.gemi_greece.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=False), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch.object(adapter._cache, "put"), \
         patch("opencheck.sources.gemi_greece.build_client") as client:
        client.return_value.__aenter__.return_value.get = get
        hits = asyncio.run(adapter.search("BUTTON", SearchKind.ENTITY))

    assert get.await_count == 1
    assert len(hits) == 1
    assert hits[0].raw["company"]["persons"]           # full record, not a summary
    assert hits[0].identifiers["gr_argemi"] == "131516209000"
    assert "lei" not in hits[0].identifiers            # never assert the LEI


def test_search_rejects_person_kind() -> None:
    adapter = GemiGreeceAdapter()
    assert asyncio.run(adapter.search("x", SearchKind.PERSON)) == []


def test_category_constants_match_the_mapper() -> None:
    """The mapper's category map is keyed by literal Greek — keep them in step."""
    from opencheck.bods.mapper import _GEMI_CATEGORY_INTERESTS

    assert _GEMI_CATEGORY_INTERESTS[CATEGORY_PARTNERS] == "shareholding"
    assert _GEMI_CATEGORY_INTERESTS[CATEGORY_BOARD] == "seniorManagingOfficial"
