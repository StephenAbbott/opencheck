"""Tests for the EITI adapter, its index matching, mapper and lookup wiring."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.bods import map_eiti, validate_shape
from opencheck.config import get_settings
from opencheck.routers.lookup import (
    _EITI_IDENTIFIER_KEY_BY_COUNTRY,
    _bh_eiti,
    _build_derived,
    _dispatch,
    _LookupCtx,
)
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.eiti import (
    EitiAdapter,
    _country_code,
    _get_index,
    _match_identification,
    _norm_forms,
    us_ein_for_lei,
)

_API = "https://eiti.org/api/v2.0"


def _equinor_recent_orgs() -> list[dict]:
    """The four organisation-years the adapter fetches revenue for: the most
    recent across EVERY spelling of Equinor UK's number. EITI files 2023
    under ``1285743`` and 2018-2021 under ``01285743`` (Phase 306)."""
    import opencheck.sources.eiti as eiti_mod

    orgs = eiti_mod._organisations("GB", "01285743")
    return sorted(orgs, key=lambda o: o.get("year") or "", reverse=True)[:4]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Identifier normalisation + committed-artifact matching
# ---------------------------------------------------------------------------


def test_norm_forms_variants() -> None:
    assert "005658214" in _norm_forms("0056.58.214")
    assert "5658214" in _norm_forms("0056.58.214")  # leading-zero-insensitive
    assert _norm_forms("  01285743 ") == ["01285743", "1285743"]
    assert _norm_forms("") == []


def test_committed_artifact_matches_equinor_uk() -> None:
    """The shipped artifact resolves Equinor UK's Companies House number
    (GLEIF registeredAs for its LEI) in several formatting variants."""
    # EITI files both "01285743" and "1285743" (Phase 306): a spelling it
    # files verbatim resolves to itself, any other to one of the two, and
    # the adapter reads the records of both either way.
    assert _match_identification("GB", "01285743") == "01285743"
    assert _match_identification("GB", "1285743") == "1285743"
    assert _match_identification("GB", "01-28-5743") in {"01285743", "1285743"}
    assert _match_identification("GB", "99999999") is None
    assert _match_identification("ZZ", "01285743") is None


def test_artifact_has_broad_country_coverage() -> None:
    from opencheck.sources.eiti import _get_index

    index, _ = _get_index()
    assert len(index) >= 40  # 56 countries at build time; floor for safety
    assert "GB" in index and "NO" in index and "MN" in index


# ---------------------------------------------------------------------------
# US EIN matching (issue #26): US EITI identifications are EINs in
# NN-NNNNNNN form, not the state-registry numbers GLEIF publishes as
# registeredAs. Matching must be punctuation-insensitive and country-scoped.
# ---------------------------------------------------------------------------


def test_norm_forms_handles_dashed_ein() -> None:
    """A US EIN (``NN-NNNNNNN``) normalises to its digits so it joins the
    committed index regardless of the dash."""
    assert _norm_forms("42-1638663") == ["42-1638663", "421638663"]


def test_committed_artifact_matches_us_ein() -> None:
    """The shipped artifact resolves a US company's EIN in both dashed and
    digits-only form. Decoys prove the filter binds: a different EIN and the
    same EIN under the wrong country both fail."""
    for variant in ("42-1638663", "421638663"):
        assert _match_identification("US", variant) == "42-1638663", variant
    assert _match_identification("US", "99-9999999") is None  # wrong EIN
    assert _match_identification("GB", "42-1638663") is None  # wrong country


# ---------------------------------------------------------------------------
# fetch_by_registration
# ---------------------------------------------------------------------------


async def test_fetch_by_registration_offline_returns_org_matches() -> None:
    """Offline: organisation matches come from the artifact; no live
    payment calls are made (revenue_years empty)."""
    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration("GB", "01285743", legal_name="Equinor UK Ltd")
    assert bundle is not None
    assert bundle["country"] == "GB"
    assert bundle["identification"] == "01285743"
    assert len(bundle["organisations"]) >= 2
    assert bundle["years"]  # e.g. ['2021', '2020', '2019', '2018']
    assert bundle["revenue_years"] == []
    assert bundle["is_stub"] is False


async def test_fetch_by_registration_no_match_returns_none() -> None:
    adapter = EitiAdapter()
    assert await adapter.fetch_by_registration("GB", "99999999") is None
    assert await adapter.fetch_by_registration("", "01285743") is None


async def test_fetch_by_registration_matches_via_us_ein() -> None:
    """US subjects match on a derived EIN tried alongside registeredAs. The
    state-registry registeredAs does NOT hit the EIN-keyed US bucket (decoy),
    so the match must come from the EIN passed as ``us_ein``."""
    adapter = EitiAdapter()
    # registeredAs here is a state-registry number absent from the US bucket;
    # if the match came from it (not the EIN) the test would be vacuous.
    assert await adapter.fetch_by_registration("US", "C1234567") is None
    bundle = await adapter.fetch_by_registration(
        "US", "C1234567", legal_name="Alpha Natural Resources, Inc.",
        us_ein="42-1638663",
    )
    assert bundle is not None
    assert bundle["country"] == "US"
    assert bundle["identification"] == "42-1638663"
    assert bundle["is_stub"] is False


async def test_fetch_by_registration_wrong_us_ein_returns_none() -> None:
    """A wrong EIN does not match even when supplied as ``us_ein`` (decoy)."""
    adapter = EitiAdapter()
    assert (
        await adapter.fetch_by_registration("US", "C1234567", us_ein="99-9999999")
        is None
    )


async def test_live_revenue_aggregation(monkeypatch, httpx_mock: HTTPXMock, tmp_path) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    def _revenue_response(org_id: str, amounts: list[float]):
        return {
            "data": [
                {
                    "label": "Petroleum Licence Fees",
                    "revenue": str(a),
                    "currency": "USD",
                    "gfs.label": "Licence fees",
                    "gfs.code": "1145E",
                    "organisation.id": org_id,
                }
                for a in amounts
            ]
        }

    # The adapter fetches revenues for up to 4 most recent org-years.
    import opencheck.sources.eiti as eiti_mod

    index, _ = eiti_mod._get_index()
    org_ids = [o["id"] for o in _equinor_recent_orgs()]
    for i, org_id in enumerate(org_ids):
        httpx_mock.add_response(
            url=f"{_API}/revenue?organisation={org_id}&limit=50",
            json=_revenue_response(org_id, [100.0 + i, 200.0]),
        )

    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration("GB", "01285743")
    assert bundle is not None
    assert len(bundle["revenue_years"]) == len(org_ids)
    assert bundle["total_usd"] == pytest.approx(
        sum(100.0 + i + 200.0 for i in range(len(org_ids)))
    )
    assert "Licence fees" in bundle["streams"]
    get_settings.cache_clear()


async def test_live_revenue_failure_degrades_to_empty_rows(
    monkeypatch, httpx_mock: HTTPXMock, tmp_path
) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    import opencheck.sources.eiti as eiti_mod

    index, _ = eiti_mod._get_index()
    for o in _equinor_recent_orgs():
        httpx_mock.add_response(
            url=f"{_API}/revenue?organisation={o['id']}&limit=50", status_code=500
        )

    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration("GB", "01285743")
    assert bundle is not None  # org match survives; payments empty
    assert bundle["total_usd"] == 0.0
    get_settings.cache_clear()


async def test_live_revenue_follows_next_pages(
    monkeypatch, httpx_mock: HTTPXMock, tmp_path
) -> None:
    """The revenue endpoint pages at 50 rows whatever ``limit`` says, and
    paginates through ``next``. Harbour Energy Plc's 2021 record (organisation
    226920) holds 130 rows; reading page one and summing it reported $249.9M
    as the year's total when page two alone added $4.1M more. The adapter
    follows ``next`` and the year is marked complete.
    """
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    import opencheck.sources.eiti as eiti_mod

    index, _ = eiti_mod._get_index()
    orgs = _equinor_recent_orgs()
    paged, *rest = orgs
    page1 = f"{_API}/revenue?organisation={paged['id']}&limit=50"
    page2 = f"{_API}/revenue?organisation={paged['id']}&limit=50&page=2"
    page3 = f"{_API}/revenue?organisation={paged['id']}&limit=50&page=3"

    def _row(amount: float) -> dict:
        return {"label": "Royalties", "revenue": str(amount), "currency": "USD",
                "gfs.label": "Royalties", "gfs.code": "1415E1"}

    httpx_mock.add_response(
        url=page1,
        json={"count": 130, "next": {"title": "Next", "href": page2},
              "data": [_row(1.0)] * 50},
    )
    httpx_mock.add_response(
        url=page2,
        json={"count": 130, "previous": {"href": page1}, "next": {"href": page3},
              "data": [_row(2.0)] * 50},
    )
    httpx_mock.add_response(
        url=page3,
        json={"count": 130, "previous": {"href": page2}, "data": [_row(4.0)] * 30},
    )
    for o in rest:
        httpx_mock.add_response(
            url=f"{_API}/revenue?organisation={o['id']}&limit=50",
            json={"count": 1, "data": [_row(10.0)]},
        )

    bundle = await EitiAdapter().fetch_by_registration("GB", "01285743")
    assert bundle is not None
    year = next(ry for ry in bundle["revenue_years"] if ry["organisation_id"] == paged["id"])
    assert len(year["rows"]) == 130
    assert year["rows_available"] == 130
    assert year["truncated"] is False
    assert year["total_usd"] == pytest.approx(50 * 1.0 + 50 * 2.0 + 30 * 4.0)
    assert bundle["truncated_years"] == []
    assert bundle["total_usd"] == pytest.approx(270.0 + 10.0 * len(rest))
    get_settings.cache_clear()


async def test_live_revenue_beyond_the_page_bound_is_marked_truncated(
    monkeypatch, httpx_mock: HTTPXMock, tmp_path
) -> None:
    """Past ``_MAX_REVENUE_PAGES`` the adapter stops, says so on the year and
    on the bundle, and the hit summary says so too — a bounded sum is never
    presented as the total."""
    from opencheck.routers.hit_builders import _bh_eiti, _LookupCtx

    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("opencheck.sources.eiti._MAX_REVENUE_PAGES", 2)
    get_settings.cache_clear()

    import opencheck.sources.eiti as eiti_mod

    index, _ = eiti_mod._get_index()
    orgs = _equinor_recent_orgs()
    paged, *rest = orgs
    page1 = f"{_API}/revenue?organisation={paged['id']}&limit=50"
    page2 = f"{_API}/revenue?organisation={paged['id']}&limit=50&page=2"
    page3 = f"{_API}/revenue?organisation={paged['id']}&limit=50&page=3"
    row = {"label": "Royalties", "revenue": "1.0", "currency": "USD",
           "gfs.label": "Royalties", "gfs.code": "1415E1"}
    httpx_mock.add_response(url=page1, json={"count": 130, "next": {"href": page2}, "data": [row] * 50})
    httpx_mock.add_response(url=page2, json={"count": 130, "next": {"href": page3}, "data": [row] * 50})
    # page3 is deliberately NOT registered: the bound stops the adapter first.
    for o in rest:
        httpx_mock.add_response(
            url=f"{_API}/revenue?organisation={o['id']}&limit=50",
            json={"count": 0, "data": []},
        )

    bundle = await EitiAdapter().fetch_by_registration("GB", "01285743")
    assert bundle is not None
    year = next(ry for ry in bundle["revenue_years"] if ry["organisation_id"] == paged["id"])
    assert len(year["rows"]) == 100
    assert year["rows_available"] == 130
    assert year["truncated"] is True
    assert bundle["truncated_years"] == [str(paged["year"])]

    hit = _bh_eiti(bundle, _LookupCtx(lei="X", legal_name="Y"))
    assert f"payment rows incomplete for {paged['year']}" in hit.summary
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Lookup wiring
# ---------------------------------------------------------------------------


def test_dispatch_includes_eiti_when_anchor_has_registration() -> None:
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "GB"
    ctx.registered_as = "01285743"
    ctx.legal_name = "Equinor UK Ltd"
    tasks = _dispatch(ctx, only="eiti")
    assert [sid for sid, _ in tasks] == ["eiti"]
    for _, coro in tasks:
        coro.close()  # avoid un-awaited coroutine warnings


def test_dispatch_skips_eiti_without_registration() -> None:
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "GB"
    ctx.registered_as = ""
    assert _dispatch(ctx, only="eiti") == []


def test_dispatch_includes_eiti_via_derived_us_ein() -> None:
    """A US subject with a derived EIN but no usable registeredAs still
    dispatches EITI — the EIN is what keys the US bucket."""
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "US"
    ctx.registered_as = ""
    ctx.derived = {"us_ein": "42-1638663"}
    tasks = _dispatch(ctx, only="eiti")
    assert [sid for sid, _ in tasks] == ["eiti"]
    for _, coro in tasks:
        coro.close()  # avoid un-awaited coroutine warnings


def test_bh_eiti_builds_hit_with_corroborating_identifier() -> None:
    ctx = _LookupCtx(lei="X" * 20)
    bundle = {
        "source_id": "eiti",
        "country": "GB",
        "identification": "01285743",
        "entity_name": "Equinor UK Ltd",
        "organisations": [{"id": "226918", "year": "2021", "label": "Equinor UK Ltd"}],
        "revenue_years": [],
        "streams": {},
        "total_usd": 6_270_001.0,
        "years": ["2021", "2018"],
        "is_stub": False,
    }
    hit = _bh_eiti(bundle, ctx)
    assert hit.source_id == "eiti"
    assert hit.hit_id == "GB:01285743"
    # GB identifications are Companies House numbers, independently
    # published by EITI → legitimate cross-source corroboration key.
    assert hit.identifiers == {"gb_coh": "01285743"}
    assert "EITI GB" in hit.summary
    assert "$6.3M USD to governments" in hit.summary


def test_bh_eiti_us_emits_us_ein_identifier() -> None:
    """A US EITI match corroborates via the ``us_ein`` key (the EIN is the US
    federal identifier EITI independently publishes)."""
    ctx = _LookupCtx(lei="X" * 20)
    bundle = {
        "source_id": "eiti",
        "country": "US",
        "identification": "42-1638663",
        "entity_name": "Alpha Natural Resources, Inc.",
        "organisations": [],
        "revenue_years": [],
        "streams": {},
        "total_usd": 0.0,
        "years": ["2018"],
        "is_stub": False,
    }
    hit = _bh_eiti(bundle, ctx)
    assert hit.hit_id == "US:42-1638663"
    assert hit.identifiers == {"us_ein": "42-1638663"}


def test_eiti_identifier_key_map_is_conservative() -> None:
    """Only countries with verified format equivalence map to OpenCheck
    identifier keys; everything else uses the neutral eiti_identification."""
    assert set(_EITI_IDENTIFIER_KEY_BY_COUNTRY) == {"GB", "NO", "NL", "US", "ZM"}


# ---------------------------------------------------------------------------
# BODS mapping
# ---------------------------------------------------------------------------


def test_map_eiti_emits_entity_statement() -> None:
    bundle = {
        "source_id": "eiti",
        "country": "GB",
        "identification": "01285743",
        "entity_name": "Equinor UK Ltd",
        "organisations": [],
        "revenue_years": [],
        "streams": {},
        "total_usd": 0.0,
        "years": [],
        "is_stub": False,
    }
    statements = list(map_eiti(bundle))
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt["recordType"] == "entity"
    assert stmt["recordDetails"]["name"] == "Equinor UK Ltd"
    ident = stmt["recordDetails"]["identifiers"][0]
    assert ident["id"] == "01285743"
    assert ident["scheme"] == "GB-COH"
    assert validate_shape(statements) == []


def test_map_eiti_unknown_country_names_the_disclosure_not_a_register() -> None:
    """Phase 239: no identifier leaves without a scheme. EITI does not say
    which register issued the number, so the scheme names the disclosure."""
    bundle = {
        "source_id": "eiti",
        "country": "MN",
        "identification": "2016656",
        "entity_name": "Tavantolgoi JSC",
        "is_stub": False,
    }
    statements = list(map_eiti(bundle))
    ident = statements[0]["recordDetails"]["identifiers"][0]
    assert ident["scheme"] == "EITI-IDENTIFICATION"
    assert "EITI" in ident["schemeName"]


def test_map_eiti_stub_yields_nothing() -> None:
    assert list(map_eiti({"is_stub": True})) == []
    assert list(map_eiti({})) == []


# ---------------------------------------------------------------------------
# The US EIN crosswalk (issue #26). PR #46 taught fetch_by_registration to
# accept a derived ``us_ein``, but nothing produced one, so no US subject
# ever matched end-to-end. These tests pin the producer.
# ---------------------------------------------------------------------------

#: One high-confidence row of the committed crosswalk: the EIN came from
#: EITI's 2015 US filing and was confirmed against the EDGAR registrant's
#: own ``ein`` field before the LEI was accepted.
_EXXON_LEI = "J3WHBG0MTS7O8ZVMDC91"
_EXXON_EIN = "13-5409005"


def test_us_ein_crosswalk_resolves_a_committed_lei() -> None:
    assert us_ein_for_lei(_EXXON_LEI) == _EXXON_EIN
    assert us_ein_for_lei(_EXXON_LEI.lower()) == _EXXON_EIN  # case-insensitive
    assert us_ein_for_lei("X" * 20) == ""  # unknown LEI
    assert us_ein_for_lei("") == ""


def test_crosswalk_rows_all_hit_the_eiti_us_bucket() -> None:
    """Every EIN in the crosswalk must still match the organisation index.

    The two artifacts are rebuilt by different scripts. If the EITI index is
    refreshed without re-running build_eiti_us_ein_index.py, US matching goes
    silently back to zero -- exactly the failure this whole change fixes --
    and nothing else would notice.
    """
    index, _ = _get_index()
    assert index.get("US"), "the committed EITI index has no US bucket"
    from opencheck.sources.eiti import _US_EIN_PATH
    import json as _json

    rows = _json.loads(_US_EIN_PATH.read_text(encoding="utf-8"))["index"]
    assert rows, "the committed crosswalk is empty"
    for lei, row in rows.items():
        assert _match_identification("US", row["ein"]) is not None, (
            f"{lei} ({row['eiti_label']}) carries EIN {row['ein']}, which is "
            "no longer in the EITI US bucket -- rebuild the crosswalk"
        )
        assert us_ein_for_lei(lei) == row["ein"]


def test_index_drops_identifications_with_no_digits() -> None:
    """EITI's US bucket ships literal 'Private' and 'Foreign' sentinels where
    a company gave no registry number. They are not identifiers, and a lookup
    must never match on the word."""
    index, _ = _get_index()
    for sentinel in ("Private", "Foreign"):
        assert sentinel not in index["US"]
        assert _match_identification("US", sentinel) is None
    # The guard is about digits, not about letters: GB's alphanumeric
    # Companies House numbers survive it.
    assert any(not k.isdigit() for k in index["GB"]), "GB bucket lost its SC/NI numbers"


def test_build_derived_populates_us_ein_from_the_crosswalk() -> None:
    ctx = _LookupCtx(lei=_EXXON_LEI)
    ctx.jurisdiction = "US"
    ctx.registered_as = "0000019017"  # a state file number, not the EIN
    _build_derived(ctx, "")
    assert ctx.derived["us_ein"] == _EXXON_EIN
    # The state-registry number is what GLEIF publishes and it is not an EIN,
    # so it must not have been reused as one.
    assert ctx.derived["us_ein"] != ctx.registered_as


def test_build_derived_omits_us_ein_off_the_crosswalk() -> None:
    """No key at all rather than an empty string -- an empty us_ein would
    still satisfy the dispatch guard's truth test somewhere downstream."""
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "US"
    _build_derived(ctx, "")
    assert "us_ein" not in ctx.derived

    # Same LEI, non-US jurisdiction: the crosswalk is never consulted.
    ctx2 = _LookupCtx(lei=_EXXON_LEI)
    ctx2.jurisdiction = "GB"
    ctx2.registered_as = "01285743"
    _build_derived(ctx2, "")
    assert "us_ein" not in ctx2.derived


async def test_us_subject_matches_eiti_end_to_end_from_the_crosswalk() -> None:
    """The loop PR #46 left open: derive -> dispatch -> match, with no live
    call and nothing hand-fed. Before the crosswalk existed this produced no
    dispatch at all for a US subject whose registeredAs is a state number."""
    ctx = _LookupCtx(lei=_EXXON_LEI)
    ctx.jurisdiction = "US"
    ctx.registered_as = "0000019017"
    ctx.legal_name = "EXXON MOBIL CORPORATION"
    _build_derived(ctx, "")

    tasks = _dispatch(ctx, only="eiti")
    assert [sid for sid, _ in tasks] == ["eiti"]
    bundle = await tasks[0][1]
    assert bundle is not None
    assert bundle["country"] == "US"
    assert bundle["identification"] == _EXXON_EIN
    assert bundle["is_stub"] is False

    # And the hit corroborates on us_ein, not on the state number.
    hit = _bh_eiti(bundle, ctx)
    assert hit.identifiers == {"us_ein": _EXXON_EIN}


# ---------------------------------------------------------------------------
# GLEIF publishes a US jurisdiction as an ISO 3166-2 SUBDIVISION (Exxon Mobil
# is "US-NJ"), not "US". Phase 155 shipped the crosswalk with an `== "US"`
# test in _build_derived and passed ctx.jurisdiction straight to the adapter,
# so US matching stayed dead in production even though every test passed --
# the tests fed "US", a value the pipeline never actually supplies. These
# pin the real shape.
# ---------------------------------------------------------------------------

#: Exxon Mobil's actual GLEIF jurisdiction, as returned by production.
_EXXON_JURISDICTION = "US-NJ"


def test_country_code_reduces_a_gleif_subdivision() -> None:
    assert _country_code("US-NJ") == "US"
    assert _country_code("us-de") == "US"
    assert _country_code("GB") == "GB"
    # Unrecognisable values pass through rather than being coerced into some
    # other country's bucket -- failing to match beats matching the wrong one.
    assert _country_code("") == ""
    assert _country_code("NOT-A-JURISDICTION") == "NOT-A-JURISDICTION"


def test_match_identification_accepts_a_subdivision_jurisdiction() -> None:
    assert _match_identification("US-NJ", _EXXON_EIN) == _EXXON_EIN
    # The country still binds: a US EIN under a GB subdivision must not match.
    assert _match_identification("GB-SCT", _EXXON_EIN) is None


def test_build_derived_populates_us_ein_under_a_subdivision() -> None:
    """The regression that made Phase 155 a no-op in production."""
    ctx = _LookupCtx(lei=_EXXON_LEI)
    ctx.jurisdiction = _EXXON_JURISDICTION
    ctx.registered_as = ""  # GLEIF publishes none for this record
    _build_derived(ctx, "")
    assert ctx.derived["us_ein"] == _EXXON_EIN


async def test_fetch_by_registration_matches_under_a_subdivision() -> None:
    """A subdivision jurisdiction matches, and the bundle is stamped with the
    COUNTRY. A bundle carrying "US-NJ" would match yet still lose the us_ein
    corroboration key downstream, so the country is asserted, not just the
    hit."""
    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration(
        _EXXON_JURISDICTION, "", legal_name="EXXON MOBIL CORPORATION",
        us_ein=_EXXON_EIN,
    )
    assert bundle is not None
    assert bundle["country"] == "US"
    assert bundle["identification"] == _EXXON_EIN


async def test_us_subject_matches_end_to_end_under_a_subdivision() -> None:
    """The production path, with the production jurisdiction value: derive ->
    dispatch -> match -> corroborate on us_ein. This is the check that the
    2026-09-04 Exxon spot-check failed."""
    ctx = _LookupCtx(lei=_EXXON_LEI)
    ctx.jurisdiction = _EXXON_JURISDICTION
    ctx.registered_as = ""
    ctx.legal_name = "EXXON MOBIL CORPORATION"
    _build_derived(ctx, "")
    assert ctx.derived.get("us_ein") == _EXXON_EIN

    tasks = _dispatch(ctx, only="eiti")
    assert [sid for sid, _ in tasks] == ["eiti"]
    bundle = await tasks[0][1]
    assert bundle is not None
    assert bundle["country"] == "US"

    hit = _bh_eiti(bundle, ctx)
    assert hit.hit_id == f"US:{_EXXON_EIN}"
    assert hit.identifiers == {"us_ein": _EXXON_EIN}


async def test_fetch_by_hit_id_accepts_a_subdivision() -> None:
    """The deepen/retry path takes ``CC:ident`` and must normalise it too."""
    adapter = EitiAdapter()
    bundle = await adapter.fetch(f"US-NJ:{_EXXON_EIN}")
    assert bundle.get("is_stub") is not True
    assert bundle["country"] == "US"


# ---------------------------------------------------------------------------
# Zambia (Phase 306). EITI's 94 Zambian identifications are ZRA TPINs; GLEIF
# files a Zambian company's PACRA number, so registeredAs never joined the ZM
# bucket and the source could not hit for any Zambian LEI. The Zambia EITI
# portal index (Phase 298) already tied each Zambian LEI to its TPIN by name;
# these pin the TPIN it supplies as a derived key, the US EIN pattern above.
# ---------------------------------------------------------------------------

#: Kansanshi Mining PLC: GLEIF files PACRA 119970037529 (RA000652) under jurisdiction ZM;
#: EITI files TPIN 1001602517 for "Kansanshi Mining Plc".
_KANSANSHI_LEI = "2549008ZVFBSUO8W2L37"
_KANSANSHI_PACRA = "119970037529"
_KANSANSHI_TPIN = "1001602517"

#: The Zambian LEIs whose portal TPIN EITI International also holds, as of the
#: Phase 298 build and the committed EITI index. A lower bound: either index
#: can grow, but a rebuild that drops one of these is a silent loss.
_ZM_LEIS_IN_EITI = {
    "213800ATOLDC9CX44W14": "1001594184",  # Maamba Collieries
    "213800ZIJ5Y4FXZMPD33": "1001862964",  # FQM Trident (EITI: Kalumbila Minerals)
    "2549008ZVFBSUO8W2L37": "1001602517",  # Kansanshi Mining
    "254900SRGQ7I4WMQOC08": "1001772785",  # Konkola Copper Mines
    "5493005OY00M9G3XSY51": "1001761145",  # ZCCM Investments Holdings
    "549300YDL92W1M757708": "1001591709",  # CNMC Luanshya Copper Mines
    "9845008756E7B43CDE48": "1001831030",  # Chambishi Copper Smelter
}


def test_pacra_number_alone_never_joins_the_zm_bucket() -> None:
    """The gap this phase closes: GLEIF's registeredAs for a Zambian LEI is a
    PACRA number, and nothing in EITI's ZM bucket is one."""
    assert _match_identification("ZM", _KANSANSHI_PACRA) is None
    assert _match_identification("ZM", _KANSANSHI_TPIN) == _KANSANSHI_TPIN


def test_zambia_index_tpins_join_the_eiti_zm_bucket() -> None:
    """Every pinned LEI's portal TPIN still matches EITI's ZM bucket. The two
    indexes are rebuilt by different scripts; this is what notices a rebuild
    that silently breaks the join."""
    from opencheck.sources.eiti_zambia import tpin_for_lei

    for lei, tpin in _ZM_LEIS_IN_EITI.items():
        assert tpin_for_lei(lei) == tpin, lei
        assert _match_identification("ZM", tpin) == tpin, lei


def test_tpin_for_lei_is_empty_off_the_index_and_for_a_name_only_record() -> None:
    from opencheck.sources.eiti_zambia import tpin_for_lei

    assert tpin_for_lei(_KANSANSHI_LEI.lower()) == _KANSANSHI_TPIN
    assert tpin_for_lei("X" * 20) == ""
    assert tpin_for_lei("") == ""
    # Blaze Metals is in the portal index by cadastre name only -- no TPIN.
    assert tpin_for_lei("984500ECC4F4DF950B64") == ""


def test_build_derived_populates_zm_tpin_for_a_zambian_lei() -> None:
    ctx = _LookupCtx(lei=_KANSANSHI_LEI)
    ctx.jurisdiction = "ZM"
    ctx.registered_as = _KANSANSHI_PACRA
    _build_derived(ctx, "RA000652")
    assert ctx.derived["zm_tpin"] == _KANSANSHI_TPIN
    # The PACRA number is what GLEIF publishes; it is not reused as a TPIN.
    assert ctx.derived["zm_tpin"] != ctx.registered_as


def test_build_derived_omits_zm_tpin_off_the_index_or_outside_zambia() -> None:
    """No key at all rather than an empty string, as for us_ein."""
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "ZM"
    _build_derived(ctx, "")
    assert "zm_tpin" not in ctx.derived

    ctx2 = _LookupCtx(lei=_KANSANSHI_LEI)
    ctx2.jurisdiction = "GB"
    ctx2.registered_as = "01285743"
    _build_derived(ctx2, "")
    assert "zm_tpin" not in ctx2.derived


def test_dispatch_includes_eiti_via_derived_zm_tpin() -> None:
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "ZM"
    ctx.registered_as = ""
    ctx.derived = {"zm_tpin": _KANSANSHI_TPIN}
    tasks = _dispatch(ctx, only="eiti")
    assert [sid for sid, _ in tasks] == ["eiti"]
    for _, coro in tasks:
        coro.close()


async def test_fetch_by_registration_matches_via_zm_tpin() -> None:
    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration(
        "ZM", _KANSANSHI_PACRA, legal_name="Kansanshi Mining PLC",
        zm_tpin=_KANSANSHI_TPIN,
    )
    assert bundle is not None
    assert bundle["country"] == "ZM"
    assert bundle["identification"] == _KANSANSHI_TPIN
    assert bundle["matched_via"] == "zm_tpin"


async def test_a_tpin_never_matches_outside_the_zm_bucket() -> None:
    """Matching is country-scoped: a TPIN handed over with another
    jurisdiction cannot borrow a ZM record."""
    adapter = EitiAdapter()
    assert await adapter.fetch_by_registration(
        "GB", "", zm_tpin=_KANSANSHI_TPIN
    ) is None
    assert await adapter.fetch_by_registration(
        "ZM", "", zm_tpin="9999999999"
    ) is None


async def test_registered_as_still_wins_and_says_so() -> None:
    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration("GB", "01285743")
    assert bundle is not None
    assert bundle["matched_via"] == "registered_as"


async def test_zambian_subject_matches_eiti_end_to_end() -> None:
    """Derive -> dispatch -> match -> corroborate, with the production
    jurisdiction and registeredAs, nothing hand-fed."""
    ctx = _LookupCtx(lei=_KANSANSHI_LEI)
    ctx.jurisdiction = "ZM"
    ctx.registered_as = _KANSANSHI_PACRA
    ctx.legal_name = "Kansanshi Mining PLC"
    _build_derived(ctx, "RA000652")

    tasks = _dispatch(ctx, only="eiti")
    assert [sid for sid, _ in tasks] == ["eiti"]
    bundle = await tasks[0][1]
    assert bundle is not None
    assert bundle["country"] == "ZM"
    assert bundle["identification"] == _KANSANSHI_TPIN
    assert bundle["matched_via"] == "zm_tpin"
    # Every spelling's records are read: 2012-13 are filed as
    # "1,001,602,517", 2014-18 as "1001602517".
    assert bundle["years"] == ["2018", "2017", "2016", "2015", "2014", "2013", "2012"]

    # Corroboration: EITI publishes the TPIN, so it is asserted under the
    # same key the Zambia portal card asserts. The LEI is never asserted.
    hit = _bh_eiti(bundle, ctx)
    assert hit.hit_id == f"ZM:{_KANSANSHI_TPIN}"
    assert hit.identifiers == {"zm_tpin": _KANSANSHI_TPIN}


def test_eiti_and_eiti_zambia_hits_assert_the_same_tpin_key() -> None:
    """The two EITI publishers corroborate on one key, so the reconciler can
    say both file the number -- and neither asserts the LEI."""
    from opencheck.routers.hit_builders import _bh_eiti_zambia
    from opencheck.sources.eiti_zambia import EitiZambiaAdapter, _load

    ctx = _LookupCtx(lei=_KANSANSHI_LEI)
    index, meta = _load()
    zm_bundle = EitiZambiaAdapter()._build_bundle(
        _KANSANSHI_LEI, index[_KANSANSHI_LEI], meta
    )
    zm_hit = _bh_eiti_zambia(zm_bundle, ctx)
    eiti_hit = _bh_eiti(
        {
            "country": "ZM", "identification": _KANSANSHI_TPIN,
            "entity_name": "Kansanshi Mining Plc", "years": ["2018"],
            "total_usd": 0.0,
        },
        ctx,
    )
    assert zm_hit.identifiers == eiti_hit.identifiers == {"zm_tpin": _KANSANSHI_TPIN}
    assert "lei" not in eiti_hit.identifiers


def test_map_eiti_zambia_identification_carries_the_tpin_scheme() -> None:
    """The BODS identifier uses the scheme the portal mapper writes, so the
    two publishers' entity statements share it."""
    from opencheck.bods.mappers.eiti import ZM_TPIN_SCHEME

    stmts = list(map_eiti({
        "source_id": "eiti", "country": "ZM", "identification": _KANSANSHI_TPIN,
        "entity_name": "Kansanshi Mining Plc", "organisations": [],
        "revenue_years": [], "streams": {}, "total_usd": 0.0, "years": ["2018"],
        "is_stub": False,
    }))
    entity = next(s for s in stmts if s.get("recordType") == "entity")
    ids = entity["recordDetails"]["identifiers"]
    assert {"id": _KANSANSHI_TPIN, "scheme": ZM_TPIN_SCHEME[0]}.items() <= ids[0].items()
    assert entity["recordDetails"]["jurisdiction"]["code"] == "ZM"


# ---------------------------------------------------------------------------
# One number, several spellings (Phase 306). EITI files the same company's
# number differently across reporting years; the card read only the years
# under the spelling that matched.
# ---------------------------------------------------------------------------


def test_exact_spelling_wins_over_a_punctuated_variant() -> None:
    assert _match_identification("ZM", "1001602517") == "1001602517"
    assert _match_identification("ZM", "1,001,602,517") == "1,001,602,517"


def test_organisations_reads_every_spelling_of_a_number() -> None:
    from opencheck.sources.eiti import _organisations

    years = sorted(o["year"] for o in _organisations("ZM", "1001602517"))
    assert years == ["2012", "2013", "2014", "2015", "2016", "2017", "2018"]
    assert _organisations("ZM", "1,001,602,517") == _organisations("ZM", "1001602517")
    # Equinor UK: 2023 is filed without the leading zero.
    gb_years = {o["year"] for o in _organisations("GB", "01285743")}
    assert {"2023", "2021", "2018"} <= gb_years


def test_a_lettered_number_is_never_merged_with_its_digits() -> None:
    """SC123456 (Scotland) and 00123456 (England & Wales) are different
    companies; the variant grouping keys a lettered number on itself."""
    from opencheck.sources.eiti import _variant_key

    assert _variant_key("SC123456") == "SC123456"
    assert _variant_key("00123456") == _variant_key("123,456") == "123456"
    assert _variant_key("400 182 426") == _variant_key("400182426")
