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
    _get_index,
    _match_identification,
    _norm_forms,
    us_ein_for_lei,
)

_API = "https://eiti.org/api/v2.0"


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
    for variant in ("01285743", "1285743", "01-28-5743"):
        assert _match_identification("GB", variant) == "01285743", variant
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
    org_ids = [o["id"] for o in index["GB"]["01285743"]][:4]
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
    for o in index["GB"]["01285743"][:4]:
        httpx_mock.add_response(
            url=f"{_API}/revenue?organisation={o['id']}&limit=50", status_code=500
        )

    adapter = EitiAdapter()
    bundle = await adapter.fetch_by_registration("GB", "01285743")
    assert bundle is not None  # org match survives; payments empty
    assert bundle["total_usd"] == 0.0
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
    assert set(_EITI_IDENTIFIER_KEY_BY_COUNTRY) == {"GB", "NO", "NL", "US"}


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


def test_map_eiti_unknown_country_omits_scheme() -> None:
    bundle = {
        "source_id": "eiti",
        "country": "MN",
        "identification": "2016656",
        "entity_name": "Tavantolgoi JSC",
        "is_stub": False,
    }
    statements = list(map_eiti(bundle))
    ident = statements[0]["recordDetails"]["identifiers"][0]
    assert "scheme" not in ident
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
