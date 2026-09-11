"""INPI: which French LEIs reach the register, and what a miss says (Phase 205).

Stephen looked up Transparency International France (969500AEH12X8M5XEO53)
and got a 404 from INPI. The register was up. The investigation found three
defects behind that one error, each pinned here:

1. **A 404 read as a failing source.** The RNE does not hold associations or
   foundations without a commercial registration, so a 404 is the register's
   ordinary answer. ``_get_company`` raised on it, and every French
   association with an LEI showed an INPI error.
2. **Spaced SIRENs.** GLEIF stores ``registeredAs`` either plain
   (``552032534``) or grouped in threes (``542 051 180``) — 391 records sampled
   on 11 Sept 2026, nothing else seen. ``normalise_siren`` stripped only the
   ends, so COACH AND GO (``941 395 501``) was requested as
   ``/companies/941%20395%20501`` and 404'd although the RNE holds it.
3. **Infogreffe LEIs never dispatched.** France files LEIs under ``RA000189``
   (Sirene, 149,237 active) and ``RA000192`` (Infogreffe, 8,145 active), both
   carrying the SIREN. Only ``RA000189`` reached INPI, so TotalEnergies SE got
   no INPI result and no ``siren``, silently.

And a fourth, found while fixing the second: ``/resolve-national-id`` matched
``registeredAs`` exactly and scoped to ``RA000189`` alone, so ``country=FR``
could not find TotalEnergies by its SIREN either.

The weekly sweep saw none of it: its subject is a commercial ``RA000189``
company with an unspaced SIREN — the one shape that worked.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import httpx
import pytest
import respx

from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, map_gleif
from opencheck.register_hops import hop_for
from opencheck.routers.lookup import _LookupCtx, _build_derived, _build_result_hit
from opencheck.sources.gleif import GleifAdapter
from opencheck.findings import INPI_NOT_IN_RNE, MAX_FINDING_CHARS
from opencheck.mcp.shaping import _sources_summary
from opencheck.routers.hit_builders import _bh_inpi
from opencheck.sources.inpi import (
    COVERAGE_404,
    INFOGREFFE_RA_CODE,
    INPI_RA_CODE,
    INPI_RA_CODES,
    InpiAdapter,
    normalise_siren,
    siren_spellings,
)
from opencheck.sources.probes import PROBES

AUTH_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
API = "https://registre-national-entreprises.inpi.fr/api/companies"


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("INPI_USERNAME", "user")
    monkeypatch.setenv("INPI_PASSWORD", "pass")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# 2. The SIREN, as GLEIF writes it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("552032534", "552032534"),  # Danone, RA000189, plain
        ("941 395 501", "941395501"),  # COACH AND GO, RA000189, spaced
        ("542 051 180", "542051180"),  # TotalEnergies SE, RA000192, spaced
        ("086 280 393", "086280393"),  # John Deere SAS — the leading zero stays
        ("  055804124  ", "055804124"),
        ("542\u00a0051\u00a0180", "542051180"),  # no-break space
        ("542\u202f051\u202f180", "542051180"),  # narrow no-break space
        ("12345678", "012345678"),  # a lost leading zero is restored
    ],
)
def test_normalise_siren(raw: str, expected: str) -> None:
    assert normalise_siren(raw) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "   ",
        "RCS Paris 542 051 180",
        "B 542 051 180",
        "542-051-180",
        "5420511800",  # ten digits
        "54205118000013",  # a SIRET is not a SIREN
        "\uff15\uff14\uff12\uff10\uff15\uff11\uff11\uff18\uff10",  # full-width digits pass str.isdigit(); not a SIREN
    ],
)
def test_normalise_siren_rejects_what_is_not_a_siren(bad: str) -> None:
    with pytest.raises(ValueError):
        normalise_siren(bad)


def test_siren_spellings() -> None:
    assert siren_spellings("542051180") == ("542051180", "542 051 180")
    assert siren_spellings("542 051 180") == ("542051180", "542 051 180")
    assert siren_spellings(" RCS Paris ") == ("RCS Paris",)


# ---------------------------------------------------------------------------
# 3. Both French authorities dispatch INPI
# ---------------------------------------------------------------------------


def test_both_french_authorities_are_claimed() -> None:
    assert INPI_RA_CODE == "RA000189"
    assert INFOGREFFE_RA_CODE == "RA000192"
    deriver = InpiAdapter.lookup_derivers[0]
    assert deriver.ra_codes == INPI_RA_CODES == frozenset({"RA000189", "RA000192"})
    assert deriver.derived_key == "siren"


@pytest.mark.parametrize(
    ("ra_code", "registered_as", "expected"),
    [
        ("RA000189", "552032534", "552032534"),  # Danone — worked before
        ("RA000189", "941 395 501", "941395501"),  # COACH AND GO — spaced
        ("RA000192", "542 051 180", "542051180"),  # TotalEnergies — Infogreffe
        ("RA000192", "939294146", "939294146"),  # RONCKET IO — Infogreffe, plain
    ],
)
def test_lookup_derives_a_clean_siren(ra_code: str, registered_as: str, expected: str) -> None:
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "FR"
    ctx.registered_as = registered_as
    _build_derived(ctx, ra_code)
    assert ctx.derived.get("siren") == expected


def test_a_malformed_french_number_skips_inpi() -> None:
    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "FR"
    ctx.registered_as = "RCS Nanterre B 542 051 180"
    _build_derived(ctx, "RA000192")
    assert "siren" not in ctx.derived


def _gleif_item(ra_code: str, registered_as: str) -> dict[str, Any]:
    return {
        "id": "529900S21EQ1BO4ESM68",
        "attributes": {
            "lei": "529900S21EQ1BO4ESM68",
            "entity": {
                "legalName": {"name": "TotalEnergies SE"},
                "jurisdiction": "FR",
                "status": "ACTIVE",
                "registeredAs": registered_as,
                "registeredAt": {"id": ra_code},
            },
        },
    }


def test_gleif_hit_bridges_on_the_siren_under_both_authorities() -> None:
    for ra_code in ("RA000189", "RA000192"):
        hit = GleifAdapter._entity_hit(_gleif_item(ra_code, "542 051 180"))
        assert hit.identifiers["siren"] == "542051180", ra_code
        # The verbatim value is still carried under the generic key.
        assert hit.identifiers["registered_as_fr"] == "542 051 180"


def test_gleif_hit_carries_no_siren_it_cannot_read() -> None:
    hit = GleifAdapter._entity_hit(_gleif_item("RA000192", "RCS Nanterre"))
    assert "siren" not in hit.identifiers


def test_infogreffe_takes_the_siren_scheme() -> None:
    assert _GLEIF_RA_TO_ORG_ID["RA000192"][0] == _GLEIF_RA_TO_ORG_ID["RA000189"][0] == "FR-INSEE"
    record = _gleif_item("RA000192", "542 051 180")
    bundle = map_gleif({"lei": record["id"], "record": record})
    entity = next(s for s in bundle if s.get("recordType") == "entity")
    ids = entity["recordDetails"]["identifiers"]
    siren = [i for i in ids if i["scheme"] == "FR-INSEE"]
    # The number is published as GLEIF filed it; merging canonicalises separators.
    assert [i["id"] for i in siren] == ["542 051 180"]
    assert "Infogreffe" in siren[0]["schemeName"]


def test_france_still_has_one_register_hop() -> None:
    """One scheme for both authorities keeps one hop — and the REG-FR alias,
    which is only created for a country with exactly one."""
    hop = hop_for("FR-INSEE")
    assert hop is not None and hop.source_id == "inpi"
    assert hop.normalise("542 051 180") == "542051180"
    alias = hop_for("REG-FR")
    assert alias is not None and alias.source_id == "inpi"


# ---------------------------------------------------------------------------
# 1. A 404 is "not in this register", not an error
# ---------------------------------------------------------------------------


def _login() -> respx.Route:
    return respx.post(AUTH_URL).mock(return_value=httpx.Response(200, json={"token": "t"}))


@respx.mock
async def test_a_404_is_a_not_found_bundle() -> None:
    _login()
    route = respx.get(f"{API}/425138393").mock(
        return_value=httpx.Response(404, json={"message": "Not Found"})
    )
    adapter = InpiAdapter()

    bundle = await adapter.fetch("425138393")  # Transparency International France

    assert route.called
    assert bundle == {
        "source_id": "inpi",
        "siren": "425138393",
        "company": None,
        "is_stub": False,
        "not_found": True,
        "coverage_note": COVERAGE_404,
    }
    # Not cached: a company registered tomorrow must be found tomorrow.
    assert adapter._cache.get_payload("inpi/company/425138393") is None


@respx.mock
async def test_a_spaced_siren_is_requested_clean() -> None:
    """The exact request COACH AND GO failed on, now sent without spaces."""
    _login()
    clean = respx.get(f"{API}/941395501").mock(
        return_value=httpx.Response(200, json={"siren": "941395501", "diffusionINSEE": "O"})
    )
    bundle = await InpiAdapter().fetch("941 395 501")
    assert clean.called
    assert bundle["company"]["siren"] == "941395501"
    assert bundle["is_stub"] is False
    assert "not_found" not in bundle


@respx.mock
async def test_a_404_after_a_token_refresh_is_still_a_miss() -> None:
    _login()
    adapter = InpiAdapter()
    adapter._token = "stale"

    def _company(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Authorization") == "Bearer stale":
            return httpx.Response(401)
        return httpx.Response(404)

    respx.get(f"{API}/503960643").mock(side_effect=_company)
    assert await adapter._get_company("503960643") is None


@pytest.mark.parametrize("status", [400, 403, 429, 500, 503])
@respx.mock
async def test_other_failures_still_raise(status: int) -> None:
    """Only a 404 is an answer. Everything else is still a source error."""
    _login()
    respx.get(f"{API}/552032534").mock(return_value=httpx.Response(status))
    with pytest.raises(httpx.HTTPStatusError):
        await InpiAdapter().fetch("552032534")


def _ctx_with_siren(siren: str) -> _LookupCtx:
    ctx = _LookupCtx(lei="969500AEH12X8M5XEO53")
    ctx.jurisdiction = "FR"
    ctx.legal_name = "TRANSPARENCY INTERNATIONAL FRANCE"
    ctx.derived["siren"] = siren
    return ctx


def _not_found_bundle() -> dict[str, Any]:
    return {
        "source_id": "inpi", "siren": "425138393", "company": None,
        "is_stub": False, "not_found": True, "coverage_note": COVERAGE_404,
    }


def test_a_not_found_bundle_is_a_note_card_that_asserts_nothing() -> None:
    """Stephen, 11 Sept: explain the gap with a short note, as KvK does, rather
    than showing no INPI card. The card must still assert nothing: no ``siren``
    (the reconciler would count it as corroboration) and no company data."""
    hit = _build_result_hit("inpi", _not_found_bundle(), _ctx_with_siren("425138393"))
    assert hit is not None
    assert hit.name == "TRANSPARENCY INTERNATIONAL FRANCE"  # the GLEIF name
    assert hit.identifiers == {}
    assert hit.raw == {"coverage_note": COVERAGE_404, "not_found": True}
    assert hit.is_stub is False
    # The row itself says it: the drawer holding the coverage note is not
    # opened for a card with no statements.
    assert hit.finding == INPI_NOT_IN_RNE
    assert len(INPI_NOT_IN_RNE) <= MAX_FINDING_CHARS
    assert "not in the RNE" in hit.summary


def test_the_note_says_what_the_register_covers_without_overclaiming() -> None:
    assert COVERAGE_404.startswith("Not in the Registre National des Entreprises.")
    assert "associations and foundations" in COVERAGE_404
    # A 404 alone does not prove the entity is an association.
    assert "usually" in COVERAGE_404
    assert "error" in COVERAGE_404  # says it is not one
    assert len(COVERAGE_404) < 400


def test_mcp_reports_the_note_card_as_not_found() -> None:
    """A note card is not data: ``found`` stays false (so "N of M sources
    returned data" does not count it) and the note travels with the row."""
    note_hit = _bh_inpi(_not_found_bundle(), "425138393", _ctx_with_siren("425138393"))
    rows = {r["id"]: r for r in _sources_summary([note_hit], {})}
    assert rows["inpi"]["found"] is False
    assert rows["inpi"]["note"] == COVERAGE_404
    assert "error" not in rows["inpi"]


def test_a_found_bundle_is_still_a_hit() -> None:
    company = {"siren": "552032534", "identite": {"entreprise": {"denomination": "DANONE"}}}
    bundle = {"source_id": "inpi", "siren": "552032534", "company": company, "is_stub": False}
    hit = _build_result_hit("inpi", bundle, _ctx_with_siren("552032534"))
    assert hit is not None
    assert hit.name == "DANONE"
    assert hit.finding is None  # unchanged: a company row falls back to its summary
    assert "coverage_note" not in hit.raw
    assert hit.identifiers == {"siren": "552032534"}


def test_the_health_probe_cannot_go_green_on_a_miss() -> None:
    """With a 404 no longer raising, only ``expect_fields`` stops a register
    that has stopped answering for the probe SIREN from reading as healthy."""
    assert "company" in PROBES["inpi"].expect_fields


# ---------------------------------------------------------------------------
# 4. Reverse lookup: SIREN → LEI finds both spellings under both authorities
# ---------------------------------------------------------------------------


async def _search_paths(local_id: str, ra_code: str) -> list[str]:
    adapter = GleifAdapter()
    get = AsyncMock(return_value={"data": []})
    with patch.object(
        GleifAdapter, "info", new_callable=PropertyMock,
        return_value=MagicMock(live_available=True),
    ), patch.object(adapter, "_get", get):
        await adapter.search_by_local_id(local_id, ra_code=ra_code)
    return [call.args[0] for call in get.await_args_list]


@pytest.mark.parametrize("ra_code", ["RA000189", "RA000192"])
async def test_a_french_siren_is_searched_in_both_spellings_under_both_authorities(
    ra_code: str,
) -> None:
    paths = await _search_paths("542051180", ra_code)
    assert len(paths) == 3  # still one request per local-id field
    for path in paths:
        assert "=542051180,542%20051%20180&" in path, path
        assert path.endswith("&filter[entity.registeredAt]=RA000189,RA000192"), path


async def test_other_countries_are_searched_exactly_as_before() -> None:
    paths = await _search_paths("SC651281", "RA000587")
    assert paths[0] == (
        "/lei-records?page[size]=5&filter[entity.registeredAs]=SC651281"
        "&filter[entity.registeredAt]=RA000587"
    )
    unscoped = await _search_paths("00102498", "")
    assert unscoped[0] == "/lei-records?page[size]=5&filter[entity.registeredAs]=00102498"
