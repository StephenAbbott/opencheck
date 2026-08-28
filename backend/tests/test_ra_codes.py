"""GLEIF Registration Authority codes — pinned, and kept in step across copies.

OpenCheck stores a country → RA code mapping in **three** places:

* ``backend/opencheck/routers/lookup.py::_RA_BY_COUNTRY`` — scopes the
  national-ID → LEI reverse lookup.
* ``frontend/src/lib/raCodes.ts`` — the country picker that feeds it.
* the RA table in ``CLAUDE.md`` — documentation.

On 2026-08-28 all three had drifted, and in the same direction: the table was
wrong in nine of eighteen rows, the backend map in nine of nineteen, and the
frontend map in **eleven of twenty**. Every wrong value was a real RA code
belonging to a different authority — Norway pointed at India's MCA, Sweden at
Singapore's ACRA — so the reverse lookup filtered on an authority the company
was not registered at and returned nothing. It failed closed, which is exactly
why it survived: a missed match looks like an absent company.

Two of them (Norway, Sweden) had already been corrected in the backend and in
CLAUDE.md months earlier without the frontend being touched, which is the case
for testing the copies against each other rather than each against a comment.

Every code below was verified against **live GLEIF records** — reading
``registeredAt.id`` off real entities via
``filter[entity.legalAddress.country]`` — not against the RA catalogue, which
only says which authorities exist. Note also that ``filter[country]`` on the
catalogue endpoint is silently ignored and returns the unfiltered global list,
so spot-checking a code that way appears to confirm whatever you already
believed. That is the most likely origin of the original values.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from opencheck.ra_codes import RA_BY_COUNTRY, SUB_REGISTRIES, ra_code_for
from opencheck.routers.lookup import _RA_BY_COUNTRY, _resolve_national_id_impl
from opencheck.sources import REGISTRY

_FRONTEND_DIR = Path(__file__).resolve().parents[2] / "frontend" / "src"
_FRONTEND_RA_CODES = _FRONTEND_DIR / "lib" / "raCodes.ts"
_FRONTEND_APP = _FRONTEND_DIR / "App.tsx"

#: Country → RA code, verified live 2026-08-28. This is the reference the other
#: copies are checked against; changing a value here without re-verifying it
#: against live GLEIF records defeats the point of the test.
VERIFIED: dict[str, str] = {
    "AT": "RA000017",  # Firmenbuch
    "BE": "RA000025",  # Crossroad Bank of Enterprises
    "BR": "RA000681",  # Receita Federal CNPJ
    "CA": "RA000072",  # Corporations Canada (federal)
    "DK": "RA000170",  # CVR
    "EE": "RA000181",  # Commercial Register
    "FR": "RA000189",  # Sirene / INSEE
    "GB": "RA000585",  # Companies House, England & Wales
    "GR": "RA000685",  # ΓΕΜΗ
    "HR": "RA000156",  # Sudski registar
    "IE": "RA000402",  # CRO
    "LT": "RA000430",  # Registrų centras
    "LV": "RA000423",  # Uzņēmumu Reģistrs
    "MT": "RA000443",  # Malta Business Registry
    "NL": "RA000463",  # KvK
    "NO": "RA000472",  # Foretaksregisteret
    "NZ": "RA000466",  # Companies Office
    "PL": "RA000484",  # KRS
    "SE": "RA000544",  # Bolagsverket
    "SG": "RA000523",  # ACRA
    "SK": "RA000526",  # Business Register
}

#: RA codes that are NOT company registries and must never appear in a
#: country map. RA000591 sat in the Companies House helper as Northern
#: Ireland's code for months.
NOT_A_COMPANY_REGISTRY: frozenset[str] = frozenset({
    "RA000591",  # The Pensions Regulator (UK)
    "RA000588",  # Mutuals Public Register (FCA)
    "RA000589",  # Charity Commission for England and Wales
    "RA000592",  # Financial Services Register (FCA)
})


def _frontend_map() -> dict[str, str]:
    """Parse ``RA_CODES`` out of the frontend module.

    Deliberately a parse of the real file rather than a duplicated literal:
    a copy here would be a fourth place to drift.
    """
    source = _FRONTEND_RA_CODES.read_text(encoding="utf-8")
    pairs = re.findall(r'\n  ([A-Z]{2}): \{\s*\n\s*raCode: "(RA\d{6})"', source)
    assert pairs, "could not parse RA_CODES out of raCodes.ts — has its shape changed?"
    return dict(pairs)


@pytest.mark.parametrize("country", sorted(_RA_BY_COUNTRY))
def test_backend_map_matches_verified_codes(country: str) -> None:
    assert country in VERIFIED, f"{country} is in _RA_BY_COUNTRY but not verified"
    assert _RA_BY_COUNTRY[country] == VERIFIED[country]


@pytest.mark.parametrize("country", sorted(_frontend_map()))
def test_frontend_map_matches_verified_codes(country: str) -> None:
    assert country in VERIFIED, f"{country} is in raCodes.ts but not verified"
    assert _frontend_map()[country] == VERIFIED[country]


def test_frontend_and_backend_agree() -> None:
    """The picker must scope the lookup to the registry the backend expects.

    Checked directly rather than only via ``VERIFIED``, because the failure
    that actually happened was the two copies diverging while each looked
    plausible on its own.
    """
    frontend = _frontend_map()
    shared = sorted(set(frontend) & set(_RA_BY_COUNTRY))
    assert shared, "the two maps share no countries — one of them failed to parse"
    mismatched = {
        c: (frontend[c], _RA_BY_COUNTRY[c])
        for c in shared
        if frontend[c] != _RA_BY_COUNTRY[c]
    }
    assert not mismatched, f"frontend/backend RA codes diverge: {mismatched}"


def test_no_country_maps_to_a_non_registry() -> None:
    for source, mapping in (("backend", _RA_BY_COUNTRY), ("frontend", _frontend_map())):
        offenders = {c: ra for c, ra in mapping.items() if ra in NOT_A_COMPANY_REGISTRY}
        assert not offenders, f"{source} maps a country to a non-registry: {offenders}"


def test_no_two_countries_share_a_code() -> None:
    """Every wrong value in the original maps was another country's real code.

    Norway held India's MCA code and Sweden held Singapore's ACRA code, so a
    collision check would have caught two of the eleven on its own.
    """
    seen: dict[str, str] = {}
    for country, ra in sorted(VERIFIED.items()):
        assert ra not in seen, f"{country} and {seen[ra]} both claim {ra}"
        seen[ra] = country


# ---------------------------------------------------------------------------
# Sub-registries: the country map alone is not the answer (Phase 141)
# ---------------------------------------------------------------------------
#
# Phase 140 corrected the values in every copy and left the shape, and the
# shape was the other half of the bug. The Companies House prefix rule lived
# in routers/search.py and the country map in routers/lookup.py, and only the
# /search bridge consulted both — so /resolve-national-id with country="GB"
# scoped a Scottish number to the England & Wales authority and returned
# nothing. Same failure mode as before: closed, silent, indistinguishable from
# a company that does not exist.

#: Companies House number prefix → the authority it is really filed under.
#: Verified live: THON MARITIME LTD, registeredAs "SC651281", sits under
#: RA000587.
GB_SUB_REGISTRIES: dict[str, str] = {
    "SC": "RA000587",  # Scottish limited companies
    "SO": "RA000587",  # Scottish limited partnerships
    "SF": "RA000587",  # Scottish qualifying partnerships
    "NI": "RA000586",  # Northern Ireland
    "NC": "RA000586",  # Northern Ireland, pre-2009 registrations
    "R0": "RA000586",  # Northern Ireland, pre-2009 registrations
}


def _frontend_sub_registries(country: str) -> dict[str, str]:
    """Parse one country's ``subRegistries`` prefix → RA code out of raCodes.ts."""
    source = _FRONTEND_RA_CODES.read_text(encoding="utf-8")
    block = re.search(
        rf"\n  {country}: \{{(.*?)\n  \}},\n", source, re.DOTALL
    )
    assert block, f"could not find the {country} entry in raCodes.ts"
    out: dict[str, str] = {}
    for prefixes, ra in re.findall(
        r'prefixes: \[([^\]]*)\], raCode: "(RA\d{6})"', block.group(1)
    ):
        for prefix in re.findall(r'"([A-Z0-9]+)"', prefixes):
            out[prefix] = ra
    return out


@pytest.mark.parametrize("prefix,expected", sorted(GB_SUB_REGISTRIES.items()))
def test_gb_prefix_selects_the_right_authority(prefix: str, expected: str) -> None:
    assert ra_code_for("GB", f"{prefix}123456") == expected
    # Case must not matter — users paste company numbers as they find them.
    assert ra_code_for("GB", f"{prefix.lower()}123456") == expected


def test_gb_without_a_prefix_keeps_the_default() -> None:
    assert ra_code_for("GB", "00102498") == "RA000585"


def test_ra_code_for_without_a_number_is_the_country_default() -> None:
    """The question "which registry is this country's" still has an answer."""
    for country, ra in RA_BY_COUNTRY.items():
        assert ra_code_for(country, "") == ra


def test_unknown_country_scopes_nothing_rather_than_guessing() -> None:
    """An unscoped GLEIF query is a wider search; a wrong scope finds nothing."""
    assert ra_code_for("ZZ", "12345") == ""
    assert ra_code_for("", "12345") == ""


def test_backend_and_frontend_sub_registries_agree() -> None:
    assert _frontend_sub_registries("GB") == GB_SUB_REGISTRIES
    backend = {
        prefix: rule.ra_code
        for rule in SUB_REGISTRIES["GB"]
        for prefix in rule.prefixes
    }
    assert backend == GB_SUB_REGISTRIES


def test_only_gb_declares_sub_registries() -> None:
    """A new one must come with a test; this fails loudly rather than silently.

    If OpenCheck adds a country whose companies split across authorities, the
    parametrised prefix cases above will not cover it, and an untested
    dispatch rule is how the original values survived.
    """
    assert set(SUB_REGISTRIES) == {"GB"}


# ---------------------------------------------------------------------------
# The wiring, not just the helper
# ---------------------------------------------------------------------------
#
# The helper was already correct before Phase 141 — the endpoint just did not
# call it. So these assert against the resolver itself.


@pytest.mark.parametrize(
    "number,country,expected",
    [
        ("SC651281", "GB", "RA000587"),
        ("NI012345", "GB", "RA000586"),
        ("00102498", "GB", "RA000585"),
        ("sc651281", "gb", "RA000587"),
        ("34362985", "NL", "RA000463"),
    ],
)
async def test_resolver_scopes_by_number_not_only_country(
    monkeypatch: pytest.MonkeyPatch, number: str, country: str, expected: str
) -> None:
    seen: dict[str, str] = {}

    async def _capture(num: str, code: str):
        seen["code"] = code
        return []

    monkeypatch.setattr(REGISTRY["gleif"], "search_by_local_id", _capture)
    result = await _resolve_national_id_impl(number=number, country=country)

    assert seen["code"] == expected
    assert result.ra_code == expected


async def test_explicit_ra_code_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """A caller who names an authority means it — including one we would not pick."""
    seen: dict[str, str] = {}

    async def _capture(num: str, code: str):
        seen["code"] = code
        return []

    monkeypatch.setattr(REGISTRY["gleif"], "search_by_local_id", _capture)
    await _resolve_national_id_impl(
        number="SC651281", country="GB", ra_code="RA000585"
    )
    assert seen["code"] == "RA000585"


def test_frontend_picker_scopes_by_number() -> None:
    """The picker must not go back to reading the flat code off the entry.

    ``entry.raCode`` at this call site is the frontend half of the same
    defect, and it looks entirely reasonable — which is why it needs pinning
    rather than reviewing.
    """
    app = _FRONTEND_APP.read_text(encoding="utf-8")
    submit = re.search(r"nationalIdSearchMutation\.mutate\(\s*(?://[^\n]*\n\s*)*\{[^}]*\}", app)
    assert submit, "could not find the national-ID submit call in App.tsx"
    assert "raCodeFor(" in submit.group(0), (
        "the national-ID search must scope with raCodeFor(country, number), "
        f"not a flat country code — found: {submit.group(0)!r}"
    )


# ---------------------------------------------------------------------------
# Reachability: a mapping nobody can select is not a mapping
# ---------------------------------------------------------------------------


def test_every_backend_country_is_offered_in_the_picker() -> None:
    """Greece shipped in the backend map with no way to choose it in the UI.

    The ΓΕΜΗ adapter added GR to _RA_BY_COUNTRY when it landed; raCodes.ts was
    not touched, so the reverse lookup supported a country the picker did not
    list. Nothing errored — an absent option is not an error.
    """
    frontend = _frontend_map()
    missing = sorted(set(RA_BY_COUNTRY) - set(frontend))
    assert not missing, f"backend supports countries the picker cannot select: {missing}"


def test_frontend_country_count_in_prose_matches_the_data() -> None:
    """A count written in prose beside the data it counts goes stale silently.

    It said seventeen while the file held twenty. Phase 139 was an entire
    phase spent on a stale count that reached production, so this one is
    pinned rather than reviewed.
    """
    source = _FRONTEND_RA_CODES.read_text(encoding="utf-8")
    stated = re.search(r"codes for the (\d+) countries", source)
    assert stated, "raCodes.ts no longer states a country count — update this test"
    assert int(stated.group(1)) == len(_frontend_map())


def test_every_frontend_entry_is_offered_in_the_picker() -> None:
    """New Zealand had a correct entry that the hand-listed picker omitted."""
    source = _FRONTEND_RA_CODES.read_text(encoding="utf-8")
    options = source.split("COUNTRY_OPTIONS", 1)[1]
    assert "Object.entries(RA_CODES)" in options, (
        "COUNTRY_OPTIONS should be derived from RA_CODES, not hand-listed — "
        "the hand-listed version silently omitted New Zealand"
    )
