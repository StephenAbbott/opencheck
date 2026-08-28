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

from opencheck.routers.lookup import _RA_BY_COUNTRY

_FRONTEND_RA_CODES = (
    Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "raCodes.ts"
)

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
