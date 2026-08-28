"""Country → GLEIF Registration Authority dispatch, in one place.

Every reverse lookup OpenCheck runs — national registration number to LEI —
has to answer one question first: *which* registration authority should the
GLEIF query be scoped to? Getting it wrong does not raise; it appends
``filter[entity.registeredAt]=<wrong code>`` and returns an empty result set,
which reads as an absent company rather than a defect.

That failure mode is why this module exists. The mapping had been written out
four times — ``routers/lookup.py``, ``routers/search.py``, ``sources/gleif.py``
and ``frontend/src/lib/raCodes.ts`` — and by 2026-08-28 every copy had drifted
separately: twenty wrong values between them, each one a real code belonging to
a different authority. Phase 140 corrected all four. Phase 141 removes the
premise, because correcting copies does not stop them diverging again.

**Two questions, not one.** Phase 140 fixed the values and left the shape,
and the shape was the remaining half of the bug: the country map and the
prefix rule lived in different modules and only one entry point consulted
both. ``/search``'s Companies House → LEI bridge applied the prefix rule;
``/resolve-national-id`` went straight to the flat country map, so
``country="GB"`` sent a Scottish number to the England & Wales authority and
found nothing. Both now go through :func:`ra_code_for`.

**Sub-registries are data, not a special case.** GB is the only country that
needs one today, but expressing it as a table rather than an ``if`` means the
frontend can mirror the same declaration and ``tests/test_ra_codes.py`` can
check the two agree — which is the only mechanism that has ever caught these
copies drifting.

Every code here was verified against **live GLEIF records** — reading
``registeredAt.id`` off real entities via ``filter[entity.legalAddress.country]``
— and not against the RA catalogue, which only says which authorities exist.
``filter[country]`` on that catalogue endpoint is silently ignored and returns
the unfiltered global list, so spot-checking a code that way appears to confirm
whatever you already believed. That is the most likely origin of the original
values, and it is worth knowing before changing anything below.
"""

from __future__ import annotations

from typing import NamedTuple

__all__ = [
    "RA_BY_COUNTRY",
    "SUB_REGISTRIES",
    "SubRegistry",
    "ch_ra_code",
    "ra_code_for",
]


#: Country (ISO 3166-1 alpha-2) → the GLEIF Registration Authority code that
#: scopes a reverse lookup for that country. Where a country files companies
#: under several authorities this is the **dominant** one; :data:`SUB_REGISTRIES`
#: refines it from the registration number. Pass an explicit ``ra_code`` to
#: target a specific authority directly.
RA_BY_COUNTRY: dict[str, str] = {
    "GB": "RA000585",  # UK Companies House (England & Wales) — see SUB_REGISTRIES
    "NL": "RA000463",  # KvK (Netherlands)
    "NO": "RA000472",  # Brønnøysund / Brreg (Norway)
    "NZ": "RA000466",  # Companies Office (New Zealand)
    "IE": "RA000402",  # CRO (Ireland)
    "LV": "RA000423",  # UR (Latvia)
    "LT": "RA000430",  # JAR (Lithuania)
    "FR": "RA000189",  # Sirene / INSEE (France)
    "SE": "RA000544",  # Bolagsverket (Sweden)
    "EE": "RA000181",  # ariregister (Estonia)
    "BE": "RA000025",  # BCE/KBO (Belgium)
    "AT": "RA000017",  # Firmenbuch (Austria)
    "PL": "RA000484",  # KRS (Poland)
    "SK": "RA000526",  # RPO (Slovakia)
    "SG": "RA000523",  # ACRA (Singapore)
    "CA": "RA000072",  # Corporations Canada
    "DK": "RA000170",  # CVR (Denmark)
    "HR": "RA000156",  # Sudski registar (Croatia)
    "MT": "RA000443",  # Malta Business Registry
    "BR": "RA000681",  # Receita Federal CNPJ (Brazil)
    "GR": "RA000685",  # ΓΕΜΗ — General Commercial Registry (Greece)
}


class SubRegistry(NamedTuple):
    """One authority within a country, selected by registration-number prefix."""

    prefixes: tuple[str, ...]
    ra_code: str
    label: str


#: Countries whose companies are filed under more than one authority, where the
#: registration number's prefix says which. Rules are tried in order; a number
#: matching none of them keeps the country's default from :data:`RA_BY_COUNTRY`.
#:
#: Companies House is the only such country OpenCheck covers. Its three
#: authorities are ``RA000585`` England & Wales, ``RA000586`` Northern Ireland
#: and ``RA000587`` Scotland — confirmed against real records (THON MARITIME
#: LTD, ``registeredAs "SC651281"``, sits under RA000587). ``RA000591`` is
#: **The Pensions Regulator** and not a company registry at all; it sat here as
#: Northern Ireland's code until Phase 140, and ``tests/test_ra_codes.py``
#: now fails if it reappears in any mapping.
SUB_REGISTRIES: dict[str, tuple[SubRegistry, ...]] = {
    "GB": (
        # Scottish limited companies are SC; Scottish limited partnerships and
        # qualifying partnerships are SO and SF.
        SubRegistry(("SC", "SO", "SF"), "RA000587", "Companies House — Scotland"),
        # Northern Irish companies are NI; NC and R0 are older registrations
        # carried over from the pre-2009 Belfast registry.
        SubRegistry(("NI", "NC", "R0"), "RA000586", "Companies House — Northern Ireland"),
    ),
}


def ch_ra_code(company_number: str) -> str:
    """Return the GLEIF Registration Authority code for a Companies House number.

    A thin, well-named alias for ``ra_code_for("GB", company_number)``, kept
    because the Companies House → LEI bridge in ``routers/search.py`` reads
    better with it and because ``opencheck.app`` re-exports the name.
    """
    return ra_code_for("GB", company_number)


def ra_code_for(country: str, number: str = "") -> str:
    """Return the RA code a reverse lookup should be scoped to.

    ``country`` is an ISO 3166-1 alpha-2 code; ``number`` is the national
    registration number, used only to pick between a country's sub-registries.
    Returns ``""`` for a country OpenCheck has no mapping for — the caller
    then queries GLEIF unscoped, which is a wider search rather than a wrong
    one.

    Passing no ``number`` yields the country's dominant authority. That is the
    right answer for "which registry is this country's", and the wrong one for
    "which registry is *this company* in" — so callers that have the number
    should always pass it.
    """
    code = country.strip().upper()
    default = RA_BY_COUNTRY.get(code, "")

    upper = (number or "").strip().upper()
    if upper:
        for rule in SUB_REGISTRIES.get(code, ()):
            if upper.startswith(rule.prefixes):
                return rule.ra_code

    return default
