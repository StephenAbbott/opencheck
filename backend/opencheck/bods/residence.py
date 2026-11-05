"""A person's country of residence, as a BODS ``residence`` address.

Open Ownership's minimum person data (``Usable beneficial ownership data``,
feature 14) is name, nationalities, month and year of birth and **countries
of residence**. BODS v0.4 carries residence as an address on the person
statement: ``{"type": "residence", "country": {"name", "code"}}``. The
``country`` object MUST have a name and SHOULD have an ISO 3166-1 alpha-2
code; nothing else is needed, and nothing else is written here — **a country
only, never a street** (the Estonia rule: the register publishes the country,
and a residential street is not ours to publish even where one is filed).

Registers file the country as free text. Companies House's
``country_of_residence`` is typed by the filer: "England", "Uk", "U.K.",
"Untied Kingdom", "Gb-Eng", "Hong Kong Sar", "Channel Islands", "New York".
:func:`resolve_country` reads it with :func:`countries.country_code` — the
one resolver every country helper shares, in a fixed order and never guessing:

1. a table of spellings registers use that pycountry does not resolve
   (:data:`countries.ALIASES`) — UK nations, abbreviations, old or common
   names, and misspellings measured in OpenCheck's Companies House cache;
2. an ISO 3166-2 subdivision code ("GB-ENG") → its country;
3. pycountry: alpha-2, alpha-3, then a name / official name / common name.

A string that resolves to nothing — "Channel Islands" (two jurisdictions),
"Korea" (two countries), "Kosovo" (no ISO 3166-1 code), "New York" (a state)
— keeps the filer's words as the country's ``name`` with **no code**: BODS
requires the name, and a stand-in code would reach the risk engine's
ISO-keyed lists (the Phase 239 input contract).

Where the published name differs from what the register filed ("England" →
United Kingdom), a ``transformation`` annotation records the filed words; an
unresolved string gets a ``commenting`` annotation saying no code was read.

The risk engine reads a person's non-service address countries for the ICIJ
jurisdiction gate (``icij_check._party_facts``, Phases 237 and 282), so a
residence country corroborates or rules out an Offshore Leaks person match.
"""

from __future__ import annotations

import re
from typing import Any

import pycountry

from .annotations import commenting, pointer, transformation
from .countries import ALIASES, UNRESOLVABLE_EXAMPLES, country_code

__all__ = [
    "ALIASES",
    "UNRESOLVABLE_EXAMPLES",
    "residence_address",
    "residence_note",
    "resolve_country",
]

def resolve_country(text: str | None) -> dict[str, str] | None:
    """The BODS country object for a free-text country, or ``None`` for an
    empty value. An unresolvable string is ``{"name": <as filed>}``. The code
    is :func:`countries.country_code`'s — the one reading every country helper
    shares."""
    filed = re.sub(r"\s+", " ", str(text or "")).strip()
    if not filed:
        return None
    code = country_code(filed)
    country = pycountry.countries.get(alpha_2=code) if code else None
    if country is None:
        return {"name": filed}
    # pycountry's common name where it has one ("Iran", "Taiwan", "Bolivia")
    # reads as people write it; the formal ISO name otherwise.
    name = getattr(country, "common_name", None) or country.name
    return {"name": name, "code": country.alpha_2}


def _alpha3(code: str) -> str:
    country = pycountry.countries.get(alpha_2=code)
    return getattr(country, "alpha_3", "") or ""


def residence_address(text: str | None) -> dict[str, Any] | None:
    """``{"type": "residence", "country": …}`` for a filed country of
    residence, or ``None`` when nothing was filed."""
    country = resolve_country(text)
    if country is None:
        return None
    return {"type": "residence", "country": country}


def residence_note(
    text: str | None, address_index: int, *, register: str
) -> dict[str, Any] | None:
    """The annotation for the residence address at ``address_index``: what
    *register* filed, when the published country is not those words or has
    no code. ``None`` when the name is the filed text and carries a code."""
    filed = re.sub(r"\s+", " ", str(text or "")).strip()
    country = resolve_country(filed)
    if country is None:
        return None
    target = pointer("recordDetails", "addresses", address_index, "country")
    if "code" not in country:
        return commenting(
            target,
            f"{register} files the country of residence as '{filed}'. No "
            "ISO 3166-1 country could be read from it, so it is given as "
            "filed, with no country code.",
        )
    if country["name"].casefold() == filed.casefold() or filed.upper() in (
        country["code"],
        _alpha3(country["code"]),
    ):
        # The filed words are the published name, or the register files the
        # ISO code itself (Latvia "LV", New Zealand "NZ"): nothing to record.
        return None
    return transformation(
        target,
        f"{register} files the country of residence as '{filed}'; OpenCheck "
        f"gives it as the ISO 3166-1 country {country['name']} "
        f"({country['code']}).",
        transformed_content=country["code"],
    )
