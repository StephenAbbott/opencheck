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
:func:`resolve_country` reads it in a fixed order and never guesses:

1. a table of spellings registers use that pycountry does not resolve
   (:data:`ALIASES`) — UK nations, abbreviations, old or common names, and
   misspellings measured in OpenCheck's Companies House cache;
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

#: Normalised spelling (lower case, dots removed, whitespace collapsed) →
#: ISO 3166-1 alpha-2. Only whole strings; never a substring match.
#: Measured on 3 Nov 2026 against 5,500 ``country_of_residence`` values in
#: OpenCheck's local Companies House cache, where every entry below that is
#: not a UK nation appeared at least once.
ALIASES: dict[str, str] = {
    # The UK and its nations, and how filers spell them.
    "uk": "GB",
    "united kingdom": "GB",
    "untied kingdom": "GB",
    "great britain": "GB",
    "britain": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "england and wales": "GB",
    "england & wales": "GB",
    # The United States.
    "usa": "US",
    "us": "US",
    "united states america": "US",
    "united states of america": "US",
    # Names pycountry no longer resolves, or never did.
    "russia": "RU",
    "turkey": "TR",
    "south korea": "KR",
    "north korea": "KP",
    "hong kong sar": "HK",
    "macau": "MO",
    "macao": "MO",
    "macau sar": "MO",
    "republic of ireland": "IE",
    "eire": "IE",
    "holland": "NL",
    "the netherlands": "NL",
    "phillipines": "PH",
    "ivory coast": "CI",
    "cape verde": "CV",
    "swaziland": "SZ",
    "burma": "MM",
    "brunei": "BN",
    "palestine": "PS",
    "vatican": "VA",
    "vatican city": "VA",
    "uae": "AE",
    "bvi": "VG",
    "british virgin islands": "VG",
    "czech republic": "CZ",
    "the bahamas": "BS",
    "the gambia": "GM",
}

#: Strings that name a place but not one ISO 3166-1 country. Listed so a test
#: can pin that they stay unresolved; resolution would refuse them anyway.
UNRESOLVABLE_EXAMPLES: tuple[str, ...] = (
    "Channel Islands",
    "Korea",
    "Kosovo",
    "New York",
)

_SUBDIVISION = re.compile(r"^[a-z]{2}-[a-z0-9]{1,3}$")


def _key(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace(".", "").strip()).lower()


def resolve_country(text: str | None) -> dict[str, str] | None:
    """The BODS country object for a free-text country, or ``None`` for an
    empty value. An unresolvable string is ``{"name": <as filed>}``."""
    filed = re.sub(r"\s+", " ", str(text or "")).strip()
    if not filed:
        return None
    key = _key(filed)
    code = ALIASES.get(key)
    if code is None and _SUBDIVISION.match(key):
        sub = pycountry.subdivisions.get(code=key.upper())
        if sub is not None:
            code = sub.country_code
    country = None
    if code:
        country = pycountry.countries.get(alpha_2=code)
    elif len(key) == 2 and key.isalpha():
        country = pycountry.countries.get(alpha_2=key.upper())
    elif len(key) == 3 and key.isalpha():
        country = pycountry.countries.get(alpha_3=key.upper())
    if country is None and not code and len(key) > 3:
        try:
            country = pycountry.countries.lookup(filed)
        except LookupError:
            country = None
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
