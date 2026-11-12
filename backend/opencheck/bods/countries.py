"""One reading of a free-text country: the ISO 3166-1 alpha-2 code it names.

Every helper that turns a register's country words into a code goes through
:func:`country_code` — ``statements._country_code`` (a bare code),
``statements._country_obj`` (a BODS ``Country`` object) and
``residence.resolve_country`` (a person's country of residence). Until the
country-lookup ticket (4 Nov 2026) those were three resolvers with three alias
tables, and they disagreed: ``_country_code("Russia")`` was ``""``,
``_country_obj("Russia")`` was ``{"name": "Russia"}`` with no code, and only
``_country_obj`` knew that pycountry calls Turkey "Türkiye". A Russian or
Turkish entity whose jurisdiction a mapper read from a country *name* reached
the risk engine with no code, so the FATF, EU high-risk, non-EU and EU
tax-list checks — which key on the ISO code — passed over it silently. That is
the failure the Phase 239 input contract exists to prevent.

The reading, in a fixed order, and never a guess:

1. :data:`ALIASES` — whole spellings registers use that pycountry does not
   resolve (UK nations, abbreviations, older or common names, native-language
   names, misspellings measured in OpenCheck's Companies House cache);
2. an ISO 3166-2 subdivision code ("GB-ENG", "US-DE") → its country;
3. pycountry: alpha-2, alpha-3, then numeric / name / official name / common
   name (``countries.lookup``, an exact case-insensitive match).

A string none of these resolves — "Channel Islands" (two jurisdictions),
"Korea" (two countries), "Kosovo" (no ISO 3166-1 code), "New York" (a state)
— has **no code**. Each wrapper keeps the filed words in its own way; none
invents a stand-in, because a stand-in code reaches the ISO-keyed lists.
"""

from __future__ import annotations

import re

import pycountry

#: Normalised spelling (see :func:`normalise`) → ISO 3166-1 alpha-2. Whole
#: strings only; never a substring match. The residence entries were measured
#: on 3 Nov 2026 against 5,500 ``country_of_residence`` values in OpenCheck's
#: Companies House cache; the native-language block is what Bolagsverket and
#: the other European sources file, carried over from ``_country_obj``.
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
    # Native-language names (Bolagsverket and other European sources).
    "sverige": "SE",
    "norge": "NO",
    "danmark": "DK",
    "suomi": "FI",
    "deutschland": "DE",
    "frankreich": "FR",
    "österreich": "AT",
    "schweiz": "CH",
    "pays-bas": "NL",
    "españa": "ES",
    "italia": "IT",
    "polska": "PL",
    # Slovene: the country column of AJPES's PRS open file ("SLOVENIJA").
    "slovenija": "SI",
    "nemčija": "DE",
    "švica": "CH",
    # Hebrew names: every country word in the Israel Corporations Authority's
    # open data on 10 Nov 2026 (the registered-address country column).
    "ישראל": "IL",
    "איי קיימן": "KY",
    "ארצות הברית": "US",
    "הממלכה המאוחדת-אנגליה": "GB",
    "גרמניה": "DE",
    "קנדה": "CA",
    "ליבריה": "LR",
    "אוסטריה": "AT",
    "הולנד": "NL",
    "גיברלטר": "GI",
    "איטליה": "IT",
    "שוויץ": "CH",
    "ספרד": "ES",
    "ברמודה": "BM",
    "פנמה": "PA",
    "איי הבתולה הבריטיים": "VG",
    "בורונדי": "BI",
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


def normalise(text: str) -> str:
    """Lower case, dots removed, whitespace collapsed — the :data:`ALIASES`
    key ("U.K." → "uk", "Hong  Kong SAR" → "hong kong sar")."""
    return re.sub(r"\s+", " ", text.replace(".", "").strip()).lower()


def country_code(text: str | None) -> str:
    """The ISO 3166-1 alpha-2 code *text* names, or ``""`` when it names none
    (or names more than one)."""
    filed = re.sub(r"\s+", " ", str(text or "")).strip()
    if not filed:
        return ""
    key = normalise(filed)
    code = ALIASES.get(key)
    if code:
        return code
    if _SUBDIVISION.match(key):
        sub = pycountry.subdivisions.get(code=key.upper())
        if sub is not None:
            return str(sub.country_code)
    if len(key) == 2 and key.isalpha():
        # Two letters are a code or nothing: "XK" must not reach the name
        # lookup, and "UK" was answered by the alias table above.
        country = pycountry.countries.get(alpha_2=key.upper())
        return country.alpha_2 if country else ""
    if len(key) == 3 and key.isalpha():
        country = pycountry.countries.get(alpha_3=key.upper())
        if country:
            return str(country.alpha_2)
    try:
        return str(pycountry.countries.lookup(filed).alpha_2)
    except LookupError:
        return ""
