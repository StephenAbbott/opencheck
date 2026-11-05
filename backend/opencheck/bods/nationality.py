"""Nationalities as Companies House files them, with an ISO code where one is clear.

Companies House files a person's nationality as free text, mostly a demonym
("British", "American"), sometimes a country ("United Kingdom", "Usa"), a
phrase ("Citizen Of Usa", "United States Citizen") or several at once
("British,Australian", "Dutch And Usa", "Irish Canadian"). Until the
Companies House officer-nationality ticket (4 Nov 2026) every CH person
statement carried ``{"name": <as filed>}`` and no code, and
``cross_check`` compares nationality on the code alone — so nationality
never corroborated a name match for anyone the register names.

:func:`ch_nationalities` splits a filed value into its parts and resolves each
to an ISO 3166-1 alpha-2 code through :data:`DEMONYMS`, then the country-name
reader every mapper uses (``statements._country_code``). A part that resolves
to nothing keeps its name alone — never a stand-in code (the Phase 239
contract). Each part keeps the register's own spelling as its ``name``.

:data:`DEMONYMS` began as the demonym/ISO pairs of the ``countryinfo`` package
(MIT), cut to the unambiguous ones: a demonym that names two countries
("Congolese", "Dominican", "Channel Islander") is left out, and one shared by a
country and its territories ("French", "American", "Chinese", "Indian",
"Norwegian") is given the sovereign country. Companies House's own spellings
were added from the register's filings ("English", "Scottish", "Hong Konger",
"Kazakh", "Hksar"). Kosovo has no ISO 3166-1 code, so "Kosovan" stays a name.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from .statements import _country_code

#: Lower-cased demonym → ISO 3166-1 alpha-2. See the module docstring.
DEMONYMS: dict[str, str] = {
    "afghan": "AF",
    "albanian": "AL",
    "algerian": "DZ",
    "american": "US",
    "american samoan": "AS",
    "andorran": "AD",
    "angolan": "AO",
    "anguillian": "AI",
    "antiguan": "AG",
    "argentine": "AR",
    "argentinean": "AR",
    "argentinian": "AR",
    "armenian": "AM",
    "aruban": "AW",
    "australian": "AU",
    "austrian": "AT",
    "azerbaijani": "AZ",
    "bahamanian": "BS",
    "bahamian": "BS",
    "bahraini": "BH",
    "bajan": "BB",
    "bangladeshi": "BD",
    "barbadian": "BB",
    "barbudan": "AG",
    "basotho": "LS",
    "batswana": "BW",
    "belarusian": "BY",
    "belarussian": "BY",
    "belgian": "BE",
    "belizean": "BZ",
    "beninese": "BJ",
    "bermudan": "BM",
    "bermudian": "BM",
    "bhutanese": "BT",
    "bolivian": "BO",
    "bosnian": "BA",
    "brazilian": "BR",
    "british": "GB",
    "british virgin islander": "VG",
    "bruneian": "BN",
    "bulgarian": "BG",
    "burkinabe": "BF",
    "burmese": "MM",
    "burundian": "BI",
    "cambodian": "KH",
    "cameroonian": "CM",
    "canadian": "CA",
    "cape verdean": "CV",
    "cape verdian": "CV",
    "caymanian": "KY",
    "central african": "CF",
    "chadian": "TD",
    "chilean": "CL",
    "chinese": "CN",
    "christmas islander": "CX",
    "cocos islander": "CC",
    "colombian": "CO",
    "comoran": "KM",
    "comorian": "KM",
    "cook islander": "CK",
    "costa rican": "CR",
    "croatian": "HR",
    "cuban": "CU",
    "cypriot": "CY",
    "czech": "CZ",
    "danish": "DK",
    "djiboutian": "DJ",
    "dutch": "NL",
    "east timorese": "TL",
    "ecuadorean": "EC",
    "ecuadorian": "EC",
    "egyptian": "EG",
    "emirati": "AE",
    "emirian": "AE",
    "english": "GB",
    "equatoguinean": "GQ",
    "equatorial guinean": "GQ",
    "eritrean": "ER",
    "estonian": "EE",
    "ethiopian": "ET",
    "falkland islander": "FK",
    "faroese": "FO",
    "fijian": "FJ",
    "filipino": "PH",
    "finnish": "FI",
    "french": "FR",
    "french polynesian": "PF",
    "gabonese": "GA",
    "gambian": "GM",
    "georgian": "GE",
    "german": "DE",
    "ghanaian": "GH",
    "gibraltarian": "GI",
    "greek": "GR",
    "greenlandic": "GL",
    "grenadian": "GD",
    "guadeloupian": "GP",
    "guamanian": "GU",
    "guatemalan": "GT",
    "guinea-bissauan": "GW",
    "guinean": "GN",
    "guyanese": "GY",
    "haitian": "HT",
    "herzegovinian": "BA",
    "honduran": "HN",
    "hong kong chinese": "HK",
    "hong konger": "HK",
    "hong kong sar": "HK",
    "hksar": "HK",
    "hongkonger": "HK",
    "hungarian": "HU",
    "i-kiribati": "KI",
    "icelander": "IS",
    "icelandic": "IS",
    "indian": "IN",
    "indonesian": "ID",
    "iranian": "IR",
    "iraqi": "IQ",
    "irish": "IE",
    "israeli": "IL",
    "italian": "IT",
    "ivorian": "CI",
    "jamaican": "JM",
    "japanese": "JP",
    "jordanian": "JO",
    "kazakh": "KZ",
    "kazakhstani": "KZ",
    "kenyan": "KE",
    "kirghiz": "KG",
    "kittian and nevisian": "KN",
    "kittitian": "KN",
    "kuwaiti": "KW",
    "kyrgyz": "KG",
    "kyrgyzstani": "KG",
    "lao": "LA",
    "laotian": "LA",
    "latvian": "LV",
    "lebanese": "LB",
    "liberian": "LR",
    "libyan": "LY",
    "liechtensteiner": "LI",
    "lithuanian": "LT",
    "luxembourger": "LU",
    "luxembourgish": "LU",
    "macanese": "MO",
    "macedonian": "MK",
    "malagasy": "MG",
    "malawian": "MW",
    "malaysian": "MY",
    "maldivan": "MV",
    "maldivian": "MV",
    "malian": "ML",
    "maltese": "MT",
    "manx": "IM",
    "marshallese": "MH",
    "mauritanian": "MR",
    "mauritian": "MU",
    "mexican": "MX",
    "micronesian": "FM",
    "moldovan": "MD",
    "monacan": "MC",
    "monegasque": "MC",
    "mongolian": "MN",
    "montenegrin": "ME",
    "montserratian": "MS",
    "moroccan": "MA",
    "mosotho": "LS",
    "motswana": "BW",
    "mozambican": "MZ",
    "myanmar": "MM",
    "namibian": "NA",
    "nauruan": "NR",
    "nepalese": "NP",
    "nepali": "NP",
    "nevisian": "KN",
    "new caledonian": "NC",
    "new zealander": "NZ",
    "ni-vanuatu": "VU",
    "nicaraguan": "NI",
    "nigerian": "NG",
    "nigerien": "NE",
    "niuean": "NU",
    "norfolk islander": "NF",
    "north korean": "KP",
    "north macedonian": "MK",
    "northern irish": "GB",
    "norwegian": "NO",
    "omani": "OM",
    "pakistani": "PK",
    "palauan": "PW",
    "palestinian": "PS",
    "panamanian": "PA",
    "papua new guinean": "PG",
    "paraguayan": "PY",
    "peruvian": "PE",
    "philippine": "PH",
    "pitcairn islander": "PN",
    "polish": "PL",
    "portuguese": "PT",
    "puerto rican": "PR",
    "qatari": "QA",
    "romanian": "RO",
    "russian": "RU",
    "rwandan": "RW",
    "sahrawi": "EH",
    "saint helenian": "SH",
    "saint lucian": "LC",
    "saint vincentian": "VC",
    "salvadoran": "SV",
    "sammarinese": "SM",
    "samoan": "WS",
    "sao tomean": "ST",
    "saudi": "SA",
    "saudi arabian": "SA",
    "scottish": "GB",
    "senegalese": "SN",
    "serbian": "RS",
    "seychellois": "SC",
    "sierra leonean": "SL",
    "singaporean": "SG",
    "slovak": "SK",
    "slovakian": "SK",
    "slovene": "SI",
    "slovenian": "SI",
    "solomon islander": "SB",
    "somali": "SO",
    "south african": "ZA",
    "south korean": "KR",
    "south sudanese": "SS",
    "spanish": "ES",
    "sri lankan": "LK",
    "sudanese": "SD",
    "surinamer": "SR",
    "surinamese": "SR",
    "swazi": "SZ",
    "swedish": "SE",
    "swiss": "CH",
    "syrian": "SY",
    "tadzhik": "TJ",
    "taiwanese": "TW",
    "tajik": "TJ",
    "tajikistani": "TJ",
    "tanzanian": "TZ",
    "thai": "TH",
    "timorese": "TL",
    "tobagonian": "TT",
    "togolese": "TG",
    "tokelauan": "TK",
    "tongan": "TO",
    "trinidadian": "TT",
    "tunisian": "TN",
    "turk": "TR",
    "turkish": "TR",
    "turkmen": "TM",
    "tuvaluan": "TV",
    "ugandan": "UG",
    "ukrainian": "UA",
    "uruguayan": "UY",
    "uzbek": "UZ",
    "uzbekistani": "UZ",
    "vaticanian": "VA",
    "venezuelan": "VE",
    "vietnamese": "VN",
    "vincentian": "VC",
    "wallis and futuna islander": "WF",
    "welsh": "GB",
    "yemeni": "YE",
    "yemenite": "YE",
    "zambian": "ZM",
    "zimbabwean": "ZW",
}

# Separators between nationalities in one filing: "British,Australian",
# "Dutch And Usa", "British/Irish", "British & Irish".
_SPLIT = re.compile(r"\s*(?:[,;/&+]|\band\b)\s*", re.IGNORECASE)
_PREFIX = re.compile(r"^(?:a\s+)?(?:citizen|national)\s+of\s+(?:the\s+)?", re.IGNORECASE)
_SUFFIX = re.compile(r"\s+(?:citizen|national)$", re.IGNORECASE)


def _key(text: str) -> str:
    return " ".join(text.split()).casefold()


def nationality_code(text: str | None) -> str:
    """The ISO alpha-2 code for one filed nationality, or ``""``.

    A demonym first, then a country name or code ("United Kingdom", "Usa",
    "Us"), each with any "Citizen of" / "… citizen" wording removed.
    """
    if not text or not text.strip():
        return ""
    bare = _SUFFIX.sub("", _PREFIX.sub("", text.strip())).strip()
    if not bare:
        return ""
    code = DEMONYMS.get(_key(bare))
    if code:
        return code
    if _key(bare) in {"other", "unknown", "not known", "n/a", "none"}:
        return ""
    return _country_code(bare)


def _parts(value: str) -> list[tuple[str, str]]:
    """``(name, code)`` per nationality in one filed value."""
    out: list[tuple[str, str]] = []
    for part in _SPLIT.split(value):
        part = " ".join(part.split())
        if not part:
            continue
        code = nationality_code(part)
        if code:
            out.append((part, code))
            continue
        # "Irish Canadian", "American British": two demonyms with no
        # separator. Split on spaces only when every word is a demonym, so a
        # name such as "Saudi Arabian" (resolved whole above) is never cut.
        words = part.split()
        if len(words) > 1 and all(_key(w) in DEMONYMS for w in words):
            out.extend((w, DEMONYMS[_key(w)]) for w in words)
            continue
        out.append((part, ""))
    return out


def ch_nationalities(values: Iterable[str | None] | str | None) -> list[dict[str, str]]:
    """BODS ``nationalities`` for one or more Companies House filed values.

    Every distinct nationality, in the order given: entries with a code are
    deduplicated by code (the first spelling is kept), entries without one by
    their name. ``"British,Australian"`` gives two entries; ``"British"`` and
    ``"English"`` from two appointments give one, ``GB``.
    """
    if values is None or isinstance(values, str):
        values = [values]
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        for name, code in _parts(value):
            key = f"code:{code}" if code else f"name:{_key(name)}"
            if key in seen:
                continue
            seen.add(key)
            out.append({"name": name, "code": code} if code else {"name": name})
    return out
