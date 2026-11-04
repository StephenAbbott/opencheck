"""One resolver for country names (the country-lookup ticket, 4 Nov 2026).

Until this ticket ``statements._country_code`` and ``statements._country_obj``
each had their own alias table and disagreed: "Russia" had no code in either
(pycountry's name is "Russian Federation"), and "Turkey" had one only in
``_country_obj`` (pycountry renamed it Türkiye). A mapper that read an
entity's jurisdiction from a country *name* wrote no code for a Russian or
Turkish entity, and the FATF, EU high-risk, non-EU and EU tax-list checks —
keyed on the ISO code — passed over it. Both helpers, ``residence.
resolve_country`` and the Wikidata label fallback now read the code from
``bods.countries.country_code``.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from opencheck.bods import countries, map_companies_house  # noqa: E402
from opencheck.bods.mappers.ftm import _ftm_jurisdiction, _ftm_resolve_nationality  # noqa: E402
from opencheck.bods.residence import resolve_country  # noqa: E402
from opencheck.bods.statements import _addr, _country_code, _country_obj  # noqa: E402

# The names the ticket lists, as registers file them.
NAMED = {
    "Russia": "RU",
    "RUSSIA": "RU",
    "Turkey": "TR",
    "Macau": "MO",
    "Brunei": "BN",
    "Cape Verde": "CV",
    "Swaziland": "SZ",
    "Burma": "MM",
    "Ivory Coast": "CI",
    "Palestine": "PS",
    "Holland": "NL",
    "Eire": "IE",
    "Republic Of Ireland": "IE",
    "Hong Kong Sar": "HK",
    "U.K.": "GB",
    "Untied Kingdom": "GB",
    "Phillipines": "PH",
    # What the helpers already resolved must still resolve.
    "United Kingdom": "GB",
    "England and Wales": "GB",
    "GB": "GB",
    "gb": "GB",
    "UK": "GB",
    "GBR": "GB",
    "RUS": "RU",
    "Iran": "IR",
    "Sverige": "SE",
    "Österreich": "AT",
    "Cayman Islands": "KY",
    "BVI": "VG",
    "GB-ENG": "GB",
}

#: Places that are not one ISO 3166-1 country: never a code, never a guess.
AMBIGUOUS = ("Korea", "Channel Islands", "Kosovo", "New York", "XK", "ZZ", "Atlantis")


@pytest.mark.parametrize("filed,code", sorted(NAMED.items()))
def test_every_helper_reads_the_same_code(filed: str, code: str) -> None:
    assert countries.country_code(filed) == code
    assert _country_code(filed) == code
    assert (_country_obj(filed) or {}).get("code") == code
    assert (resolve_country(filed) or {}).get("code") == code


@pytest.mark.parametrize("filed", AMBIGUOUS)
def test_an_ambiguous_or_unknown_name_stays_uncoded_everywhere(filed: str) -> None:
    assert countries.country_code(filed) == ""
    assert _country_code(filed) == ""
    assert _country_obj(filed) == {"name": filed}
    assert resolve_country(filed) == {"name": filed}


def test_the_helpers_agree_on_every_alias_and_every_iso_country() -> None:
    import pycountry

    values = [*countries.ALIASES, *countries.UNRESOLVABLE_EXAMPLES]
    for c in pycountry.countries:
        values += [c.alpha_2, c.alpha_3, c.name]
    for value in values:
        code = _country_code(value)
        assert (_country_obj(value) or {}).get("code", "") == code, value
        assert (resolve_country(value) or {}).get("code", "") == code, value


def test_empty_values_are_nothing() -> None:
    for empty in ("", "   ", None):
        assert _country_code(empty) == ""
        assert resolve_country(empty) is None
    assert _country_obj("") is None
    assert _country_obj("   ") is None


def test_country_object_keeps_the_iso_name() -> None:
    # The Country object's name is pycountry's ISO name, as before; only the
    # code is shared. (residence keeps its common-name rule.)
    assert _country_obj("Russia") == {"name": "Russian Federation", "code": "RU"}
    assert _country_obj("Turkey") == {"name": "Türkiye", "code": "TR"}
    assert _addr("registered", "1 Street", "Russia")["country"]["code"] == "RU"


def test_an_ftm_subdivision_jurisdiction_keeps_its_subdivision_code() -> None:
    # The shared resolver reads "US-DE" as the United States; a BODS
    # jurisdiction may carry the subdivision itself, so the FtM path keeps it.
    assert _ftm_jurisdiction({"jurisdiction": ["US-DE"]}) == ("US-DE", "US-DE")
    assert _ftm_jurisdiction({"jurisdiction": ["Russia"]}) == ("Russia", "RU")
    assert _ftm_resolve_nationality("ru") == {"name": "Russian Federation", "code": "RU"}
    assert _ftm_resolve_nationality("suhh") == {"name": "suhh"}


def _corporate_officer(country: str) -> dict:
    return {
        "source_id": "companies_house",
        "company_number": "OC000001",
        "profile": {"company_name": "EXAMPLE LLP", "type": "llp"},
        "officers": {"items": [{
            "name": "EXAMPLE HOLDINGS LIMITED",
            "officer_role": "corporate-llp-designated-member",
            "appointed_on": "2015-01-01",
            "address": {"address_line_1": "1 Example Street", "country": country},
            "identification": {
                "identification_type": "non-eea",
                "legal_authority": "COMPANIES LAW",
                "place_registered": "REGISTRAR",
                "registration_number": "123",
            },
            "links": {"officer": {"appointments": "/officers/EX1/appointments"}},
        }]},
        "pscs": {"items": []},
        "related_companies": {},
    }


@pytest.mark.parametrize("country,code", [("Russia", "RU"), ("Turkey", "TR"), ("Holland", "NL")])
def test_a_companies_house_officer_filed_with_a_country_name_gets_its_code(
    country: str, code: str
) -> None:
    # A corporate officer whose registration details name no country takes
    # its jurisdiction from the service address. Before the ticket "Russia"
    # and "Turkey" wrote no code here, so the ISO-keyed lists skipped it.
    stmts = list(map_companies_house(_corporate_officer(country)))
    officer = [s for s in stmts if s["recordType"] == "entity"][1]["recordDetails"]
    assert officer["jurisdiction"]["code"] == code
    assert officer["addresses"][0]["country"]["code"] == code


def test_no_alias_table_is_left_in_the_wrappers() -> None:
    # One table: a second copy is how the helpers came to disagree.
    import inspect

    from opencheck.bods import statements

    for fn in (statements._country_code, statements._country_obj):
        src = inspect.getsource(fn)
        assert "pycountry.countries.lookup" not in src
        assert "dict[str, str] = {" not in src
