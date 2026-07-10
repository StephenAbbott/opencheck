"""Tests for the AriregisterAdapter (public web scraper edition).

All HTTP calls are mocked via respx so no network access is needed.
"""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from opencheck.sources.ariregister import (
    AriregisterAdapter,
    EE_RA_CODE,
    _clean,
    _is_estonian_personal_code,
    _is_registry_code,
    _parse_beneficial_owners,
    _parse_date,
    _parse_info,
    _parse_officers,
    _parse_shareholders,
    _split_name,
)
from opencheck.sources.base import SearchKind

# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

_HTML = """<html><body>
<h2>Nordic Foods 1 OÜ (17441866)</h2>
<div class="row mt-4">
  <div class="col-md-4 text-muted">Registry code</div>
  <div class="col font-weight-bold">17441866</div>
</div>
<div class="row mt-4">
  <div class="col-md-4 text-muted">Legal form</div>
  <div class="col font-weight-bold">Private limited company</div>
</div>
<div class="row mt-4">
  <div class="col-md-4 text-muted">Status</div>
  <div class="col font-weight-bold">Entered into the register</div>
</div>
<div class="row mt-4">
  <div class="col-md-4 text-muted">Registered</div>
  <div class="col font-weight-bold">20.02.2026</div>
</div>
<div class="row mt-4">
  <div class="col-md-4 text-muted">Address</div>
  <div class="col font-weight-bold">
    Saare maakond, Saaremaa vald, Pidula-Kuusiku küla, 93466
    <a href="#">Open map<img src="/static/icons/map.svg"></a>
  </div>
</div>
<table>
<tr><th>Name</th><th>Personal identification code</th><th>Role</th><th>Start - end</th></tr>
<tr><td>Eva-Maria Kaerma</td><td>49301230014</td><td>Management board member</td><td>20.02.2026</td></tr>
</table>
<table>
<tr><th>Participation</th><th>Contribution</th><th>Name</th><th>Code</th><th>Start - End</th></tr>
<tr><td>33.33%</td><td>834.00 EUR</td><td>Haudemaja OÜ</td><td>16765745</td><td>20.02.2026</td></tr>
<tr><td>33.33%</td><td>834.00 EUR</td><td>OÜ TerraBorealis</td><td>17318746</td><td>20.02.2026</td></tr>
<tr><td>33.33%</td><td>834.00 EUR Sole ownership</td><td>CasCar Capital OÜ</td><td>17437636</td><td>20.02.2026</td></tr>
</table>
<table>
<tr><th>Name</th><th>Personal identification code / date of birth</th><th>Manner of exercising control</th><th>Start - end</th></tr>
<tr><td>Anne-Liis Theisen</td><td>49103275238</td><td>Indirect ownership</td><td>20.02.2026</td></tr>
<tr><td>Dominik Philipp Matyka</td><td>38212090515</td><td>Indirect ownership</td><td>20.02.2026</td></tr>
</table>
</body></html>"""

_AUTOCOMPLETE_JSON = {
    "status": "OK",
    "data": [{
        "company_id": 9000439169, "reg_code": 17441866,
        "name": "Nordic Foods 1 OÜ", "historical_names": [],
        "status": "R", "legal_address": "Saare maakond, 93466",
        "zip_code": "93466", "legal_form": "5",
        "url": "https://ariregister.rik.ee/eng/company/17441866/Nordic-Foods-1-OÜ",
    }],
}

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def test_clean():
    assert _clean("<b>  Hello   </b>") == "Hello"

def test_is_personal_code():
    assert _is_estonian_personal_code("49301230014")
    assert not _is_estonian_personal_code("16765745")
    assert not _is_estonian_personal_code("00000000000")

def test_is_registry_code():
    assert _is_registry_code("16765745")
    assert not _is_registry_code("49301230014")

def test_parse_date():
    assert _parse_date("20.02.2026") == "2026-02-20"
    assert _parse_date("") is None

def test_split_name():
    assert _split_name("Eva-Maria Kaerma") == ("Eva-Maria", "Kaerma")
    assert _split_name("Dominik Philipp Matyka") == ("Dominik Philipp", "Matyka")
    assert _split_name("Kaerma") == ("", "Kaerma")

def test_parse_info():
    info = _parse_info(_HTML)
    assert info["Registry code"] == "17441866"
    assert info["Legal form"] == "Private limited company"
    assert info["Registered"] == "20.02.2026"
    assert "Open map" not in info.get("Address", "")
    assert "93466" in info.get("Address", "")

def test_parse_officers():
    officers = _parse_officers(_HTML)
    assert len(officers) == 1
    o = officers[0]
    assert o["eesnimi"] == "Eva-Maria"
    assert o["nimi_arinimi"] == "Kaerma"
    assert o["isiku_roll"] == "JUHL"
    assert o["algus_kpv"] == "2026-02-20"

def test_parse_shareholders():
    shareholders = _parse_shareholders(_HTML)
    assert len(shareholders) == 3
    s = shareholders[0]
    assert s["nimi_arinimi"] == "Haudemaja OÜ"
    assert s["isiku_tyyp"] == "J"
    assert s["osaluse_protsent"] == "33.33"
    assert s["osaluse_suurus"] == "834.00"
    assert s["osaluse_valuuta"] == "EUR"
    assert s["isikukood_registrikood"] == "16765745"
    # "Sole ownership" must not appear in parsed amount
    assert s["osaluse_suurus"] == "834.00"

def test_parse_beneficial_owners():
    bos = _parse_beneficial_owners(_HTML)
    assert len(bos) == 2
    b = bos[0]
    assert b["eesnimi"] == "Anne-Liis"
    assert b["nimi"] == "Theisen"
    assert b["isikukood"] == "49103275238"
    assert b["kontrolli_teostamise_viis"] == "K"
    assert b["algus_kpv"] == "2026-02-20"

def test_parse_officers_multiple_roles():
    html = """<table>
    <tr><th>Name</th><th>Personal identification code</th><th>Role</th><th>Start - end</th></tr>
    <tr><td>John Smith</td><td>39001010011</td><td>Procurist</td><td>01.01.2020</td></tr>
    <tr><td>Jane Doe</td><td>48001010012</td><td>Liquidator</td><td>01.01.2021</td></tr>
    <tr><td>Unknown Role</td><td>38001010013</td><td>Some future role</td><td>01.01.2022</td></tr>
    </table>"""
    officers = _parse_officers(html)
    # Unknown roles are skipped
    assert len(officers) == 2
    assert officers[0]["isiku_roll"] == "PROK"
    assert officers[1]["isiku_roll"] == "LIKV"

def test_shareholder_personal_code_is_type_f():
    html = """<table>
    <tr><th>Participation</th><th>Contribution</th><th>Name</th><th>Code</th><th>Start - End</th></tr>
    <tr><td>50.00%</td><td>1000.00 EUR</td><td>Jaan Tamm</td><td>38001010011</td><td>01.01.2020</td></tr>
    </table>"""
    shareholders = _parse_shareholders(html)
    assert shareholders[0]["isiku_tyyp"] == "F"

def test_bo_direct_ownership_maps_to_o():
    html = """<table>
    <tr><th>Name</th><th>Personal identification code / date of birth</th><th>Manner of exercising control</th><th>Start - end</th></tr>
    <tr><td>Mari Mets</td><td>48001010011</td><td>Direct ownership</td><td>01.01.2020</td></tr>
    </table>"""
    bos = _parse_beneficial_owners(html)
    assert bos[0]["kontrolli_teostamise_viis"] == "O"

# Post-2026-07-10 page: the register replaced the beneficial-owners table
# with an authentication prompt for anonymous visitors (observed live on the
# changeover day — issue #28). The scraper must degrade to an empty BO list
# while everything else still parses.
_HTML_BO_WITHDRAWN = _HTML.replace(
    """<table>
<tr><th>Name</th><th>Personal identification code / date of birth</th><th>Manner of exercising control</th><th>Start - end</th></tr>
<tr><td>Anne-Liis Theisen</td><td>49103275238</td><td>Indirect ownership</td><td>20.02.2026</td></tr>
<tr><td>Dominik Philipp Matyka</td><td>38212090515</td><td>Indirect ownership</td><td>20.02.2026</td></tr>
</table>""",
    """<div class="row mt-4">
  <div class="col-md-4 text-muted">Beneficial owners</div>
  <div class="col">
    <p>To see the beneficial owners data Authentication is required to request access.
       Please choose a suitable method:</p>
    <a href="#" class="btn">Authenticate</a>
  </div>
</div>""",
)


def test_bo_withdrawal_degrades_gracefully():
    """No BO table (post-2026-07-10 anonymous view) → empty list, no error,
    and the auth-prompt markup must not confuse the other table parsers."""
    assert _parse_beneficial_owners(_HTML_BO_WITHDRAWN) == []
    assert len(_parse_officers(_HTML_BO_WITHDRAWN)) == 1
    assert len(_parse_shareholders(_HTML_BO_WITHDRAWN)) == 3


# ---------------------------------------------------------------------------
# Adapter integration (HTTP mocked)
# ---------------------------------------------------------------------------

@pytest.fixture
def adapter():
    return AriregisterAdapter()

def test_ra_code():
    assert EE_RA_CODE == "RA000181"

def test_requires_no_api_key(adapter):
    assert not adapter.info.requires_api_key

def test_live_available(adapter):
    assert adapter.info.live_available

@pytest.mark.asyncio
async def test_fetch_full_bundle(adapter):
    with respx.mock:
        respx.get(
            "https://ariregister.rik.ee/eng/company/17441866/company_print_json"
        ).mock(return_value=Response(200, text=_HTML,
                                    headers={"content-type": "text/html; charset=utf-8"}))
        bundle = await adapter.fetch("17441866", legal_name="Nordic Foods 1 OÜ")

    assert not bundle["is_stub"]
    assert bundle["name"] == "Nordic Foods 1 OÜ"
    assert bundle["legal_form"] == "Private limited company"
    assert bundle["status"] == "Entered into the register"
    assert bundle["registration_date"] == "2026-02-20"
    assert "93466" in (bundle["address"] or "")
    assert len(bundle["officers"]) == 1
    assert len(bundle["shareholders"]) == 3
    assert len(bundle["beneficial_owners"]) == 2

@pytest.mark.asyncio
async def test_fetch_full_bundle_without_bo_table(adapter):
    """End-to-end on the post-2026-07-10 anonymous page shape (issue #28):
    the bundle carries officers + shareholders and an empty BO list."""
    with respx.mock:
        respx.get(
            "https://ariregister.rik.ee/eng/company/17441866/company_print_json"
        ).mock(return_value=Response(200, text=_HTML_BO_WITHDRAWN,
                                    headers={"content-type": "text/html; charset=utf-8"}))
        bundle = await adapter.fetch("17441866", legal_name="Nordic Foods 1 OÜ")

    assert not bundle["is_stub"]
    assert len(bundle["officers"]) == 1
    assert len(bundle["shareholders"]) == 3
    assert bundle["beneficial_owners"] == []

@pytest.mark.asyncio
async def test_fetch_stub_on_404(adapter):
    with respx.mock:
        respx.get(
            "https://ariregister.rik.ee/eng/company/99999999/company_print_json"
        ).mock(return_value=Response(404, text="Not found"))
        bundle = await adapter.fetch("99999999")

    assert bundle["is_stub"]

@pytest.mark.asyncio
async def test_fetch_stub_on_redirect_away_from_company(adapter):
    """When company not found, ariregister.rik.ee redirects to /eng?wmsg=..."""
    with respx.mock:
        # The response URL is changed to simulate a redirect away from /eng/company/
        resp = Response(200, text="<html><title>Search</title></html>",
                        headers={"content-type": "text/html"})
        # Simulate redirect: final URL won't contain /eng/company/
        respx.get(
            "https://ariregister.rik.ee/eng/company/00000001/company_print_json"
        ).mock(return_value=Response(
            200, text="<html><title>Search</title></html>",
            headers={"content-type": "text/html"},
        ))

        # Patch the URL check to simulate redirect detection
        import opencheck.sources.ariregister as m
        orig = m._PRINT_URL
        m._PRINT_URL = "https://ariregister.rik.ee/eng?wmsg=notfound"
        try:
            bundle = await adapter.fetch("00000001")
        finally:
            m._PRINT_URL = orig

    assert bundle["is_stub"]

@pytest.mark.asyncio
async def test_search_returns_hits(adapter):
    with respx.mock:
        respx.get("https://ariregister.rik.ee/eng/api/autocomplete").mock(
            return_value=Response(200, json=_AUTOCOMPLETE_JSON)
        )
        hits = await adapter.search("Nordic Foods", SearchKind.ENTITY)

    assert len(hits) == 1
    assert hits[0].hit_id == "17441866"
    assert hits[0].name == "Nordic Foods 1 OÜ"
    assert not hits[0].is_stub

@pytest.mark.asyncio
async def test_search_empty_on_error(adapter):
    with respx.mock:
        respx.get("https://ariregister.rik.ee/eng/api/autocomplete").mock(
            side_effect=Exception("network error")
        )
        hits = await adapter.search("test", SearchKind.ENTITY)
    assert hits == []
