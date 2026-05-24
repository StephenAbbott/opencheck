"""Ariregister adapter and BODS mapper tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import json

import pytest
from pytest_httpx import HTTPXMock

from opencheck.bods.mapper import map_ariregister, _ee_date
from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.ariregister import (
    AriregisterAdapter,
    EE_RA_CODE,
    _extract_json_from_soap,
    _norm_date,
    _as_list,
    _parse_persons,
    _SOAP_URL,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("ARIREGISTER_USERNAME", "test_user")
    monkeypatch.setenv("ARIREGISTER_PASSWORD", "test_pass")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Sample SOAP responses
# ---------------------------------------------------------------------------

_COMPANY_PAYLOAD = {
    "ettevotjad": {
        "ettevotja": {
            "arinimi": "Bolt Technology OÜ",
            "registrikood": "14064835",
            "oiguslik_vorm": "OÜ",
            "kmkr_nr": "EE101968727",
            "staatus": "R",
            "esmaregistreerimise_kpv": "2013-12-12Z",
            "aadress": {
                "taisaadress": "Vana-Lõuna 15",
                "postiindeks": "10134",
                "ehak_nimetus": "Tallinn",
                "asukohamaa": "Eesti",
            },
            "kaardile_kantud_isikud": {
                "isik": [
                    {
                        "isiku_tyyp": "F",
                        "isiku_roll": "JUHL",
                        "kirje_id": 1001,
                        "eesnimi": "Martin",
                        "nimi_arinimi": "Villig",
                        "algus_kpv": "2013-12-12Z",
                        "lopp_kpv": None,
                        "synniaeg": "1993-10-23Z",
                        "valis_kood_riik": "EST",
                        "isikukood_registrikood": None,
                    },
                    {
                        "isiku_tyyp": "J",
                        "isiku_roll": "OSAN",
                        "kirje_id": 1002,
                        "eesnimi": None,
                        "nimi_arinimi": "Bolt Group OÜ",
                        "isikukood_registrikood": "16460009",
                        "osaluse_protsent": "100.00",
                        "osaluse_suurus": "2500",
                        "osaluse_valuuta": "EUR",
                        "algus_kpv": "2021-01-01Z",
                        "lopp_kpv": None,
                        "synniaeg": None,
                        "valis_kood_riik": None,
                    },
                ]
            },
        }
    }
}

_BO_PAYLOAD = {
    "kasusaajad": {
        "kasusaaja": {
            "kirje_id": 2001,
            "eesnimi": "Martin",
            "nimi": "Villig",
            "isikukood": "39310235099",
            "synniaeg": "1993-10-23Z",
            "valis_kood_riik": "EST",
            "aadress_riik": "EE",
            "kontrolli_teostamise_viis": "O",
            "algus_kpv": "2021-01-01Z",
            "lopp_kpv": None,
        }
    }
}


def _soap_response(payload: dict) -> str:
    """Wrap a payload dict in a minimal SOAP response envelope."""
    return (
        '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">'
        "<SOAP-ENV:Body>"
        '<ns1:Response xmlns:ns1="http://arireg.x-road.eu/producer/">'
        f"<ns1:keha>{json.dumps(payload)}</ns1:keha>"
        "</ns1:Response>"
        "</SOAP-ENV:Body>"
        "</SOAP-ENV:Envelope>"
    )


# ---------------------------------------------------------------------------
# Unit tests: pure helpers
# ---------------------------------------------------------------------------


def test_ee_ra_code():
    assert EE_RA_CODE == "RA000181"


def test_norm_date_api_format():
    assert _norm_date("2013-12-12Z") == "2013-12-12"


def test_norm_date_api_datetime():
    assert _norm_date("2013-12-12T00:00:00.000Z") == "2013-12-12"


def test_norm_date_bulk_format():
    assert _norm_date("12.12.2013") == "2013-12-12"


def test_norm_date_none():
    assert _norm_date(None) is None


def test_norm_date_empty():
    assert _norm_date("") is None


def test_as_list_dict():
    assert _as_list({"a": 1}) == [{"a": 1}]


def test_as_list_list():
    assert _as_list([1, 2]) == [1, 2]


def test_as_list_none():
    assert _as_list(None) == []


def test_extract_json_from_soap():
    payload = {"test": 123}
    soap = _soap_response(payload)
    result = _extract_json_from_soap(soap)
    assert result == payload


def test_extract_json_from_soap_no_keha():
    with pytest.raises(ValueError, match="No <keha>"):
        _extract_json_from_soap("<envelope><body>no keha here</body></envelope>")


def test_parse_persons_splits_shareholder_and_officer():
    company = _COMPANY_PAYLOAD["ettevotjad"]["ettevotja"]
    shareholders, officers = _parse_persons(company)
    # OSAN role → shareholder
    assert len(shareholders) == 1
    assert shareholders[0]["nimi_arinimi"] == "Bolt Group OÜ"
    # JUHL role → officer
    assert len(officers) == 1
    assert officers[0]["eesnimi"] == "Martin"


def test_parse_persons_normalises_dates():
    company = _COMPANY_PAYLOAD["ettevotjad"]["ettevotja"]
    shareholders, officers = _parse_persons(company)
    assert officers[0]["algus_kpv"] == "2013-12-12"
    assert shareholders[0]["algus_kpv"] == "2021-01-01"


# ---------------------------------------------------------------------------
# Unit test: _ee_date mapper helper handles ISO format (regression guard)
# ---------------------------------------------------------------------------


def test_ee_date_handles_api_iso_format():
    assert _ee_date("2013-12-12Z") == "2013-12-12"


def test_ee_date_handles_clean_iso():
    assert _ee_date("2013-12-12") == "2013-12-12"


def test_ee_date_still_handles_bulk_format():
    assert _ee_date("12.12.2013") == "2013-12-12"


# ---------------------------------------------------------------------------
# Integration: fetch() with mocked HTTP
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_fetch_returns_live_bundle(httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(_COMPANY_PAYLOAD),
        status_code=200,
    )
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(_BO_PAYLOAD),
        status_code=200,
    )

    adapter = AriregisterAdapter()
    bundle = await adapter.fetch("14064835", legal_name="Bolt Technology OÜ")

    assert bundle["is_stub"] is False
    assert bundle["source_id"] == "ariregister"
    assert bundle["registry_code"] == "14064835"
    assert bundle["name"] == "Bolt Technology OÜ"
    assert bundle["legal_form"] == "OÜ"
    assert bundle["vat_number"] == "EE101968727"
    assert bundle["status"] == "R"
    assert bundle["registration_date"] == "2013-12-12"
    assert "Tallinn" in (bundle["address"] or "")
    assert len(bundle["shareholders"]) == 1
    assert len(bundle["officers"]) == 1
    assert len(bundle["beneficial_owners"]) == 1


@pytest.mark.anyio
async def test_fetch_returns_stub_without_credentials(monkeypatch):
    monkeypatch.delenv("ARIREGISTER_USERNAME", raising=False)
    monkeypatch.delenv("ARIREGISTER_PASSWORD", raising=False)
    get_settings.cache_clear()

    adapter = AriregisterAdapter()
    bundle = await adapter.fetch("14064835", legal_name="Bolt Technology OÜ")
    assert bundle["is_stub"] is True
    assert bundle["name"] == "Bolt Technology OÜ"


@pytest.mark.anyio
async def test_fetch_returns_stub_when_company_not_found(httpx_mock: HTTPXMock):
    # Empty ettevotjad
    empty_payload = {"ettevotjad": {}}
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(empty_payload),
        status_code=200,
    )

    adapter = AriregisterAdapter()
    bundle = await adapter.fetch("99999999")
    assert bundle["is_stub"] is True


@pytest.mark.anyio
async def test_search_returns_empty_list():
    adapter = AriregisterAdapter()
    hits = await adapter.search("Bolt", SearchKind.ENTITY)
    assert hits == []


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_map_ariregister_produces_valid_statements(httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(_COMPANY_PAYLOAD),
        status_code=200,
    )
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(_BO_PAYLOAD),
        status_code=200,
    )

    adapter = AriregisterAdapter()
    bundle = await adapter.fetch("14064835")
    stmts = list(map_ariregister(bundle))

    assert stmts, "Expected at least one BODS statement"
    types = {s["recordType"] for s in stmts}
    # Should have entity statement for the company
    assert "entity" in types
    # Should have person statement(s) for the shareholder/officer
    assert "person" in types
    # Should have relationship statement(s)
    assert "relationship" in types


def test_map_ariregister_stub_returns_empty():
    bundle = {
        "source_id": "ariregister",
        "registry_code": "14064835",
        "name": "",
        "is_stub": True,
        "shareholders": [],
        "officers": [],
        "beneficial_owners": [],
    }
    assert list(map_ariregister(bundle)) == []


@pytest.mark.anyio
async def test_map_ariregister_bo_statement(httpx_mock: HTTPXMock):
    """Beneficial owner appears as a person + BO relationship statement."""
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(_COMPANY_PAYLOAD),
        status_code=200,
    )
    httpx_mock.add_response(
        url=_SOAP_URL,
        method="POST",
        text=_soap_response(_BO_PAYLOAD),
        status_code=200,
    )

    adapter = AriregisterAdapter()
    bundle = await adapter.fetch("14064835")
    stmts = list(map_ariregister(bundle))

    bo_stmts = [
        s for s in stmts
        if s.get("recordType") == "relationship"
        and any(
            i.get("beneficialOwnershipOrControl") is True
            for i in (s.get("recordDetails") or {}).get("interests", [])
        )
    ]
    assert bo_stmts, "Expected at least one BO relationship statement"


# ---------------------------------------------------------------------------
# Adapter info
# ---------------------------------------------------------------------------


def test_adapter_info_with_credentials():
    adapter = AriregisterAdapter()
    info = adapter.info
    assert info.id == "ariregister"
    assert info.live_available is True
    assert info.requires_api_key is True
    assert info.is_national_register is True
    assert SearchKind.ENTITY in info.supports


def test_adapter_info_without_credentials(monkeypatch):
    monkeypatch.delenv("ARIREGISTER_USERNAME", raising=False)
    monkeypatch.delenv("ARIREGISTER_PASSWORD", raising=False)
    get_settings.cache_clear()

    adapter = AriregisterAdapter()
    assert adapter.info.live_available is False
