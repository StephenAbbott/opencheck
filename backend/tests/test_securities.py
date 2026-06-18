"""Tests for the securities service and /securities endpoint.

GLEIF, OpenFIGI and OpenSanctions are mocked at the httpx level (no network).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from opencheck import securities as svc
from opencheck.app import app
from opencheck.config import get_settings


# ---------------------------------------------------------------------------
# Fake httpx client routed by URL
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeClient:
    def __init__(self, *, gleif: Any, openfigi_by_isin: dict, opensanctions: Any) -> None:
        self._gleif = gleif
        self._figi = openfigi_by_isin
        self._os = opensanctions
        self.figi_calls: list[list[str]] = []

    async def get(self, url: str, params=None, headers=None) -> _Resp:
        if "/isins" in url:
            return _Resp(self._gleif)
        if "search/securities" in url:
            return _Resp(self._os)
        raise AssertionError(f"unexpected GET {url}")

    async def post(self, url: str, json=None, headers=None) -> _Resp:
        assert url == svc._OPENFIGI_URL
        isins = [job["idValue"] for job in json]
        self.figi_calls.append(isins)
        results = []
        for isin in isins:
            meta = self._figi.get(isin)
            results.append({"data": [meta]} if meta else {"warning": "No identifier found."})
        return _Resp(results)


class _FakeCM:
    def __init__(self, client: _FakeClient) -> None:
        self._c = client

    async def __aenter__(self) -> _FakeClient:
        return self._c

    async def __aexit__(self, *a) -> bool:
        return False


def _gleif_isins_payload(isins: list[str], total: int) -> dict:
    return {
        "data": [{"type": "isins", "attributes": {"isin": i}} for i in isins],
        "meta": {"pagination": {"total": total}},
    }


@pytest.fixture(autouse=True)
def _live_with_keys(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENFIGI_API_KEY", "figi-key")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "os-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# assemble_securities
# ---------------------------------------------------------------------------


async def test_offline_returns_unavailable(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["available"] is False
    assert out["total"] == 0 and out["securities"] == []


async def test_full_assembly_types_and_flags_sanctioned():
    client = _FakeClient(
        gleif=_gleif_isins_payload(["DE000A1", "DE000A2"], total=22499),
        openfigi_by_isin={
            "DE000A1": {"securityType2": "Warrant", "name": "DB Warrant", "ticker": "DBW", "exchCode": "GR", "marketSector": "Equity"},
            "DE000A2": {"securityType2": "Common Stock", "name": "DB Share", "ticker": "DBK", "exchCode": "GR", "marketSector": "Equity"},
            "XS0848530001": {"securityType2": "Bond", "name": "Sanctioned Bond", "ticker": None, "exchCode": "LSE", "marketSector": "Corp"},
        },
        opensanctions={
            "results": [
                {"id": "NK-1", "caption": "Sanctioned Bond", "target": True,
                 "datasets": ["us_ofac_sdn", "eu_fsf"], "topics": ["sanction"],
                 "properties": {"isin": ["XS0848530001"]}},
            ]
        },
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")

    assert out["available"] is True
    assert out["total"] == 22499
    # The GLEIF page is typed by OpenFIGI.
    page = {s["isin"]: s for s in out["securities"]}
    assert page["DE000A1"]["type"] == "Warrant"
    assert page["DE000A2"]["type"] == "Common Stock"
    assert all(not s["sanctioned"] for s in out["securities"])
    # Sanctioned ISIN (not in the GLEIF page) surfaces with regimes + type.
    assert len(out["sanctioned"]) == 1
    s = out["sanctioned"][0]
    assert s["isin"] == "XS0848530001" and s["sanctioned"] is True
    assert s["type"] == "Bond"
    assert "US OFAC SDN" in s["regimes"] and "EU" in s["regimes"]
    # CC-BY-NC notice present because there is sanctioned data.
    assert out["license_notices"] and out["license_notices"][0]["source_id"] == "opensanctions"
    assert set(out["sources"]) == {"gleif", "openfigi", "opensanctions"}


async def test_sanctioned_security_with_zero_gleif_isins():
    """Rosneft case: GLEIF has no ISINs, but OpenSanctions still surfaces the
    sanctioned ones."""
    client = _FakeClient(
        gleif=_gleif_isins_payload([], total=0),
        openfigi_by_isin={"US67812M2070": {"securityType2": "Depositary Receipt", "name": "ROSNEFT GDR", "exchCode": "OTC", "marketSector": "Equity"}},
        opensanctions={
            "results": [
                {"id": "NK-2", "caption": "Rosneft", "target": True,
                 "datasets": ["us_ofac_sdn"], "topics": ["sanction"],
                 "properties": {"isin": ["US67812M2070"]}},
            ]
        },
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("253400JT3MQWNDKMJE44")
    assert out["total"] == 0 and out["securities"] == []
    assert len(out["sanctioned"]) == 1
    assert out["sanctioned"][0]["type"] == "Depositary Receipt"


async def test_opensanctions_error_degrades_gracefully():
    class _OSErrClient(_FakeClient):
        async def get(self, url, params=None, headers=None):
            if "search/securities" in url:
                return _Resp({}, status=500)
            return await super().get(url, params=params, headers=headers)

    client = _OSErrClient(
        gleif=_gleif_isins_payload(["DE000A1"], total=1),
        openfigi_by_isin={"DE000A1": {"securityType2": "Warrant", "name": "W"}},
        opensanctions={},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["available"] is True
    assert out["sanctioned"] == []  # OS failed → no sanctioned, but not fatal
    assert out["securities"][0]["isin"] == "DE000A1"
    assert out["license_notices"] == []


async def test_unsanctioned_result_is_ignored():
    client = _FakeClient(
        gleif=_gleif_isins_payload(["DE000A1"], total=1),
        openfigi_by_isin={"DE000A1": {"securityType2": "Warrant"}},
        opensanctions={"results": [
            {"id": "X", "caption": "Not sanctioned", "target": False, "topics": ["corp.public"],
             "properties": {"isin": ["DE000A1"]}},
        ]},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["sanctioned"] == []
    assert out["securities"][0]["sanctioned"] is False


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


def test_endpoint_rejects_bad_lei():
    with TestClient(app) as client:
        r = client.get("/securities", params={"lei": "not-a-lei"})
    assert r.status_code == 400


def test_endpoint_offline_returns_available_false(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    with TestClient(app) as client:
        r = client.get("/securities", params={"lei": "7LTWFZYICNSX8D621K86"})
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is False
    assert body["lei"] == "7LTWFZYICNSX8D621K86"
