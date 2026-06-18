"""Tests for the securities service, /securities endpoint, and the bulk
sanctioned-securities index extractor.

GLEIF and OpenFIGI are mocked at the httpx level (no network). The OpenSanctions
sanctioned overlay reads a local JSON index (built by extract_securities.py),
so it's exercised with a temp fixture file.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from opencheck import securities as svc
from opencheck.app import app
from opencheck.config import get_settings

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Fake httpx client routed by URL (GLEIF + OpenFIGI only)
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
    def __init__(self, *, gleif: Any, openfigi_by_isin: dict) -> None:
        self._gleif = gleif
        self._figi = openfigi_by_isin

    async def get(self, url: str, params=None, headers=None) -> _Resp:
        assert "/isins" in url, f"unexpected GET {url}"
        return _Resp(self._gleif)

    async def post(self, url: str, json=None, headers=None) -> _Resp:
        assert url == svc._OPENFIGI_URL
        results = []
        for job in json:
            meta = self._figi.get(job["idValue"])
            results.append({"data": [meta]} if meta else {"warning": "No identifier found."})
        return _Resp(results)


class _FakeCM:
    def __init__(self, client: _FakeClient) -> None:
        self._c = client

    async def __aenter__(self) -> _FakeClient:
        return self._c

    async def __aexit__(self, *a) -> bool:
        return False


def _gleif_payload(isins: list[str], total: int) -> dict:
    return {
        "data": [{"type": "isins", "attributes": {"isin": i}} for i in isins],
        "meta": {"pagination": {"total": total}},
    }


def _write_index(tmp_path: Path, monkeypatch, mapping: dict) -> None:
    path = tmp_path / "sanctioned_isins.json"
    path.write_text(json.dumps(mapping), encoding="utf-8")
    monkeypatch.setenv("OPENCHECK_SECURITIES_INDEX_FILE", str(path))
    svc.reset_index_cache()
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENFIGI_API_KEY", "figi-key")
    get_settings.cache_clear()
    svc.reset_index_cache()
    yield
    get_settings.cache_clear()
    svc.reset_index_cache()


# ---------------------------------------------------------------------------
# assemble_securities
# ---------------------------------------------------------------------------


async def test_offline_returns_unavailable(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["available"] is False and out["securities"] == []


async def test_full_assembly_types_and_flags_sanctioned(monkeypatch, tmp_path):
    _write_index(tmp_path, monkeypatch, {
        "7LTWFZYICNSX8D621K86": {
            "name": "Deutsche Bank", "id": "NK-1",
            "isins": ["XS0848530001"], "regimes": ["US OFAC SDN", "EU"],
        },
    })
    client = _FakeClient(
        gleif=_gleif_payload(["DE000A1", "DE000A2"], total=22499),
        openfigi_by_isin={
            "DE000A1": {"securityType2": "Warrant", "name": "DB Warrant", "ticker": "DBW", "exchCode": "GR"},
            "DE000A2": {"securityType2": "Common Stock", "name": "DB Share", "ticker": "DBK", "exchCode": "GR"},
            "XS0848530001": {"securityType2": "Bond", "name": "Sanctioned Bond", "exchCode": "LSE"},
        },
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")

    assert out["available"] is True and out["total"] == 22499
    page = {s["isin"]: s for s in out["securities"]}
    assert page["DE000A1"]["type"] == "Warrant"
    assert all(not s["sanctioned"] for s in out["securities"])
    assert len(out["sanctioned"]) == 1
    s = out["sanctioned"][0]
    assert s["isin"] == "XS0848530001" and s["type"] == "Bond"
    assert "US OFAC SDN" in s["regimes"] and "EU" in s["regimes"]
    assert out["license_notices"] and out["license_notices"][0]["source_id"] == "opensanctions"
    assert set(out["sources"]) == {"gleif", "openfigi", "opensanctions"}


async def test_sanctioned_security_with_zero_gleif_isins(monkeypatch, tmp_path):
    """Rosneft case: GLEIF has no ISINs, but the index still surfaces sanctioned ones."""
    _write_index(tmp_path, monkeypatch, {
        "253400JT3MQWNDKMJE44": {
            "name": "Rosneft", "id": "NK-2",
            "isins": ["US67812M2070"], "regimes": ["US OFAC SDN", "EO 14071 investment ban"],
        },
    })
    client = _FakeClient(
        gleif=_gleif_payload([], total=0),
        openfigi_by_isin={"US67812M2070": {"securityType2": "Depositary Receipt", "name": "ROSNEFT GDR", "exchCode": "OTC"}},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("253400JT3MQWNDKMJE44")
    assert out["total"] == 0 and out["securities"] == []
    assert len(out["sanctioned"]) == 1
    assert out["sanctioned"][0]["type"] == "Depositary Receipt"
    assert "EO 14071 investment ban" in out["sanctioned"][0]["regimes"]


async def test_overlay_off_without_index(monkeypatch, tmp_path):
    """No index file configured → GLEIF + OpenFIGI only, no sanctioned banner."""
    monkeypatch.delenv("OPENCHECK_SECURITIES_INDEX_FILE", raising=False)
    get_settings.cache_clear()
    client = _FakeClient(
        gleif=_gleif_payload(["DE000A1"], total=1),
        openfigi_by_isin={"DE000A1": {"securityType2": "Warrant"}},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["sanctioned"] == []
    assert "opensanctions" not in out["sources"]
    assert out["license_notices"] == []


async def test_lei_not_in_index_is_clean(monkeypatch, tmp_path):
    _write_index(tmp_path, monkeypatch, {"OTHERLEI000000000000": {"isins": ["X"], "regimes": []}})
    client = _FakeClient(
        gleif=_gleif_payload(["DE000A1"], total=1),
        openfigi_by_isin={"DE000A1": {"securityType2": "Warrant"}},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["sanctioned"] == []
    assert out["securities"][0]["sanctioned"] is False
    # The source was still consulted (index configured), so it's listed.
    assert "opensanctions" in out["sources"]


async def test_overlay_from_url(monkeypatch, tmp_path):
    """The index can be loaded from a URL (GitHub raw / release asset / S3)."""
    monkeypatch.delenv("OPENCHECK_SECURITIES_INDEX_FILE", raising=False)
    monkeypatch.setenv("OPENCHECK_SECURITIES_INDEX_URL", "https://example.com/idx.json")
    get_settings.cache_clear()
    svc.reset_index_cache()
    blob = json.dumps({
        "7LTWFZYICNSX8D621K86": {"name": "DB", "id": "NK-1", "isins": ["XS0848530001"], "regimes": ["EU"]},
    }).encode("utf-8")

    class _U:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return blob

    client = _FakeClient(
        gleif=_gleif_payload([], total=0),
        openfigi_by_isin={"XS0848530001": {"securityType2": "Bond"}},
    )
    with patch("opencheck.securities.urllib.request.urlopen", lambda url, timeout=30: _U()):
        with patch.object(svc, "build_client", lambda: _FakeCM(client)):
            out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert len(out["sanctioned"]) == 1
    assert out["sanctioned"][0]["isin"] == "XS0848530001"
    assert "opensanctions" in out["sources"]


def test_sanctioned_securities_signal(monkeypatch, tmp_path):
    _write_index(tmp_path, monkeypatch, {
        "7LTWFZYICNSX8D621K86": {
            "id": "NK-1", "isins": ["XS1", "XS2"],
            "regimes": ["US OFAC SDN", "EU"], "eo_14071": True,
        },
    })
    sig = svc.sanctioned_securities_signal("7ltwfzyicnsx8d621k86")  # case-insensitive
    assert sig is not None
    assert sig["code"] == "SANCTIONED_SECURITY" and sig["confidence"] == "high"
    assert sig["evidence"]["isin_count"] == 2
    assert sig["evidence"]["eo_14071"] is True
    assert "US OFAC SDN" in sig["summary"]
    assert svc.sanctioned_securities_signal("OTHERLEI000000000000") is None


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
    assert r.json()["available"] is False


# ---------------------------------------------------------------------------
# extract_securities.py — CSV → index
# ---------------------------------------------------------------------------


def test_extractor_builds_index_filtering_correctly():
    ex = _load_script("extract_securities")
    rows = [
        # sanctioned, has LEI + ISINs → kept
        {"caption": "Rosneft", "lei": "253400JT3MQWNDKMJE44", "isins": "US67812M2070;XS0123456789",
         "sanctioned": "t", "eo_14071": "t",
         "risk_datasets": "us_ofac_sdn;gb_hmt_invbans;ext_eu_esma_firds;ext_gb_fca_firds", "id": "NK-2"},
        # private sanctioned co, no LEI → dropped
        {"caption": "Private LLC", "lei": "", "isins": "", "sanctioned": "t", "eo_14071": "f",
         "risk_datasets": "us_ofac_sdn", "id": "NK-3"},
        # has LEI but not sanctioned/eo (just a reference row) → dropped
        {"caption": "Clean Corp", "lei": "549300CLEANCLEAN0001", "isins": "GB00CLEAN001",
         "sanctioned": "f", "eo_14071": "f", "risk_datasets": "", "id": "NK-4"},
        # sanctioned + LEI but no ISINs → dropped (nothing to show)
        {"caption": "No Sec", "lei": "549300NOSEC00000001", "isins": "",
         "sanctioned": "t", "eo_14071": "f", "risk_datasets": "eu_fsf", "id": "NK-5"},
    ]
    index = ex.build_index(rows)
    assert set(index) == {"253400JT3MQWNDKMJE44"}
    entry = index["253400JT3MQWNDKMJE44"]
    assert entry["isins"] == ["US67812M2070", "XS0123456789"]
    assert "US OFAC SDN" in entry["regimes"]
    assert "UK investment ban" in entry["regimes"]
    assert "EO 14071 investment ban" in entry["regimes"]
    # External reference datasets (FIRDS etc.) are not sanction regimes.
    assert not any("firds" in r for r in entry["regimes"])
    assert not any(r.startswith("ext_") for r in entry["regimes"])
    assert entry["eo_14071"] is True and entry["sanctioned"] is True
