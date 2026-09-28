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

import httpx
import pytest
from fastapi.testclient import TestClient

from opencheck import securities as svc
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.gleif_throttle import GleifRateLimitedError

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
    def __init__(self, *, gleif: Any, openfigi_by_isin: dict, gleif_error: Exception | None = None) -> None:
        self._gleif = gleif
        self._figi = openfigi_by_isin
        self._gleif_error = gleif_error

    async def get(self, url: str, params=None, headers=None) -> _Resp:
        assert "/isins" in url, f"unexpected GET {url}"
        if self._gleif_error is not None:
            raise self._gleif_error
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


def _gleif_429() -> httpx.HTTPStatusError:
    """The exact shape the Phase 143 transport hands back after its one retry."""
    req = httpx.Request("GET", svc._GLEIF_ISINS_URL.format(lei="7LTWFZYICNSX8D621K86"))
    resp = httpx.Response(429, request=req)
    return httpx.HTTPStatusError(
        "Client error '429 Too Many Requests'", request=req, response=resp
    )


@pytest.mark.parametrize(
    "gleif_error",
    [_gleif_429(), GleifRateLimitedError("budget exhausted"), httpx.ConnectTimeout("timed out")],
    ids=["429-handed-back", "throttle-refused-to-send", "network"],
)
async def test_gleif_failure_still_serves_sanctioned_overlay(monkeypatch, tmp_path, gleif_error):
    """Phase 145: the sanctions check reads a LOCAL index — GLEIF saying no
    (crawler-saturation 429s, live 2026-08-29) must degrade the ISIN list,
    not 500 the one check this section exists for."""
    _write_index(tmp_path, monkeypatch, {
        "7LTWFZYICNSX8D621K86": {
            "name": "Deutsche Bank", "id": "NK-1",
            "isins": ["XS0848530001"], "regimes": ["US OFAC SDN", "EU"],
        },
    })
    client = _FakeClient(
        gleif=None,
        gleif_error=gleif_error,
        # OpenFIGI is a different host, untouched by GLEIF's rate limit — the
        # sanctioned ISIN must still be typed for the banner.
        openfigi_by_isin={"XS0848530001": {"securityType2": "Bond", "name": "Sanctioned Bond"}},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")

    assert out["available"] is True
    assert out["isin_list_available"] is False
    assert out["isin_list_unavailable_reason"] in {"rate_limited", "unreachable"}
    assert out["total"] == 0 and out["securities"] == []
    assert len(out["sanctioned"]) == 1
    s = out["sanctioned"][0]
    assert s["isin"] == "XS0848530001" and s["type"] == "Bond"
    assert "US OFAC SDN" in s["regimes"]
    # GLEIF did not answer, so it is not claimed as a source; the overlay is.
    assert "gleif" not in out["sources"]
    assert "opensanctions" in out["sources"]
    assert out["license_notices"] and out["license_notices"][0]["source_id"] == "opensanctions"


async def test_gleif_failure_with_no_index_degrades_without_raising(monkeypatch, tmp_path):
    """No overlay configured AND GLEIF down → nothing to show, still no 500.
    The frontend reads isin_list_available + sources and reports the panel
    error itself (the check genuinely did not run on such a deployment)."""
    monkeypatch.delenv("OPENCHECK_SECURITIES_INDEX_FILE", raising=False)
    get_settings.cache_clear()
    client = _FakeClient(gleif=None, gleif_error=_gleif_429(), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["available"] is True
    assert out["isin_list_available"] is False
    assert out["sanctioned"] == [] and out["securities"] == []
    assert "gleif" not in out["sources"] and "opensanctions" not in out["sources"]


async def test_gleif_success_reports_isin_list_available(monkeypatch, tmp_path):
    client = _FakeClient(
        gleif=_gleif_payload(["DE000A1"], total=1),
        openfigi_by_isin={"DE000A1": {"securityType2": "Warrant"}},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("7LTWFZYICNSX8D621K86")
    assert out["isin_list_available"] is True
    assert "gleif" in out["sources"]


# ---------------------------------------------------------------------------
# Phase 253: the GLEIF ISIN page is cached; a stale page stands in, labelled
# ---------------------------------------------------------------------------


class _CountingClient(_FakeClient):
    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.gleif_calls = 0

    async def get(self, url: str, params=None, headers=None) -> _Resp:
        self.gleif_calls += 1
        return await super().get(url, params=params, headers=headers)


def _age_cache_entry(lei: str, days: float, page: int = 1) -> None:
    """Rewrite a cached ISIN page's fetch time to ``days`` ago."""
    hit = svc._isins_cache.get(svc._isins_cache_key(lei, page, svc.PAGE_SIZE))
    assert hit is not None, "expected a cached page"
    wrapped = hit.payload
    wrapped["_cached_at"] = wrapped["_cached_at"] - days * 86_400
    hit.path.write_text(json.dumps(wrapped), encoding="utf-8")


_LEI = "7LTWFZYICNSX8D621K86"


async def test_a_fresh_cached_page_is_served_without_asking_gleif(monkeypatch, tmp_path):
    ok = _CountingClient(gleif=_gleif_payload(["DE000A1"], total=1), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        first = await svc.assemble_securities(_LEI)
        second = await svc.assemble_securities(_LEI)
    assert ok.gleif_calls == 1
    assert second["total"] == 1 and [s["isin"] for s in second["securities"]] == ["DE000A1"]
    assert second["isin_list_available"] is True and second["isin_list_stale"] is False
    assert second["isin_list_unavailable_reason"] is None
    assert second["isin_list_as_of"] == first["isin_list_as_of"]
    assert "gleif" in second["sources"]


async def test_zero_isins_is_cached_too(monkeypatch, tmp_path):
    """Most LEIs have no ISINs at all (97% in GLEIF's own mapping file) —
    the empty answer is the one worth not re-asking for."""
    ok = _CountingClient(gleif=_gleif_payload([], total=0), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        await svc.assemble_securities(_LEI)
        out = await svc.assemble_securities(_LEI)
    assert ok.gleif_calls == 1
    assert out["isin_list_available"] is True and out["total"] == 0


async def test_a_day_old_page_is_re_asked(monkeypatch, tmp_path):
    ok = _CountingClient(gleif=_gleif_payload(["DE000A1"], total=1), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        await svc.assemble_securities(_LEI)
        _age_cache_entry(_LEI, 1.5)
        out = await svc.assemble_securities(_LEI)
    assert ok.gleif_calls == 2
    assert out["isin_list_stale"] is False


@pytest.mark.parametrize(
    ("gleif_error", "reason"),
    [
        (GleifRateLimitedError("reserved", reason="held_for_lookups"), "held_for_lookups"),
        (GleifRateLimitedError("budget exhausted"), "rate_limited"),
        (_gleif_429(), "rate_limited"),
        (httpx.ConnectTimeout("timed out"), "unreachable"),
    ],
    ids=["held-for-lookups", "throttle-budget", "429", "network"],
)
async def test_a_stale_page_stands_in_labelled_when_gleif_cannot_be_asked(
    monkeypatch, tmp_path, gleif_error, reason
):
    ok = _FakeClient(gleif=_gleif_payload(["DE000A1"], total=1), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        first = await svc.assemble_securities(_LEI)
    _age_cache_entry(_LEI, 5)
    down = _FakeClient(gleif=None, gleif_error=gleif_error, openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(down)):
        out = await svc.assemble_securities(_LEI)
    assert out["isin_list_available"] is True
    assert out["isin_list_stale"] is True
    assert out["isin_list_unavailable_reason"] == reason
    assert out["total"] == 1 and [s["isin"] for s in out["securities"]] == ["DE000A1"]
    # Dated to when GLEIF was asked — five days before the first answer.
    from datetime import datetime, timedelta
    as_of = datetime.fromisoformat(out["isin_list_as_of"])
    assert as_of < datetime.fromisoformat(first["isin_list_as_of"]) - timedelta(days=4)
    assert "gleif" in out["sources"]


async def test_a_month_old_page_does_not_stand_in(monkeypatch, tmp_path):
    ok = _FakeClient(gleif=_gleif_payload(["DE000A1"], total=1), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        await svc.assemble_securities(_LEI)
    _age_cache_entry(_LEI, svc.ISINS_STALE_MAX_DAYS + 1)
    down = _FakeClient(
        gleif=None,
        gleif_error=GleifRateLimitedError("reserved", reason="held_for_lookups"),
        openfigi_by_isin={},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(down)):
        out = await svc.assemble_securities(_LEI)
    assert out["isin_list_available"] is False
    assert out["isin_list_stale"] is False and out["isin_list_as_of"] is None
    assert out["isin_list_unavailable_reason"] == "held_for_lookups"
    assert out["securities"] == [] and "gleif" not in out["sources"]


async def test_a_failed_answer_is_never_cached(monkeypatch, tmp_path):
    down = _FakeClient(gleif=None, gleif_error=_gleif_429(), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(down)):
        await svc.assemble_securities(_LEI)
    assert svc._isins_cache.get(svc._isins_cache_key(_LEI, 1, svc.PAGE_SIZE)) is None


async def test_pages_are_cached_separately(monkeypatch, tmp_path):
    ok = _CountingClient(gleif=_gleif_payload(["DE000A1"], total=40), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        await svc.assemble_securities(_LEI, page=1)
        await svc.assemble_securities(_LEI, page=2)
        await svc.assemble_securities(_LEI, page=2)
    assert ok.gleif_calls == 2


def test_endpoint_carries_the_reason_and_stale_fields(monkeypatch, tmp_path):
    fake = _FakeClient(
        gleif=None,
        gleif_error=GleifRateLimitedError("reserved", reason="held_for_lookups"),
        openfigi_by_isin={},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(fake)):
        with TestClient(app) as client:
            r = client.get("/securities", params={"lei": _LEI})
    body = r.json()
    assert r.status_code == 200
    assert body["isin_list_available"] is False
    assert body["isin_list_unavailable_reason"] == "held_for_lookups"
    assert body["isin_list_stale"] is False and body["isin_list_as_of"] is None


# ---------------------------------------------------------------------------
# Phase 258: GLEIF's ISIN-to-LEI file answers first
# ---------------------------------------------------------------------------


class _NoGleifClient(_FakeClient):
    """Fails the test if /securities calls GLEIF while a table can answer."""

    async def get(self, url: str, params=None, headers=None) -> _Resp:
        raise AssertionError(f"GLEIF was called with a usable table: {url}")


@pytest.fixture
def isin_table(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    from opencheck import isin_index

    csv_path = tmp_path / "lei-isin.csv"
    rows = [f"{_LEI},DE000A{i:06d}" for i in range(25)]
    csv_path.write_text("LEI,ISIN\n" + "\n".join(reversed(rows)) + "\n", encoding="utf-8")
    db = tmp_path / "isin.sqlite"
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(db))
    get_settings.cache_clear()
    isin_index.reset_for_tests()
    uploaded = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    isin_index.build_index(csv_path, db, file_name="isin-lei-x.zip", uploaded_at=uploaded)
    yield uploaded
    isin_index.reset_for_tests()
    get_settings.cache_clear()


async def test_the_table_answers_without_asking_gleif(isin_table):
    from opencheck import gleifstats

    client = _NoGleifClient(gleif=None, openfigi_by_isin={"DE000A000000": {"securityType2": "Bond"}})
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities(_LEI)
    assert out["total"] == 25
    assert [s["isin"] for s in out["securities"]] == [f"DE000A{i:06d}" for i in range(20)]
    assert out["securities"][0]["type"] == "Bond"
    assert out["isin_list_available"] is True and out["isin_list_stale"] is False
    assert out["isin_list_source"] == "gleif_file"
    assert out["isin_list_unavailable_reason"] is None
    assert out["isin_list_as_of"].startswith(isin_table[:16])
    assert "gleif" in out["sources"]
    assert gleifstats.stats()["securities_served"]["file"] == 1


async def test_the_table_pages_in_isin_order(isin_table):
    client = _NoGleifClient(gleif=None, openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities(_LEI, page=2)
    assert [s["isin"] for s in out["securities"]] == [f"DE000A{i:06d}" for i in range(20, 25)]


async def test_an_lei_the_file_does_not_list_is_none_with_no_call(isin_table):
    client = _NoGleifClient(gleif=None, openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        out = await svc.assemble_securities("213800WMPZ7LH3F92517")
    assert out["total"] == 0 and out["securities"] == []
    assert out["isin_list_available"] is True
    assert out["isin_list_source"] == "gleif_file"


async def test_an_old_table_falls_back_to_the_live_call(isin_table, monkeypatch):
    monkeypatch.setenv("OPENCHECK_ISIN_INDEX_MAX_AGE_DAYS", "-1")
    get_settings.cache_clear()
    ok = _CountingClient(gleif=_gleif_payload(["DE000A1"], total=1), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        out = await svc.assemble_securities(_LEI)
    assert ok.gleif_calls == 1
    assert out["total"] == 1 and out["isin_list_source"] == "gleif_api"


async def test_without_a_table_the_source_is_the_api(monkeypatch, tmp_path):
    ok = _FakeClient(gleif=_gleif_payload(["DE000A1"], total=1), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(ok)):
        out = await svc.assemble_securities(_LEI)
    assert out["isin_list_source"] == "gleif_api"


async def test_a_failed_list_has_no_source(monkeypatch, tmp_path):
    down = _FakeClient(gleif=None, gleif_error=_gleif_429(), openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(down)):
        out = await svc.assemble_securities(_LEI)
    assert out["isin_list_source"] is None


def test_endpoint_carries_the_list_source(isin_table):
    client = _NoGleifClient(gleif=None, openfigi_by_isin={})
    with patch.object(svc, "build_client", lambda: _FakeCM(client)):
        with TestClient(app) as tc:
            body = tc.get("/securities", params={"lei": _LEI}).json()
    assert body["isin_list_source"] == "gleif_file" and body["total"] == 25


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


def test_endpoint_returns_200_with_overlay_when_gleif_rate_limited(monkeypatch, tmp_path):
    """The live 2026-08-29 failure, end to end: GLEIF 429 → 200, not 500,
    with the sanctioned overlay applied and the degradation declared."""
    _write_index(tmp_path, monkeypatch, {
        "7LTWFZYICNSX8D621K86": {
            "name": "Deutsche Bank", "id": "NK-1",
            "isins": ["XS0848530001"], "regimes": ["EU"],
        },
    })
    fake = _FakeClient(
        gleif=None,
        gleif_error=_gleif_429(),
        openfigi_by_isin={"XS0848530001": {"securityType2": "Bond"}},
    )
    with patch.object(svc, "build_client", lambda: _FakeCM(fake)):
        with TestClient(app) as client:
            r = client.get("/securities", params={"lei": "7LTWFZYICNSX8D621K86"})
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert body["isin_list_available"] is False
    assert body["total"] == 0 and body["securities"] == []
    assert [s["isin"] for s in body["sanctioned"]] == ["XS0848530001"]
    assert "gleif" not in body["sources"] and "opensanctions" in body["sources"]


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
