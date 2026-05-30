"""Tests for the Croatian Court Register (Sudski registar) adapter and BODS mapper.

Fixture data is modelled on a live ``/detalji_subjekta`` response for
INA-INDUSTRIJA NAFTE, d.d. (MBS 080000604, OIB 27759560625).

No network calls are made; the HTTP client is mocked at the httpx level.
``SUDREG_CLIENT_ID`` / ``SUDREG_CLIENT_SECRET`` and ``OPENCHECK_ALLOW_LIVE``
are monkeypatched.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import opencheck.sources.sudreg_croatia as sudreg_mod
from opencheck.sources.sudreg_croatia import (
    SUDREG_RA_CODE,
    SudregCroatiaAdapter,
    normalise_mbs,
)
from opencheck.bods.mapper import map_sudreg_croatia


# ---------------------------------------------------------------------------
# Fixtures — /detalji_subjekta response snapshot (INA d.d.)
# ---------------------------------------------------------------------------

_INA_SUBJECT: dict[str, Any] = {
    "mbs": 80000604,
    "oib": 27759560625,
    "potpuni_mbs": "080000604",
    "potpuni_oib": "27759560625",
    "status": 1,
    "datum_osnivanja": "1990-10-31T00:00:00",
    "tvrtka": {"ime": "INA-INDUSTRIJA NAFTE, d.d.", "naznaka_imena": "INA-INDUSTRIJA NAFTE"},
    "skracena_tvrtka": {"ime": "INA, d.d."},
    "sjediste": {
        "naziv_zupanije": "Grad Zagreb",
        "naziv_opcine": "Zagreb",
        "naziv_naselja": "Zagreb",
        "ulica": "Avenija Većeslava Holjevca",
        "kucni_broj": 10,
    },
    "pravni_oblik": {"vrsta_pravnog_oblika": {"sifra": 4, "naziv": "dioničko društvo", "kratica": "d.d."}},
    "temeljni_kapitali": [{"temeljni_kapital_rbr": 2, "valuta": {"sifra": 978, "naziv": "euro"}, "iznos": 1200000000}],
}


def _make_bundle(subject: dict[str, Any] | None = None, **overrides: Any) -> dict[str, Any]:
    bundle = {
        "source_id": "sudreg_croatia",
        "mbs": "080000604",
        "oib": "27759560625",
        "subject": subject if subject is not None else dict(_INA_SUBJECT),
        "legal_name": "INA-INDUSTRIJA NAFTE, D.D.",
        "is_stub": False,
    }
    bundle.update(overrides)
    return bundle


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestNormaliseMbs:
    def test_pads_to_9_digits(self) -> None:
        assert normalise_mbs("80000604") == "080000604"

    def test_already_padded_unchanged(self) -> None:
        assert normalise_mbs("080000604") == "080000604"

    def test_strips_non_digits_and_whitespace(self) -> None:
        assert normalise_mbs("  080-000-604 ") == "080000604"

    def test_empty_returns_empty(self) -> None:
        assert normalise_mbs("") == ""
        assert normalise_mbs("abc") == ""


class TestConstant:
    def test_ra_code(self) -> None:
        assert SUDREG_RA_CODE == "RA000156"


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


class TestMapSudregCroatia:
    def test_stub_yields_nothing(self) -> None:
        assert list(map_sudreg_croatia({"is_stub": True, "mbs": "080000604"})) == []

    def test_empty_bundle_yields_nothing(self) -> None:
        assert list(map_sudreg_croatia({})) == []

    def test_missing_subject_yields_nothing(self) -> None:
        assert list(map_sudreg_croatia(_make_bundle(subject={}))) == []

    def test_entity_statement_produced(self) -> None:
        stmts = list(map_sudreg_croatia(_make_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 1

    def test_entity_name(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        assert entity["recordDetails"]["name"] == "INA-INDUSTRIJA NAFTE, d.d."

    def test_name_falls_back_to_legal_name(self) -> None:
        subject = dict(_INA_SUBJECT)
        subject["tvrtka"] = {}
        subject["skracena_tvrtka"] = {}
        entity = next(s for s in map_sudreg_croatia(_make_bundle(subject=subject)) if s["recordType"] == "entity")
        assert entity["recordDetails"]["name"] == "INA-INDUSTRIJA NAFTE, D.D."

    def test_hr_mbs_identifier(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in entity["recordDetails"]["identifiers"]}
        assert ids["HR-MBS"] == "080000604"

    def test_hr_oib_identifier(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in entity["recordDetails"]["identifiers"]}
        assert ids["HR-OIB"] == "27759560625"

    def test_jurisdiction_hr(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        assert entity["recordDetails"]["jurisdiction"]["code"] == "HR"

    def test_founding_date_trimmed_from_iso(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        assert entity["recordDetails"]["foundingDate"] == "1990-10-31"

    def test_alternate_name_is_short_name(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        assert "INA, d.d." in (entity["recordDetails"].get("alternateNames") or [])

    def test_address_includes_street_and_house(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        addrs = entity["recordDetails"].get("addresses") or []
        assert addrs and "Avenija Većeslava Holjevca 10" in addrs[0]["address"]
        assert addrs[0]["country"]["code"] == "HR"

    def test_only_entity_statement_no_persons(self) -> None:
        for stmt in map_sudreg_croatia(_make_bundle()):
            assert stmt["recordType"] == "entity"

    def test_official_register_source_type(self) -> None:
        entity = next(s for s in map_sudreg_croatia(_make_bundle()) if s["recordType"] == "entity")
        assert "officialRegister" in entity["source"]["type"]

    def test_deterministic_ids(self) -> None:
        ids1 = [s["statementId"] for s in map_sudreg_croatia(_make_bundle())]
        ids2 = [s["statementId"] for s in map_sudreg_croatia(_make_bundle())]
        assert ids1 == ids2


# ---------------------------------------------------------------------------
# Adapter: fetch (unit — mocked HTTP)
# ---------------------------------------------------------------------------


def _mock_client(detalji_status: int = 200, detalji_json: Any = None) -> AsyncMock:
    """Build a mock httpx client supporting the OAuth POST + detail GET."""
    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json.return_value = {"access_token": "tok", "token_type": "bearer", "expires_in": 21600}

    detail_resp = MagicMock()
    detail_resp.status_code = detalji_status
    detail_resp.json.return_value = detalji_json if detalji_json is not None else dict(_INA_SUBJECT)

    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.post = AsyncMock(return_value=token_resp)
    client.get = AsyncMock(return_value=detail_resp)
    return client


@pytest.mark.asyncio
async def test_fetch_returns_bundle(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("SUDREG_CLIENT_ID", "cid..")
    monkeypatch.setenv("SUDREG_CLIENT_SECRET", "secret..")
    get_settings.cache_clear()
    sudreg_mod._token_cache = None

    with patch("opencheck.sources.sudreg_croatia.build_client", return_value=_mock_client()):
        adapter = SudregCroatiaAdapter()
        bundle = await adapter.fetch("80000604", legal_name="INA-INDUSTRIJA NAFTE, D.D.")

    get_settings.cache_clear()
    sudreg_mod._token_cache = None

    assert bundle.get("is_stub") is not True
    assert bundle["mbs"] == "080000604"
    assert bundle["oib"] == "27759560625"
    assert bundle["subject"]["tvrtka"]["ime"] == "INA-INDUSTRIJA NAFTE, d.d."


@pytest.mark.asyncio
async def test_fetch_not_found_returns_stub(monkeypatch, tmp_path) -> None:
    """A 400 (invalid/unknown identifier) response yields a stub bundle."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("SUDREG_CLIENT_ID", "cid..")
    monkeypatch.setenv("SUDREG_CLIENT_SECRET", "secret..")
    get_settings.cache_clear()
    sudreg_mod._token_cache = None

    client = _mock_client(detalji_status=400, detalji_json={"error_code": 508, "error_message": "x", "log_id": 1})
    with patch("opencheck.sources.sudreg_croatia.build_client", return_value=client):
        adapter = SudregCroatiaAdapter()
        bundle = await adapter.fetch("99999999")

    get_settings.cache_clear()
    sudreg_mod._token_cache = None

    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_fetch_no_credentials_returns_stub(monkeypatch, tmp_path) -> None:
    """Without credentials the adapter is not live → stub, no network call."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("SUDREG_CLIENT_ID", raising=False)
    monkeypatch.delenv("SUDREG_CLIENT_SECRET", raising=False)
    get_settings.cache_clear()

    adapter = SudregCroatiaAdapter()
    bundle = await adapter.fetch("080000604", legal_name="INA")
    assert bundle["is_stub"] is True
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fetch_empty_identifier_returns_stub(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    adapter = SudregCroatiaAdapter()
    bundle = await adapter.fetch("")
    assert bundle["is_stub"] is True
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_search_returns_empty(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings
    from opencheck.sources.base import SearchKind

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    adapter = SudregCroatiaAdapter()
    assert await adapter.search("INA", kind=SearchKind.ENTITY) == []
    get_settings.cache_clear()
