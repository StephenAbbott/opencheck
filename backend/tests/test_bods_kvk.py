"""Tests for the KvK → BODS v0.4 mapper and KvKAdapter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio

from opencheck.bods import map_kvk, validate_shape
from opencheck.sources.kvk import KVK_RA_CODE, KvKAdapter, normalise_kvk

# ---------------------------------------------------------------------------
# Sample fixtures (based on KvK Open Data API schema)
# ---------------------------------------------------------------------------

_COMPANY_SPLITTY = {
    "datumAanvang": "20250202",
    "actief": "J",
    "rechtsvormCode": "BV",
    "postcodeRegio": 10,
    "activiteiten": [
        {"sbiCode": "6201", "soortActiviteit": "Hoofdactiviteit"},
    ],
    "lidstaat": "NL",
}

_COMPANY_INACTIVE = {
    "datumAanvang": "20101231",
    "actief": "N",
    "rechtsvormCode": "NV",
    "postcodeRegio": 11,
    "activiteiten": [],
    "lidstaat": "NL",
}

_COMPANY_INSOLVENT = {
    "datumAanvang": "20150601",
    "actief": "J",
    "insolventieCode": "FAIL",
    "rechtsvormCode": "BV",
    "postcodeRegio": 20,
    "activiteiten": [{"sbiCode": "4641", "soortActiviteit": "Nevenactiviteit"}],
    "lidstaat": "NL",
}

_COMPANY_NO_DATE = {
    "actief": "J",
    "rechtsvormCode": "BV",
    "postcodeRegio": 5,
    "activiteiten": [],
    "lidstaat": "NL",
}


def _bundle(
    company: dict | None = None,
    kvk_number: str = "96332751",
    legal_name: str = "Splitty B.V.",
) -> dict:
    return {
        "source_id": "kvk",
        "kvk_number": kvk_number,
        "company": company if company is not None else _COMPANY_SPLITTY,
        "legal_name": legal_name,
        "is_stub": False,
    }


# ---------------------------------------------------------------------------
# KvK number normalisation utility
# ---------------------------------------------------------------------------


def test_normalise_kvk_8_digit() -> None:
    assert normalise_kvk("96332751") == "96332751"


def test_normalise_kvk_zero_pad() -> None:
    assert normalise_kvk("1234567") == "01234567"


def test_normalise_kvk_strips_whitespace() -> None:
    assert normalise_kvk("  59581883  ") == "59581883"


def test_kvk_ra_code() -> None:
    assert KVK_RA_CODE == "RA000463"


# ---------------------------------------------------------------------------
# map_kvk — basic shape
# ---------------------------------------------------------------------------


def test_map_kvk_produces_one_entity() -> None:
    stmts = list(map_kvk(_bundle()))
    assert len(stmts) == 1
    assert stmts[0]["recordType"] == "entity"


def test_map_kvk_entity_name() -> None:
    stmts = list(map_kvk(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "Splitty B.V."


def test_map_kvk_entity_jurisdiction() -> None:
    stmts = list(map_kvk(_bundle()))
    jur = stmts[0]["recordDetails"]["jurisdiction"]
    assert jur["code"] == "NL"
    assert "Netherlands" in jur["name"]


def test_map_kvk_identifier_scheme() -> None:
    stmts = list(map_kvk(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "NL-KVK" in schemes


def test_map_kvk_identifier_value() -> None:
    stmts = list(map_kvk(_bundle()))
    kvk_id = next(
        i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "NL-KVK"
    )
    assert kvk_id == "96332751"


# ---------------------------------------------------------------------------
# map_kvk — founding date
# ---------------------------------------------------------------------------


def test_map_kvk_founding_date_parsed() -> None:
    stmts = list(map_kvk(_bundle()))
    assert stmts[0]["recordDetails"]["foundingDate"] == "2025-02-02"


def test_map_kvk_founding_date_absent_when_missing() -> None:
    stmts = list(map_kvk(_bundle(_COMPANY_NO_DATE)))
    assert "foundingDate" not in stmts[0]["recordDetails"]


def test_map_kvk_founding_date_different_company() -> None:
    stmts = list(map_kvk(_bundle(_COMPANY_INACTIVE, legal_name="Inactive Corp NV")))
    assert stmts[0]["recordDetails"]["foundingDate"] == "2010-12-31"


# ---------------------------------------------------------------------------
# map_kvk — early exits
# ---------------------------------------------------------------------------


def test_map_kvk_stub_returns_empty() -> None:
    bundle = {
        "source_id": "kvk",
        "kvk_number": "12345678",
        "company": None,
        "legal_name": "Some Corp",
        "is_stub": True,
    }
    assert list(map_kvk(bundle)) == []


def test_map_kvk_empty_company_returns_empty() -> None:
    bundle = {
        "source_id": "kvk",
        "kvk_number": "12345678",
        "company": {},
        "legal_name": "Some Corp",
        "is_stub": False,
    }
    assert list(map_kvk(bundle)) == []


def test_map_kvk_missing_legal_name_returns_empty() -> None:
    stmts = list(map_kvk(_bundle(legal_name="")))
    assert stmts == []


def test_map_kvk_missing_kvk_number_returns_empty() -> None:
    bundle = {
        "source_id": "kvk",
        "kvk_number": "",
        "company": _COMPANY_SPLITTY,
        "legal_name": "Splitty B.V.",
        "is_stub": False,
    }
    assert list(map_kvk(bundle)) == []


# ---------------------------------------------------------------------------
# map_kvk — source block
# ---------------------------------------------------------------------------


def test_map_kvk_source_url_contains_kvk_number() -> None:
    stmts = list(map_kvk(_bundle()))
    source = stmts[0].get("source") or {}
    url = source.get("url", "")
    assert "96332751" in url


# ---------------------------------------------------------------------------
# BODS validator compliance
# ---------------------------------------------------------------------------


def test_map_kvk_passes_validator() -> None:
    issues = validate_shape(map_kvk(_bundle()))
    assert issues == [], issues


def test_map_kvk_inactive_company_passes_validator() -> None:
    issues = validate_shape(map_kvk(_bundle(_COMPANY_INACTIVE, legal_name="Inactive Corp NV")))
    assert issues == [], issues


def test_map_kvk_insolvent_company_passes_validator() -> None:
    issues = validate_shape(map_kvk(_bundle(_COMPANY_INSOLVENT, legal_name="Bankrupt BV")))
    assert issues == [], issues


def test_map_kvk_no_date_passes_validator() -> None:
    issues = validate_shape(map_kvk(_bundle(_COMPANY_NO_DATE)))
    assert issues == [], issues


# ---------------------------------------------------------------------------
# KvKAdapter.fetch() — 429 retry logic
# ---------------------------------------------------------------------------

_KVK_API_RESPONSE = {
    "datumAanvang": "20250101",
    "actief": "J",
    "rechtsvormCode": "NV",
    "postcodeRegio": 1,
    "activiteiten": [],
    "lidstaat": "NL",
}


def _make_response(status_code: int, json_body: dict | None = None, headers: dict | None = None):
    """Return a minimal httpx.Response mock."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = httpx.Headers(headers or {})
    if json_body is not None:
        resp.json = MagicMock(return_value=json_body)
    if status_code >= 400:
        resp.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                f"{status_code}",
                request=MagicMock(),
                response=resp,
            )
        )
    else:
        resp.raise_for_status = MagicMock(return_value=None)
    return resp


@pytest.mark.asyncio
async def test_kvk_fetch_success_no_retry(monkeypatch) -> None:
    """Happy-path: single successful GET, no retry needed."""
    ok_resp = _make_response(200, _KVK_API_RESPONSE)

    monkeypatch.setattr(
        "opencheck.sources.kvk.get_settings",
        lambda: MagicMock(allow_live=True),
    )

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=ok_resp)

    with patch("opencheck.sources.kvk.build_client", return_value=mock_client):
        adapter = KvKAdapter()
        # Bypass cache
        adapter._cache.get_payload = MagicMock(return_value=None)
        adapter._cache.put = MagicMock()

        bundle = await adapter.fetch("35000363", legal_name="Ahold Delhaize")

    assert mock_client.get.call_count == 1
    assert bundle["company"] == _KVK_API_RESPONSE
    assert bundle["legal_name"] == "Ahold Delhaize"


@pytest.mark.asyncio
async def test_kvk_fetch_retries_on_429_then_succeeds(monkeypatch) -> None:
    """One 429 followed by a 200 — should succeed after one retry."""
    resp_429 = _make_response(429, headers={"Retry-After": "0"})
    resp_200 = _make_response(200, _KVK_API_RESPONSE)

    monkeypatch.setattr(
        "opencheck.sources.kvk.get_settings",
        lambda: MagicMock(allow_live=True),
    )
    monkeypatch.setattr("opencheck.sources.kvk.asyncio.sleep", AsyncMock())

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(side_effect=[resp_429, resp_200])

    with patch("opencheck.sources.kvk.build_client", return_value=mock_client):
        adapter = KvKAdapter()
        adapter._cache.get_payload = MagicMock(return_value=None)
        adapter._cache.put = MagicMock()

        bundle = await adapter.fetch("35000363")

    assert mock_client.get.call_count == 2
    assert bundle["company"] == _KVK_API_RESPONSE


@pytest.mark.asyncio
async def test_kvk_fetch_raises_after_max_retries(monkeypatch) -> None:
    """All attempts return 429 — should raise HTTPStatusError after exhausting retries."""
    resp_429 = _make_response(429, headers={"Retry-After": "0"})

    monkeypatch.setattr(
        "opencheck.sources.kvk.get_settings",
        lambda: MagicMock(allow_live=True),
    )
    monkeypatch.setattr("opencheck.sources.kvk.asyncio.sleep", AsyncMock())

    # _MAX_RETRIES = 3 means 4 total attempts (initial + 3 retries).
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=resp_429)

    with patch("opencheck.sources.kvk.build_client", return_value=mock_client):
        adapter = KvKAdapter()
        adapter._cache.get_payload = MagicMock(return_value=None)
        adapter._cache.put = MagicMock()

        with pytest.raises(httpx.HTTPStatusError):
            await adapter.fetch("35000363")

    # 4 total attempts (_MAX_RETRIES=3, so initial + 3)
    assert mock_client.get.call_count == 4


@pytest.mark.asyncio
async def test_kvk_fetch_uses_exponential_backoff_without_retry_after(monkeypatch) -> None:
    """Without Retry-After, backoff should double: 2s, 4s."""
    sleep_calls: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    resp_429 = _make_response(429)  # no Retry-After header
    resp_200 = _make_response(200, _KVK_API_RESPONSE)

    monkeypatch.setattr(
        "opencheck.sources.kvk.get_settings",
        lambda: MagicMock(allow_live=True),
    )
    monkeypatch.setattr("opencheck.sources.kvk.asyncio.sleep", fake_sleep)

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(side_effect=[resp_429, resp_429, resp_200])

    with patch("opencheck.sources.kvk.build_client", return_value=mock_client):
        adapter = KvKAdapter()
        adapter._cache.get_payload = MagicMock(return_value=None)
        adapter._cache.put = MagicMock()

        await adapter.fetch("35000363")

    assert sleep_calls == [2.0, 4.0]
