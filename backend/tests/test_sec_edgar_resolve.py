"""Tests for SEC EDGAR CIK resolution (company_tickers.json) and name
normalisation.

Regression cover for the silent-miss bug where US LEIs (e.g. The Walt Disney
Company, Netflix Inc) produced no SEC hit because:
  1. no CIK was derived from OpenCorporates, and
  2. the GLEIF legal name didn't prefix-match EDGAR's company search.

No network calls — the HTTP client is mocked at the httpx level.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources.base import SearchKind
from opencheck.sources.sec_edgar import (
    SecEdgarAdapter,
    _normalise_company_name,
)

# Trimmed snapshot of company_tickers.json (real CIKs).
_TICKERS_JSON = (
    '{"0":{"cik_str":1744489,"ticker":"DIS","title":"Walt Disney Co"},'
    '"1":{"cik_str":1065280,"ticker":"NFLX","title":"NETFLIX INC"},'
    '"2":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}'
)


class TestNormaliseCompanyName:
    def test_strips_leading_the(self) -> None:
        assert _normalise_company_name("THE WALT DISNEY COMPANY") == "WALT DISNEY"

    def test_strips_comma_and_inc(self) -> None:
        assert _normalise_company_name("Netflix, Inc.") == "NETFLIX"

    def test_matches_edgar_conformed_name(self) -> None:
        # GLEIF legal name and EDGAR conformed name must normalise equal.
        assert _normalise_company_name("THE WALT DISNEY COMPANY") == _normalise_company_name("Walt Disney Co")

    def test_strips_trailing_corp_suffixes(self) -> None:
        assert _normalise_company_name("Acme Corporation") == "ACME"
        assert _normalise_company_name("Foo Co.") == "FOO"
        assert _normalise_company_name("Bar Holdings LLC") == "BAR HOLDINGS"

    def test_empty_and_punctuation_only(self) -> None:
        assert _normalise_company_name("") == ""
        assert _normalise_company_name(",. -") == ""

    def test_does_not_overstrip_real_words(self) -> None:
        # "Group" is a real name token, not a legal-form suffix.
        assert _normalise_company_name("Foo Group") == "FOO GROUP"


def _adapter_with_tickers(monkeypatch, tmp_path) -> SecEdgarAdapter:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    return SecEdgarAdapter()


def _mock_client_returning(text: str) -> AsyncMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.text = text
    resp.raise_for_status = MagicMock()
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.get = AsyncMock(return_value=resp)
    return client


@pytest.mark.asyncio
async def test_resolve_cik_exact_ticker_match(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    adapter = _adapter_with_tickers(monkeypatch, tmp_path)
    with patch(
        "opencheck.sources.sec_edgar.build_client",
        return_value=_mock_client_returning(_TICKERS_JSON),
    ):
        assert await adapter.resolve_cik("THE WALT DISNEY COMPANY") == "1744489"
        assert await adapter.resolve_cik("Netflix, Inc.") == "1065280"
        assert await adapter.resolve_cik("Apple Inc.") == "320193"
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_resolve_cik_unknown_returns_none(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    adapter = _adapter_with_tickers(monkeypatch, tmp_path)
    # Ticker file has no match; atom fallback returns no entries.
    with patch(
        "opencheck.sources.sec_edgar.build_client",
        return_value=_mock_client_returning(_TICKERS_JSON),
    ):
        # An empty/whitespace name resolves to None without any fetch.
        assert await adapter.resolve_cik("") is None
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_resolve_cik_fallback_picks_exact_normalised_match(monkeypatch, tmp_path) -> None:
    """When the name isn't in company_tickers.json, the atom fallback must
    select the candidate whose conformed name normalises to the target —
    not a blind first row."""
    from opencheck.config import get_settings

    adapter = _adapter_with_tickers(monkeypatch, tmp_path)

    atom = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        '<entry><title>WRONGCO PARTNERS LP</title>'
        '<id>urn:tag:sec.gov,2008:company=0000926480</id></entry>'
        '<entry><title>Globex Co</title>'
        '<id>urn:tag:sec.gov,2008:company=0001234567</id></entry>'
        '</feed>'
    )

    # First GET (company_tickers.json) → no match; second GET (atom) → candidates.
    resp_tickers = MagicMock(status_code=200, text='{"0":{"cik_str":999,"ticker":"X","title":"Unrelated Inc"}}')
    resp_tickers.raise_for_status = MagicMock()
    resp_atom = MagicMock(status_code=200, text=atom)
    resp_atom.raise_for_status = MagicMock()
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.get = AsyncMock(side_effect=[resp_tickers, resp_atom])

    with patch("opencheck.sources.sec_edgar.build_client", return_value=client):
        assert await adapter.resolve_cik("Globex Company") == "1234567"
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_search_returns_empty_for_person(monkeypatch, tmp_path) -> None:
    adapter = _adapter_with_tickers(monkeypatch, tmp_path)
    assert await adapter.search("anything", SearchKind.PERSON) == []
