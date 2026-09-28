"""Phase 254 — a GLEIF legal name resolves to its EDGAR CIK.

Phase 252 fixed the filing fetch, and Moody's still read "no record" in
production: the CIK step failed first. GLEIF's "MOODY'S CORPORATION"
normalised to "MOODY S" (apostrophe → space) and EDGAR's "MOODYS CORP /DE/"
to "MOODYS CORP DE" (the state tag stopped the legal-form strip), so neither
the ticker index nor the company-search fallback could match. 552 of the
8,004 titles in company_tickers.json carry a trailing tag like that.

Titles below are real company_tickers.json rows (28 Sept 2026).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.config import get_settings
from opencheck.sources.sec_edgar import (
    SecEdgarAdapter,
    _edgar_title_key,
    _latest_per_reporter,
    _normalise_company_name,
    _strip_edgar_title_tag,
)


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("MOODYS CORP /DE/", "MOODYS CORP"),
        ("ICU MEDICAL INC/DE", "ICU MEDICAL INC"),
        ("COSTCO WHOLESALE CORP /NEW", "COSTCO WHOLESALE CORP"),
        ("FNB CORP/PA/", "FNB CORP"),
        ("Gores Holdings X, Inc. / CI", "Gores Holdings X, Inc."),
        ("Circle Energy, Inc./NV", "Circle Energy, Inc."),
        ("Alight, Inc. / Delaware", "Alight, Inc."),
        ("Spirax-Sarco Engineering PLC/ADR", "Spirax-Sarco Engineering PLC"),
        ("NATIONAL RURAL UTILITIES COOPERATIVE FINANCE CORP /DC/",
         "NATIONAL RURAL UTILITIES COOPERATIVE FINANCE CORP"),
        # A slash that is part of the name stays.
        ("Cadeler A/S", "Cadeler A/S"),
        ("DATA I/O CORP", "DATA I/O CORP"),
        ("Apple Inc.", "Apple Inc."),
    ],
)
def test_strip_edgar_title_tag(title: str, expected: str) -> None:
    assert _strip_edgar_title_tag(title) == expected


@pytest.mark.parametrize(
    ("gleif_name", "edgar_title"),
    [
        ("MOODY'S CORPORATION", "MOODYS CORP /DE/"),
        ("McDonald's Corporation", "MCDONALDS CORP"),
        ("Kohl's Corporation", "KOHLS Corp"),
        ("Lowe's Companies, Inc.", "LOWES COMPANIES INC"),
        ("MACY'S, INC.", "Macy's, Inc."),
        ("Hormel Foods Corporation", "HORMEL FOODS CORP /DE/"),
        ("Waters Corporation", "WATERS CORP /DE/"),
        ("Costco Wholesale Corporation", "COSTCO WHOLESALE CORP /NEW"),
        ("VeriSign, Inc.", "VERISIGN INC/CA"),
        # Curly apostrophe on the GLEIF side.
        ("MOODY’S CORPORATION", "MOODYS CORP /DE/"),
    ],
)
def test_gleif_name_meets_edgar_title(gleif_name: str, edgar_title: str) -> None:
    assert _normalise_company_name(gleif_name) == _edgar_title_key(edgar_title)


def test_gleif_names_are_never_tag_stripped() -> None:
    # The tag rule is for EDGAR's conformed names only; a GLEIF name keeps
    # every token it has.
    assert _normalise_company_name("NOVO NORDISK A/S") == "NOVO NORDISK A S"


def _adapter(monkeypatch, tmp_path) -> SecEdgarAdapter:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    return SecEdgarAdapter()


def _client(*texts: str) -> AsyncMock:
    responses = []
    for text in texts:
        resp = MagicMock(status_code=200, text=text)
        resp.raise_for_status = MagicMock()
        responses.append(resp)
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.get = AsyncMock(side_effect=responses)
    return client


def _tickers(*rows: tuple[int, str, str]) -> str:
    return json.dumps(
        {str(i): {"cik_str": cik, "ticker": tk, "title": title} for i, (cik, tk, title) in enumerate(rows)}
    )


_TICKERS = _tickers(
    (1059556, "MCO", "MOODYS CORP /DE/"),
    (63908, "MCD", "MCDONALDS CORP"),
    (48465, "HRL", "HORMEL FOODS CORP /DE/"),
    # Three issuers share one key once their tags go.
    (765207, "FNLC", "First Bancorp, Inc /ME/"),
    (811589, "FBNC", "FIRST BANCORP /NC/"),
    (1057706, "FBP", "FIRST BANCORP /PR/"),
    # One issuer, two share classes: one CIK, not ambiguous.
    (1652044, "GOOGL", "Alphabet Inc."),
    (1652044, "GOOG", "Alphabet Inc."),
)


@pytest.mark.asyncio
async def test_resolve_cik_moodys_and_friends(monkeypatch, tmp_path) -> None:
    adapter = _adapter(monkeypatch, tmp_path)
    with patch("opencheck.sources.sec_edgar.build_client", return_value=_client(_TICKERS)):
        assert await adapter.resolve_cik("MOODY'S CORPORATION") == "1059556"
        assert await adapter.resolve_cik("McDonald's Corporation") == "63908"
        assert await adapter.resolve_cik("Hormel Foods Corporation") == "48465"
        assert await adapter.resolve_cik("Alphabet Inc.") == "1652044"
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_a_key_two_issuers_share_resolves_to_neither(monkeypatch, tmp_path) -> None:
    """Until Phase 254 the first title in the file won — a lookup of one
    First Bancorp could be handed another's filings."""
    adapter = _adapter(monkeypatch, tmp_path)
    # The company-search fallback lists the same three, which must not
    # break the tie either.
    atom = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        '<entry><title>First Bancorp, Inc /ME/</title><id>urn:tag:sec.gov,2008:company=0000765207</id></entry>'
        '<entry><title>FIRST BANCORP /NC/</title><id>urn:tag:sec.gov,2008:company=0000811589</id></entry>'
        '<entry><title>FIRST BANCORP /PR/</title><id>urn:tag:sec.gov,2008:company=0001057706</id></entry>'
        "</feed>"
    )
    with patch("opencheck.sources.sec_edgar.build_client", return_value=_client(_TICKERS, atom)):
        assert await adapter.resolve_cik("First Bancorp") is None
    index = adapter._ticker_index or {}
    assert "FIRST BANCORP" not in index
    assert index["MOODYS"] == "1059556"
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fallback_accepts_a_single_tagged_match(monkeypatch, tmp_path) -> None:
    adapter = _adapter(monkeypatch, tmp_path)
    atom = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        '<entry><title>GLOBEX HOLDINGS CORP /NV/</title><id>urn:tag:sec.gov,2008:company=0001234567</id></entry>'
        '<entry><title>GLOBEX HOLDINGS PARTNERS LP</title><id>urn:tag:sec.gov,2008:company=0000926480</id></entry>'
        "</feed>"
    )
    with patch("opencheck.sources.sec_edgar.build_client", return_value=_client(_TICKERS, atom)):
        assert await adapter.resolve_cik("Globex Holdings Corporation") == "1234567"
    get_settings.cache_clear()


def test_an_amendment_spelled_with_a_full_stop_replaces_the_original() -> None:
    # McDonald's (CIK 63908), as EDGAR served it on 28 Sept 2026: one filer
    # (CIK 19617), a 13G at 5.1% and a later 13G/A at 4.3%, the name spelled
    # with and without the trailing full stop. Only the amendment stands.
    original = {
        "reporter": {"name": "JPMORGAN CHASE & CO", "percent_of_class": 5.1},
        "filer_cik": "19617",
        "filed": "2026-05-13",
    }
    amendment = {
        "reporter": {"name": "JPMORGAN CHASE & CO.", "percent_of_class": 4.3},
        "filer_cik": "19617",
        "filed": "2026-07-22",
    }
    assert _latest_per_reporter([amendment, original]) == [amendment]
    assert _latest_per_reporter([original, amendment]) == [amendment]
