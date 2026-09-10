"""The Companies House filing fetch reads the whole history, or says it did not.

Phase 194. The cap was 10 pages of 100 — and Companies House answers
filing-history newest first, so a company with more than 1,000 filings had its
*oldest* end silently removed. Lloyds Bank PLC has 2,404. That is the same
defect Phase 192 fixed on the officers list, in the tab the board-turnover
work renders.
"""

from __future__ import annotations

import httpx
import pytest
from pytest_httpx import HTTPXMock

from opencheck.timeline import service

_API = "https://api.company-information.service.gov.uk"


def _page(start: int, size: int, total: int) -> dict:
    count = max(0, min(size, total - start))
    return {
        "total_count": total,
        "items": [
            {"category": "officers", "type": "AP01", "date": "2020-01-01"}
            for _ in range(count)
        ],
    }


def _mock_history(httpx_mock: HTTPXMock, total: int, *, pages: int) -> None:
    for page in range(pages):
        start = page * service._CH_PAGE_SIZE
        httpx_mock.add_response(
            url=(
                f"{_API}/company/00002065/filing-history"
                f"?items_per_page={service._CH_PAGE_SIZE}&start_index={start}"
            ),
            json=_page(start, service._CH_PAGE_SIZE, total),
        )


async def test_a_long_history_is_read_past_the_old_thousand_filing_cap(
    httpx_mock: HTTPXMock,
) -> None:
    """Lloyds Bank PLC's own number: 2,404 filings, which the old cap cut to
    1,000 without saying so."""
    _mock_history(httpx_mock, 2404, pages=25)

    async with httpx.AsyncClient() as client:
        filings, truncated = await service._ch_filings(client, "00002065", "key")

    assert len(filings) == 2404
    assert truncated is False


async def test_a_history_longer_than_the_cap_says_so(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bound still exists; what changed is that hitting it is reported.
    An unmarked short history is indistinguishable from a short company."""
    monkeypatch.setattr(service, "_CH_PAGE_CAP", 3)
    _mock_history(httpx_mock, 10_000, pages=3)

    async with httpx.AsyncClient() as client:
        filings, truncated = await service._ch_filings(client, "00002065", "key")

    assert len(filings) == 300
    assert truncated is True


async def test_a_short_history_costs_one_call(httpx_mock: HTTPXMock) -> None:
    _mock_history(httpx_mock, 12, pages=1)

    async with httpx.AsyncClient() as client:
        filings, truncated = await service._ch_filings(client, "00002065", "key")

    assert len(filings) == 12
    assert truncated is False
    assert len(httpx_mock.get_requests()) == 1


# ---------------------------------------------------------------------------
# Phase 198 — the officers list behind the board stream, same defect class
# ---------------------------------------------------------------------------

def _officers_page(start: int, size: int, total: int) -> dict:
    count = max(0, min(size, total - start))
    return {
        "total_results": total,
        "active_count": 16,
        "resigned_count": total - 16,
        "items": [
            {
                "name": f"OFFICER, Number {start + i}",
                "officer_role": "director",
                "appointed_on": "2000-01-01",
                "links": {
                    "officer": {"appointments": f"/officers/o{start + i}/appointments"}
                },
            }
            for i in range(count)
        ],
    }


def _mock_officers(httpx_mock: HTTPXMock, total: int, *, pages: int) -> None:
    for page in range(pages):
        start = page * service._CH_PAGE_SIZE
        httpx_mock.add_response(
            url=(
                f"{_API}/company/00002065/officers"
                f"?items_per_page={service._CH_PAGE_SIZE}"
                f"&start_index={start}&register_view=false"
            ),
            json=_officers_page(start, service._CH_PAGE_SIZE, total),
        )


async def test_the_board_stream_reads_every_officer_not_the_default_page(
    httpx_mock: HTTPXMock,
) -> None:
    """The register's default page is 35, and a board *history* is mostly
    people who have left — Lloyds Bank PLC lists 116 once the resigned ones
    are asked for. Phase 192 fixed this on the lookup adapter's copy of the
    call; this is the timeline's own."""
    _mock_officers(httpx_mock, 232, pages=3)

    async with httpx.AsyncClient() as client:
        payload = await service._ch_officers(client, "00002065", "key")

    assert payload is not None
    assert len(payload["items"]) == 232


async def test_resigned_officers_are_asked_for_explicitly(
    httpx_mock: HTTPXMock,
) -> None:
    """``register_view=false`` is the difference between a board history and
    a snapshot of today's board — the default omits everyone who left."""
    _mock_officers(httpx_mock, 12, pages=1)

    async with httpx.AsyncClient() as client:
        await service._ch_officers(client, "00002065", "key")

    assert "register_view=false" in str(httpx_mock.get_requests()[0].url)


async def test_a_register_refusal_is_none_not_an_empty_board(
    httpx_mock: HTTPXMock,
) -> None:
    """"No officers" and "did not read the officers" are different answers,
    and an empty list says both. The endpoint's ``officers_available`` is
    built on this distinction."""
    httpx_mock.add_response(
        url=(
            f"{_API}/company/00002065/officers"
            f"?items_per_page={service._CH_PAGE_SIZE}"
            f"&start_index=0&register_view=false"
        ),
        status_code=404,
    )

    async with httpx.AsyncClient() as client:
        assert await service._ch_officers(client, "00002065", "key") is None
