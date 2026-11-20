"""A register that does not answer is never read as "no record".

The codebase review of 20 Nov 2026 found three adapters that returned an
empty stub on a 5xx or a timeout without recording a degradation, so the
pipeline counted the outage as "Answered with no record" (the class PR 653
fixed for Latvia): KRS (Poland), ABR (Australia) and Firmenbuch (Austria).
Firmenbuch also put ``str(exc)`` into the bundle. Each adapter now records a
``source_fetch`` degradation and returns a stub, and a genuine miss still
records nothing.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from httpx import Response

from opencheck import degradation
from opencheck.config import get_settings


@pytest.fixture
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("ABN_GUID", "test-guid-1234")
    monkeypatch.setenv("FIRMENBUCH_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _fetch_failures(records):
    return [d for d in records if d.check == degradation.CHECK_SOURCE_FETCH]


# --- KRS -------------------------------------------------------------------

_KRS = "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/"


@pytest.mark.parametrize(
    "answer",
    [Response(503), httpx.ReadTimeout("slow")],
    ids=["http-503", "timeout"],
)
async def test_krs_failure_is_a_degradation(live, answer) -> None:
    from opencheck.sources.krs_poland import KrsPolandAdapter

    with respx.mock, degradation.recording() as records:
        route = respx.get(url__startswith=_KRS)
        if isinstance(answer, Exception):
            route.mock(side_effect=answer)
        else:
            route.mock(return_value=answer)
        bundle = await KrsPolandAdapter().fetch("0000028860")
    assert bundle["is_stub"] is True
    (failure,) = _fetch_failures(records)
    assert failure.source_id == "krs_poland"
    expected = degradation.REASON_TIMEOUT if isinstance(answer, Exception) else degradation.REASON_UPSTREAM_ERROR
    assert failure.reason == expected


async def test_krs_404_on_every_register_is_no_record(live) -> None:
    from opencheck.sources.krs_poland import KrsPolandAdapter

    with respx.mock, degradation.recording() as records:
        respx.get(url__startswith=_KRS).mock(return_value=Response(404))
        bundle = await KrsPolandAdapter().fetch("0000028860")
    assert bundle["is_stub"] is True
    assert _fetch_failures(records) == []


# --- ABR -------------------------------------------------------------------

_ABR = "https://abr.business.gov.au/json/AbnDetails.aspx"


@pytest.mark.parametrize(
    "answer",
    [Response(502), httpx.ConnectError("refused")],
    ids=["http-502", "network"],
)
async def test_abr_failure_is_a_degradation(live, answer) -> None:
    from opencheck.sources.abr_australia import AbrAustraliaAdapter

    with respx.mock, degradation.recording() as records:
        route = respx.get(url__startswith=_ABR)
        if isinstance(answer, Exception):
            route.mock(side_effect=answer)
        else:
            route.mock(return_value=answer)
        bundle = await AbrAustraliaAdapter().fetch("31976733718", legal_name="X")
    assert bundle["is_stub"] is True
    (failure,) = _fetch_failures(records)
    assert failure.source_id == "abr_australia"


# --- Firmenbuch --------------------------------------------------------------

_FB = "https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws"


async def test_firmenbuch_failure_is_a_stub_and_carries_no_exception_text(live) -> None:
    from opencheck.sources.firmenbuch import FirmenbuchAdapter

    with respx.mock, degradation.recording() as records:
        respx.post(url__startswith=_FB).mock(return_value=Response(503, text="down"))
        bundle = await FirmenbuchAdapter().fetch("473888w", legal_name="X")
    assert bundle["is_stub"] is True
    assert bundle["extract"] is None
    # A short label only: httpx's message carries the request URL.
    assert bundle["soap_error"] == "HTTP 503"
    (failure,) = _fetch_failures(records)
    assert failure.source_id == "firmenbuch"
