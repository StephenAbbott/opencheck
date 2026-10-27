"""Tests for the KvK (Netherlands Chamber of Commerce) open-data adapter.

All HTTP calls are mocked via respx so no network access is needed.

The key behaviour under test is graceful handling of HTTP 404 from the KvK
``basisbedrijfsgegevens`` High-Value Dataset. That dataset only covers
companies registered as a BV or NV with registered business activity; every
other KvK number answers 404. The adapter must degrade to a coverage note
rather than surfacing the raw ``HTTPStatusError`` as a broken source card.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from httpx import Response

from opencheck.sources.kvk import KVK_RA_CODE, KvKAdapter, normalise_kvk
from opencheck.routers.lookup import _bh_kvk, _LookupCtx


_URL = "https://opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer"


@pytest.fixture
def adapter():
    return KvKAdapter()


def test_ra_code():
    assert KVK_RA_CODE == "RA000463"


def test_normalise_kvk_zero_pads():
    assert normalise_kvk("33011433") == "33011433"
    assert normalise_kvk(" 215011 ") == "00215011"


def test_requires_no_api_key(adapter):
    assert not adapter.info.requires_api_key


@pytest.mark.asyncio
async def test_fetch_returns_company_on_200(adapter, monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    with respx.mock:
        respx.get(f"{_URL}/17085815").mock(
            return_value=Response(
                200,
                json={
                    "kvkNummer": "17085815",
                    "rechtsvorm": "NV",
                    "activiteiten": [{"sbiCode": "7010", "soortActiviteit": "Hoofdactiviteit"}],
                },
            )
        )
        bundle = await adapter.fetch("17085815", legal_name="ASML Holding N.V.")

    assert bundle["company"] is not None
    assert bundle["company"]["rechtsvorm"] == "NV"
    assert "coverage_note" not in bundle
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fetch_404_degrades_to_coverage_note(adapter, monkeypatch, tmp_path):
    """A 404 (not in the BV/NV-only open set) must not raise — it returns a
    company-less bundle carrying a coverage note."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    with respx.mock:
        respx.get(f"{_URL}/33011433").mock(
            return_value=Response(404, json={"detail": "not found"})
        )
        bundle = await adapter.fetch("33011433", legal_name="Heineken N.V.")

    assert bundle["company"] is None
    assert not bundle["is_stub"]
    assert "coverage_note" in bundle
    assert "open-data set" in bundle["coverage_note"]
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_hit_builder_surfaces_coverage_note(adapter, monkeypatch, tmp_path):
    """The coverage note must reach the SourceHit raw so the frontend can show
    it instead of an empty/error card."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    with respx.mock:
        respx.get(f"{_URL}/33011433").mock(return_value=Response(404))
        bundle = await adapter.fetch("33011433", legal_name="Heineken N.V.")

    ctx = _LookupCtx(lei="724500K5PTPSST86UQ23", legal_name="Heineken N.V.")
    hit = _bh_kvk(bundle, "33011433", ctx)
    assert hit.name == "Heineken N.V."
    assert "coverage_note" in hit.raw
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Phase 324: an unanswered request degrades instead of raising
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "exc", "reason", "detail"),
    [
        (Response(429), None, "rate_limited", "HTTP 429"),
        (Response(503), None, "upstream_error", "HTTP 503"),
        (None, httpx.ReadTimeout("timed out"), "timeout", "ReadTimeout"),
        (None, httpx.ConnectError("refused"), "upstream_error", "ConnectError"),
    ],
)
async def test_an_unanswered_request_degrades_instead_of_raising(
    adapter, monkeypatch, tmp_path, response, exc, reason, detail
):
    # The BODS quality sweep of 12 Oct 2026: KvK's 429 outlasted the three
    # retries, raise_for_status() sent it out of fetch(), and /deepen answered
    # HTTP 500 for ASML Holding.
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck import degradation
    from opencheck.config import get_settings

    get_settings.cache_clear()
    puts: list[str] = []
    monkeypatch.setattr(adapter._cache, "put", lambda key, *a, **k: puts.append(key))
    with respx.mock:
        route = respx.get(f"{_URL}/17085815")
        if exc is not None:
            route.mock(side_effect=exc)
        else:
            route.mock(return_value=response)
        with degradation.recording() as recorded:
            bundle = await adapter.fetch("17085815", legal_name="ASML Holding N.V.")

    assert bundle["company"] is None
    assert bundle["is_stub"] is True
    assert "coverage_note" not in bundle  # not the 404 "not in the open set" note
    assert puts == []  # a failure is never cached as "no record"
    [entry] = [d for d in recorded if d.source_id == "kvk"]
    assert entry.reason == reason
    assert detail in entry.detail
    assert "17085815" not in entry.detail
    if response is not None and response.status_code == 429:
        # Sent once: the adapter no longer retries a refusal itself.
        assert route.call_count == 1
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# 27 Oct 2026: request pacing — one request a minute, through build_client
# ---------------------------------------------------------------------------


@pytest.fixture
def paced(monkeypatch, tmp_path):
    """Live mode with the real KvK throttle on a fake clock (20 s wait)."""
    from opencheck import host_throttle as ht
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_KVK_RATE_LIMIT_PER_MINUTE", "1")
    monkeypatch.setenv("OPENCHECK_KVK_THROTTLE_MAX_WAIT_S", "20")
    get_settings.cache_clear()
    ht.reset_host_throttles_for_tests()

    class Clock:
        now = 5000.0

        def __call__(self) -> float:
            return self.now

        async def sleep(self, s: float) -> None:
            self.now += s

    clock = Clock()
    ht._throttles[ht.KVK_OPENDATA_HOST] = ht.HostThrottle(
        ht.KVK_OPENDATA_HOST, clock=clock, sleep=clock.sleep
    )
    yield clock
    ht.reset_host_throttles_for_tests()
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_concurrent_fetches_of_one_number_send_one_request(adapter, paced):
    # A lookup and the /deepen behind it used to send the same request twice
    # inside one minute; the second was a guaranteed 429. Now the second waits
    # for the first and is served from the cache it fills.
    import asyncio

    with respx.mock:
        route = respx.get(f"{_URL}/17085815").mock(
            return_value=Response(200, json={"actief": "J", "rechtsvormCode": "NV"})
        )
        first, second = await asyncio.gather(
            adapter.fetch("17085815", legal_name="ASML Holding N.V."),
            adapter.fetch("17085815"),
        )
    assert route.call_count == 1
    assert first["company"] == second["company"] == {"actief": "J", "rechtsvormCode": "NV"}
    assert second["legal_name"] == "ASML Holding N.V."


@pytest.mark.asyncio
async def test_a_second_number_inside_the_minute_degrades_without_sending(adapter, paced):
    # The production pattern of 27 Oct 2026 (ASML, then Heineken two minutes
    # later): with the minute spent and no slot inside the 20 s wait, the
    # second request is never sent — so it cannot extend the refusal — and the
    # card says why, honestly.
    from opencheck import degradation

    with respx.mock:
        asml = respx.get(f"{_URL}/17085815").mock(return_value=Response(200, json={"actief": "J"}))
        heineken = respx.get(f"{_URL}/33011433").mock(
            return_value=Response(200, json={"actief": "J"})
        )
        await adapter.fetch("17085815", legal_name="ASML Holding N.V.")
        paced.now += 10  # ten seconds later
        with degradation.recording() as recorded:
            bundle = await adapter.fetch("33011433", legal_name="Heineken N.V.")
    assert asml.call_count == 1
    assert heineken.call_count == 0
    assert bundle["is_stub"] is True and bundle["company"] is None
    [entry] = [d for d in recorded if d.source_id == "kvk"]
    assert entry.reason == "rate_limited"
    assert "not sent" in entry.detail


@pytest.mark.asyncio
async def test_a_second_number_within_the_wait_is_answered(adapter, paced):
    # 45 s after the last request the next slot is 15 s away — inside the
    # 20 s wait — so the lookup queues and gets the real record.
    with respx.mock:
        respx.get(f"{_URL}/17085815").mock(return_value=Response(200, json={"actief": "J"}))
        heineken = respx.get(f"{_URL}/33011433").mock(
            return_value=Response(200, json={"actief": "J", "rechtsvormCode": "NV"})
        )
        await adapter.fetch("17085815")
        paced.now += 45
        bundle = await adapter.fetch("33011433", legal_name="Heineken N.V.")
    assert heineken.call_count == 1
    assert bundle["is_stub"] is False
    assert bundle["company"] == {"actief": "J", "rechtsvormCode": "NV"}
