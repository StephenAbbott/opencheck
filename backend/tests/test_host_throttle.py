"""Per-host request pacing (opencheck/host_throttle.py) — KvK open data.

``opendata.kvk.nl`` allows one request per minute per IP and answers a spent
quota with a bare 429 (no Retry-After). Until 27 Oct 2026 nothing paced it and
the adapter's own retries gave up after ~14 s of a 60 s window, so two of three
Dutch company cards checked in production that morning rendered placeholder KvK
data. These tests pin the pacing with a fake clock: a minute-long window runs
in no time.
"""

from __future__ import annotations

import httpx
import pytest

from opencheck import host_throttle as ht
from opencheck.config import get_settings

_KVK = "https://opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer/17085815"


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.slept = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        self.slept += seconds


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    monkeypatch.setenv("OPENCHECK_KVK_RATE_LIMIT_PER_MINUTE", "1")
    monkeypatch.setenv("OPENCHECK_KVK_THROTTLE_MAX_WAIT_S", "20")
    get_settings.cache_clear()
    ht.reset_host_throttles_for_tests()
    fake = FakeClock()
    ht._throttles[ht.KVK_OPENDATA_HOST] = ht.HostThrottle(
        ht.KVK_OPENDATA_HOST, clock=fake, sleep=fake.sleep
    )
    yield fake
    ht.reset_host_throttles_for_tests()
    get_settings.cache_clear()


def _client(statuses: list[int], sent: list[float], clock: FakeClock) -> httpx.AsyncClient:
    """A client whose upstream answers ``statuses`` in turn, logging send times."""
    answers = iter(statuses)

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(clock.now)
        return httpx.Response(next(answers), json={"actief": "J"})

    return httpx.AsyncClient(transport=ht.HostThrottledTransport(httpx.MockTransport(handler)))


@pytest.mark.asyncio
async def test_second_request_waits_for_the_window_when_the_wait_allows(clock, monkeypatch):
    monkeypatch.setenv("OPENCHECK_KVK_THROTTLE_MAX_WAIT_S", "90")
    get_settings.cache_clear()
    sent: list[float] = []
    async with _client([200, 200], sent, clock) as client:
        assert (await client.get(_KVK)).status_code == 200
        assert (await client.get(_KVK)).status_code == 200
    assert len(sent) == 2
    assert sent[1] - sent[0] >= 60.0  # one a minute, not two in a burst


@pytest.mark.asyncio
async def test_a_request_with_no_slot_in_time_is_refused_without_being_sent(clock):
    # Interactive wait (20 s): the minute is spent, so the second request
    # would only be refused. It is not sent at all.
    sent: list[float] = []
    async with _client([200], sent, clock) as client:
        await client.get(_KVK)
        with pytest.raises(ht.HostRateLimitedError) as exc:
            await client.get(_KVK)
    assert len(sent) == 1
    assert exc.value.host == "opendata.kvk.nl"
    assert "not sent" in str(exc.value)
    assert clock.slept == 0.0  # refused at once: the slot was known to be too far


@pytest.mark.asyncio
async def test_a_bare_429_penalises_one_full_window(clock):
    # KvK's 429 carries no Retry-After: the honest guess is a whole minute.
    sent: list[float] = []
    async with _client([429], sent, clock) as client:
        response = await client.get(_KVK)
    assert response.status_code == 429  # handed back: 60 s is past the 20 s wait
    assert len(sent) == 1
    throttle = ht.get_host_throttle(ht.KVK_OPENDATA_HOST)
    assert throttle is not None
    assert throttle.next_slot_at() == pytest.approx(clock.now + 60.0)


@pytest.mark.asyncio
async def test_a_batch_wait_queues_through_a_refused_minute(clock, monkeypatch):
    # The sweep's setting (130 s): a shared runner IP whose minute was already
    # spent gets one 429, waits the window out, and is answered.
    monkeypatch.setenv("OPENCHECK_KVK_THROTTLE_MAX_WAIT_S", "130")
    get_settings.cache_clear()
    sent: list[float] = []
    async with _client([429, 200], sent, clock) as client:
        response = await client.get(_KVK)
    assert response.status_code == 200
    assert len(sent) == 2
    assert sent[1] - sent[0] >= 60.0


@pytest.mark.asyncio
async def test_retries_stop_at_the_deadline(clock, monkeypatch):
    monkeypatch.setenv("OPENCHECK_KVK_THROTTLE_MAX_WAIT_S", "130")
    get_settings.cache_clear()
    sent: list[float] = []
    async with _client([429, 429, 429, 429], sent, clock) as client:
        response = await client.get(_KVK)
    assert response.status_code == 429
    assert len(sent) == 3  # at 0, 60 and 120 s; a fourth (180 s) is past 130 s
    assert sent[-1] - sent[0] <= 130.0


@pytest.mark.asyncio
async def test_retry_after_is_honoured_when_sent(clock, monkeypatch):
    monkeypatch.setenv("OPENCHECK_KVK_THROTTLE_MAX_WAIT_S", "90")
    get_settings.cache_clear()
    sent: list[float] = []
    answers = iter([httpx.Response(429, headers={"Retry-After": "75"}), httpx.Response(200)])

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(clock.now)
        return next(answers)

    async with httpx.AsyncClient(
        transport=ht.HostThrottledTransport(httpx.MockTransport(handler))
    ) as client:
        assert (await client.get(_KVK)).status_code == 200
    assert sent[1] - sent[0] >= 75.0


@pytest.mark.asyncio
async def test_other_hosts_are_untouched(clock):
    sent: list[float] = []
    async with _client([200, 200, 200], sent, clock) as client:
        for _ in range(3):
            assert (await client.get("https://api.example.org/x")).status_code == 200
    assert len(sent) == 3
    assert clock.slept == 0.0
    assert ht.get_host_throttle("api.example.org") is None


@pytest.mark.asyncio
async def test_a_zero_limit_disables_the_throttle(clock, monkeypatch):
    monkeypatch.setenv("OPENCHECK_KVK_RATE_LIMIT_PER_MINUTE", "0")
    get_settings.cache_clear()
    sent: list[float] = []
    async with _client([200, 200], sent, clock) as client:
        await client.get(_KVK)
        await client.get(_KVK)
    assert len(sent) == 2
    assert clock.slept == 0.0


def test_build_client_installs_the_host_throttle():
    from opencheck.http import build_client

    client = build_client()
    transport = client._transport
    found = False
    while transport is not None:
        if isinstance(transport, ht.HostThrottledTransport):
            found = True
            break
        transport = getattr(transport, "_inner", None)
    assert found
