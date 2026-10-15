"""``http.request_with_retry`` — the 429/5xx loop KvK and SEC EDGAR share (issue #303).

The helper replaced two pasted copies; these tests pin the behaviour both
copies had, so the adapters' own tests (test_bods_kvk, test_sec_edgar_resolve)
only need to check the wiring.
"""

from __future__ import annotations

import httpx
import pytest

from opencheck import http as oc_http
from opencheck.sources import kvk, sec_edgar


def _sender(*statuses_and_headers):
    """A zero-argument send that answers each call with the next response."""
    responses = [
        httpx.Response(s, headers=h) if isinstance(h, dict) else httpx.Response(s)
        for s, h in statuses_and_headers
    ]
    calls = {"n": 0}

    async def send() -> httpx.Response:
        r = responses[calls["n"]]
        calls["n"] += 1
        return r

    return send, calls


@pytest.fixture
def waits(monkeypatch):
    seen: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        seen.append(seconds)

    monkeypatch.setattr("opencheck.http.asyncio.sleep", fake_sleep)
    return seen


async def test_a_non_retryable_answer_returns_at_once(waits):
    send, calls = _sender((200, None))
    r = await oc_http.request_with_retry(
        send, max_retries=3, backoff_base=2.0, backoff_max=30.0
    )
    assert r.status_code == 200
    assert calls["n"] == 1
    assert waits == []


async def test_backoff_doubles_and_is_capped(waits):
    send, calls = _sender((429, None), (429, None), (429, None), (200, None))
    r = await oc_http.request_with_retry(
        send, max_retries=3, backoff_base=2.0, backoff_max=5.0
    )
    assert r.status_code == 200
    assert calls["n"] == 4
    assert waits == [2.0, 4.0, 5.0]


async def test_retry_after_is_honoured_and_capped(waits):
    send, _ = _sender(
        (429, {"Retry-After": "3"}), (429, {"Retry-After": "120"}), (200, None)
    )
    await oc_http.request_with_retry(
        send, max_retries=3, backoff_base=2.0, backoff_max=30.0
    )
    assert waits == [3.0, 30.0]


async def test_an_unparseable_retry_after_waits_the_current_backoff(waits):
    # An HTTP-date Retry-After is not parsed — the backoff applies, uncapped,
    # exactly as the two loops this helper replaced behaved.
    send, _ = _sender(
        (429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}), (200, None)
    )
    await oc_http.request_with_retry(
        send, max_retries=3, backoff_base=2.0, backoff_max=1.0
    )
    assert waits == [2.0]


async def test_exhausted_retries_return_the_last_retryable_response(waits):
    send, calls = _sender((429, None), (429, None), (429, None))
    r = await oc_http.request_with_retry(
        send, max_retries=2, backoff_base=1.0, backoff_max=8.0
    )
    assert r.status_code == 429
    assert calls["n"] == 3  # the first try plus two retries
    assert waits == [1.0, 2.0]  # no wait after the last attempt


async def test_only_statuses_in_retry_on_are_retried(waits):
    # KvK's default set is 429 alone: a 503 comes straight back.
    send, calls = _sender((503, None), (200, None))
    r = await oc_http.request_with_retry(
        send, max_retries=3, backoff_base=2.0, backoff_max=30.0
    )
    assert r.status_code == 503
    assert calls["n"] == 1
    # EDGAR's set includes 5xx.
    send, calls = _sender((503, None), (200, None))
    r = await oc_http.request_with_retry(
        send,
        max_retries=2,
        backoff_base=1.0,
        backoff_max=8.0,
        retry_on=sec_edgar._RETRYABLE_STATUS_CODES,
    )
    assert r.status_code == 200
    assert calls["n"] == 2


def test_the_adapters_keep_their_own_settings():
    # The settings are each adapter's; only the loop is shared.
    assert (kvk._MAX_RETRIES, kvk._RETRY_BACKOFF_BASE, kvk._RETRY_BACKOFF_MAX) == (
        3,
        2.0,
        30.0,
    )
    assert (
        sec_edgar._MAX_RETRIES,
        sec_edgar._RETRY_BACKOFF_BASE,
        sec_edgar._RETRY_BACKOFF_MAX,
    ) == (2, 1.0, 8.0)
    assert frozenset({429, 500, 502, 503, 504}) == sec_edgar._RETRYABLE_STATUS_CODES


def test_no_adapter_pastes_its_own_retry_loop():
    import inspect

    for module in (kvk, sec_edgar):
        source = inspect.getsource(module)
        assert "asyncio.sleep" not in source, module.__name__
        assert "request_with_retry" in source, module.__name__
