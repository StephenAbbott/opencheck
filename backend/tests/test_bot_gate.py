"""Phase 144 — the /lookup-stream bot gate.

``/lookup-stream`` exists for the interactive app and has always been
disallowed in robots.txt; a crawler running it anyway triggers the full
adapter fan-out and drains the shared upstream budgets human lookups queue
behind (measured 2026-08-29: ~19 bot lookup-streams/min while human cold
anchors stalled 15–21s at the GLEIF throttle). Declared bots — the
``memwatch.is_bot`` User-Agent classifier, the same one /memstats counts
with — get a 403 that points them at the crawlable ``/entity`` pages and the
plain ``/lookup`` JSON API instead.

The gate deliberately does NOT cover ``/lookup``: that is the promoted
programmatic API, and ``python``/``curl``/``httpx`` User-Agents are its
legitimate callers.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings

_LEI = "ZZZZ00000000000000ZZ"  # shape-valid, unknown — offline lookups 404

_BOT_UAS = [
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "GPTBot/1.0 (+https://openai.com/gptbot)",
    "python-httpx/0.27.0",
    "",  # empty UA counts as a bot — real browsers always send one
]

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    """Offline, tmp data root — the gate must fire before any pipeline work."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENCHECK_BOT_GATE_LOOKUP_STREAM", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("ua", _BOT_UAS, ids=["googlebot", "gptbot", "httpx", "empty"])
def test_bot_user_agents_are_refused_on_lookup_stream(
    client: TestClient, ua: str
) -> None:
    r = client.get(
        "/lookup-stream", params={"lei": _LEI}, headers={"User-Agent": ua}
    )
    assert r.status_code == 403
    detail = r.json()["detail"]
    # The refusal must hand the caller its two legitimate routes.
    assert "/entity/" in detail
    assert "/lookup?lei=" in detail
    assert "robots" in detail


def test_browser_user_agent_passes_the_gate(client: TestClient) -> None:
    """A real browser UA reaches the pipeline (offline → SSE with an error
    event, but crucially NOT a 403)."""
    r = client.get(
        "/lookup-stream", params={"lei": _LEI}, headers={"User-Agent": _BROWSER_UA}
    )
    assert r.status_code != 403


def test_gate_can_be_disabled_by_env(client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_BOT_GATE_LOOKUP_STREAM", "0")
    get_settings.cache_clear()
    r = client.get(
        "/lookup-stream",
        params={"lei": _LEI},
        headers={"User-Agent": _BOT_UAS[0]},
    )
    assert r.status_code != 403


def test_lookup_json_api_is_never_gated(client: TestClient) -> None:
    """``/lookup`` is the programmatic API — a python UA is a legitimate
    caller and must reach the pipeline (offline unknown LEI → 404, not 403)."""
    r = client.get(
        "/lookup",
        params={"lei": _LEI},
        headers={"User-Agent": "python-httpx/0.27.0"},
    )
    assert r.status_code == 404
    assert "No GLEIF record" in r.json()["detail"]


# --- The QA door (bot_gate.py): OpenCheck's own test agents ---------------

_HEADLESS_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) HeadlessChrome/131.0.0.0 Safari/537.36"
)
_TOKEN = "qa-" + "x" * 40


def _with_token(monkeypatch, token: str) -> None:
    monkeypatch.setenv("OPENCHECK_QA_BYPASS_TOKEN", token)
    get_settings.cache_clear()


def test_headless_chrome_is_refused_without_the_token(client, monkeypatch) -> None:
    _with_token(monkeypatch, _TOKEN)
    r = client.get(
        "/lookup-stream", params={"lei": _LEI}, headers={"User-Agent": _HEADLESS_UA}
    )
    assert r.status_code == 403


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/lookup-stream", {"lei": _LEI}),
        ("/lookup-register-stream", {"scheme": "GB-COH", "id": "00000000"}),
    ],
    ids=["lookup-stream", "lookup-register-stream"],
)
def test_the_qa_token_passes_the_gate(client, monkeypatch, path, params) -> None:
    _with_token(monkeypatch, _TOKEN)
    r = client.get(
        path,
        params=params,
        headers={"User-Agent": _HEADLESS_UA, "X-OpenCheck-QA": _TOKEN},
    )
    assert r.status_code != 403


def test_a_wrong_token_is_refused(client, monkeypatch) -> None:
    _with_token(monkeypatch, _TOKEN)
    r = client.get(
        "/lookup-stream",
        params={"lei": _LEI},
        headers={"User-Agent": _HEADLESS_UA, "X-OpenCheck-QA": _TOKEN + "y"},
    )
    assert r.status_code == 403


@pytest.mark.parametrize("token", ["", "short-token"], ids=["unset", "too-short"])
def test_no_door_without_a_long_token(client, monkeypatch, token) -> None:
    """Unset or guessable: the header opens nothing, not even when it matches."""
    _with_token(monkeypatch, token)
    r = client.get(
        "/lookup-stream",
        params={"lei": _LEI},
        headers={"User-Agent": _HEADLESS_UA, "X-OpenCheck-QA": token},
    )
    assert r.status_code == 403


def test_qa_query_parameter_alone_opens_nothing(client, monkeypatch) -> None:
    """``?qa=1`` is the analytics opt-out, typed by anyone; it is not a pass."""
    _with_token(monkeypatch, _TOKEN)
    r = client.get(
        "/lookup-stream",
        params={"lei": _LEI, "qa": "1"},
        headers={"User-Agent": _HEADLESS_UA},
    )
    assert r.status_code == 403


def test_the_batch_and_saved_report_gates_honour_the_token(client, monkeypatch) -> None:
    from opencheck import bot_gate

    _with_token(monkeypatch, _TOKEN)

    class _Req:
        def __init__(self, headers):
            self.headers = headers

    assert bot_gate.refuses(_Req({"user-agent": _HEADLESS_UA}))
    assert not bot_gate.refuses(
        _Req({"user-agent": _HEADLESS_UA, "X-OpenCheck-QA": _TOKEN})
    )
    for module in ("batch", "saved_reports"):
        source = Path(__file__).parents[1] / "opencheck" / "routers" / f"{module}.py"
        assert "bot_gate.refuses(request)" in source.read_text()
