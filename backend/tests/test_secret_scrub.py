"""Phase 233 — no response, event or stored record carries a query-string key.

Datafordeler CVR (``?apiKey=``) and OpenCorporates (``?api_token=``) put
their key in the URL, and httpx puts the URL in every ``HTTPStatusError``.
Before this phase a 401/429/5xx from either reached the ``source_error`` SSE
event, ``/lookup`` and ``/search`` errors, the replay cache and — through it —
a saved report, whose hashed bytes cannot be redacted afterwards.

The end-to-end tests inject a key-bearing URL into a failing adapter **with
the key absent from settings**, so the configured-value scrub cannot hide a
regression in the structural layers (describe the status error; scrub
credential-named parameters). The value scrub has its own tests.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from opencheck import saved_reports as sr
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers import lookup as lookup_mod
from opencheck.routers.lookup import _fmt_source_error as lookup_fmt
from opencheck.routers.search import _fmt_source_error as search_fmt
from opencheck.secret_scrub import REDACTED, describe_exception, safe_message, scrub
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.schemas import SourceSchemaError

LEI = "213800LH1BZH3DI6G760"
KEY = "sk-live-4f1c2b9e8d7a6f5e"
TOKEN = "oc-token-77aa88bb99cc"
CVR_URL = f"https://graphql.datafordeler.dk/CVR/v2?apiKey={KEY}"
OC_URL = f"https://api.opencorporates.com/v0.4/companies/gb/00102498?api_token={TOKEN}"
BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/128 Safari/537.36"


def _status_error(url: str, code: int = 401) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    response = httpx.Response(code, request=request)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:  # the real message httpx builds
        return exc
    raise AssertionError("raise_for_status did not raise")


# ---- the premise --------------------------------------------------------------


def test_httpx_really_puts_the_key_in_the_message() -> None:
    """If httpx ever stops doing this the phase is moot — but not wrong."""
    assert KEY in str(_status_error(CVR_URL))


# ---- describing an exception --------------------------------------------------


@pytest.mark.parametrize("code,reason", [(401, "Unauthorized"), (429, "Too Many Requests"), (503, "Service Unavailable")])
def test_a_status_error_is_described_by_code_and_host_only(code: int, reason: str) -> None:
    text = describe_exception(_status_error(CVR_URL, code))
    assert text == f"HTTPStatusError: HTTP {code} {reason} from graphql.datafordeler.dk"
    assert KEY not in text and "apiKey" not in text and "/CVR/v2" not in text


def test_both_routers_format_through_the_scrub() -> None:
    for fmt in (lookup_fmt, search_fmt):
        for exc in (_status_error(CVR_URL), _status_error(OC_URL, 500)):
            text = fmt(exc)
            assert KEY not in text and TOKEN not in text
            assert text.startswith("HTTPStatusError: HTTP ")
        # A schema error is still labelled as one, and still scrubbed.
        schema = fmt(SourceSchemaError("cvr_denmark", f"bad field at {CVR_URL}"))
        assert schema.startswith("Source API changed — ") and KEY not in schema


def test_a_rewrapped_status_error_is_scrubbed_by_parameter_name() -> None:
    wrapped = RuntimeError(f"CVR request failed: {_status_error(CVR_URL)}")
    text = describe_exception(wrapped)
    assert KEY not in text
    assert f"apiKey={REDACTED}" in text
    assert text.startswith("RuntimeError: CVR request failed")


def test_the_catch_all_message_drops_the_type_prefix_but_not_the_scrub() -> None:
    assert safe_message(_status_error(OC_URL, 502)) == "HTTP 502 Bad Gateway from api.opencorporates.com"
    assert TOKEN not in safe_message(ValueError(OC_URL))


# ---- scrub --------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["apiKey", "api_key", "API-KEY", "api_token", "access_token", "token", "key",
     "client_secret", "password", "signature"],
)
def test_credential_named_parameters_are_redacted(name: str) -> None:
    text = scrub(f"GET https://x.example/p?q=shell&{name}=s3cr3tvalue&page=2 failed")
    assert "s3cr3tvalue" not in text
    assert f"{name}={REDACTED}" in text
    assert "q=shell" in text and "page=2" in text


def test_ordinary_parameters_are_left_alone() -> None:
    text = "https://x.example/search?keyword=bp&tokens_used=4&monkey=1"
    assert scrub(text) == text


def test_url_userinfo_is_redacted() -> None:
    text = scrub("connect failed: https://svc-user:hunter22@db.example:5432/x")
    assert "hunter22" not in text and "svc-user" not in text
    assert f"https://{REDACTED}@db.example" in text


def test_a_configured_secret_is_redacted_wherever_it_appears(monkeypatch: pytest.MonkeyPatch) -> None:
    """The value layer: a key in a header echo, a JSON body or a path — any
    place no parameter-name rule anticipates."""
    monkeypatch.setenv("CVR_DENMARK_API_KEY", KEY)
    monkeypatch.setenv("OPENCORPORATES_API_KEY", TOKEN)
    get_settings.cache_clear()
    try:
        text = scrub(f'upstream said {{"auth": "{KEY}"}} at /v1/{TOKEN}/x')
        assert KEY not in text and TOKEN not in text
        assert text.count(REDACTED) == 2
    finally:
        get_settings.cache_clear()


def test_a_short_setting_is_not_used_as_a_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMI_API_KEY", "abc")
    get_settings.cache_clear()
    try:
        assert scrub("abcdef") == "abcdef"
    finally:
        get_settings.cache_clear()


# ---- end to end: events, responses and the saved record -----------------------


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_DB_FILE", str(tmp_path / "saved_reports.sqlite"))
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_PRUNE_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    # Deliberately NOT configured: only the structural layers may protect.
    monkeypatch.delenv("CVR_DENMARK_API_KEY", raising=False)
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    sr.reset_for_tests()
    lookup_mod._REPLAY_CACHE.clear()
    target = tmp_path / "data" / "cache" / "bods_data" / "gleif" / f"{LEI}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({
        "statementId": "e-subject", "recordType": "entity",
        "recordDetails": {"name": "Bundle Co P.L.C.", "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                          "identifiers": [{"id": LEI, "scheme": "XI-LEI"}, {"id": "12345678", "scheme": "GB-COH"}]},
    }) + "\n")
    yield tmp_path
    lookup_mod._REPLAY_CACHE.clear()
    sr.reset_for_tests()
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app, headers={"User-Agent": BROWSER_UA})


@pytest.fixture
def failing_ch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Companies House is dispatched for the seeded GB bundle; make it fail
    the way CVR does on a revoked key."""
    adapter = REGISTRY["companies_house"]

    async def fetch(*_a, **_kw):
        raise _status_error(CVR_URL, 401)

    async def search(*_a, **_kw):
        raise _status_error(OC_URL, 429)

    monkeypatch.setattr(adapter, "fetch", fetch)
    monkeypatch.setattr(adapter, "search", search)


def _assert_clean(blob: str) -> None:
    assert KEY not in blob, "the CVR key leaked"
    assert TOKEN not in blob, "the OpenCorporates token leaked"
    assert "apiKey=" not in blob and "api_token=" not in blob


def test_lookup_stream_and_saved_report_carry_no_key(env: Path, client: TestClient, failing_ch: None) -> None:
    with client.stream("GET", "/lookup-stream", params={"lei": LEI}) as r:
        stream = "".join(r.iter_text())
    _assert_clean(stream)
    # The failure is still reported, with what a reader can act on.
    assert "HTTPStatusError: HTTP 401 Unauthorized from graphql.datafordeler.dk" in stream

    body = client.get("/lookup", params={"lei": LEI}).json()
    assert body["errors"]["companies_house"] == (
        "HTTPStatusError: HTTP 401 Unauthorized from graphql.datafordeler.dk"
    )
    _assert_clean(json.dumps(body))

    entry = lookup_mod.replay_entry(LEI)
    assert entry is not None
    _assert_clean(json.dumps(sr.serialise_events(entry.events)))

    saved = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": body["run_completed_at"]})
    assert saved.status_code == 201, saved.text
    rid = saved.json()["report_id"]
    _assert_clean(client.get(f"/saved-reports/{rid}").text)
    _assert_clean(client.get(f"/saved-reports/{rid}.json").text)


def test_lookup_source_retry_carries_no_key(env: Path, client: TestClient, failing_ch: None) -> None:
    r = client.get("/lookup-source", params={"lei": LEI, "source_id": "companies_house"})
    assert r.status_code == 200
    _assert_clean(r.text)
    assert r.json()["error"].startswith("HTTPStatusError: HTTP 401")


def test_search_errors_and_stream_carry_no_key(env: Path, client: TestClient, failing_ch: None) -> None:
    r = client.get("/search", params={"q": "bundle co", "kind": SearchKind.ENTITY.value})
    assert r.status_code == 200
    _assert_clean(r.text)
    assert r.json()["errors"]["companies_house"] == (
        "HTTPStatusError: HTTP 429 Too Many Requests from api.opencorporates.com"
    )
    with client.stream("GET", "/stream", params={"q": "bundle co", "kind": SearchKind.ENTITY.value}) as s:
        _assert_clean("".join(s.iter_text()))


def test_the_catch_all_handler_carries_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import APIRouter

    router = APIRouter()

    @router.get("/__phase233_boom")
    async def boom() -> None:
        raise _status_error(CVR_URL, 500)

    app.include_router(router)
    try:
        r = TestClient(app, raise_server_exceptions=False).get("/__phase233_boom")
    finally:
        app.router.routes[:] = [rt for rt in app.router.routes if getattr(rt, "path", "") != "/__phase233_boom"]
    assert r.status_code == 500
    _assert_clean(r.text)
    assert r.json()["detail"] == "Internal server error: HTTP 500 Internal Server Error from graphql.datafordeler.dk"


async def test_a_batch_row_reason_carries_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    from opencheck import batch

    async def failing_lookup(*_a, **_kw):
        raise RuntimeError(f"upstream failed: {_status_error(OC_URL, 503)}")

    monkeypatch.setattr(lookup_mod, "_lookup_impl", failing_lookup)
    kind, row, _ = await batch._one(LEI, 0, False, asyncio.Semaphore(1))
    assert kind == "row_failed"
    assert TOKEN not in json.dumps(row)
    # Re-wrapped, so the parameter-name layer is what caught it.
    assert f"api_token={REDACTED}" in row["reason"]
    assert row["reason"].startswith("RuntimeError: upstream failed")
