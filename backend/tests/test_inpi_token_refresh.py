"""INPI Bearer-token expiry — the retry that never retried.

The RNE issues a Bearer token from ``/api/sso/login`` and the adapter holds it
in-process for the life of the server. When the token expires the company call
returns 401, and ``_get_company`` is written to refresh once and retry.

It didn't. ``_refresh_token`` opened with a double-checked-locking guard —
``if self._token: return self._token`` — which is correct for the race it was
written for (two coroutines both needing a *first* token) and wrong for the one
that matters: called from the 401 path, ``self._token`` is still set to the
**expired** token, so the "refresh" handed the same dead token straight back.
The retry re-sent it, got 401 again, and raised.

The effect in production was not a blip. The token is only obtained once per
process and never cleared, so INPI worked from deploy until first expiry and
401'd on every French lookup after that, until the service restarted. Spotted
on a live lookup (Lego France) while the weekly sweep reported INPI healthy —
the sweep runs in a fresh process, so it always takes the first-login path and
can never reach the expiry path at all.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from opencheck.sources.inpi import InpiAdapter

AUTH_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
COMPANY_URL = "https://registre-national-entreprises.inpi.fr/api/companies/832434856"

COMPANY_PAYLOAD = {
    "siren": "832434856",
    "formality": {"content": {"personneMorale": {"identite": {}}}},
}


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("INPI_USERNAME", "user")
    monkeypatch.setenv("INPI_PASSWORD", "pass")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@respx.mock
async def test_an_expired_token_is_actually_replaced():
    """A 401 must produce a NEW token, not the stale one handed back."""
    adapter = InpiAdapter()
    adapter._token = "stale-token"  # as if the process has been up for a day

    login = respx.post(AUTH_URL).mock(
        return_value=httpx.Response(200, json={"token": "fresh-token"})
    )

    seen: list[str | None] = []

    def _company(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("Authorization")
        seen.append(auth)
        if auth == "Bearer stale-token":
            return httpx.Response(401, json={"message": "Expired JWT Token"})
        return httpx.Response(200, json=COMPANY_PAYLOAD)

    respx.get(COMPANY_URL).mock(side_effect=_company)

    bundle = await adapter._get_company("832434856")

    assert bundle["siren"] == "832434856"
    assert login.called, "a 401 must trigger a login, not reuse the cached token"
    assert seen == ["Bearer stale-token", "Bearer fresh-token"], (
        "the retry must carry the refreshed token; sending the stale one again "
        "is the bug this test exists for"
    )
    assert adapter._token == "fresh-token"


@respx.mock
async def test_the_first_token_is_fetched_once_and_cached():
    """The guard being fixed must still prevent a login per request."""
    adapter = InpiAdapter()
    login = respx.post(AUTH_URL).mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.get(COMPANY_URL).mock(return_value=httpx.Response(200, json=COMPANY_PAYLOAD))

    await adapter._get_company("832434856")
    await adapter._get_company("832434856")

    assert login.call_count == 1, "the token should be cached across calls"


@respx.mock
async def test_a_concurrent_caller_does_not_relogin_after_someone_refreshed():
    """Two coroutines hitting 401 together must share one refresh.

    This is the race the original guard was written for, and the fix has to
    keep it: whoever loses the lock finds a token that is no longer the stale
    one it saw, and uses it rather than logging in again.
    """
    adapter = InpiAdapter()
    adapter._token = "stale-token"
    login = respx.post(AUTH_URL).mock(
        return_value=httpx.Response(200, json={"token": "fresh-token"})
    )

    def _company(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Authorization") == "Bearer stale-token":
            return httpx.Response(401, json={"message": "Expired JWT Token"})
        return httpx.Response(200, json=COMPANY_PAYLOAD)

    respx.get(COMPANY_URL).mock(side_effect=_company)

    import asyncio

    await asyncio.gather(
        adapter._get_company("832434856"),
        adapter._get_company("832434856"),
    )

    assert login.call_count == 1, (
        "both callers saw the same stale token, so exactly one login should "
        "have happened"
    )
