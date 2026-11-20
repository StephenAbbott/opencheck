"""NZ register answers expire after a week (the post-go-live NZ address check).

Under the Companies (Address Information) Amendment Act 2025, in force since
18 Nov 2026, a director can replace the residential address the public
register shows with an alternative one. A cached NZBN or Role Search answer
with no expiry kept the old residential address until the next deploy, so both
caches are now read with ``NZ_CACHE_MAX_AGE_DAYS``.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from opencheck.cache import Cache
from opencheck.sources.nz_companies import NZ_CACHE_MAX_AGE_DAYS, NzCompaniesAdapter
from tests.test_nz_companies import FULL_ENTITY, _live, _make_client, _resp

_NZBN = "9429000035170"


def _age(key: str, days: float) -> None:
    path = Cache()._live() / f"{key}.json"
    wrapper = json.loads(path.read_text())
    wrapper["_cached_at"] = time.time() - days * 86_400
    path.write_text(json.dumps(wrapper))


def test_the_expiry_is_a_week() -> None:
    assert NZ_CACHE_MAX_AGE_DAYS == 7


@pytest.mark.parametrize(("age_days", "refetched"), [(6, False), (8, True)])
@pytest.mark.asyncio
async def test_a_cached_company_is_read_again_after_a_week(
    monkeypatch, tmp_path, age_days: float, refetched: bool
) -> None:
    _live(monkeypatch, tmp_path)
    key = f"nz_companies/company-v2/{_NZBN}"
    Cache().put(key, {"stale": True, "is_stub": False})
    _age(key, age_days)
    calls: list[str] = []

    async def route(url, **kw):
        calls.append(url)
        return _resp(200, FULL_ENTITY)

    with patch(
        "opencheck.sources.nz_companies.build_client",
        return_value=_make_client(route),
    ):
        bundle = await NzCompaniesAdapter().fetch(_NZBN, legal_name="")

    assert bool(calls) is refetched
    assert ("stale" in bundle) is not refetched


@pytest.mark.asyncio
async def test_role_search_answers_are_read_with_the_expiry(monkeypatch, tmp_path) -> None:
    from opencheck import nz_associations

    _live(monkeypatch, tmp_path)
    monkeypatch.setenv("NZBN_ROLE_SEARCH_API_KEY", "rs-key")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    cache = MagicMock()
    cache.get_payload.return_value = (
        {"available": True, "company_number": "1166320", "people": []},
        "live",
    )
    with patch.object(nz_associations, "_cache", cache):
        await nz_associations.assemble_associations("1166320")
        await nz_associations.person_associations("1166320", "Jane Doe")

    assert cache.get_payload.call_count >= 2
    for call in cache.get_payload.call_args_list:
        assert call.kwargs.get("max_age_days") == NZ_CACHE_MAX_AGE_DAYS
