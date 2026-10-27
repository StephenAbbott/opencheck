"""The Slovak registers say when they did not answer (the silent-stub fix).

Found by the BODS quality sweep of 26 Oct 2026 (``deepen_empty|rpo_slovakia``):
production ``/deepen?source=rpo_slovakia&hit_id=35815256`` answered after
~16 s with ``{"is_stub": true}`` and no statements, while the weekly
source-health sweep on GitHub Actions read the same record in 1.3 s an hour
earlier. ``RpoSlovakiaAdapter._get_list`` caught every exception and returned
``[]``, and ``fetch`` turned ``[]`` into a stub — so a register that did not
answer read exactly like live mode switched off: no ``source_error``, no
degradation, and a Slovak lookup that said nothing about the gap.
``rpvs_slovakia``, which shares the ``sk_ico`` key, had the same shape twice.

Now a failure is raised (the pipeline turns it into a ``source_error`` and a
``source_fetch`` degradation, and the card offers a retry), it is never
cached, and an empty answer is cached as a definitive absence that expires.
"""

from __future__ import annotations

import re
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation
from opencheck.cache import Cache
from opencheck.routers import lookup as lookup_mod
from opencheck.sources.rpo_slovakia import RpoSlovakiaAdapter
from opencheck.sources.rpvs_slovakia import RpvsSlovakiaAdapter

ICO = "35815256"
RPO_URL = re.compile(r"https://api\.statistics\.sk/rpo/v1/search\?identifier=35815256.*")
RPVS_RESOLVE = re.compile(r"https://rpvs\.gov\.sk/opendatav2/PartneriVerejnehoSektora.*")
RPVS_DETAIL = re.compile(r"https://rpvs\.gov\.sk/opendatav2/Partneri\(4242\).*")

RPO_RECORD: dict[str, Any] = {
    "id": 1,
    "identifiers": [{"type": {"id": "ICO", "value": "IČO"}, "value": ICO}],
    "fullNames": [{"value": "Slovenský plynárenský priemysel, a.s.", "validFrom": "1999-01-01"}],
    "addresses": [],
    "establishment": "1999-01-01",
}


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _cached(key: str) -> bool:
    return (Cache()._live() / f"{key}.json").is_file()


# ---------------------------------------------------------------------------
# rpo_slovakia
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("timed out"),
        httpx.ConnectError("refused"),
    ],
    ids=["read-timeout", "connect-error"],
)
@respx.mock
async def test_rpo_raises_when_the_register_does_not_answer(failure):
    respx.get(RPO_URL).mock(side_effect=failure)
    with pytest.raises(type(failure)):
        await RpoSlovakiaAdapter().fetch(ICO)
    assert not _cached(f"rpo_slovakia/ico/{ICO}")


@pytest.mark.parametrize("status", [429, 500, 503])
@respx.mock
async def test_rpo_raises_on_an_http_error_and_caches_nothing(status):
    respx.get(RPO_URL).mock(return_value=httpx.Response(status))
    with pytest.raises(httpx.HTTPStatusError):
        await RpoSlovakiaAdapter().fetch(ICO)
    assert not _cached(f"rpo_slovakia/ico/{ICO}")


@respx.mock
async def test_rpo_raises_on_an_unreadable_body():
    respx.get(RPO_URL).mock(return_value=httpx.Response(200, text="<html>busy</html>"))
    with pytest.raises(ValueError):
        await RpoSlovakiaAdapter().fetch(ICO)
    assert not _cached(f"rpo_slovakia/ico/{ICO}")


@respx.mock
async def test_rpo_answer_is_cached_and_read_back():
    route = respx.get(RPO_URL).mock(return_value=httpx.Response(200, json=[RPO_RECORD]))
    bundle = await RpoSlovakiaAdapter().fetch(ICO)
    assert bundle["is_stub"] is False
    assert bundle["name"].startswith("Slovenský plynárenský")
    again = await RpoSlovakiaAdapter().fetch(ICO)
    assert again["name"] == bundle["name"]
    assert route.call_count == 1


@respx.mock
async def test_rpo_empty_answer_is_a_definitive_absence_that_expires():
    respx.get(RPO_URL).mock(return_value=httpx.Response(200, json=[]))
    bundle = await RpoSlovakiaAdapter().fetch(ICO)
    assert bundle["is_stub"] is True
    payload = Cache().get_payload(f"rpo_slovakia/ico/{ICO}")
    # put_absent writes a cached None (which get_payload stops honouring
    # after ABSENT_TTL_DAYS), never an everlasting empty list.
    assert payload is not None and payload[0] is None


@respx.mock
async def test_rpo_name_search_failure_reaches_the_search_router():
    respx.get(re.compile(r"https://api\.statistics\.sk/rpo/v1/search\?fullName=.*")).mock(
        side_effect=httpx.ReadTimeout("timed out")
    )
    from opencheck.sources.base import SearchKind

    with pytest.raises(httpx.ReadTimeout):
        await RpoSlovakiaAdapter().search("Slovenský plynárenský", SearchKind.ENTITY)


# ---------------------------------------------------------------------------
# rpvs_slovakia
# ---------------------------------------------------------------------------


@respx.mock
async def test_rpvs_raises_when_the_resolve_step_does_not_answer():
    respx.get(RPVS_RESOLVE).mock(side_effect=httpx.ReadTimeout("timed out"))
    with pytest.raises(httpx.ReadTimeout):
        await RpvsSlovakiaAdapter().fetch(ICO)
    assert not _cached(f"rpvs_slovakia/resolve/{ICO}")


@respx.mock
async def test_rpvs_raises_when_the_partner_record_does_not_answer():
    respx.get(RPVS_RESOLVE).mock(
        return_value=httpx.Response(200, json={"value": [{"Id": 1, "Partner": {"Id": 4242}}]})
    )
    respx.get(RPVS_DETAIL).mock(return_value=httpx.Response(503))
    with pytest.raises(httpx.HTTPStatusError):
        await RpvsSlovakiaAdapter().fetch(ICO)
    assert not _cached("rpvs_slovakia/partner/4242")


@respx.mock
async def test_rpvs_not_a_partner_is_still_an_answer():
    respx.get(RPVS_RESOLVE).mock(return_value=httpx.Response(200, json={"value": []}))
    bundle = await RpvsSlovakiaAdapter().fetch(ICO)
    assert bundle["is_stub"] is True
    payload = Cache().get_payload(f"rpvs_slovakia/resolve/{ICO}")
    assert payload is not None and payload[0] is None


# ---------------------------------------------------------------------------
# What the lookup makes of it
# ---------------------------------------------------------------------------


@respx.mock
async def test_a_timeout_becomes_a_source_error_and_a_degradation_in_the_lookup():
    """The pipeline's own path: ``_run_source`` → ``_source_outcome`` →
    ``add_source_errors``. Before the fix this was a stub with no error."""
    respx.get(RPO_URL).mock(side_effect=httpx.ReadTimeout("timed out"))
    src_id, result, prov = await lookup_mod._run_source(
        "rpo_slovakia", RpoSlovakiaAdapter().fetch(ICO)
    )
    assert isinstance(result, httpx.ReadTimeout)

    ctx = lookup_mod._LookupCtx(lei="097900BHLN0000098553", legal_name="SPP", jurisdiction="SK")
    outcome = lookup_mod._source_outcome(src_id, result, prov, ctx)
    assert outcome.hits == []
    assert outcome.error and "ReadTimeout" in outcome.error

    degraded: list = []
    degradation.add_source_errors(
        degraded, {src_id: outcome.error}, with_data=set()
    )
    assert [(d.source_id, d.check, d.reason) for d in degraded] == [
        ("rpo_slovakia", degradation.CHECK_SOURCE_FETCH, degradation.REASON_TIMEOUT)
    ]
