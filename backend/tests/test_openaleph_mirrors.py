"""OpenAleph drops its GLEIF mirror collection (Phase 286).

OpenAleph republishes the GLEIF Concatenated Data File as a collection of its
own (``foreign_id: gleif``, collection 404 on the flagship). A record from it is
GLEIF read twice — the lookup already asks GLEIF directly — so letting it
through made OpenAleph count as a second answering source. NIPPON SUISAN
(U.S.A.), INC (549300I5BCFMO2W0QI94) was the case that showed it: production's
only OpenAleph hit was the GLEIF record, reached through ``POST /match``.

Each test puts a mirror record beside an original one and checks that only
the original survives, at every place results enter the adapter.
"""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.openaleph import (
    _MIRROR_COLLECTIONS,
    OpenAlephAdapter,
    _is_mirror,
    _without_mirrors,
)

_API = "https://search.openaleph.org/api/2"
_LEI = "549300I5BCFMO2W0QI94"
_NAME = "NIPPON SUISAN (U.S.A.), INC."


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENALEPH_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _gleif_record(name: str = _NAME, lei: str = _LEI) -> dict:
    """The flagship's GLEIF-mirror shape, as served on 5 Oct 2026."""
    return {
        "id": f"lei-{lei}.4bb4e7c1d1c677d1f7a0b2a83033176c89f697a3",
        "schema": "Organization",
        "caption": name,
        "score": 120.0,
        "properties": {"name": [name], "leiCode": [lei]},
        "collection": {
            "id": "404",
            "collection_id": "404",
            "foreign_id": "gleif",
            "label": "GLEIF Concatenated Data File",
        },
    }


def _original_record(name: str = _NAME, lei: str = _LEI) -> dict:
    """An ESMA FIRDS record — LEI-keyed, but original data that must stay."""
    return {
        "id": f"eu-esma-firds-{lei}",
        "schema": "Organization",
        "caption": name,
        "score": 100.0,
        "properties": {"name": [name], "leiCode": [lei]},
        "collection": {
            "id": "861",
            "foreign_id": "eu_esma_firds",
            "label": "EU Financial Instruments Reference Data System (FIRDS)",
        },
    }


# ---------------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------------


def test_gleif_is_the_only_mirror_collection() -> None:
    assert _MIRROR_COLLECTIONS == frozenset({"gleif"})


def test_is_mirror_keys_on_foreign_id_not_label_or_numeric_id() -> None:
    assert _is_mirror(_gleif_record())
    assert not _is_mirror(_original_record())
    # The numeric id is one instance's accident; a different instance that
    # numbers the GLEIF file differently is still the mirror.
    assert _is_mirror({"collection": {"id": "9", "foreign_id": "gleif"}})
    # A collection merely *labelled* GLEIF-ish is not, unless keyed as it.
    assert not _is_mirror(
        {"collection": {"id": "404", "foreign_id": "x", "label": "GLEIF"}}
    )
    # Case and whitespace in the foreign_id do not defeat the rule.
    assert _is_mirror({"collection": {"foreign_id": " GLEIF "}})


def test_without_mirrors_tolerates_missing_collection_and_junk() -> None:
    no_collection = {"id": "x", "properties": {}}
    assert _without_mirrors(None) == []
    assert _without_mirrors([no_collection, "junk", _gleif_record()]) == [no_collection]


# ---------------------------------------------------------------------------
# Every GET entry point
# ---------------------------------------------------------------------------


async def test_fetch_by_lei_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=(
            f"{_API}/entities?filter:properties.leiCode={_LEI}"
            "&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": [_gleif_record(), _original_record()]},
    )
    hits = await OpenAlephAdapter().fetch_by_lei(_LEI)
    assert [h.hit_id for h in hits] == [f"eu-esma-firds-{_LEI}"]


async def test_fetch_by_lei_mirror_only_is_no_hit(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=(
            f"{_API}/entities?filter:properties.leiCode={_LEI}"
            "&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": [_gleif_record()]},
    )
    assert await OpenAlephAdapter().fetch_by_lei(_LEI) == []


async def test_fetch_by_oc_url_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote

    oc_url = "https://opencorporates.com/companies/us_wa/601234567"
    httpx_mock.add_response(
        url=(
            f"{_API}/entities?filter:properties.opencorporatesUrl={quote(oc_url)}"
            "&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": [_gleif_record()]},
    )
    assert await OpenAlephAdapter().fetch_by_oc_url("us_wa/601234567") == []


async def test_fetch_by_registration_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=(
            f"{_API}/entities?filter:properties.registrationNumber=00102498"
            "&filter:properties.jurisdiction=gb&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": [_gleif_record(), _original_record()]},
    )
    hits = await OpenAlephAdapter().fetch_by_registration("GB", "00102498")
    assert [h.hit_id for h in hits] == [f"eu-esma-firds-{_LEI}"]


async def test_fetch_by_name_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote

    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote(_NAME)}&filter:schema=LegalEntity&limit=5",
        json={"results": [_gleif_record()]},
    )
    assert await OpenAlephAdapter().fetch_by_name(_NAME) == []


async def test_search_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote

    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote(_NAME)}&filter:schema=LegalEntity&limit=10",
        json={"results": [_gleif_record(), _original_record()]},
    )
    hits = await OpenAlephAdapter().search(_NAME, SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [f"eu-esma-firds-{_LEI}"]


# ---------------------------------------------------------------------------
# POST /match — the path the Nippon Suisan record actually took
# ---------------------------------------------------------------------------


async def test_match_entity_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_API}/match?limit=5",
        json={"status": "ok", "results": [_gleif_record()]},
    )
    hits = await OpenAlephAdapter().match_entity(
        {"schema": "Company", "properties": {"name": [_NAME], "leiCode": [_LEI]}}
    )
    assert hits == []


async def test_match_entity_cutoff_ignores_a_dropped_mirror_top_hit(
    httpx_mock: HTTPXMock,
) -> None:
    """The relative cutoff is measured against what is surfaced. A GLEIF
    record scoring 400 must not push a genuine 90-scoring record under the
    25% bar after the GLEIF record itself has been dropped."""
    gleif = {**_gleif_record(), "score": 400.0}
    other = {
        **_original_record(name="NIPPON SUISAN USA"),
        "score": 90.0,
        "properties": {"name": ["NIPPON SUISAN USA"]},  # no shared identifier
    }
    httpx_mock.add_response(
        method="POST",
        url=f"{_API}/match?limit=5",
        json={"status": "ok", "results": [gleif, other]},
    )
    hits = await OpenAlephAdapter().match_entity(
        {"schema": "Company", "properties": {"name": [_NAME], "leiCode": [_LEI]}}
    )
    assert [h.hit_id for h in hits] == [other["id"]]


# ---------------------------------------------------------------------------
# Percolation — covers the subject-name strategy and the related-party screen
# ---------------------------------------------------------------------------


async def test_percolate_text_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        json={"results": [_gleif_record(), _original_record()]},
    )
    results = await OpenAlephAdapter().percolate_text(_NAME, schema="LegalEntity")
    assert [r["id"] for r in (results or [])] == [f"eu-esma-firds-{_LEI}"]


async def test_percolate_text_mirror_only_ran_clean(httpx_mock: HTTPXMock) -> None:
    """Mirror-only is ``[]`` (ran, nothing to report), never ``None`` (could
    not run) — dropping a mirror must not turn into a degradation notice."""
    httpx_mock.add_response(method="POST", json={"results": [_gleif_record()]})
    assert await OpenAlephAdapter().percolate_text(_NAME) == []


async def test_fetch_by_name_percolate_drops_the_mirror(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(method="POST", json={"results": [_gleif_record()]})
    assert await OpenAlephAdapter().fetch_by_name_percolate(_NAME) == []


# ---------------------------------------------------------------------------
# The whole cascade, Nippon Suisan shape: the mirror at every step → no hits
# ---------------------------------------------------------------------------


async def test_cascade_with_only_the_mirror_answers_with_no_hits(
    httpx_mock: HTTPXMock, monkeypatch
) -> None:
    from opencheck.routers import lookup as lookup_mod

    ctx = lookup_mod._LookupCtx(lei=_LEI)
    ctx.legal_name = _NAME

    adapter = REGISTRY["openaleph"]
    mentions_calls: list[str] = []

    async def no_mentions(entity_id, *_a, **_kw):
        mentions_calls.append(entity_id)
        return None

    monkeypatch.setattr(adapter, "fetch_mentions", no_mentions)
    # Every strategy the cascade reaches is served the GLEIF record only:
    # fetch_by_lei (GET), /match (POST), percolate (POST), q= fallback (GET).
    httpx_mock.add_response(
        method="GET", json={"results": [_gleif_record()]}, is_reusable=True
    )
    httpx_mock.add_response(
        method="POST", json={"results": [_gleif_record()]}, is_reusable=True
    )

    result = await lookup_mod._openaleph_strategies(ctx)
    assert result == []
    # No hit, so no mentions lookup ran off the mirror record either.
    assert mentions_calls == []
