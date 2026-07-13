"""Live OpenAleph adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.openaleph import OpenAlephAdapter

_API = "https://search.openaleph.org/api/2"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_entity_search_maps_results(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities?q=acme&filter:schema=LegalEntity&limit=10",
        json={
            "results": [
                {
                    "id": "aleph-123",
                    "schema": "Company",
                    "properties": {
                        "name": ["Acme Holdings"],
                        "leiCode": ["LEI0000000000000ACME"],
                    },
                    "collection": {
                        "id": 42,
                        "foreign_id": "icij-leaks",
                        "label": "ICIJ leaks",
                    },
                }
            ]
        },
    )

    adapter = OpenAlephAdapter()
    hits = await adapter.search("acme", SearchKind.ENTITY)

    assert len(hits) == 1
    hit = hits[0]
    assert hit.is_stub is False
    assert hit.name == "Acme Holdings"
    assert hit.hit_id == "aleph-123"
    assert hit.identifiers["lei"] == "LEI0000000000000ACME"
    assert "ICIJ leaks" in hit.summary


async def test_fetch_pulls_collection_metadata(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities/aleph-123",
        json={
            "id": "aleph-123",
            "schema": "Company",
            "properties": {"name": ["Acme Holdings"]},
            "collection": {"id": "42"},
        },
    )
    httpx_mock.add_response(
        url=f"{_API}/collections/42",
        json={"id": 42, "label": "ICIJ leaks", "license": "CC BY-NC 4.0"},
    )

    adapter = OpenAlephAdapter()
    bundle = await adapter.fetch("aleph-123")
    assert bundle["entity"]["id"] == "aleph-123"
    assert bundle["collection"]["license"] == "CC BY-NC 4.0"


async def test_auth_header_sent_when_key_set(
    httpx_mock: HTTPXMock, monkeypatch
) -> None:
    monkeypatch.setenv("OPENALEPH_API_KEY", "secret")
    get_settings.cache_clear()

    httpx_mock.add_response(
        url=f"{_API}/entities?q=acme&filter:schema=LegalEntity&limit=10",
        match_headers={"Authorization": "ApiKey secret"},
        json={"results": []},
    )

    adapter = OpenAlephAdapter()
    await adapter.search("acme", SearchKind.ENTITY)


async def test_stub_path_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = OpenAlephAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


# ---------------------------------------------------------------------------
# fetch_by_lei
# ---------------------------------------------------------------------------

_ERICSSON_ENTITY = {
    "id": "aleph-ericsson-001",
    "schema": "Company",
    "caption": "Ericsson AB",
    "properties": {
        "name": ["Ericsson AB"],
        "leiCode": ["549300MLH00Y3BN4HD49"],
        "jurisdiction": ["se"],
        "registrationNumber": ["556056-6258"],
    },
    "collection": {"id": 7, "foreign_id": "orbis", "label": "Bureau van Dijk Orbis"},
}


async def test_fetch_by_lei_returns_hits(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.leiCode=549300MLH00Y3BN4HD49"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_lei("549300MLH00Y3BN4HD49")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"
    assert hits[0].name == "Ericsson AB"
    assert hits[0].is_stub is False
    assert hits[0].identifiers["lei"] == "549300MLH00Y3BN4HD49"


async def test_fetch_by_lei_empty_result(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.leiCode=UNKNOWNLEI00000000XX"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_lei("UNKNOWNLEI00000000XX")
    assert hits == []


async def test_fetch_by_lei_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_lei("549300MLH00Y3BN4HD49")
    assert hits == []


# ---------------------------------------------------------------------------
# fetch_by_oc_url
# ---------------------------------------------------------------------------


async def test_fetch_by_oc_url_returns_hits(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    oc_url = "https://opencorporates.com/companies/se/556056-6258"
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.opencorporatesUrl={quote(oc_url)}"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_oc_url("se/556056-6258")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"


async def test_fetch_by_oc_url_empty_result(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    oc_url = "https://opencorporates.com/companies/gb/99999999"
    httpx_mock.add_response(
        url=f"{_API}/entities?filter:properties.opencorporatesUrl={quote(oc_url)}"
            f"&filter:schema=LegalEntity&limit=5",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_oc_url("gb/99999999")
    assert hits == []


async def test_fetch_by_oc_url_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_oc_url("se/556056-6258")
    assert hits == []


# ---------------------------------------------------------------------------
# fetch_by_registration
# ---------------------------------------------------------------------------


async def test_fetch_by_registration_returns_hits(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=(
            f"{_API}/entities"
            f"?filter:properties.registrationNumber={quote('556056-6258')}"
            f"&filter:properties.jurisdiction={quote('se')}"
            f"&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("se", "556056-6258")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"
    assert hits[0].name == "Ericsson AB"


async def test_fetch_by_registration_uppercases_jurisdiction_lowercased(
    httpx_mock: HTTPXMock,
) -> None:
    """Jurisdiction is always sent lowercase regardless of caller input."""
    from urllib.parse import quote
    httpx_mock.add_response(
        url=(
            f"{_API}/entities"
            f"?filter:properties.registrationNumber={quote('00102498')}"
            f"&filter:properties.jurisdiction={quote('gb')}"
            f"&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("GB", "00102498")
    assert hits == []


async def test_fetch_by_registration_empty_result(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=(
            f"{_API}/entities"
            f"?filter:properties.registrationNumber={quote('ZZZZZZZZ')}"
            f"&filter:properties.jurisdiction={quote('fr')}"
            f"&filter:schema=LegalEntity&limit=5"
        ),
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("fr", "ZZZZZZZZ")
    assert hits == []


async def test_fetch_by_registration_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_registration("se", "556056-6258")
    assert hits == []


# ---------------------------------------------------------------------------
# fetch_by_name (strategy 4 — name-based fallback)
# ---------------------------------------------------------------------------

async def test_fetch_by_name_returns_hits(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Ericsson AB')}&filter:schema=LegalEntity&limit=5",
        json={"results": [_ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-ericsson-001"
    assert hits[0].name == "Ericsson AB"


async def test_fetch_by_name_empty_result(httpx_mock: HTTPXMock) -> None:
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Nonexistent Corp')}&filter:schema=LegalEntity&limit=5",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Nonexistent Corp")
    assert hits == []


async def test_fetch_by_name_stub_when_live_disabled(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert hits == []


async def test_fetch_by_name_strips_quotes_that_break_aleph(httpx_mock: HTTPXMock) -> None:
    """A legal name with nested ASCII quotes (e.g. Rosneft) must not be sent
    verbatim — unbalanced quotes make Aleph's query_string parser 500."""
    from urllib.parse import quote
    raw_name = 'Публичное акционерное общество "Нефтяная компания "Роснефть"'
    sanitised = "Публичное акционерное общество Нефтяная компания Роснефть"
    # The hit must bear the queried name — the name gate (issue #21) rejects
    # anything else, so this fixture doubles as proof the gate is quote-blind.
    rosneft = {
        "id": "aleph-rosneft-001",
        "schema": "Company",
        "caption": raw_name,
        "properties": {"name": [raw_name]},
        "collection": {"id": 1, "foreign_id": "gleif", "label": "GLEIF"},
    }
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote(sanitised)}&filter:schema=LegalEntity&limit=5",
        json={"results": [rosneft]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name(raw_name)
    assert len(hits) == 1  # request used the sanitised query — no 500


# --- name-equivalence gate (issue #21) -------------------------------------
#
# Aleph's free-text q= is BM25-ranked, and a rank is not a match: the query
# "Canada Basketball" returned exactly ONE hit — an unrelated Honduran cleaning
# company — which was therefore also the top-scoring hit (score 6.6), so no
# relative cutoff could catch it; and "The Foundation Foundation" returns GB
# Group plc at score 77, so no absolute cutoff could either. Both were verified
# against the live index. A hit is kept only when it BEARS the queried name.

_CLEANING_CO = {
    "id": "aleph-zoe-001",
    "schema": "Company",
    "caption": "Empresa de Limpieza y Mantenimiento Zoe",
    "properties": {"name": ["Empresa de Limpieza y Mantenimiento Zoe"]},
    "collection": {"id": 9, "foreign_id": "ocds-hn", "label": "OCDS: Honduras"},
}


async def test_fetch_by_name_rejects_hit_that_does_not_bear_the_name(
    httpx_mock: HTTPXMock,
) -> None:
    """The Canada Basketball false positive: a lone, unrelated top hit."""
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Canada Basketball')}&filter:schema=LegalEntity&limit=5",
        json={"results": [_CLEANING_CO]},
    )
    adapter = OpenAlephAdapter()
    assert await adapter.fetch_by_name("Canada Basketball") == []


async def test_fetch_by_name_keeps_only_the_name_bearing_hits(
    httpx_mock: HTTPXMock,
) -> None:
    """Mixed result set: genuine hit kept, high-scoring impostor dropped."""
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Ericsson AB')}&filter:schema=LegalEntity&limit=5",
        json={"results": [_CLEANING_CO, _ERICSSON_ENTITY]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert [h.hit_id for h in hits] == ["aleph-ericsson-001"]


async def test_fetch_by_name_gate_is_case_and_punctuation_insensitive(
    httpx_mock: HTTPXMock,
) -> None:
    """Live hits arrive as "ERICSSON AB" for a subject named "Ericsson AB"."""
    from urllib.parse import quote
    shouty = {
        **_ERICSSON_ENTITY,
        "id": "aleph-ericsson-002",
        "caption": "ERICSSON AB.",
        "properties": {"name": ["ERICSSON AB."]},
    }
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Ericsson AB')}&filter:schema=LegalEntity&limit=5",
        json={"results": [shouty]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert len(hits) == 1


async def test_fetch_by_name_gate_accepts_alias_and_previous_name(
    httpx_mock: HTTPXMock,
) -> None:
    """An entity legitimately known under an alias still matches."""
    from urllib.parse import quote
    aliased = {
        "id": "aleph-alias-001",
        "schema": "Company",
        "caption": "Telefonaktiebolaget LM Ericsson",
        "properties": {
            "name": ["Telefonaktiebolaget LM Ericsson"],
            "alias": ["Ericsson AB"],
        },
        "collection": {"id": 7, "foreign_id": "orbis", "label": "Orbis"},
    }
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Ericsson AB')}&filter:schema=LegalEntity&limit=5",
        json={"results": [aliased]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Ericsson AB")
    assert len(hits) == 1
    assert hits[0].hit_id == "aleph-alias-001"


async def test_fetch_by_name_tolerates_server_error(httpx_mock: HTTPXMock) -> None:
    """If Aleph still 500s on a free-text search, degrade to no results rather
    than surfacing an error card for the source."""
    from urllib.parse import quote
    httpx_mock.add_response(
        url=f"{_API}/entities?q={quote('Boom Corp')}&filter:schema=LegalEntity&limit=5",
        status_code=500,
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name("Boom Corp")
    assert hits == []


# ---------------------------------------------------------------------------
# Mentions enrichment (OpenAleph 5.3 — /entities/{id}/mentions)
# ---------------------------------------------------------------------------


async def test_fetch_mentions_parses_documents(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{_API}/entities/aleph-123/mentions?limit=5&facet=collection_id&facet_size=10",
        json={
            "status": "ok",
            "total": 33,
            "total_type": "eq",
            "facets": {
                "collection_id": {
                    "values": [
                        {"id": "1", "label": "AskTheEU FOI documents", "count": 20},
                        {"id": "2", "label": "Leak X", "count": 13},
                    ]
                }
            },
            "results": [
                {
                    "id": "doc-1",
                    "schema": "Pages",
                    "caption": "Annex1 1.pdf",
                    "collection": {
                        "label": "AskTheEU FOI documents",
                        "foreign_id": "asktheeu",
                        "category": "library",
                    },
                    "links": {"ui": "https://search.openaleph.org/entities/doc-1"},
                },
                {
                    "id": "doc-2",
                    "schema": "Email",
                    "caption": "RE: contract award",
                    "collection": {"foreign_id": "leak-x", "category": "leak"},
                    "links": {},
                },
            ],
        },
    )

    adapter = OpenAlephAdapter()
    mentions = await adapter.fetch_mentions("aleph-123")

    assert mentions is not None
    assert mentions["total"] == 33
    assert len(mentions["documents"]) == 2
    # Issue #23: the breakdown is the collection_id FACET — exact across all 33
    # mentions — never counted from the 2 sampled documents, which would have
    # reported 1 + 1 and misrepresented the other 31.
    assert mentions["collections"] == [
        {"label": "AskTheEU FOI documents", "count": 20},
        {"label": "Leak X", "count": 13},
    ]
    assert sum(c["count"] for c in mentions["collections"]) == mentions["total"]
    first = mentions["documents"][0]
    assert first["title"] == "Annex1 1.pdf"
    assert first["collection"] == "AskTheEU FOI documents"
    assert first["category"] == "library"
    assert first["url"].endswith("/entities/doc-1")
    # Falls back to foreign_id when the collection has no label.
    assert mentions["documents"][1]["collection"] == "leak-x"


async def test_fetch_mentions_degrades_to_none_on_error(httpx_mock: HTTPXMock) -> None:
    """Pre-5.3 instances 404 on /mentions — enrichment must degrade quietly."""
    httpx_mock.add_response(
        url=f"{_API}/entities/aleph-123/mentions?limit=5&facet=collection_id&facet_size=10",
        status_code=404,
    )
    adapter = OpenAlephAdapter()
    assert await adapter.fetch_mentions("aleph-123") is None


async def test_fetch_mentions_none_when_live_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    try:
        adapter = OpenAlephAdapter()
        assert await adapter.fetch_mentions("aleph-123") is None
    finally:
        get_settings.cache_clear()


async def test_lookup_strategies_attach_mentions_summary(monkeypatch) -> None:
    """_openaleph_strategies enriches top hits with the mentions count —
    informational only (raw payload + summary suffix, no identifiers)."""
    from opencheck.routers import lookup as lookup_mod
    from opencheck.sources import REGISTRY
    from opencheck.sources.base import SourceHit

    ctx = lookup_mod._LookupCtx(lei="LEI0000000000000ACME")
    adapter = REGISTRY["openaleph"]

    hit = SourceHit(
        source_id="openaleph", hit_id="aleph-123", kind=SearchKind.ENTITY,
        name="Acme Holdings", summary="collection: ICIJ leaks · Company",
        identifiers={"aleph_id": "aleph-123"}, raw={}, is_stub=False,
    )

    async def fake_fetch_by_lei(lei):
        return [hit]

    async def fake_fetch_mentions(entity_id, limit=5):
        assert entity_id == "aleph-123"
        return {"total": 33, "documents": [{"title": "Annex1 1.pdf",
                "collection": "AskTheEU", "category": "library", "url": ""}]}

    monkeypatch.setattr(adapter, "fetch_by_lei", fake_fetch_by_lei)
    monkeypatch.setattr(adapter, "fetch_mentions", fake_fetch_mentions)

    result = await lookup_mod._openaleph_strategies(ctx)
    assert len(result) == 1
    assert result[0].summary.endswith("mentioned in 33 documents")
    assert result[0].raw["openaleph_mentions"]["total"] == 33


async def test_lookup_strategies_tolerate_mentions_failure(monkeypatch) -> None:
    from opencheck.routers import lookup as lookup_mod
    from opencheck.sources import REGISTRY
    from opencheck.sources.base import SourceHit

    ctx = lookup_mod._LookupCtx(lei="LEI0000000000000ACME")
    adapter = REGISTRY["openaleph"]

    hit = SourceHit(
        source_id="openaleph", hit_id="aleph-999", kind=SearchKind.ENTITY,
        name="Acme Holdings", summary="collection: ICIJ leaks · Company",
        identifiers={"aleph_id": "aleph-999"}, raw={}, is_stub=False,
    )

    async def fake_fetch_by_lei(lei):
        return [hit]

    async def boom(entity_id, limit=5):
        raise RuntimeError("mentions exploded")

    monkeypatch.setattr(adapter, "fetch_by_lei", fake_fetch_by_lei)
    monkeypatch.setattr(adapter, "fetch_mentions", boom)

    result = await lookup_mod._openaleph_strategies(ctx)
    assert len(result) == 1
    assert "mentioned in" not in result[0].summary


async def test_fetch_mentions_without_facets_yields_empty_breakdown(
    httpx_mock: HTTPXMock,
) -> None:
    """An instance that returns no facets (or an empty one) must still give a
    valid mentions payload — the card simply shows no archive chips."""
    httpx_mock.add_response(
        url=f"{_API}/entities/aleph-9/mentions?limit=5&facet=collection_id&facet_size=10",
        json={"status": "ok", "total": 4, "results": [], "facets": {}},
    )
    adapter = OpenAlephAdapter()
    mentions = await adapter.fetch_mentions("aleph-9")
    assert mentions == {"total": 4, "documents": [], "collections": []}


async def test_fetch_mentions_cache_key_includes_limit(httpx_mock: HTTPXMock) -> None:
    """Regression: the cache key omitted `limit`, so a limit=5 response was
    replayed for a limit=50 request (found while building the breakdown)."""
    for limit, total in ((5, 61), (50, 61)):
        httpx_mock.add_response(
            url=f"{_API}/entities/aleph-7/mentions"
            f"?limit={limit}&facet=collection_id&facet_size=10",
            json={"status": "ok", "total": total, "results": [], "facets": {}},
        )
    adapter = OpenAlephAdapter()
    assert (await adapter.fetch_mentions("aleph-7", limit=5))["total"] == 61
    # A different limit must issue a *new* request, not replay the cached one.
    assert (await adapter.fetch_mentions("aleph-7", limit=50))["total"] == 61
