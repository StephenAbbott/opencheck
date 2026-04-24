"""OpenSanctions adapter.

OpenSanctions exposes a FollowTheMoney (FtM) shaped search API at
``https://api.opensanctions.org`` under ``CC BY-NC 4.0``. The
non-commercial clause is why every ``/deepen`` response that includes
OpenSanctions data is flagged with a ``license_notice`` so downstream
consumers (exports, reports) can warn before re-publishing.

Live endpoints (Phase 2):

* ``GET /search/default?q=<query>&schema=<Company|Person>`` — entity/person search.
* ``GET /entities/{entity_id}`` — full FtM entity with nested related parties.

Authentication: ``Authorization: ApiKey <key>``. Gated on
``allow_live=true`` + key. Every response is cached.
"""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://api.opensanctions.org"
_CACHE_NS = "opensanctions"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _schema_for(kind: SearchKind) -> str:
    # OpenSanctions schema taxonomy uses FtM names; "LegalEntity" and
    # "Person" are the broad super-schemas that cover most results.
    return "LegalEntity" if kind == SearchKind.ENTITY else "Person"


class OpenSanctionsAdapter(SourceAdapter):
    id = "opensanctions"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenSanctions",
            homepage="https://www.opensanctions.org/",
            license="CC-BY-NC-4.0",
            attribution="Data from OpenSanctions.org, licensed CC BY-NC 4.0.",
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=True,
            live_available=bool(settings.opensanctions_api_key and settings.allow_live),
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if not self.info.live_available:
            return self._stub_search(query, kind)

        schema = _schema_for(kind)
        payload = await self._get(
            f"/search/default?q={quote(query)}&schema={schema}&limit=10",
            cache_key=f"{_CACHE_NS}/search/{schema}/{_slug(query)}",
        )
        return [self._hit(item, kind) for item in payload.get("results", [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        if not self.info.live_available:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        payload = await self._get(
            f"/entities/{quote(hit_id)}",
            cache_key=f"{_CACHE_NS}/entity/{_slug(hit_id)}",
        )
        return {
            "source_id": self.id,
            "entity_id": hit_id,
            "entity": payload,
        }

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        settings = get_settings()
        assert settings.opensanctions_api_key, "live_available should have been false"

        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}{path}",
                headers={"Authorization": f"ApiKey {settings.opensanctions_api_key}"},
            )
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory (live)
    # ------------------------------------------------------------------

    @staticmethod
    def _hit(item: dict[str, Any], kind: SearchKind) -> SourceHit:
        ftm_id = item.get("id") or ""
        caption = item.get("caption") or "Unknown"
        props = item.get("properties") or {}
        datasets = item.get("datasets") or []
        topics = item.get("topics") or props.get("topics") or []

        summary_bits: list[str] = []
        if topics:
            summary_bits.append("topics: " + ", ".join(topics[:3]))
        if datasets:
            summary_bits.append(f"{len(datasets)} dataset(s)")
        if not summary_bits:
            summary_bits.append(item.get("schema") or "Entity")

        identifiers: dict[str, str] = {"opensanctions_id": ftm_id}
        # OpenSanctions often carries cross-identifiers under properties:
        for key, scheme in (
            ("leiCode", "lei"),
            ("wikidataId", "wikidata_qid"),
            ("registrationNumber", "registration_number"),
        ):
            values = props.get(key)
            if values:
                identifiers[scheme] = values[0] if isinstance(values, list) else values

        return SourceHit(
            source_id="opensanctions",
            hit_id=ftm_id,
            kind=kind,
            name=caption,
            summary=" · ".join(summary_bits),
            identifiers=identifiers,
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="NK-stub-0001",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub OpenSanctions record — set OPENCHECK_ALLOW_LIVE=true + "
                    "OPENSANCTIONS_API_KEY to query live."
                ),
                identifiers={"opensanctions_id": "NK-stub-0001"},
                raw={
                    "id": "NK-stub-0001",
                    "schema": "Company" if kind == SearchKind.ENTITY else "Person",
                    "caption": f"{query} (stub)",
                    "datasets": ["stub"],
                    "topics": [],
                },
            )
        ]
