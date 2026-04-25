"""OpenAleph adapter.

OpenAleph (the open-source successor to the Aleph project operated at
<https://search.openaleph.org/>) exposes a FtM-shaped search API at
``/api/2/``. Collections carry their own license metadata — there is no
single "OpenAleph license". We surface each matching collection's
license on the source card so users know what they're looking at.

Live endpoints (Phase 2):

* ``GET /api/2/entities?q=<query>&filter:schema=<Company|Person>`` — search.
* ``GET /api/2/entities/{entity_id}`` — single FtM entity with properties + collection.
* ``GET /api/2/collections/{collection_id}`` — collection metadata (for license).

API keys are optional and per-user; when set, they unlock additional
collections.
"""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://search.openaleph.org/api/2"
_CACHE_NS = "openaleph"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _schema_for(kind: SearchKind) -> str:
    return "LegalEntity" if kind == SearchKind.ENTITY else "Person"


class OpenAlephAdapter(SourceAdapter):
    id = "openaleph"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenAleph",
            homepage="https://search.openaleph.org/",
            license="per-collection",
            attribution=(
                "Data from OpenAleph — per-collection license; see each "
                "source card for the specific terms."
            ),
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=False,  # keys are optional and per-user
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        schema = _schema_for(kind)
        cache_key = f"{_CACHE_NS}/search/{schema}/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query, kind)

        payload = await self._get(
            f"/entities?q={quote(query)}&filter:schema={schema}&limit=10",
            cache_key=cache_key,
        )
        return [self._hit(item, kind) for item in payload.get("results", [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        cache_key = f"{_CACHE_NS}/entity/{_slug(hit_id)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        entity = await self._get(
            f"/entities/{quote(hit_id)}",
            cache_key=cache_key,
        )

        # Chase the collection so we can surface its license.
        collection_block = entity.get("collection") or {}
        collection_id = collection_block.get("id") or collection_block.get("foreign_id")
        collection: dict[str, Any] | None = None
        if collection_id:
            try:
                collection = await self._get(
                    f"/collections/{quote(str(collection_id))}",
                    cache_key=f"{_CACHE_NS}/collection/{_slug(str(collection_id))}",
                )
            except Exception:  # noqa: BLE001
                # Some collections are private; a 403 shouldn't block the fetch.
                collection = None

        return {
            "source_id": self.id,
            "entity_id": hit_id,
            "entity": entity,
            "collection": collection,
        }

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        settings = get_settings()
        headers: dict[str, str] = {}
        if settings.openaleph_api_key:
            headers["Authorization"] = f"ApiKey {settings.openaleph_api_key}"

        async with build_client() as client:
            response = await client.get(f"{_API_BASE}{path}", headers=headers)
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory (live)
    # ------------------------------------------------------------------

    @staticmethod
    def _hit(item: dict[str, Any], kind: SearchKind) -> SourceHit:
        entity_id = item.get("id") or ""
        props = item.get("properties") or {}
        name = (
            (props.get("name") or [None])[0]
            or item.get("caption")
            or "Unknown entity"
        )
        collection = item.get("collection") or {}
        collection_label = collection.get("label") or collection.get("foreign_id") or ""

        summary_bits: list[str] = []
        if collection_label:
            summary_bits.append(f"collection: {collection_label}")
        schema = item.get("schema")
        if schema:
            summary_bits.append(schema)
        if not summary_bits:
            summary_bits.append("OpenAleph entity")

        identifiers: dict[str, str] = {"aleph_id": entity_id}
        for key, scheme in (
            ("leiCode", "lei"),
            ("wikidataId", "wikidata_qid"),
            ("registrationNumber", "registration_number"),
        ):
            values = props.get(key)
            if values:
                identifiers[scheme] = values[0] if isinstance(values, list) else values

        return SourceHit(
            source_id="openaleph",
            hit_id=entity_id,
            kind=kind,
            name=name,
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
                hit_id="aleph-stub-0001",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub OpenAleph record — set OPENCHECK_ALLOW_LIVE=true to query live. "
                    "Per-collection licensing will appear on live source cards."
                ),
                identifiers={"aleph_id": "aleph-stub-0001"},
                raw={
                    "id": "aleph-stub-0001",
                    "schema": "Company" if kind == SearchKind.ENTITY else "Person",
                    "properties": {"name": [f"{query} (stub)"]},
                    "collection": {"label": "Stub Collection", "foreign_id": "stub"},
                },
            )
        ]
