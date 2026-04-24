"""GLEIF adapter.

GLEIF exposes the Level 1 LEI record (legal entity) and Level 2 relationship
records (direct/ultimate parent) via a JSON:API-formatted REST endpoint at
``https://api.gleif.org/api/v1``. No authentication is required and the
data is CC0.

Live endpoints used (Phase 2):

* ``GET /lei-records?filter[fulltext]=<query>`` — entity search.
* ``GET /lei-records/{lei}`` — Level 1 record for a single LEI.
* ``GET /lei-records/{lei}/direct-parent`` — Level 2 direct parent (optional).
* ``GET /lei-records/{lei}/ultimate-parent`` — Level 2 ultimate parent (optional).

The parent calls return 404 when no relationship is on file; we treat that
as "no parent" rather than an error.
"""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import quote

import httpx

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://api.gleif.org/api/v1"
_CACHE_NS = "gleif"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


class GleifAdapter(SourceAdapter):
    id = "gleif"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="GLEIF — Global Legal Entity Identifier Foundation",
            homepage="https://www.gleif.org/",
            license="CC0-1.0",
            attribution="Contains LEI data from GLEIF, available under CC0 1.0.",
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        if not self.info.live_available:
            return self._stub_search(query)

        payload = await self._get(
            f"/lei-records?filter[fulltext]={quote(query)}&page[size]=10",
            cache_key=f"{_CACHE_NS}/search/{_slug(query)}",
        )
        return [self._entity_hit(item) for item in payload.get("data", [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Return Level 1 record + Level 2 relationships / exceptions for an LEI.

        ``hit_id`` is an LEI (20-char alphanumeric). When a parent endpoint
        404s we probe the matching reporting-exception endpoint so that
        ``NATURAL_PERSONS`` / ``NO_LEI`` / ``NON_CONSOLIDATING`` cases can
        be surfaced in the BODS output (as anonymousEntity / unknownPerson
        bridging statements) instead of silently vanishing.
        """
        if not self.info.live_available:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        lei = hit_id.strip().upper()
        record = await self._get(
            f"/lei-records/{quote(lei)}",
            cache_key=f"{_CACHE_NS}/lei/{lei}",
        )

        direct_parent, direct_exception = await self._parent_or_exception(
            lei, "direct"
        )
        ultimate_parent, ultimate_exception = await self._parent_or_exception(
            lei, "ultimate"
        )

        return {
            "source_id": self.id,
            "lei": lei,
            "record": record.get("data") or record,
            "direct_parent": (direct_parent or {}).get("data"),
            "ultimate_parent": (ultimate_parent or {}).get("data"),
            "direct_parent_exception": (direct_exception or {}).get("data"),
            "ultimate_parent_exception": (ultimate_exception or {}).get("data"),
        }

    async def _parent_or_exception(
        self, lei: str, kind: str
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Return ``(parent, exception)`` — at most one of the pair is non-None.

        GLEIF exposes the exception reason on a sibling endpoint:
        ``/lei-records/{lei}/{kind}-parent-reporting-exception``.
        """
        parent = await self._get_optional(
            f"/lei-records/{quote(lei)}/{kind}-parent",
            cache_key=f"{_CACHE_NS}/lei/{lei}/{kind}-parent",
        )
        if parent is not None:
            return parent, None

        exception = await self._get_optional(
            f"/lei-records/{quote(lei)}/{kind}-parent-reporting-exception",
            cache_key=f"{_CACHE_NS}/lei/{lei}/{kind}-parent-exception",
        )
        return None, exception

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        async with build_client() as client:
            response = await client.get(f"{_API_BASE}{path}")
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    async def _get_optional(
        self, path: str, *, cache_key: str
    ) -> dict[str, Any] | None:
        """Like ``_get`` but returns ``None`` on 404 (no relationship on file)."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        try:
            async with build_client() as client:
                response = await client.get(f"{_API_BASE}{path}")
                if response.status_code == 404:
                    self._cache.put(cache_key, None)
                    return None
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                self._cache.put(cache_key, None)
                return None
            raise

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(item: dict[str, Any]) -> SourceHit:
        attrs = item.get("attributes") or {}
        entity = attrs.get("entity") or {}
        legal_name = (entity.get("legalName") or {}).get("name") or "Unknown entity"
        lei = attrs.get("lei") or item.get("id") or ""
        jurisdiction = entity.get("jurisdiction") or ""
        status = entity.get("status") or ""
        summary_bits = [f"LEI {lei}"]
        if jurisdiction:
            summary_bits.append(jurisdiction)
        if status:
            summary_bits.append(status.lower())
        identifiers: dict[str, str] = {"lei": lei}

        # GLEIF often mirrors a local registry id in registeredAs.
        registered_as = entity.get("registeredAs")
        if registered_as and jurisdiction:
            identifiers[f"registered_as_{jurisdiction.lower()}"] = registered_as

        return SourceHit(
            source_id="gleif",
            hit_id=lei,
            kind=SearchKind.ENTITY,
            name=legal_name,
            summary=" · ".join(summary_bits),
            identifiers=identifiers,
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="STUB000000000000LEI0",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub LEI record — set OPENCHECK_ALLOW_LIVE=true to query live.",
                identifiers={"lei": "STUB000000000000LEI0"},
                raw={"lei": "STUB000000000000LEI0", "legalName": f"{query} (stub)"},
            )
        ]
