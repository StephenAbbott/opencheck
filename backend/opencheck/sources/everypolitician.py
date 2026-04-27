"""EveryPolitician adapter (via OpenSanctions PEPs dataset).

EveryPolitician was revived in 2026 by the OpenSanctions team and is
now maintained through the Poliloom crowdsourcing tool. The data is
served as the OpenSanctions ``peps`` dataset and accessed through the
same API as OpenSanctions itself — we share ``OPENSANCTIONS_API_KEY``.

Live endpoints:

* ``GET /search/peps?q=<query>&schema=Person&limit=10`` — name match,
  scoped to the PEPs dataset.
* ``GET /entities/{entity_id}`` — full FtM Person record with positions.

Coverage caveat (from §4.5 of the project plan): non-hits do not prove
non-PEP status. The frontend surfaces this caveat alongside the
adapter's results.
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
_DATASET = "peps"
_CACHE_NS = "everypolitician"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


class EveryPoliticianAdapter(SourceAdapter):
    id = "everypolitician"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="EveryPolitician",
            homepage="https://everypolitician.org/",
            description=(
                "EveryPolitician is a global database of political "
                "office-holders, from rulers, law-makers to judges and "
                "more."
            ),
            license="CC-BY-NC-4.0",
            attribution=(
                "EveryPolitician data, served via the OpenSanctions PEPs "
                "dataset. Licensed CC BY-NC 4.0."
            ),
            supports=[SearchKind.PERSON],
            requires_api_key=True,  # shares OPENSANCTIONS_API_KEY
            live_available=bool(
                settings.opensanctions_api_key and settings.allow_live
            ),
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.PERSON:
            return []
        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        payload = await self._get(
            f"/search/{_DATASET}?q={quote(query)}&schema=Person&limit=10",
            cache_key=cache_key,
        )
        return [self._hit(item) for item in payload.get("results", [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        cache_key = f"{_CACHE_NS}/entity/{_slug(hit_id)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        payload = await self._get(
            f"/entities/{quote(hit_id)}",
            cache_key=cache_key,
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
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _hit(item: dict[str, Any]) -> SourceHit:
        ftm_id = item.get("id") or ""
        caption = item.get("caption") or "Unknown politician"
        props = item.get("properties") or {}
        positions = props.get("position") or []
        countries = props.get("country") or []

        summary_bits: list[str] = []
        if positions:
            summary_bits.append(positions[0])
        if countries:
            summary_bits.append(countries[0].upper())
        if not summary_bits:
            summary_bits.append("politician")

        identifiers: dict[str, str] = {"opensanctions_id": ftm_id}
        wikidata = props.get("wikidataId")
        if wikidata:
            identifiers["wikidata_qid"] = (
                wikidata[0] if isinstance(wikidata, list) else wikidata
            )

        return SourceHit(
            source_id="everypolitician",
            hit_id=ftm_id,
            kind=SearchKind.PERSON,
            name=caption,
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
                hit_id="poli-stub-0001",
                kind=SearchKind.PERSON,
                name=f"{query} (stub)",
                summary=(
                    "Stub EveryPolitician record — set OPENCHECK_ALLOW_LIVE=true "
                    "+ OPENSANCTIONS_API_KEY to query live."
                ),
                identifiers={"wikidata_qid": "Q0"},
                raw={
                    "id": "poli-stub-0001",
                    "schema": "Person",
                    "caption": f"{query} (stub)",
                    "properties": {"position": ["stub position"], "country": ["xx"]},
                },
            )
        ]
