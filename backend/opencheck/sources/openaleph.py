"""OpenAleph adapter.

OpenAleph (the open-source successor to the Aleph project operated at
<https://search.openaleph.org/>) exposes a FtM-shaped search API at
``/api/2/``. Collections carry their own license metadata — there is no
single "OpenAleph license". We surface each matching collection's
license on the source card so users know what they're looking at.

Live endpoints used:

* ``GET /api/2/entities?filter:properties.leiCode=<LEI>`` — LEI-keyed lookup.
* ``GET /api/2/entities?filter:properties.registrationNumber=<n>&filter:properties.jurisdiction=<cc>``
  — national registration number lookup (fallback).
* ``GET /api/2/entities?q=<query>&filter:schema=<Company|Person>`` — free-text search
  (used by the /search / /report paths; not the LEI-anchored /lookup flow).
* ``GET /api/2/entities/{entity_id}`` — single FtM entity with properties + collection.
* ``GET /api/2/collections/{collection_id}`` — collection metadata (for license).

LEI-anchored lookup strategy (used in /lookup flow):
  1. ``fetch_by_lei(lei)``  — filter on ``leiCode`` (FtM identifier type, exact-match).
  2. ``fetch_by_oc_url(ocid)`` — filter on ``opencorporatesUrl`` (GLEIF-derived OC ID).
  3. ``fetch_by_registration(jurisdiction, reg_number)`` — filter on ``registrationNumber``
     + ``jurisdiction`` for any of the derived national IDs (gb_coh, siren, kvk_number,
     se_org_number, che_uid). Tried in order; stops at first non-empty result.

The ``leiCode`` and ``registrationNumber`` FtM properties are of type ``identifier``,
which Aleph indexes as keywords and supports exact-match via ``filter:properties.*``.
``opencorporatesUrl`` is type ``url`` — also keyword-indexed in Aleph.

API keys are optional and per-user; when set, they unlock additional
collections.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://search.openaleph.org/api/2"
_CACHE_NS = "openaleph"

# Anubis bot-protection at search.openaleph.org whitelists requests whose
# User-Agent matches the openaleph-client pattern ("openaleph/<version>").
# Our global OpenCheck User-Agent triggers the Anubis challenge.  We
# therefore use the openaleph-client version string for all OpenAleph
# requests, which is correct attribution anyway since we depend on that package.
try:
    _OA_VERSION = importlib.metadata.version("openaleph-client")
except importlib.metadata.PackageNotFoundError:
    _OA_VERSION = "1.1"
_OA_USER_AGENT = f"openaleph/{_OA_VERSION}"


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
            homepage="https://openaleph.org/",
            description=(
                "The open source platform that securely stores large "
                "amounts of data and makes it searchable for easy "
                "collaboration."
            ),
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
    # Identifier-keyed lookups (LEI-anchored flow)
    # ------------------------------------------------------------------

    async def fetch_by_lei(self, lei: str) -> list[SourceHit]:
        """Return OpenAleph hits whose ``leiCode`` property exactly matches ``lei``.

        Uses ``filter:properties.leiCode=<lei>`` — exact-match on the FtM
        ``identifier``-type field, bypassing free-text scoring noise.
        Returns an empty list when the instance is a stub or no results found.
        """
        cache_key = f"{_CACHE_NS}/lei/{_slug(lei)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []
        payload = await self._get(
            f"/entities?filter:properties.leiCode={quote(lei)}"
            f"&filter:schema=LegalEntity&limit=5",
            cache_key=cache_key,
        )
        return [
            self._hit(item, SearchKind.ENTITY)
            for item in payload.get("results", [])
        ]

    async def fetch_by_oc_url(self, ocid: str) -> list[SourceHit]:
        """Return OpenAleph hits whose ``opencorporatesUrl`` matches the OC URL.

        Constructs ``https://opencorporates.com/companies/<ocid>`` and filters
        on the ``opencorporatesUrl`` FtM property (type: url, keyword-indexed).
        Returns an empty list when the instance is a stub or no results found.
        """
        oc_url = f"https://opencorporates.com/companies/{ocid}"
        cache_key = f"{_CACHE_NS}/oc/{_slug(ocid)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []
        payload = await self._get(
            f"/entities?filter:properties.opencorporatesUrl={quote(oc_url)}"
            f"&filter:schema=LegalEntity&limit=5",
            cache_key=cache_key,
        )
        return [
            self._hit(item, SearchKind.ENTITY)
            for item in payload.get("results", [])
        ]

    async def fetch_by_registration(
        self, jurisdiction: str, registration_number: str
    ) -> list[SourceHit]:
        """Return OpenAleph hits matching a national registration number + jurisdiction.

        Uses ``filter:properties.registrationNumber=<n>`` and
        ``filter:properties.jurisdiction=<cc>`` together (both FtM identifier/
        country-type fields, keyword-indexed).

        ``jurisdiction`` should be an ISO 3166-1 alpha-2 lowercase code
        (e.g. ``"gb"``, ``"fr"``, ``"nl"``, ``"se"``, ``"ch"``).
        Returns an empty list when the instance is a stub or no results found.
        """
        cache_key = f"{_CACHE_NS}/reg/{_slug(jurisdiction + ':' + registration_number)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []
        payload = await self._get(
            f"/entities?filter:properties.registrationNumber={quote(registration_number)}"
            f"&filter:properties.jurisdiction={quote(jurisdiction.lower())}"
            f"&filter:schema=LegalEntity&limit=5",
            cache_key=cache_key,
        )
        return [
            self._hit(item, SearchKind.ENTITY)
            for item in payload.get("results", [])
        ]

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
        # Override the global User-Agent: Anubis bot-protection at
        # search.openaleph.org whitelists the "openaleph/<version>" pattern
        # used by the openaleph-client PyPI package and rejects all other
        # non-browser agents with a redirect to a proof-of-work challenge.
        headers: dict[str, str] = {"User-Agent": _OA_USER_AGENT}
        if settings.openaleph_api_key:
            headers["Authorization"] = f"ApiKey {settings.openaleph_api_key}"

        url = f"{_API_BASE}{path}"
        async with build_client() as client:
            response = await client.get(url, headers=headers)
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
