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
* ``GET /api/2/entities/{entity_id}/mentions`` — Document-family entities whose
  indexed text mentions the entity's name variants (OpenAleph 5.3, the inverse
  of the percolation/Screening feature). Used for informational enrichment.
* ``POST /api/2/match`` — similar-entity matching for a caller-supplied FtM
  entity (identifier-aware: leiCode / registrationNumber / jurisdiction
  participate, unlike free-text ``q=`` search). Tried as a precision upgrade
  before the free-text name fallback. Requires an API key on the flagship
  instance — the edge returns 405 for anonymous POSTs to this path even
  though the app route allows them.

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
import json
import re
import unicodedata
from typing import Any
from urllib.parse import quote

import httpx

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..matching import canonical_identifier, canonical_url
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://search.openaleph.org/api/2"
_CACHE_NS = "openaleph"

# How many collections to surface in the mentions breakdown (issue #23).
_MENTION_FACET_SIZE = 10

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


# Aleph parses the ``q`` parameter as an Elasticsearch query_string (Lucene
# syntax), so unbalanced double quotes or stray backslashes in a legal name
# make the parser return HTTP 500 — e.g. Rosneft's name carries nested ASCII
# quotes: Публичное акционерное общество "Нефтяная компания "Роснефть". Strip
# those before searching. Any other reserved metacharacter that still trips the
# parser is absorbed by the tolerant 5xx handling in ``_get_tolerant``.
_Q_UNSAFE = re.compile(r"[\"“”\\]")


def _sanitise_q(text: str) -> str:
    """Make a free-text query safe for Aleph's query_string parser."""
    return " ".join(_Q_UNSAFE.sub(" ", text).split()).strip()


def _schema_for(kind: SearchKind) -> str:
    return "LegalEntity" if kind == SearchKind.ENTITY else "Person"


# FtM "names" group — the properties a legitimate name match may live in.
_NAME_PROPS = ("name", "alias", "previousName")

_NAME_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)


def _normalise_name(text: str) -> str:
    """Casefold + strip punctuation/diacritics + collapse whitespace.

    Mirrors ``reconcile._normalise_name`` — same discipline, same reason: names
    are compared, never scored.
    """
    if not text:
        return ""
    folded = unicodedata.normalize("NFKD", text)
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    folded = _NAME_PUNCT.sub(" ", folded.casefold())
    return " ".join(folded.split())


def _bears_name(item: dict[str, Any], wanted: str) -> bool:
    """True when the hit itself carries ``wanted`` among its FtM names.

    The honest test for a *name* search: a hit returned for the query "Canada
    Basketball" must actually be called Canada Basketball. BM25 relevance is
    not a match confidence — it ranks within a corpus, so a lone weak result
    is still rank #1 (Canada Basketball's only hit, an unrelated Honduran
    cleaning company, scored 6.6 and topped its result set), and a strong
    score can still be the wrong company ("The Foundation Foundation" returns
    GB Group plc at 77). Neither an absolute nor a relative score threshold
    separates those from Ericsson's genuine hits (81–114) — but every genuine
    hit *bears the name*, and none of the false positives do.
    """
    if not wanted:
        return False
    props = item.get("properties") or {}
    for prop in _NAME_PROPS:
        values = props.get(prop) or []
        if isinstance(values, str):
            values = [values]
        for value in values:
            if _normalise_name(str(value)) == wanted:
                return True
    # Some collections put the display name only in the caption.
    return _normalise_name(str(item.get("caption") or "")) == wanted


class OpenAlephAdapter(SourceAdapter):
    id = "openaleph"

    # The lookup cascade tries up to seven sequential queries (LEI → OC URL
    # → five registration numbers → name) — allow more than one HTTP budget.
    lookup_timeout_s = 60.0

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

        q = _sanitise_q(query)
        if not q:
            return []
        payload = await self._get_tolerant(
            f"/entities?q={quote(q)}&filter:schema={schema}&limit=10",
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

    async def fetch_by_name(self, legal_name: str) -> list[SourceHit]:
        """Return OpenAleph hits that actually *bear* ``legal_name``.

        Last-resort fallback when every identifier-keyed strategy — including
        the FtM ``/match`` step — comes back empty. ``/match`` needs
        ``OPENALEPH_API_KEY`` (the flagship 405s anonymous POSTs), so on a
        keyless deployment this is the only name-based path; it stays.

        **Gated on name equivalence** (issue #21): Aleph's free-text ``q=``
        is BM25-ranked, and a rank is not a match — the query "Canada
        Basketball" returned exactly one hit, an unrelated Honduran cleaning
        company, which was therefore also the top-scoring hit. Hits are kept
        only when one of their own FtM names (``name`` / ``alias`` /
        ``previousName``, or the caption) normalises equal to the subject's
        legal name. Verified against the live index: this drops the Canada
        Basketball and "The Foundation Foundation" false positives while
        keeping every genuine Ericsson AB hit (State Aid Transparency,
        OpenTender Sweden, European Defence Fund).
        """
        cache_key = f"{_CACHE_NS}/name/{_slug(legal_name)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []
        q = _sanitise_q(legal_name)
        if not q:
            return []
        payload = await self._get_tolerant(
            f"/entities?q={quote(q)}&filter:schema=LegalEntity&limit=5",
            cache_key=cache_key,
        )
        wanted = _normalise_name(legal_name)
        return [
            self._hit(item, SearchKind.ENTITY)
            for item in payload.get("results", [])
            if _bears_name(item, wanted)
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
    # FtM entity matching (POST /api/2/match)
    # ------------------------------------------------------------------

    # Non-corroborated matches must score at least this fraction of the top
    # hit's score to be surfaced. Relative rather than absolute because
    # FtM/BM25 scores vary with name length and term rarity — an absolute
    # threshold that works for "BP P.L.C." misfires for long Cyrillic names.
    _MATCH_RELATIVE_CUTOFF = 0.25

    # FtM identifier properties checked for subject↔hit corroboration.
    _MATCH_CORROBORATING_PROPS = ("leiCode", "registrationNumber", "opencorporatesUrl")

    @classmethod
    def _match_corroborated(
        cls, subject_props: dict[str, Any], hit_props: dict[str, Any]
    ) -> bool:
        """True when the hit's own properties share an identifier with the
        subject — the strongest signal that both describe the same entity.

        Values are canonicalised the way OpenSanctions' ftmg does before it
        treats them as a shared key: leiCode / registrationNumber via
        ``canonical_identifier`` (StrictFormat + the 7-char minimum, so a
        coincidental short code can't corroborate), opencorporatesUrl via
        ``canonical_url`` (trailing-slash / scheme differences don't defeat
        the match).
        """
        for prop in cls._MATCH_CORROBORATING_PROPS:
            normalise = canonical_url if prop == "opencorporatesUrl" else canonical_identifier
            subject_values = {
                norm
                for v in (subject_props.get(prop) or [])
                if (norm := normalise(v)) is not None
            }
            hit_values = {
                norm
                for v in (hit_props.get(prop) or [])
                if (norm := normalise(v)) is not None
            }
            if subject_values & hit_values:
                return True
        return False

    async def match_entity(
        self, ftm_entity: dict[str, Any], limit: int = 5
    ) -> list[SourceHit]:
        """Return entities similar to a caller-supplied FtM entity.

        POSTs the entity (``{"schema", "properties"}``) to ``/api/2/match``.
        Unlike the free-text ``q=`` fallback, FtM matching is identifier-
        aware — leiCode, registrationNumber, jurisdiction and dates all
        participate alongside fuzzy name features — so it is tried first
        when every identifier-keyed strategy comes back empty.

        Acceptance gating: hits whose own properties corroborate one of the
        subject's identifiers are always kept and ranked first
        (``raw["identifier_corroborated"] = True``); other hits are kept
        only when scoring ≥ ``_MATCH_RELATIVE_CUTOFF`` of the top score.

        Verified live 2026-07-02: the flagship edge 405s anonymous POSTs
        to this path, so an API key is required; without one this method
        short-circuits to ``[]``. Any HTTP failure also degrades to ``[]``
        (the free-text fallback still runs after it).
        """
        settings = get_settings()
        if not settings.openaleph_api_key:
            return []
        cache_key = (
            f"{_CACHE_NS}/match/{_slug(json.dumps(ftm_entity, sort_keys=True))}"
        )
        if not self.info.live_available and not self._cache.has(cache_key):
            return []

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            payload = cached[0]
        else:
            headers = {
                "User-Agent": _OA_USER_AGENT,
                "Authorization": f"ApiKey {settings.openaleph_api_key}",
            }
            try:
                async with build_client() as client:
                    response = await client.post(
                        f"{_API_BASE}/match",
                        params={"limit": limit},
                        json=ftm_entity,
                        headers=headers,
                    )
                    if not response.is_success:
                        return []
                    payload = response.json()
            except Exception:  # noqa: BLE001
                return []
            self._cache.put(cache_key, payload)

        results = (payload.get("results") or [])[:limit]
        subject_props = ftm_entity.get("properties") or {}
        top_score = max(
            (item.get("score") or 0.0 for item in results), default=0.0
        )

        corroborated_hits: list[SourceHit] = []
        other_hits: list[SourceHit] = []
        for item in results:
            score = item.get("score")
            corroborated = self._match_corroborated(
                subject_props, item.get("properties") or {}
            )
            if not corroborated and isinstance(score, (int, float)):
                if top_score and score < top_score * self._MATCH_RELATIVE_CUTOFF:
                    continue
            hit = self._hit(item, SearchKind.ENTITY)
            hit.raw["identifier_corroborated"] = corroborated
            if isinstance(score, (int, float)):
                hit.raw["match_score"] = score
                hit.summary = f"{hit.summary} · FtM match score {score:.0f}"
            if corroborated:
                hit.summary = f"{hit.summary} · identifier corroborated"
                corroborated_hits.append(hit)
            else:
                other_hits.append(hit)
        return corroborated_hits + other_hits

    # ------------------------------------------------------------------
    # Mentions enrichment (OpenAleph 5.3 — reverse percolation)
    # ------------------------------------------------------------------

    async def fetch_mentions(
        self, entity_id: str, limit: int = 5
    ) -> dict[str, Any] | None:
        """Return documents in the instance whose text mentions this entity.

        Wraps the 5.3 ``/entities/{id}/mentions`` endpoint — the inverse of
        the percolation/Screening feature: given a named FtM entity, find
        Document-family entities (leaks, court records, news archives…)
        containing any of its name variants as a phrase.

        Purely informational enrichment: mentions are name-derived, so they
        must never be treated as identifier corroboration. Any failure
        (pre-5.3 instance → 404, timeout, auth) degrades to ``None`` rather
        than surfacing an error card.

        Returns ``{total, documents, collections}``. ``collections`` is a
        breakdown of **which archives mention the entity, with exact counts
        across all ``total`` mentions** — taken from the ``collection_id``
        facet, not counted from the sampled documents (issue #23). This
        matters: the endpoint returns ``total`` alongside only a page of
        documents, so counting categories over the sample would misreport the
        rest (Shell: 5 sampled of 61). The ``category`` facet is empty on the
        flagship, and per-document categories are near-uniformly "library" —
        collection labels ("Epstein Estate documents from the US Oversight
        Committee") are both exact and far more informative.
        """
        cache_key = f"{_CACHE_NS}/mentions/{_slug(entity_id)}/{limit}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return None
        try:
            payload = await self._get(
                f"/entities/{quote(entity_id)}/mentions"
                f"?limit={limit}&facet=collection_id&facet_size={_MENTION_FACET_SIZE}",
                cache_key=cache_key,
            )
        except Exception:  # noqa: BLE001
            return None

        total = payload.get("total") or 0
        documents: list[dict[str, str]] = []
        for item in (payload.get("results") or [])[:limit]:
            collection = item.get("collection") or {}
            documents.append(
                {
                    "title": item.get("caption") or "",
                    "collection": collection.get("label")
                    or collection.get("foreign_id")
                    or "",
                    "category": collection.get("category") or "",
                    "url": (item.get("links") or {}).get("ui") or "",
                }
            )

        collections: list[dict[str, Any]] = []
        facet = (payload.get("facets") or {}).get("collection_id") or {}
        for value in (facet.get("values") or [])[:_MENTION_FACET_SIZE]:
            label = value.get("label") or value.get("id") or ""
            count = value.get("count") or 0
            if label and count:
                collections.append({"label": str(label), "count": int(count)})

        return {"total": int(total), "documents": documents, "collections": collections}

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

    async def _get_tolerant(self, path: str, *, cache_key: str) -> dict[str, Any]:
        """Like ``_get`` but treats a 4xx/5xx from a *best-effort free-text
        search* as 'no results' instead of surfacing an error card. The Aleph
        query_string parser 500s on some legal names (unbalanced quotes etc.),
        and a name fallback should degrade quietly rather than fail the source."""
        try:
            return await self._get(path, cache_key=cache_key)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code in (400, 422) or code >= 500:
                return {"results": []}
            raise

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
