"""OpenSanctions adapter.

OpenSanctions exposes a FollowTheMoney (FtM) shaped search API at
``https://api.opensanctions.org`` under ``CC BY-NC 4.0``. The
non-commercial clause is why every ``/deepen`` response that includes
OpenSanctions data is flagged with a ``license_notice`` so downstream
consumers (exports, reports) can warn before re-publishing.

Live endpoints (Phase 2):

* ``GET /search/default?q=<query>&schema=<Company|Person>&topic=...`` — entity/person search,
  scoped to risk-relevant topics only (OpenSanctions' ``target_topics``:
  sanctions, export controls, debarment, PEPs, criminality, reg. actions).
* ``GET /entities/{entity_id}`` — full FtM entity with nested related parties.

Authentication: ``Authorization: ApiKey <key>``. Gated on
``allow_live=true`` + key. Every response is cached.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import httpx

from ..cache import Cache
from ..config import get_settings
from ..findings import finding_opensanctions
from ..http import build_client, sanitize_name_query
from ..topics import topic_list
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://api.opensanctions.org"
_CACHE_NS = "opensanctions"

# Risk-relevant topics for screening. Purely-descriptive topics like
# ``corp.public`` and ``corp.listed`` are intentionally excluded — entities
# that appear in OpenSanctions only because they are referenced in
# enrichment/KYB datasets (e.g. GLEIF, GEM, ESMA) should not surface as hits
# in a risk-screening search.
#
# This mirrors OpenSanctions' own ``target_topics`` — their curated set of
# "this is a risk flag, not a description" topics — rather than a hand-picked
# subset. Keeping the two in step means new datasets are picked up
# automatically instead of being silently filtered out. The previous
# nine-topic subset dropped ``us_bis_mieu`` entirely (every topic-bearing
# entity in it carries only ``export.control``) and hid the 11 entities in
# ``sa_pcct_terrorism_list`` tagged ``crime.terror`` without ``sanction``.
#
# Source of truth: https://data.opensanctions.org/meta/model.json → ``target_topics``
# Human-readable: https://www.opensanctions.org/docs/topics/
# Verified against the model published 2026-11-06 (30 topics: ``corp.clone``
# and ``geo.risk`` joined the 2026-08-05 list of 28, unannounced). Both are
# fetched and raise no signal — see ``risk._INFORMATIONAL_TOPICS``.
_RISK_TOPICS: tuple[str, ...] = (
    # Sanctions and adjacent designations.
    "sanction",
    "sanction.linked",
    "sanction.control",
    "sanction.counter",
    # Export controls and trade restrictions.
    "export.control",
    "export.control.linked",
    "export.risk",
    # Investment restrictions.
    "invest.ban",
    "invest.risk",
    # Procurement exclusion and corporate disqualification.
    "debarment",
    "corp.disqual",
    # A firm impersonating a real one (the FCA Warning List's clone firms);
    # applies to the impersonator only, never the firm it copies.
    "corp.clone",
    # Political exposure.
    "role.pep",
    "role.rca",
    "role.oligarch",
    # Criminality.
    "crime",
    "crime.boss",
    "crime.fin",
    "crime.fraud",
    "crime.terror",
    "crime.theft",
    "crime.traffick",
    "crime.war",
    "wanted",
    # Maritime risk.
    "mare.shadow",
    "mare.detained",
    # Regulatory action and residual watchlisting.
    "reg.action",
    "reg.warn",
    "poi",
    # Designations of a whole place (FtM's ``Territory`` schema, 2 Sept 2026).
    # No published dataset sets it yet, and a Territory is neither a
    # LegalEntity nor a Person, so OpenCheck's schema-filtered search cannot
    # return one; mirrored so the scope stays OpenSanctions' own list.
    "geo.risk",
)
_TOPIC_PARAMS = "&".join(f"topic={t}" for t in _RISK_TOPICS)

# The topic scope shapes the *request* but is not part of the cache key below
# unless we put it there — so widening ``_RISK_TOPICS`` would otherwise keep
# serving responses cached under the old, narrower scope indefinitely
# (``_get`` reads the cache with no ``max_age_days``). Fingerprinting the
# scope into the key makes this and every future change self-invalidating.
# Entries under a superseded fingerprint are orphaned and can be deleted from
# ``data/cache/live/opensanctions/search/`` at leisure.
_TOPIC_FINGERPRINT = hashlib.sha256(_TOPIC_PARAMS.encode("utf-8")).hexdigest()[:8]

# The fingerprint above self-invalidates when *we* change the topic scope. It
# says nothing about the data moving under us, and OpenSanctions' coverage
# expands on their schedule, not ours: on 2026-09-15 `eu_journal_sanctions`
# went from a few thousand entities to roughly 8,000 — vessels, export-control
# listings and sectorally-restricted companies that `eu_fsf` excludes by scope
# — and entities already in `eu_fsf` gained a second source in `datasets`.
# Nothing in the cache key changes for either, so an entry written the day
# before would otherwise be served indefinitely: `get_payload` does not expire
# anything unless asked. A screening answer that is silently a month old is
# the failure mode this whole adapter exists to avoid, so both the search and
# the entity paths cap the age of a live-tier entry.
#
# Seven days is chosen against how the upstream publishes: daily builds, so a
# week bounds the staleness at roughly seven builds while still absorbing the
# repeat lookups a single session makes. Demo fixtures are never expired
# (`Cache.get_payload` only ages the live tier), so the offline demo path is
# untouched.
_MAX_CACHE_AGE_DAYS = 7.0


# ---------------------------------------------------------------------------
# Batched ``/match`` for related-party person screening (the batched-/match
# ticket, 10 Nov 2026)
# ---------------------------------------------------------------------------
#
# ``cross_check`` used to send one ``GET /search`` per related person. It now
# sends their names, with the birth year and nationalities the bundle holds,
# as FollowTheMoney examples in one ``POST /match/default`` — up to
# ``MATCH_BATCH_SIZE`` queries per request, OpenSanctions' own limit.
#
# Billing (OpenSanctions' metering FAQ, read 10 Nov 2026): ``/match`` is
# charged one credit per *query* in the batch, ``/search`` one per request.
# Batching therefore saves round trips and the per-key request quota, not
# credits — a lookup screening 25 people spends 25 credits either way.
#
# OpenSanctions' score orders the candidates and decides nothing else:
# ``cutoff=0`` returns every scored candidate (yente drops results at or
# below the cutoff, 0.5 by default, before we would see them), and the
# ``match`` flag is ignored. Which candidate becomes a signal is still
# decided by ``cross_check``'s own gates — 0.88 similarity, the single-token
# person guard, birth-year and patronymic conflicts — exactly as on the
# ``/search`` path. ``match: true`` setting confidence is the ICIJ lesson of
# Phase 237.
MATCH_BATCH_SIZE = 100
_MATCH_ALGORITHM = "logic-v2"
_MATCH_LIMIT = 10
_MATCH_PARAMS = (
    f"algorithm={_MATCH_ALGORITHM}&limit={_MATCH_LIMIT}"
    f"&threshold=0.7&cutoff=0.0&{_TOPIC_PARAMS}"
)
# The request parameters are part of every cached answer's meaning, so they
# are in its key — the ``_TOPIC_FINGERPRINT`` rule.
_MATCH_FINGERPRINT = hashlib.sha256(_MATCH_PARAMS.encode("utf-8")).hexdigest()[:8]


def person_match_example(
    name: str, birth_year: int | None = None, nationalities: tuple[str, ...] = ()
) -> dict[str, Any] | None:
    """The FollowTheMoney ``Person`` example ``/match`` scores against.

    The name goes as JSON, never through the Lucene parser, so the
    reserved-character bug class of ``/search`` (quotes, ``A/S``, a dangling
    ``+``) cannot occur on this path. The birth year and nationalities are
    the ones ``cross_check`` reads for its own corroboration; a value that is
    not a four-digit year or an alpha-2 code is left out rather than sent in
    a shape the matcher would misread.
    """
    name = " ".join(str(name or "").split())
    if not name:
        return None
    props: dict[str, list[str]] = {"name": [name]}
    if birth_year is not None and 1000 <= int(birth_year) <= 9999:
        props["birthDate"] = [str(int(birth_year))]
    codes = sorted(
        {
            str(c).strip().lower()
            for c in nationalities
            if re.fullmatch(r"[A-Za-z]{2}", str(c).strip())
        }
    )
    if codes:
        props["nationality"] = codes
    return {"schema": "Person", "properties": props}


def _query_status_error(status: int, request: httpx.Request) -> httpx.HTTPStatusError:
    """A per-query failure inside a 200 batch, shaped like the HTTP error it
    stands for so ``risk.classify_degradation_reason`` words it the same way
    (a 429 is ``rate_limited``, anything else ``upstream_error``)."""
    return httpx.HTTPStatusError(
        f"OpenSanctions /match answered HTTP {status} for one query",
        request=request,
        response=httpx.Response(status, request=request),
    )


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _schema_for(kind: SearchKind) -> str:
    # OpenSanctions schema taxonomy uses FtM names; "LegalEntity" and
    # "Person" are the broad super-schemas that cover most results.
    return "LegalEntity" if kind == SearchKind.ENTITY else "Person"


class OpenSanctionsAdapter(SourceAdapter):
    id = "opensanctions"

    # OpenSanctions' *entity records* for ordinary companies are its GLEIF
    # and UK PSC dataset mirrors (a Novo Nordisk record carries GLEIF's LEI
    # and creation date verbatim). Its sanctions, PEP and debarment
    # *listings* are original — lineage discounts corroboration of the
    # record, never the finding (see sources/lineage.py).
    derived_from = frozenset({"gleif", "companies_house"})

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenSanctions",
            homepage="https://www.opensanctions.org/",
            description=(
                "Sanctions lists, PEPs, debarments, and regulatory actions "
                "from the OpenSanctions open-source database."
            ),
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
        # The /search endpoint sits on a Lucene query parser: an unbalanced
        # double quote (e.g. the ASCII gershayim in Israeli names, בע"מ) is
        # a 400 — sanitise before it reaches the URL. Identity for clean
        # names, so their cache keys are unchanged.
        query = sanitize_name_query(query)
        if not query:
            return []
        schema = _schema_for(kind)
        cache_key = f"{_CACHE_NS}/search/{schema}/{_TOPIC_FINGERPRINT}/{_slug(query)}"
        # A demo fixture for this query overrides the live_available check
        # so the app can demo offline.
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query, kind)

        payload = await self._get(
            f"/search/default?q={quote(query)}&schema={schema}&limit=10&{_TOPIC_PARAMS}",
            cache_key=cache_key,
        )
        return [self._hit(item, kind) for item in payload.get("results", [])]

    # The advanced-search-filters ticket: OpenSanctions person records carry
    # ``birthDate`` and ``nationality`` / ``citizenship`` / ``country``, so the
    # person filters are applied to its own data. A record with no birth date
    # or no country is not a match — a filter that kept them would read as
    # "born in 1952" for people whose birth date nobody published.
    search_filters = frozenset(("nationality", "birth_year"))

    async def search_filtered(
        self, query: str, kind: SearchKind, filters: Any
    ) -> list[SourceHit]:
        from .. import search_filters as sf

        if kind != SearchKind.PERSON or not (filters.nationality or filters.birth_year):
            return await self.search(query, kind)
        query = sanitize_name_query(query)
        if not query:
            return []
        schema = _schema_for(kind)
        cache_key = f"{_CACHE_NS}/search50/{schema}/{_TOPIC_FINGERPRINT}/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []
        payload = await self._get(
            f"/search/default?q={quote(query)}&schema={schema}&limit=50&{_TOPIC_PARAMS}",
            cache_key=cache_key,
        )
        kept: list[SourceHit] = []
        for item in payload.get("results", []):
            props = item.get("properties") or {}
            if filters.birth_year and not sf.birth_year_matches(
                props.get("birthDate") or [], filters.birth_year
            ):
                continue
            if filters.nationality and not sf.nationality_matches(
                (props.get("nationality") or [])
                + (props.get("citizenship") or [])
                + (props.get("country") or []),
                filters.nationality,
            ):
                continue
            kept.append(self._hit(item, kind))
        return kept[:10]

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
    # Batched /match (persons)
    # ------------------------------------------------------------------

    async def match_persons(
        self, queries: Mapping[str, dict[str, Any]]
    ) -> dict[str, list[SourceHit] | BaseException]:
        """Screen many people in as few ``POST /match/default`` calls as
        possible.

        ``queries`` maps a caller-chosen key to ``{name, birth_year,
        nationalities}``. The answer maps every key to its hits — the same
        ``SourceHit`` shape ``search`` returns — or to the exception that
        stopped that query, so one failed chunk or one refused query is a
        failure for those names only and never passes for "screened, nothing
        found". Never raises.

        Each query's answer is cached on its own (the example and the request
        parameters are the key) and aged out after ``_MAX_CACHE_AGE_DAYS``
        exactly as ``search`` is, so a repeat lookup sends only the names it
        has not seen this week.
        """
        settings = get_settings()
        can_refetch = bool(settings.opensanctions_api_key and settings.allow_live)
        live = self.info.live_available
        out: dict[str, list[SourceHit] | BaseException] = {}
        pending: dict[str, tuple[dict[str, Any], str]] = {}
        for key, q in queries.items():
            example = person_match_example(
                q.get("name") or "",
                q.get("birth_year"),
                tuple(q.get("nationalities") or ()),
            )
            if example is None:
                out[key] = []
                continue
            canonical = json.dumps(example, sort_keys=True, ensure_ascii=False)
            cache_key = f"{_CACHE_NS}/match/Person/{_MATCH_FINGERPRINT}/{_slug(canonical)}"
            cached = self._cache.get_payload(
                cache_key, max_age_days=_MAX_CACHE_AGE_DAYS if can_refetch else None
            )
            if cached is not None:
                out[key] = self._match_hits(cached[0])
            elif not live:
                out[key] = self._stub_search(example["properties"]["name"][0], SearchKind.PERSON)
            else:
                pending[key] = (example, cache_key)

        keys = list(pending)
        for start in range(0, len(keys), MATCH_BATCH_SIZE):
            chunk = keys[start : start + MATCH_BATCH_SIZE]
            try:
                responses, request = await self._post_match(
                    {k: pending[k][0] for k in chunk}
                )
            except Exception as exc:  # noqa: BLE001 — recorded per query
                for k in chunk:
                    out[k] = exc
                continue
            for k in chunk:
                answer = responses.get(k)
                status = (answer or {}).get("status", 200) if isinstance(answer, dict) else None
                if not isinstance(answer, dict) or status != 200:
                    out[k] = _query_status_error(int(status or 502), request)
                    continue
                self._cache.put(pending[k][1], answer)
                out[k] = self._match_hits(answer)
        return out

    async def _post_match(
        self, queries: dict[str, dict[str, Any]]
    ) -> tuple[dict[str, Any], httpx.Request]:
        settings = get_settings()
        assert settings.opensanctions_api_key, "live_available should have been false"
        async with build_client() as client:
            response = await client.post(
                f"{_API_BASE}/match/default?{_MATCH_PARAMS}",
                json={"queries": queries},
                headers={"Authorization": f"ApiKey {settings.opensanctions_api_key}"},
            )
            response.raise_for_status()
            payload = response.json()
        responses = payload.get("responses") if isinstance(payload, dict) else None
        if not isinstance(responses, dict):
            raise ValueError("OpenSanctions /match answered without a responses object")
        return responses, response.request

    def _match_hits(self, answer: dict[str, Any]) -> list[SourceHit]:
        return [
            self._hit(item, SearchKind.PERSON)
            for item in answer.get("results") or []
            if isinstance(item, dict)
        ]

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any]:
        settings = get_settings()
        # Only expire what we can replace. Without a key (or with live calls
        # off) an aged-out entry cannot be re-fetched, and treating it as a
        # miss would walk straight into the assertion below — so in that
        # configuration a stale entry is still the best answer available, and
        # the provenance recorder reports it as cached either way.
        can_refetch = bool(settings.opensanctions_api_key and settings.allow_live)
        cached = self._cache.get_payload(
            cache_key, max_age_days=_MAX_CACHE_AGE_DAYS if can_refetch else None
        )
        if cached is not None:
            return cached[0]

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
            # Labels, not slugs: this line reached the reader as
            # "topics: corp.disqual, debarment, export.control". See
            # ``opencheck.topics`` for why translating is not judging.
            summary_bits.append("topics: " + ", ".join(topic_list(topics)[:3]))
        if datasets:
            # Not "N dataset(s)". The parenthesised plural is a programmer
            # writing one string for two cases; every other count in the
            # product agrees in number (`resultCount` exists for exactly
            # this), and this one reached the reader as "1 dataset(s)".
            n = len(datasets)
            summary_bits.append(f"{n} dataset" if n == 1 else f"{n} datasets")
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
            finding=finding_opensanctions(item),
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
