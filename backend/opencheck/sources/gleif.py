"""GLEIF adapter.

GLEIF exposes the Level 1 LEI record (legal entity) and Level 2 relationship
records (direct/ultimate parent and direct children) via a JSON:API-formatted
REST endpoint at ``https://api.gleif.org/api/v1``. No authentication is
required and the data is CC0.

Live endpoints used:

* ``GET /lei-records?filter[fulltext]=<query>`` — entity search.
* ``GET /lei-records/{lei}`` — Level 1 record for a single LEI.
* ``GET /lei-records/{lei}/direct-parent`` — Level 2 direct parent (optional).
* ``GET /lei-records/{lei}/ultimate-parent`` — Level 2 ultimate parent (optional).
* ``GET /lei-records/{lei}/direct-children?page[size]=100&page[number]=1`` —
  first page of direct subsidiaries + total count (optional).

The parent calls return 404 when no relationship is on file; we treat that
as "no parent" rather than an error.  The children call returns an empty list
(or 404) when the entity has no subsidiaries in GLEIF.  Only the first page
(≤ 10 records) is fetched; the pagination total is stored in the bundle so
the UI can display "showing X of N".
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any
from urllib.parse import quote

import httpx

from .. import mirrorstats, provenance
from ..cache import Cache
from ..config import get_settings
from ..gleif_throttle import GleifRateLimitedError, get_throttle
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.gleif import GLEIFBundle
from .bolagsverket import BV_RA_CODE as _BV_RA_CODE, normalise_org_number as _normalise_org_number
from .brreg import NO_RA_CODE as _BRREG_RA_CODE, normalise_orgnr as _normalise_orgnr
from .corporations_canada import CA_CORP_RA_CODE as _CA_CORP_RA_CODE, normalise_corp_id as _normalise_corp_id
from .cro import IE_RA_CODE as _CRO_RA_CODE, normalise_crn as _normalise_crn
from .malta_mbr import MT_RA_CODE as _MT_RA_CODE, normalise_mt_crn as _normalise_mt_crn
from .cr_hongkong import HK_RA_CODES as _HK_RA_CODES, normalise_hk_brn as _normalise_hk_brn
from .cnpj_brazil import BR_RA_CODE as _BR_RA_CODE, normalise_cnpj as _normalise_cnpj
from .inpi import INPI_RA_CODE as _INPI_RA_CODE, normalise_siren as _normalise_siren
from .kvk import KVK_RA_CODE as _KVK_RA_CODE, normalise_kvk as _normalise_kvk
from .prh import FI_RA_CODE as _PRH_RA_CODE, normalise_ytunnus as _normalise_ytunnus
from .ares import CZ_RA_CODE as _CZ_RA_CODE, normalise_ico as _normalise_ico
from .krs_poland import PL_KRS_RA_CODE as _PL_KRS_RA_CODE, normalise_krs as _normalise_krs
from .firmenbuch import AT_FB_RA_CODE as _AT_FB_RA_CODE, normalise_fn as _normalise_fn
from .jar_lithuania import LT_RA_CODE as _LT_RA_CODE, normalise_code as _normalise_lt_code
from .bce_belgium import BCE_RA_CODE as _BCE_RA_CODE, normalise_enterprise_number as _normalise_enterprise_number
from .rpo_slovakia import SK_RPO_RA_CODE as _SK_RPO_RA_CODE, normalise_ico as _normalise_sk_ico
from .ur_latvia import LV_RA_CODE as _LV_RA_CODE, normalise_regcode as _normalise_lv_regcode
from .zefix import CH_RA_CODES as _ZEFIX_RA_CODES, format_uid as _zefix_format_uid
from .cvr_denmark import DK_CVR_RA_CODE as _DK_CVR_RA_CODE, normalise_cvr as _normalise_cvr

_API_BASE = "https://api.gleif.org/api/v1"
_CACHE_NS = "gleif"

#: Provenance detail for a store-served anchor, by why the store answered.
#: Shared with the subsidiary network so the two say the same thing.
SNAPSHOT_DETAIL: dict[str, str] = {
    "fallback": "GLEIF Golden Copy snapshot (live API rate-limited)",
    "mirror": "GLEIF Golden Copy mirror",
}

# Relationship data (parent, ultimate-parent, direct-children) is re-fetched
# when the cached entry is older than this many days.  Ownership structures
# can change at any time; a 1-day TTL ensures stale (or now-inactive)
# relationships are not served indefinitely from cache.
_RELATIONSHIP_CACHE_MAX_AGE_DAYS = 1.0

# Main LEI record (entity name, address, registration status).  These change
# less frequently than relationships but can be updated when an entity renews
# its LEI or amends its registered details.  7-day TTL is a reasonable balance
# between freshness and avoiding excessive API load.
_LEI_RECORD_CACHE_MAX_AGE_DAYS = 7.0

# GLEIF Registration Authority codes for Companies House sub-registries.
# Typo-tolerance fallback for name search (issue #33). When the exact fulltext
# query returns zero hits, ``search`` retries with each token dropped in turn
# (leave-one-out) — a single-character typo lives in one token, so the variant
# that drops it matches on the remaining correct tokens. Bounds and marking:
_MAX_RELAX_TOKENS = 12  # skip the per-token fan-out for implausibly long names
_RELAX_RESULT_LIMIT = 10  # cap the relaxed candidate list (mirrors page[size])
# Appended to the summary of any hit that came from the relaxed fallback rather
# than an exact match, so callers / UI can flag it as a typo-tolerant suggestion.
_RELAXED_SUMMARY_SUFFIX = "approximate match"

# GLEIF filter fields for reverse lookup (local-id → LEI).
# Tried in order; the first that returns results wins.
_LOCAL_ID_FILTER_FIELDS = [
    "filter[entity.registeredAs]",
    "filter[registration.validatedAs]",
    "filter[registration.otherValidationAuthorities.validatedAs]",
]


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
            name="GLEIF",
            homepage="https://search.gleif.org/#/search/",
            description=(
                "Legal entity information from the Global Legal Entity "
                "Identifier Foundation."
            ),
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

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        payload = await self._get(
            f"/lei-records?filter[fulltext]={quote(query)}&page[size]=10",
            cache_key=cache_key,
        )
        hits = [self._entity_hit(item) for item in payload.get("data", [])]
        if hits:
            return hits
        # Zero-result trigger (issue #33): the exact fulltext query matched
        # nothing, so fall back to a typo-tolerant relaxed search. Clean queries
        # that already resolve never reach this and pay no extra API calls.
        return await self._relaxed_search(query)

    async def _relaxed_search(self, query: str) -> list[SourceHit]:
        """Typo-tolerant fallback for :meth:`search` (issue #33).

        Runs one fulltext search per token with that single token dropped
        (*leave-one-out*): a one-character typo lives in exactly one token, so
        the variant that drops the typo'd token matches on the remaining
        correct tokens. Candidates are ranked by how many leave-one-out
        variants surfaced them (consensus) — the real entity, whose distinctive
        tokens survive across variants, floats up — breaking ties on the best
        (lowest) rank any variant gave it. Each surfaced hit has its summary
        marked ``approximate match``.

        Returns ``[]`` (no fallback) when live mode is off, or the query has
        fewer than two tokens (nothing to drop) or an implausibly large token
        count (guards the per-token API fan-out).
        """
        if not self.info.live_available:
            return []
        tokens = query.split()
        if not 2 <= len(tokens) <= _MAX_RELAX_TOKENS:
            return []

        relaxed_queries = [
            " ".join(tokens[:i] + tokens[i + 1 :]) for i in range(len(tokens))
        ]
        payloads = await asyncio.gather(
            *(
                self._get(
                    f"/lei-records?filter[fulltext]={quote(rq)}&page[size]=10",
                    cache_key=f"{_CACHE_NS}/search/{_slug(rq)}",
                )
                for rq in relaxed_queries
            )
        )

        votes: dict[str, int] = {}
        best_rank: dict[str, int] = {}
        item_by_lei: dict[str, dict[str, Any]] = {}
        for payload in payloads:
            seen: set[str] = set()
            for rank, item in enumerate(payload.get("data", []), start=1):
                attrs = item.get("attributes") or {}
                lei: str = attrs.get("lei") or item.get("id") or ""
                if not lei or lei in seen:
                    continue
                seen.add(lei)
                votes[lei] = votes.get(lei, 0) + 1
                best_rank[lei] = min(best_rank.get(lei, rank), rank)
                item_by_lei.setdefault(lei, item)

        ranked = sorted(votes, key=lambda lei: (-votes[lei], best_rank[lei]))
        hits: list[SourceHit] = []
        for lei in ranked[:_RELAX_RESULT_LIMIT]:
            hit = self._entity_hit(item_by_lei[lei])
            hits.append(
                hit.model_copy(
                    update={"summary": f"{hit.summary} · {_RELAXED_SUMMARY_SUFFIX}"}
                )
            )
        return hits

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
        lei = hit_id.strip().upper()
        cache_key = f"{_CACHE_NS}/lei/{lei}"

        # Phase 179: mirror first. When the flag is on and the entity-pages
        # file is a Phase 178 mirror that holds this LEI, the anchor — record,
        # both parents or the exceptions filed in their place, the children
        # page — is served from it and GLEIF is not called at all. A miss
        # (an LEI issued since the mirror's watermark, or a trimmed file)
        # falls through to the live order below, exactly as before, and the
        # adapter cache keeps that live answer for repeat lookups until the
        # next refresh brings the mirror up to date. With the flag off, or a
        # v1 file, nothing here runs and the order is Phase 143's.
        settings = get_settings()
        if settings.gleif_mirror_first:
            bundle = self._mirror_bundle(lei)
            if bundle is not None:
                await self._crossref_topup(lei, bundle)
                validate_raw("gleif", GLEIFBundle, bundle)
                return bundle

        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        # Phase 144: when a Golden Copy snapshot exists for this LEI, bound the
        # live attempt — a saturated GLEIF budget otherwise makes the user sit
        # out the throttle's full max wait (~15–21s measured on 2026-08-29)
        # before the very same snapshot is served. No snapshot row (or the
        # early fallback disabled) keeps the unbounded live attempt, so
        # deployments without the entity-pages DB behave exactly as before.
        snapshot_after = settings.gleif_snapshot_after_s
        timeout: float | None = (
            snapshot_after
            if snapshot_after > 0 and self._snapshot_available(lei)
            else None
        )
        try:
            (
                record,
                (direct_parent, direct_exception),
                (ultimate_parent, ultimate_exception),
                (direct_children, direct_children_total),
            ) = await asyncio.wait_for(
                asyncio.gather(
                    self._get(
                        f"/lei-records/{quote(lei)}",
                        cache_key=cache_key,
                        max_age_days=_LEI_RECORD_CACHE_MAX_AGE_DAYS,
                    ),
                    self._parent_or_exception(lei, "direct"),
                    self._parent_or_exception(lei, "ultimate"),
                    self._fetch_direct_children(lei),
                ),
                timeout=timeout,
            )
        except (GleifRateLimitedError, TimeoutError) as exc:
            # Live GLEIF is rate-limiting (or, with a snapshot in hand, simply
            # not answering within the bound) and no cache entry — fresh or
            # stale — could stand in. Last resort before failing the whole
            # lookup: serve the anchor from the entity-pages Golden Copy
            # snapshot, honestly badged as such (Phase 143; bound added in
            # Phase 144).
            bundle = self._snapshot_bundle(lei)
            if bundle is None:
                if isinstance(exc, TimeoutError):  # store row vanished mid-flight
                    raise GleifRateLimitedError(
                        f"live GLEIF fetch for {lei} exceeded "
                        f"{snapshot_after:.0f}s and no snapshot row was found"
                    ) from exc
                raise
            validate_raw("gleif", GLEIFBundle, bundle)
            return bundle

        bundle = {
            "source_id": self.id,
            "lei": lei,
            "record": record.get("data") or record,
            "direct_parent": (direct_parent or {}).get("data"),
            "ultimate_parent": (ultimate_parent or {}).get("data"),
            "direct_parent_exception": (direct_exception or {}).get("data"),
            "ultimate_parent_exception": (ultimate_exception or {}).get("data"),
            "direct_children": direct_children,
            "direct_children_total": direct_children_total,
        }
        validate_raw("gleif", GLEIFBundle, bundle)
        return bundle

    async def fetch_entity(self, lei: str) -> dict[str, Any]:
        """The Level 1 ``entity`` block alone — one request, no relationships.

        For callers that need only what the record says about the entity
        itself (``registeredAs`` / ``registeredAt`` for the dispatch-drift
        check, Phase 162). ``fetch`` costs four to six GLEIF requests per LEI
        because it also walks both parents, their reporting exceptions and
        the children; a sweep of two dozen anchors through it exhausted the
        shared 50/min budget and reported every remaining anchor as drift.
        Same cache key and TTL as the record ``fetch`` reads, so the two never
        disagree about an LEI.
        """
        lei = lei.strip().upper()
        if get_settings().gleif_mirror_first:
            row = self._mirror_row(lei)
            if row is not None:
                from ..entity_pages import gleif_record_from_row

                mirrorstats.record("entity.mirror")
                return (gleif_record_from_row(row).get("attributes") or {}).get("entity") or {}
            mirrorstats.record("entity.miss_live")
        record = await self._get(
            f"/lei-records/{quote(lei)}",
            cache_key=f"{_CACHE_NS}/lei/{lei}",
            max_age_days=_LEI_RECORD_CACHE_MAX_AGE_DAYS,
        )
        data = record.get("data") or record
        attrs = data.get("attributes") or {}
        return attrs.get("entity") or {}

    # ------------------------------------------------------------------
    # Phase 179 — the mirror as the first source, not the last resort
    # ------------------------------------------------------------------

    @staticmethod
    def _mirror_row(lei: str):
        """The store row for ``lei`` when the configured file is a Phase 178
        mirror, else ``None`` — a v1 file is never served first, because its
        rows cannot carry ``registeredAs`` or a reporting exception and a
        lookup served from one would silently lose the registry bridges."""
        from ..entity_pages import get_store

        store = get_store()
        if store is None or not store.is_mirror:
            return None
        return store.get(lei)

    def _mirror_bundle(self, lei: str) -> dict[str, Any] | None:
        """The anchor bundle from the mirror, or ``None`` on a miss (counted)."""
        from ..entity_pages import get_store

        store = get_store()
        if store is None or not store.is_mirror:
            mirrorstats.record("anchor.no_mirror")
            return None
        bundle = self._snapshot_bundle(lei, reason="mirror")
        if bundle is None:
            mirrorstats.record("anchor.miss_live")
            return None
        mirrorstats.record("anchor.mirror")
        # What the live path would have spent: the record, a call per parent
        # kind, plus the exception probe for each kind with no parent, plus
        # the children page. Counted from the bundle, not assumed.
        saved = 2 + sum(
            1 + (0 if bundle.get(f"{kind}_parent") is not None else 1)
            for kind in ("direct", "ultimate")
        )
        mirrorstats.record("anchor.live_calls_saved", saved)
        return bundle

    #: The top-up never dips into the last third of the window: a real
    #: lookup's own calls come first.
    _TOPUP_RESERVE_FRACTION = 3

    #: Level 1 attributes the live API carries that the Golden Copy does not:
    #: the cross-reference ids from GLEIF's mapping programmes. The mapper
    #: publishes them as identifiers and the pipeline dispatches
    #: OpenCorporates on ``ocid`` and corroborates MEIP on ``spglobal``, so a
    #: mirror-served anchor without them would silently lose a source.
    _CROSSREF_ATTRS = ("ocid", "qcc", "bic", "mic", "spglobal")

    async def _crossref_topup(self, lei: str, bundle: dict[str, Any]) -> None:
        """One bounded live call on top of a mirror-served anchor, for what
        bulk cannot provide (Phase 179).

        The curated-subject export diff that gates this phase showed the
        mirror alone dropping two to three identifiers per entity statement —
        the OpenCorporates, QCC, S&P, BIC and MIC ids GLEIF inlines from its
        mapping files, which are not in the Golden Copy. So a mirror hit still
        spends **one** live call — the Level 1 record, cached seven days like
        the live path's — and merges those attributes into the mirror record.
        Everything else in the bundle stays the mirror's. The call is skipped
        when live mode is off, when the throttle has no headroom (a real
        lookup's calls come first) or when GLEIF does not answer within the
        snapshot bound; the anchor is then served without the cross-reference
        ids, and ``crossrefs_available`` says so. With
        ``OPENCHECK_GLEIF_LIVE_CONFIRM`` on, the same record is compared with
        the mirror on every path the mapper reads and the result counted — the
        freshness measurement costs nothing extra. Phase 181's mapping files
        would make this call unnecessary.
        """
        bundle["crossrefs_available"] = False
        settings = get_settings()
        if not settings.allow_live:
            mirrorstats.record("anchor.topup_skipped")
            return
        limit = settings.gleif_rate_limit_per_minute
        reserve = max(limit // self._TOPUP_RESERVE_FRACTION, 1) if limit > 0 else 0
        cache_key = f"{_CACHE_NS}/lei/{lei}"
        cached = self._cache.get_payload(cache_key, max_age_days=_LEI_RECORD_CACHE_MAX_AGE_DAYS)
        if cached is None and not get_throttle().has_headroom(reserve):
            mirrorstats.record("anchor.topup_skipped")
            return
        bound = settings.gleif_snapshot_after_s or None
        try:
            payload = await asyncio.wait_for(
                self._get(
                    f"/lei-records/{quote(lei)}",
                    cache_key=cache_key,
                    max_age_days=_LEI_RECORD_CACHE_MAX_AGE_DAYS,
                ),
                timeout=bound,
            )
        except Exception:  # noqa: BLE001 — never fail a mirror-served anchor
            mirrorstats.record("anchor.topup_failed")
            return
        mirrorstats.record("anchor.topup_cached" if cached is not None else "anchor.topup_live")
        live = payload.get("data") or payload
        live_attrs = live.get("attributes") or {}
        record_attrs = bundle["record"].setdefault("attributes", {})
        for key in self._CROSSREF_ATTRS:
            value = live_attrs.get(key)
            if value not in (None, "", []):
                record_attrs[key] = value
        bundle["crossrefs_available"] = True
        if settings.gleif_live_confirm:
            from ..entity_pages import differing_paths

            diffs = differing_paths(bundle["record"], live)
            if diffs:
                mirrorstats.record("confirm.differs")
                mirrorstats.record_differing_paths(diffs)
            else:
                mirrorstats.record("confirm.same")

    @staticmethod
    def _snapshot_available(lei: str) -> bool:
        """Cheap check: does the entity-pages Golden Copy hold this LEI?

        One indexed primary-key read. Decides whether ``fetch`` may bound the
        live attempt (Phase 144) — with no snapshot to fall back on, cutting
        the live attempt short would only trade a slow answer for none.
        """
        from ..entity_pages import get_store

        store = get_store()
        return store is not None and store.get(lei) is not None

    def _snapshot_bundle(
        self, lei: str, *, reason: str = "fallback"
    ) -> dict[str, Any] | None:
        """Anchor bundle from the entity-pages Golden Copy SQLite, or ``None``.

        ``reason`` names why the store is answering — ``"fallback"`` (Phase
        143: live GLEIF is rate-limiting) or ``"mirror"`` (Phase 179: the
        mirror is the first source) — and is what the provenance detail and
        the bundle's ``snapshot_source`` say. The bundle is built the same way
        either way; only the wording differs, because the reader deserves to
        know whether the snapshot was chosen or forced.

        Phase 143's last line of degradation before a lookup fails outright:
        when live GLEIF is rate-limiting and no cache entry (fresh or stale)
        exists, serve the anchor from the same local snapshot the ``/entity``
        pages are rendered from. It carries what the store holds — since
        Phase 178 that is the full Level 1 record (names, both addresses,
        ``registeredAs``/``registeredAt``, category, legal form, dates),
        direct/ultimate parent records, the reporting exceptions filed in
        their place, and the first page of direct children with the store's
        total. On a pre-178 file the detail, exceptions and cross-reference
        ids are simply absent, so the registry bridges and exception chips
        quietly skip for this lookup rather than being guessed at. Provenance
        is recorded as ``snapshot`` with the Golden Copy publish date, so every
        statement mapped from this bundle says what it is.
        """
        from ..entity_pages import get_store, gleif_record_from_row

        store = get_store()
        if store is None:
            return None
        row = store.get(lei)
        if row is None:
            return None

        # Shared with the subsidiary-network snapshot fallback so the two
        # snapshot paths cannot drift in what they claim from a store row.
        _record = gleif_record_from_row

        related = store.get_many([row.direct_parent_lei, row.ultimate_parent_lei])

        def _parent(parent_lei: str | None) -> dict[str, Any] | None:
            if not parent_lei:
                return None
            parent_row = related.get(parent_lei)
            if parent_row is not None:
                return _record(parent_row)
            # The store row names a parent the store itself lacks (trimmed
            # build, or a non-LEI edge) — keep the LEI, claim nothing else.
            return {"id": parent_lei, "attributes": {"lei": parent_lei}}

        children_rows, children_total = store.children(lei, limit=100)

        # Phase 178: the store now holds the REPEX file, so a subject with no
        # parent of a kind can carry the reporting exception it filed — the
        # bridge statements and the Phase 114 chip work exactly as they do
        # live. On a v1 file `exceptions()` is empty and nothing is claimed.
        exceptions = store.exceptions(lei)

        def _exception(kind: str, parent: dict[str, Any] | None) -> dict[str, Any] | None:
            if parent is not None:
                return None
            row_ = exceptions.get(kind)
            return row_.record() if row_ is not None else None

        # Dated to the Golden Copy publish the file reflects — the full
        # timestamp since Phase 178 (three publishes a day), the date on an
        # older file — so the badge says how old the mirror actually is.
        provenance.record_snapshot(store.watermark(), SNAPSHOT_DETAIL[reason])

        direct_parent = _parent(row.direct_parent_lei)
        ultimate_parent = _parent(row.ultimate_parent_lei)
        return {
            "source_id": self.id,
            "lei": lei,
            "record": _record(row),
            "direct_parent": direct_parent,
            "ultimate_parent": ultimate_parent,
            "direct_parent_exception": _exception("direct", direct_parent),
            "ultimate_parent_exception": _exception("ultimate", ultimate_parent),
            "direct_children": [_record(r) for r in children_rows],
            "direct_children_total": children_total,
            # Not schema fields (extra="allow") — let tests and logs tell a
            # snapshot-served anchor from a live one, and a chosen mirror read
            # (Phase 179) from a forced fallback (Phase 143).
            "snapshot_fallback": reason == "fallback",
            "snapshot_source": reason,
        }


    async def _fetch_direct_children(
        self, lei: str
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch the first page of direct subsidiaries for ``lei``.

        Returns ``(records, total)`` where ``records`` is a list of up to 10
        Level 1 lei-record data objects (same shape as the Level 1 fetch) and
        ``total`` is the full count reported by GLEIF pagination metadata.

        Returns ``([], 0)`` when:
        * live mode is disabled and the result is not cached
        * GLEIF reports no children for this entity (empty ``data`` or 404)
        """
        cache_key = f"{_CACHE_NS}/lei/{lei}/direct-children-p1-s100"
        payload = await self._get_optional(
            f"/lei-records/{quote(lei)}/direct-children?page[size]=100&page[number]=1",
            cache_key=cache_key,
            max_age_days=_RELATIONSHIP_CACHE_MAX_AGE_DAYS,
        )
        if payload is None:
            return [], 0

        records: list[dict[str, Any]] = payload.get("data") or []
        total: int = (
            (payload.get("meta") or {})
            .get("pagination", {})
            .get("total", len(records))
        )
        return records, total

    async def _parent_or_exception(
        self, lei: str, kind: str
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Return ``(parent, exception)`` — at most one of the pair is non-None.

        The parent endpoint (``/direct-parent`` or ``/ultimate-parent``) returns
        the parent's L1 record when the accounting-consolidation relationship is
        ACTIVE, and a 404 when the relationship is INACTIVE or has never been
        filed.  The ``_RELATIONSHIP_CACHE_MAX_AGE_DAYS`` TTL ensures that a
        formerly-active relationship that has since become inactive (or been
        removed by GLEIF) will be re-checked on the next lookup rather than
        served from stale cache indefinitely.

        GLEIF exposes the exception reason on a sibling endpoint:
        ``/lei-records/{lei}/{kind}-parent-reporting-exception``.
        """
        parent = await self._get_optional(
            f"/lei-records/{quote(lei)}/{kind}-parent",
            cache_key=f"{_CACHE_NS}/lei/{lei}/{kind}-parent",
            max_age_days=_RELATIONSHIP_CACHE_MAX_AGE_DAYS,
        )
        if parent is not None:
            return parent, None

        exception = await self._get_optional(
            f"/lei-records/{quote(lei)}/{kind}-parent-reporting-exception",
            cache_key=f"{_CACHE_NS}/lei/{lei}/{kind}-parent-exception",
            max_age_days=_RELATIONSHIP_CACHE_MAX_AGE_DAYS,
        )
        return None, exception

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(
        self, path: str, *, cache_key: str, max_age_days: float | None = None
    ) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key, max_age_days=max_age_days)
        if cached is not None:
            return cached[0]

        try:
            async with build_client() as client:
                response = await client.get(f"{_API_BASE}{path}")
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPStatusError, GleifRateLimitedError) as exc:
            if (
                isinstance(exc, httpx.HTTPStatusError)
                and exc.response.status_code != 429
            ):
                raise
            # Rate-limited (an observed 429, or the shared budget refusing to
            # send one). A stale cache entry beats no answer: re-read with the
            # TTL waived — the cache layer records the entry's true age as
            # `cached` provenance, so the response never claims freshness it
            # doesn't have.
            stale = self._cache.get_payload(cache_key)
            if stale is not None:
                return stale[0]
            raise GleifRateLimitedError(str(exc)) from exc

        self._cache.put(cache_key, payload)
        return payload

    async def _get_optional(
        self, path: str, *, cache_key: str, max_age_days: float | None = None
    ) -> dict[str, Any] | None:
        """Like ``_get`` but returns ``None`` on 404 (no relationship on file).

        ``max_age_days`` — when set, cached entries older than this many days
        are treated as a miss and re-fetched.  Pass
        ``_RELATIONSHIP_CACHE_MAX_AGE_DAYS`` for parent / children calls so
        that ownership structures that have changed in GLEIF are not served
        from stale cache indefinitely.

        Offline demo behaviour: when ``live_available`` is false and the
        cache has no entry, treat as "no relationship" rather than
        firing a network call. This keeps demo fixtures focused on the
        statements that actually matter.
        """
        cached = self._cache.get_payload(cache_key, max_age_days=max_age_days)
        if cached is not None:
            return cached[0]
        if not self.info.live_available:
            return None

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
            if exc.response.status_code == 429:
                stale = self._cache.get_payload(cache_key)
                if stale is not None:
                    return stale[0]
                raise GleifRateLimitedError(str(exc)) from exc
            raise
        except GleifRateLimitedError:
            # Budget refused to send. Same stale-beats-nothing rule as ``_get``.
            stale = self._cache.get_payload(cache_key)
            if stale is not None:
                return stale[0]
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

        # GLEIF often mirrors a local registry id in registeredAs. We
        # surface that under both a generic key (for reference) and the
        # well-known cross-source bridge key when one applies, so the
        # reconciler can bridge GLEIF ↔ Companies House on the same UK
        # company number.
        registered_as = entity.get("registeredAs")
        registered_at_id = (entity.get("registeredAt") or {}).get("id") or ""
        if registered_as and jurisdiction:
            identifiers[f"registered_as_{jurisdiction.lower()}"] = registered_as
            if jurisdiction.upper() == "GB":
                identifiers["gb_coh"] = registered_as
            # Swiss UID — expose as ``che_uid`` so the reconciler can bridge
            # GLEIF ↔ Zefix on the same CHE number.
            if registered_at_id in _ZEFIX_RA_CODES:
                identifiers["che_uid"] = _zefix_format_uid(registered_as)
            # Dutch KvK number — expose as ``kvk_number`` so the reconciler
            # can bridge GLEIF ↔ KvK on the same registration number.
            if registered_at_id == _KVK_RA_CODE:
                identifiers["kvk_number"] = _normalise_kvk(registered_as)
            # French SIREN — expose as ``siren`` so the reconciler can bridge
            # GLEIF ↔ INPI on the same registration number.
            if registered_at_id == _INPI_RA_CODE:
                identifiers["siren"] = _normalise_siren(registered_as)
            # Swedish organisation number — expose as ``se_org_number`` so
            # the reconciler can bridge GLEIF ↔ Bolagsverket.
            if registered_at_id == _BV_RA_CODE:
                try:
                    identifiers["se_org_number"] = _normalise_org_number(registered_as)
                except ValueError:
                    pass
            # Norwegian organisation number — expose as ``no_orgnr`` so
            # the reconciler can bridge GLEIF ↔ Brreg.
            if registered_at_id == _BRREG_RA_CODE:
                identifiers["no_orgnr"] = _normalise_orgnr(registered_as)
            # Irish company registration number — expose as ``ie_crn`` so
            # the reconciler can bridge GLEIF ↔ CRO.
            if registered_at_id == _CRO_RA_CODE:
                identifiers["ie_crn"] = _normalise_crn(registered_as)
            # Maltese registration number — expose as ``mt_crn`` so the
            # reconciler can bridge GLEIF ↔ Malta Business Registry.
            if registered_at_id == _MT_RA_CODE:
                identifiers["mt_crn"] = _normalise_mt_crn(registered_as)
            # Hong Kong Business Registration Number — expose as ``hk_brn``
            # so the reconciler can bridge GLEIF ↔ Companies Registry. Both
            # HK authorities (RA000388 / RA000389) file the BRN here.
            if registered_at_id in _HK_RA_CODES:
                try:
                    identifiers["hk_brn"] = _normalise_hk_brn(registered_as)
                except ValueError:
                    pass
            # Brazilian CNPJ — expose as ``br_cnpj`` so the reconciler can
            # bridge GLEIF ↔ Receita Federal CNPJ register.
            if registered_at_id == _BR_RA_CODE:
                identifiers["br_cnpj"] = _normalise_cnpj(registered_as)
            # Finnish Business ID (Y-tunnus) — expose as ``fi_ytunnus`` so
            # the reconciler can bridge GLEIF ↔ PRH.
            if registered_at_id == _PRH_RA_CODE:
                identifiers["fi_ytunnus"] = _normalise_ytunnus(registered_as)
            # Latvian registration number — expose as ``lv_regcode`` so
            # the reconciler can bridge GLEIF ↔ UR Latvia.
            if registered_at_id == _LV_RA_CODE:
                identifiers["lv_regcode"] = _normalise_lv_regcode(registered_as)
            # Lithuanian entity code — expose as ``lt_code`` so the
            # reconciler can bridge GLEIF ↔ JAR Lithuania.
            if registered_at_id == _LT_RA_CODE:
                identifiers["lt_code"] = _normalise_lt_code(registered_as)
            # Czech IČO — expose as ``cz_ico`` so the reconciler can
            # bridge GLEIF ↔ ARES.
            if registered_at_id == _CZ_RA_CODE:
                identifiers["cz_ico"] = _normalise_ico(registered_as)
            # Polish KRS number — expose as ``pl_krs`` so the reconciler
            # can bridge GLEIF ↔ KRS Poland.
            if registered_at_id == _PL_KRS_RA_CODE:
                identifiers["pl_krs"] = _normalise_krs(registered_as)
            # Austrian Firmenbuchnummer — expose as ``at_fn`` so the reconciler
            # can bridge GLEIF ↔ Firmenbuch.
            if registered_at_id == _AT_FB_RA_CODE:
                identifiers["at_fn"] = _normalise_fn(registered_as)
            # Slovak IČO — expose as ``sk_ico`` so the reconciler can bridge
            # GLEIF ↔ RPO Slovakia.
            if registered_at_id == _SK_RPO_RA_CODE:
                identifiers["sk_ico"] = _normalise_sk_ico(registered_as)
            # Belgian enterprise number — expose as ``be_enterprise_number`` so
            # the reconciler can bridge GLEIF ↔ BCE/KBO.
            if registered_at_id == _BCE_RA_CODE:
                identifiers["be_enterprise_number"] = _normalise_enterprise_number(registered_as)
            # Canadian federal corporation number — expose as ``ca_corp_id`` so
            # the reconciler can bridge GLEIF ↔ Corporations Canada.
            if registered_at_id == _CA_CORP_RA_CODE:
                identifiers["ca_corp_id"] = _normalise_corp_id(registered_as)
            # Danish CVR number — expose as ``dk_cvr`` so the reconciler can
            # bridge GLEIF ↔ CVR Denmark.
            if registered_at_id == _DK_CVR_RA_CODE:
                identifiers["dk_cvr"] = _normalise_cvr(registered_as)

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
    # Reverse lookup: local registry ID → LEI
    # ------------------------------------------------------------------

    async def search_by_local_id(
        self,
        local_id: str,
        ra_code: str = "",
    ) -> list[SourceHit]:
        """Find LEI records by a local registry identifier.

        The GLEIF API exposes three fields that may carry local IDs:

        * ``entity.registeredAs`` — the primary local registry number,
          e.g. the UK Companies House number or German Handelsregister number.
        * ``registration.validatedAs`` — the identifier used by the
          Validation Agent when validating the LEI application.
        * ``registration.otherValidationAuthorities.validatedAs`` — same
          as above but for additional validation authorities (can be null
          or occur multiple times on the same LEI record).

        All three are queried in sequence; hits are deduplicated by LEI.

        ``ra_code`` should be the GLEIF Registration Authority code for the
        issuing registry (e.g. ``"RA000585"`` for Companies House England &
        Wales).  Including it avoids false positives when multiple registries
        share the same local number format.  Pass ``""`` to skip the filter.

        Returns an empty list when live mode is disabled.
        """
        if not self.info.live_available:
            return []

        seen_leis: set[str] = set()
        hits: list[SourceHit] = []

        for field in _LOCAL_ID_FILTER_FIELDS:
            params = f"page[size]=5&{field}={quote(local_id)}"
            if ra_code:
                params += f"&filter[entity.registeredAt]={quote(ra_code)}"
            cache_key = f"{_CACHE_NS}/by-local-id/{_slug(params)}"

            try:
                payload = await self._get(f"/lei-records?{params}", cache_key=cache_key)
                for item in payload.get("data") or []:
                    attrs = item.get("attributes") or {}
                    lei = attrs.get("lei") or item.get("id") or ""
                    if lei and lei not in seen_leis:
                        seen_leis.add(lei)
                        hits.append(self._entity_hit(item))
            except Exception:  # noqa: BLE001
                pass

        return hits

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
