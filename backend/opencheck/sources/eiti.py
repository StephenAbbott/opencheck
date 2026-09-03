"""EITI (Extractive Industries Transparency Initiative) adapter — ESG category.

EITI (https://eiti.org/) publishes company-level *payments to governments*
(taxes, royalties, licence fees…) disclosed under the EITI Standard by 65+
implementing countries, with GFS revenue classification and USD-normalised
amounts. This complements the GEM/Climate TRACE source: GEM covers assets
and emissions; EITI covers extractive-sector fiscal flows.

How the lookup works
--------------------
1. **Match (offline)**: the EITI API's documented ``identification`` filter
   is not implemented server-side (verified 2026-07-07 — nonsense values
   return the unfiltered set), so matching happens against the committed
   artifact ``opencheck/data/eiti_organisations.json.gz`` (built by
   ``scripts/build_eiti_index.py``; re-run when EITI refreshes summary data,
   roughly quarterly). The lookup pipeline passes the GLEIF anchor's
   ``(jurisdiction, registeredAs)`` — because EITI identifications are
   national registry/tax numbers, this matches any LEI holder in any EITI
   country, not just those with a dedicated OpenCheck adapter. Verified
   formats: GB → Companies House numbers (incl. SC/FC prefixes), NO →
   9-digit orgnr, NL → KvK-adjacent, US → EINs.

   **US is the exception**: EITI's US identifications are federal EINs and
   GLEIF publishes a US ``registeredAs`` as the *state* file number, so
   ``registeredAs`` can never match. The lookup pipeline therefore also
   passes a derived ``us_ein``, which ``us_ein_for_lei()`` reads from the
   committed ``eiti_us_ein_by_lei.json`` crosswalk (issue #26).
   Identifications carrying no digit at all are dropped at load: EITI's US
   bucket ships literal ``Private`` and ``Foreign`` sentinel rows where the
   company gave no number, and those are not identifiers.
2. **Payments (live)**: for the most recent reporting years, payment rows
   come from ``GET /api/v2.0/revenue?organisation={id}`` — the one
   server-side filter verified to work — and are aggregated per year and
   per GFS revenue stream.

No API key required. Licence: EITI content-use policy — free republication
with credit to "EITI International Secretariat, eiti.org".
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import re
from pathlib import Path
from typing import Any

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.eiti import EitiBundle

log = logging.getLogger(__name__)

_API_BASE = "https://eiti.org/api/v2.0"
_CACHE_NS = "eiti"

_INDEX_PATH = Path(__file__).resolve().parent.parent / "data" / "eiti_organisations.json.gz"

#: How many of the most recent reporting years to fetch live payments for.
_MAX_REVENUE_YEARS = 4

#: LEI -> EITI US federal EIN, built by ``scripts/build_eiti_us_ein_index.py``.
_US_EIN_PATH = Path(__file__).resolve().parent.parent / "data" / "eiti_us_ein_by_lei.json"

# Lazy module-level singletons.
_index: dict[str, dict[str, list[dict[str, Any]]]] | None = None
_norm_index: dict[str, dict[str, str]] | None = None  # cc -> normform -> ident
_us_ein_by_lei: dict[str, str] | None = None


_DIGITS_RE = re.compile(r"\D+")


def _norm_forms(value: str) -> list[str]:
    """Normalised comparison forms for a national identifier.

    Exact string first, then digits-only, then leading-zero-insensitive.
    Handles GLEIF/EITI formatting drift: ``0056.58.214`` vs ``005658214``,
    ``1285743`` vs ``01285743`` (GB zero-padding), spaced orgnrs, etc.
    """
    value = (value or "").strip().upper()
    if not value:
        return []
    forms = [value]
    digits = _DIGITS_RE.sub("", value)
    if digits and digits != value:
        forms.append(digits)
    if digits:
        stripped = digits.lstrip("0")
        if stripped and stripped not in forms:
            forms.append(stripped)
    return forms


def _get_index() -> tuple[
    dict[str, dict[str, list[dict[str, Any]]]], dict[str, dict[str, str]]
]:
    """Load the committed organisation index (and its normalised lookup)."""
    global _index, _norm_index
    if _index is None or _norm_index is None:
        try:
            with gzip.open(_INDEX_PATH, "rt", encoding="utf-8") as f:
                data = json.load(f)
            _index = data.get("index") or {}
            log.info(
                "EITI organisation index loaded: %s identifications, %s countries",
                data.get("meta", {}).get("identifications"),
                data.get("meta", {}).get("countries"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI organisation index unavailable: %s", exc)
            _index = {}
        _norm_index = {}
        for cc, idents in _index.items():
            # EITI's identification column carries sentinel words where the
            # company gave no registry number: the US bucket holds literal
            # "Private" and "Foreign" rows. A value with no digit in it is
            # not an identifier in any jurisdiction -- GB's alphanumeric
            # "SC123456" still has digits -- so drop those rows outright
            # rather than let a lookup match the word "Private".
            for ident in [i for i in idents if not _DIGITS_RE.sub("", i or "")]:
                log.debug("EITI %s: ignoring non-identifier %r", cc, ident)
                idents.pop(ident, None)
            bucket: dict[str, str] = {}
            for ident in idents:
                for form in _norm_forms(ident):
                    bucket.setdefault(form, ident)
            _norm_index[cc] = bucket
    return _index, _norm_index


def us_ein_for_lei(lei: str) -> str:
    """The EITI-published federal EIN for an LEI, or ``""`` when there is none.

    EITI's US identifications are federal EINs; GLEIF publishes a US
    ``registeredAs`` as the *state* file number and never the EIN, so without
    this a US subject can never join the organisation index.
    ``routers/lookup.py::_build_derived`` calls this to populate
    ``ctx.derived["us_ein"]`` before dispatch, which is the producer
    ``fetch_by_registration``'s ``us_ein`` argument has been waiting for.

    The crosswalk is committed rather than derived live. The US left EITI in
    November 2017, so its bucket is a frozen single-year (2015) table of 27
    EINs; resolving them offline keeps an EDGAR round-trip off every US
    lookup, and still covers the entities EDGAR can no longer resolve by name
    because they have since merged or delisted. Built (with per-row evidence)
    by ``scripts/build_eiti_us_ein_index.py``.
    """
    global _us_ein_by_lei
    if _us_ein_by_lei is None:
        try:
            with _US_EIN_PATH.open(encoding="utf-8") as f:
                data = json.load(f)
            _us_ein_by_lei = {
                str(k).strip().upper(): str((v or {}).get("ein") or "")
                for k, v in (data.get("index") or {}).items()
                if (v or {}).get("ein")
            }
            log.info(
                "EITI US EIN crosswalk loaded: %s LEIs", len(_us_ein_by_lei)
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI US EIN crosswalk unavailable: %s", exc)
            _us_ein_by_lei = {}
    return _us_ein_by_lei.get((lei or "").strip().upper(), "")


def _reset_caches_for_tests() -> None:
    """Drop the module-level singletons so a test can point at a fixture."""
    global _index, _norm_index, _us_ein_by_lei
    _index = None
    _norm_index = None
    _us_ein_by_lei = None


def _match_identification(country: str, registered_as: str) -> str | None:
    """Return the EITI identification matching a GLEIF registeredAs value."""
    index, norm_index = _get_index()
    cc = (country or "").strip().upper()
    bucket = norm_index.get(cc)
    if not bucket:
        return None
    for form in _norm_forms(registered_as):
        ident = bucket.get(form)
        if ident:
            return ident
    return None



def _failure_label(exc: BaseException) -> str:
    """Short label for degradation.reason_for_failure, from a swallowed error."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status is not None:
        return f"HTTP {status}"
    return type(exc).__name__

class EitiAdapter(SourceAdapter):
    """EITI payments-to-governments adapter — ESG category."""

    id = "eiti"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="EITI — Extractive Industries Transparency Initiative",
            homepage="https://eiti.org/",
            description=(
                "Company-level payments to governments (taxes, royalties, "
                "licence fees) disclosed under the EITI Standard by 65+ "
                "implementing countries, with GFS revenue classification."
            ),
            license="EITI open data (free reuse with attribution)",
            attribution="EITI International Secretariat, eiti.org",
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            category="esg",
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed source; free-text search intentionally empty
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Registration-keyed lookup (called by the lookup pipeline)
    # ------------------------------------------------------------------

    async def fetch_by_registration(
        self,
        jurisdiction: str,
        registered_as: str,
        legal_name: str = "",
        us_ein: str = "",
    ) -> dict[str, Any] | None:
        """Match a company against the EITI index.

        Tries the GLEIF anchor's ``registeredAs`` first, then a derived
        ``us_ein`` (EITI's US identifications are federal EINs, not the
        state-registry numbers GLEIF publishes as ``registeredAs``, so US
        subjects only join via the EIN). Matching is country-scoped and
        punctuation-insensitive (``42-1638663`` == ``421638663``).

        Returns ``None`` when the company is not in the EITI data. On a
        match, returns the bundle: organisation records from the artifact
        plus live payment aggregates for the most recent reporting years
        (payments only when live mode is enabled — the organisation match
        itself is offline data and always available).
        """
        cc = (jurisdiction or "").strip().upper()
        ident = _match_identification(cc, registered_as)
        if ident is None and us_ein:
            ident = _match_identification(cc, us_ein)
        if ident is None:
            return None
        return await self._build_bundle(cc, ident, legal_name=legal_name)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by ``CC:identification`` hit id (deepen / retry path)."""
        cc, _, ident = hit_id.partition(":")
        bundle = await self._build_bundle(cc.strip().upper(), ident.strip())
        if bundle is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return bundle

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_bundle(
        self, cc: str, ident: str, legal_name: str = ""
    ) -> dict[str, Any] | None:
        index, _ = _get_index()
        orgs = list((index.get(cc) or {}).get(ident) or [])
        if not orgs:
            return None
        # Most recent reporting years first; undated records last.
        orgs.sort(key=lambda o: (o.get("year") or ""), reverse=True)
        entity_name = next(
            (o.get("label") for o in orgs if o.get("label")), None
        ) or legal_name or ident

        revenue_years: list[dict[str, Any]] = []
        if self.info.live_available:
            revenue_years = await self._fetch_revenue_years(
                orgs[:_MAX_REVENUE_YEARS]
            )

        streams: dict[str, float] = {}
        total_usd = 0.0
        for ry in revenue_years:
            for row in ry["rows"]:
                if row.get("currency") == "USD" and row.get("revenue"):
                    total_usd += row["revenue"]
                    key = row.get("gfs_label") or row.get("label") or "Other"
                    streams[key] = streams.get(key, 0.0) + row["revenue"]

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "country": cc,
            "identification": ident,
            "entity_name": entity_name,
            "organisations": orgs,
            "revenue_years": revenue_years,
            "streams": dict(
                sorted(streams.items(), key=lambda kv: -kv[1])
            ),
            "total_usd": total_usd,
            "years": sorted({o.get("year") for o in orgs if o.get("year")}, reverse=True),
            "is_stub": False,
        }
        validate_raw("eiti", EitiBundle, bundle)
        return bundle

    async def _fetch_revenue_years(
        self, orgs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Fetch live payment rows per organisation record, in parallel."""

        async def one(org: dict[str, Any]) -> dict[str, Any]:
            org_id = str(org.get("id"))
            cache_key = f"{_CACHE_NS}/revenue/{org_id}"
            cached = self._cache.get_payload(cache_key)
            if cached is not None:
                payload = cached[0]
            else:
                try:
                    async with build_client() as client:
                        response = await client.get(
                            f"{_API_BASE}/revenue",
                            params={"organisation": org_id, "limit": 50},
                            headers={"Accept": "application/json"},
                        )
                        response.raise_for_status()
                        payload = response.json()
                    self._cache.put(cache_key, payload)
                except Exception as exc:  # noqa: BLE001
                    log.warning("EITI revenue fetch failed for %s: %s", org_id, exc)
                    # An empty payments list because the API refused us is not
                    # the same claim as an empty payments list because the
                    # company reported none — and until Phase 121 they were
                    # indistinguishable to every consumer, including the
                    # weekly sweep, which reported EITI healthy while eiti.org
                    # was 403ing every revenue call from CI.
                    degradation.record(
                        self.id,
                        "The EITI revenue API did not answer "
                        f"({type(exc).__name__}); payment rows are missing for "
                        "one or more matched organisations. An empty payments "
                        "list here is not a statement that none were reported.",
                        reason=degradation.reason_for_failure(_failure_label(exc)),
                    )
                    payload = {"data": []}
            rows = []
            total = 0.0
            for r in payload.get("data") or []:
                try:
                    amount = float(r.get("revenue") or 0.0)
                except (TypeError, ValueError):
                    amount = 0.0
                row = {
                    "label": r.get("label"),
                    "revenue": amount,
                    "currency": r.get("currency"),
                    "gfs_label": r.get("gfs.label"),
                    "gfs_code": r.get("gfs.code"),
                }
                rows.append(row)
                if r.get("currency") == "USD":
                    total += amount
            return {
                "year": org.get("year"),
                "organisation_id": org_id,
                "total_usd": total,
                "rows": rows,
            }

        return list(await asyncio.gather(*[one(o) for o in orgs]))
