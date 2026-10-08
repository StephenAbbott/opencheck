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

   **Zambia is the same shape** (Phase 306): EITI's 94 Zambian
   identifications are ZRA TPINs and GLEIF files the PACRA number, so the
   pipeline passes a derived ``zm_tpin`` read from the Zambia EITI portal
   index (``eiti_zambia.tpin_for_lei``), which joined each Zambian LEI to its
   TPIN by name.
   Identifications carrying no digit at all are dropped at load: EITI's US
   bucket ships literal ``Private`` and ``Foreign`` sentinel rows where the
   company gave no number, and those are not identifiers.
2. **Payments (live)**: for the most recent reporting years, payment rows
   come from ``GET /api/v2.0/revenue?organisation={id}`` — the one
   server-side filter verified to work — and are aggregated per year and
   per GFS revenue stream. The endpoint pages at 50 rows regardless of
   ``limit``; the adapter follows ``next`` for a bounded number of pages
   and marks a year ``truncated`` when the API's ``count`` exceeds what was
   read, so a bounded sum is never presented as the year's total.

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

#: The revenue endpoint serves at most 50 rows per page whatever ``limit``
#: says (``limit=500`` still returns 50, verified 2026-10-07) and paginates
#: through a ``next`` link. Most organisation-years are well under 50 rows,
#: but a project-level filer is not: Harbour Energy Plc's 2021 record carries
#: 130 rows, and until this bound existed the adapter read the first page and
#: reported its sum as the year's total. Four pages is 200 rows — enough for
#: every organisation-year seen so far; anything beyond is recorded as
#: ``truncated`` rather than silently summed short.
_REVENUE_PAGE_SIZE = 50
_MAX_REVENUE_PAGES = 4

#: LEI -> EITI US federal EIN, built by ``scripts/build_eiti_us_ein_index.py``.
_US_EIN_PATH = Path(__file__).resolve().parent.parent / "data" / "eiti_us_ein_by_lei.json"

# Lazy module-level singletons.
_index: dict[str, dict[str, list[dict[str, Any]]]] | None = None
_norm_index: dict[str, dict[str, str]] | None = None  # cc -> normform -> ident
#: cc -> ident -> every spelling of the same number in that bucket (Phase 306).
_variants: dict[str, dict[str, list[str]]] | None = None
_us_ein_by_lei: dict[str, str] | None = None


_DIGITS_RE = re.compile(r"\D+")
#: An ISO 3166-1 alpha-2 country, optionally followed by an ISO 3166-2
#: subdivision ("US-NJ").
_JURISDICTION_RE = re.compile(r"^([A-Z]{2})(?:-[A-Z0-9]{1,3})?$")


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
    global _index, _norm_index, _variants
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
            # Exact spellings first, derived forms after, so a value EITI
            # files verbatim resolves to that spelling rather than to whichever
            # variant happened to come first in the file (Phase 306: EITI files
            # Kansanshi's TPIN as both "1,001,602,517" and "1001602517").
            bucket: dict[str, str] = {}
            for ident in idents:
                bucket.setdefault(_norm_forms(ident)[0], ident)
            for ident in idents:
                for form in _norm_forms(ident)[1:]:
                    bucket.setdefault(form, ident)
            _norm_index[cc] = bucket
        _variants = _build_variants(_index)
    return _index, _norm_index


def _variant_key(ident: str) -> str:
    """The key under which two spellings of one number are the same record.

    Digits without leading zeros, for an identification that is digits plus
    punctuation (``1,001,602,517`` / ``1001602517``, ``00245717`` /
    ``245717``, ``400 182 426`` / ``400182426``). An identification carrying
    any letter keys on itself: ``SC123456`` and ``123456`` are two different
    Companies House companies.
    """
    value = (ident or "").strip().upper()
    if any(ch.isalpha() for ch in value):
        return value
    return _DIGITS_RE.sub("", value).lstrip("0") or value


def _build_variants(
    index: dict[str, dict[str, list[dict[str, Any]]]],
) -> dict[str, dict[str, list[str]]]:
    """cc -> ident -> all spellings of it in that country's bucket.

    EITI files one company's number in several spellings across reporting
    years -- 212 numbers in 16 country buckets of the committed index, from
    thousands separators (IQ, ZM), dropped leading zeros (GB, AM, GY) and
    spaced groups (MZ, KZ). The organisation records of every spelling are one
    company's, so a match reads them all; before Phase 306 a card showed only
    the years filed under the spelling that matched.
    """
    out: dict[str, dict[str, list[str]]] = {}
    for cc, idents in index.items():
        groups: dict[str, list[str]] = {}
        for ident in idents:
            groups.setdefault(_variant_key(ident), []).append(ident)
        out[cc] = {
            ident: group for group in groups.values() for ident in group
        }
    return out


def _organisations(cc: str, ident: str) -> list[dict[str, Any]]:
    """Every organisation record EITI files under any spelling of ``ident``."""
    index, _ = _get_index()
    bucket = index.get(cc) or {}
    spellings = ((_variants or {}).get(cc) or {}).get(ident) or [ident]
    seen: set[str] = set()
    orgs: list[dict[str, Any]] = []
    for spelling in spellings:
        for org in bucket.get(spelling) or []:
            key = str(org.get("id") or id(org))
            if key not in seen:
                seen.add(key)
                orgs.append(org)
    return orgs


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
    global _index, _norm_index, _us_ein_by_lei, _variants
    _index = None
    _norm_index = None
    _variants = None
    _us_ein_by_lei = None


def _country_code(jurisdiction: str) -> str:
    """The ISO 3166-1 alpha-2 country of a GLEIF ``jurisdiction`` value.

    GLEIF publishes a jurisdiction as a plain country code for most of the
    world, but for the US it publishes the ISO 3166-2 **subdivision** the
    entity is incorporated in: Exxon Mobil is ``US-NJ``, and a sample of US
    records returns ``US-DE``, ``US-WY``, ``US-SD``, ``US-PA``. EITI keys its
    organisation index by country, so a subdivision has to be reduced to its
    country before anything can match.

    This is why US EITI matching stayed dead after Phase 155 shipped the
    crosswalk: the pipeline hands this adapter ``ctx.jurisdiction`` verbatim,
    and ``US-NJ`` finds no bucket. Sampled across eleven EITI implementing
    countries on 2026-09-04 (GB, NO, NL, NG, ID, MN, KZ, PH, GH, PE, US),
    **only the US carries subdivisions** — which is exactly why the one
    country the crosswalk exists for is the one country it broke on.

    Anything that is not a recognisable country-or-subdivision code is
    returned unchanged, so an unexpected value fails to match rather than
    being coerced into some other country's bucket.
    """
    value = (jurisdiction or "").strip().upper()
    match = _JURISDICTION_RE.match(value)
    return match.group(1) if match else value


def _match_identification(country: str, registered_as: str) -> str | None:
    """Return the EITI identification matching a GLEIF registeredAs value."""
    index, norm_index = _get_index()
    cc = _country_code(country)
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
        zm_tpin: str = "",
    ) -> dict[str, Any] | None:
        """Match a company against the EITI index.

        Tries the GLEIF anchor's ``registeredAs`` first, then a derived
        ``us_ein`` (EITI's US identifications are federal EINs, not the
        state-registry numbers GLEIF publishes as ``registeredAs``, so US
        subjects only join via the EIN), then a derived ``zm_tpin`` (EITI's
        Zambian identifications are ZRA TPINs; GLEIF files the PACRA number).
        Matching is country-scoped and punctuation-insensitive
        (``42-1638663`` == ``421638663``), so a TPIN can only ever join the
        ZM bucket. The bundle's ``matched_via`` names the key that joined.

        Returns ``None`` when the company is not in the EITI data. On a
        match, returns the bundle: organisation records from the artifact
        plus live payment aggregates for the most recent reporting years
        (payments only when live mode is enabled — the organisation match
        itself is offline data and always available).
        """
        # Normalised here, not only inside _match_identification, because cc
        # also becomes the bundle's ``country`` and the ``CC:ident`` hit id --
        # a bundle stamped "US-NJ" would miss _EITI_IDENTIFIER_KEY_BY_COUNTRY
        # and lose the us_ein corroboration key on the way to the report.
        cc = _country_code(jurisdiction)
        ident: str | None = None
        matched_via: str | None = None
        for via, value in (
            ("registered_as", registered_as),
            ("us_ein", us_ein),
            ("zm_tpin", zm_tpin),
        ):
            if not value:
                continue
            ident = _match_identification(cc, value)
            if ident is not None:
                matched_via = via
                break
        if ident is None:
            return None
        return await self._build_bundle(
            cc, ident, legal_name=legal_name, matched_via=matched_via
        )

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by ``CC:identification`` hit id (deepen / retry path)."""
        cc, _, ident = hit_id.partition(":")
        bundle = await self._build_bundle(_country_code(cc), ident.strip())
        if bundle is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return bundle

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_bundle(
        self,
        cc: str,
        ident: str,
        legal_name: str = "",
        matched_via: str | None = None,
    ) -> dict[str, Any] | None:
        orgs = _organisations(cc, ident)
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
            # Reporting years whose payment rows exceeded the page bound, so
            # ``total_usd`` and ``streams`` understate them. Empty is the norm.
            "truncated_years": [
                str(ry.get("year") or ry.get("organisation_id"))
                for ry in revenue_years if ry.get("truncated")
            ],
            "is_stub": False,
        }
        if matched_via:
            bundle["matched_via"] = matched_via
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
                        payload = await self._fetch_revenue_pages(client, org_id)
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
            count = payload.get("count")
            try:
                count_int = int(count) if count is not None else None
            except (TypeError, ValueError):
                count_int = None
            return {
                "year": org.get("year"),
                "organisation_id": org_id,
                "total_usd": total,
                "rows": rows,
                # What the API says it holds for this organisation-year, and
                # whether the pages read reached it. A consumer that sums
                # ``rows`` can tell a complete year from a bounded one.
                "rows_available": count_int,
                "truncated": bool(count_int is not None and count_int > len(rows)),
            }

        return list(await asyncio.gather(*[one(o) for o in orgs]))

    async def _fetch_revenue_pages(self, client: Any, org_id: str) -> dict[str, Any]:
        """Read an organisation's revenue rows across up to ``_MAX_REVENUE_PAGES``.

        Page one is the documented filter form; later pages follow the
        ``next.href`` the API returns verbatim. Returns one payload in the
        single-page shape — ``data`` merged across pages, ``count`` as the API
        reported it — so the cache and the aggregation above see no difference
        between a one-page and a four-page organisation.
        """
        response = await client.get(
            f"{_API_BASE}/revenue",
            params={"organisation": org_id, "limit": _REVENUE_PAGE_SIZE},
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        first = response.json()
        data = list(first.get("data") or [])
        count = first.get("count")
        pages = 1
        next_href = ((first.get("next") or {}).get("href") or "").strip()
        while next_href and pages < _MAX_REVENUE_PAGES:
            response = await client.get(
                next_href, headers={"Accept": "application/json"}
            )
            response.raise_for_status()
            page = response.json()
            data.extend(page.get("data") or [])
            pages += 1
            next_href = ((page.get("next") or {}).get("href") or "").strip()
        return {"data": data, "count": count}
