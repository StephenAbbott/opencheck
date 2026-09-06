"""EITI State-Owned Enterprises (SOE) Database adapter — CDD category.

This is a **separate** EITI product from the ``eiti`` adapter (payments to
governments, via ``eiti.org/api/v2.0``) and from ``eiti_assessment`` (the
Company Assessment of EITI's supporting companies). This one is the **SOE
roster**: the state-owned enterprises reported through the EITI, with an
explicit state-ownership classification that lives nowhere else in OpenCheck.

Its distinctive value is a **state-ownership context signal**
(``STATE_CONTROLLED``, which falls out of the BODS shape the mapper emits)
plus SOE enrichment, rather than a second payments feed.

Repointed at the new global database (Phase 172)
-------------------------------------------------
The roster now comes from ``eiti-database.eiti.org`` → ``view_soeList``:
**194** state-owned enterprises rather than 125, covering **2017–2024** rather
than 2017–2022, with name variants deduplicated by EITI's own UUIDv5 key. The
old host is still live and ``build_eiti_soe_index.py --source old`` still reads
it, kept as a fallback until the new index has proven itself.

How the lookup works (the ClimateTRACE/GEM pattern)
---------------------------------------------------
The SOE database carries **no native LEI**, while OpenCheck is anchored
end-to-end on the LEI. So identity resolution is done **once, offline**, by
``scripts/build_eiti_soe_index.py``, which commits a gzipped, LEI-keyed
artifact at ``opencheck/data/eiti_soe_index.json.gz``. At runtime this adapter
loads that index and answers ``fetch_by_lei`` as a dict lookup — no live call
on the hot path. With ``allow_live`` on, a matched hit is deepened with payment
rows from the new database's ``view_payments_detailed``, filtered on
``eiti_id_company``.

Coverage is **2 of 194**, and that is the data rather than the matching
-----------------------------------------------------------------------
Every one of the 194 joins to ``metadata_companies`` with ``legal_entity_id``,
``open_corporates_id`` and ``estma_id`` all empty, and a name-and-country search
against GLEIF returns no candidate at all for 183 of them. Re-running the worst
40 without the country filter produced one extra candidate, and it was wrong.
These companies do not hold LEIs. The adapter stays because the classification
is worth having and because EITI may yet publish identifiers — see the
"Repoint EITI SOE index" ticket, which set that expectation before the number
was known.

Identifier corroboration — **nothing is asserted**
---------------------------------------------------
The SOE database does not publish the LEI (OpenCheck derives it at build time),
so ``lei`` was always barred by the corroboration rule in ``CLAUDE.md``.
``eiti_soe_id`` used to be asserted as "the identifier EITI itself publishes"
and **no longer is**: EITI regenerated its entire company id space in this
release — UUIDv4 to a UUIDv5 over a normalised name, now prefixed
``eiti_id_company:`` — so the value OpenCheck published cannot be looked up in
the database it came from. A key a source can regenerate wholesale is a
deduplication key, not a registry number. It stays in the bundle because the
live payments query is keyed on it, and it stays out of
``SourceHit.identifiers`` and out of the BODS statements.

No API key required. Licence: EITI content-use policy — free republication with
credit to "EITI International Secretariat, eiti.org". (Note: do **not** ingest
the OpenSanctions ``eiti_soe`` mirror instead — it adds a CC-BY-NC restriction
that blocks any paid tier; going direct to EITI keeps the licence clean.)
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from .. import degradation, provenance
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.eiti_soe import EitiSoeBundle

log = logging.getLogger(__name__)

# Datasette JSON API for optional live enrichment of a matched hit.
# The new global database. The old host (soe-database.eiti.org) is still live
# but its `companies.json?eiti_id_company__exact=` filter only resolves the old
# UUIDv4 ids, which this index no longer carries — see the repoint note in
# scripts/build_eiti_soe_index.py.
_API_BASE = "https://eiti-database.eiti.org/eiti_database"
_QUERY_URL = f"{_API_BASE}/-/query.json"
_CACHE_NS = "eiti_soe"

#: Committed, LEI-keyed index artifact (built by scripts/build_eiti_soe_index.py).
#: Overridable via env for tests / alternative snapshots.
_INDEX_PATH = Path(
    os.environ.get("EITI_SOE_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "eiti_soe_index.json.gz")
)

# Lazy module-level singleton (LEI -> index record). Tests may set this directly.
_index: dict[str, dict[str, Any]] | None = None
#: Upstream extract date of the committed index (``meta.source_snapshot``),
#: recorded as the snapshot's retrieval time on every match.
_index_snapshot: str | None = None


def _get_index() -> dict[str, dict[str, Any]]:
    """Load the committed LEI-keyed SOE index (cached in a module singleton)."""
    global _index, _index_snapshot
    if _index is None:
        try:
            with gzip.open(_INDEX_PATH, "rt", encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("index") or {}
            # Normalise keys to upper-case 20-char LEIs.
            _index = {
                str(k).strip().upper(): v
                for k, v in raw.items()
                if len(str(k).strip()) == 20
            }
            meta = data.get("meta") or {}
            _index_snapshot = meta.get("source_snapshot") or meta.get("built")
            log.info(
                "EITI SOE index loaded: %s SOEs resolved to LEI (%s source snapshot)",
                meta.get("resolved_lei", len(_index)),
                meta.get("source_snapshot"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI SOE index unavailable: %s", exc)
            _index = {}
    return _index


def _record_index_provenance() -> None:
    """Declare that this answer came from the committed SOE index.

    Without this the adapter records nothing on the index match, so a bundle
    resolves ``live`` on the strength of the payment rows alone — over-claiming
    the freshness of its central assertion, which is that the company is an SOE
    and comes from a snapshot built on ``meta.source_snapshot``. Provenance
    takes the worst liveness across a fetch, so recording the snapshot here is
    what makes the bundle report itself as only as fresh as its stalest part.
    (The mirror image of the Ariregister bug in PR #153, which under-claimed.)
    """
    built_at: datetime | None = None
    if _index_snapshot:
        try:
            built_at = datetime.fromisoformat(_index_snapshot).replace(tzinfo=timezone.utc)
        except ValueError:
            built_at = None
    provenance.record_snapshot(
        built_at,
        "EITI SOE Database index committed to the repository",
    )


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singleton so a fresh index is loaded."""
    global _index
    _index = None



def _failure_label(exc: BaseException) -> str:
    """Short label for degradation.reason_for_failure, from a swallowed error."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status is not None:
        return f"HTTP {status}"
    return type(exc).__name__

class EitiSoeAdapter(SourceAdapter):
    """EITI State-Owned Enterprises Database adapter — CDD category."""

    id = "eiti_soe"

    #: LEI-keyed source. Dispatched directly in routers/lookup.py alongside the
    #: other LEI-keyed sources (opensanctions, climatetrace, bods_gleif), not via
    #: an RA-code deriver.
    lookup_timeout_s = 20.0

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="EITI State-Owned Enterprises Database",
            homepage="https://eiti-database.eiti.org/eiti_database/view_soeList",
            description=(
                "194 state-owned enterprises reported through the EITI across "
                "39 implementing countries, 2017–2024, with a state-ownership "
                "classification, sectors, audited-financial-statement links and "
                "stock listings. Surfaces a state-ownership context signal by "
                "LEI. EITI publishes no LEI, OpenCorporates id or national "
                "registration number for any of them, so coverage is limited to "
                "the enterprises whose names match GLEIF exactly."
            ),
            license="EITI open data (free reuse with attribution)",
            attribution="EITI International Secretariat, eiti.org",
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            category="cdd",
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed source; free-text search intentionally empty
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # LEI-based lookup (called by the lookup pipeline)
    # ------------------------------------------------------------------

    def covers_lei(self, lei: str) -> bool:
        """Whether the committed EITI SOE index holds this LEI.

        The lookup pipeline asks before dispatching, so a company this file
        cannot possibly describe is never announced as a source being queried
        and never counted in "N of N sources answered". Absence means the LEI is not in the index, which is not the same as evidence the company is not state-owned — this
        governs whether the source is *applicable*, and says nothing about the
        company.

        Reads the index without declaring provenance: nothing has been fetched.
        """
        return (lei or "").strip().upper() in _get_index()

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the SOE bundle for a LEI, or ``None`` when not an SOE.

        The match itself is offline (committed index). When live mode is on, a
        matched hit is deepened with payment/context rows from the Datasette API.
        """
        lei_norm = (lei or "").strip().upper()
        index = await asyncio.to_thread(_get_index)
        record = index.get(lei_norm)
        if record is None:
            return None
        _record_index_provenance()
        return await self._build_bundle(lei_norm, record)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        lei_norm = (hit_id or "").strip().upper()
        index = await asyncio.to_thread(_get_index)
        record = index.get(lei_norm)
        if record is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        _record_index_provenance()
        return await self._build_bundle(lei_norm, record)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_bundle(
        self, lei: str, record: dict[str, Any]
    ) -> dict[str, Any]:
        soe = dict(record.get("soe") or {})
        match_method = record.get("match_method")
        match_confidence = record.get("match_confidence") or "medium"

        payments: list[dict[str, Any]] = []
        if self.info.live_available and soe.get("eiti_id_company"):
            payments = await self._fetch_payments(str(soe["eiti_id_company"]))

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "lei": lei,
            "entity_name": soe.get("company_name") or soe.get("original_company_name"),
            "name_variants": soe.get("name_variants") or [],
            "gleif_legal_name": record.get("gleif_legal_name"),
            "country_name": soe.get("country_name"),
            "is_state_owned": True,
            "country": soe.get("country") or soe.get("iso_alpha2"),
            "sector": soe.get("sector"),
            "commodities": soe.get("commodities") or [],
            "company_type": soe.get("company_type"),
            "government_entity": soe.get("government_entity"),
            "opencorporates_id": soe.get("opencorporates_id"),
            "eiti_id_company": soe.get("eiti_id_company"),
            "eiti_id_government": soe.get("eiti_id_government"),
            "audited_financial_statement": soe.get("audited_financial_statement"),
            "public_listing_or_website": soe.get("public_listing_or_website"),
            "years": soe.get("years") or [],
            "match_method": match_method,
            "match_confidence": match_confidence,
            "payments": payments,
            "is_stub": False,
        }
        validate_raw("eiti_soe", EitiSoeBundle, bundle)
        return bundle

    async def _fetch_payments(self, eiti_id_company: str) -> list[dict[str, Any]]:
        """Best-effort live payment/context rows for one SOE (Datasette API).

        Uses the documented Datasette filter form
        ``…/companies.json?eiti_id_company__exact=<id>&_shape=array&_size=max``.
        Failures are swallowed — the offline match is the source of truth.
        """
        cache_key = f"{_CACHE_NS}/companies/{eiti_id_company}"
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        try:
            async with build_client() as client:
                response = await client.get(
                    _QUERY_URL,
                    params={
                        # Filtered on the id, never a name join: joining
                        # view_payments_detailed to view_soeList on name trips
                        # `sql_time_limit_ms` (2.5 s) while the id filter
                        # answers in well under one.
                        "sql": (
                            "select year, revenue_stream_name, payment_value, "
                            "payment_value_usd, currency_code, project_name, "
                            "gov_entity_name from view_payments_detailed "
                            "where eiti_id_company = :cid "
                            "order by year desc limit 200"
                        ),
                        ":cid": eiti_id_company,
                        "_shape": "objects",
                    },
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                body = response.json()
                rows = body.get("rows", []) if isinstance(body, dict) else body
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI SOE payment fetch failed for %s: %s", eiti_id_company, exc)
            # The SOE classification comes from the committed index and still
            # stands; the payment rows do not. Saying so is the difference
            # between "this SOE reported no payments" and "we could not ask".
            degradation.record(
                self.id,
                "The EITI SOE database did not answer "
                f"({type(exc).__name__}); the state-ownership classification "
                "stands, but payment rows are missing.",
                reason=degradation.reason_for_failure(_failure_label(exc)),
            )
            rows = []
        out: list[dict[str, Any]] = []
        for r in rows if isinstance(rows, list) else []:
            out.append(
                {
                    "year": r.get("year"),
                    "revenue_stream": r.get("revenue_stream_name"),
                    "revenue_value": r.get("payment_value_usd") or r.get("payment_value"),
                    "currency": r.get("currency_code"),
                    "project": r.get("project_name"),
                }
            )
        self._cache.put(cache_key, out)
        return out
