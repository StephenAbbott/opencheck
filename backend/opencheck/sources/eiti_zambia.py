"""Zambia EITI data portal adapter — ESG category.

The Zambia EITI (ZEITI) "Fusion Portal" (https://portal.zambiaeiti.org/)
publishes Zambia Revenue Authority tax receipts, the EITI reconciliation
payment report, company employment figures, the mining-rights cadastre and a
WARMA water-permit offences list. This adapter serves the slice of it that
OpenCheck can tie to an LEI: payments, employment, mining rights and water
offences, for each Zambian LEI holder whose name the portal files.

It sits beside the ``eiti`` adapter rather than inside it. ``eiti`` reads
EITI International's global summary data; this reads Zambia's own portal,
which is richer (ZRA's receipts by tax type, the cadastre, WARMA) and more
recent (to 2024 for payments, Q2 2025 for rights).

The join, and what is asserted
------------------------------
Every portal row is keyed on the ZRA **TPIN**; GLEIF's Zambian records carry
the **PACRA number**, and nothing relates the two. So
``scripts/build_eiti_zambia_index.py`` matches each Zambian LEI record to the
ZRA tables **by normalised legal name** to find its TPIN, then joins every
other table on that TPIN — offline, once. The committed artifact
``opencheck/data/eiti_zambia_index.json.gz`` is keyed by LEI, and
``fetch_by_lei`` is a dict lookup with no network on the hot path.

Identifier corroboration (``CLAUDE.md``): the portal publishes the TPIN on
every row it files for the company, so ``zm_tpin`` is asserted. The LEI is
OpenCheck's name match and is **not** asserted; the mapper records it as an
``identifying`` annotation instead.

Scope (Stephen, 7 October 2026): the LEIs that can be matched by name, and no
others. Lafarge Cement Zambia Plc is left out — its LEI name matches nothing
in the ZRA tables, because the company trades as Chilanga Cement Plc.

Nothing here is ownership. The rights cadastre gives a holder's share of a
licence, which is a share in a mining right, not in the company.

Licence: ZEITI's Open Data Policy (2016) adopts the Open Definition — free to
use, modify and share for any purpose — but names no licence; the policy
itself is cited. The 2026 EITI Validation scores Zambia "Very good" on
Requirement 7.2 (data accessibility and open data).
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .. import provenance
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.eiti_zambia import EitiZambiaBundle

log = logging.getLogger(__name__)

PORTAL_URL = "https://portal.zambiaeiti.org/"
POLICY_URL = "https://eiti.org/sites/default/files/attachments/zambia_open_data_policy.pdf"

#: Committed, LEI-keyed index (built by scripts/build_eiti_zambia_index.py).
_INDEX_PATH = Path(
    os.environ.get("EITI_ZAMBIA_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "eiti_zambia_index.json.gz")
)

_index: dict[str, dict[str, Any]] | None = None
_meta: dict[str, Any] | None = None


def _load() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load the committed index (cached module singleton)."""
    global _index, _meta
    if _index is None:
        try:
            with gzip.open(_INDEX_PATH, "rt", encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("index") or {}
            _index = {
                str(k).strip().upper(): v
                for k, v in raw.items()
                if len(str(k).strip()) == 20
            }
            _meta = data.get("meta") or {}
            log.info("Zambia EITI portal index loaded: %s LEI-matched companies", len(_index))
        except Exception as exc:  # noqa: BLE001
            log.warning("Zambia EITI portal index unavailable: %s", exc)
            _index = {}
            _meta = {}
    return _index, _meta or {}


def _load_and_declare() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load the index and declare the payload as curated, never live.

    The index is a committed harvest, not a call to the portal, so without an
    explicit declaration the provenance would resolve to 'stub'.
    """
    index, meta = _load()
    harvested_at: datetime | None = None
    built = str(meta.get("built") or "").strip()
    if built:
        try:
            harvested_at = datetime.fromisoformat(built)
            if harvested_at.tzinfo is None:
                harvested_at = harvested_at.replace(tzinfo=timezone.utc)
        except ValueError:
            harvested_at = None
    provenance.record_curated(
        "Harvest of the Zambia EITI data portal committed to the repository",
        harvested_at=harvested_at,
    )
    return index, meta


def tpin_for_lei(lei: str) -> str:
    """The ZRA TPIN this index ties to an LEI, or ``""`` when there is none.

    Phase 306: the ``eiti`` adapter holds EITI International's Zambian
    identifications keyed by TPIN, while GLEIF files a Zambian company's PACRA
    number, so ``registeredAs`` never joins it. This index already resolved
    the TPIN for each name-matched LEI; ``routers/lookup.py::_build_derived``
    reads it into ``ctx.derived["zm_tpin"]``, as ``us_ein_for_lei`` does for
    the US. Read from this index rather than a second committed crosswalk so
    the two cannot drift.

    The link LEI → TPIN is the build's name match (graded medium); the TPIN
    itself is the portal's own key. A ``name_only`` record has no TPIN and
    gives ``""``. Declares no provenance: nothing is fetched.
    """
    index, _ = _load()
    record = index.get((lei or "").strip().upper()) or {}
    tpins = [str(t).strip() for t in record.get("tpins") or [] if str(t).strip()]
    return tpins[0] if tpins else ""


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singletons so a fresh index is loaded."""
    global _index, _meta
    _index = None
    _meta = None


class EitiZambiaAdapter(SourceAdapter):
    """Zambia EITI data portal — ESG category."""

    id = "eiti_zambia"

    #: LEI-keyed source, dispatched directly in routers/lookup.py beside the
    #: other LEI-keyed offline indexes (eiti_soe, eiti_bo), not by RA code.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        index, meta = _load()
        return SourceInfo(
            id=self.id,
            name="Zambia EITI data portal",
            homepage=PORTAL_URL,
            description=(
                "Zambia Revenue Authority tax receipts, the EITI reconciliation "
                "payment report, employment, mining rights and WARMA water-permit "
                "offences from the Zambia EITI portal, for Zambian LEI holders "
                f"matched to the portal by name ({len(index)} of "
                f"{meta.get('gleif_zm_records') or 'the'} Zambian LEIs). Rows are "
                "keyed on the ZRA TPIN; GLEIF files the PACRA number, so the "
                "link is a name match made offline."
            ),
            license="ZEITI Open Data Policy (2016) — open data, no named licence",
            attribution=(
                "Zambia Extractive Industries Transparency Initiative (ZEITI), "
                "portal.zambiaeiti.org; data from the Zambia Revenue Authority, "
                "the Ministry of Mines and Minerals Development and WARMA"
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(index),
            category="esg",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # LEI-keyed source; free-text search intentionally empty.
        return []

    def covers_lei(self, lei: str) -> bool:
        """Whether the committed index holds this LEI.

        Absence means OpenCheck could not tie the LEI to the portal, not that
        the company made no payments. Reads without declaring provenance:
        nothing has been fetched.
        """
        index, _ = _load()
        return (lei or "").strip().upper() in index

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the portal bundle for a LEI, or ``None`` if absent."""
        lei_norm = (lei or "").strip().upper()
        index, meta = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return None
        return self._build_bundle(lei_norm, record, meta)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        lei_norm = (hit_id or "").strip().upper()
        index, meta = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return self._build_bundle(lei_norm, record, meta)

    def _build_bundle(
        self, lei: str, record: dict[str, Any], meta: dict[str, Any]
    ) -> dict[str, Any]:
        tpins = [str(t) for t in record.get("tpins") or [] if t]
        datasets = meta.get("datasets") or {}
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": lei,
            "lei": lei,
            "is_stub": False,
            **record,
            # Corroboration rule: the TPIN is the portal's own key for the rows
            # returned. One company, one TPIN in every case seen; if a build
            # ever finds two, both are carried and the first is asserted.
            "identifiers": {"zm_tpin": tpins[0]} if tpins else {},
            "datasets": {
                code: {**(datasets.get(code) or {}), "url": f"{PORTAL_URL}datasets/{code}"}
                for code in record.get("datasets_used") or []
            },
            "portal_url": PORTAL_URL,
            "licence": meta.get("licence"),
            "licence_url": meta.get("licence_url") or POLICY_URL,
            "built": meta.get("built"),
        }
        validate_raw("eiti_zambia", EitiZambiaBundle, bundle)
        return bundle
