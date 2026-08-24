"""Pooled EITI national beneficial ownership registers adapter — CDD category.

One source for the beneficial ownership registers of EITI implementing
countries, rather than one adapter per register. The pooled-company universe
is small — smaller still once filtered to LEI holders — so a per-register
adapter would put a whole source card behind one or two companies each
(architecture decision recorded on the Notion ticket, 2026-08-18).

Registers pooled at launch (each with a different sourcing approach — see
``scripts/build_eiti_bo_index.py``):

* **DRC** — ITIE-RDC Registre des propriétaires effectifs (bulk XLSX export;
  the only EITI BO register anywhere with a bulk download).
* **Armenia** — State Register BO declarations at ``old.e-register.am``
  (per-declaration **BODS v0.2 JSON**, upconverted to v0.4 by the mapper).
* **Nigeria** — the committed ``cac_nigeria`` PSC harvest filtered to NEITI
  solid-minerals-covered companies (the filter evidence is dated per record).
* **Indonesia** — slot reserved; the AHU API is in maintenance.

Excluded for now: Tajikistan (all-rights-reserved licence) and Trinidad &
Tobago (register frozen ~2021).

Offline / vendored (why)
------------------------
None of the pooled registers offers an API. Harvesting happens once, offline,
in ``scripts/build_eiti_bo_index.py``; the committed, **LEI-keyed** artifact
``opencheck/data/eiti_bo_index.json.gz`` is loaded here and ``fetch_by_lei``
is a dict lookup — no network on the hot path. The index is LEI-only by
design: unmatched register companies stay in the committed raw harvests and
are counted in the artifact's manifest.

Identifier corroboration
------------------------
No pooled register publishes the LEI — OpenCheck derives it at build time
(registration-number equality against GLEIF first, normalised-name equality
as a lower-confidence fallback). Per the corroboration rule in ``CLAUDE.md``,
this adapter must **not** assert ``lei`` in ``SourceHit.identifiers``; it
asserts only identifiers the register itself publishes (``am_regnum`` /
``am_tin`` / ``ng_cac_rc`` / ``cd_nif``).

Licence: public registers; the DRC and Armenia registers state no licence and
are included with attribution (decision recorded 2026-08-18). Per-register
licence notes ride in the artifact manifest and surface in exports.
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
from .schemas.eiti_bo import EitiBoBundle

log = logging.getLogger(__name__)

#: Committed, LEI-keyed pooled index (built by scripts/build_eiti_bo_index.py).
#: Overridable via env for tests / alternative snapshots.
_INDEX_PATH = Path(
    os.environ.get("EITI_BO_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "eiti_bo_index.json.gz")
)

# Lazy module-level singletons. Tests may reset via _reset_index_for_tests().
_index: dict[str, dict[str, Any]] | None = None
_meta: dict[str, Any] | None = None


def _load() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load the committed LEI-keyed pooled index (cached module singleton)."""
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
            log.info(
                "EITI BO pooled index loaded: %s LEI-matched entities across %s registers",
                _meta.get("entities", len(_index)),
                len(_meta.get("registers") or {}),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI BO pooled index unavailable: %s", exc)
            _index = {}
            _meta = {}
    return _index, _meta or {}


def _load_and_declare() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load the index and declare the payload as curated, never live.

    The index is a committed harvest, not a call to any register, so without
    an explicit declaration the provenance would resolve to 'stub' and
    under-claim (the cac_nigeria precedent).
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
        "Pooled harvest of EITI national BO registers committed to the repository",
        harvested_at=harvested_at,
    )
    return index, meta


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singletons so a fresh index is loaded."""
    global _index, _meta
    _index = None
    _meta = None


class EitiBoAdapter(SourceAdapter):
    """Pooled EITI national beneficial ownership registers — CDD category."""

    id = "eiti_bo"

    #: LEI-keyed source. Dispatched directly in routers/lookup.py alongside the
    #: other LEI-keyed sources (eiti_soe, cac_nigeria, bods_gleif), not via an
    #: RA-code deriver.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        index, meta = _load()
        registers = meta.get("registers") or {}
        pooled = sum(
            int(r.get("companies_harvested") or 0) for r in registers.values()
        )
        return SourceInfo(
            id=self.id,
            name="EITI countries — national beneficial ownership registers",
            homepage="https://eiti.org/beneficial-ownership",
            description=(
                "Beneficial ownership of extractive companies pooled from the "
                "national BO registers of EITI implementing countries — DRC "
                "(ITIE-RDC register, bulk export), Armenia (State Register "
                "BODS v0.2 declarations) and Nigeria (CAC PSC register, NEITI "
                f"solid-minerals subset). Curated offline harvest of {pooled} "
                f"companies, of which {len(index)} resolve to an LEI (the "
                "launch index is LEI-only). Indonesia slot reserved."
            ),
            license=(
                "Public registers; DRC and Armenia state no licence — included "
                "with attribution (see the artifact manifest for per-register terms)"
            ),
            attribution=(
                "ITIE-RDC (itierdc.net); Ministry of Justice of Armenia "
                "(old.e-register.am); Corporate Affairs Commission, Nigeria "
                "(bor.cac.gov.ng); seed lists: EITI Armenia (eiti.am), NEITI"
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(index),
            category="cdd",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # Identifier-keyed (LEI) source; free-text search intentionally empty.
        return []

    def covers_lei(self, lei: str) -> bool:
        """Whether the committed pooled EITI BO index holds this LEI.

        The lookup pipeline asks before dispatching, so a company this file
        cannot possibly describe is never announced as a source being queried
        and never counted in "N of N sources answered". Absence means the LEI is not in the pooled index, which is not the same as evidence of no register-published ownership — this
        governs whether the source is *applicable*, and says nothing about the
        company.

        Reads the index without declaring provenance: nothing has been fetched.
        """
        index, _ = _load()
        return (lei or "").strip().upper() in index

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the pooled-register bundle for a LEI, or ``None`` if absent."""
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
        register_id = str(record.get("register_id") or "")
        register_meta = (meta.get("registers") or {}).get(register_id) or {}
        # Corroboration rule: assert only identifiers the register itself
        # publishes — never the derived LEI.
        identifiers = {
            k: str(v) for k, v in (record.get("local_ids") or {}).items() if v
        }
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": lei,
            "lei": lei,
            "is_stub": False,
            "record": record,
            "identifiers": identifiers,
            "register_id": register_id,
            "register_name": register_meta.get("name"),
            "register_url": register_meta.get("url"),
            "register_licence": register_meta.get("licence"),
        }
        validate_raw("eiti_bo", EitiBoBundle, bundle)
        return bundle
