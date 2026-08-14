"""Nigeria CAC beneficial ownership register adapter — CDD category.

Serves beneficial ownership (Persons with Significant Control) data from the
Nigerian **Corporate Affairs Commission** public register at
``bor.cac.gov.ng`` — Africa's first public beneficial ownership register (CAC;
Open Ownership partner country; BODS-relevant). GLEIF Registration Authority
code ``RA000469``.

Offline / vendored (why)
------------------------
The CAC register is fully public, but its **official** API is restricted to
Nigerian government / law-enforcement agencies — there is no sanctioned
third-party API. So rather than scrape the site's private JSON API on the hot
path, a small curated example set (10 LEI-anchored Nigerian companies) is
harvested **once, offline** by ``scripts/build_cac_nigeria_index.py`` and
committed as an LEI-keyed index at ``opencheck/data/cac_nigeria_psc.json``.

At runtime this adapter loads that committed index and answers ``fetch_by_lei``
as a dict lookup — no network. ``bods.map_cac_nigeria`` maps each record to
BODS v0.4. When a live adapter is built later (pending engagement with the CAC
and its technical partner Oasis Management), it can return the same per-record
shape and reuse the same mapper.

Identifier corroboration
------------------------
The CAC BOR is keyed on the company RC number and does **not** publish the LEI —
OpenCheck *derives* it at build time via GLEIF (``registeredAs`` +
``registeredAt=RA000469``). Per the corroboration rule in ``CLAUDE.md`` /
``routers/lookup.py``, this adapter therefore must **not** assert ``lei`` in
``SourceHit.identifiers``; it asserts only the identifier the CAC itself
publishes (the RC number, ``ng_cac_rc``).

Licence: the CAC BOR is a public register (``bor.cac.gov.ng``); no
non-commercial restriction applies.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .. import provenance
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

log = logging.getLogger(__name__)

#: Committed, LEI-keyed index (built by scripts/build_cac_nigeria_index.py).
#: Overridable via env for tests / alternative snapshots.
_INDEX_PATH = Path(
    os.environ.get("CAC_NIGERIA_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "cac_nigeria_psc.json")
)

# Lazy module-level singleton (LEI -> record). Tests may set this directly.
_index: dict[str, dict[str, Any]] | None = None

#: Date the committed index was harvested from the CAC register, read from the
#: file's own ``meta.harvested``. This is a genuine retrieval date — unlike the
#: file's mtime, which only records when git wrote it to this machine — so it is
#: what the BODS ``source.retrievedAt`` reports for this adapter.
_harvested_at: datetime | None = None


def _load_and_declare() -> dict[str, dict[str, Any]]:
    """Load the index and declare the payload as curated, never live.

    The index is a committed example set, not a call to the register. This
    fetch touches neither the HTTP client nor the response cache, so without an
    explicit declaration it would resolve to 'stub' and under-claim.
    """
    index = _get_index()
    provenance.record_curated(
        "Curated CAC BOR example set committed to the repository",
        harvested_at=_harvested_at,
    )
    return index


def _get_index() -> dict[str, dict[str, Any]]:
    """Load the committed LEI-keyed CAC index (cached in a module singleton)."""
    global _index
    if _index is None:
        try:
            with open(_INDEX_PATH, encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("index") or {}
            _index = {
                str(k).strip().upper(): v
                for k, v in raw.items()
                if len(str(k).strip()) == 20
            }
            meta = data.get("meta") or {}
            global _harvested_at
            harvested = str(meta.get("harvested") or "").strip()
            if harvested:
                try:
                    _harvested_at = datetime.fromisoformat(harvested).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    _harvested_at = None
            log.info(
                "CAC Nigeria index loaded: %s entities (%s harvest)",
                meta.get("entities", len(_index)),
                meta.get("harvested"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("CAC Nigeria index unavailable: %s", exc)
            _index = {}
    return _index


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singleton so a fresh index is loaded."""
    global _index
    _index = None


class CacNigeriaAdapter(SourceAdapter):
    """Nigeria CAC beneficial ownership register adapter — CDD category."""

    id = "cac_nigeria"

    #: LEI-keyed source. Dispatched directly in routers/lookup.py alongside the
    #: other LEI-keyed sources (bods_gleif, eiti_soe), not via an RA-code deriver.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Nigeria CAC — Persons with Significant Control register",
            homepage="https://bor.cac.gov.ng",
            description=(
                "Beneficial ownership (Persons with Significant Control) from "
                "Nigeria's Corporate Affairs Commission public register — "
                "Africa's first public beneficial ownership register. Curated "
                "example set: 10 LEI-anchored Nigerian companies harvested from "
                "the CAC's public search register and mapped to BODS v0.4. Not a "
                "live feed — the CAC's official API is restricted to Nigerian "
                "government agencies."
            ),
            license="Public register (bor.cac.gov.ng)",
            attribution=(
                "Corporate Affairs Commission (CAC), Federal Republic of Nigeria — "
                "bor.cac.gov.ng"
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(_get_index()),
            category="cdd",
            is_national_register=True,
            country="NG",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # Identifier-keyed (LEI) source; free-text search intentionally empty.
        return []

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the CAC PSC bundle for a LEI, or ``None`` when not in the set."""
        lei_norm = (lei or "").strip().upper()
        index = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return None
        return self._build_bundle(lei_norm, record)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        lei_norm = (hit_id or "").strip().upper()
        index = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return self._build_bundle(lei_norm, record)

    def _build_bundle(self, lei: str, record: dict[str, Any]) -> dict[str, Any]:
        rc = record.get("rc") or ""
        # Corroboration rule: the CAC BOR publishes the RC number, not the LEI
        # (OpenCheck derives the LEI via GLEIF at build time). Assert only the
        # RC — the identifier the register itself publishes.
        identifiers = {"ng_cac_rc": str(rc)} if rc else {}
        return {
            "source_id": self.id,
            "hit_id": lei,
            "lei": lei,
            "is_stub": False,
            "record": record,
            "identifiers": identifiers,
        }
