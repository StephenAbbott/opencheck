"""OECD-UNSD MEIP adapter — the register of the 500 largest groups, as BODS.

MEIP (the OECD-UNSD Multinational Enterprise Information Platform) publishes
an annual "Global Register" of the world's 500 largest multinational
enterprise groups and their subsidiaries — 126,658 entities in the 31 December
2024 edition — and, since September 2026, publishes it in **BODS v0.4**. That
release is what this adapter serves: the OECD's own entity and relationship
statements, unmodified, read from the ``meip.sqlite`` store (``opencheck/meip.py``)
that ``scripts/build_meip.py`` packs from the BODS file and the spreadsheet.

What the source asserts, and no more
------------------------------------
The register records *group membership* — that a company belongs to one of
the 500 groups under the platform's statistical methodology. It is not a
shareholder register: every relationship is ``unknownInterest`` with
``directOrIndirect: unknown``, no share, no dates, and every edge runs
straight from a subsidiary to its group head (the multi-hop chain the
spreadsheet partly knows is not in the BODS file). So a **subsidiary** subject
gets one upward edge per membership — the group head's entity statement and
the relationship — and a **group head** subject gets its own entity
statement; its children are the Subsidiaries tab's list, never graph nodes.

Two things the Phase 208 review measured, both carried through rather than
corrected (Stephen, 15 Sept 2026): roughly 4% of head rows carry the LEI of a
different legal entity in the group (VINCI's head record is VINCI
Construction Grands Projets; Munich Re's is a UK pension trustee), and 113
LEIs are listed in two groups. Both are shown as published — the finding
sentence names the *group*, never says "owned by", and every membership is
listed — because a source's disagreement with GLEIF Level 2 on the same page
is the point, not a defect to hide.

Offline by design: the register is annual; the whole file is a 66 MB SQLite
release asset downloaded at boot. Without it the source covers nothing —
``covers_lei`` is False for every LEI and the lookup never announces it.

Licence: OECD Terms and Conditions — reuse and redistribution for any purpose
with attribution.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from .. import provenance
from ..meip import GROUP_COUNT, MEIP_URL, store
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.meip import MeipBundle

log = logging.getLogger(__name__)


def _declare(edition: str) -> None:
    """The store is a published bulk dataset with a reference date, not a
    call to a register — declare it as a snapshot of that date so the card
    says "31 Dec 2024 register", not "live"."""
    built_at: datetime | None = None
    if edition:
        try:
            built_at = datetime.fromisoformat(edition).replace(tzinfo=UTC)
        except ValueError:
            built_at = None
    provenance.record_snapshot(
        built_at,
        f"OECD-UNSD MEIP Global Register, {edition or 'undated'} edition, BODS v0.4 release",
    )


class MeipAdapter(SourceAdapter):
    """OECD-UNSD MEIP — corporate group membership, as the OECD's BODS."""

    id = "meip"

    #: LEI-keyed source. Dispatched directly in routers/lookup.py alongside the
    #: other LEI-keyed sources (eiti_soe, cac_nigeria), not via an RA-code
    #: deriver. A SQLite read; the budget is generous for a cold disk.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        st = store()
        edition = st.edition or "31 Dec 2024"
        return SourceInfo(
            id=self.id,
            name="OECD-UNSD Multinational Enterprise Information Platform (MEIP)",
            homepage=MEIP_URL,
            description=(
                f"Group membership from the OECD-UNSD Global Register of the {GROUP_COUNT} "
                "largest multinational enterprise groups and their subsidiaries, as the "
                "OECD publishes it in BODS v0.4 (register of "
                f"{edition}). One edge per membership, from a subsidiary to its group "
                "head, with the OECD's hierarchy classification; no shares, no dates, no "
                "natural persons. A group head's subsidiaries are listed on the "
                "Subsidiaries tab. Annual release; served from a local copy."
            ),
            license="OECD-Terms",
            attribution=(
                "OECD-UNSD Multinational Enterprise Information Platform (MEIP), "
                "Global Register — © OECD"
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=st.available,
            category="cdd",
            is_national_register=False,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # Identifier-keyed (LEI) source; free-text search intentionally empty.
        return []

    def covers_lei(self, lei: str) -> bool:
        """Whether the register lists this LEI at all — on any record.

        The lookup pipeline asks before dispatching, so a company the register
        does not name is never announced as a source being queried. Reads the
        store without declaring provenance: nothing has been fetched.
        """
        return store().covers(lei)

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """The OECD's statements for a LEI, or ``None`` when not in the register."""
        return await asyncio.to_thread(self._fetch_sync, lei)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        bundle = await asyncio.to_thread(self._fetch_sync, hit_id)
        if bundle is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return bundle

    def _fetch_sync(self, lei: str) -> dict[str, Any] | None:
        st = store()
        bundle = st.bundle_for_lei(lei)
        if bundle is None:
            return None
        _declare(bundle.get("edition") or "")
        validate_raw(self.id, MeipBundle, bundle)
        return bundle
