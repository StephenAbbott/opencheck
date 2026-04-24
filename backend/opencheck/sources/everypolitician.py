"""EveryPolitician adapter — Phase 0 stub.

Phase 3 will read EveryPolitician data via OpenSanctions' PEPs dataset
(reusing ``OPENSANCTIONS_API_KEY``) — EveryPolitician was revived by
the OpenSanctions team and is maintained through the Poliloom
crowdsourcing tool (launched 2026-03-24).
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo


class EveryPoliticianAdapter(SourceAdapter):
    id = "everypolitician"

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="EveryPolitician (via OpenSanctions / Poliloom)",
            homepage="https://everypolitician.org/",
            license="CC-BY-NC-4.0",
            attribution=(
                "EveryPolitician data, maintained by OpenSanctions. "
                "Licensed CC BY-NC 4.0."
            ),
            supports=[SearchKind.PERSON],
            requires_api_key=True,  # shares OPENSANCTIONS_API_KEY
            live_available=bool(settings.opensanctions_api_key and settings.allow_live),
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.PERSON:
            return []
        return [
            SourceHit(
                source_id=self.id,
                hit_id="poli-stub-0001",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub EveryPolitician record — live adapter pending (Phase 3). "
                    "Cross-links politicians by Wikidata Q-ID."
                ),
                identifiers={"wikidata_qid": "Q0"},
                raw={
                    "id": "poli-stub-0001",
                    "name": f"{query} (stub)",
                    "position": "stub position",
                    "country": "stub country",
                },
            )
        ]

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
