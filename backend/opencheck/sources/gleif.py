"""GLEIF adapter — Phase 0 stub.

Phase 2 will call <https://api.gleif.org/api/v1/lei-records> and the
RR-CDF (Level 2) endpoints; no API key required, CC0 license.
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo


class GleifAdapter(SourceAdapter):
    id = "gleif"

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="GLEIF — Global Legal Entity Identifier Foundation",
            homepage="https://www.gleif.org/",
            license="CC0-1.0",
            attribution="Contains LEI data from GLEIF, available under CC0 1.0.",
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        return [
            SourceHit(
                source_id=self.id,
                hit_id="STUB000000000000LEI0",
                kind=kind,
                name=f"{query} (stub)",
                summary="Stub LEI record — GLEIF live adapter pending (Phase 2).",
                identifiers={"lei": "STUB000000000000LEI0"},
                raw={"lei": "STUB000000000000LEI0", "legalName": f"{query} (stub)"},
            )
        ]

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
