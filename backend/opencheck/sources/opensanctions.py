"""OpenSanctions adapter — Phase 0 stub.

Phase 2 will call <https://api.opensanctions.org/search/default> using
``OPENSANCTIONS_API_KEY``. Licensed CC BY-NC 4.0 — flagged at export
time when NC-constrained data would be included.
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo


class OpenSanctionsAdapter(SourceAdapter):
    id = "opensanctions"

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenSanctions",
            homepage="https://www.opensanctions.org/",
            license="CC-BY-NC-4.0",
            attribution="Data from OpenSanctions.org, licensed CC BY-NC 4.0.",
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=True,
            live_available=bool(settings.opensanctions_api_key and settings.allow_live),
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="NK-stub-0001",
                kind=kind,
                name=f"{query} (stub)",
                summary="Stub OpenSanctions record — live adapter pending (Phase 2).",
                identifiers={"opensanctions_id": "NK-stub-0001"},
                raw={
                    "id": "NK-stub-0001",
                    "schema": "Company" if kind == SearchKind.ENTITY else "Person",
                    "caption": f"{query} (stub)",
                    "datasets": ["stub"],
                    "topics": [],
                },
            )
        ]

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
