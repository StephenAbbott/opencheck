"""OpenAleph adapter — Phase 0 stub.

Phase 2 will call <https://search.openaleph.org/api/2/> and surface
each matching collection's license metadata on the source card.
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo


class OpenAlephAdapter(SourceAdapter):
    id = "openaleph"

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenAleph",
            homepage="https://search.openaleph.org/",
            license="per-collection",
            attribution=(
                "Data from OpenAleph — per-collection license; see each "
                "source card for the specific terms."
            ),
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=False,  # key is per-collection, not global
            live_available=settings.allow_live,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="aleph-stub-0001",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub OpenAleph record — live adapter pending (Phase 2). "
                    "Per-collection licensing will be surfaced on the source card."
                ),
                identifiers={"aleph_id": "aleph-stub-0001"},
                raw={
                    "id": "aleph-stub-0001",
                    "schema": "Company" if kind == SearchKind.ENTITY else "Person",
                    "properties": {"name": [f"{query} (stub)"]},
                    "collection": {"label": "Stub Collection", "foreign_id": "stub"},
                },
            )
        ]

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
