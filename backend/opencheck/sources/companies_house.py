"""UK Companies House adapter — Phase 0 stub.

Phase 1 will call the live API at
<https://api.company-information.service.gov.uk>
using the key in ``COMPANIES_HOUSE_API_KEY``.
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo


class CompaniesHouseAdapter(SourceAdapter):
    id = "companies_house"

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="UK Companies House",
            homepage="https://find-and-update.company-information.service.gov.uk/",
            license="OGL-3.0",
            attribution=(
                "Contains public sector information licensed under the "
                "Open Government Licence v3.0 (Companies House)."
            ),
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=True,
            live_available=bool(settings.companies_house_api_key and settings.allow_live),
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # Phase 0: deterministic stub.
        if kind == SearchKind.ENTITY:
            return [
                SourceHit(
                    source_id=self.id,
                    hit_id="00000000",
                    kind=kind,
                    name=f"{query} (stub)",
                    summary="Stub company record — Companies House live adapter pending (Phase 1).",
                    identifiers={"gb_coh": "00000000"},
                    raw={"company_number": "00000000", "title": f"{query} (stub)"},
                )
            ]
        return [
            SourceHit(
                source_id=self.id,
                hit_id="officer-stub-0",
                kind=kind,
                name=f"{query} (stub)",
                summary="Stub officer record — Companies House live adapter pending (Phase 1).",
                identifiers={},
                raw={"name": f"{query} (stub)", "kind": "officer"},
            )
        ]

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
