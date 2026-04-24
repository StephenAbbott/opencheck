"""Wikidata adapter — Phase 0 stub.

Phase 3 will issue SPARQL queries against
``WIKIDATA_SPARQL_ENDPOINT`` (default: query.wikidata.org) — no key
required; respects WDQS rate limits. Wikidata Q-IDs act as the primary
cross-source identifier bridge.
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo


class WikidataAdapter(SourceAdapter):
    id = "wikidata"

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Wikidata",
            homepage="https://www.wikidata.org/",
            license="CC0-1.0",
            attribution="Wikidata structured data, CC0 1.0.",
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="Q0",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub Wikidata item — live SPARQL adapter pending (Phase 3). "
                    "Q-IDs will bridge this hit to Companies House, GLEIF, "
                    "OpenSanctions and EveryPolitician."
                ),
                identifiers={"wikidata_qid": "Q0"},
                raw={
                    "id": "Q0",
                    "label": f"{query} (stub)",
                    "sitelink": "https://www.wikidata.org/wiki/Q0",
                },
            )
        ]

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
