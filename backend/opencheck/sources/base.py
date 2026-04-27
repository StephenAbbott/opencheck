"""Base protocol and shared types for source adapters.

Every source adapter in OpenCheck implements the same small surface:

* ``info`` — static metadata (id, name, license, attribution, supported kinds)
* ``search(query, kind)`` — returns a list of ``SourceHit`` for a user query
* ``fetch(hit_id)`` — returns full record detail for a single hit

In Phase 0, every adapter returns stub data — no network calls. Each
adapter's stub response is deterministic so frontend and tests can rely
on it. As phases progress, adapters gain live code paths gated on the
``allow_live`` setting.

Every live response will eventually be mapped into BODS v0.4 statements
by the mapper in ``opencheck.bods``. Adapters themselves don't emit BODS
directly — they return a neutral ``SourceHit`` shape plus raw payload,
and the mapper is responsible for the translation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SearchKind(str, Enum):
    """What a user is searching for."""

    ENTITY = "entity"
    PERSON = "person"


class SourceInfo(BaseModel):
    """Static metadata about a source — surfaced on the /sources endpoint."""

    id: str = Field(description="Stable adapter identifier, e.g. 'companies_house'.")
    name: str = Field(description="Human-readable source name.")
    homepage: str = Field(description="Canonical source homepage URL.")
    description: str = Field(
        default="",
        description=(
            "Short user-facing one-liner describing what the source is. "
            "Distinct from ``attribution``, which is the credit line."
        ),
    )
    license: str = Field(description="Short license identifier (e.g. 'OGL-3.0', 'CC0', 'CC-BY-NC-4.0').")
    attribution: str = Field(description="Attribution string to display alongside data.")
    supports: list[SearchKind] = Field(
        description="Which search kinds this adapter supports (entity / person / both)."
    )
    requires_api_key: bool = Field(
        description="Whether live mode needs an API key configured."
    )
    live_available: bool = Field(
        description="Whether live mode is available right now (key present AND allow_live=true)."
    )


class SourceHit(BaseModel):
    """A single search result from a source.

    Adapters populate the neutral fields below. The BODS mapper (Phase 1+)
    consumes ``raw`` plus the adapter's knowledge of its own schema to
    produce BODS v0.4 statements.
    """

    source_id: str = Field(description="Adapter id (matches SourceInfo.id).")
    hit_id: str = Field(description="Adapter-local identifier for this hit.")
    kind: SearchKind = Field(description="Entity or person.")
    name: str = Field(description="Primary display name.")
    summary: str = Field(description="Short human-readable line used in chat.")
    identifiers: dict[str, str] = Field(
        default_factory=dict,
        description="Cross-source identifiers (e.g. {'lei': '...', 'gb_coh': '...', 'wikidata_qid': '...'}).",
    )
    raw: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw payload (stub-shaped in Phase 0).",
    )
    is_stub: bool = Field(
        default=True,
        description="True when this hit was produced by the Phase 0 stub path.",
    )


class SourceAdapter(ABC):
    """Abstract base class for all source adapters."""

    #: Subclasses set this. Must be unique across the registry.
    id: str

    @property
    @abstractmethod
    def info(self) -> SourceInfo:
        """Static metadata about this source."""

    @abstractmethod
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Search this source for a query of the given kind."""

    @abstractmethod
    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch full record detail for a previously-returned hit."""
