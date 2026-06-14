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
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, ClassVar, Literal

from pydantic import BaseModel, Field, field_serializer

# Source ids whose **raw** source payload must not be redistributed via
# OpenCheck (licence terms permit derived/BODS output but not bulk re-publication
# of the raw records). Populated from the registry at import time (see
# sources/__init__.py) so the SourceHit serializer below can redact them.
RAW_SUPPRESSED_SOURCE_IDS: frozenset[str] = frozenset()


def raw_redaction_notice(source_id: str) -> dict[str, str]:
    """Placeholder returned in place of a suppressed source's raw payload."""
    return {
        "_redacted": (
            "Raw source data is not redistributed via OpenCheck for licensing "
            "reasons. The mapped BODS statements are still provided; obtain the "
            "raw record from the original source."
        ),
        "source_id": source_id,
    }


@dataclass(frozen=True)
class LookupDeriver:
    """How to derive a local identifier from the GLEIF LEI anchor record.

    When the anchor record's ``registeredAt.id`` is one of ``ra_codes``,
    the lookup pipeline stores ``normalise(registeredAs)`` under
    ``derived_key`` and dispatches the declaring adapter. ``normalise``
    may raise ValueError for malformed local IDs — the adapter is then
    skipped for that lookup.
    """

    ra_codes: frozenset[str]
    derived_key: str
    normalise: Callable[[str], str]


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
    category: Literal["cdd", "esg"] = Field(
        default="cdd",
        description=(
            "Broad purpose category. 'cdd' = customer due diligence / compliance; "
            "'esg' = environmental, social and governance data."
        ),
    )
    is_national_register: bool = Field(
        default=False,
        description=(
            "True when this source is an official national company or beneficial "
            "ownership register (e.g. Companies House, Bolagsverket). False for "
            "aggregators, cross-border databases, and ESG sources."
        ),
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

    @field_serializer("raw")
    def _redact_raw(self, raw: dict[str, Any], _info: Any) -> dict[str, Any]:
        """Redact the raw payload on serialization for sources whose licence
        does not permit raw re-publication (e.g. OpenCorporates). The derived
        BODS output is unaffected — only the verbatim source record is withheld.
        This runs for every ``model_dump`` / ``model_dump_json``, so it covers
        search, lookup, lookup-source, the export manifest and SSE alike."""
        if self.source_id in RAW_SUPPRESSED_SOURCE_IDS:
            return raw_redaction_notice(self.source_id)
        return raw


class SourceAdapter(ABC):
    """Abstract base class for all source adapters."""

    #: Subclasses set this. Must be unique across the registry.
    id: str

    #: Whether this source's **raw** payload may be redistributed via OpenCheck.
    #: When False, the raw record is redacted from all API responses and exports
    #: (the derived BODS output is still served). Used where a licence permits
    #: derived works but not bulk re-publication of the raw data (OpenCorporates).
    republish_raw: ClassVar[bool] = True

    # --- LEI-anchored lookup wiring (self-describing adapters) ------------
    # National-register adapters declare how the lookup pipeline reaches
    # them; the pipeline builds its dispatch tables from the registry, so
    # there is nothing to wire by hand in routers/lookup.py.

    #: RA-code rules that derive this adapter's local identifier from the
    #: GLEIF anchor record. Empty → not derived from RA codes.
    lookup_derivers: ClassVar[tuple[LookupDeriver, ...]] = ()

    #: Derived-identifier keys that trigger dispatch of this adapter, in
    #: priority order (first present key wins). Defaults to the keys of
    #: ``lookup_derivers``; override when the adapter dispatches on a key
    #: derived elsewhere (e.g. rpvs_slovakia reuses rpo's ``sk_ico``, and
    #: companies_house uses the GB jurisdiction special case).
    lookup_dispatch_keys: ClassVar[tuple[str, ...]] = ()

    #: Whether ``fetch()`` should receive ``legal_name=`` from the anchor.
    lookup_pass_legal_name: ClassVar[bool] = False

    #: Wall-clock budget (seconds) for this adapter inside one lookup. The
    #: pipeline cancels the fetch and emits a source_error when exceeded, so
    #: one hung source can never stall the whole lookup. Slow-by-design
    #: adapters (Datafordeler CVR, the OpenAleph strategy cascade) override.
    lookup_timeout_s: ClassVar[float] = 30.0

    @classmethod
    def lookup_keys(cls) -> tuple[str, ...]:
        """Dispatch keys for the lookup pipeline (explicit or derived)."""
        return cls.lookup_dispatch_keys or tuple(
            d.derived_key for d in cls.lookup_derivers
        )

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
