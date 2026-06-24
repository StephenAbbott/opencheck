"""Time Machine — change-over-time model and per-source emitters.

See ``docs/time-machine.md`` for the design. ``model`` holds the source-agnostic
``ChangeEvent`` + ``ChangeType`` codelist; ``gleif`` is the first emitter.
"""

from __future__ import annotations

from .ariregister import ariregister_change_events
from .assemble import Timeline, TimelineEntry, assemble_timeline
from .companies_house import classify_companies_house_filing
from .cvr_denmark import cvr_change_events
from .gleif import classify_gleif_modification, relationship_interest_dates
from .nz_companies import nz_change_events
from .model import (
    CHANGE_TYPES,
    ChangeEvent,
    ChangeType,
    ChangeTypeSpec,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

__all__ = [
    "CHANGE_TYPES",
    "ChangeEvent",
    "ChangeType",
    "ChangeTypeSpec",
    "DateBasis",
    "DateConfidence",
    "RecordType",
    "Tier",
    "Timeline",
    "TimelineEntry",
    "ariregister_change_events",
    "assemble_timeline",
    "classify_companies_house_filing",
    "classify_gleif_modification",
    "cvr_change_events",
    "nz_change_events",
    "relationship_interest_dates",
]
