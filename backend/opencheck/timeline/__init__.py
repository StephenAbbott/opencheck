"""Time Machine — change-over-time model and per-source emitters.

See ``docs/time-machine.md`` for the design. ``model`` holds the source-agnostic
``ChangeEvent`` + ``ChangeType`` codelist; ``gleif`` is the first emitter.
"""

from __future__ import annotations

from .gleif import classify_gleif_modification, relationship_interest_dates
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
    "classify_gleif_modification",
    "relationship_interest_dates",
]
