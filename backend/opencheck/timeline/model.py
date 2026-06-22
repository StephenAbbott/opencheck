"""Time Machine — source-agnostic change model + controlled codelist.

See ``docs/time-machine.md`` for the design rationale. This module defines the
canonical :class:`ChangeEvent` and the controlled :class:`ChangeType` codelist
that every source emitter (GLEIF, Companies House, …) maps into.

Design decisions baked in here (agreed in design):

- **Raw-first.** A ``ChangeEvent`` always carries the faithful registry change
  (``raw_*`` fields) — even administrative noise. Notability (``tier``) is a
  *derived* classification carried on the event; suppression happens in the
  renderer, never by dropping events. This keeps the full audit trail and lets
  the UI offer "show everything, including the noise" without re-querying.
- **Honest dates.** ``date_basis`` records what the date *means* (a real
  effective date vs. when a registry merely recorded the change vs. a snapshot
  window), so two sources of differing precision are never rendered as if they
  were the same.
- **Economic vs. recorded dates.** For relationship (ownership) changes the
  BODS interest ``startDate`` / ``endDate`` live in ``interest_start_date`` /
  ``interest_end_date`` and come from the source's *period* data — distinct from
  ``event_date`` (when the change was recorded). See the Morrisons worked
  example in the spec for why this matters.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RecordType(str, Enum):
    """What kind of thing the change is about."""

    ENTITY = "entity"
    RELATIONSHIP = "relationship"


class Tier(int, Enum):
    """Notability tier. 1/2 render by default; 3 is kept but suppressed."""

    OWNERSHIP_CONTROL = 1  # ownership / control actually moved
    IDENTITY_STATUS = 2  # entity identity / status changed
    ADMIN_NOISE = 3  # administrative noise (kept, suppressed by default)


class DateBasis(str, Enum):
    """What the event date actually represents."""

    EFFECTIVE = "effective"  # real filing / effective date (e.g. Companies House)
    RECORDED = "recorded"  # when the source recorded it (e.g. GLEIF publish date)
    SNAPSHOT_WINDOW = "snapshot_window"  # only known to fall between two snapshots


class DateConfidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ChangeType(str, Enum):
    """Controlled, source-agnostic codelist. Each source maps its own raw
    vocabulary into these. Unmapped / noise changes carry ``change_type=None``."""

    # --- Tier 1: ownership / control moved ---
    OWNER_ADDED = "OWNER_ADDED"
    OWNER_REMOVED = "OWNER_REMOVED"
    CONTROL_BAND_CHANGED = "CONTROL_BAND_CHANGED"
    CONTROL_NATURE_CHANGED = "CONTROL_NATURE_CHANGED"
    PARENT_CHANGED = "PARENT_CHANGED"
    REPORTING_EXCEPTION_CHANGED = "REPORTING_EXCEPTION_CHANGED"
    # --- Tier 2: entity identity / status ---
    STATUS_CHANGED = "STATUS_CHANGED"
    SUCCESSION = "SUCCESSION"
    LEGAL_NAME_CHANGE = "LEGAL_NAME_CHANGE"
    LEGAL_FORM_CHANGE = "LEGAL_FORM_CHANGE"
    JURISDICTION_CHANGE = "JURISDICTION_CHANGE"
    REGISTRATION_RETIRED = "REGISTRATION_RETIRED"
    ADDRESS_CHANGE = "ADDRESS_CHANGE"


# BODS v0.4 recordStatus a change maps onto when synthesising statements.
_NEW = "new"
_UPDATED = "updated"
_CLOSED = "closed"


@dataclass(frozen=True)
class ChangeTypeSpec:
    """Static metadata for one ``ChangeType`` — its tier, what it's about, its
    human label, and how it maps onto BODS v0.4."""

    change_type: ChangeType
    tier: Tier
    record_type: RecordType
    label: str
    bods_record_status: str  # _NEW | _UPDATED | _CLOSED
    description: str = ""


CHANGE_TYPES: dict[ChangeType, ChangeTypeSpec] = {
    ChangeType.OWNER_ADDED: ChangeTypeSpec(
        ChangeType.OWNER_ADDED, Tier.OWNERSHIP_CONTROL, RecordType.RELATIONSHIP,
        "Owner / parent added", _NEW,
        "A beneficial owner or parent relationship began.",
    ),
    ChangeType.OWNER_REMOVED: ChangeTypeSpec(
        ChangeType.OWNER_REMOVED, Tier.OWNERSHIP_CONTROL, RecordType.RELATIONSHIP,
        "Owner / parent removed", _CLOSED,
        "A beneficial owner or parent relationship ended.",
    ),
    ChangeType.CONTROL_BAND_CHANGED: ChangeTypeSpec(
        ChangeType.CONTROL_BAND_CHANGED, Tier.OWNERSHIP_CONTROL, RecordType.RELATIONSHIP,
        "Control band changed", _UPDATED,
        "Ownership/voting crossed a statutory band (25/50/75/100%).",
    ),
    ChangeType.CONTROL_NATURE_CHANGED: ChangeTypeSpec(
        ChangeType.CONTROL_NATURE_CHANGED, Tier.OWNERSHIP_CONTROL, RecordType.RELATIONSHIP,
        "Nature of control changed", _UPDATED,
        "The kind of control changed (shares / voting / right to appoint).",
    ),
    ChangeType.PARENT_CHANGED: ChangeTypeSpec(
        ChangeType.PARENT_CHANGED, Tier.OWNERSHIP_CONTROL, RecordType.RELATIONSHIP,
        "Parent changed", _UPDATED,
        "The direct or ultimate parent changed to a different entity.",
    ),
    ChangeType.REPORTING_EXCEPTION_CHANGED: ChangeTypeSpec(
        ChangeType.REPORTING_EXCEPTION_CHANGED, Tier.OWNERSHIP_CONTROL, RecordType.RELATIONSHIP,
        "Reporting exception changed", _UPDATED,
        "Flip between a reported parent and a reporting exception "
        "(e.g. NO_KNOWN_PERSON, NON_CONSOLIDATING).",
    ),
    ChangeType.STATUS_CHANGED: ChangeTypeSpec(
        ChangeType.STATUS_CHANGED, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Status changed", _UPDATED,
        "Entity status changed (e.g. Active → Inactive / Dissolved).",
    ),
    ChangeType.SUCCESSION: ChangeTypeSpec(
        ChangeType.SUCCESSION, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Succession / merger", _UPDATED,
        "Entity was succeeded or merged into another entity.",
    ),
    ChangeType.LEGAL_NAME_CHANGE: ChangeTypeSpec(
        ChangeType.LEGAL_NAME_CHANGE, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Legal name changed", _UPDATED,
        "The registered legal name changed.",
    ),
    ChangeType.LEGAL_FORM_CHANGE: ChangeTypeSpec(
        ChangeType.LEGAL_FORM_CHANGE, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Legal form changed", _UPDATED,
        "The legal form changed class (e.g. PLC → private limited).",
    ),
    ChangeType.JURISDICTION_CHANGE: ChangeTypeSpec(
        ChangeType.JURISDICTION_CHANGE, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Jurisdiction changed", _UPDATED,
        "The legal jurisdiction changed.",
    ),
    ChangeType.REGISTRATION_RETIRED: ChangeTypeSpec(
        ChangeType.REGISTRATION_RETIRED, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Registration retired", _CLOSED,
        "Registration moved to RETIRED / MERGED / ANNULLED / DUPLICATE.",
    ),
    ChangeType.ADDRESS_CHANGE: ChangeTypeSpec(
        ChangeType.ADDRESS_CHANGE, Tier.IDENTITY_STATUS, RecordType.ENTITY,
        "Address changed", _UPDATED,
        "A registered or headquarters address line changed "
        "(region recodes are treated as noise).",
    ),
}


@dataclass
class ChangeEvent:
    """One change to an entity or relationship, faithful to the source.

    ``raw_*`` fields are always populated. ``change_type`` / ``tier`` are the
    derived classification; a Tier-3 / unmapped event is still a valid event —
    it just won't render by default.
    """

    source_id: str
    subject_id: str
    record_type: RecordType

    # --- raw, source-faithful ---
    raw_change_type: str
    raw_field: str | None = None
    value_old: str | None = None
    value_new: str | None = None
    raw_payload_ref: str | None = None

    # --- derived classification ---
    change_type: ChangeType | None = None
    tier: Tier = Tier.ADMIN_NOISE
    boosted: bool = False
    boost_reason: str | None = None

    # --- temporal + provenance ---
    event_date: str | None = None  # ISO date; for GLEIF, the *recorded* date
    date_basis: DateBasis = DateBasis.RECORDED
    date_confidence: DateConfidence = DateConfidence.MEDIUM
    date_range: tuple[str, str] | None = None
    # Economic interest dates (relationships) — from the source's PERIOD data,
    # NOT from event_date. See the spec's "critical rule".
    interest_start_date: str | None = None
    interest_end_date: str | None = None
    # The other end of a relationship change (e.g. the parent LEI for a GLEIF
    # ownership change). None for entity-record changes.
    counterparty: str | None = None

    @property
    def is_notable(self) -> bool:
        """Renders by default: a Tier-1/2 event, or a boosted Tier-3 event."""
        return self.tier in (Tier.OWNERSHIP_CONTROL, Tier.IDENTITY_STATUS) or self.boosted


__all__ = [
    "RecordType",
    "Tier",
    "DateBasis",
    "DateConfidence",
    "ChangeType",
    "ChangeTypeSpec",
    "CHANGE_TYPES",
    "ChangeEvent",
]
