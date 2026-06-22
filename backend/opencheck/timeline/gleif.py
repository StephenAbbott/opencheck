"""GLEIF change-log emitter — maps GLEIF field-modifications to ``ChangeEvent``.

GLEIF exposes a live, per-LEI, field-level change log
(``/lei-records/{lei}/field-modifications``; via the GLEIF MCP,
``gleif_get_modification_history_by_lei``). Each modification carries
``modificationType`` (INITIAL/INSERT/UPDATE/DELETE), an XML ``field`` path, a
``date`` (when GLEIF *recorded* it), and ``valueOld`` / ``valueNew``. Query
``record_type=LEI`` for entity-record changes and ``record_type=RR`` for
relationship (parent) changes.

This is the first emitter for the Time Machine model. Notability is a field-path
allowlist plus two value-aware guards proven necessary by the Morrisons dry-run
(see ``docs/time-machine.md``):

1. **LegalForm encoding backfills are not real changes.** Only flag a legal-form
   change when the form *class* actually changes; suppress re-encodings and
   placeholder backfills.
2. **Relationship interest dates come from the period, not the publish date.**
   The modification ``date`` is recorded-provenance only; the economic
   ``startDate`` / ``endDate`` come from ``RelationshipPeriod`` data.
"""

from __future__ import annotations

from collections.abc import Iterable

from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_LEI_PREFIX = "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/"
_RR_PREFIX = "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"

# Registration statuses that mean the LEI itself was retired/closed (Tier 2).
# LAPSED / ISSUED cycling is administrative noise (Tier 3) — a boost candidate.
_RETIRED_REG_STATUSES = {"RETIRED", "MERGED", "ANNULLED", "DUPLICATE"}

# Partial ELF-code → coarse legal-form *class*. Observed from live GLEIF data;
# extend as needed. "8888" is GLEIF's placeholder for "no/other legal form".
# The class comparison (not the raw code) is what decides notability, so a
# re-encoding within the same class — or a backfill from the UNKNOWN placeholder
# — is correctly treated as noise.
_ELF_CLASS: dict[str, str] = {
    "B6ES": "PUBLIC_LIMITED",   # GB public limited company (PLC)
    "H0PO": "PRIVATE_LIMITED",  # GB private limited company
    "8888": "UNKNOWN",          # GLEIF placeholder / free-text OtherLegalForm
}


def _normalise_field(field_path: str | None) -> str:
    """Strip the long record prefix and any trailing XML attribute leaf."""
    if not field_path:
        return ""
    for prefix in (_LEI_PREFIX, _RR_PREFIX):
        if field_path.startswith(prefix):
            field_path = field_path[len(prefix):]
            break
    if "/@" in field_path:  # drop attribute leaves like /@xml:lang, /@type
        field_path = field_path.split("/@", 1)[0]
    return field_path


def _elf_class(code: str | None) -> str:
    if not code:
        return "UNKNOWN"
    return _ELF_CLASS.get(code.strip().upper(), "UNKNOWN")


def _iso_date(value: str | None) -> str | None:
    """GLEIF dates look like '2021-12-09T16:00:00Z' — keep the date part."""
    if not value:
        return None
    return value[:10]


def _classify_entity(field: str, mtype: str, old: str | None, new: str | None):
    """Return (change_type, tier) for an LEI-record modification."""
    # INITIAL modifications are the record's starting state, not a change.
    if mtype == "INITIAL":
        return None, Tier.ADMIN_NOISE

    if field == "lei:Entity/lei:LegalName":
        return ChangeType.LEGAL_NAME_CHANGE, Tier.IDENTITY_STATUS

    if field == "lei:Entity/lei:LegalForm/lei:EntityLegalFormCode":
        # Guard 1: only notable when the form CLASS changes. A re-encoding or a
        # backfill from the UNKNOWN placeholder (e.g. 8888 → B6ES) is noise.
        old_cls, new_cls = _elf_class(old), _elf_class(new)
        if old_cls != new_cls and "UNKNOWN" not in {old_cls, new_cls}:
            return ChangeType.LEGAL_FORM_CHANGE, Tier.IDENTITY_STATUS
        return None, Tier.ADMIN_NOISE

    if field == "lei:Entity/lei:EntityStatus":
        return ChangeType.STATUS_CHANGED, Tier.IDENTITY_STATUS

    if field == "lei:Entity/lei:LegalJurisdiction":
        return ChangeType.JURISDICTION_CHANGE, Tier.IDENTITY_STATUS

    if field.startswith("lei:Entity/lei:SuccessorEntity"):
        return ChangeType.SUCCESSION, Tier.IDENTITY_STATUS

    if field == "lei:Registration/lei:RegistrationStatus":
        if (new or "").strip().upper() in _RETIRED_REG_STATUSES:
            return ChangeType.REGISTRATION_RETIRED, Tier.IDENTITY_STATUS
        return None, Tier.ADMIN_NOISE  # LAPSED/ISSUED cycle — boost candidate

    if "Address" in field:
        # Region recodes (GB-UKM → GB-ENG → GB-BRD) are administrative noise;
        # changes to actual address lines / city / country are notable.
        if field.endswith("/lei:Region"):
            return None, Tier.ADMIN_NOISE
        return ChangeType.ADDRESS_CHANGE, Tier.IDENTITY_STATUS

    return None, Tier.ADMIN_NOISE


def _classify_relationship(field: str, mtype: str, old: str | None, new: str | None):
    """Return (change_type, tier) for an RR (relationship) modification."""
    # The relationship's existence is anchored on its RelationshipType field;
    # treat that as the single canonical "relationship began/ended" marker so we
    # emit one ownership event per relationship rather than one per attribute.
    if field == "rr:Relationship/rr:RelationshipType":
        if mtype in ("INITIAL", "INSERT"):
            return ChangeType.OWNER_ADDED, Tier.OWNERSHIP_CONTROL
        if mtype == "DELETE":
            return ChangeType.OWNER_REMOVED, Tier.OWNERSHIP_CONTROL
        return None, Tier.ADMIN_NOISE

    if field == "rr:Relationship/rr:RelationshipStatus":
        if (new or "").strip().upper() == "INACTIVE":
            return ChangeType.OWNER_REMOVED, Tier.OWNERSHIP_CONTROL
        return None, Tier.ADMIN_NOISE

    # Everything else on a relationship record (periods, validation refs,
    # renewal/update dates, node id types, qualifiers) is housekeeping.
    return None, Tier.ADMIN_NOISE


def classify_gleif_modification(
    mod: dict, *, payload_ref: str | None = None
) -> ChangeEvent:
    """Map one GLEIF field-modification (its ``attributes`` dict) to a
    ``ChangeEvent``. ``raw_*`` fields are always populated; classification falls
    back to Tier-3 / ``change_type=None`` for anything not on the allowlist."""
    raw_record_type = (mod.get("recordType") or "LEI").upper()
    record_type = RecordType.RELATIONSHIP if raw_record_type == "RR" else RecordType.ENTITY

    raw_field = mod.get("field")
    field = _normalise_field(raw_field)
    mtype = (mod.get("modificationType") or "").upper()
    old, new = mod.get("valueOld"), mod.get("valueNew")

    counterparty: str | None = None
    if record_type is RecordType.RELATIONSHIP:
        change_type, tier = _classify_relationship(field, mtype, old, new)
        # The other end of the relationship (the parent LEI) rides in context.
        context = mod.get("context") or {}
        counterparty = context.get("endNode")
    else:
        change_type, tier = _classify_entity(field, mtype, old, new)

    return ChangeEvent(
        source_id="gleif",
        subject_id=mod.get("lei") or "",
        record_type=record_type,
        raw_change_type=mtype or "",
        raw_field=raw_field,
        value_old=old,
        value_new=new,
        raw_payload_ref=payload_ref,
        change_type=change_type,
        tier=tier,
        event_date=_iso_date(mod.get("date")),
        # GLEIF's date is when GLEIF recorded the change, not when it happened.
        date_basis=DateBasis.RECORDED,
        date_confidence=DateConfidence.MEDIUM,
        counterparty=counterparty,
    )


def relationship_interest_dates(
    mods: Iterable[dict],
) -> tuple[str | None, str | None]:
    """Derive the economic interest (start, end) for a relationship from its
    ``RelationshipPeriod`` data — the dates a Tier-1 OWNER_ADDED event should
    carry, distinct from any modification ``date``.

    Heuristic for the draft: earliest period start, latest period end. (Open
    question: prefer RELATIONSHIP_PERIOD over ACCOUNTING_PERIOD precedence —
    the period type lives on a sibling field without a shared index, so this is
    a best-effort minimum for now.)
    """
    starts: list[str] = []
    ends: list[str] = []
    for mod in mods:
        field = _normalise_field(mod.get("field"))
        value = _iso_date(mod.get("valueNew") or mod.get("valueOld"))
        if not value:
            continue
        if field.endswith("rr:RelationshipPeriod/rr:StartDate"):
            starts.append(value)
        elif field.endswith("rr:RelationshipPeriod/rr:EndDate"):
            ends.append(value)
    return (min(starts) if starts else None, max(ends) if ends else None)


__all__ = ["classify_gleif_modification", "relationship_interest_dates"]
