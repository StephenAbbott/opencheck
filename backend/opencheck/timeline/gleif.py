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


def _is_attribute(field_path: str | None) -> bool:
    """An XML attribute leaf (``/@type``, ``/@xml:lang``) — the language or
    kind of the element it sits on, never a change to its value. A
    transliterated address's ``@type`` (PREFERRED_ASCII_TRANSLITERATED_LEGAL_ADDRESS)
    read as an address change on the History tab until this was checked
    (Eide Seafood, 23 Nov 2026)."""
    return field_path is not None and "/@" in field_path


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

    # GLEIF's ASCII transliteration of an address it already holds, and the
    # other-language versions of it: the company did not move.
    if "OtherAddress" in field:
        return None, Tier.ADMIN_NOISE

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
    label: str | None = None
    shown_old, shown_new = old, new
    if _is_attribute(raw_field):
        # An attribute names the kind or language of a value; the value's own
        # field carries the change.
        change_type, tier = None, Tier.ADMIN_NOISE
        if record_type is RecordType.RELATIONSHIP:
            counterparty = (mod.get("context") or {}).get("endNode")
    elif record_type is RecordType.RELATIONSHIP:
        change_type, tier = _classify_relationship(field, mtype, old, new)
        # The other end of the relationship (the parent LEI) rides in context.
        context = mod.get("context") or {}
        counterparty = context.get("endNode")
        if change_type is not None:
            # GLEIF Level 2 is accounting consolidation, never ownership: the
            # row says so in GLEIF's terms, not "Owner / parent added", and
            # never shows the codelist value (IS_ULTIMATELY_CONSOLIDATED_BY).
            kind = _consolidation_kind(
                context.get("relationshipType") or new or old
            )
            if change_type is ChangeType.OWNER_ADDED:
                label = "Consolidating parent recorded"
                shown_old, shown_new = None, f"Consolidated by its {kind}"
            else:
                label = "Consolidating parent removed"
                shown_old, shown_new = None, f"No longer consolidated by its {kind}"
    else:
        change_type, tier = _classify_entity(field, mtype, old, new)

    return ChangeEvent(
        source_id="gleif",
        subject_id=mod.get("lei") or "",
        record_type=record_type,
        raw_change_type=mtype or "",
        raw_field=raw_field,
        value_old=shown_old,
        value_new=shown_new,
        raw_payload_ref=payload_ref,
        change_type=change_type,
        tier=tier,
        event_date=_iso_date(mod.get("date")),
        # GLEIF's date is when GLEIF recorded the change, not when it happened.
        date_basis=DateBasis.RECORDED,
        date_confidence=DateConfidence.MEDIUM,
        counterparty=counterparty,
        label=label,
    )


def _consolidation_kind(relationship_type: str | None) -> str:
    """GLEIF's relationship type in words: which consolidating parent."""
    rt = (relationship_type or "").strip().upper()
    if rt == "IS_ULTIMATELY_CONSOLIDATED_BY":
        return "ultimate parent"
    if rt == "IS_DIRECTLY_CONSOLIDATED_BY":
        return "direct parent"
    if rt == "IS_INTERNATIONAL_BRANCH_OF":
        return "head office (international branch)"
    if rt == "IS_FUND-MANAGED_BY":
        return "fund manager"
    if rt == "IS_SUBFUND_OF":
        return "umbrella fund"
    if rt == "IS_FEEDER_TO":
        return "master fund"
    return "parent"


_PERIOD_START = "rr:RelationshipPeriod/rr:StartDate"
_PERIOD_END = "rr:RelationshipPeriod/rr:EndDate"
_PERIOD_TYPE = "rr:RelationshipPeriod/rr:PeriodType"


def relationship_periods(mods: Iterable[dict]) -> list[dict] | None:
    """Rebuild one relationship's ``RelationshipPeriod`` list from its log.

    GLEIF's log paths carry no index, so the period a ``StartDate`` belongs to
    is not in its path. The log is served newest first and, within one
    publication, in reverse document order; read oldest first, each period's
    ``StartDate`` / ``EndDate`` come before its ``PeriodType``, which closes
    it (checked on EIDE SEAFOOD AS and PEPSICO DE ARGENTINA SRL, 23 Nov 2026).
    A later UPDATE or DELETE of a date names the value it replaces, which
    finds its period when exactly one period holds it.

    Returns ``None`` when the log cannot be read that way — a date left
    without a type, a type with no dates, or an update that matches no single
    period. The caller then shows no period at all rather than a guessed one.
    """
    periods: list[dict] = []
    pending: dict = {}
    for mod in reversed(list(mods)):
        field = _normalise_field(mod.get("field"))
        if not field.endswith((_PERIOD_START, _PERIOD_END, _PERIOD_TYPE)):
            continue
        mtype = (mod.get("modificationType") or "").upper()
        key = (
            "start" if field.endswith(_PERIOD_START)
            else "end" if field.endswith(_PERIOD_END)
            else "type"
        )
        old, new = mod.get("valueOld"), mod.get("valueNew")
        if key in ("start", "end"):
            old, new = _iso_date(old), _iso_date(new)
        else:
            old = (old or "").strip().upper() or None
            new = (new or "").strip().upper() or None

        if mtype in ("INITIAL", "INSERT"):
            if key == "type":
                if not pending:
                    return None
                pending["type"] = new
                periods.append(pending)
                pending = {}
            else:
                if key in pending:
                    return None
                pending[key] = new
            continue

        if mtype in ("UPDATE", "DELETE"):
            if pending:
                return None
            matches = [p for p in periods if p.get(key) == old]
            if len(matches) != 1:
                return None
            if mtype == "DELETE" and key == "type":
                periods.remove(matches[0])
            elif mtype == "DELETE":
                matches[0].pop(key, None)
            else:
                matches[0][key] = new
            continue
    if pending:
        return None
    return periods


def relationship_interest_dates(
    mods: Iterable[dict],
) -> tuple[str | None, str | None]:
    """The economic interest (start, end) for a relationship: the dates of its
    ``RELATIONSHIP_PERIOD``, start and end from that one period.

    GLEIF files other periods beside it — the ACCOUNTING_PERIOD of the
    accounts that evidence the link, a DOCUMENT_FILING_PERIOD — and their
    ends are not the relationship's. Until 23 Nov 2026 this took the earliest
    start and the latest end across every period, so EIDE SEAFOOD AS's
    current consolidation (from 30 Dec 2022, no end) read "to 2023-12-31", the
    end of the accounts filed for it. A relationship period with no end date
    has no end; a log that cannot be paired (``relationship_periods`` is
    None) gives no dates.
    """
    periods = relationship_periods(mods)
    if not periods:
        return None, None
    rel = [p for p in periods if p.get("type") == "RELATIONSHIP_PERIOD"]
    if len(rel) != 1:
        return None, None
    return rel[0].get("start"), rel[0].get("end")


__all__ = [
    "classify_gleif_modification",
    "relationship_interest_dates",
    "relationship_periods",
]
