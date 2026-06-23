"""New Zealand change emitter — Time Machine events from dated NZBN records.

The third emitter for the Time Machine, and a third *shape* of source. GLEIF is
a field-diff stream and Companies House a typed filing stream; New Zealand is a
set of **dated current-and-historic records** that we reconstruct events from —
and the dates are real effective dates (appointment / cessation / status start),
so NZ events are ``DateBasis.EFFECTIVE`` / ``DateConfidence.HIGH``.

Inputs (all from the NZBN API, fetched by ``NzCompaniesAdapter.fetch_timeline_data``):

* the ``FullEntity`` — current + historic shareholders (with appointment /
  vacation dates) and directors (``roles`` with start / end dates);
* ``/history/entity-names``, ``/history/entity-statuses``, ``/history/addresses``
  — dated identity changes.

Ownership (shareholders) and control (directors) both map to
``OWNER_ADDED`` / ``OWNER_REMOVED`` Tier-1 relationship events; the role and any
share percentage ride in ``counterparty`` (the codelist has no director-specific
type, and the counterparty keeps it readable). Name / status / address history
map to the Tier-2 identity change types.
"""

from __future__ import annotations

from typing import Any

from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)


def _date(value: Any) -> str | None:
    s = str(value or "").strip()
    return s[:10] if s else None


def _full_name(p: Any) -> str:
    if not isinstance(p, dict):
        return ""
    full = str(p.get("fullName") or "").strip()
    if full:
        return full
    parts = [p.get("firstName"), p.get("middleNames"), p.get("lastName")]
    return " ".join(str(x).strip() for x in parts if x and str(x).strip())


def _shareholder_name(individual: Any, other: Any) -> str:
    if isinstance(other, dict) and str(other.get("currentEntityName") or "").strip():
        return str(other["currentEntityName"]).strip()
    return _full_name(individual)


def _addr_line(a: Any) -> str | None:
    if not isinstance(a, dict):
        return None
    parts = [a.get("address1"), a.get("address2"), a.get("address3"), a.get("address4"),
             a.get("postCode")]
    line = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
    return line or None


def _entity_event(subject: str, change_type: ChangeType, *, value_old: str | None,
                  value_new: str | None, date: str | None) -> ChangeEvent:
    return ChangeEvent(
        source_id="nz_companies", subject_id=subject, record_type=RecordType.ENTITY,
        raw_change_type="record", change_type=change_type, tier=Tier.IDENTITY_STATUS,
        value_old=value_old, value_new=value_new, event_date=date,
        date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
    )


def _emit_owner(events: list[ChangeEvent], subject: str, label: str,
                start: str | None, end: str | None) -> None:
    """Reconstruct add/remove relationship events from a dated role/shareholding."""
    common = dict(
        source_id="nz_companies", subject_id=subject,
        record_type=RecordType.RELATIONSHIP, raw_change_type="record",
        tier=Tier.OWNERSHIP_CONTROL, counterparty=label,
        interest_start_date=start, interest_end_date=end,
        date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
    )
    if start:
        events.append(ChangeEvent(change_type=ChangeType.OWNER_ADDED, event_date=start, **common))
    if end:
        events.append(ChangeEvent(change_type=ChangeType.OWNER_REMOVED, event_date=end, **common))


def _history_changes(items: Any, value_key: str, fallback_key: str | None,
                     change_type: ChangeType, subject: str) -> list[ChangeEvent]:
    """Turn a dated history array (names / statuses) into transition events.

    The earliest entry is the original state (not a change); each later entry is
    a change from the previous value at its ``startDate``."""
    rows = [r for r in (items or []) if isinstance(r, dict)]

    def _val(r: dict) -> str:
        v = str(r.get(value_key) or "").strip()
        if not v and fallback_key:
            v = str(r.get(fallback_key) or "").strip()
        return v

    rows = [r for r in rows if _val(r)]
    rows.sort(key=lambda r: _date(r.get("startDate")) or "")
    out: list[ChangeEvent] = []
    for i in range(1, len(rows)):
        out.append(_entity_event(
            subject, change_type,
            value_old=_val(rows[i - 1]) or None, value_new=_val(rows[i]) or None,
            date=_date(rows[i].get("startDate")),
        ))
    return out


def _address_changes(addr_list: Any, subject: str) -> list[ChangeEvent]:
    """Per address type, turn the dated address list into ADDRESS_CHANGE events."""
    rows = [a for a in (addr_list or []) if isinstance(a, dict) and _addr_line(a)]
    by_type: dict[str, list[dict]] = {}
    for a in rows:
        by_type.setdefault(str(a.get("addressType") or "ADDRESS"), []).append(a)
    out: list[ChangeEvent] = []
    for group in by_type.values():
        group.sort(key=lambda a: _date(a.get("startDate")) or "")
        for i in range(1, len(group)):
            out.append(_entity_event(
                subject, ChangeType.ADDRESS_CHANGE,
                value_old=_addr_line(group[i - 1]), value_new=_addr_line(group[i]),
                date=_date(group[i].get("startDate")),
            ))
    return out


def nz_change_events(data: dict[str, Any]) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from the NZBN FullEntity + history."""
    if not data:
        return []
    full = data.get("full") or {}
    subject = str(data.get("company_number") or data.get("nzbn") or "")
    events: list[ChangeEvent] = []

    cd = full.get("company-details") or {}
    sh = cd.get("shareholding") or {}
    try:
        total = float(sh.get("numberOfShares") or 0)
    except (TypeError, ValueError):
        total = 0.0

    # Current shareholders (with share %).
    for alloc in sh.get("shareAllocation") or []:
        if not isinstance(alloc, dict):
            continue
        try:
            shares = float(alloc.get("allocation") or 0)
        except (TypeError, ValueError):
            shares = 0.0
        pct = round(shares / total * 100, 2) if total else None
        for h in alloc.get("shareholder") or []:
            if not isinstance(h, dict):
                continue
            name = _shareholder_name(h.get("individualShareholder"), h.get("otherShareholder"))
            if not name:
                continue
            label = f"{name} — shareholder" + (f" ({pct}%)" if pct is not None else "")
            _emit_owner(events, subject, label,
                        _date(h.get("appointmentDate")), _date(h.get("vacationDate")))

    # Historic shareholders (no % on file).
    for hs in sh.get("historicShareholder") or []:
        if not isinstance(hs, dict):
            continue
        name = _shareholder_name(
            hs.get("historicIndividualShareholder"), hs.get("historicOtherShareholder"))
        if not name:
            continue
        _emit_owner(events, subject, f"{name} — shareholder",
                    _date(hs.get("appointmentDate")), _date(hs.get("vacationDate")))

    # Directors / role-holders (control).
    for r in full.get("roles") or []:
        if not isinstance(r, dict):
            continue
        ent = (r.get("roleEntity") or {}).get("entityName")
        name = (str(ent).strip() if ent else "") or _full_name(r.get("rolePerson"))
        if not name:
            continue
        role = (str(r.get("roleType") or "").strip() or "Director").lower()
        _emit_owner(events, subject, f"{name} — {role}",
                    _date(r.get("startDate")), _date(r.get("endDate")))

    # Identity history (effective-dated transitions).
    events += _history_changes(
        data.get("name_history"), "entityName", None, ChangeType.LEGAL_NAME_CHANGE, subject)
    events += _history_changes(
        data.get("status_history"), "entityStatusDescription", "entityStatusCode",
        ChangeType.STATUS_CHANGED, subject)
    events += _address_changes(data.get("address_history"), subject)
    return events


__all__ = ["nz_change_events"]
