"""Israeli change emitter — Time Machine events from the Corporations Authority's
change log (``ica-changes`` on data.gov.il, CC BY).

The eighth emitter, and the thinnest record any of them reads. Each row says
that the Authority registered a request of one type for the corporation on one
day — **never what it changed to**: no person, no share count, no old or new
name, no new status. So every event here states a filing, in the register's
words, and nothing the file does not say:

* winding-up, liquidation, revival and strike-off requests → Tier 2
  ``STATUS_CHANGED``; mergers → Tier 2 ``SUCCESSION``; a change of name →
  Tier 2 ``LEGAL_NAME_CHANGE``. ``value_new`` is the filing ("Voluntary
  winding-up (פירוק מרצון) filed"), never a status the company reached.
* share transfers, allotments and shareholder updates → Tier 1
  ``SHARES_FILED`` (Stephen, 11 Nov 2026: the ownership tier, worded "filed,
  parties not published"). A relationship event, so each filing is its own
  entry and none is merged with another source's ownership change.
* directors added or removed → Tier 4 ``OFFICER_APPOINTED`` /
  ``OFFICER_RESIGNED``, with no person (the file names none).
* everything else — charges, officers, capital, articles, the annual report
  and its sub-changes, corrections — Tier 3, raw-first.

Dates are the register's status-update day: ``DateBasis.EFFECTIVE`` /
``HIGH``, the dates the register itself gives. The file keeps one year;
OpenCheck's index carries older rows forward from build to build (Stephen,
11 Nov 2026), so the coverage sentence on the tab names the day OpenCheck's
copy starts (``REGISTER_READ_NOTES.ica_israel`` in ``lib/historyMode.ts``).

Inputs: ``ica_israel.change_rows(number)`` — the index's rows for one number.
"""

from __future__ import annotations

from typing import Any

from ..sources.ica_israel_changes import DIRECTOR_ADDED, group_of, label_for
from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_SOURCE = "ica_israel"

#: Group → the notable change type its filings are.
_NOTABLE_GROUPS: dict[str, ChangeType] = {
    "status": ChangeType.STATUS_CHANGED,
    "merger": ChangeType.SUCCESSION,
    "name": ChangeType.LEGAL_NAME_CHANGE,
}

#: Appended to every share filing — the file names no party.
SHARES_NOTE = "parties not published"


def _event(number: str, row: dict[str, Any]) -> ChangeEvent:
    code = row.get("code")
    label = label_for(code, row.get("words"))
    group = group_of(code)
    field = f"request code {code}"
    if row.get("charge_id"):
        field += f" · charge {row['charge_id']}"
    common: dict[str, Any] = {
        "source_id": _SOURCE,
        "subject_id": number,
        "raw_field": field,
        "event_date": row.get("day"),
        "date_basis": DateBasis.EFFECTIVE,
        "date_confidence": DateConfidence.HIGH,
    }
    notable = _NOTABLE_GROUPS.get(group)
    if notable is not None:
        return ChangeEvent(
            record_type=RecordType.ENTITY, raw_change_type=label,
            value_new=f"{label} filed", change_type=notable,
            tier=Tier.IDENTITY_STATUS, **common,
        )
    if group == "shares":
        return ChangeEvent(
            record_type=RecordType.RELATIONSHIP, raw_change_type=label,
            value_new=f"{label} filed; {SHARES_NOTE}",
            change_type=ChangeType.SHARES_FILED, tier=Tier.OWNERSHIP_CONTROL, **common,
        )
    if group == "directors":
        added = code is not None and int(code) in DIRECTOR_ADDED
        return ChangeEvent(
            record_type=RecordType.RELATIONSHIP, raw_change_type=label,
            change_type=ChangeType.OFFICER_APPOINTED if added else ChangeType.OFFICER_RESIGNED,
            tier=Tier.BOARD_CHANGE, **common,
        )
    return ChangeEvent(
        record_type=RecordType.ENTITY, raw_change_type=label,
        change_type=None, tier=Tier.ADMIN_NOISE, **common,
    )


def ica_israel_change_events(number: str, rows: list[dict[str, Any]] | None) -> list[ChangeEvent]:
    """One event per change-log row for ``number``; ``[]`` for none."""
    return [_event(number, row) for row in rows or [] if row.get("day")]


__all__ = ["SHARES_NOTE", "ica_israel_change_events"]
