"""GLEIF Legal Entity Events in BODS (Phase 305).

Phase 301 put GLEIF's Legal Entity Events into the Golden Copy mirror and
serves them, like the live API, as ``entity.eventGroups``. This module is how
the GLEIF entity statement reads them. Two things come out of it:

``dissolutionDate``
    GLEIF stopped filling ``entity.expiration``: on the 7 Oct 2026 Golden
    Copy it was null on all 254,762 INACTIVE records, and on the live API.
    The events are now the only date GLEIF gives for an entity ceasing.
    :func:`dissolution_event` picks the latest COMPLETED event of a type in
    :data:`TERMINAL_TYPES` — and the mapper asks only when ``entity.status``
    is INACTIVE (Stephen, 8 Oct 2026). The status gate is the decision, not a
    precaution: 2,825 ACTIVE records carry a completed merger (the acquirer's
    side of it), 456 a completed absorption and 864 a completed dissolution,
    and a date set on any of them would contradict GLEIF's own status.
    An IN_PROGRESS or withdrawn event never sets it.

one annotation per material event
    Every event except address and other-name changes
    (:data:`EXCLUDED_TYPES`, the watchlist's list), in any status, as a
    ``commenting`` annotation whose sentence states the type, the status and
    the day, and whose ``gleifLegalEntityEvent`` property carries GLEIF's
    fields as published. BODS v0.4 names are plain strings, so a legal name
    change cannot be a dated name — it points at ``/recordDetails/name``.

The calendar day (:func:`event_day`)
------------------------------------
GLEIF stores an effective date as a UTC timestamp, and the filer's local
midnight shows through: across the 7 Oct 2026 Golden Copy 445k effective
dates sit at 00:00Z, 319k at 22:00Z, 210k at 23:00Z, 68k at 21:00Z, 58k at
18:30Z and 53k at 16:00Z — midnight in CEST, CET, EEST, IST and CST.
Trimming the UTC timestamp puts every one of those a day early: GLEIF has
Westlake Pharmacy Services Ltd dissolved ``2024-10-24T22:00:00Z``, Companies
House says 25 October 2024. A UTC time at or after 12:00 is therefore read
as the next calendar day (Stephen, 8 Oct 2026), which recovers the local
date for midnight anywhere from UTC−12 to UTC+12. A timestamp with its own
non-zero offset is already local and keeps its own date. The annotation
always carries GLEIF's exact timestamp beside the day it was read as.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta
from typing import Any

from .annotations import commenting, pointer

#: Event types that, COMPLETED on an INACTIVE entity, date its end. Measured
#: on the 7 Oct 2026 Golden Copy: 253,927 of 254,762 INACTIVE records carry at
#: least one of these, completed.
TERMINAL_TYPES: frozenset[str] = frozenset(
    {
        "DISSOLUTION",
        "LIQUIDATION",
        "BANKRUPTCY",
        "ABSORPTION",
        "MERGERS_AND_ACQUISITIONS",
        "BREAKUP",
        "INSOLVENCY",
    }
)

#: Not material: renewal-time re-keying of addresses and other names. The
#: watchlist uses the same list (``watchlist.CORPORATE_EVENT_EXCLUDED``).
EXCLUDED_TYPES: frozenset[str] = frozenset(
    {"CHANGE_LEGAL_ADDRESS", "CHANGE_HQ_ADDRESS", "CHANGE_OTHER_NAMES"}
)

#: Event types in words. An unlisted type is lower-cased with its underscores
#: dropped. ``routers/watch.py`` and ``frontend/src/lib/watchlist.ts`` read
#: the same table (the latter as a copy, pinned by a test).
EVENT_TYPE_WORDS: dict[str, str] = {
    "CHANGE_LEGAL_NAME": "legal name change",
    "CHANGE_LEGAL_FORM": "legal form change",
    "MERGERS_AND_ACQUISITIONS": "merger or acquisition",
    "SPINOFF": "spin-off",
    "TRANSFORMATION_UMBRELLA_TO_STANDALONE": "fund transformation (umbrella to standalone)",
}
EVENT_STATUS_WORDS: dict[str, str] = {
    "COMPLETED": "completed",
    "IN_PROGRESS": "in progress",
    "WITHDRAWN_CANCELLED": "withdrawn or cancelled",
}

#: The annotation property carrying GLEIF's fields as published.
EVENT_PROPERTY = "gleifLegalEntityEvent"

#: The fields copied into :data:`EVENT_PROPERTY`, in this order.
_EVENT_FIELDS: tuple[str, ...] = (
    "type",
    "status",
    "effectiveDate",
    "recordedDate",
    "validationDocuments",
    "validationReference",
)

#: Annotation targets by event type. Anything else describes the entity.
_TYPE_TARGET: dict[str, str] = {"CHANGE_LEGAL_NAME": pointer("recordDetails", "name")}


def type_words(t: Any) -> str:
    t = str(t or "")
    return EVENT_TYPE_WORDS.get(t, t.lower().replace("_", " ") or "event")


def status_words(st: Any) -> str:
    st = str(st or "")
    return EVENT_STATUS_WORDS.get(st, st.lower().replace("_", " ") or "status not given")


def event_day(value: Any) -> str | None:
    """The calendar day (``YYYY-MM-DD``) a GLEIF event timestamp stands for.

    * a bare date is that date;
    * a UTC timestamp (``Z`` / ``+00:00``) at or after 12:00 is the next day —
      local midnight east of UTC, stored as UTC (see the module docstring);
    * a timestamp with any other offset keeps its own date;
    * anything unreadable is ``None``, never a guess.
    """
    raw = str(value or "").strip()
    if len(raw) < 10:
        return None
    try:
        day = date.fromisoformat(raw[:10])
    except ValueError:
        return None
    if len(raw) == 10:
        return day.isoformat()
    try:
        stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    offset = stamp.utcoffset()
    if (offset is None or offset == timedelta(0)) and stamp.hour >= 12:
        day = day + timedelta(days=1)
    return day.isoformat()


def human_day(day: str) -> str:
    """``2024-10-25`` → ``25 October 2024`` — house style, the findings
    sentences' own :func:`findings.human_date`."""
    from ..findings import human_date

    return human_date(day) or day


def events_of(entity_block: dict[str, Any] | None) -> list[dict[str, Any]]:
    """GLEIF's events for an entity, flattened from ``eventGroups`` in file
    order, each carrying its group's ``groupType``. Live API and mirror give
    the same shape (``entity_pages._event_groups``); the mirror holds at most
    five events per record, the API more — nothing here depends on which."""
    out: list[dict[str, Any]] = []
    for group in (entity_block or {}).get("eventGroups") or []:
        if not isinstance(group, dict):
            continue
        for event in group.get("events") or []:
            if isinstance(event, dict) and event.get("type"):
                out.append({**event, "groupType": group.get("groupType")})
    return out


def material(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in events if str(e.get("type") or "").upper() not in EXCLUDED_TYPES]


def dissolution_event(events: Iterable[dict[str, Any]]) -> tuple[str, dict[str, Any]] | None:
    """``(day, event)`` for the latest COMPLETED terminal event with a
    readable effective date, or ``None``. Latest, because a record with two
    (927 INACTIVE records) is a liquidation then a dissolution, or a company
    restored and dissolved again; the last is when it ceased. Ties keep the
    first in file order. The caller decides whether the entity has ceased —
    this only reads the date."""
    best: tuple[str, dict[str, Any]] | None = None
    for event in events:
        if str(event.get("status") or "").upper() != "COMPLETED":
            continue
        if str(event.get("type") or "").upper() not in TERMINAL_TYPES:
            continue
        day = event_day(event.get("effectiveDate"))
        if day and (best is None or day > best[0]):
            best = (day, event)
    return best


def event_sentence(event: dict[str, Any]) -> str:
    """``GLEIF records a legal entity event: liquidation, in progress,
    effective 6 October 2026 (GLEIF timestamp 2026-10-06T00:00:00Z; recorded
    6 October 2026).``"""
    kind = f"{type_words(event.get('type'))}, {status_words(event.get('status'))}"
    head = f"GLEIF records a legal entity event: {kind}"
    raw_effective = str(event.get("effectiveDate") or "")
    day = event_day(raw_effective)
    if day:
        head += f", effective {human_day(day)}"
    notes: list[str] = []
    if day and raw_effective and raw_effective != day:
        notes.append(f"GLEIF timestamp {raw_effective}")
    elif raw_effective and not day:
        notes.append(f"effective date as published: {raw_effective}")
    recorded = event_day(event.get("recordedDate"))
    if recorded:
        notes.append(f"recorded {human_day(recorded)}")
    if notes:
        head += f" ({'; '.join(notes)})"
    return head + "."


def event_annotations(
    events: Iterable[dict[str, Any]],
    *,
    dissolution: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """One ``commenting`` annotation per material event, in file order.

    ``dissolution`` is the event the mapper dated ``dissolutionDate`` from
    (the same object :func:`dissolution_event` returned); its annotation
    points at that field. Every other event points at the entity, except a
    legal name change, which points at the name.
    """
    out: list[dict[str, Any]] = []
    for event in material(events):
        if dissolution is not None and event is dissolution:
            target = pointer("recordDetails", "dissolutionDate")
        else:
            etype = str(event.get("type") or "").upper()
            target = _TYPE_TARGET.get(etype, pointer("recordDetails"))
        annotation = commenting(target, event_sentence(event))
        fields = {k: event[k] for k in _EVENT_FIELDS if event.get(k)}
        day = event_day(event.get("effectiveDate"))
        if day:
            fields["effectiveDay"] = day
        if event.get("groupType"):
            fields["groupType"] = event["groupType"]
        annotation[EVENT_PROPERTY] = fields
        out.append(annotation)
    return out
