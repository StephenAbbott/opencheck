"""French association change emitter — Time Machine events from the Journal
officiel des associations et fondations d'entreprise (JOAFE, DILA, Licence
Ouverte), read by :mod:`..sources.joafe`.

The ninth emitter. A JOAFE notice publishes what an association declared to
its prefecture: its creation, a change (of name, of seat, of purpose or of its
statutes) or its dissolution, dated by the declaration. An accounts deposit
says the association or foundation published its annual accounts for a year.
Stephen's decisions (11 Nov 2026, the JOAFE History ticket):

* creation and dissolution → Tier 2 ``STATUS_CHANGED``; a new name → Tier 2
  ``LEGAL_NAME_CHANGE`` with the old and new names; a seat transfer → Tier 2
  ``ADDRESS_CHANGE`` worded with the **commune and postcode only** (B: the
  seat's street lines name people);
* any other change (purpose, statutes) → Tier 3, never its text;
* accounts deposits → Tier 3, the admin stream (C: about 90 % of rows);
* a rectification replaces the notice it names; a cancellation removes it and
  leaves one Tier 3 row saying the notice was withdrawn.

Dates: a declaration day is ``EFFECTIVE`` / ``HIGH``, the date the association
gave; without one the publication day is ``RECORDED``. An accounts deposit is
dated by DILA's validation day (``RECORDED``), else its publication.

``subject_id`` is the RNA number for a notice and the SIREN for a deposit;
``raw_payload_ref`` is DILA's notice id.

Inputs: the ``rows`` of :func:`..sources.joafe.notices`.
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

_SOURCE = "joafe"

_MONTHS = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sept", "Oct", "Nov", "Dec",
)

#: The JOAFE's kinds of notice, in English with the French beside them.
KIND_WORDS: dict[str, str] = {
    "Création": "Creation declared (déclaration de création)",
    "Modification": "Change declared (déclaration de modification)",
    "Dissolution": "Dissolution declared (déclaration de dissolution)",
    "Compte": "Annual accounts published (dépôt des comptes)",
    "Suspension d’activité": "Activity suspended (suspension d’activité)",
}

CORRECTED = "corrected notice"


def _day_words(day: str) -> str:
    year, month, dd = day.split("-")
    return f"{int(dd)} {_MONTHS[int(month) - 1]} {year}"


def _seat_words(seat: dict[str, str] | None) -> str | None:
    if not seat:
        return None
    commune, postcode = seat.get("commune"), seat.get("postcode")
    if commune and postcode:
        return f"{commune} ({postcode})"
    return commune or postcode


def _kind_label(row: dict[str, Any]) -> str:
    kind = row.get("kind") or ""
    return KIND_WORDS.get(kind, kind or "Notice published")


def _dated(row: dict[str, Any]) -> tuple[str | None, DateBasis, DateConfidence]:
    if row.get("source") == "dca":
        day = row.get("accounts_validated") or row.get("published")
        return day, DateBasis.RECORDED, DateConfidence.HIGH
    if row.get("declared"):
        return row["declared"], DateBasis.EFFECTIVE, DateConfidence.HIGH
    return row.get("published"), DateBasis.RECORDED, DateConfidence.MEDIUM


def _common(row: dict[str, Any], field: str) -> dict[str, Any]:
    day, basis, confidence = _dated(row)
    subject = row.get("rna") if row.get("source") != "dca" else (row.get("siren") or row.get("rna"))
    return {
        "source_id": _SOURCE,
        "subject_id": subject or "",
        "raw_field": field,
        "raw_payload_ref": row.get("id") or None,
        "event_date": day,
        "date_basis": basis,
        "date_confidence": confidence,
    }


def _accounts_event(row: dict[str, Any]) -> ChangeEvent:
    label = KIND_WORDS["Compte"]
    end = row.get("accounts_period_end")
    if end:
        label += f", year ending {_day_words(end)}"
    if row.get("accounts_version"):
        label += f" ({CORRECTED})"
    return ChangeEvent(
        record_type=RecordType.ENTITY, raw_change_type=label, value_new=label,
        change_type=None, tier=Tier.ADMIN_NOISE,
        **_common(row, "accounts deposit"),
    )


def _notice_events(row: dict[str, Any]) -> list[ChangeEvent]:
    kind = row.get("kind") or ""
    corrected = f" ({CORRECTED})" if row.get("state") == "Rectificatif" else ""
    field = f"JOAFE notice · {kind or 'notice'}"
    if kind == "Création":
        label = KIND_WORDS[kind] + corrected
        return [ChangeEvent(
            record_type=RecordType.ENTITY, raw_change_type=label,
            value_new="Declared", change_type=ChangeType.STATUS_CHANGED,
            tier=Tier.IDENTITY_STATUS, **_common(row, field),
        )]
    if kind == "Dissolution":
        label = KIND_WORDS[kind] + corrected
        return [ChangeEvent(
            record_type=RecordType.ENTITY, raw_change_type=label,
            value_new="Dissolution declared", change_type=ChangeType.STATUS_CHANGED,
            tier=Tier.IDENTITY_STATUS, **_common(row, field),
        )]
    if kind == "Modification":
        out: list[ChangeEvent] = []
        if row.get("title_new") and row.get("title_new") != row.get("title_old"):
            out.append(ChangeEvent(
                record_type=RecordType.ENTITY,
                raw_change_type="Change of name declared" + corrected,
                value_old=row.get("title_old"), value_new=row["title_new"],
                change_type=ChangeType.LEGAL_NAME_CHANGE, tier=Tier.IDENTITY_STATUS,
                **_common(row, field + " · name"),
            ))
        new_seat = _seat_words(row.get("seat_new"))
        if new_seat:
            out.append(ChangeEvent(
                record_type=RecordType.ENTITY,
                raw_change_type="Transfer of seat declared" + corrected,
                value_old=_seat_words(row.get("seat_old")), value_new=new_seat,
                change_type=ChangeType.ADDRESS_CHANGE, tier=Tier.IDENTITY_STATUS,
                **_common(row, field + " · seat"),
            ))
        if row.get("purpose_changed"):
            out.append(ChangeEvent(
                record_type=RecordType.ENTITY,
                raw_change_type="Change of purpose declared" + corrected,
                change_type=None, tier=Tier.ADMIN_NOISE,
                **_common(row, field + " · purpose"),
            ))
        if not out:
            label = KIND_WORDS[kind] + corrected
            out.append(ChangeEvent(
                record_type=RecordType.ENTITY, raw_change_type=label,
                change_type=None, tier=Tier.ADMIN_NOISE, **_common(row, field),
            ))
        return out
    label = _kind_label(row) + corrected
    return [ChangeEvent(
        record_type=RecordType.ENTITY, raw_change_type=label,
        change_type=None, tier=Tier.ADMIN_NOISE, **_common(row, field),
    )]


def _withdrawn_event(row: dict[str, Any]) -> ChangeEvent:
    ref = row.get("corrects") or {}
    day = ref.get("published")
    label = (
        f"Notice of {_day_words(day)} withdrawn (annulation)" if day
        else "Notice withdrawn (annulation)"
    )
    common = _common(row, "JOAFE notice · cancellation")
    common.update(event_date=row.get("published"), date_basis=DateBasis.RECORDED,
                  date_confidence=DateConfidence.HIGH)
    return ChangeEvent(
        record_type=RecordType.ENTITY, raw_change_type=label, value_new=label,
        change_type=None, tier=Tier.ADMIN_NOISE, **common,
    )


def _settled(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop every notice a later rectification or cancellation names, and
    every accounts deposit a later version of the same year replaces."""
    replaced: set[tuple[str, str]] = set()
    for row in rows:
        ref = row.get("corrects")
        if row.get("state") in ("Rectificatif", "Annulation") and ref:
            replaced.add((ref["published"], ref["number"]))
    latest_accounts: dict[tuple[Any, Any], int] = {}
    for row in rows:
        if row.get("source") == "dca":
            key = (row.get("siren") or row.get("rna"), row.get("accounts_period_end"))
            latest_accounts[key] = max(latest_accounts.get(key, 0), row.get("accounts_version") or 0)
    out = []
    for row in rows:
        if row.get("source") == "dca":
            key = (row.get("siren") or row.get("rna"), row.get("accounts_period_end"))
            if (row.get("accounts_version") or 0) < latest_accounts[key]:
                continue
        elif (
            row.get("state") not in ("Rectificatif", "Annulation")
            and (row.get("published"), row.get("number")) in replaced
        ):
            continue
        out.append(row)
    return out


def joafe_change_events(rows: list[dict[str, Any]] | None) -> list[ChangeEvent]:
    """The events for one association's JOAFE rows; ``[]`` for none."""
    events: list[ChangeEvent] = []
    for row in _settled(list(rows or [])):
        if row.get("source") == "dca":
            events.append(_accounts_event(row))
        elif row.get("state") == "Annulation":
            events.append(_withdrawn_event(row))
        else:
            events.extend(_notice_events(row))
    return [e for e in events if e.event_date]


__all__ = ["CORRECTED", "KIND_WORDS", "joafe_change_events"]
