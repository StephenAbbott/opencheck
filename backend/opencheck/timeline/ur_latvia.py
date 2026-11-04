"""Latvian change emitter — Time Machine events from UR's dated open data.

The seventh emitter, in the New Zealand shape: a set of **dated current
records** that events are reconstructed from. Every date here is the
register's own, so an event is ``DateBasis.EFFECTIVE`` / ``DateConfidence.HIGH``
where the table carries the date the change took effect (``date_from``,
``registered``, ``terminated``, ``date_to``, a proceeding's start and end)
and ``DateBasis.RECORDED`` / ``HIGH`` where it carries only the day UR entered
the row (``registered_on`` on the beneficial-owner and officer tables).

**What UR's open data does not publish — measured on 3 Nov 2026, the
Estonia–Latvia emitters ticket:** an end date on any people table. The
beneficial-owner, member (SIA shareholder), stockholder (AS shareholder) and
officer tables are current rows only; a removed party simply leaves the next
day's file, and a row is edited in place. So this emitter says when every
*current* owner, shareholder, beneficial owner and officer arrived —
additions only, never a removal — and the History tab says so beside the
register (``REGISTER_READ_NOTES.ur_latvia`` in ``lib/historyMode.ts``). The
one true history UR publishes is the name history (``date_to`` on each former
name), and the dated event tables: liquidations, reorganisations, insolvency
proceedings, suspensions and prohibitions, securing measures.

Tiers (Stephen, 3 Nov 2026): SIA members, AS stockholders and beneficial
owners → Tier-1 ``OWNER_ADDED``; officers → Tier-4 ``OFFICER_APPOINTED``;
registration, termination, name changes, liquidations, reorganisations,
insolvency and suspensions → Tier 2; securing measures and a depository's
dematerialised-shares row → Tier 3, raw-first.

Inputs: ``UrLatviaAdapter.fetch_timeline_data`` — the lookup's five tables
and six more, every row in the CKAN datastore's shape.
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

_SOURCE = "ur_latvia"


def _day(value: Any) -> str | None:
    s = str(value or "").strip()
    return s[:10] if len(s) >= 10 else None


def _text(row: dict[str, Any], key: str) -> str:
    return str(row.get(key) or "").strip()


# --------------------------------------------------------------------------- #
# The register's vocabularies, in English with the register's Latvian term.
# A code not listed reads as the register's own ``*_text`` words.
# --------------------------------------------------------------------------- #

LIQUIDATION_LABELS: dict[str, str] = {
    "LIQUIDATION": "Liquidation process (likvidācijas process)",
    "ACTIVITY_TERMINATED": "Activity terminated (darbības izbeigšana)",
}

REORGANIZATION_LABELS: dict[str, str] = {
    "DIVISION": "Divided by splitting (sadalīta sašķelšanas ceļā)",
    "SEPARATION": "Divided by spin-off (sadalīta nodalīšanas ceļā)",
    "ACQUISITION": "Merged by acquisition (apvienošanas ceļā pievienota)",
    "MERGER": "Merged (apvienošana)",
    "TRANSFORMATION": "Transformed (pārveidošana)",
}

SUSPENSION_LABELS: dict[str, str] = {
    "ACTIVITY_SUSPENDED": "Activity suspended (darbības apturēšana)",
    "ECONOMIC_ACTIVITY_SUSPENDED": "Economic activity suspended (saimnieciskās darbības apturēšana)",
    "ACTIVITY_RESUMED": "Activity resumed (darbības atjaunošana)",
    "ACTIVITY_PROHIBITED": "Activity prohibited (darbības aizliegums)",
}

SECURING_MEASURE_LABELS: dict[str, str] = {
    "PROHIBITION": "Prohibition (aizliegums)",
}

INSOLVENCY_FORM_LABELS: dict[str, str] = {
    "INSOLVENCY": "Insolvency proceedings (maksātnespējas process)",
    "LEGAL_PROTECTION": "Legal protection proceedings (tiesiskās aizsardzības process)",
    "EXTRAJUDICIAL_LEGAL_PROTECTION": "Out-of-court legal protection proceedings (ārpustiesas tiesiskās aizsardzības process)",
}

GOVERNING_BODY_LABELS: dict[str, str] = {
    "EXECUTIVE_BOARD": "management board (valde)",
    "EXECUTIVE_BODY": "executive body",
    "SUPERVISORY_BOARD": "supervisory board (padome)",
    "COUNCIL": "council (padome)",
    "AUDIT_COMMISSION": "audit commission (revīzijas komisija)",
    "LIQUIDATOR": "liquidator (likvidators)",
    "ADMINISTRATOR": "administrator (administrators)",
    "TRUSTEE_IN_BANKRUPTCY": "trustee in bankruptcy",
}

POSITION_LABELS: dict[str, str] = {
    "BOARD_MEMBER": "board member (valdes loceklis)",
    "CHAIR_OF_BOARD": "chair of the board (valdes priekšsēdētājs)",
    "OWNER": "owner (īpašnieks)",
    "LIQUIDATOR": "liquidator (likvidators)",
    "ADMINISTRATOR": "administrator (administrators)",
}

#: How a person table names the holder when it names anyone.
_LV_FORENAME_SURNAME = ("forename", "surname")


def _label(code: str, table: dict[str, str], register_text: str) -> str:
    return table.get(code) or register_text or code


def _words(value: str) -> str:
    """``ECONOMIC_ACTIVITY_SUSPENDED`` → ``economic activity suspended``."""
    return value.replace("_", " ").strip().lower()


# --------------------------------------------------------------------------- #
# Event builders
# --------------------------------------------------------------------------- #

def _entity_event(subject: str, change_type: ChangeType | None, *, raw: str,
                  value_old: str | None = None, value_new: str | None,
                  date: str | None, basis: DateBasis = DateBasis.EFFECTIVE,
                  tier: Tier = Tier.IDENTITY_STATUS, field: str | None = None,
                  counterparty: str | None = None) -> ChangeEvent:
    return ChangeEvent(
        source_id=_SOURCE, subject_id=subject, record_type=RecordType.ENTITY,
        raw_change_type=raw, raw_field=field, change_type=change_type, tier=tier,
        value_old=value_old, value_new=value_new, event_date=date,
        date_basis=basis, date_confidence=DateConfidence.HIGH, counterparty=counterparty,
    )


def _party_added(subject: str, label: str, start: str | None, *, basis: DateBasis,
                 officer: bool, raw: str) -> ChangeEvent | None:
    """One arrival. UR publishes no departures, so there is never a
    matching removal — the tab says so."""
    if not start:
        return None
    return ChangeEvent(
        source_id=_SOURCE, subject_id=subject, record_type=RecordType.RELATIONSHIP,
        raw_change_type=raw, counterparty=label, interest_start_date=start,
        change_type=ChangeType.OFFICER_APPOINTED if officer else ChangeType.OWNER_ADDED,
        tier=Tier.BOARD_CHANGE if officer else Tier.OWNERSHIP_CONTROL,
        event_date=start, date_basis=basis, date_confidence=DateConfidence.HIGH,
    )


def _holder_name(row: dict[str, Any]) -> str:
    name = _text(row, "name")
    if name:
        return name
    return " ".join(p for p in (_text(row, k) for k in _LV_FORENAME_SURNAME) if p).strip()


def _share(row: dict[str, Any]) -> str | None:
    """``"100 shares of 10.00 EUR"`` where the row states a holding."""
    shares = row.get("number_of_shares")
    try:
        n = float(shares if shares is not None else "")
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    count = f"{n:,.0f}" if n == int(n) else f"{n:,}"
    nominal = row.get("share_nominal_value")
    currency = _text(row, "share_currency")
    try:
        value = float(nominal if nominal is not None else "")
    except (TypeError, ValueError):
        value = 0.0
    unit = "share" if n == 1 else "shares"
    if value > 0 and currency:
        return f"{count} {unit} of {value:,.2f} {currency}"
    return f"{count} {unit}"


# --------------------------------------------------------------------------- #
# Tables
# --------------------------------------------------------------------------- #

def _entity_events(subject: str, entity: dict[str, Any]) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    registered = _day(entity.get("registered"))
    regtype = _text(entity, "regtype_text") or _text(entity, "regtype")
    if registered:
        # The registration itself is administrative, as a Companies House
        # incorporation filing is: the company existing is not a change to it.
        events.append(_entity_event(
            subject, None, raw="registered", tier=Tier.ADMIN_NOISE,
            value_new=f"Registered in the {regtype}" if regtype else "Registered", date=registered,
        ))
    terminated = _day(entity.get("terminated"))
    if terminated:
        closed = _text(entity, "closed")
        events.append(_entity_event(
            subject, ChangeType.STATUS_CHANGED, raw="terminated", field=f"closed {closed}" if closed else None,
            value_old="Registered", value_new="Terminated (izbeigta)", date=terminated,
        ))
    return events


def _name_events(subject: str, current_name: str, names: list[dict[str, Any]]) -> list[ChangeEvent]:
    """Each former name ended on ``date_to``; the name after it is the next
    former name by date, else the current one. The register publishes no
    start dates, so the first name's own start is not an event."""
    dated = sorted(
        ((_day(n.get("date_to")) or "", _text(n, "name")) for n in names if _text(n, "name")),
        key=lambda r: r[0],
    )
    events: list[ChangeEvent] = []
    for i, (day, name) in enumerate(dated):
        following = dated[i + 1][1] if i + 1 < len(dated) else current_name
        if not following or following == name:
            continue
        events.append(_entity_event(
            subject, ChangeType.LEGAL_NAME_CHANGE, raw="name history",
            value_old=name, value_new=following, date=day or None,
        ))
    return events


def _liquidation_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    for row in rows:
        code = _text(row, "liquidation_type")
        label = _label(code, LIQUIDATION_LABELS, _text(row, "liquidation_type_text"))
        grounds = _text(row, "grounds_for_liquidation")
        events.append(_entity_event(
            subject, ChangeType.STATUS_CHANGED, raw=f"liquidation {code}".strip(),
            field=grounds or None, value_new=label, date=_day(row.get("date_from")),
        ))
    return events


def _reorganization_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    for row in rows:
        code = _text(row, "reorganization_type")
        label = _label(code, REORGANIZATION_LABELS, _text(row, "reorganization_type_text"))
        source = _text(row, "source_entity_regcode")
        final = _text(row, "final_entity_regcode")
        subject_is_source = source.lstrip("0") == subject.lstrip("0")
        other = final if subject_is_source else source
        side = "into" if subject_is_source else "from"
        events.append(_entity_event(
            subject, ChangeType.SUCCESSION, raw=f"reorganisation {code}".strip(),
            value_new=f"{label} — {side} registration number {other}" if other else label,
            date=_day(row.get("registered")),
            counterparty=f"registration number {other}" if other else None,
        ))
    return events


def _insolvency_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    for row in rows:
        form = _text(row, "proceeding_form")
        label = _label(form, INSOLVENCY_FORM_LABELS, _words(form))
        case = _text(row, "court_case_initial_number")
        court = _text(row, "court_name")
        field = ", ".join(p for p in (case, court) if p) or None
        started = _day(row.get("proceeding_started_on"))
        if started:
            events.append(_entity_event(
                subject, ChangeType.STATUS_CHANGED, raw=f"insolvency {form} opened".strip(),
                field=field, value_new=f"{label} opened", date=started,
            ))
        ended = _day(row.get("proceeding_ended_on"))
        if ended:
            resolution = _text(row, "proceeding_resolution_name")
            events.append(_entity_event(
                subject, ChangeType.STATUS_CHANGED, raw=f"insolvency {form} ended".strip(),
                field=field, value_old=f"{label} opened",
                value_new=f"{label} ended" + (f": {resolution}" if resolution else ""), date=ended,
            ))
    return events


def _suspension_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    for row in rows:
        code = _text(row, "suspension_code")
        label = _label(code, SUSPENSION_LABELS, _text(row, "suspension_code_text"))
        events.append(_entity_event(
            subject, ChangeType.STATUS_CHANGED, raw=f"suspension {code}".strip(),
            field=_text(row, "ground_for") or None, value_new=label, date=_day(row.get("date_from")),
        ))
    return events


def _securing_measure_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    """A prohibition on disposing of shares or reducing the capital, by a
    bailiff or the tax authority: administrative, raw-first (Tier 3)."""
    events: list[ChangeEvent] = []
    for row in rows:
        code = _text(row, "securing_measure_type")
        label = _label(code, SECURING_MEASURE_LABELS, _text(row, "securing_measure_type_text"))
        details = _text(row, "type_details")
        institution = _text(row, "institution_name")
        case = _text(row, "case_number")
        text = label + (f": {details}" if details else "")
        events.append(_entity_event(
            subject, None, raw=f"securing measure {code}".strip(), tier=Tier.ADMIN_NOISE,
            field=", ".join(p for p in (case, institution) if p) or None,
            value_new=text, date=_day(row.get("date_from")),
        ))
    return events


def _member_events(subject: str, rows: list[dict[str, Any]], *, what: str) -> list[ChangeEvent]:
    """SIA members and AS stockholders: an arrival dated by ``date_from``,
    the register's effective date (``registered_on`` where it has none)."""
    events: list[ChangeEvent] = []
    for row in rows:
        kind = _text(row, "entity_type")
        if kind == "DEPOSITORY":
            depository = _text(row, "depository_name") or "a central securities depository"
            events.append(_entity_event(
                subject, None, raw="dematerialised stocks", tier=Tier.ADMIN_NOISE,
                value_new=f"Shares held in dematerialised form at {depository}; holders are not named by the register",
                date=_day(row.get("date_from")) or _day(row.get("registered_on")),
            ))
            continue
        name = _holder_name(row)
        if not name:
            continue
        share = _share(row)
        label = f"{name} — {what}" + (f" ({share})" if share else "")
        start = _day(row.get("date_from"))
        basis = DateBasis.EFFECTIVE
        if not start:
            start = _day(row.get("registered_on"))
            basis = DateBasis.RECORDED
        event = _party_added(subject, label, start, basis=basis, officer=False, raw=what)
        if event:
            events.append(event)
    return events


def _bo_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    """Beneficial owners: dated only by the day UR entered the row."""
    events: list[ChangeEvent] = []
    for row in rows:
        name = _holder_name(row)
        if not name:
            continue
        nationality = _text(row, "nationality")
        label = f"{name} — beneficial owner" + (f" ({nationality})" if nationality else "")
        event = _party_added(
            subject, label, _day(row.get("registered_on")), basis=DateBasis.RECORDED,
            officer=False, raw="beneficial owner",
        )
        if event:
            events.append(event)
    return events


def _officer_events(subject: str, rows: list[dict[str, Any]]) -> list[ChangeEvent]:
    """Officers: the board stream, dated by the day UR entered the row."""
    events: list[ChangeEvent] = []
    for row in rows:
        name = _holder_name(row)
        if not name:
            continue
        body = _text(row, "governing_body")
        position = _text(row, "position")
        role_parts = [
            POSITION_LABELS.get(position) or (_words(position) if position else ""),
            GOVERNING_BODY_LABELS.get(body) or (_words(body) if body else ""),
        ]
        role = ", ".join(p for p in role_parts if p) or "officer"
        rights = _text(row, "rights_of_representation_type")
        if rights:
            role += f"; represents {_words(rights)}"
        event = _party_added(
            subject, f"{name} — {role}", _day(row.get("registered_on")),
            basis=DateBasis.RECORDED, officer=True, raw="officer",
        )
        if event:
            events.append(event)
    return events


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def ur_latvia_change_events(data: dict[str, Any] | None) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from ``UrLatviaAdapter.fetch_timeline_data``."""
    if not data:
        return []
    subject = str(data.get("regcode") or "")
    entity: dict[str, Any] = data.get("entity") or {}
    events: list[ChangeEvent] = []
    events += _entity_events(subject, entity)
    events += _name_events(subject, _text(entity, "name"), data.get("historical_names") or [])
    events += _liquidation_events(subject, data.get("liquidations") or [])
    events += _reorganization_events(subject, data.get("reorganizations") or [])
    events += _insolvency_events(subject, data.get("insolvency") or [])
    events += _suspension_events(subject, data.get("suspensions") or [])
    events += _securing_measure_events(subject, data.get("securing_measures") or [])
    events += _member_events(subject, data.get("members") or [], what="shareholder (dalībnieks)")
    events += _member_events(subject, data.get("stockholders") or [], what="stockholder (akcionārs)")
    events += _bo_events(subject, data.get("beneficial_owners") or [])
    events += _officer_events(subject, data.get("officers") or [])
    return events


__all__ = [
    "GOVERNING_BODY_LABELS", "INSOLVENCY_FORM_LABELS", "LIQUIDATION_LABELS", "POSITION_LABELS",
    "REORGANIZATION_LABELS", "SECURING_MEASURE_LABELS", "SUSPENSION_LABELS", "ur_latvia_change_events",
]
