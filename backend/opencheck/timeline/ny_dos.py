"""New York change emitter — Time Machine events from DOS filings (Phase 263).

The sixth register on the History tab. Its shape is closest to Denmark's: the
ordinary adapter fetch already returns everything (four datasets keyed on the
DOS ID), and events are reconstructed from the rows rather than read off a
change feed.

* **Name changes** — the Name Status History, one ``LEGAL_NAME_CHANGE`` per
  later name, dated by the ``eff_date`` of the filing that made it (the
  All Filings row with the same ``film_num``), else by its filing date. Both
  are dates DOS published for the change itself: ``EFFECTIVE``.
* **Status changes** — the Entity Status History, one ``STATUS_CHANGED`` per
  transition, dated the same way. A domestic entity that goes inactive on a
  merger or consolidation certificate is a ``SUCCESSION``: it was absorbed.
* **Chief executive officers** — the Tier-4 board stream, from the CEO named
  on each statement. DOS publishes no appointment date, only the statement
  that names the CEO, so a change is known to fall **between two
  statements**: ``SNAPSHOT_WINDOW``, which the tab words "approximate", with
  the window on the event. Rows point at the person statement the graph draws
  for a current CEO (``party_statement_id``), as Companies House rows do.
* **Every other filing** — a Tier-3 administrative row (biennial statements,
  certificates of change, amendments). Kept, suppressed by default.

Assumed-name filings are never read: they sit under a separate numbering that
collides with DOS IDs (see ``sources/ny_dos.py``).
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

_SOURCE = "ny_dos"

#: Documents whose filing, when it makes a domestic entity inactive, means it
#: was absorbed into another rather than dissolved.
_SUCCESSION_MARKERS = ("MERGER", "CONSOLIDATION")


def _filing_date(filing: dict[str, Any] | None, fallback_row: dict[str, Any]) -> str | None:
    from ..sources.ny_dos import iso_date

    return iso_date((filing or {}).get("eff_date")) or iso_date(fallback_row.get("date_filed"))


def _name_events(
    dos_id: str, name_rows: list[dict[str, Any]], by_film: dict[str, dict[str, Any]]
) -> tuple[list[ChangeEvent], set[str]]:
    """Name transitions, and the films they were read from."""
    from ..sources.ny_dos import clean_field

    rows = [r for r in name_rows if clean_field(r.get("corp_name"))]
    out: list[ChangeEvent] = []
    used: set[str] = set()
    for previous, current in zip(rows, rows[1:]):
        old, new = clean_field(previous.get("corp_name")), clean_field(current.get("corp_name"))
        if old == new:
            continue
        film = clean_field(current.get("film_num"))
        filing = by_film.get(film)
        if film:
            used.add(film)
        out.append(
            ChangeEvent(
                source_id=_SOURCE,
                subject_id=dos_id,
                record_type=RecordType.ENTITY,
                raw_change_type=clean_field((filing or {}).get("documenttype")) or "name",
                raw_field="corp_name",
                value_old=old,
                value_new=new,
                change_type=ChangeType.LEGAL_NAME_CHANGE,
                tier=Tier.IDENTITY_STATUS,
                event_date=_filing_date(filing, current),
                date_basis=DateBasis.EFFECTIVE,
                date_confidence=DateConfidence.HIGH,
            )
        )
    return out, used


def _status_events(
    dos_id: str,
    status_rows: list[dict[str, Any]],
    by_film: dict[str, dict[str, Any]],
    *,
    domestic: bool,
) -> tuple[list[ChangeEvent], set[str]]:
    """Status transitions, and the films they were read from."""
    from ..sources.ny_dos import STATUS_INACTIVE, clean_field

    out: list[ChangeEvent] = []
    used: set[str] = set()
    previous = ""
    for row in status_rows:
        status = clean_field(row.get("status"))
        if not status:
            continue
        if previous and status != previous:
            film = clean_field(row.get("film_num"))
            filing = by_film.get(film)
            document = clean_field((filing or {}).get("documenttype"))
            absorbed = (
                domestic
                and status == STATUS_INACTIVE
                and any(m in document.upper() for m in _SUCCESSION_MARKERS)
            )
            out.append(
                ChangeEvent(
                    source_id=_SOURCE,
                    subject_id=dos_id,
                    record_type=RecordType.ENTITY,
                    raw_change_type=document or "status",
                    raw_field="status",
                    value_old=previous,
                    value_new=status,
                    change_type=ChangeType.SUCCESSION if absorbed else ChangeType.STATUS_CHANGED,
                    tier=Tier.IDENTITY_STATUS,
                    event_date=_filing_date(filing, row),
                    date_basis=DateBasis.EFFECTIVE,
                    date_confidence=DateConfidence.HIGH,
                )
            )
            if film:
                used.add(film)
        previous = status
    return out, used


def _ceo_events(
    dos_id: str,
    addresses: list[dict[str, Any]],
    *,
    company_name: str,
    formed_on: str | None,
    current: set[str],
) -> list[ChangeEvent]:
    """Appointments and departures between consecutive statements naming a CEO."""
    from .. import names
    from ..bods.mappers.us_ny import ny_ceo_statement_id
    from ..sources.ny_dos import ADDR_CHIEF_EXECUTIVE, ceo_name, clean_field, iso_date

    statements: list[tuple[str | None, list[str]]] = []
    index: dict[str, int] = {}
    for row in addresses:
        if clean_field(row.get("addr_type")) != ADDR_CHIEF_EXECUTIVE:
            continue
        person = ceo_name(row.get("name"))
        if not person or names.org_comparable_name(person) == names.org_comparable_name(company_name):
            continue
        key = clean_field(row.get("film_num")) or clean_field(row.get("date_filed"))
        if key not in index:
            index[key] = len(statements)
            statements.append((iso_date(row.get("date_filed")), []))
        people = statements[index[key]][1]
        if person not in people:
            people.append(person)

    out: list[ChangeEvent] = []

    def event(change_type: ChangeType, person: str, window: tuple[str | None, str | None]) -> None:
        start, end = window
        out.append(
            ChangeEvent(
                source_id=_SOURCE,
                subject_id=dos_id,
                record_type=RecordType.RELATIONSHIP,
                raw_change_type="chief executive officer",
                raw_field=(
                    "chief_executive_officer"
                    + (f" (between the statements of {start} and {end})" if start and end else "")
                ),
                change_type=change_type,
                tier=Tier.BOARD_CHANGE,
                counterparty=person,
                party_statement_id=ny_ceo_statement_id(dos_id, person) if person in current else None,
                event_date=end,
                date_basis=DateBasis.SNAPSHOT_WINDOW,
                date_confidence=DateConfidence.LOW,
                date_range=(start, end) if start and end else None,
            )
        )

    previous_date: str | None = formed_on
    previous_people: list[str] = []
    for filed_on, people in statements:
        window = (previous_date, filed_on)
        for person in people:
            if person not in previous_people:
                event(ChangeType.OFFICER_APPOINTED, person, window)
        for person in previous_people:
            if person not in people:
                event(ChangeType.OFFICER_RESIGNED, person, window)
        previous_date, previous_people = filed_on, people
    return out


def ny_dos_change_events(bundle: dict[str, Any]) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from a ``NyDosAdapter.fetch`` bundle."""
    from ..sources.ny_dos import clean_field, entity_filings_of, iso_date, summarise

    if not bundle or bundle.get("is_stub") or not bundle.get("filings"):
        return []
    summary = summarise(bundle)
    dos_id = summary.get("dos_id") or ""
    filings = entity_filings_of(bundle)
    by_film = {clean_field(f.get("film_num")): f for f in filings if f.get("film_num")}

    def ordered(key: str) -> list[dict[str, Any]]:
        rows = [r for r in bundle.get(key) or [] if isinstance(r, dict)]
        return [r for _, r in sorted(enumerate(rows), key=lambda p: (clean_field(p[1].get("date_filed")), p[0]))]

    events: list[ChangeEvent] = []
    name_events, name_films = _name_events(dos_id, ordered("name_history"), by_film)
    status_events, status_films = _status_events(
        dos_id, ordered("status_history"), by_film, domestic=bool(summary.get("domestic"))
    )
    events += name_events + status_events
    events += _ceo_events(
        dos_id,
        ordered("addresses"),
        company_name=summary.get("name") or "",
        formed_on=summary.get("formed_on") or summary.get("authority_on"),
        current=set(summary.get("ceos") or []),
    )

    # Every other filing is an administrative row. The filings a name or
    # status event was read from are not repeated.
    for filing in filings:
        film = clean_field(filing.get("film_num"))
        if film and (film in status_films or film in name_films):
            continue
        events.append(
            ChangeEvent(
                source_id=_SOURCE,
                subject_id=dos_id,
                record_type=RecordType.ENTITY,
                raw_change_type=clean_field(filing.get("documenttype")) or "filing",
                raw_field=clean_field(filing.get("law")) or None,
                raw_payload_ref=film or None,
                change_type=None,
                tier=Tier.ADMIN_NOISE,
                event_date=iso_date(filing.get("eff_date")) or iso_date(filing.get("date_filed")),
                date_basis=DateBasis.EFFECTIVE,
                date_confidence=DateConfidence.HIGH,
            )
        )
    return events


__all__ = ["ny_dos_change_events"]
