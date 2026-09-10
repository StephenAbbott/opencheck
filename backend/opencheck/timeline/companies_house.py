"""Companies House change emitter — maps filing-history events to ``ChangeEvent``.

The second emitter for the Time Machine model, and the proof that the model is
source-agnostic. Companies House is *event-typed*, not field-diffed: its
filing-history API returns discrete filings with a ``category`` (and a ``type``
code like ``PSC01``), each with a real filing/effective ``date``. So where the
GLEIF emitter diffs ``valueOld``/``valueNew``, this one maps a controlled
*filing vocabulary* into the same ``ChangeType`` codelist.

Two consequences worth noting versus GLEIF:

- CH dates are **effective**, not merely recorded → ``DateBasis.EFFECTIVE`` and
  ``DateConfidence.HIGH``. (GLEIF was ``RECORDED`` / ``MEDIUM``.)
- A CH filing is an event, not a field transition, so ``value_old`` /
  ``value_new`` are left ``None``; ``raw_payload_ref`` points at the filing.

Reference: Companies House filing-history API ``category`` enum and PSC
transaction codes (PSC01–PSC09).
"""

from __future__ import annotations

from ..bods.mapper import _ch_officer_id, ch_person_statement_id
from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_T1 = Tier.OWNERSHIP_CONTROL
_T2 = Tier.IDENTITY_STATUS
_T3 = Tier.ADMIN_NOISE
_T4 = Tier.BOARD_CHANGE

# PSC (persons-with-significant-control) transaction codes → (change_type, tier).
# PSC01-03 notify a new PSC; PSC07 ceases one; PSC08/09 are PSC *statements*
# (e.g. "company has no PSC"); PSC04-06 change a PSC's details — most often the
# nature/level of control, though a later parser may find some are address-only.
_PSC_TYPES: dict[str, tuple[ChangeType, Tier]] = {
    "PSC01": (ChangeType.OWNER_ADDED, _T1),
    "PSC02": (ChangeType.OWNER_ADDED, _T1),
    "PSC03": (ChangeType.OWNER_ADDED, _T1),
    "PSC04": (ChangeType.CONTROL_NATURE_CHANGED, _T1),
    "PSC05": (ChangeType.CONTROL_NATURE_CHANGED, _T1),
    "PSC06": (ChangeType.CONTROL_NATURE_CHANGED, _T1),
    "PSC07": (ChangeType.OWNER_REMOVED, _T1),
    "PSC08": (ChangeType.REPORTING_EXCEPTION_CHANGED, _T1),
    "PSC09": (ChangeType.REPORTING_EXCEPTION_CHANGED, _T1),
}

# Officer filing types → change_type. Phase 194: these were the "filterable
# layer later" the note below anticipated. The register files a discrete form
# per event and puts the person's name in ``description_values.officer_name``,
# so a board history is already in the filing history OpenCheck fetches — it
# was arriving as untyped, unnamed Tier-3 rows (568 of Lloyds Bank PLC's 2,404
# filings). The legacy 288a/288b/288c forms are the same three events before
# 2009 and are what make a deep board history possible at all.
_OFFICER_TYPES: dict[str, ChangeType] = {
    "AP01": ChangeType.OFFICER_APPOINTED,  # director
    "AP02": ChangeType.OFFICER_APPOINTED,  # corporate director
    "AP03": ChangeType.OFFICER_APPOINTED,  # secretary
    "AP04": ChangeType.OFFICER_APPOINTED,  # corporate secretary
    "288A": ChangeType.OFFICER_APPOINTED,  # pre-2009 appointment
    "TM01": ChangeType.OFFICER_RESIGNED,  # director
    "TM02": ChangeType.OFFICER_RESIGNED,  # secretary
    "288B": ChangeType.OFFICER_RESIGNED,  # pre-2009 termination
    "CH01": ChangeType.OFFICER_DETAILS_CHANGED,  # director's particulars
    "CH02": ChangeType.OFFICER_DETAILS_CHANGED,  # corporate director's
    "CH03": ChangeType.OFFICER_DETAILS_CHANGED,  # secretary's
    "CH04": ChangeType.OFFICER_DETAILS_CHANGED,  # corporate secretary's
    "288C": ChangeType.OFFICER_DETAILS_CHANGED,  # pre-2009 change of particulars
}

#: The officer role a filing type is about, for the row's own words. Absent
#: means the register did not say which kind of officer.
_OFFICER_ROLES: dict[str, str] = {
    "AP01": "director",
    "AP02": "corporate director",
    "AP03": "secretary",
    "AP04": "corporate secretary",
    "TM01": "director",
    "TM02": "secretary",
    "CH01": "director",
    "CH02": "corporate director",
    "CH03": "secretary",
    "CH04": "corporate secretary",
}

#: The two changes that are board turnover — somebody joined, somebody left.
_BOARD_TURNOVER = frozenset(
    {ChangeType.OFFICER_APPOINTED, ChangeType.OFFICER_RESIGNED}
)

_OFFICERS_CATEGORY = "officers"


#: What the register's own words mean, for the filings whose code does not say.
#: Lloyds Bank PLC files 239 of its 568 officer filings as a bare ``288`` with
#: the event in ``description_values.description`` — "New director appointed",
#: "Director resigned", "Director's particulars changed". Ordered: a
#: termination reads "Appointment terminated director …", which contains the
#: word "appointment", so the ending is tested before the beginning.
_LEGACY_OFFICER_PHRASES: tuple[tuple[str, ChangeType], ...] = (
    ("terminated", ChangeType.OFFICER_RESIGNED),
    ("resigned", ChangeType.OFFICER_RESIGNED),
    ("particulars", ChangeType.OFFICER_DETAILS_CHANGED),
    ("appointed", ChangeType.OFFICER_APPOINTED),
)


def _officer_classification(
    ftype: str, description: str | None = None
) -> tuple[ChangeType | None, str | None]:
    """Read what an officer filing was, from its type code or the register's words.

    Companies House prefixes a corrected filing with the form it replaces —
    ``RP01AP01`` is a replacement of a director appointment — so the code that
    says what happened is at the end.

    Pre-2009 the form is a bare ``288`` and the event is only in the
    description the register wrote, which is read here rather than discarded:
    without it, 42% of Lloyds Bank PLC's board filings — everything before
    electronic filing, which is most of a company's life — would be a row that
    says nothing. A filing that neither the code nor the words explain stays
    untyped: it is a board filing whose kind we cannot name, and saying
    "details changed" would be inventing one.
    """
    code = ftype.upper()
    for form, change_type in _OFFICER_TYPES.items():
        if code == form or code.endswith(form):
            return change_type, _OFFICER_ROLES.get(form)

    words = (description or "").lower()
    if words:
        role = "secretary" if "secretary" in words else "director" if "director" in words else None
        for phrase, change_type in _LEGACY_OFFICER_PHRASES:
            if phrase in words:
                return change_type, role
    return None, None


# Non-PSC filing categories → (change_type, tier). Anything not listed (accounts,
# confirmation-statement, capital, gazette, incorporation, mortgage,
# resolution, …) is administrative noise for an *ownership* timeline and falls
# through to Tier 3 — kept, suppressed by default. Share-capital filings stay
# Tier 3; they are not beneficial-ownership changes. Officer filings are the
# exception, and get Tier 4 — see ``_OFFICER_TYPES``.
_CATEGORY_TYPES: dict[str, tuple[ChangeType, Tier]] = {
    "change-of-name": (ChangeType.LEGAL_NAME_CHANGE, _T2),
    "reregistration": (ChangeType.LEGAL_FORM_CHANGE, _T2),  # e.g. PLC → Ltd
    "address": (ChangeType.ADDRESS_CHANGE, _T2),
    "dissolution": (ChangeType.STATUS_CHANGED, _T2),
    "liquidation": (ChangeType.STATUS_CHANGED, _T2),
    "insolvency": (ChangeType.STATUS_CHANGED, _T2),
    "restoration": (ChangeType.STATUS_CHANGED, _T2),
}

# Categories whose changes are about an ownership relationship, not the entity.
_RELATIONSHIP_CATEGORIES = {"persons-with-significant-control"}


def _iso_date(value: str | None) -> str | None:
    if not value:
        return None
    return value[:10]


def _self_link(item: dict) -> str | None:
    links = item.get("links")
    if isinstance(links, dict) and links.get("self"):
        return links["self"]
    return item.get("transaction_id")


def classify_companies_house_filing(
    item: dict, *, company_id: str = ""
) -> ChangeEvent:
    """Map one Companies House filing-history item to a ``ChangeEvent``.

    ``item`` is a filing-history entry (``category``, ``type``, ``date``,
    optional ``action_date``, ``links``/``transaction_id``). ``company_id`` is
    the stable subject recordId (defaults to the company number).
    """
    category = (item.get("category") or "").lower()
    ftype = (item.get("type") or "").upper()
    values = item.get("description_values")
    officer_name = (values or {}).get("officer_name") if isinstance(values, dict) else None

    if category == _OFFICERS_CATEGORY:
        # Who sits on the board is a relationship between a person and the
        # company, so it is typed as one — but it is never beneficial
        # ownership, which is why it has a tier of its own rather than a
        # place among the notable rows.
        legacy_description = (
            (values or {}).get("description") if isinstance(values, dict) else None
        )
        change_type, role = _officer_classification(
            ftype, str(legacy_description) if legacy_description else item.get("description")
        )
        # Phase 198: every officer filing is Tier 3 now, including the
        # appointments and resignations Phase 194 promoted. The board stream
        # is built from the officers list instead (``officer_change_events``),
        # which names every person and carries the register's officer id, so
        # a row can address the person the graph draws. Leaving these at Tier
        # 4 as well would count the same appointment twice, from two records
        # of it. They keep their ``change_type`` — a typed row in the
        # administrative stream still says what it was.
        tier = _T3
        return ChangeEvent(
            source_id="companies_house",
            subject_id=company_id or item.get("company_number") or "",
            record_type=RecordType.RELATIONSHIP,
            raw_change_type=ftype or category,
            raw_field=f"{category}/{role}" if role else category or None,
            value_old=None,
            value_new=None,
            raw_payload_ref=_self_link(item),
            change_type=change_type,
            tier=tier,
            # The filing carries the officer's name and no officer id, so this
            # is as far as a board row can point.
            counterparty=str(officer_name) if officer_name else None,
            event_date=_iso_date(item.get("action_date")) or _iso_date(item.get("date")),
            date_basis=DateBasis.EFFECTIVE,
            date_confidence=DateConfidence.HIGH,
        )

    if category in _RELATIONSHIP_CATEGORIES:
        record_type = RecordType.RELATIONSHIP
        # Dispatch on the PSC transaction code; unknown PSC codes are still
        # PSC-related, so surface them as a control change rather than noise.
        change_type, tier = _PSC_TYPES.get(
            ftype, (ChangeType.CONTROL_NATURE_CHANGED, _T1)
        )
    else:
        record_type = RecordType.ENTITY
        change_type, tier = _CATEGORY_TYPES.get(category, (None, _T3))

    # CH gives a real effective date where known (action_date), else the filing
    # date — both are effective-basis, high confidence.
    event_date = _iso_date(item.get("action_date")) or _iso_date(item.get("date"))

    return ChangeEvent(
        source_id="companies_house",
        subject_id=company_id or item.get("company_number") or "",
        record_type=record_type,
        raw_change_type=ftype or category,
        raw_field=category or None,
        value_old=None,  # CH filings are events, not field transitions
        value_new=None,
        raw_payload_ref=_self_link(item),
        change_type=change_type,
        tier=tier,
        event_date=event_date,
        date_basis=DateBasis.EFFECTIVE,
        date_confidence=DateConfidence.HIGH,
    )


def officer_change_events(
    officers_payload: dict, *, company_id: str = ""
) -> list[ChangeEvent]:
    """Board turnover from the register's officers list, one event per date.

    Phase 198. The board stream was built from filing history, which was free
    — ``/history`` already fetched it — but a filing publishes only a name, so
    a row could say who joined and never point at them. This reads the
    officers list instead, where the register keys every officer with the id
    Phase 193 keys a person statement on. Measured on Lloyds Bank PLC: 195
    dated events against the filing history's 246, but **195 of 195 name the
    person and carry the id** where only 75 of 246 named anyone at all.

    What that trades away is the tail: Companies House holds officer records
    from 1992 for this company while its filings go back to 1986, so 40 events
    leave the board stream. They are not lost — every officer filing is still
    in the administrative stream, typed.

    One officer yields up to two events: appointed, and resigned. An officer
    with neither date yields none — the register knows them, but not when.
    """
    events: list[ChangeEvent] = []
    for officer in officers_payload.get("items") or []:
        if not isinstance(officer, dict):
            continue
        name = str(officer.get("name") or "").strip() or None
        role = (officer.get("officer_role") or "").strip().lower() or None
        officer_id = _ch_officer_id(officer)
        # The same id the mapper keys the person statement on, derived by the
        # mapper's own function so the two cannot drift apart.
        person_sid = (
            ch_person_statement_id(company_id, officer) if officer_id else None
        )
        link = ((officer.get("links") or {}).get("officer") or {}).get("appointments")

        for date_field, change_type in (
            ("appointed_on", ChangeType.OFFICER_APPOINTED),
            ("resigned_on", ChangeType.OFFICER_RESIGNED),
        ):
            date = _iso_date(officer.get(date_field))
            if not date:
                continue
            events.append(
                ChangeEvent(
                    source_id="companies_house",
                    subject_id=company_id,
                    record_type=RecordType.RELATIONSHIP,
                    raw_change_type=date_field,
                    raw_field=f"{_OFFICERS_CATEGORY}/{role}" if role else _OFFICERS_CATEGORY,
                    value_old=None,
                    value_new=None,
                    raw_payload_ref=link,
                    change_type=change_type,
                    tier=_T4,
                    counterparty=name,
                    party_id=officer_id,
                    party_statement_id=person_sid,
                    # appointed_on / resigned_on are the dates the appointment
                    # began and ended, not when the register was told.
                    event_date=date,
                    date_basis=DateBasis.EFFECTIVE,
                    date_confidence=DateConfidence.HIGH,
                )
            )
    return events


__all__ = ["classify_companies_house_filing", "officer_change_events"]
