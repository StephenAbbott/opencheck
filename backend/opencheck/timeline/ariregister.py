"""Estonian change emitter — Time Machine events from e-Äriregister history.

The fourth emitter, and the same *shape* as New Zealand: a set of **dated
current-and-historic records** that we reconstruct events from. The dates are
real registry effective dates (``algus_kpv`` start / ``lopp_kpv`` end), so every
Estonian event is ``DateBasis.EFFECTIVE`` / ``DateConfidence.HIGH``.

Inputs (fetched by ``AriregisterAdapter.fetch_timeline_data`` over the RIK
X-Road SOAP open-data API, parsed here from the documented XML responses):

* ``detailandmed_v2`` with ``ainult_kehtivad=0`` (history on) — the registry
  card's dated blocks: ``arinimed`` (names), ``aadressid`` (addresses),
  ``oiguslikud_vormid`` (legal forms), ``staatused`` (statuses), and the
  persons on / off the card (``kaardile_kantud_isikud`` board + ``OSAN``
  shareholders; ``kaardivalised_isikud`` ``O`` shareholders);
* ``tegelikudKasusaajad_v2`` with ``ainult_kehtivad=0`` — beneficial owners
  (``kasusaaja``) with start dates + manner of control.

Identity blocks (names / addresses / legal forms / statuses) are dated intervals
→ Tier-2 transition events. Persons and beneficial owners are dated holdings →
Tier-1 ``OWNER_ADDED`` / ``OWNER_REMOVED`` relationship events (role / share /
control manner ride in ``counterparty``, mirroring the NZ emitter — the codelist
has no director- or BO-specific type).

NOTE: beneficial-owner events are included for now. Estonia's planned switch
to legitimate-interest BO access was POSTPONED on its 2026-07-10 start date
(https://news.err.ee/1610074816/ — current access rules remain until a revised
framework is adopted; no new date announced). The BO branch stays isolated in
``_bo_events`` so it can be dropped wholesale when the restriction eventually
takes effect — see https://github.com/StephenAbbott/opencheck/issues/22.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Callable

from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_SOURCE = "ariregister"


# --------------------------------------------------------------------------- #
# XML helpers — namespace-agnostic, tolerant of missing nodes
# --------------------------------------------------------------------------- #

def _ln(el: ET.Element) -> str:
    """Local tag name, stripping any ``{namespace}`` prefix."""
    return el.tag.rsplit("}", 1)[-1]


def _parse(xml_text: str | None) -> ET.Element | None:
    if not xml_text:
        return None
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError:
        return None


def _first(el: ET.Element | None, name: str) -> ET.Element | None:
    """First descendant (or self) with the given local name."""
    if el is None:
        return None
    for d in el.iter():
        if _ln(d) == name:
            return d
    return None


def _items(parent: ET.Element | None, list_name: str) -> list[ET.Element]:
    """The ``<item>`` children of the first ``<list_name>`` block."""
    container = _first(parent, list_name)
    if container is None:
        return []
    return [c for c in container if _ln(c) == "item"]


def _text(el: ET.Element | None, name: str) -> str | None:
    """Stripped text of the first direct child with the given local name."""
    if el is None:
        return None
    for c in el:
        if _ln(c) == name:
            t = (c.text or "").strip()
            return t or None
    return None


def _iso(raw: str | None) -> str | None:
    """Normalise a RIK date to ISO ``YYYY-MM-DD``.

    Handles the XML form ``1998-09-23Z``, ``DD.MM.YYYY``, and — defensively —
    the JSON form where dates arrive as epoch-second floats. Empty / unparseable
    input returns ``None``.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    try:  # JSON epoch-seconds fallback (Europe/Tallinn local midnight)
        epoch = float(s)
    except ValueError:
        return None
    from datetime import datetime, timezone
    try:
        from zoneinfo import ZoneInfo

        tz: Any = ZoneInfo("Europe/Tallinn")
    except Exception:  # noqa: BLE001
        tz = timezone.utc
    return datetime.fromtimestamp(epoch, tz).date().isoformat()


def _person_name(item: ET.Element) -> str:
    parts = [_text(item, "eesnimi"), _text(item, "nimi_arinimi")]
    return " ".join(p for p in parts if p).strip()


# --------------------------------------------------------------------------- #
# Event builders (same posture as the NZ emitter)
# --------------------------------------------------------------------------- #

def _entity_event(subject: str, change_type: ChangeType, *, value_old: str | None,
                  value_new: str | None, date: str | None) -> ChangeEvent:
    return ChangeEvent(
        source_id=_SOURCE, subject_id=subject, record_type=RecordType.ENTITY,
        raw_change_type="record", change_type=change_type, tier=Tier.IDENTITY_STATUS,
        value_old=value_old, value_new=value_new, event_date=date,
        date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
    )


def _emit_owner(events: list[ChangeEvent], subject: str, label: str,
                start: str | None, end: str | None) -> None:
    """Reconstruct add/remove relationship events from a dated holding/role."""
    common = dict(
        source_id=_SOURCE, subject_id=subject, record_type=RecordType.RELATIONSHIP,
        raw_change_type="record", tier=Tier.OWNERSHIP_CONTROL, counterparty=label,
        interest_start_date=start, interest_end_date=end,
        date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
    )
    if start:
        events.append(ChangeEvent(change_type=ChangeType.OWNER_ADDED, event_date=start, **common))
    if end:
        events.append(ChangeEvent(change_type=ChangeType.OWNER_REMOVED, event_date=end, **common))


def _emit_transitions(events: list[ChangeEvent], subject: str, items: list[ET.Element],
                      value_fn: Callable[[ET.Element], str | None],
                      change_type: ChangeType) -> None:
    """Turn a dated interval list (names / addresses / forms / statuses) into
    transition events: the earliest value is the original state, each later
    value is a change from the previous one at its ``algus_kpv``."""
    rows: list[tuple[str, str]] = []
    for it in items:
        val = value_fn(it)
        if val:
            rows.append((_iso(_text(it, "algus_kpv")) or "", val))
    _emit_transition_rows(events, subject, rows, change_type)


def _emit_transition_rows(events: list[ChangeEvent], subject: str,
                          rows: list[tuple[str, str]], change_type: ChangeType) -> None:
    """The transitions in ``(start day, value)`` rows — shared by the SOAP
    and REST readings so the two cannot word a change differently."""
    rows = sorted(rows, key=lambda r: r[0])
    for i in range(1, len(rows)):
        events.append(_entity_event(
            subject, change_type,
            value_old=rows[i - 1][1], value_new=rows[i][1], date=rows[i][0] or None,
        ))


# --------------------------------------------------------------------------- #
# Block extractors
# --------------------------------------------------------------------------- #

def _address_value(item: ET.Element) -> str | None:
    full = _text(item, "aadress_ads__ads_normaliseeritud_taisaadress")
    if full:
        return full
    parts = [_text(item, "tanav_maja_korter"), _text(item, "ehak_nimetus")]
    line = ", ".join(p for p in parts if p)
    return line or None


def _persons(events: list[ChangeEvent], subject: str, company: ET.Element,
             list_name: str) -> None:
    for it in _items(company, list_name):
        name = _person_name(it)
        if not name:
            continue
        role = (_text(it, "isiku_roll_tekstina") or _text(it, "isiku_roll") or "").strip()
        label = f"{name} — {role.lower()}" if role else name
        _emit_owner(events, subject, label,
                    _iso(_text(it, "algus_kpv")), _iso(_text(it, "lopp_kpv")))


def _bo_events(subject: str, bo_xml: str | None) -> list[ChangeEvent]:
    """Beneficial owners from ``tegelikudKasusaajad_v2``. Isolated so it can be
    dropped wholesale when RIK's BO access rules change on 10 July 2026."""
    root = _parse(bo_xml)
    container = _first(root, "kasusaajad")
    if container is None:
        return []
    events: list[ChangeEvent] = []
    for k in container:
        if _ln(k) != "kasusaaja":
            continue
        parts = [_text(k, "eesnimi"), _text(k, "nimi")]
        name = " ".join(p for p in parts if p).strip()
        if not name:
            continue
        manner = (_text(k, "kontrolli_teostamise_viis_tekstina")
                  or _text(k, "kontrolli_teostamise_viis") or "").strip()
        label = f"{name} — beneficial owner" + (f" ({manner})" if manner else "")
        _emit_owner(events, subject, label,
                    _iso(_text(k, "algus_kpv")),
                    _iso(_text(k, "lopp_kpv") or _text(k, "loppemise_kpv")))
    return events


# --------------------------------------------------------------------------- #
# Registry-card entries (the card-entry phase)
# --------------------------------------------------------------------------- #

#: The register's entry types (``kandeliik``) in English, with the Estonian
#: term the register uses. Read off the 16 Oct 2026 registrikaardid file
#: (2,161,627 entries); a type not listed reads as the register's own words.
CARD_ENTRY_LABELS: dict[str, str] = {
    "1": "Founding entry (asutamiskanne)",
    "2": "First entry (esmakanne)",
    "3": "Re-registration entry (ümberregistreerimiskanne)",
    "6": "Deletion entry (kustutamiskanne)",
    "7": "Declaration of bankruptcy (pankroti väljakuulutamiskanne)",
    "8": "Termination entry (lõpetamiskanne)",
    "9": "Transformation entry without a new card (ümberkujundamiskanne)",
    "10": "Change entry, not published (mitteavaldatav muutmiskanne)",
    "12": "Change entry (muutmiskanne)",
    "13": "Change of capital (kapitali muutmise kanne)",
    "14": "Change of business name (ärinime muutmise kanne)",
    "15": "Published change entry (avaldatav muutmiskanne)",
    "16": "Continuation of activity (tegevuse jätkamise kanne)",
    "17": "Additional liquidation (täiendava likvideerimise kanne)",
    "18": "Note (märkus)",
    "19": "Deletion entry without a keeper of documents (kustutamiskanne dokumentide hoidjata)",
    "21": "Commercial pledge entered (kommertspandi avamiskanne)",
    "22": "Commercial pledge deleted (kommertspandi kustutamiskanne)",
    "23": "Commercial pledge changed (kommertspandi muutmiskanne)",
    "24": "Commercial pledge changed under Commercial Code § 33(7¹)",
    "25": "Commercial pledge card closed in the former registry department",
    "30": "Restored to the register to continue activity (registrisse ennistamise kanne)",
    "32": "Change entry under Commercial Code § 525(2)",
    "33": "Change entry under Code of Civil Procedure § 595¹(1)",
    "34": "Transformation entry (ümberkujundamiskanne)",
    "41": "Card opened in the registry department of the new seat",
    "42": "Card closed in the former registry department (note)",
    "43": "First, declaratory transformation entry",
    "44": "Second, constitutive transformation entry",
    "45": "Former card of a transformed company closed (note)",
    "47": "Second, constitutive merger entry (on the acquiring company's card)",
    "49": "First, declaratory division entry (on the acquiring company's card)",
    "51": "Division entry of the company dividing off (second, constitutive)",
    "52": "Third, informative division entry (on the acquiring company's card)",
    "54": "First, declaratory division entry (on the new or other company's card)",
    "64": "First, declaratory division entry (on the new company's card)",
    "67": "Merger of agencies (on the merging agency's card)",
    "75": "Merger of agencies (on the new agency's card)",
}

#: Entry types that are a change of status, name or succession — drawn as
#: a notable History row **only when the register's own dated history was
#: not read** (no credentials, or the SOAP call failed). When it was read,
#: the same change is already a notable row with its before and after, so
#: every card entry stays in the administrative stream.
CARD_ENTRY_NOTABLE: dict[str, ChangeType] = {
    "6": ChangeType.STATUS_CHANGED, "7": ChangeType.STATUS_CHANGED,
    "8": ChangeType.STATUS_CHANGED, "16": ChangeType.STATUS_CHANGED,
    "19": ChangeType.STATUS_CHANGED, "30": ChangeType.STATUS_CHANGED,
    "14": ChangeType.LEGAL_NAME_CHANGE,
    "9": ChangeType.SUCCESSION, "34": ChangeType.SUCCESSION,
    "43": ChangeType.SUCCESSION, "44": ChangeType.SUCCESSION,
    "45": ChangeType.SUCCESSION, "47": ChangeType.SUCCESSION,
    "49": ChangeType.SUCCESSION, "51": ChangeType.SUCCESSION,
    "52": ChangeType.SUCCESSION, "54": ChangeType.SUCCESSION,
    "64": ChangeType.SUCCESSION, "67": ChangeType.SUCCESSION,
    "75": ChangeType.SUCCESSION,
}


def card_entry_label(entry_type: str | None, register_text: str | None = None) -> str:
    """An entry type in English with the register's term; the register's
    own words for a type not listed; the bare type number when the register
    publishes no wording (types 9998 and 9999, written when the non-profit
    register moved onto the e-Business Register in 2018)."""
    code = str(entry_type or "").strip()
    if code in CARD_ENTRY_LABELS:
        return CARD_ENTRY_LABELS[code]
    if register_text:
        return register_text
    return f"Register entry (type {code}, no description published)" if code else "Register entry"


def card_entry_events(
    subject: str, entries: list[dict[str, Any]], *, history_read: bool,
) -> list[ChangeEvent]:
    """One dated event per registry-card entry. Administrative (Tier 3) by
    default; the status, name and succession entries are notable only when
    ``history_read`` is false — the card is then the only dated Estonian
    history there is."""
    events: list[ChangeEvent] = []
    for e in entries:
        day = e.get("day")
        code = str(e.get("entry_type") or "")
        label = card_entry_label(code, e.get("entry_text"))
        field = f"card {e.get('card_type') or '?'} {e.get('card_no') or '?'} · entry {e.get('entry_no')}"
        notable = None if history_read else CARD_ENTRY_NOTABLE.get(code)
        events.append(ChangeEvent(
            source_id=_SOURCE, subject_id=subject, record_type=RecordType.ENTITY,
            raw_change_type=label, raw_field=field,
            value_new=label if notable else None,
            change_type=notable,
            tier=Tier.IDENTITY_STATUS if notable else Tier.ADMIN_NOISE,
            event_date=day, date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
        ))
    return events


def general_events(subject: str, general: dict[str, Any] | None) -> list[ChangeEvent]:
    """Dated events from the general data (the üldandmed phase): every status
    change the register records — a decision to dissolve, a bankruptcy
    declared or ended, activity continued, a special regime — and every
    merger, division or transformation, naming the other companies where
    the entry does. Notable (Tier 2), dated as entered."""
    from ..bods.mappers.estonia import EE_STATUS_CHANGE_LABELS, EE_SUCCESSION_LABELS

    events: list[ChangeEvent] = []
    for change in (general or {}).get("status_changes") or []:
        code = str(change.get("type") or "")
        label = EE_STATUS_CHANGE_LABELS.get(code) or change.get("type_text") or "Status change"
        events.append(ChangeEvent(
            source_id=_SOURCE, subject_id=subject, record_type=RecordType.ENTITY,
            raw_change_type=f"status change {code}".strip(), raw_field=f"entry {change.get('entry_no')}",
            value_new=label, change_type=ChangeType.STATUS_CHANGED, tier=Tier.IDENTITY_STATUS,
            event_date=change.get("day"), date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
        ))
    for change in (general or {}).get("succession") or []:
        code = str(change.get("type") or "")
        label = EE_SUCCESSION_LABELS.get(code, "Legal succession")
        partners = [
            p.get("name") or f"registry code {p.get('code')}" for p in change.get("partners") or []
        ] or [f"registry code {c}" for c in change.get("codes") or []]
        events.append(ChangeEvent(
            source_id=_SOURCE, subject_id=subject, record_type=RecordType.ENTITY,
            raw_change_type=f"succession {code}".strip(), raw_field=f"entry {change.get('entry_no')}",
            value_new=label, change_type=ChangeType.SUCCESSION, tier=Tier.IDENTITY_STATUS,
            event_date=change.get("day"), date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
            counterparty=", ".join(partners) or None,
        ))
    return events


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def _rest_address(entry: dict[str, Any]) -> str | None:
    raw = entry.get("address")
    address: dict[str, Any] = raw if isinstance(raw, dict) else {}
    full = (address.get("adsNormalizedFullAddress") or "").strip()
    if full:
        return full
    parts = [(address.get("address") or "").strip(), (address.get("ehakAsText") or "").strip()]
    line = ", ".join(p for p in parts if p)
    return line or None


def _rest_statuses(general: dict[str, Any]) -> list[tuple[str, str]]:
    """Status intervals as far as REST gives them. REST carries the current
    status, the first entry and the deletion date, not SOAP's dated
    ``staatused`` list: so "entered" at the first entry, then the current
    status where it differs — dated by the deletion, else by the latest
    status-change entry. A deletion reads exactly as SOAP has it; a
    liquidation's earlier back-and-forth is not reconstructed (the index's
    general data dates every status-change entry on the same tab)."""
    raw = general.get("status")
    status: dict[str, Any] = raw if isinstance(raw, dict) else {}
    first = _iso(general.get("firstEntryTime"))
    rows: list[tuple[str, str]] = []
    if first:
        rows.append((first, "Entered into the register"))
    code = (status.get("status") or "").strip()
    words = (status.get("statusAsText") or code).strip()
    if code and code != "R" and words:
        when = _iso(general.get("deletionDate")) if code == "K" else None
        if not when:
            changes = [_iso(c.get("startDate")) for c in general.get("statusChanges") or [] if isinstance(c, dict)]
            when = max((c for c in changes if c), default=None)
        if when:
            rows.append((when, words))
    return rows


def _rest_events(subject: str, body: dict[str, Any]) -> list[ChangeEvent]:
    """The REST reading of ``detailandmed_v2``'s blocks (the REST-history
    phase): names, addresses, legal forms, statuses and every person the
    card and its lists have named, dated by the register."""
    from ..sources.ariregister_history import rest_person_rows

    events: list[ChangeEvent] = []
    general = body.get("generalData") or {}
    _emit_transition_rows(events, subject, [
        (_iso(n.get("startDate")) or "", (n.get("content") or "").strip())
        for n in general.get("legalEntityNames") or [] if isinstance(n, dict) and (n.get("content") or "").strip()
    ], ChangeType.LEGAL_NAME_CHANGE)
    _emit_transition_rows(events, subject, [
        (_iso(a.get("startDate")) or "", v)
        for a in general.get("addresses") or [] if isinstance(a, dict) and (v := _rest_address(a))
    ], ChangeType.ADDRESS_CHANGE)
    _emit_transition_rows(events, subject, [
        (_iso(f.get("startDate")) or "", v)
        for f in general.get("legalForm") or []
        if isinstance(f, dict) and (v := (f.get("contentAsText") or f.get("content") or "").strip())
    ], ChangeType.LEGAL_FORM_CHANGE)
    _emit_transition_rows(events, subject, _rest_statuses(general), ChangeType.STATUS_CHANGED)
    for row in rest_person_rows(body):   # no codes: names, roles and dates only
        name = " ".join(p for p in (row.get("eesnimi"), row.get("nimi_arinimi")) if p).strip()
        if not name:
            continue
        role = (row.get("isiku_roll_tekstina") or row.get("isiku_roll") or "").strip()
        label = f"{name} — {role.lower()}" if role else name
        _emit_owner(events, subject, label, _iso(row.get("algus_kpv")), _iso(row.get("lopp_kpv")))
    return events


def ariregister_change_events(data: dict[str, Any]) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from the e-Äriregister history.

    ``data`` is what ``AriregisterAdapter.fetch_timeline_data`` returns:
    ``{"registry_code": str, "detail_xml": str|None, "detail_rest":
    dict|None, "bo_xml": str|None}`` — the card from REST when the key is
    set, else from SOAP; the beneficial owners always from SOAP.
    """
    if not data:
        return []
    subject = str(data.get("registry_code") or "")
    events: list[ChangeEvent] = []
    if isinstance(data.get("detail_rest"), dict):
        events += _rest_events(subject, data["detail_rest"])
        events += _bo_events(subject, data.get("bo_xml"))
        return events

    root = _parse(data.get("detail_xml"))
    company = None
    container = _first(root, "ettevotjad")
    if container is not None:
        company = next((c for c in container if _ln(c) == "item"), None)

    if company is not None:
        _emit_transitions(events, subject, _items(company, "arinimed"),
                          lambda it: _text(it, "sisu"), ChangeType.LEGAL_NAME_CHANGE)
        _emit_transitions(events, subject, _items(company, "aadressid"),
                          _address_value, ChangeType.ADDRESS_CHANGE)
        _emit_transitions(events, subject, _items(company, "oiguslikud_vormid"),
                          lambda it: _text(it, "sisu_tekstina") or _text(it, "sisu"),
                          ChangeType.LEGAL_FORM_CHANGE)
        _emit_transitions(events, subject, _items(company, "staatused"),
                          lambda it: _text(it, "staatus_tekstina") or _text(it, "staatus"),
                          ChangeType.STATUS_CHANGED)
        # Persons on the card (board + OSAN shareholders) and off the card (O).
        _persons(events, subject, company, "kaardile_kantud_isikud")
        _persons(events, subject, company, "kaardivalised_isikud")

    events += _bo_events(subject, data.get("bo_xml"))
    return events


__all__ = ["CARD_ENTRY_LABELS", "ariregister_change_events", "card_entry_events", "card_entry_label", "general_events"]
