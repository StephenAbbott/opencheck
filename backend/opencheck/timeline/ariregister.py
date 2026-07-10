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
    rows.sort(key=lambda r: r[0])
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
# Public entry point
# --------------------------------------------------------------------------- #

def ariregister_change_events(data: dict[str, Any]) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from the e-Äriregister SOAP responses.

    ``data`` is what ``AriregisterAdapter.fetch_timeline_data`` returns:
    ``{"registry_code": str, "detail_xml": str|None, "bo_xml": str|None}``.
    """
    if not data:
        return []
    subject = str(data.get("registry_code") or "")
    events: list[ChangeEvent] = []

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


__all__ = ["ariregister_change_events"]
