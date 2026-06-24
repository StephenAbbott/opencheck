"""Denmark (CVR) change emitter — Time Machine events from bitemporal records.

CVR (Datafordeler) is *bitemporal*: every name / address / legal-form / status /
branche record carries a ``virkningFra`` / ``virkningTil`` validity period. The
``cvr_denmark`` adapter already fetches the full history (no point-in-time filter)
and preserves it in the bundle's ``_raw_*`` lists, so this emitter reconstructs
change events with **no extra API calls**.

``virkningFra`` is a real *effective* date (validity time, not when the registry
recorded it), so CVR events are ``DateBasis.EFFECTIVE`` / ``DateConfidence.HIGH``
— the same high-quality dates as Companies House and New Zealand.

Mapped change types (all Tier-2 identity/status, entity-record):
  * ``_raw_navn``        (sekvens 0)  → ``LEGAL_NAME_CHANGE``
  * ``_raw_form``                     → ``LEGAL_FORM_CHANGE``
  * ``_raw_virksomhed``  (status)     → ``STATUS_CHANGED``
  * ``_raw_adressering`` (seat)       → ``ADDRESS_CHANGE``
  * ``_raw_branche``     (sekvens 0)  → unmapped Tier-3 admin noise (kept, hidden)

Participants (``_raw_deltager``) are intentionally skipped: they reference only an
``enhedsId`` (no name — CVRPerson is access-restricted), so there is nothing
readable to show.
"""

from __future__ import annotations

from typing import Any, Callable

from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

# Raw CVR status code → readable label (raw-first: fall back to the raw code).
_STATUS_LABELS = {
    "AKTIV": "active",
    "OPHOERT": "dissolved",
    "OPLØST": "dissolved",
    "UNDER_KONKURS": "in bankruptcy",
    "UNDER_TVANGSOPLOSNING": "in forced dissolution",
    "UNDER_FRIVILLIG_LIKVIDATION": "in voluntary liquidation",
    "TVANGSOPLOEST_FEJLREGISTRERING": "dissolved (error registration)",
    "SLETTET": "deleted",
}


def _date(value: Any) -> str | None:
    s = str(value or "").strip()
    return s[:10] if s else None


def _navn_val(r: dict) -> str:
    return str(r.get("vaerdi") or "").strip()


def _form_val(r: dict) -> str:
    return str(r.get("vaerdiTekst") or r.get("vaerdi") or "").strip()


def _branche_val(r: dict) -> str:
    return str(r.get("vaerdi") or "").strip()


def _status_val(r: dict) -> str:
    s = str(r.get("status") or "").strip()
    if not s:
        return ""
    return _STATUS_LABELS.get(s.upper(), s.lower())


def _addr_line(a: Any) -> str | None:
    if not isinstance(a, dict):
        return None
    parts = [
        a.get("CVRAdresse_vejnavn"),
        a.get("CVRAdresse_husnummerFra"),
        a.get("CVRAdresse_postnummer"),
        a.get("CVRAdresse_postdistrikt"),
        a.get("CVRAdresse_kommunenavn"),
    ]
    line = ", ".join(str(p).strip() for p in parts if p is not None and str(p).strip())
    return line or None


def _period_changes(
    rows: Any,
    val_fn: Callable[[dict], str],
    change_type: ChangeType | None,
    tier: Tier,
    subject: str,
    *,
    raw_change_type: str,
    seq0: bool = False,
) -> list[ChangeEvent]:
    """Turn a dated bitemporal block into transition events.

    Records are ordered by ``virkningFra``; the earliest non-empty value is the
    original state (not a change), and each later record whose value *actually*
    differs from the previous one is a change at its ``virkningFra``. No-op
    re-registrations (same value, new period) are skipped.
    """
    items = [r for r in (rows or []) if isinstance(r, dict)]
    if seq0:
        items = [r for r in items if (r.get("sekvens") or 0) == 0]
    items = [r for r in items if val_fn(r)]
    items.sort(key=lambda r: str(r.get("virkningFra") or ""))

    out: list[ChangeEvent] = []
    prev: str | None = None
    for r in items:
        v = val_fn(r)
        if prev is not None and v != prev:
            out.append(
                ChangeEvent(
                    source_id="cvr_denmark", subject_id=subject,
                    record_type=RecordType.ENTITY, raw_change_type=raw_change_type,
                    change_type=change_type, tier=tier,
                    value_old=prev, value_new=v, event_date=_date(r.get("virkningFra")),
                    date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
                )
            )
        prev = v
    return out


def _address_changes(rows: Any, subject: str) -> list[ChangeEvent]:
    """Reconstruct ADDRESS_CHANGE events from the registered-seat address history."""
    items = [a for a in (rows or []) if isinstance(a, dict)]
    seat = [a for a in items if "beliggenhed" in (a.get("AdresseringAnvendelse") or "").lower()]
    items = [a for a in (seat or items) if _addr_line(a)]
    items.sort(key=lambda a: str(a.get("virkningFra") or ""))

    out: list[ChangeEvent] = []
    prev: str | None = None
    for a in items:
        v = _addr_line(a)
        if prev is not None and v != prev:
            out.append(
                ChangeEvent(
                    source_id="cvr_denmark", subject_id=subject,
                    record_type=RecordType.ENTITY, raw_change_type="address",
                    change_type=ChangeType.ADDRESS_CHANGE, tier=Tier.IDENTITY_STATUS,
                    value_old=prev, value_new=v, event_date=_date(a.get("virkningFra")),
                    date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
                )
            )
        prev = v
    return out


def cvr_change_events(bundle: dict[str, Any]) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from a CVR (Datafordeler) bundle."""
    if not bundle:
        return []
    subject = str(bundle.get("cvr_number") or "")
    events: list[ChangeEvent] = []
    events += _period_changes(
        bundle.get("_raw_navn"), _navn_val, ChangeType.LEGAL_NAME_CHANGE,
        Tier.IDENTITY_STATUS, subject, raw_change_type="navn", seq0=True)
    events += _period_changes(
        bundle.get("_raw_form"), _form_val, ChangeType.LEGAL_FORM_CHANGE,
        Tier.IDENTITY_STATUS, subject, raw_change_type="virksomhedsform")
    events += _period_changes(
        bundle.get("_raw_virksomhed"), _status_val, ChangeType.STATUS_CHANGED,
        Tier.IDENTITY_STATUS, subject, raw_change_type="status")
    events += _address_changes(bundle.get("_raw_adressering"), subject)
    # Industry (branche) recodes are administrative — kept raw-first but Tier-3,
    # so they only show under "administrative changes".
    events += _period_changes(
        bundle.get("_raw_branche"), _branche_val, None,
        Tier.ADMIN_NOISE, subject, raw_change_type="branche", seq0=True)
    return events


__all__ = ["cvr_change_events"]
