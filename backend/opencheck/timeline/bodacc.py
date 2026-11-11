"""BODACC emitter — History events from France's official bulletin of notices.

The ninth emitter. Every notice ``opencheck.bodacc`` returns about the company
(as the subject, or as the seller of a business) is one event, linked to the
notice on bodacc.fr (``record_url``). Stephen's decisions (11 Nov 2026):
every family is shown (decision C) and no person is ever named (decision B,
enforced by the reader's allowlist). The tiers:

* **Tier 2 ``STATUS_CHANGED``** — a collective procedure opened, converted,
  extended or resumed; a plan adopted or modified; proceedings closed; a
  conciliation agreement; professional recovery; a removal from the register
  (radiation); a dissolution. A judgment is dated by the judgment
  (``EFFECTIVE`` / ``HIGH``), anything else by its publication.
* **Tier 2 ``LEGAL_NAME_CHANGE``** with the new name (a company's, never a
  person's), **``SUCCESSION``** for a universal transfer of assets, and
  **``BUSINESS_SOLD``** — the company sold a business, the buyer named by SIREN
  and company name where the buyer is a company (decision F).
* **Tier 3** — everything else: other modifications, purchases, a new
  establishment, the registration, accounts filed, claims filed in a
  procedure, a withdrawn notice. Dated by publication (``RECORDED``).

Every BODACC event stands alone in the merge (``assemble._DISCRETE_SOURCES``):
an opening and a closure inside the 400-day window are two rows.
"""

from __future__ import annotations

from typing import Any

from .. import bodacc as _b
from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_SOURCE = "bodacc"

#: Kind → the notable change type it is. Everything else is Tier 3.
_STATUS_KINDS = frozenset({
    _b.KIND_INSOLVENCY_OPENED, _b.KIND_INSOLVENCY_CLOSED, _b.KIND_INSOLVENCY_PLAN,
    _b.KIND_CONCILIATION, _b.KIND_PROFESSIONAL_RECOVERY, _b.KIND_DEREGISTERED,
    _b.KIND_DISSOLUTION,
})
_NOTABLE: dict[str, ChangeType] = {
    **{k: ChangeType.STATUS_CHANGED for k in _STATUS_KINDS},
    _b.KIND_NAME_CHANGE: ChangeType.LEGAL_NAME_CHANGE,
    _b.KIND_UNIVERSAL_TRANSFER: ChangeType.SUCCESSION,
    _b.KIND_BUSINESS_SOLD: ChangeType.BUSINESS_SOLD,
}

_FAMILY_WORDS = {
    "collective": "collective proceedings",
    "conciliation": "conciliation",
    "retablissement_professionnel": "professional recovery",
    "radiation": "removal from the register",
    "modification": "modification",
    "vente": "sale of a business",
    "creation": "establishment created",
    "immatriculation": "registration",
    "dpc": "accounts filed",
    "divers": "other notice",
}


def counterparty_words(party: dict[str, Any] | None) -> str | None:
    """The buyer of a business, as the notice names it — a company only."""
    if not party:
        return None
    name, siren = party.get("name"), party.get("siren")
    if name and siren:
        return f"{name} (SIREN {siren})"
    if siren:
        return f"SIREN {siren}"
    return "a buyer that is not a company (not named)"


def _event(siren: str, notice: dict[str, Any]) -> ChangeEvent:
    kind = notice.get("kind") or _b.KIND_OTHER
    label = notice.get("label") or "Notice"
    effective = notice.get("effective")
    family = _FAMILY_WORDS.get(notice.get("family") or "", notice.get("family") or "notice")
    field = f"BODACC {notice.get('publication') or ''} · {family}".replace("  ", " ")
    if notice.get("notice_type") == "rectificatif":
        field += " · corrected notice"
    common: dict[str, Any] = {
        "source_id": _SOURCE,
        "subject_id": siren,
        "raw_change_type": label,
        "raw_field": field,
        "raw_payload_ref": notice.get("id"),
        "record_url": notice.get("url"),
        "event_date": effective or notice.get("published"),
        "date_basis": DateBasis.EFFECTIVE if effective else DateBasis.RECORDED,
        "date_confidence": DateConfidence.HIGH,
    }
    notable = _NOTABLE.get(kind)
    if notable is ChangeType.BUSINESS_SOLD:
        return ChangeEvent(
            record_type=RecordType.RELATIONSHIP, change_type=notable,
            tier=Tier.IDENTITY_STATUS, value_new=label,
            counterparty=counterparty_words(notice.get("counterparty")), **common,
        )
    if notable is ChangeType.LEGAL_NAME_CHANGE:
        return ChangeEvent(
            record_type=RecordType.ENTITY, change_type=notable,
            tier=Tier.IDENTITY_STATUS, value_new=notice.get("detail") or label, **common,
        )
    if notable is not None:
        return ChangeEvent(
            record_type=RecordType.ENTITY, change_type=notable,
            tier=Tier.IDENTITY_STATUS, value_new=label, **common,
        )
    return ChangeEvent(
        record_type=RecordType.ENTITY, change_type=None, tier=Tier.ADMIN_NOISE,
        value_new=label, **common,
    )


def bodacc_change_events(siren: str, payload: dict[str, Any] | None) -> list[ChangeEvent]:
    """One event per notice in ``payload`` (``bodacc.notices_for``)."""
    return [
        _event(siren, n)
        for n in (payload or {}).get("notices") or []
        if n.get("published")
    ]


__all__ = ["bodacc_change_events", "counterparty_words"]
