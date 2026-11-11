"""France change emitter — Time Machine events from INSEE's Sirene history periods.

The ``sirene`` source's ``GET /siren/{siren}`` answer carries every period of
the legal unit (``periodesUniteLegale``): a span with a ``dateDebut`` /
``dateFin`` and the unit's values for that span. The adapter keeps them
(``sources.sirene.history_periods``, oldest first) and this module reads one
period against the one before it, so the History tab costs no extra request
and covers every French SIREN — companies, associations, foundations, public
bodies.

Measured on INSEE's StockUniteLegaleHistorique of 1 Nov 2026 over 5,998
ISSUED French LEIs (the Notion ticket "France / Sirene history periods on the
History tab"); Stephen's decisions A–E (11 Nov 2026) are what this implements:

* **Only a value that changes from one real value to another is an event.**
  INSEE's ``changement…`` flags are not read: a blank becoming a value is the
  first time INSEE recorded it (204 of 249 state changes, 2,182 of 2,196 ESS
  changes), and the société-à-mission flag fires with no change of value
  (1,532 of 1,809). The first period is never an event; its start is a
  placeholder (1900-01-01) or the start of INSEE's computerised record
  (1954-01-01, …), not a founding.
* **Name** (``denominationUniteLegale``) → ``LEGAL_NAME_CHANGE``, notable.
  A change of case, accent, spacing or punctuation only is not one
  (25 of 2,198: "SOLAIRE DIRECT" → "SOLAIREDIRECT").
* **State** A → C → ``STATUS_CHANGED`` "ceased"; C → A "active again".
* **Legal form** (``categorieJuridiqueUniteLegale``) → ``LEGAL_FORM_CHANGE``,
  notable (decision A), worded from INSEE's own labels.
* **Head office** (``nicSiegeUniteLegale``) → an administrative row naming the
  new establishment's SIRET, with no address (decision B): the address is one
  more ``/siret`` call per move against a 30-a-minute key.
* **Activity** within one NAF version → an administrative row; a change across
  versions (NAF 1993 → Rev1 → Rev2, and NAF 2025 from 1 Jan 2027) is a recode
  and is dropped (decision D).
* **Trading name, ESS and société à mission** → administrative rows, the last
  two only on a real ``N`` ↔ ``O`` change (decision E). A sole trader's
  personal name is never read: the adapter does not keep those fields.
* **Dates.** ``dateDebut`` is INSEE's own date for the change: ``EFFECTIVE``.
  Except that many older changes were dated by INSEE's annual processing —
  1 January of each year up to 2008, or 25 December — where the year is right
  and the day is not (900 of 2,198 renames). Those are ``RECORDED`` with low
  confidence and the tab says "recorded by INSEE in <year>" (decision C).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from .model import ChangeEvent, ChangeType, DateBasis, DateConfidence, RecordType, Tier

_SOURCE = "sirene"

#: INSEE's administrative states.
_STATE_WORDS = {"A": "active", "C": "ceased"}

#: The last year whose 1 January INSEE used as a processing date. From 2009
#: a 1 January change is as likely to be a real one (a merger effective at the
#: start of a financial year), so it is taken as dated.
_BATCH_NEW_YEAR_UNTIL = 2008


def batch_dated(day: str | None) -> bool:
    """Whether INSEE's date is a processing date whose day cannot be trusted:
    1 January up to :data:`_BATCH_NEW_YEAR_UNTIL`, or any 25 December."""
    if not day or len(day) < 10:
        return False
    month_day = day[5:10]
    if month_day == "12-25":
        return True
    try:
        return month_day == "01-01" and int(day[:4]) <= _BATCH_NEW_YEAR_UNTIL
    except ValueError:
        return False


def _fold(text: str | None) -> str:
    """The name with case, accents, spacing and punctuation set aside."""
    ascii_text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9]", "", ascii_text.upper())


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def _category_words(code: str) -> str:
    from ..bods.mappers.france import sirene_category_label

    label = sirene_category_label(code)
    return f"{label} ({code})" if label else code


def _usual_name(period: dict[str, Any]) -> str:
    parts = [
        _clean(period.get(f"denominationUsuelle{i}UniteLegale")) for i in (1, 2, 3)
    ]
    return " / ".join(p for p in parts if p)


def sirene_change_events(bundle: dict[str, Any]) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from a ``SireneAdapter.fetch`` bundle."""
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return []
    siren = str(bundle.get("siren") or "")
    periods = [p for p in bundle.get("periods") or [] if isinstance(p, dict)]
    periods.sort(key=lambda p: str(p.get("dateDebut") or ""))
    events: list[ChangeEvent] = []

    for previous, current in zip(periods, periods[1:]):
        day = str(current.get("dateDebut") or "")[:10] or None
        if day is None:
            continue
        batch = batch_dated(day)
        basis = DateBasis.RECORDED if batch else DateBasis.EFFECTIVE
        confidence = DateConfidence.LOW if batch else DateConfidence.HIGH

        def event(
            *,
            raw_field: str,
            old: str | None,
            new: str | None,
            change_type: ChangeType | None,
            tier: Tier,
            raw_change_type: str,
        ) -> None:
            events.append(
                ChangeEvent(
                    source_id=_SOURCE,
                    subject_id=siren,
                    record_type=RecordType.ENTITY,
                    raw_change_type=raw_change_type,
                    raw_field=raw_field,
                    value_old=old,
                    value_new=new,
                    change_type=change_type,
                    tier=tier,
                    event_date=day,
                    date_basis=basis,
                    date_confidence=confidence,
                )
            )

        # Name — notable.
        old_name = _clean(previous.get("denominationUniteLegale"))
        new_name = _clean(current.get("denominationUniteLegale"))
        if old_name and new_name and _fold(old_name) != _fold(new_name):
            event(
                raw_field="denominationUniteLegale",
                old=old_name,
                new=new_name,
                change_type=ChangeType.LEGAL_NAME_CHANGE,
                tier=Tier.IDENTITY_STATUS,
                raw_change_type="name",
            )

        # Administrative state — notable.
        old_state = _clean(previous.get("etatAdministratifUniteLegale"))
        new_state = _clean(current.get("etatAdministratifUniteLegale"))
        if old_state in _STATE_WORDS and new_state in _STATE_WORDS and old_state != new_state:
            event(
                raw_field="etatAdministratifUniteLegale",
                old=_STATE_WORDS[old_state],
                new=_STATE_WORDS[new_state] if new_state == "C" else "active again",
                change_type=ChangeType.STATUS_CHANGED,
                tier=Tier.IDENTITY_STATUS,
                raw_change_type="state",
            )

        # Legal form — notable (decision A).
        old_form = _clean(previous.get("categorieJuridiqueUniteLegale"))
        new_form = _clean(current.get("categorieJuridiqueUniteLegale"))
        if old_form and new_form and old_form != new_form:
            event(
                raw_field="categorieJuridiqueUniteLegale",
                old=_category_words(old_form),
                new=_category_words(new_form),
                change_type=ChangeType.LEGAL_FORM_CHANGE,
                tier=Tier.IDENTITY_STATUS,
                raw_change_type="legal form",
            )

        # Head office — administrative, the new SIRET only (decision B).
        old_nic = _clean(previous.get("nicSiegeUniteLegale"))
        new_nic = _clean(current.get("nicSiegeUniteLegale"))
        if old_nic and new_nic and old_nic != new_nic:
            event(
                raw_field="head office",
                old=f"SIRET {siren}{old_nic}",
                new=f"SIRET {siren}{new_nic}",
                change_type=None,
                tier=Tier.ADMIN_NOISE,
                raw_change_type="head office moved",
            )

        # Activity within one NAF version — administrative (decision D).
        old_naf = _clean(previous.get("activitePrincipaleUniteLegale"))
        new_naf = _clean(current.get("activitePrincipaleUniteLegale"))
        old_nom = _clean(previous.get("nomenclatureActivitePrincipaleUniteLegale"))
        new_nom = _clean(current.get("nomenclatureActivitePrincipaleUniteLegale"))
        if old_naf and new_naf and old_naf != new_naf and old_nom and old_nom == new_nom:
            event(
                raw_field="main activity",
                old=f"{old_naf} ({old_nom})",
                new=f"{new_naf} ({new_nom})",
                change_type=None,
                tier=Tier.ADMIN_NOISE,
                raw_change_type="main activity",
            )

        # Trading name — administrative (decision E).
        old_usual, new_usual = _usual_name(previous), _usual_name(current)
        if old_usual and new_usual and _fold(old_usual) != _fold(new_usual):
            event(
                raw_field="trading name",
                old=old_usual,
                new=new_usual,
                change_type=None,
                tier=Tier.ADMIN_NOISE,
                raw_change_type="trading name",
            )

        # ESS and société à mission — administrative, N ↔ O only (decision E).
        for key, field in (
            ("economieSocialeSolidaireUniteLegale", "social and solidarity economy"),
            ("societeMissionUniteLegale", "société à mission"),
        ):
            old_flag, new_flag = _clean(previous.get(key)), _clean(current.get(key))
            if {old_flag, new_flag} == {"N", "O"}:
                event(
                    raw_field=field,
                    old="no" if old_flag == "N" else "yes",
                    new="no" if new_flag == "N" else "yes",
                    change_type=None,
                    tier=Tier.ADMIN_NOISE,
                    raw_change_type=field,
                )
    return events


__all__ = ["batch_dated", "sirene_change_events"]
