"""Lithuania — Register of Legal Entities (JAR) → BODS v0.4.

Split out of ``mapper.py`` in Phase 246; rewritten for the open-data index in
Phase 325. ``mapper.py`` re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..annotations import annotate, pointer, transformation
from ..statements import SOURCE_NAMES, _addr, dated_by_cut, make_entity_statement


def map_jar_lithuania(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Every statement dated by the register's cut (Phase 315). See
    ``_map_jar_lithuania`` for the mapping."""
    yield from dated_by_cut(_map_jar_lithuania(bundle), (bundle or {}).get("cut"))


def _jar_entity_type(form_code: str | None) -> str:
    """``stateBody`` for budget-funded public bodies, else ``registeredEntity``.

    Everything in JAR was created by registration in it. The exceptions are
    the forms that are part of the state — budget institutions, the central
    bank, regional development councils. State and municipal **enterprises**
    stay ``registeredEntity``: an SOE is not a government body (Phase 240).
    Until Phase 325 this read scraped abbreviations and had it the other way
    round — enterprises as ``stateBody``, associations as ``legalEntity``.
    """
    from ...sources.jar_lithuania import STATE_BODY_FORMS  # local import avoids a cycle

    return "stateBody" if str(form_code or "") in STATE_BODY_FORMS else "registeredEntity"


def _map_jar_lithuania(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a JarLithuaniaAdapter bundle to one BODS v0.4 entity statement.

    The register's open data carries the entity alone: name, code, registered
    address, legal form, registration date, legal status and, for a
    deregistered entity, the deregistration date. No officers, no owners —
    participants moved to JANGIS, a legitimate-interest system — so there is
    nothing to relate the entity to.
    """
    from ...sources.jar_lithuania import STATUSES, form_label  # local import avoids a cycle

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return

    lt_code: str = str(bundle.get("lt_code") or bundle.get("hit_id") or "").strip()
    if not lt_code:
        return
    legal_form: str = str(bundle.get("legal_form") or "").strip()
    form_code = bundle.get("legal_form_code")
    withheld = bool(bundle.get("name_withheld"))
    name: str = str(bundle.get("name") or "").strip()
    if not name:
        name = f"{legal_form} {lt_code}" if legal_form else f"LT-JAR {lt_code}"

    raw_address: str = str(bundle.get("address") or "").strip()
    registered_on = bundle.get("registered_on") or None
    deregistered_on = (bundle.get("deregistered_on") or None) if bundle.get("deregistered") else None
    # The register publishes a deregistration date before the registration
    # date for 273 entities (12 Oct 2026; mostly early-1990s state
    # enterprises, whose "registration" reads as a later entry into JAR).
    # BODS forbids a dissolution before founding. The deregistration is the
    # event that changes a decision, so it stays and the founding date is
    # left out — said in an annotation, with both of the register's dates.
    inverted = bool(registered_on and deregistered_on and deregistered_on < registered_on)
    statement = make_entity_statement(
        source_id="jar_lithuania",
        local_id=lt_code,
        name=name,
        jurisdiction=("Lithuania", "LT"),
        identifiers=[
            {
                "id": lt_code,
                "scheme": "LT-JAR",
                "schemeName": "Juridinių Asmenų Registras (Lithuanian Register of Legal Entities)",
            }
        ],
        founding_date=None if inverted else registered_on,
        addresses=[_addr("registered", raw_address, "LT")] if raw_address else [],
        entity_type=_jar_entity_type(form_code),
        entity_details=form_label(legal_form, form_code),
        source_url=bundle.get("link") or None,
    )

    if withheld:
        # The open data puts the legal form's name where a sole
        # proprietorship's or partnership's name would be — those names
        # usually carry the owner's. The name above is GLEIF's, or the form
        # and code when no GLEIF name reached the mapper.
        borrowed = bool(str(bundle.get("name") or "").strip())
        annotate(
            statement,
            transformation(
                pointer("recordDetails", "name"),
                (
                    "The Register of Legal Entities' open data does not publish "
                    f"names for this legal form ({legal_form or 'unstated'}); "
                    + (
                        "this is the legal name GLEIF holds for the LEI."
                        if borrowed
                        else "this is the legal form and entity code, not a name."
                    )
                ),
            ),
        )

    label = SOURCE_NAMES["jar_lithuania"]
    if bundle.get("deregistered"):
        _liveness.apply_register_status(
            statement,
            source_label=label,
            liveness=_liveness.TERMINAL,
            raw="Išregistruotas (deregistered)",
            since=bundle.get("deregistered_on") or None,
        )
    else:
        cls, gloss, _rank = STATUSES.get(
            str(bundle.get("status_code") or ""), (_liveness.UNKNOWN, None, None)
        )
        raw_status = str(bundle.get("status") or "").strip()
        _liveness.apply_register_status(
            statement,
            source_label=label,
            liveness=cls,  # type: ignore[arg-type]
            raw=(f"{raw_status} ({gloss})" if raw_status and gloss else raw_status) or None,
            since=(bundle.get("status_since") or None) if cls == _liveness.PENDING else None,
        )
    if inverted:
        annotate(
            statement,
            transformation(
                pointer("recordDetails", "dissolutionDate"),
                (
                    f"The register gives this entity a registration date of "
                    f"{registered_on}, after its deregistration on "
                    f"{deregistered_on}. BODS does not allow a dissolution "
                    "before founding, so foundingDate is left out and the "
                    "deregistration date kept."
                ),
            ),
        )
    yield statement
