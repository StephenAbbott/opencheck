"""Colombia — RUES (Confecámaras, via datos.gov.co) → BODS v0.4.

``mapper.py`` re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..annotations import annotate, commenting, pointer, transformation
from ..statements import (
    SOURCE_NAMES,
    _stable_id,
    content_addressed,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


def _rues_role_details(summary: dict[str, Any]) -> str:
    chamber = summary.get("chamber")
    where = f" at the {chamber.title()} chamber of commerce" if chamber else ""
    return f"Legal representative (representante legal), as recorded in RUES{where}"


def _rues_entity_details(summary: dict[str, Any]) -> str:
    form = summary.get("legal_form") or ""
    text = form.capitalize() if form else "Registered"
    text += " in the Colombian mercantile register (RUES)"
    if summary.get("chamber") and summary.get("matricula"):
        text += (
            f"; matrícula {summary['matricula']} at the "
            f"{summary['chamber'].title()} chamber of commerce"
        )
    return text


@content_addressed
def map_rues_colombia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a RuesColombiaAdapter bundle to BODS v0.4 statements.

    One entityStatement for the company, identified by its NIT under
    ``CO-RUE`` (written ``900396759-5``, the form the GLEIF mapper writes for
    a check-digit-valid NIT, so the two corroborate). The razón social is cut
    where it starts listing the other names the company may use; the full
    text is kept in a ``transformation`` annotation and the sigla becomes an
    alternate name.

    ``foundingDate`` is the earliest matrícula date across the NIT's rows —
    the day a chamber first registered the company, which for a company is
    its registration after constitution; a ``commenting`` annotation says so.

    The legal representative is carried by **name and role only**
    (``seniorManagingOfficial``, no ``beneficialOwnershipOrControl``) — RUES
    publishes their identity number beside the name and the adapter never
    reads it. A representative identified by a NIT is a company and is
    mapped as an entity, with no identifier (RUES gives no check digit).
    """
    from ...sources.rues_colombia import (  # local import avoids a cycle
        CO_RUE_SCHEME,
        CO_RUE_SCHEME_NAME,
        LIVE_STATUSES,
        TERMINAL_STATUSES,
        record_url,
        summarise,
    )

    if not bundle or bundle.get("is_stub") or not bundle.get("rows"):
        return
    summary = summarise(bundle)
    nit = summary["nit"]
    name = summary["name"]
    if not nit or not name:
        return
    source_url = record_url(nit)

    alternates = [summary["sigla"]] if summary.get("sigla") else []
    subject = make_entity_statement(
        source_id="rues_colombia",
        local_id=nit,
        statement_date=summary.get("updated_on"),
        name=name,
        jurisdiction=("Colombia", "CO"),
        identifiers=[
            {"id": summary["nit_display"], "scheme": CO_RUE_SCHEME, "schemeName": CO_RUE_SCHEME_NAME}
        ],
        founding_date=summary.get("first_registered_on"),
        alternate_names=alternates,
        entity_type="registeredEntity",
        entity_details=_rues_entity_details(summary),
        source_url=source_url,
    )
    if summary.get("alias_clause"):
        annotate(
            subject,
            transformation(
                pointer("recordDetails", "name"),
                "RUES records the name as “"
                + summary["razon_social"]
                + "”; OpenCheck shows the part before the other names the "
                "company may use.",
            ),
        )
    if summary.get("first_registered_on"):
        annotate(
            subject,
            commenting(
                pointer("recordDetails", "foundingDate"),
                "foundingDate is the earliest date a Colombian chamber of "
                "commerce registered the company (fecha de matrícula), which "
                "can be later than its constitution.",
            ),
        )
    status = summary.get("status") or ""
    klass = _liveness.classify(status, live=LIVE_STATUSES, terminal=TERMINAL_STATUSES)
    _liveness.apply_register_status(
        subject,
        source_label=SOURCE_NAMES["rues_colombia"],
        liveness=klass,
        raw=status or None,
        since=summary.get("cancelled_on") if klass == _liveness.TERMINAL else None,
    )
    yield subject
    subject_id: str = subject["recordId"]

    representative = summary.get("representative")
    if not representative:
        return
    from ... import names as _names_mod

    local_id = f"{nit}:rl:{_names_mod.display_name_key(representative) or representative}"
    if summary.get("representative_is_entity"):
        party = make_entity_statement(
            source_id="rues_colombia",
            local_id=local_id,
            statement_date=summary.get("updated_on"),
            name=representative,
            entity_type="registeredEntity",
            source_url=source_url,
        )
        party_type = "entity"
        party_id = _stable_id("rues_colombia", "entity", local_id)
    else:
        party = make_person_statement(
            source_id="rues_colombia",
            local_id=local_id,
            statement_date=summary.get("updated_on"),
            full_name=representative,
            source_url=source_url,
        )
        party_type = "person"
        party_id = _stable_id("rues_colombia", "person", local_id)
    yield party
    interest = set_beneficial_ownership(
        {
            "type": "seniorManagingOfficial",
            "directOrIndirect": "direct",
            "details": _rues_role_details(summary),
        },
        "rues_colombia",
    )
    yield make_relationship_statement(
        source_id="rues_colombia",
        local_id=local_id,
        statement_date=summary.get("updated_on"),
        subject_statement_id=subject_id,
        interested_party_statement_id=party_id,
        interested_party_type=party_type,
        interests=[interest],
        source_url=source_url,
    )
