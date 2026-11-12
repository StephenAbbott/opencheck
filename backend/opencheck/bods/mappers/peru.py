"""Peru — OECE Registro Nacional de Proveedores (conformación jurídica) → BODS.

``mapper.py`` re-exports every name here.

The RNP file is each supplier's self-declaration to a procurement regulator
of who its partners or shareholders, legal representatives and governing-body
members are. It is not a company register and not a beneficial-ownership
register, and every statement says so in a ``commenting`` annotation. Rules
(Stephen's decisions on the Notion ticket, 16 Sept 2026):

* ``SOCIO_ACCIONISTA_TITULAR`` → ``shareholding`` with **no** ``share`` (the
  file publishes none) where the supplier's legal form has shares or
  participations; anywhere else (an association, a cooperative, a foreign
  company's branch) → ``otherInfluenceOrControl`` with the register's words.
  An E.I.R.L.'s titular owns the whole business by law, and the details say
  so — the figure is still not written as a ``share``, because the file does
  not state it.
* ``REPRESENTANTE_LEGAL`` → ``seniorManagingOfficial``.
* ``ORGANO_ADMINISTRACION`` → ``boardMember``.
* ``beneficialOwnershipOrControl`` is never true.
* A person is one node per supplier (the folded name within the filing); a
  company member carries ``PE-RUC`` when the file gives a company RUC; an
  entry naming a class of people ("OTROS ACCIONISTAS") is an unspecified
  party; an estate or marital partnership is an ``arrangement``.
* ``FECHA_INICIO_VIGENCIA`` is when the entry took effect in the RNP, not
  when the holding or the appointment began: said in the details, never a
  ``startDate``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from ..annotations import annotate, commenting
from ..statements import (
    content_addressed,
    dated_by_cut,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)

_PE_SOURCE = "oece_rnp"

#: The note every statement carries.
PE_SELF_DECLARATION_NOTE = (
    "From the Registro Nacional de Proveedores kept by OECE (Peru): the "
    "supplier's own declaration of its legal make-up, made to register as a "
    "supplier to the Peruvian state. It is not a company register or a "
    "beneficial-ownership register, publishes no percentages, and the people "
    "it names are members, representatives or board members, not beneficial "
    "owners. OpenCheck removes every person's document number."
)

_PE_UNSPECIFIED_REASON = "informationUnknownToPublisher"


def _pe_since_clause(since: str | None) -> str:
    return f"; entry in effect in the RNP since {since}" if since else ""


def _pe_interest(role: str, form: str | None, since: str | None) -> dict[str, Any]:
    """The interest for one declared entry."""
    from ...sources.oece_rnp import EIRL_FORM, SHARE_FORMS

    form_text = (form or "").strip()
    if role == "S":
        if form_text == EIRL_FORM:
            details = (
                "Titular of an individual limited liability enterprise "
                "(SOCIO_ACCIONISTA_TITULAR), as declared to the RNP; by law "
                "(Decreto Ley 21621) the titular holds the whole enterprise"
            )
            kind = "shareholding"
        elif form_text in SHARE_FORMS:
            details = (
                "Partner or shareholder (SOCIO_ACCIONISTA_TITULAR), as declared "
                "to the RNP; no percentage is published"
            )
            kind = "shareholding"
        else:
            details = (
                "Declared as partner, shareholder or owner "
                f"(SOCIO_ACCIONISTA_TITULAR) of a {form_text.lower() or 'supplier'} "
                "to the RNP; the form has no shares"
            )
            kind = "otherInfluenceOrControl"
    elif role == "R":
        details = "Legal representative (REPRESENTANTE_LEGAL), as declared to the RNP"
        kind = "seniorManagingOfficial"
    else:
        details = "Member of the governing body (ORGANO_ADMINISTRACION), as declared to the RNP"
        kind = "boardMember"
    return {
        "type": kind,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": False,
        "details": details + _pe_since_clause(since),
    }


def _pe_note(statement: dict[str, Any]) -> dict[str, Any]:
    return annotate(statement, commenting("/recordDetails", PE_SELF_DECLARATION_NOTE))


def _map_oece_rnp(bundle: dict[str, Any]) -> Iterator[dict[str, Any]]:
    from ...sources.oece_rnp import (
        DATASET_URL,
        PE_RUC_SCHEME,
        PE_RUC_SCHEME_NAME,
        fold,
        form_label,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    ruc = (bundle.get("ruc") or "").strip()
    name = (bundle.get("legal_name") or bundle.get("name") or "").strip()
    # The file names no supplier: without GLEIF's (or the filing's) name
    # there is nothing honest to call the company, so nothing is drawn.
    if not ruc or not name:
        return
    source_url = bundle.get("link") or DATASET_URL
    form = (bundle.get("form") or "").strip()
    gloss = form_label(form)
    entity_details = f"{gloss[0].upper()}{gloss[1:]} ({form})" if gloss and gloss != form.capitalize() else (form or None)

    def ruc_identifier(value: str) -> dict[str, str]:
        return {"id": value, "scheme": PE_RUC_SCHEME, "schemeName": PE_RUC_SCHEME_NAME}

    company = make_entity_statement(
        source_id=_PE_SOURCE,
        local_id=ruc,
        name=name,
        jurisdiction=("Peru", "PE"),
        identifiers=[ruc_identifier(ruc)],
        entity_details=entity_details,
        source_url=source_url,
    )
    yield _pe_note(company)
    company_id = company["recordId"]

    seen: dict[str, tuple[str, str]] = {}
    for party in bundle.get("parties") or []:
        role = party.get("role")
        kind = party.get("kind")
        party_name = " ".join(str(party.get("name") or "").split())
        seq = party.get("seq")
        if role not in {"S", "R", "O"} or not party_name:
            continue
        interest = _pe_interest(role, form, party.get("since"))
        if kind == "U":
            yield _pe_note(
                make_relationship_statement(
                    source_id=_PE_SOURCE,
                    local_id=f"{ruc}:rel:{seq}",
                    subject_statement_id=company_id,
                    interested_party_unspecified={
                        "reason": _PE_UNSPECIFIED_REASON,
                        "description": party_name,
                    },
                    interests=[interest],
                    source_url=source_url,
                )
            )
            continue
        member_ruc = party.get("ruc") if kind == "E" else None
        key = f"ruc:{member_ruc}" if member_ruc else f"{kind}:{fold(party_name)}"
        if key not in seen:
            local = f"{ruc}:party:{key}"
            if kind == "P":
                statement = make_person_statement(
                    source_id=_PE_SOURCE,
                    local_id=local,
                    full_name=party_name,
                    source_url=source_url,
                )
                party_type = "person"
            else:
                statement = make_entity_statement(
                    source_id=_PE_SOURCE,
                    local_id=local,
                    name=party_name,
                    # The file gives no member's country; a RUC is Peruvian.
                    jurisdiction=("Peru", "PE") if member_ruc else None,
                    identifiers=[ruc_identifier(member_ruc)] if member_ruc else [],
                    entity_type="arrangement" if kind == "A" else "registeredEntity" if member_ruc else "legalEntity",
                    entity_details=(
                        "Estate or marital partnership, as declared to the RNP"
                        if kind == "A" else None
                    ),
                    source_url=source_url,
                )
                party_type = "entity"
            yield _pe_note(statement)
            seen[key] = (statement["recordId"], party_type)
        party_id, party_type = seen[key]
        yield _pe_note(
            make_relationship_statement(
                source_id=_PE_SOURCE,
                local_id=f"{ruc}:rel:{seq}",
                subject_statement_id=company_id,
                interested_party_statement_id=party_id,
                interested_party_type=party_type,
                interests=[interest],
                source_url=source_url,
            )
        )


@content_addressed
def map_oece_rnp(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an OeceRnpAdapter fetch bundle to BODS v0.4 statements, every one
    dated by the file's cut. A note card and a stub yield nothing."""
    return dated_by_cut(_map_oece_rnp(bundle), bundle.get("cut") if bundle else None)
