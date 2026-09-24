"""Romania — ANAF (live) and ONRC (bulk) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from dataclasses import field
from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _country_obj,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ---------------------------------------------------------------------------
# Romania — ANAF (live) and ONRC (bulk) → BODS
#
# Two registers, two mappers, deliberately not merged. ANAF is the tax
# authority and ONRC the trade register; they answer about the same company
# from different records, under different terms (ONRC CC BY 4.0, ANAF no
# stated licence), and OpenCheck's reconciler treats agreement between two
# sources as corroboration. Folding them into one bundle would turn two
# observations into one and lose the attribution split.
#
# Neither register publishes beneficial ownership. Shareholders (asociați) are
# behind ONRC's paid certificat constatator, so nothing below is a BO claim:
# the people are legal representatives — officers — and their interests are
# typed accordingly.
# ---------------------------------------------------------------------------

#: Romania's identifier schemes. ``RO-ONRC`` is org-id's code for the trade
#: register's ``COD_INMATRICULARE``; the fiscal code (CUI) is a different
#: number issued by a different authority and gets its own scheme rather than
#: being flattened onto the register's.
RO_ONRC_SCHEME = "RO-ONRC"
RO_ONRC_SCHEME_NAME = (
    "Registrul Comerțului — National Trade Register Office (Romania)"
)
RO_CUI_SCHEME = "RO-CUI"
RO_CUI_SCHEME_NAME = (
    "Cod Unic de Înregistrare — fiscal code, Agenția Națională de "
    "Administrare Fiscală (Romania)"
)


#: ONRC ``CALITATE`` role slug → BODS v0.4 ``interestType``.
#:
#: Checked against ``libcovebods/data/schema-0-4-0/relationship-record.json``
#: rather than a summary — that file is the enum, and an earlier phase shipped
#: a value that was not in it because a reference doc disagreed with the
#: schema.
#:
#: Three groups, and the split that matters is the third:
#:
#: * **Management appointed by the company** → ``seniorManagingOfficial``.
#:   The Romanian *administrator* is the managing officer of an SRL or SA;
#:   this is the CLAUDE.md rule that directors are seniorManagingOfficial and
#:   never ``appointmentOfBoard``, which is a right to appoint, not a seat.
#: * **Seats on a two-tier board** → ``boardMember``. Romanian SAs may adopt a
#:   dualist system with a *directorat* (management board) supervised by a
#:   *consiliu de supraveghere*; those two roles are seats, not the executive.
#: * **Insolvency office-holders** → ``controlByLegalFramework``. A
#:   *lichidator judiciar* or *administrator judiciar* is appointed by the
#:   court under Law 85/2014 and their powers come from the insolvency statute,
#:   not from the company's articles or its shareholders. Typing them as
#:   senior managing officials would say the company chose them.
#:
#:   ``administrator special`` is the exception inside that group and is
#:   deliberately **not** ``controlByLegalFramework``: under Law 85/2014 the
#:   special administrator is elected by the *shareholders* to represent their
#:   interests during the procedure, so it is a company appointment like any
#:   other.
RO_ROLE_INTEREST: dict[str, str] = {
    # Company-appointed management.
    "administrator": "seniorManagingOfficial",
    "administrator_representative": "seniorManagingOfficial",
    "entity_representative": "seniorManagingOfficial",
    "entity_administrator_representative": "seniorManagingOfficial",
    "sole_director_general": "seniorManagingOfficial",
    "legal_representative": "seniorManagingOfficial",
    "administrator_manager": "seniorManagingOfficial",
    "interim_administrator": "seniorManagingOfficial",
    "reinstated_administrator": "seniorManagingOfficial",
    "special_administrator": "seniorManagingOfficial",
    "agent": "seniorManagingOfficial",
    # Two-tier board seats.
    "supervisory_board_member": "boardMember",
    "management_board_member": "boardMember",
    # Court-appointed insolvency office-holders.
    "liquidator": "controlByLegalFramework",
    "interim_liquidator": "controlByLegalFramework",
    "judicial_liquidator": "controlByLegalFramework",
    "interim_judicial_liquidator": "controlByLegalFramework",
    "judicial_administrator": "controlByLegalFramework",
    "interim_judicial_administrator": "controlByLegalFramework",
    "composition_administrator": "controlByLegalFramework",
}

#: ONRC status codes that mean the company is gone, or on its way. Read off
#: ``N_STARE_FIRMA.CSV`` (197 codes) on the 2 September 2026 export.
_RO_TERMINAL_STATUS = {"1084"}
_RO_PENDING_STATUS = {
    "1049",  # dizolvare
    "1052",  # lichidare
    "1070",  # faliment
    "1107",  # insolvență
    "1109",  # dizolvare de drept
    "1113",  # dizolvare judiciară
}


def _ro_liveness(status_code: str | None, status: str | None) -> str:
    """Classify an ONRC status code. Unknown codes stay ``unknown``."""
    code = (status_code or "").strip()
    if code in _RO_TERMINAL_STATUS:
        return _liveness.TERMINAL
    if code in _RO_PENDING_STATUS:
        return _liveness.PENDING
    if code == "1048":  # funcțiune
        return _liveness.LIVE
    return _liveness.UNKNOWN


def _ro_address(row: dict[str, Any]) -> dict[str, str] | None:
    parts = [
        (row.get("address") or "").strip(),
        (row.get("locality") or "").strip(),
        (row.get("county") or "").strip(),
        (row.get("postal_code") or "").strip(),
    ]
    text = ", ".join(p for p in parts if p)
    if not text:
        return None
    address: dict[str, str] = {"type": "registered", "address": text}
    country = _country_obj((row.get("country") or "").strip())
    if country and country.get("code"):
        address["country"] = country["code"]
    return address


def map_onrc_romania(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an OnrcRomaniaAdapter bundle to BODS v0.4 statements.

    Yields the company, then — for every legal representative the register
    files — a person or entity statement and the relationship that gives them
    their role.

    Two shapes are worth knowing before reading the output:

    * **A representative can be a legal entity.** 265,799 of 3,689,931 rows
      name an insolvency-practitioner firm or a corporate administrator, so
      the interested party is an entity statement, not a person.
    * **Corporate directorship is filed as two rows**, not one: the entity as
      ``administrator`` and a natural person beside it as *reprezentant al
      persoanei juridice*. Both are emitted as they are filed. OpenCheck does
      not collapse them into "the person controls the company through the
      firm" — the register does not say the two rows are connected, and
      inferring the link would be OpenCheck's claim, not ONRC's.

    Dates of birth are published at the precision ONRC publishes them, which
    is a full date on 87.2% of rows.
    """
    # ``not_found`` as well as ``is_stub``: a miss carries is_stub False so its
    # note card survives the pipeline's blanket stub drop, and it has no
    # company row to map.
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    number = (bundle.get("registration_number") or "").strip().upper()
    name = (company.get("name") or bundle.get("name") or "").strip()
    if not number or not name:
        return

    identifiers: list[dict[str, str]] = [
        {"id": number, "scheme": RO_ONRC_SCHEME, "schemeName": RO_ONRC_SCHEME_NAME}
    ]
    cui = (bundle.get("cui") or company.get("cui") or "").strip()
    if cui:
        identifiers.append(
            {"id": cui, "scheme": RO_CUI_SCHEME, "schemeName": RO_CUI_SCHEME_NAME}
        )

    address = _ro_address(company)
    entity = make_entity_statement(
        source_id="onrc_romania",
        local_id=number,
        name=name,
        jurisdiction=("Romania", "RO"),
        identifiers=identifiers,
        founding_date=(company.get("registered_on") or None),
        addresses=[address] if address else [],
        entity_type="registeredEntity",
        entity_details=(company.get("legal_form") or None),
        source_url=bundle.get("link"),
    )
    status = (company.get("status") or "").strip()
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["onrc_romania"],
        liveness=_ro_liveness(company.get("status_code"), status),
        raw=status or None,
    )
    yield entity

    for index, rep in enumerate(bundle.get("representatives") or []):
        rep_name = (rep.get("name") or "").strip()
        if not rep_name:
            continue
        slug = (rep.get("role_slug") or "").strip()
        role_text = (rep.get("role") or "").strip()
        # An unmapped slug becomes unknownInterest rather than defaulting to a
        # management role: the register's vocabulary is closed and covered, so
        # an unknown value means ONRC changed something.
        interest_type = RO_ROLE_INTEREST.get(slug, "unknownInterest")

        party_local = f"{number}:{index}:{rep_name}"
        if rep.get("is_entity"):
            party = make_entity_statement(
                source_id="onrc_romania",
                local_id=party_local,
                name=rep_name,
                jurisdiction=("Romania", "RO"),
                entity_type="registeredEntity",
                source_url=bundle.get("link"),
            )
            party_type = "entity"
        else:
            party = make_person_statement(
                source_id="onrc_romania",
                local_id=party_local,
                full_name=rep_name,
                birth_date=(rep.get("birth_date") or None),
                source_url=bundle.get("link"),
            )
            party_type = "person"
        yield party

        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if role_text:
            # The register's own word for the role, verbatim. Romanian
            # insolvency law distinguishes a lichidator from a lichidator
            # judiciar and the BODS type does not, so the distinction lives
            # here rather than being translated away.
            interest["details"] = role_text
        yield make_relationship_statement(
            source_id="onrc_romania",
            local_id=f"{number}:rep:{index}",
            subject_statement_id=entity["statementId"],
            interested_party_statement_id=party["statementId"],
            interested_party_type=party_type,
            interests=[interest],
            source_url=bundle.get("link"),
        )


def map_anaf_romania(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AnafRomaniaAdapter bundle to one BODS v0.4 entity statement.

    ANAF publishes no people — it is a taxpayer register — so this mapper
    emits the company and nothing else.

    Two ANAF fields change what OpenCheck can say about whether a company
    still exists, and both are used: ``stare_inactiv.dataRadiere`` is the
    striking-off date and becomes a ``dissolutionDate``; ``statusInactivi``
    marks a taxpayer declared inactive, which is not dissolution and is
    therefore ``pending``, never ``terminal``.

    The trade-register number ANAF returns is emitted as an identifier under
    the ONRC scheme, because that is whose number it is — ANAF is repeating
    ONRC's, not asserting one of its own. That is also what lets the
    reconciler corroborate an ANAF record against an ONRC one.
    """
    if not bundle or bundle.get("is_stub"):
        return
    if bundle.get("not_found") or bundle.get("coverage_note"):
        return

    record: dict[str, Any] = bundle.get("record") or {}
    general: dict[str, Any] = record.get("date_generale") or {}
    cui = str(bundle.get("cui") or general.get("cui") or "").strip()
    name = (general.get("denumire") or bundle.get("legal_name") or "").strip()
    if not cui or not name:
        return

    identifiers: list[dict[str, str]] = [
        {"id": cui, "scheme": RO_CUI_SCHEME, "schemeName": RO_CUI_SCHEME_NAME}
    ]
    reg_number = (
        (bundle.get("registration_number") or general.get("nrRegCom") or "")
        .strip()
        .upper()
    )
    if reg_number:
        identifiers.append(
            {
                "id": reg_number,
                "scheme": RO_ONRC_SCHEME,
                "schemeName": RO_ONRC_SCHEME_NAME,
            }
        )

    inactive: dict[str, Any] = record.get("stare_inactiv") or {}
    struck_off = _anaf_date(inactive.get("dataRadiere"))

    addresses: list[dict[str, str]] = []
    seat = _anaf_address(record.get("adresa_sediu_social") or {}, "s")
    if seat:
        addresses.append({"type": "registered", "address": seat, "country": "RO"})
    fiscal = _anaf_address(record.get("adresa_domiciliu_fiscal") or {}, "d")
    if fiscal and fiscal != seat:
        addresses.append({"type": "business", "address": fiscal, "country": "RO"})

    stmt = make_entity_statement(
        source_id="anaf_romania",
        local_id=cui,
        name=name,
        jurisdiction=("Romania", "RO"),
        identifiers=identifiers,
        founding_date=_anaf_date(general.get("data_inregistrare")),
        dissolution_date=struck_off,
        addresses=addresses,
        entity_type="registeredEntity",
        entity_details=(general.get("forma_juridica") or "").strip() or None,
        source_url=bundle.get("link"),
    )

    raw_status = (general.get("stare_inregistrare") or "").strip()
    if struck_off:
        liveness_class = _liveness.TERMINAL
    elif inactive.get("statusInactivi"):
        liveness_class = _liveness.PENDING
    elif raw_status.upper().startswith("INREGISTRAT"):
        liveness_class = _liveness.LIVE
    else:
        liveness_class = _liveness.UNKNOWN
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["anaf_romania"],
        liveness=liveness_class,
        raw=raw_status or None,
        since=struck_off,
    )
    yield stmt


def _anaf_date(raw: Any) -> str | None:
    """ANAF writes ISO dates already; empty strings mean 'no date'."""
    text = str(raw or "").strip()
    return text if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) else None


def _anaf_address(block: dict[str, Any], prefix: str) -> str:
    """Join one ANAF address block. ``prefix`` is 's' (seat) or 'd' (fiscal).

    The two blocks carry the same concepts under different field prefixes,
    which is why this takes the prefix rather than guessing.
    """
    if not block:
        return ""
    def field(name: str) -> str:
        return str(block.get(f"{prefix}{name}") or "").strip()

    street = " ".join(p for p in (field("denumire_Strada"), field("numar_Strada")) if p)
    parts = [
        street,
        field("detalii_Adresa"),
        field("denumire_Localitate"),
        field("denumire_Judet"),
        field("cod_Postal"),
    ]
    return ", ".join(p for p in parts if p)
