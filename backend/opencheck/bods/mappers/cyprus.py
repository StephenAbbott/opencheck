"""Cyprus — DRCOR (data.gov.cy) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ---------------------------------------------------------------------------
# Cyprus DRCOR (data.gov.cy open data)
# ---------------------------------------------------------------------------
#
# DRCOR's open dataset carries company + role-holder data but no
# shareholders, so this mapper produces:
#   * one entityStatement for the organisation,
#   * one personStatement / entityStatement per official, and
#   * one relationshipStatement (seniorManagingOfficial) per official.


# Tokens that mark an "official" row as a corporate body rather than a person
# (Cyprus secretaries are frequently companies). Greek "ΛΤΔ" / "ΛΙΜΙΤΕΔ" and
# the Latin equivalents.
_CY_ORG_OFFICIAL_TOKENS = (
    "LTD", "LIMITED", "PLC", "ΛΤΔ", "ΛΙΜΙΤΕΔ", "SECRETARIAL", "SERVICES LIMITED",
)


def _cy_official_is_org(name: str) -> bool:
    up = (name or "").upper()
    return any(tok in up for tok in _CY_ORG_OFFICIAL_TOKENS)


def map_cyprus_drcor(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CyprusDrcorAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Cypriot organisation.
    * One person/entity statement per official (corporate officials such as
      secretarial companies are emitted as entityStatements).
    * One relationshipStatement (seniorManagingOfficial) per official.
    """
    # Local import to avoid a circular import at module load time.
    from ...sources.cyprus_drcor import _field as _cy_field

    if not bundle or bundle.get("is_stub"):
        return

    reg_no = (bundle.get("reg_no") or "").strip()
    organisation = bundle.get("organisation") or {}
    officials = bundle.get("officials") or []
    if not reg_no or not organisation:
        return

    name = _cy_field(organisation, "org_name") or bundle.get("name") or f"CY {reg_no}"
    source_url = bundle.get("link") or None

    # ── Identifiers ───────────────────────────────────────────────────────
    type_code = _cy_field(organisation, "org_type_code") or "HE"
    display_reg = f"{type_code}{reg_no}" if type_code else reg_no
    identifiers: list[dict[str, str]] = [
        {
            "id": display_reg,
            "scheme": "CY-DRCOR",
            "schemeName": (
                "Cyprus registration number — Department of Registrar of "
                "Companies and Intellectual Property"
            ),
        }
    ]

    # ── Founding date + address ───────────────────────────────────────────
    reg_date = _cy_field(organisation, "reg_date")
    founding_date: str | None = reg_date if re.match(r"\d{4}-\d{2}-\d{2}", reg_date) else None

    addresses: list[dict[str, Any]] = []
    addr = bundle.get("address") or {}
    addr_parts = [
        p for p in (
            _cy_field(addr, "street"),
            _cy_field(addr, "building"),
            _cy_field(addr, "territory"),
        ) if p
    ]
    if addr_parts:
        addresses.append(_addr("registered", ", ".join(addr_parts), "CY"))

    # ── 1. Entity statement ───────────────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="cyprus_drcor",
        local_id=reg_no,
        name=name,
        jurisdiction=("Cyprus", "CY"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
        # The register's own organisation-type wording ("Limited Company")
        # is a local name, so it goes to entityType.details. entityType.subtype
        # is a closed codelist in BODS 0.4 and never takes free text (Phase 214).
        entity_details=_cy_field(organisation, "org_type") or None,
    )
    # DRCOR organisation status (Phase 151).
    cy_status = (_cy_field(organisation, "org_status") or "").strip()
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["cyprus_drcor"],
        liveness=_liveness.classify(
            cy_status,
            live=("Active", "Εγγεγραμμένη"),
            pending=("Under Liquidation", "In Liquidation", "Under Strike Off", "Strike Off Pending"),
            terminal=("Dissolved", "Struck Off", "Deleted", "Liquidated"),
        ),
        raw=cy_status or None,
    )
    yield company_stmt
    company_stmt_id = company_stmt["statementId"]

    # ── 2. Officials ──────────────────────────────────────────────────────
    seen: set[str] = set()
    for idx, official in enumerate(officials):
        full_name = _cy_field(official, "official_name")
        if not full_name:
            continue
        position = _cy_field(official, "official_position") or "Official"
        is_org = _cy_official_is_org(full_name)
        party_local = f"{reg_no}:official:{idx}:{full_name.lower()}"

        if party_local not in seen:
            if is_org:
                party_stmt = make_entity_statement(
                    source_id="cyprus_drcor",
                    local_id=party_local,
                    name=full_name,
                    jurisdiction=("Cyprus", "CY"),
                    source_url=source_url,
                )
            else:
                party_stmt = make_person_statement(
                    source_id="cyprus_drcor",
                    local_id=party_local,
                    full_name=full_name,
                    source_url=source_url,
                )
            yield party_stmt
            seen.add(party_local)
            party_stmt_id = party_stmt["statementId"]
        else:
            kind = "entity" if is_org else "person"
            party_stmt_id = _stable_id("cyprus_drcor", kind, party_local)

        interests: list[dict[str, Any]] = [
            {
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": position,
            }
        ]
        yield make_relationship_statement(
            source_id="cyprus_drcor",
            local_id=f"{reg_no}:official:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=party_stmt_id,
            interested_party_type="entity" if is_org else "person",
            interests=interests,
            source_url=source_url,
        )
