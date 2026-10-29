"""Cyprus — DRCOR (data.gov.cy) → BODS.

Split out of ``mapper.py`` in Phase 246; rewritten for the index built from
the register's own CSV columns when the source was wired into the lookup
(the Cyprus wiring, 29 Oct 2026). ``mapper.py`` re-exports every name here.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .. import liveness as _liveness
from ..officer_roles import mark_officer
from ..statements import (
    SOURCE_NAMES,
    _addr,
    content_addressed,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)

# ---------------------------------------------------------------------------
# Cyprus DRCOR (data.gov.cy open data)
# ---------------------------------------------------------------------------
#
# The register publishes organisations and their officials — no shareholders
# and no beneficial owners — so this mapper produces one entity statement for
# the organisation and, per official, a party statement and a relationship.
# An official is a name and one of nine positions: the register files no
# person/organisation flag, no country and no date, so a corporate official
# is told from its name (Cyprus secretaries are very often companies) and
# carries no jurisdiction (Phase 295: an assumed CY would corroborate a
# Cypriot name match it has no right to).

_CY_SCHEME = "CY-DRCOR"
_CY_SCHEME_NAME = (
    "Cyprus registration number — Department of Registrar of Companies and "
    "Intellectual Property"
)

#: Tokens that mark an official as a corporate body rather than a person —
#: Greek "ΛΤΔ" / "ΛΙΜΙΤΕΔ" and the Latin forms. Matched on whole words.
_CY_ORG_OFFICIAL_TOKENS = frozenset(
    {"LTD", "LIMITED", "PLC", "LLC", "INC", "CORP", "CORPORATION", "SA", "S.A.", "AG",
     "BV", "B.V.", "NV", "N.V.", "GMBH", "ΛΤΔ", "ΛΙΜΙΤΕΔ", "SECRETARIAL", "SECRETARIES",
     "NOMINEES", "TRUSTEES", "SERVICES", "HOLDINGS", "ΕΠΕ", "Α.Ε.", "ΑΕ"}
)

#: Position → BODS interest. A director or alternate director is a senior
#: managing official; a secretary, assistant / deputy secretary or an
#: authorised person (an overseas company's representative) has no v0.4 role
#: code and is ``otherInfluenceOrControl`` **marked as an officer** (the
#: Phase 340 ``opencheckOfficerPosition`` rule, so the canvas draws a role,
#: not control). A partner holds a partnership interest, not shares — the
#: CVR Denmark reading (``otherInfluenceOrControl``, not an officer). The
#: owner of a business name (``Ιδιοκτήτης``) is the person or company
#: trading under it: ``otherInfluenceOrControl`` too, said in the details,
#: because a business name has no capital to hold a share of.
_CY_POSITION_INTERESTS: dict[str, tuple[str, bool]] = {
    "Διευθυντής": ("seniorManagingOfficial", False),
    "Αντικαταστάτης Διευθυντής": ("seniorManagingOfficial", False),
    "Γραμματέας": ("otherInfluenceOrControl", True),
    "Βοηθός Γραμματέας": ("otherInfluenceOrControl", True),
    "Αναπληρωτής Γραμματέας": ("otherInfluenceOrControl", True),
    "Εξουσιοδοτημένο Πρόσωπο": ("otherInfluenceOrControl", True),
    "Ομόρρυθμος Συνέταιρος": ("otherInfluenceOrControl", False),
    "Ετερόρρυθμος Συνέταιρος": ("otherInfluenceOrControl", False),
    "Ιδιοκτήτης": ("otherInfluenceOrControl", False),
}


def _cy_official_is_org(name: str) -> bool:
    words = {w.strip(".,()") for w in (name or "").upper().replace("/", " ").split()}
    return bool(words & _CY_ORG_OFFICIAL_TOKENS)


def _cy_interest(position: str | None) -> dict[str, Any]:
    """The interest for one official, from the register's position."""
    from ...sources.cyprus_drcor import position_label

    text = (position or "").strip()
    kind, officer = _CY_POSITION_INTERESTS.get(text, ("otherInfluenceOrControl", True))
    label = position_label(text) or "Official"
    interest: dict[str, Any] = {
        "type": kind,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": False,
        "details": f"{label} ({text})" if text and label != text else label,
    }
    if officer:
        mark_officer(interest, label)
    return interest


@content_addressed
def map_cyprus_drcor(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CyprusDrcorAdapter fetch bundle to BODS v0.4 statements.

    Yields one entityStatement for the organisation, then per official a
    person or entity statement (corporate officials by name) and a
    relationshipStatement typed by the register's position. A note card
    (``not_found``) and a stub yield nothing.
    """
    from ...sources.cyprus_drcor import (
        TYPE_CODES,
        display_number,
        form_label,
        register_identifier,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return

    reg_no = (bundle.get("reg_no") or "").strip()
    type_code = (bundle.get("type_code") or "C").strip()
    if not reg_no or type_code not in TYPE_CODES:
        return

    name = bundle.get("name") or bundle.get("legal_name") or f"CY {display_number(type_code, reg_no)}"
    source_url = bundle.get("link") or None

    # Spelled as GLEIF files it under RA000161 ("ΗΕ 449149", Greek letters),
    # so the two statements meet on CY-DRCOR.
    identifiers = [
        {"id": register_identifier(type_code, reg_no), "scheme": _CY_SCHEME, "schemeName": _CY_SCHEME_NAME}
    ]

    addresses: list[dict[str, Any]] = []
    addr = bundle.get("address") or {}
    parts = [str(addr[k]).strip() for k in ("street", "building", "territory") if addr.get(k)]
    if parts:
        addresses.append(_addr("registered", ", ".join(parts), "CY"))

    # The register's own type and sub-type wording, glossed, as the details
    # of a closed-codelist entityType (Phase 214).
    org_type = (bundle.get("org_type") or "").strip()
    sub_type = (bundle.get("sub_type") or "").strip()
    gloss = form_label(type_code, sub_type)
    greek = " — ".join(p for p in (org_type, sub_type) if p)
    if gloss and greek:
        entity_details: str | None = f"{gloss[0].upper()}{gloss[1:]} ({greek})"
    else:
        entity_details = gloss or greek or None

    company = make_entity_statement(
        source_id="cyprus_drcor",
        local_id=f"{type_code}:{reg_no}",
        name=name,
        jurisdiction=("Cyprus", "CY"),
        identifiers=identifiers,
        founding_date=bundle.get("registered_on") or None,
        addresses=addresses,
        entity_details=entity_details,
        source_url=source_url,
    )
    status = (bundle.get("status") or "").strip()
    cls = (bundle.get("status_class") or "unknown").strip()
    _liveness.apply_register_status(
        company,
        source_label=SOURCE_NAMES["cyprus_drcor"],
        liveness=_liveness.classify(
            cls, live=("live",), pending=("pending",), terminal=("terminal",)
        ),
        raw=status or None,
        since=bundle.get("status_date") or None,
    )
    yield company
    company_id = company["recordId"]

    seen: dict[str, str] = {}
    for idx, official in enumerate(bundle.get("officials") or []):
        full_name = (official.get("name") or "").strip()
        if not full_name:
            continue
        seq = official.get("seq", idx)
        is_org = _cy_official_is_org(full_name)
        party_key = ("entity" if is_org else "person") + ":" + full_name.casefold()
        if party_key not in seen:
            party_local = f"{type_code}:{reg_no}:official:{seq}"
            if is_org:
                party = make_entity_statement(
                    source_id="cyprus_drcor",
                    local_id=party_local,
                    name=full_name,
                    # The register files no country for an official (Phase 295).
                    jurisdiction=None,
                    source_url=source_url,
                )
            else:
                party = make_person_statement(
                    source_id="cyprus_drcor",
                    local_id=party_local,
                    full_name=full_name,
                    source_url=source_url,
                )
            yield party
            seen[party_key] = party["recordId"]
        yield make_relationship_statement(
            source_id="cyprus_drcor",
            local_id=f"{type_code}:{reg_no}:official-rel:{seq}",
            subject_statement_id=company_id,
            interested_party_statement_id=seen[party_key],
            interested_party_type="entity" if is_org else "person",
            interests=[_cy_interest(official.get("position"))],
            source_url=source_url,
        )
