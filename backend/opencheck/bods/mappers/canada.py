"""Canada — Corporations Canada (ISED federal register) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from .. import liveness as _liveness
from ..oor import mark_role
from ..statements import (
    content_addressed,
    SOURCE_NAMES,
    _addr,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# Corporations Canada — ISED federal register
# ----------------------------------------------------------------------


def _cc_corp_url(corp_id: str) -> str:
    return (
        f"https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html"
        f"?corpId={corp_id}&V_TOKEN=null&LANGUAGE_ID=1"
    )


def _cc_current_name(corp: dict[str, Any]) -> str:
    """Return the current primary corporation name."""
    names = corp.get("corporationNames") or []
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current") and (cn.get("nameType") or "").lower() == "primary":
            return (cn.get("name") or "").strip()
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current"):
            return (cn.get("name") or "").strip()
    if names:
        cn = (names[-1].get("CorporationName") or {})
        return (cn.get("name") or "").strip()
    return ""


def _cc_index_address(row: dict[str, Any]) -> list[dict[str, Any]]:
    """The registered office as the open data writes it."""
    parts = [
        str(row.get(k) or "").strip()
        for k in ("street", "street2", "city", "province", "postal_code")
    ]
    text = ", ".join(p for p in parts if p)
    if not text:
        return []
    return [_addr("registered", text, (row.get("country") or "CA").strip() or "CA")]


def _cc_api_names(corp: dict[str, Any], current: str) -> list[str]:
    """Names the API lists that are not the current one (former names, and
    the other-language form where it lists one)."""
    names: list[str] = []
    for entry in corp.get("corporationNames") or []:
        name = ((entry.get("CorporationName") or {}).get("name") or "").strip()
        if name and name != current and name not in names:
            names.append(name)
    return names


#: The federal Act a corporation is governed by → ISO 20275 ELF, for the Acts
#: ISO 5009 lists a director under (its legislation column names each).
#: Keyed on the Act's name as the open-data file writes it, lowercased, before
#: any " - Part …".
CA_ACT_ELF: dict[str, str] = {
    "canada business corporations act": "UDLA",
    "canada not-for-profit corporations act": "D90J",
    "canada cooperatives act": "MGE0",
    "pension fund societies act": "M4IF",
}

#: ISO 5009 codes for a director, by (ELF, role). Reviewed against GLEIF's
#: list 1.0.0.
CA_OOR: dict[tuple[str, str], str] = {
    ("UDLA", "director"): "WYPWKM",
    ("D90J", "director"): "BATZJU",
    ("MGE0", "director"): "RQST1K",
    ("M4IF", "director"): "3EA57L",
}


def ca_act_elf(act_name: str) -> str | None:
    """The ELF for a federal corporation's governing Act, or None."""
    key = re.split(r"\s+-\s+", (act_name or "").strip(), maxsplit=1)[0]
    key = re.sub(r"\s+", " ", key).lower()
    return CA_ACT_ELF.get(key)


@content_addressed
def map_corporations_canada(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CorporationsCanadaAdapter fetch bundle to BODS v0.4 statements.

    The bundle carries the daily open-data index row (``index``), the ISED
    API record (``corporation``) and the current directors; either of the
    first two may be missing (the Canada index ticket, 9 Nov 2026). The API
    is preferred for what it dates (activities, the current name), the index
    for what it words more finely (the Status Detail) and for everything when
    the API did not answer.

    Yields:
    * One entityStatement for the Canadian federal corporation.
    * One personStatement per current director.
    * One relationshipStatement (seniorManagingOfficial) per director.
    """
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return

    from ...sources.corporations_canada import display_status, status_class, status_words

    corp_id: str = bundle.get("corp_id") or ""
    corp: dict[str, Any] = bundle.get("corporation") or {}
    row: dict[str, Any] = bundle.get("index") or {}
    directors: list[dict[str, Any]] = bundle.get("directors") or []

    name: str = (
        _cc_current_name(corp) or (row.get("name") or "").strip()
        or bundle.get("legal_name") or corp_id
    )
    if not corp_id or not name:
        return

    source_url = _cc_corp_url(corp_id)

    # ── Founding and dissolution dates from activities ────────────────────
    founding_date: str | None = None
    dissolution_date: str | None = None
    for act_entry in corp.get("activities") or []:
        act = act_entry.get("activity") or {}
        act_type = (act.get("activity") or "").lower()
        # "Continuance (Act)" is a continuance; "Discontinuance" is not.
        if act_type.split(" (")[0] in ("incorporation", "continuance", "amalgamation") and not founding_date:
            founding_date = act.get("date") or None
        # The same activity log records the end of the corporation (Phase 151).
        if any(k in act_type for k in ("dissolution", "dissolved", "discontinuance", "amalgamated into")):
            dissolution_date = act.get("date") or dissolution_date
    # The open data's anniversary date is the incorporation (or continuance,
    # amalgamation) date — equal to the API's on every record checked.
    founding_date = founding_date or row.get("anniversary") or None

    # ── Address: the API's (documented typo ``adresses``), else the index's ─
    addresses: list[dict[str, Any]] = []
    for addr_entry in corp.get("adresses") or []:
        addr = addr_entry.get("address") or {}
        lines = addr.get("addressLine") or []
        city = (addr.get("city") or "").strip()
        province = (addr.get("provinceCode") or "").strip()
        postal = (addr.get("postalCode") or "").strip()
        country = (addr.get("countryCode") or "CA").strip()
        parts = [ln.strip() for ln in lines if ln and ln.strip()]
        parts += [p for p in (city, province, postal) if p]
        if parts:
            addresses.append(_addr("registered", ", ".join(parts), country))
        break  # use only the first address entry
    if not addresses and row:
        addresses = _cc_index_address(row)

    # ── Identifiers ───────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = [
        {
            "id": corp_id,
            "scheme": "CA-CC",
            "schemeName": "Canada Corporation Number (Corporations Canada)",
        }
    ]
    bn = ""
    bn_block = corp.get("businessNumbers") or {}
    if isinstance(bn_block, dict):
        bn = (bn_block.get("businessNumber") or "").strip()
    bn = bn or (row.get("bn") or "").strip()
    if bn:
        identifiers.append({
            "id": bn,
            "scheme": "CA-BN",
            "schemeName": "Canada Revenue Agency Business Number",
        })

    # The other-language form of the name (the open data's "form 2") and any
    # name the API lists beside the current one.
    alternate_names = _cc_api_names(corp, name)
    other = (row.get("name_other") or "").strip()
    if other and other != name and other not in alternate_names:
        alternate_names.append(other)
    row_name = (row.get("name") or "").strip()
    if row_name and row_name != name and row_name not in alternate_names:
        alternate_names.append(row_name)

    # ── 1. Entity statement ───────────────────────────────────────────────
    act_name = (corp.get("act") or row.get("act") or "").strip()
    company_stmt = make_entity_statement(
        source_id="corporations_canada",
        local_id=corp_id,
        name=name,
        jurisdiction=("Canada", "CA"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        alternate_names=alternate_names,
        entity_details=f"Governed by the {act_name}" if act_name else None,
        source_url=source_url,
    )
    # Status: the index's Status Detail where it has one ("Dissolved for
    # non-compliance", "Active - Dissolution Pending (non-compliance)"), the
    # API's status otherwise; dated by the API's dissolution activity.
    if row:
        raw_status = status_words(row.get("status"), row.get("detail"))
        cc_liveness = status_class(row.get("status"), row.get("detail"))
    else:
        raw_status = (corp.get("status") or "").strip()
        cc_liveness = status_class(raw_status)
    if cc_liveness == _liveness.UNKNOWN and dissolution_date:
        cc_liveness = _liveness.TERMINAL
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["corporations_canada"],
        liveness=cc_liveness,
        raw=display_status(raw_status) or None,
        since=dissolution_date if cc_liveness == _liveness.TERMINAL else None,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["recordId"]

    # ── 2. Director statements ────────────────────────────────────────────
    seen_person_ids: set[str] = set()
    company_elf = ca_act_elf(act_name)

    for idx, director in enumerate(directors):
        first = (director.get("firstName") or "").strip()
        last = (director.get("lastName") or "").strip()
        full_name = " ".join(p for p in [first, last] if p)
        if not full_name:
            continue

        person_local_id = f"{corp_id}:director:{idx}:{full_name.lower()}"

        if person_local_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="corporations_canada",
                local_id=person_local_id,
                full_name=full_name,
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_local_id)
        else:
            person_stmt = {"recordId": _stable_id("corporations_canada", "person", person_local_id)}

        interests: list[dict[str, Any]] = [
            mark_role({
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": "Director",
            }, CA_OOR, company_elf, "director")
        ]

        yield make_relationship_statement(
            source_id="corporations_canada",
            local_id=f"{corp_id}:director:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["recordId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )
