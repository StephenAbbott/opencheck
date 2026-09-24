"""Canada — Corporations Canada (ISED federal register) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

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


def map_corporations_canada(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CorporationsCanadaAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Canadian federal corporation.
    * One personStatement per current director.
    * One relationshipStatement (seniorManagingOfficial) per director.
    """
    if not bundle or bundle.get("is_stub"):
        return

    corp_id: str = bundle.get("corp_id") or ""
    corp: dict[str, Any] = bundle.get("corporation") or {}
    directors: list[dict[str, Any]] = bundle.get("directors") or []

    name: str = _cc_current_name(corp) or bundle.get("legal_name") or corp_id
    if not corp_id or not name:
        return

    source_url = _cc_corp_url(corp_id)

    # ── Founding date from activities ─────────────────────────────────────
    founding_date: str | None = None
    dissolution_date: str | None = None
    for act_entry in corp.get("activities") or []:
        act = act_entry.get("activity") or {}
        act_type = (act.get("activity") or "").lower()
        if act_type in ("incorporation", "continuance", "amalgamation") and not founding_date:
            founding_date = act.get("date") or None
        # The same activity log records the end of the corporation (Phase 151).
        if any(k in act_type for k in ("dissolution", "dissolved", "discontinuance", "amalgamated into")):
            dissolution_date = act.get("date") or dissolution_date

    # ── Address from adresses (documented API typo) ───────────────────────
    addresses: list[dict[str, Any]] = []
    for addr_entry in corp.get("adresses") or []:
        addr = addr_entry.get("address") or {}
        lines = addr.get("addressLine") or []
        city = (addr.get("city") or "").strip()
        postal = (addr.get("postalCode") or "").strip()
        country = (addr.get("countryCode") or "CA").strip()
        parts = [ln.strip() for ln in lines if ln.strip()]
        if city:
            parts.append(city)
        if postal:
            parts.append(postal)
        if parts:
            addresses.append(_addr("registered", ", ".join(parts), country))
        break  # use only the first address entry

    # ── Identifiers ───────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = [
        {
            "id": corp_id,
            "scheme": "CA-CORP",
            "schemeName": "Corporations Canada — ISED federal register",
        }
    ]
    bn_block = corp.get("businessNumbers") or {}
    if isinstance(bn_block, dict):
        bn = (bn_block.get("businessNumber") or "").strip()
        if bn:
            identifiers.append({
                "id": bn,
                "scheme": "CA-BN",
                "schemeName": "Canada Revenue Agency Business Number",
            })

    # ── 1. Entity statement ───────────────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="corporations_canada",
        local_id=corp_id,
        name=name,
        jurisdiction=("Canada", "CA"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # Corporation ``status`` (Active / Dissolved / Amalgamated / Discontinued /
    # Inactive) plus any dissolution activity date (Phase 151).
    cc_status = (corp.get("status") or "").strip()
    cc_liveness = _liveness.classify(
        cc_status,
        live=("active",),
        pending=("dissolution pending", "pending dissolution", "in liquidation"),
        terminal=("dissolved", "amalgamated", "discontinued", "inactive", "revoked"),
    )
    if cc_liveness == _liveness.UNKNOWN and dissolution_date:
        cc_liveness = _liveness.TERMINAL
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["corporations_canada"],
        liveness=cc_liveness,
        raw=cc_status or None,
        since=dissolution_date,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. Director statements ────────────────────────────────────────────
    seen_person_ids: set[str] = set()

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
            person_stmt = {
                "statementId": _stable_id("corporations_canada", "person", person_local_id)
            }

        interests: list[dict[str, Any]] = [
            {
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": "Director",
            }
        ]

        yield make_relationship_statement(
            source_id="corporations_canada",
            local_id=f"{corp_id}:director:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )
