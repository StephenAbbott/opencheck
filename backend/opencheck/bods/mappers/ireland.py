"""Ireland — CRO (Companies Registration Office) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ----------------------------------------------------------------------
# CRO (Companies Registration Office Ireland) → BODS
# ----------------------------------------------------------------------
#
# The CRO Open Data Portal provides entity-level data only — no
# officer or director records are available from the free tier. We
# therefore emit a single entityStatement per company. The Open
# Services API (key-gated) can extend this with officers in future.

# CRO company_type values → BODS entityType
# https://core.cro.ie (type codes seen in practice)
_CRO_ENTITY_TYPES: dict[str, str] = {
    # Registered entities (limited liability companies)
    "LTD": "registeredEntity",
    "DAC": "registeredEntity",
    "PLC": "registeredEntity",
    "UC":  "registeredEntity",     # Unlimited company
    "CLG": "registeredEntity",     # Company limited by guarantee
    "EEIG": "registeredEntity",    # European Economic Interest Grouping
    "SE":  "registeredEntity",     # Societas Europaea
    "ICAV": "registeredEntity",    # Investment limited partnership
    "ILP": "registeredEntity",
}


def _cro_entity_type(company_type: str) -> str:
    """Map a CRO company type string to a BODS entityType."""
    # The company_type field carries a full description, e.g.
    # "PLC - Public Limited Company" or "LTD - Private company limited by shares".
    # Extract the leading abbreviation.
    code = (company_type or "").split("-")[0].strip().split("(")[0].strip().upper()
    for prefix, bods_type in _CRO_ENTITY_TYPES.items():
        if code.startswith(prefix):
            return bods_type
    return "registeredEntity"


def _cro_address(rec: dict[str, Any]) -> dict[str, str] | None:
    """Build a BODS address dict from CRO company_address_1..4 fields."""
    lines = [
        (rec.get(f"company_address_{i}") or "").strip()
        for i in range(1, 5)
    ]
    non_empty = [l for l in lines if l]
    if not non_empty:
        return None
    eircode = (rec.get("eircode") or "").strip()
    if eircode:
        non_empty.append(eircode)
    return _addr("registered", ", ".join(non_empty), "IE")


def map_cro(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CroAdapter fetch bundle to a single BODS v0.4 entity statement.

    Only entity data is available from the CRO Open Data Portal. When the
    CRO Open Services API key is configured (future enhancement), officer
    records will be added here as person + relationship statements.
    """
    if not bundle or bundle.get("is_stub"):
        return

    crn: str = str(bundle.get("crn") or "")
    company: dict[str, Any] = bundle.get("company") or {}

    name: str = (
        (company.get("company_name") or "").strip()
        or bundle.get("legal_name")
        or f"IE-CRN {crn}"
    )
    if not crn or not name:
        return

    source_url = f"https://core.cro.ie/company/{crn}"

    # Registration date comes as "1996-06-05T00:00:00" — take the date part.
    reg_date_raw = company.get("company_reg_date") or ""
    founding_date = reg_date_raw[:10] if reg_date_raw else None

    company_type = (company.get("company_type") or "").strip()
    entity_type = _cro_entity_type(company_type)

    identifiers: list[dict[str, str]] = [
        {
            "id": crn,
            "scheme": "IE-CRO",
            "schemeName": "Companies Registration Office Ireland",
        }
    ]

    nace = (company.get("nace_v2_code") or "").strip()
    if nace:
        identifiers.append({
            "id": nace,
            "scheme": "NACE2",
            "schemeName": "NACE Rev. 2 activity code",
        })

    address = _cro_address(company)

    cro_entity = make_entity_statement(
        source_id="cro",
        local_id=crn,
        name=name,
        jurisdiction=("Ireland", "IE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        entity_type=entity_type,
        source_url=source_url,
    )
    # CRO ``company_status`` (Phase 151): "Normal" is live; the insolvency
    # and strike-off-listed states are pending; dissolved / struck off end
    # the company. ``company_status_date`` when the open-data row has it.
    cro_status = (company.get("company_status") or "").strip()
    cro_liveness = _liveness.classify(
        cro_status,
        live=("Normal",),
        pending=("Liquidation", "Receivership", "Examinership", "Strike Off Listed", "Strike-off Listed"),
        terminal=("Dissolved", "Struck Off", "Struck-off", "Amalgamated", "Ceased"),
    )
    _liveness.apply_register_status(
        cro_entity,
        source_label=SOURCE_NAMES["cro"],
        liveness=cro_liveness,
        raw=cro_status or None,
        since=(
            str(company.get("company_status_date") or "")[:10] or None
            if cro_liveness != _liveness.LIVE
            else None
        ),
    )
    yield cro_entity
