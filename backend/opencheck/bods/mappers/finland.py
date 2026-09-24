"""Finland — PRH (Finnish Patent and Registration Office) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ----------------------------------------------------------------------
# PRH — Finnish Patent and Registration Office (Patentti- ja rekisterihallitus)
# ----------------------------------------------------------------------

# Finnish company form codes → BODS entityType.
# PRH uses uppercase abbreviations from Finnish company law.
_PRH_ENTITY_TYPES: dict[str, str] = {
    "OY": "registeredEntity",    # Osakeyhtiö — private limited company
    "OYJ": "registeredEntity",   # Julkinen osakeyhtiö — public limited company
    "KY": "registeredEntity",    # Kommandiittiyhtiö — limited partnership
    "AY": "registeredEntity",    # Avoin yhtiö — general partnership
    "OOY": "registeredEntity",   # Osuuskunta — co-operative
    "OK": "registeredEntity",    # Osuuskunta — co-operative (alternate code)
    "SÄÄ": "registeredEntity",   # Säätiö — foundation
    "VOJ": "registeredEntity",   # Vakuutusosakeyhtiö — insurance company
    "MTY": "registeredEntity",   # Maatilatalouden yhtymä — farm association
    "ETS": "registeredEntity",   # Eurooppayhtiö (SE) — Societas Europaea
    "EOY": "registeredEntity",   # Eurooppaosuuskunta (SCE) — European co-op
}


def _prh_entity_type(company_form: str) -> str:
    """Map a PRH companyForm code to a BODS entityType."""
    code = (company_form or "").strip().upper()
    return _PRH_ENTITY_TYPES.get(code, "registeredEntity")


def _prh_current_name(names: list[dict[str, Any]]) -> str:
    """Extract the current primary name from the PRH names array."""
    active = [n for n in names if not n.get("endDate")]
    primary = [n for n in active if n.get("order") == 0]
    if primary:
        return (primary[0].get("name") or "").strip()
    if active:
        return (active[0].get("name") or "").strip()
    if names:
        return (names[0].get("name") or "").strip()
    return ""


def _prh_address(company: dict[str, Any]) -> dict[str, str] | None:
    """Build a BODS address dict from PRH address fields.

    PRH nests addresses in ``addresses[]`` or ``postAddress[]``. We
    prefer the first registered/visiting address, then postal.
    """
    for key in ("addresses", "postAddresses"):
        addrs = company.get(key) or []
        for addr in addrs:
            if addr.get("endDate"):
                continue
            parts = [
                (addr.get("street") or "").strip(),
                (addr.get("postCode") or "").strip(),
                (addr.get("city") or addr.get("postOffice") or "").strip(),
            ]
            non_empty = [p for p in parts if p]
            if non_empty:
                return _addr("registered", " ".join(non_empty), "FI")
    return None


def map_prh(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a PrhAdapter fetch bundle to a BODS v0.4 entity statement.

    Only entity data is available from the PRH YTJ Open Data API.
    Officer/role data requires the paid Virre service and is not
    included here.
    """
    if not bundle or bundle.get("is_stub"):
        return

    ytunnus: str = bundle.get("ytunnus") or ""
    company: dict[str, Any] = bundle.get("company") or {}

    names = company.get("names") or []
    name: str = (
        _prh_current_name(names)
        or bundle.get("legal_name")
        or f"FI-YTUNNUS {ytunnus}"
    )
    if not ytunnus or not name:
        return

    source_url = f"https://tietopalvelu.ytj.fi/yritystiedot.aspx?yavain={ytunnus}"

    company_form = (company.get("companyForm") or "").strip()
    entity_type = _prh_entity_type(company_form)

    # Registration date — look in businessId block.
    business_id = company.get("businessId") or {}
    founding_date = (business_id.get("registrationDate") or "")[:10] or None

    identifiers: list[dict[str, str]] = [
        {
            "id": ytunnus,
            "scheme": "FI-PRH",
            "schemeName": "Patentti- ja rekisterihallitus (PRH) — Finnish Trade Register",
        }
    ]

    # Business line code (TOL/NACE equivalent).
    biz_lines = company.get("mainBusinessLine") or []
    for bl in biz_lines:
        if bl.get("endDate"):
            continue
        code = (bl.get("code") or "").strip()
        if code:
            identifiers.append({
                "id": code,
                "scheme": "FI-TOL",
                "schemeName": "Finnish Standard Industrial Classification (TOL 2008)",
            })
        break  # Only the current primary business line.

    address = _prh_address(company)

    prh_entity = make_entity_statement(
        source_id="prh",
        local_id=ytunnus,
        name=name,
        jurisdiction=("Finland", "FI"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        entity_type=entity_type,
        source_url=source_url,
    )
    # YTJ open data (Phase 151): a ``liquidations`` block means a pending
    # process; an ``endDate`` on the company is its end. Otherwise the
    # register says nothing about liveness beyond listing it, so nothing is
    # asserted.
    prh_end = str(company.get("endDate") or "")[:10] or None
    if prh_end:
        _liveness.apply_register_status(
            prh_entity,
            source_label=SOURCE_NAMES["prh"],
            liveness=_liveness.TERMINAL,
            raw="endDate",
            since=prh_end,
        )
    elif company.get("liquidations"):
        liq = company.get("liquidations")
        first = liq[0] if isinstance(liq, list) and liq and isinstance(liq[0], dict) else {}
        _liveness.apply_register_status(
            prh_entity,
            source_label=SOURCE_NAMES["prh"],
            liveness=_liveness.PENDING,
            raw=str(first.get("type") or first.get("description") or "liquidation"),
            since=str(first.get("registrationDate") or "")[:10] or None,
        )
    yield prh_entity
