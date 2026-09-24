"""Malta — MBR (Malta Business Registry) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


def map_malta_mbr(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a MaltaMbrAdapter fetch bundle to a single BODS v0.4 entity statement.

    The MBR Open Data API exposes entity data only (no officers, shareholders
    or beneficial owners), so just one entityStatement is produced — the legal
    form is carried in ``entityType.details``.
    """
    if not bundle or bundle.get("is_stub"):
        return

    reg: str = str(bundle.get("mt_crn") or "")
    company: dict[str, Any] = bundle.get("company") or {}

    name: str = (
        (company.get("name") or "").strip()
        or bundle.get("legal_name")
        or (f"MT {reg}" if reg else "")
    )
    if not reg or not name:
        return

    # A stable, dereferenceable URL for the record (space percent-encoded).
    source_url = (
        "https://openapi.baros.mbr.mt/api/v1/companies/" + reg.replace(" ", "%20")
    )

    reg_date = (company.get("registration_date") or "").strip()
    founding_date = reg_date[:10] if reg_date else None

    legal_form = (company.get("type") or "").strip() or None

    identifiers: list[dict[str, str]] = [
        {
            "id": reg,
            "scheme": "MT-MBR",
            "schemeName": "Malta Business Registry",
        }
    ]

    # Registered office — concatenate the non-empty address components.
    parts = [
        (company.get("street") or "").strip(),
        (company.get("address") or "").strip(),
        (company.get("locality") or "").strip(),
        (company.get("postcode") or "").strip(),
    ]
    addr_str = ", ".join(p for p in parts if p)
    address = _addr("registered", addr_str, "MT") if addr_str else None

    mbr_entity = make_entity_statement(
        source_id="malta_mbr",
        local_id=reg,
        name=name,
        jurisdiction=("Malta", "MT"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        entity_type="registeredEntity",
        entity_details=legal_form,
        source_url=source_url,
    )
    # MBR ``state`` + ``status_effective_date`` (Phase 151). Labels seen in
    # the open-data API: Active, Struck Off, Dissolved, In Liquidation,
    # Defunct, Removed.
    mbr_state = (company.get("state") or "").strip()
    mbr_liveness = _liveness.classify(
        mbr_state,
        live=("active",),
        pending=("in liquidation", "in dissolution", "under liquidation"),
        terminal=("struck off", "dissolved", "defunct", "removed", "liquidated"),
    )
    mbr_since = (company.get("status_effective_date") or "").strip()[:10] or None
    _liveness.apply_register_status(
        mbr_entity,
        source_label=SOURCE_NAMES["malta_mbr"],
        liveness=mbr_liveness,
        raw=mbr_state or None,
        since=mbr_since if mbr_liveness != _liveness.LIVE else None,
    )
    yield mbr_entity
