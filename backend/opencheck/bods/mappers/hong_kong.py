"""Hong Kong — Companies Registry → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


def map_cr_hongkong(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CrHongKongAdapter fetch bundle to a single BODS v0.4 entity statement.

    The Companies Registry's open data carries entity data only — no officers,
    secretaries, shareholders or beneficial owners — so one entityStatement is
    produced. The English name is primary and the Chinese name goes to
    ``alternateNames``; the BRN is identified as ``HK-BRN``.

    The dataset lists live companies only and publishes no status field, so a
    record's presence is written as a ``live`` register status with no verbatim
    label (there is none to quote).
    """
    from ...sources.cr_hongkong import (  # local import avoids a cycle
        HK_BRN_SCHEME,
        HK_BRN_SCHEME_NAME,
        clean_field,
        company_url,
        parse_hk_date,
    )

    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    brn = clean_field(company.get("Brn")) or clean_field(bundle.get("hk_brn"))
    english = clean_field(company.get("English_Company_Name"))
    chinese = clean_field(company.get("Chinese_Company_Name"))
    name = english or chinese or clean_field(bundle.get("legal_name"))
    if not brn or not name:
        return

    alternate_names = [chinese] if chinese and chinese != name else []

    address_text = clean_field(company.get("Address_of_Registered_Office"))
    address = _addr("registered", address_text, "HK") if address_text else None

    entity = make_entity_statement(
        source_id="cr_hongkong",
        local_id=brn,
        name=name,
        jurisdiction=("Hong Kong", "HK"),
        identifiers=[
            {"id": brn, "scheme": HK_BRN_SCHEME, "schemeName": HK_BRN_SCHEME_NAME}
        ],
        founding_date=parse_hk_date(company.get("Date_of_Incorporation")),
        addresses=[address] if address else [],
        alternate_names=alternate_names,
        entity_type="registeredEntity",
        entity_details=clean_field(company.get("Company_Type")) or None,
        source_url=company_url(brn),
    )
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["cr_hongkong"],
        liveness=_liveness.LIVE,
    )
    yield entity
