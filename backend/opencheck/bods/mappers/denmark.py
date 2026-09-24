"""Denmark — CVR (Datafordeler) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ---------------------------------------------------------------------------
# CVR Denmark → BODS
# ---------------------------------------------------------------------------


def map_cvr_denmark(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CVR Denmark bundle to BODS v0.4 statements.

    CVRPerson (natural persons) is a restricted entity not available without
    a separate access application, so this mapper produces entity statements
    only — no person or ownership-or-control statements.

    The entity identifier uses the "DK-CVR" scheme (CVR number).
    """
    cvr_number: str = bundle.get("cvr_number", "")
    if bundle.get("is_stub") or not cvr_number:
        return
    name: str = bundle.get("name", "") or cvr_number
    status: str = bundle.get("status", "unknown")
    start_date: str | None = bundle.get("start_date")
    end_date: str | None = bundle.get("end_date")
    legal_form_text: str | None = bundle.get("legal_form_text")
    branche_code: str | None = bundle.get("branche_code")
    source_url: str | None = bundle.get("source_url")
    addr_raw: dict[str, Any] | None = bundle.get("address")

    # Identifiers.
    identifiers: list[dict[str, str]] = [
        {
            "id": cvr_number,
            "scheme": "DK-CVR",
            "schemeName": (
                "Det Centrale Virksomhedsregister — Danish Central Business Register"
            ),
        }
    ]

    # Address block.
    addresses: list[dict[str, Any]] = []
    if addr_raw:
        parts = []
        street = addr_raw.get("CVRAdresse_vejnavn", "")
        house = addr_raw.get("CVRAdresse_husnummerFra", "")
        if street:
            parts.append(f"{street} {house}".strip())
        postal = addr_raw.get("CVRAdresse_postnummer", "")
        city = addr_raw.get("CVRAdresse_postdistrikt", "")
        if postal or city:
            parts.append(f"{postal} {city}".strip())
        country_code = addr_raw.get("CVRAdresse_landekode", "DK") or "DK"
        country_names = {"DK": "Denmark"}
        country_name = country_names.get(country_code, country_code)
        full_address = ", ".join(p for p in parts if p)
        if full_address:
            addresses.append(_addr("registered", full_address, country_code))

    # Jurisdiction.
    jurisdiction = ("Denmark", "DK")

    # Entity statement.
    stmt = make_entity_statement(
        source_id="cvr_denmark",
        local_id=cvr_number,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        founding_date=start_date,
        addresses=addresses,
        source_url=source_url,
    )

    # entityType block.  BODS v0.4 entityType.subtype is a restricted enum
    # (governmentDepartment, stateAgency, other, trust, nomination) and does
    # not accept arbitrary legal-form text.  The Danish legal form label is
    # stored in the non-schema annotation field "legalFormLabel" for
    # informational use only; libcovebods ignores unknown extra fields.
    record_details = stmt.get("recordDetails") or {}
    record_details["entityType"] = {"type": "registeredEntity"}
    if legal_form_text:
        record_details["legalFormLabel"] = legal_form_text

    # Register status → liveness (Phase 151). ``status`` is the adapter's
    # normalised English label (``_STATUS_MAP`` in sources/cvr_denmark.py,
    # falling back to the API's own text); ``end_date`` is
    # ``virksomhedOphoersdato``. A published end date is the register's own
    # statement that the company ended and outranks the label.
    cvr_liveness = _liveness.classify(
        status,
        live=("active",),
        pending=("in bankruptcy", "in forced dissolution", "in voluntary liquidation"),
        terminal=("dissolved", "dissolved (error registration)", "deleted"),
    )
    if end_date:
        cvr_liveness = _liveness.TERMINAL
    stmt["recordDetails"] = record_details
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["cvr_denmark"],
        liveness=cvr_liveness,
        raw=bundle.get("status_label") or (status if status != "unknown" else None),
        since=end_date,
    )

    # Industry code (DB07/NACE) as supplemental annotation.
    if branche_code:
        record_details["primaryIndustryCode"] = branche_code

    stmt["recordDetails"] = record_details

    yield stmt
