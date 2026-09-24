"""United States, District of Columbia — DLCP Corporations Division → BODS v0.4.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from ... import names as _names_mod
from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _country_obj,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ----------------------------------------------------------------------
# Washington DC — DLCP Corporations Division → BODS v0.4
# ----------------------------------------------------------------------

#: The jurisdiction DLCP registers into. GLEIF files these companies under the
#: ISO 3166-2 subdivision ``US-DC``; BODS wants a country, so the country is
#: the United States and the District is named in ``entityType.details`` and
#: the registered address.
_DC_JURISDICTION = ("United States", "US")

#: What § 29-102.11(a)(6) actually asks for, said once, and attached to every
#: interest this source emits. Without it a reader sees "unknown interest" and
#: has no way to tell that the register asked a 10%-or-control question and
#: got back a list with no roles and no percentages.
_DC_INTEREST_DETAILS = (
    "Owner or controller filed on the DC biennial report under D.C. Code "
    "§ 29-102.11(a)(6): a person whose aggregate direct or indirect, legal or "
    "beneficial ownership of a governance or total distributional interest "
    "exceeds 10%, or who controls the entity's financial or operational "
    "decisions or can direct its day-to-day operations. DLCP publishes no "
    "role and no percentage for the filing."
)


def _dc_epoch_to_date(value: Any) -> str | None:
    """An ArcGIS epoch-milliseconds field as an ISO date, or None.

    DLCP's ``EFFECTIVE_DATE`` is milliseconds since the epoch and is negative
    for a company incorporated before 1970 (``ARCADIA SPORTING CLUB OF
    AMERICA``, 1926), which is why this divides rather than truncating.
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _dc_business_address(company: dict[str, Any]) -> list[dict[str, Any]]:
    """The company's registered address as DLCP holds it, or ``[]``.

    Four numbered street lines (the register's own column names carry the
    ``BUSNIESS`` misspelling), then city and state. The ZIP and the country
    are isolated into their own BODS fields.
    """
    from ...sources.dlcp_dc import clean_field  # local import avoids a cycle

    parts = [
        clean_field(company.get(key))
        for key in (
            "BUSNIESS_ADDRESS_LINE1",
            "BUSNIESS_ADDRESS_LINE2",
            "BUSNIESS_ADDRESS_LINE3",
            "BUSNIESS_ADDRESS_LINE4",
            "BUSINESS_CITY",
            "BUSINESS_STATE",
        )
    ]
    text = ", ".join(p for p in parts if p)
    if not text:
        return []
    address: dict[str, Any] = {"type": "registered", "address": text}
    zipcode = clean_field(company.get("ZIPCODE"))
    if zipcode:
        address["postCode"] = zipcode
    country = _country_obj(clean_field(company.get("BUSINESS_COUNTRY")))
    if country:
        address["country"] = country
    return [address]


def map_dlcp_dc(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a DlcpDcAdapter bundle to BODS v0.4 statements.

    Yields one entityStatement for the DC company, and per owner row a person-
    or entityStatement plus an ownership-or-control relationshipStatement.

    The interest is always ``unknownInterest`` with ``directOrIndirect:
    "unknown"`` and **no share**: D.C. Code § 29-102.11(a)(6) covers direct
    and indirect holdings of a governance *or* distributional interest, and
    DLCP publishes neither the role nor the percentage. Inventing either would
    be a claim the register never made.

    ``beneficialOwnershipOrControl`` is routed through the regimes registry
    (bo_regimes.py) on the record kind: ``bo_person`` -> true (the filing is a
    beneficial ownership declaration under the DC regime), ``bo_entity`` ->
    false (an entity interested party is never a beneficial owner — the same
    rule that gives UK corporate RLEs false). :func:`classify_owner` decides
    which, from the legal form in the name, since DLCP publishes no owner
    type.

    Owners are deduped by name within one company, so the single duplicate row
    seen per 4,000 (2026-09-18) does not become two relationships. A row with
    a blank name (1.2% of rows) becomes its own ``unknownPerson`` party rather
    than being dropped: the register recorded a filing, and the gap is in what
    it published.

    Trade names become ``alternateNames`` on the company. The registered agent
    is **not** emitted as a relationship — an agent holds no interest — but is
    named in the finding.
    """
    from ...sources.dlcp_dc import (  # local import avoids a cycle
        DC_FILE_NUMBER_SCHEME,
        DC_FILE_NUMBER_SCHEME_NAME,
        LIVE_STATUSES,
        TERMINAL_STATUSES,
        classify_owner,
        clean_field,
        parse_owner_address,
        record_url,
    )

    if not bundle or bundle.get("is_stub"):
        return
    company: dict[str, Any] = bundle.get("company") or {}
    file_number = clean_field(company.get("FILE_NUMBER"))
    name = clean_field(company.get("BUSINESS_NAME"))
    if not file_number or not name:
        return

    source_url = record_url(0, f"FILE_NUMBER = '{file_number}'")
    model_type = clean_field(company.get("MODELTYPE"))

    subject_stmt = make_entity_statement(
        source_id="dlcp_dc",
        local_id=file_number,
        name=name,
        jurisdiction=_DC_JURISDICTION,
        identifiers=[
            {
                "id": file_number,
                "scheme": DC_FILE_NUMBER_SCHEME,
                "schemeName": DC_FILE_NUMBER_SCHEME_NAME,
            }
        ],
        founding_date=_dc_epoch_to_date(company.get("EFFECTIVE_DATE")),
        addresses=_dc_business_address(company),
        alternate_names=list(bundle.get("trade_names") or []),
        entity_type="registeredEntity",
        entity_details=(
            f"{model_type} registered with the DC Corporations Division"
            if model_type
            else "Registered with the DC Corporations Division"
        ),
        source_url=source_url,
    )
    status = clean_field(company.get("ENTITY_STATUS"))
    _liveness.apply_register_status(
        subject_stmt,
        source_label=SOURCE_NAMES["dlcp_dc"],
        liveness=_liveness.classify(
            status, live=LIVE_STATUSES, terminal=TERMINAL_STATUSES
        ),
        raw=status or None,
    )
    yield subject_stmt
    subject_id: str = subject_stmt["statementId"]

    owners_url = record_url(2, f"INITIALFILENUMBER = '{file_number}'")
    seen: set[str] = set()
    for index, row in enumerate(bundle.get("owners") or []):
        owner_name = clean_field(row.get("NAME"))
        key = _names_mod.display_name_key(owner_name) if owner_name else f"blank:{index}"
        if key in seen:
            continue
        seen.add(key)

        # BODS v0.4 scopes the address type by record kind: ``residence`` is
        # valid only on a person, ``business`` only on an entity. The register
        # publishes one address per owner and does not say which it is, so a
        # person's is filed as their residence (what the statute asks for) and
        # an entity's as a business address.
        owner_kind = "person" if not owner_name else classify_owner(owner_name)
        address = parse_owner_address(
            row.get("ADDRESS"),
            address_type="business" if owner_kind == "entity" else "residence",
        )
        addresses = [address] if address else []

        if not owner_name:
            local_id = f"{file_number}:unnamed:{index}"
            yield make_person_statement(
                source_id="dlcp_dc",
                local_id=local_id,
                full_name=_DC_UNNAMED_OWNER,
                person_type="unknownPerson",
                addresses=addresses,
                source_url=owners_url,
            )
            party_id = _stable_id("dlcp_dc", "person", local_id)
            party_type = "person"
            record_kind = "bo_person"
        elif owner_kind == "entity":
            local_id = f"{file_number}:entity:{owner_name}"
            yield make_entity_statement(
                source_id="dlcp_dc",
                local_id=local_id,
                name=owner_name,
                addresses=addresses,
                entity_type="registeredEntity",
                entity_details=(
                    "Filed as an owner or controller on the DC biennial "
                    "report; DLCP publishes no identifier or jurisdiction "
                    "for the filing"
                ),
                source_url=owners_url,
            )
            party_id = _stable_id("dlcp_dc", "entity", local_id)
            party_type = "entity"
            record_kind = "bo_entity"
        else:
            local_id = f"{file_number}:person:{owner_name}"
            yield make_person_statement(
                source_id="dlcp_dc",
                local_id=local_id,
                full_name=owner_name,
                addresses=addresses,
                source_url=owners_url,
            )
            party_id = _stable_id("dlcp_dc", "person", local_id)
            party_type = "person"
            record_kind = "bo_person"

        interest = set_beneficial_ownership(
            {
                "type": "unknownInterest",
                "directOrIndirect": "unknown",
                "details": _DC_INTEREST_DETAILS,
            },
            "dlcp_dc",
            record_kind=record_kind,
        )
        yield make_relationship_statement(
            source_id="dlcp_dc",
            local_id=local_id,
            subject_statement_id=subject_id,
            interested_party_statement_id=party_id,
            interested_party_type=party_type,
            interests=[interest],
            source_url=owners_url,
        )


#: Display name for an owner row DLCP published with an empty name field
#: (46 of a random 4,000 rows, 2026-09-18). ``unknownPerson``, not
#: ``anonymousPerson``: the gap is in what the open dataset carries, not a
#: withholding by the company, so it must not reach the opaque-ownership risk
#: signal (risk.py fires on anonymous* only).
_DC_UNNAMED_OWNER = "Unnamed owner or controller"
