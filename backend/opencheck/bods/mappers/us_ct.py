"""United States, Connecticut — Secretary of the State business registry → BODS v0.4.

The Connecticut wiring (12 Nov 2026). ``mapper.py`` re-exports every name
defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    content_addressed,
    SOURCE_NAMES,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)

_CT_SOURCE = "ct_sots"

#: What a principal's designation means, as an interest. A director is a
#: board member; an officer a senior managing official. A principal with no
#: designation is an LLC member or manager or an LLP partner — the register
#: does not say which — and is carried as a senior managing official worded
#: that way: **never** a shareholding, never ownership (Stephen's plan, 12 Nov
#: 2026; the Florida "authorized member" precedent).
_CT_DESIGNATION_INTEREST: dict[str, str] = {
    "Director": "boardMember",
    "Officer": "seniorManagingOfficial",
}

UNDESIGNATED_DETAILS = (
    "Principal — a member, manager or partner; the Connecticut Secretary of "
    "the State does not say which"
)


def _ct_entity_details(summary: dict[str, Any]) -> str:
    """``entityType.details``: the register's business type and citizenship."""
    business_type = summary.get("business_type") or ""
    head = f"{business_type} business" if business_type else "Business"
    if summary.get("domestic"):
        text = f"{head} formed in Connecticut, registered with the Connecticut Secretary of the State"
    else:
        text = f"Foreign {head.lower()} registered with the Connecticut Secretary of the State"
        formed = summary.get("formed_in")
        if formed:
            text += f"; formed in “{formed}” as filed"
    return text


def ct_principal_interests(designations: list[str], business_id: str) -> list[dict[str, Any]]:
    """One interest per designation the principal holds (a blank one: one)."""
    interests: list[dict[str, Any]] = []
    for designation in designations or [""]:
        kind = _CT_DESIGNATION_INTEREST.get(designation, "seniorManagingOfficial")
        if designation:
            details = (
                f"{designation} of the business, as listed on the Connecticut "
                "Secretary of the State's register of principals"
            )
        else:
            details = UNDESIGNATED_DETAILS
        interest: dict[str, Any] = {
            "type": kind,
            "directOrIndirect": "direct",
            "details": details,
        }
        interests.append(set_beneficial_ownership(interest, _CT_SOURCE))
    return interests


def ct_principal_local_id(business_id: str, name: str, entity: bool) -> str:
    from ... import names as _names_mod

    kind = "entity" if entity else "person"
    return f"{business_id}:principal:{kind}:{_names_mod.display_name_key(name) or name}"


def ct_principal_record_id(business_id: str, name: str, entity: bool = False) -> str:
    """The ``recordId`` of a principal's party statement (tests and joins)."""
    return _stable_id(
        _CT_SOURCE, "entity" if entity else "person",
        ct_principal_local_id(business_id, name, entity),
    )


@content_addressed
def map_ct_sots(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CtSotsAdapter bundle to BODS v0.4 statements.

    One entityStatement for the business; per principal a person (or, for a
    company named with a legal-form token and no first or last name, an
    entity with no identifier and no jurisdiction) and one relationship with
    an interest per designation. Name and role only. No
    ``beneficialOwnershipOrControl`` (Connecticut publishes no beneficial
    owners). Former names from the Name Change History are ``alternateNames``.

    ``foundingDate``: the registration date of a **domestic** business; a
    foreign business's registration date is when it registered in
    Connecticut, not when it was formed, so it is left out.
    """
    from ...sources.ct_sots import (  # local import avoids a cycle
        CT_SCHEME,
        CT_SCHEME_NAME,
        LIVE_STATUSES,
        TERMINAL_STATUSES,
        address_object,
        jurisdiction_of,
        principals_url,
        record_url,
        summarise,
    )

    if not bundle or bundle.get("is_stub") or not bundle.get("master"):
        return
    summary = summarise(bundle)
    business_id = summary["business_id"]
    name = summary["name"]
    if not business_id or not name:
        return
    master = bundle["master"]

    address = address_object(master)
    subject_stmt = make_entity_statement(
        source_id=_CT_SOURCE,
        local_id=business_id,
        name=name,
        jurisdiction=jurisdiction_of(master),
        identifiers=[{"id": business_id, "scheme": CT_SCHEME, "schemeName": CT_SCHEME_NAME}],
        founding_date=summary.get("registered_on") if summary["domestic"] else None,
        addresses=[address] if address else [],
        alternate_names=list(summary.get("former_names") or []),
        entity_type="registeredEntity",
        entity_details=_ct_entity_details(summary),
        source_url=record_url(business_id),
    )
    status = summary.get("status") or ""
    if summary["domestic"]:
        klass = _liveness.classify(status, live=LIVE_STATUSES, terminal=TERMINAL_STATUSES)
    else:
        # A foreign business that withdrew or lost its registration only
        # stopped being registered in Connecticut — never terminal, so it
        # reads as unknown and the finding says what ended.
        klass = _liveness.classify(status, live=LIVE_STATUSES)
    _liveness.apply_register_status(
        subject_stmt,
        source_label=SOURCE_NAMES[_CT_SOURCE],
        liveness=klass,
        raw=status or None,
        since=summary.get("dissolved_on") if klass == _liveness.TERMINAL else None,
    )
    yield subject_stmt
    subject_id: str = subject_stmt["recordId"]

    url = principals_url(summary.get("record_id") or "")
    for principal in summary.get("principals") or []:
        entity = bool(principal["entity"])
        local_id = ct_principal_local_id(business_id, principal["name"], entity)
        if entity:
            party = make_entity_statement(
                source_id=_CT_SOURCE,
                local_id=local_id,
                name=principal["name"],
                jurisdiction=None,
                source_url=url,
            )
        else:
            party = make_person_statement(
                source_id=_CT_SOURCE,
                local_id=local_id,
                full_name=principal["name"],
                source_url=url,
            )
            legal = party["recordDetails"]["names"][0]
            if principal.get("family"):
                legal["familyName"] = principal["family"]
            if principal.get("given"):
                legal["givenName"] = principal["given"]
        yield party
        yield make_relationship_statement(
            source_id=_CT_SOURCE,
            local_id=f"{local_id}:rel",
            subject_statement_id=subject_id,
            interested_party_statement_id=party["recordId"],
            interested_party_type="entity" if entity else "person",
            interests=ct_principal_interests(principal["designations"], business_id),
            source_url=url,
        )
