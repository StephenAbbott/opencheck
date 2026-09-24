"""Greece — General Commercial Registry (ΓΕΜΗ) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any

from .. import liveness as _liveness
from ..statements import (
    BODSBundle,
    SOURCE_NAMES,
    _addr,
    _today,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ---------------------------------------------------------------------------
# Greek General Commercial Registry (ΓΕΜΗ)
# ---------------------------------------------------------------------------

def _gemi() -> Any:
    """The ΓΕΜΗ adapter module — local import avoids a circular import.

    Several adapters import this mapper, so the mapper cannot import the
    sources package at module scope. Same pattern as ``_role_to_interest``
    (firmenbuch) and ``_cy_field`` (cyprus_drcor) below.
    """
    from ...sources import gemi_greece  # local import avoids circular

    return gemi_greece


#: ``persons[].category`` → how to read that person's relationship to the
#: company. The **category** decides this, not the free-text ``role``:
#: ``Εταίροι`` (partners) carry real ``percentage`` values, while
#: ``Διοικητικό συμβούλιο`` (board of directors) always carry ``"-"``.
#: Verified against live records for an ΑΕ, an ΙΚΕ and an ΕΕ on 2026-08-28.
_GEMI_CATEGORY_INTERESTS: dict[str, str] = {
    "Εταίροι": "shareholding",              # gemi_greece.CATEGORY_PARTNERS
    "Διοικητικό συμβούλιο": "seniorManagingOfficial",  # …CATEGORY_BOARD
}

#: Role fragments that indicate management or representation *in addition to*
#: whatever the category says. A partner who is also a Διαχειριστής holds two
#: distinct interests — a shareholding and a management role — and flattening
#: them into one would lose a fact the register published.
_GEMI_MANAGER_TOKENS = ("διαχειριστ", "εκπρόσωπ", "εκπροσωπ")

#: Greek partnership membership classes. The distinction is substantive —
#: an ομόρρυθμος partner has unlimited liability, an ετερόρρυθμος one does
#: not — so it is preserved rather than flattened to "partner".
_GEMI_PARTNER_CLASSES: dict[str, str] = {
    "ομόρρυθμο": "General partner (ομόρρυθμο μέλος) — unlimited liability",
    "ετερόρρυθμο": "Limited partner (ετερόρρυθμο μέλος) — limited liability",
}

#: ΓΕΜΗ emits already-ISO dates (``"1998-09-16"``). Declared here rather than
#: reused from ``findings`` — that module imports from ``sources``, which
#: imports adapters that import this mapper.
_GEMI_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _gemi_date(value: Any) -> str | None:
    """A ΓΕΜΗ date as ``YYYY-MM-DD``, or None.

    Dates arrive already ISO-formatted (``"1998-09-16"``); anything else is
    dropped rather than guessed at.
    """
    text = str(value or "").strip()
    return text[:10] if _GEMI_ISO_DATE.match(text) else None


def _gemi_is_past(value: str | None) -> bool:
    """True when a date is strictly in the past.

    Board appointments carry a **fixed future** ``dtTo`` (a term expiry, e.g.
    2023-10-27 → 2028-10-27). Treating that as a closed record would report
    every sitting Greek director as departed, so only a past ``dtTo`` closes a
    relationship.
    """
    return bool(value) and value < _today()


def _gemi_addresses(company: dict[str, Any]) -> list[dict[str, str]]:
    street = " ".join(
        part for part in (
            str(company.get("street") or "").strip(),
            str(company.get("streetNumber") or "").strip(),
        ) if part
    )
    municipality = _gemi().english_label("municipalities", company.get("municipality"))
    parts = [
        street,
        str(company.get("city") or "").strip(),
        municipality,
        str(company.get("zipCode") or "").strip(),
    ]
    address = ", ".join(part for part in parts if part)
    return [_addr("registered", address, "GR")] if address else []


def _gemi_identifiers(company: dict[str, Any]) -> list[dict[str, str]]:
    """Only identifiers ΓΕΜΗ itself publishes — never the LEI we arrived by."""
    identifiers: list[dict[str, str]] = []
    argemi = str(company.get("arGemi") or "").strip()
    if argemi:
        identifiers.append({
            "id": argemi,
            "scheme": "GR-GEMI",
            "schemeName": "General Commercial Registry (ΓΕΜΗ)",
        })
    afm = str(company.get("afm") or "").strip()
    if afm:
        identifiers.append({
            "id": afm,
            "scheme": "GR-AFM",
            "schemeName": "Greek Tax Registry Number (ΑΦΜ)",
        })
    return identifiers


def _gemi_person_local_id(argemi: str, person: dict[str, Any], index: int) -> str:
    """A stable local id for one person entry.

    ΓΕΜΗ publishes no personal identifier — no birth date, no tax number, just
    a name — so the id is derived from the name plus the role and start date
    that distinguish two entries for the same name. ``index`` is the final
    tie-breaker so two byte-identical rows still get distinct statements
    rather than silently collapsing into one.
    """
    name = str(person.get("personName") or person.get("businessName") or "").strip()
    return "|".join([
        argemi,
        name.casefold(),
        str(person.get("role") or "").strip().casefold(),
        str(person.get("dtFrom") or ""),
        str(index),
    ])


def _gemi_interests(person: dict[str, Any], source_id: str) -> list[dict[str, Any]]:
    """Interests for one ``persons[]`` entry.

    Branches on ``category`` and then refines with ``role``. A partner who is
    also a manager gets two interests, not one.

    ``beneficialOwnershipOrControl`` is deliberately never set: ΓΕΜΗ is a
    commercial register recording legal and registered holdings, and Greece's
    beneficial ownership register (Κεντρικό Μητρώο Πραγματικών Δικαιούχων) is
    a separate, non-public register. ``set_beneficial_ownership`` with
    ``asserted=None`` omits the flag for any source not in
    ``_BO_ASSERTING_SOURCES``, which ΓΕΜΗ is not — BODS reads the absence as
    "not stated", which is the honest claim.
    """
    category = str(person.get("category") or "").strip()
    role = str(person.get("role") or "").strip()
    role_folded = role.casefold()
    start = _gemi_date(person.get("dtFrom"))
    end = _gemi_date(person.get("dtTo"))

    def _build(interest_type: str, *, share: float | None = None) -> dict[str, Any]:
        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
        }
        if share is not None:
            interest["share"] = {"exact": share}
        if start:
            interest["startDate"] = start
        if end:
            interest["endDate"] = end
        if role:
            interest["details"] = role
        return set_beneficial_ownership(interest, source_id, asserted=None)

    interests: list[dict[str, Any]] = []
    primary = _GEMI_CATEGORY_INTERESTS.get(category)

    if primary == "shareholding":
        interests.append(_build("shareholding", share=_gemi().parse_percentage(person.get("percentage"))))
        # A partner who also manages or represents the company holds a second,
        # different interest. Board members already map to the management
        # interest, so this only applies on the partner branch.
        if any(token in role_folded for token in _GEMI_MANAGER_TOKENS):
            interests.append(_build("seniorManagingOfficial"))
    elif primary:
        interests.append(_build(primary))
    else:
        # An unrecognised category is recorded, not dropped: the raw Greek
        # survives in ``details`` so a reviewer can see what the register said.
        interests.append(_build("otherInfluenceOrControl"))

    return interests


def _gemi_partner_class(role: str) -> str | None:
    """The general/limited partnership class named in a role string, if any."""
    folded = role.casefold()
    for token, description in _GEMI_PARTNER_CLASSES.items():
        if token in folded:
            return description
    return None


def map_gemi_greece(bundle: dict[str, Any]) -> BODSBundle:
    """Map a ΓΕΜΗ fetch bundle to BODS v0.4 statements.

    Produces one entity statement for the company, then a person or entity
    statement plus a relationship statement for each ``persons[]`` entry.

    An **ΑΕ (société anonyme) yields officers but no owners** — its share
    register is not part of ΓΕΜΗ publicity. That is the Greek regime, not an
    absence of data, and callers must not present it as ownership being
    withheld.
    """
    statements = BODSBundle()
    company = bundle.get("company")
    if not isinstance(company, dict):
        return statements

    argemi = str(company.get("arGemi") or bundle.get("gr_argemi") or "").strip()
    if not argemi:
        return statements

    source_id = bundle.get("source_id") or "gemi_greece"
    url = _gemi().company_url(argemi)

    name = str(company.get("coNameEl") or bundle.get("legal_name") or "").strip()
    # Deduplicated: the Latin name and the Latin distinctive title are often
    # the same string (BUTTON P.C. carries "BUTTON" as both coTitlesEl and
    # coTitlesEn), and BODS alternateNames should not repeat itself.
    alternates: list[str] = []
    for key in ("coNamesEn", "coTitlesEl", "coTitlesEn"):
        for value in (company.get(key) or []):
            text = str(value).strip()
            if text and text != name and text not in alternates:
                alternates.append(text)

    # A dissolved company's ``lastStatusChange`` is when it left the register.
    # It is only a dissolution date when the status is actually inactive —
    # for a live company the same field is just the last time anything moved.
    is_active = _gemi().status_is_active(company.get("status"))
    last_change = _gemi_date(company.get("lastStatusChange"))
    dissolution = last_change if is_active is False else None

    status_label = _gemi().english_label("companyStatuses", company.get("status"))
    legal_form = _gemi().english_label("legalTypes", company.get("legalType"))
    details = " · ".join(part for part in (legal_form, status_label) if part) or None

    entity = make_entity_statement(
        source_id=source_id,
        local_id=argemi,
        name=name,
        jurisdiction=("Greece", "GR"),
        identifiers=_gemi_identifiers(company),
        founding_date=_gemi_date(company.get("incorporationDate")),
        addresses=_gemi_addresses(company),
        alternate_names=alternates,
        entity_details=details,
        source_url=url,
    )
    # Shared liveness path (Phase 151): the codelist's ``isActive`` is the
    # classification; an unknown status stays unknown, never "inactive".
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["gemi_greece"],
        liveness=(
            _liveness.LIVE if is_active is True
            else _liveness.TERMINAL if is_active is False
            else _liveness.UNKNOWN
        ),
        raw=status_label,
        since=dissolution,
    )
    statements.extend([entity])

    for index, person in enumerate(company.get("persons") or []):
        if not isinstance(person, dict):
            continue
        person_name = str(person.get("personName") or "").strip()
        business_name = str(person.get("businessName") or "").strip()
        if not person_name and not business_name:
            continue

        local_id = _gemi_person_local_id(argemi, person, index)
        role = str(person.get("role") or "").strip()

        if business_name:
            # A partner that is itself a company. ΓΕΜΗ gives only its name
            # here — no ΓΕΜΗ number for the holder — so it is an entity
            # statement with no identifiers rather than a resolvable link.
            party = make_entity_statement(
                source_id=source_id,
                local_id=f"party:{local_id}",
                name=business_name,
                entity_type="legalEntity",
                entity_details=_gemi_partner_class(role),
                source_url=url,
            )
            party_type = "entity"
        else:
            party = make_person_statement(
                source_id=source_id,
                local_id=f"party:{local_id}",
                full_name=person_name,
                source_url=url,
            )
            party_type = "person"
        statements.extend([party])

        interests = _gemi_interests(person, source_id)
        ended = _gemi_is_past(_gemi_date(person.get("dtTo")))
        statements.extend([
            make_relationship_statement(
                source_id=source_id,
                local_id=local_id,
                subject_statement_id=entity["recordId"],
                interested_party_statement_id=party["recordId"],
                interested_party_type=party_type,
                interests=interests,
                source_url=url,
                record_status="closed" if ended else "new",
            )
        ])

    return statements
