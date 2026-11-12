"""United States, Pennsylvania — Department of State (data.pa.gov) → BODS v0.4.

``mapper.py`` re-exports every name defined here.

Decisions (the Pennsylvania ticket, 12 Nov 2026 — by the Florida precedent):

* **No status is stated.** The dataset is the Department's list of active
  registrations, which it says overstates the active businesses. The company
  carries a ``commenting`` note saying so, never a register status, never a
  ``dissolutionDate``.
* **Titles are roles, never ownership** — "Owner", "Member", "Stockholder",
  "100% Owner" and "General Partner" included. Each party is a
  ``seniorManagingOfficial`` with the title as filed in ``details``; no
  ``beneficialOwnershipOrControl`` (Pennsylvania has no BO register).
* **Organizer and Incorporator** — who signed the formation filing — are
  ``otherInfluenceOrControl`` marked as an officer (Phase 340).
* **Name and title only.** The Department publishes no party address. A party
  named with a legal form is an entity with no jurisdiction (Phase 295).
* **Domestic only gets a jurisdiction and a ``foundingDate``.** A foreign
  registration's home is not published, and its creation date is the day it
  registered in Pennsylvania — said in a note.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..annotations import annotate, commenting, pointer
from ..officer_roles import mark_officer
from ..statements import (
    _addr,
    content_addressed,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)

_PA_SOURCE = "pa_dos"


def _pa_entity_details(summary: dict[str, Any]) -> str:
    kind = summary.get("registration_type") or "Business registration"
    return (
        f"{kind}, registered with the Pennsylvania Department of State "
        f"(filing number {summary['filing_number']})"
    )


def _pa_interest(title: str | None) -> dict[str, Any]:
    from ...sources.pa_dos import GOVERNOR_GLOSS, clean_field, is_formation_title

    text = clean_field(title)
    where = "as listed by the Pennsylvania Department of State"
    if not text:
        return {
            "type": "seniorManagingOfficial",
            "directOrIndirect": "direct",
            "details": f"Listed without a title, {where}",
        }
    if is_formation_title(text):
        return mark_officer(
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct",
                "details": f"{text}, who signed the formation filing, {where}",
            },
            text,
        )
    details = f"{text} ({GOVERNOR_GLOSS})" if text.lower() == "governor" else text
    return {
        "type": "seniorManagingOfficial",
        "directOrIndirect": "direct",
        "details": f"{details}, {where}",
    }


@content_addressed
def map_pa_dos(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a PaDosAdapter bundle to BODS v0.4 statements.

    One entityStatement for the business, identified by its bare filing
    number under ``US-PA`` (the form the GLEIF mapper writes for RA000632, so
    the two corroborate), and per named party a person or entity statement
    and one relationship carrying an interest per title. A note card
    (``not_found``) and a stub yield nothing.
    """
    from ...sources.pa_dos import (
        LIST_NOTE,
        PA_DOS_SCHEME,
        PA_DOS_SCHEME_NAME,
        record_url,
        summarise,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found") or not bundle.get("rows"):
        return
    summary = summarise(bundle)
    number = summary["filing_number"]
    name = summary["name"]
    if not number or not name:
        return
    source_url = record_url(number)
    cut = summary.get("data_cut")
    domestic = summary["domestic"]

    addresses = []
    if summary.get("address"):
        addresses.append(_addr("registered", summary["address"], "US"))

    company = make_entity_statement(
        source_id=_PA_SOURCE,
        local_id=number,
        statement_date=cut,
        name=name,
        # The ISO 3166-2 subdivision GLEIF files RA000632 companies under,
        # so the register's statement and GLEIF's agree (the US-PA ticket,
        # 12 Nov 2026). A foreign registration's home is not published.
        jurisdiction=("Pennsylvania, United States", "US-PA") if domestic else None,
        identifiers=[{"id": number, "scheme": PA_DOS_SCHEME, "schemeName": PA_DOS_SCHEME_NAME}],
        founding_date=summary.get("created_on") if domestic else None,
        addresses=addresses,
        entity_type="registeredEntity",
        entity_details=_pa_entity_details(summary),
        source_url=source_url,
    )
    annotate(company, commenting(pointer("recordDetails"), LIST_NOTE))
    if not domestic and summary.get("created_on"):
        annotate(company, commenting(
            pointer("recordDetails"),
            "A foreign registration: the Pennsylvania Department of State "
            f"records {summary['created_on']} as the day it registered in "
            "Pennsylvania, which is not its founding; its home jurisdiction "
            "is not published.",
        ))
    yield company
    company_id: str = company["recordId"]

    for party in summary["parties"]:
        local_id = f"{number}:party:{party['name'].casefold()}"
        if party["is_entity"]:
            statement = make_entity_statement(
                source_id=_PA_SOURCE,
                local_id=local_id,
                statement_date=cut,
                name=party["name"],
                jurisdiction=None,
                source_url=source_url,
            )
            party_type = "entity"
        else:
            statement = make_person_statement(
                source_id=_PA_SOURCE,
                local_id=local_id,
                statement_date=cut,
                full_name=party["name"],
                source_url=source_url,
            )
            legal = statement["recordDetails"]["names"][0]
            if party.get("family"):
                legal["familyName"] = party["family"]
            if party.get("given"):
                legal["givenName"] = party["given"]
            party_type = "person"
        party_id = statement["recordId"]
        yield statement
        titles = party["titles"] or [None]
        yield make_relationship_statement(
            source_id=_PA_SOURCE,
            local_id=f"{local_id}:rel",
            statement_date=cut,
            subject_statement_id=company_id,
            interested_party_statement_id=party_id,
            interested_party_type=party_type,
            interests=[_pa_interest(t) for t in titles],
            source_url=source_url,
        )
