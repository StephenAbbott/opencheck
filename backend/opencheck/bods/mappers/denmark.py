"""Denmark — CVR (Datafordeler) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    content_addressed,
    SOURCE_NAMES,
    _addr,
    latest_record_date,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)

#: The scheme of CVR's participant number (enhedsnummer) — the key CVR
#: itself gives a person (datacvr.virk.dk/enhed/person/<number>), never a CPR
#: number. One person per value: BackgroundCheck's
#: ``ONE_PERSON_PER_VALUE_SCHEMES`` holds its lower-cased form.
CVR_PARTICIPANT_SCHEME = "DK-CVR-ENHEDSNUMMER"
CVR_PARTICIPANT_SCHEME_NAME = "CVR participant number (enhedsnummer)"

#: The interest's words. A fully liable partner answers for the firm's debts
#: without limit and holds a partnership interest, not shares; CVR states no
#: size, so neither does OpenCheck, and nothing is said about beneficial
#: ownership (no regime: the flag is left unset).
CVR_PARTICIPANT_DETAILS = (
    "Fully liable participant (fuldt ansvarlig deltager), as registered in CVR"
)


# ---------------------------------------------------------------------------
# CVR Denmark → BODS
# ---------------------------------------------------------------------------


@content_addressed
def map_cvr_denmark(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CVR Denmark bundle to BODS v0.4 statements.

    Since Phase F (21 Oct 2026) a partnership's fully liable participants —
    the one participant relation Datafordeler's open CVR data carries — are
    mapped too: a person keyed on CVR's participant number (name only; CVR
    publishes no birth date or CPR number here), or a company keyed on its
    CVR number, each with one relationship whose interests are the periods
    CVR records. Directors, boards and owners are not in this data.

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

    yield from _cvr_participants(bundle, stmt["recordId"], source_url)


def _cvr_participants(
    bundle: dict[str, Any], subject_id: str, source_url: str | None
) -> Iterable[dict[str, Any]]:
    """Person/entity + relationship statements for the fully liable participants."""
    for part in bundle.get("participants") or []:
        pid = str(part.get("participant_id") or "")
        kind = part.get("kind")
        if not pid or kind not in ("person", "entity"):
            # An unresolved unit is left out rather than guessed at.
            continue
        periods = part.get("periods") or []
        dated = latest_record_date(
            [p.get("from") for p in periods] + [p.get("to") for p in periods]
        )
        name = (part.get("name") or "").strip()
        local_id = f"participant-{pid}"
        if kind == "person":
            if name:
                party = make_person_statement(
                    source_id="cvr_denmark",
                    local_id=local_id,
                    full_name=name,
                    identifiers=[
                        {
                            "id": pid,
                            "scheme": CVR_PARTICIPANT_SCHEME,
                            "schemeName": CVR_PARTICIPANT_SCHEME_NAME,
                        }
                    ],
                    source_url=source_url,
                    statement_date=dated,
                )
            else:
                party = make_person_statement(
                    source_id="cvr_denmark",
                    local_id=local_id,
                    full_name="Unnamed fully liable participant",
                    person_type="unknownPerson",
                    source_url=source_url,
                    statement_date=dated,
                )
            party_type = "person"
        else:
            cvr = part.get("cvr_number")
            party = make_entity_statement(
                source_id="cvr_denmark",
                # The company's own CVR record is keyed on its CVR number,
                # so a partner company looked up in its own right is one node.
                local_id=cvr or local_id,
                name=name or (cvr or "Unnamed participant company"),
                jurisdiction=("Denmark", "DK"),
                identifiers=(
                    [
                        {
                            "id": cvr,
                            "scheme": "DK-CVR",
                            "schemeName": (
                                "Det Centrale Virksomhedsregister — Danish Central "
                                "Business Register"
                            ),
                        }
                    ]
                    if cvr
                    else []
                ),
                source_url=f"https://datacvr.virk.dk/enhed/virksomhed/{cvr}" if cvr else source_url,
                statement_date=dated,
            )
            party_type = "entity"
        yield party
        interests = []
        for period in periods:
            interest: dict[str, Any] = {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct",
                "details": CVR_PARTICIPANT_DETAILS,
            }
            if period.get("from"):
                interest["startDate"] = str(period["from"])[:10]
            if period.get("to"):
                interest["endDate"] = str(period["to"])[:10]
            interests.append(set_beneficial_ownership(interest, "cvr_denmark"))
        yield make_relationship_statement(
            source_id="cvr_denmark",
            local_id=local_id,
            subject_statement_id=subject_id,
            interested_party_statement_id=party["recordId"],
            interested_party_type=party_type,
            interests=interests,
            source_url=source_url,
            statement_date=dated,
        )
