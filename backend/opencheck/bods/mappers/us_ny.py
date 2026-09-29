"""United States, New York — Department of State, Division of Corporations → BODS v0.4.

Phase 263. ``mapper.py`` re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)

#: Statuses on the Entity Status History, by what they mean for a DOMESTIC
#: entity. ``Inactive`` for a domestic entity is DOS's word for "no longer
#: exists" (the All Filings overview). ``Suspended`` and ``Discontinued`` are
#: left unclassified — neither is a dissolution and neither is plainly live.
_NY_LIVE_STATUSES: tuple[str, ...] = ("Active",)
_NY_TERMINAL_STATUSES: tuple[str, ...] = ("Inactive",)


def _ny_role_details(filed_on: str | None) -> str:
    """What the CEO relationship says about itself — the register's words."""
    when = f" filed {filed_on}" if filed_on else ""
    return (
        "Chief executive officer, as named on the statement"
        f"{when} with the New York Department of State"
    )


def _ny_entity_details(summary: dict[str, Any]) -> str:
    """``entityType.details``: DOS's own wording for the entity type.

    A foreign entity whose home jurisdiction DOS writes with a code that is not
    ISO (``EN``, ``QU``) gets the code in words here, since the statement's
    ``jurisdiction`` is then left out rather than guessed.
    """
    from ...sources.ny_dos import jurisdiction_of

    entity_type = summary.get("entity_type") or ""
    text = (
        f"{entity_type} filed with the New York Department of State"
        if entity_type
        else "Filed with the New York Department of State"
    )
    juris = summary.get("juris") or ""
    if not summary.get("domestic") and juris and jurisdiction_of(juris) is None:
        text += f"; jurisdiction of incorporation filed as “{juris}”"
    return text


def map_ny_dos(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a NyDosAdapter bundle to BODS v0.4 statements.

    One entityStatement for the entity; for a corporation that names one, a
    personStatement per **current** chief executive officer and a
    ``seniorManagingOfficial`` relationship. "Current" means named on the
    latest filing that names any CEO; an earlier CEO is history, and the
    History tab carries it, not the graph.

    The CEO is carried by **name and role only** (Stephen, 29 Sept 2026): DOS
    publishes an address beside the name, and it may be a home address. No
    ``beneficialOwnershipOrControl`` is set — New York has no beneficial
    ownership register, and an executive officer is not a declared owner.

    Former names and assumed names become ``alternateNames``. The principal
    executive office (DOS address type 4) is the entity's ``business``
    address; the service-of-process address is not carried — it is often a
    registered agent's or a law firm's.

    ``foundingDate``: the formation filing for a domestic entity; for a
    foreign one, the home-jurisdiction incorporation date DOS records
    (``for_inc_date``) — never the date it was authorised in New York, which
    is not when it was founded.
    """
    from ...sources.ny_dos import (  # local import avoids a cycle
        NY_DOS_SCHEME,
        NY_DOS_SCHEME_NAME,
        address_object,
        jurisdiction_of,
        record_url,
        summarise,
    )

    if not bundle or bundle.get("is_stub") or not bundle.get("filings"):
        return
    summary = summarise(bundle)
    dos_id = summary["dos_id"]
    name = summary["name"]
    if not dos_id or not name:
        return

    source_url = record_url(dos_id)
    if summary["domestic"]:
        jurisdiction = ("United States", "US")
        founding = summary.get("formed_on")
    else:
        jurisdiction = jurisdiction_of(summary.get("juris"))
        founding = summary.get("incorporated_on")

    office = address_object(summary.get("principal_office"), address_type="business")

    subject_stmt = make_entity_statement(
        source_id="ny_dos",
        local_id=dos_id,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=[{"id": dos_id, "scheme": NY_DOS_SCHEME, "schemeName": NY_DOS_SCHEME_NAME}],
        founding_date=founding,
        addresses=[office] if office else [],
        alternate_names=list(summary.get("former_names") or [])
        + list(summary.get("assumed_names") or []),
        entity_type="registeredEntity",
        entity_details=_ny_entity_details(summary),
        source_url=source_url,
    )
    status = summary.get("status") or ""
    if summary["domestic"]:
        klass = _liveness.classify(
            status, live=_NY_LIVE_STATUSES, terminal=_NY_TERMINAL_STATUSES
        )
    else:
        # For a foreign entity "Inactive" means its authority to do business
        # in New York ended — the company itself may be alive and well at
        # home, so it is not classified as terminal.
        klass = _liveness.classify(status, live=_NY_LIVE_STATUSES)
    _liveness.apply_register_status(
        subject_stmt,
        source_label=SOURCE_NAMES["ny_dos"],
        liveness=klass,
        raw=status or None,
        since=summary.get("status_since") if klass == _liveness.TERMINAL else None,
    )
    yield subject_stmt
    subject_id: str = subject_stmt["statementId"]

    ceo_url = record_url(dos_id, "2tms-hftb")
    for person in summary.get("ceos") or []:
        local_id = ny_ceo_local_id(dos_id, person)
        yield make_person_statement(
            source_id="ny_dos",
            local_id=local_id,
            full_name=person,
            source_url=ceo_url,
        )
        interest = set_beneficial_ownership(
            {
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "details": _ny_role_details(summary.get("ceos_filed_on")),
            },
            "ny_dos",
        )
        yield make_relationship_statement(
            source_id="ny_dos",
            local_id=local_id,
            subject_statement_id=subject_id,
            interested_party_statement_id=_stable_id("ny_dos", "person", local_id),
            interested_party_type="person",
            interests=[interest],
            source_url=ceo_url,
        )


def ny_ceo_local_id(dos_id: str, person: str) -> str:
    """The local id a CEO's person statement is built on.

    Public because the History tab's board rows point at the same person
    statement the graph draws (``timeline/ny_dos.py``).
    """
    from ... import names as _names_mod

    return f"{dos_id}:ceo:{_names_mod.display_name_key(person) or person}"


def ny_ceo_statement_id(dos_id: str, person: str) -> str:
    """The ``statementId`` :func:`map_ny_dos` gives this CEO."""
    return _stable_id("ny_dos", "person", ny_ceo_local_id(dos_id, person))
