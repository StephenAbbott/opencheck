"""Guernsey — the Guernsey Registry's public search → BODS.

One entity statement for the registered entity, and per **corporate** official
an entity statement and a relationship. Individual officials are not named
(Stephen, 30 Oct 2026): the adapter keeps only their role and appointment
date, and this mapper says how many there are in one ``commenting``
annotation on the entity rather than drawing a nameless person.

The registry publishes no shareholders and no beneficial owners, so no
interest here is ``beneficialOwnershipOrControl: true``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pycountry

from .. import liveness as _liveness
from ..annotations import annotate, commenting, pointer
from ..former_names import former_name_annotations
from ..officer_roles import mark_officer
from ..statements import (
    SOURCE_NAMES,
    _addr,
    content_addressed,
    make_entity_statement,
    make_relationship_statement,
)

#: The Bailiwick's islands as the registry writes them in an address; all are
#: ISO 3166-1 ``GG``.
_GG_ISLANDS = frozenset({"guernsey", "alderney", "sark", "herm", "jethou", "lihou", "brecqhou"})

#: The registry's status words, classified (exact labels; ``classify`` is
#: case-insensitive on the whole label). "In Default" — an annual validation
#: not filed — is no terminal process and is left unclassified; the finding
#: still says it. A "Dissolved (…)" variant not listed here is caught by
#: :func:`gg_liveness`.
GG_STATUS_LIVE: tuple[str, ...] = ("Normal", "Registered", "Current")
GG_STATUS_PENDING: tuple[str, ...] = (
    "Strike Off Listed",
    "Voluntary strike off application",
    "Voluntary winding up Part 1",
    "Voluntary winding up Part 2",
    "Compulsory winding up Part 1",
)
GG_STATUS_TERMINAL: tuple[str, ...] = (
    "Dissolved",
    "Struck Off",
    "Wound Up",
    "Compulsorily Wound Up",
    "Amalgamated",
    "Migrated",
    "Converted",
    "Ceased",
    "Expired",
    "Historic",
)


def gg_liveness(status: str | None) -> _liveness.LivenessClass:
    """The liveness class of a registry status word."""
    text = " ".join(str(status or "").split())
    cls = _liveness.classify(
        text, live=GG_STATUS_LIVE, pending=GG_STATUS_PENDING, terminal=GG_STATUS_TERMINAL
    )
    if cls == _liveness.UNKNOWN and text.lower().startswith("dissolved"):
        return _liveness.TERMINAL
    return cls


#: Role word (lower case) → (interest type, marked as an officer?).
#: A director runs the company; a foundation's councillors are its board and
#: its chair the board's chair. A general partner and an LLP member hold the
#: partnership — control, not an office. A foundation's guardian enforces its
#: purposes — oversight, said as influence. A resident agent, secretary or
#: treasurer has no v0.4 role code and is ``otherInfluenceOrControl`` marked
#: as an officer (Phase 340), so the canvas draws a role, not control.
GG_ROLE_INTERESTS: dict[str, tuple[str, bool]] = {
    "director": ("seniorManagingOfficial", False),
    # A charity's or NPO's managing officials other than its chair.
    "other managing official": ("seniorManagingOfficial", False),
    "councillor": ("boardMember", False),
    "chairperson": ("boardChair", False),
    "chairman": ("boardChair", False),
    "general partner": ("otherInfluenceOrControl", False),
    "member": ("otherInfluenceOrControl", False),
    "guardian": ("otherInfluenceOrControl", False),
    "resident agent": ("otherInfluenceOrControl", True),
    "secretary": ("otherInfluenceOrControl", True),
    "treasurer": ("otherInfluenceOrControl", True),
}

_ROLE_DETAILS: dict[str, str] = {
    "member": "Member of the limited liability partnership",
    "general partner": "General partner of the limited partnership",
    "guardian": "Guardian of the foundation",
    "councillor": "Councillor of the foundation",
}


def gg_role(role: str | None) -> tuple[str, bool]:
    """``(interest type, officer?)`` for a registry role; an unknown role is a
    marked officer — a role is the reading that claims least."""
    key = " ".join(str(role or "").split()).lower()
    return GG_ROLE_INTERESTS.get(key, ("otherInfluenceOrControl", True))


def gg_interest(role: str | None, appointed: str | None) -> dict[str, Any]:
    text = " ".join(str(role or "").split()) or "Official"
    kind, officer = gg_role(text)
    interest: dict[str, Any] = {
        "type": kind,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": False,
        "details": _ROLE_DETAILS.get(text.lower(), text),
    }
    if appointed:
        interest["startDate"] = appointed
    if officer:
        mark_officer(interest, text)
    return interest


def _country_code(name: str | None) -> str:
    text = " ".join(str(name or "").split())
    if not text:
        return ""
    if text.lower() in _GG_ISLANDS:
        return "GG"
    try:
        return pycountry.countries.lookup(text).alpha_2
    except LookupError:
        return ""


def _role_counts(officials: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for o in officials:
        if o.get("kind") == "person":
            role = " ".join(str(o.get("role") or "official").split())
            counts[role] = counts.get(role, 0) + 1
    return counts


def gg_individuals_note(officials: list[dict[str, Any]]) -> str | None:
    """The sentence that stands in for the officials who are individuals."""
    counts = _role_counts(officials)
    if not counts:
        return None
    total = sum(counts.values())
    roles = ", ".join(
        f"{n} {r.lower()}" + ("s" if n != 1 and not r.lower().endswith("s") else "")
        for r, n in counts.items()
    )
    noun = "individual" if total == 1 else "individuals"
    return (
        f"The Guernsey Registry lists {total} {noun} among this entity's current "
        f"officials ({roles}). OpenCheck does not republish individuals' names "
        "from this register; corporate officials are shown."
    )


def _entity_statement(
    *,
    number: str,
    name: str,
    entity_type: str | None,
    source_url: str,
    registered_on: str | None = None,
    address: dict[str, Any] | None = None,
    alternate_names: list[str] | None = None,
) -> dict[str, Any]:
    from ...sources.guernsey_registry import GG_SCHEME, GG_SCHEME_NAME

    addresses = []
    if address and address.get("text"):
        addresses.append(_addr("registered", address["text"], _country_code(address.get("country"))))
    return make_entity_statement(
        source_id="guernsey_registry",
        local_id=number,
        name=name,
        jurisdiction=("Guernsey", "GG"),
        identifiers=[{"id": number, "scheme": GG_SCHEME, "schemeName": GG_SCHEME_NAME}],
        founding_date=registered_on,
        addresses=addresses,
        alternate_names=alternate_names or [],
        entity_details=entity_type or None,
        source_url=source_url,
    )


def _apply_status(stmt: dict[str, Any], status: str | None, since: str | None) -> None:
    cls = gg_liveness(status)
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["guernsey_registry"],
        liveness=cls,
        raw=status or None,
        # A live entity's status date is its registration date — no news.
        since=since if cls in (_liveness.PENDING, _liveness.TERMINAL) else None,
    )


@content_addressed
def map_guernsey_registry(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a GuernseyRegistryAdapter bundle to BODS v0.4 statements.

    A note card (``not_found``) and a stub yield nothing.
    """
    from ...sources.guernsey_registry import KIND_GG_ENTITY, KIND_PERSON, PORTAL_URL

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    entity = bundle.get("entity") or {}
    number = (entity.get("number") or bundle.get("gg_registry_number") or "").strip().upper()
    if not number:
        return
    name = entity.get("name") or bundle.get("legal_name") or f"Guernsey Registry {number}"

    former = [f for f in bundle.get("former_names") or [] if f.get("name") and f["name"] != name]
    alternate_names: list[str] = []
    for f in former:
        if f["name"] not in alternate_names:
            alternate_names.append(f["name"])

    company = _entity_statement(
        number=number,
        name=name,
        entity_type=entity.get("type"),
        source_url=PORTAL_URL,
        registered_on=entity.get("registered_on"),
        address=entity.get("address"),
        alternate_names=alternate_names,
    )
    annotate(
        company,
        *former_name_annotations(
            alternate_names, [(f["name"], None, f.get("since")) for f in former]
        ),
    )
    _apply_status(company, entity.get("status"), entity.get("status_since"))
    officials = [o for o in bundle.get("officials") or [] if isinstance(o, dict)]
    note = gg_individuals_note(officials)
    if note:
        annotate(company, commenting(pointer("recordDetails"), note))
    yield company
    company_id = company["recordId"]

    seen: dict[str, str] = {}
    for o in officials:
        if o.get("kind") == KIND_PERSON or not o.get("name"):
            continue
        if o.get("kind") == KIND_GG_ENTITY and o.get("number"):
            key = f"gg:{o['number']}"
            if key not in seen:
                party = _entity_statement(
                    number=o["number"],
                    name=o["name"],
                    entity_type=o.get("type"),
                    source_url=PORTAL_URL,
                )
                _apply_status(party, o.get("status"), None)
                yield party
                seen[key] = party["recordId"]
        else:
            # A body registered elsewhere: the registry files no country or
            # number for it, so none is asserted (Phase 295).
            key = "body:" + " ".join(o["name"].split()).casefold()
            if key not in seen:
                party = make_entity_statement(
                    source_id="guernsey_registry",
                    local_id=f"{number}:official:{o.get('official_key') or key}",
                    name=o["name"],
                    jurisdiction=None,
                    source_url=PORTAL_URL,
                )
                yield party
                seen[key] = party["recordId"]
        yield make_relationship_statement(
            source_id="guernsey_registry",
            local_id=f"{number}:official-rel:{o.get('relationship_id') or key}",
            subject_statement_id=company_id,
            interested_party_statement_id=seen[key],
            interested_party_type="entity",
            interests=[gg_interest(o.get("role"), o.get("appointed"))],
            source_url=PORTAL_URL,
        )
