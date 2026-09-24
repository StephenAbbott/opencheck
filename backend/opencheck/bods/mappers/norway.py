"""Norway — Brønnøysundregistrene (brreg) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# Brreg (Brønnøysundregistrene) → BODS
# ----------------------------------------------------------------------
#
# Brreg returns an entity record and a list of role-holders. We map:
#   entity         → entityStatement
#   each person    → personStatement (deduplicated by name+dob)
#   each role      → relationshipStatement (OOC) using interest type:
#
# Brreg role codes → BODS interest type:
#   DAGL  Daglig leder (CEO/MD)        → otherInfluenceOrControl
#   INNH  Innehaver (Proprietor)       → otherInfluenceOrControl
#   REPR  Representant                 → otherInfluenceOrControl
#   FFØR  Forretningsfører (manager)   → otherInfluenceOrControl
#   LEDE  Styrets leder (Chair)        → boardChair
#   NEST  Nestleder (Vice-chair)       → boardMember
#   MEDL  Styremedlem (Board member)   → boardMember
#   VARA  Varamedlem (Deputy member)   → boardMember
#   DTHO  Delta i st. f. st.           → boardMember
# All other codes (KONT, SOBSERV, KREV, BOBE, etc.) are skipped.

_BRREG_ROLE_MAP: dict[str, tuple[str, str]] = {
    "DAGL": ("otherInfluenceOrControl", "CEO / Daglig leder"),
    "INNH": ("otherInfluenceOrControl", "Proprietor / Innehaver"),
    "REPR": ("otherInfluenceOrControl", "Representative / Representant"),
    "FFØR": ("otherInfluenceOrControl", "Manager / Forretningsfører"),
    "LEDE": ("boardChair", "Chair / Styrets leder"),
    "NEST": ("boardMember", "Vice-chair / Nestleder"),
    "MEDL": ("boardMember", "Board member / Styremedlem"),
    "VARA": ("boardMember", "Deputy member / Varamedlem"),
    "DTHO": ("boardMember", "Board delegate / Delta i styret"),
}


def _brreg_address(block: dict[str, Any] | None) -> dict[str, str] | None:
    """Build a BODS address dict from a Brreg adresse block, or None."""
    if not block:
        return None
    parts: list[str] = []
    for line in block.get("adresse") or []:
        if line:
            parts.append(line)
    poststed = block.get("poststed") or ""
    postnummer = block.get("postnummer") or ""
    if postnummer and poststed:
        parts.append(f"{postnummer} {poststed}")
    elif poststed:
        parts.append(poststed)
    return _addr("registered", ", ".join(parts), block.get("landkode") or "NO")


def _brreg_person_local_id(person: dict[str, Any], idx: int) -> str:
    """Stable local ID for a person: name+dob hash, fallback to idx."""
    navn = person.get("navn") or {}
    fornavn = (navn.get("fornavn") or "").strip().lower()
    etternavn = (navn.get("etternavn") or "").strip().lower()
    dob = (person.get("fodselsdato") or "").strip()
    key = f"{fornavn}:{etternavn}:{dob}"
    if fornavn or etternavn:
        return hashlib.sha256(key.encode()).hexdigest()[:16]
    return f"person-{idx}"


def _brreg_full_name(person: dict[str, Any]) -> str:
    navn = person.get("navn") or {}
    fornavn = (navn.get("fornavn") or "").strip()
    etternavn = (navn.get("etternavn") or "").strip()
    if fornavn and etternavn:
        return f"{fornavn} {etternavn}"
    return etternavn or fornavn or ""


def _company_url_brreg(orgnr: str) -> str:
    return f"https://w2.brreg.no/enhet/sok/detalj.jsp?orgnr={orgnr}"


def map_brreg(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a BrregAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Norwegian company.
    * One personStatement per unique role-holder.
    * One relationshipStatement (OOC) per role record.
    """
    if not bundle or bundle.get("is_stub"):
        return

    orgnr: str = bundle.get("orgnr") or ""
    entity: dict[str, Any] = bundle.get("entity") or {}
    roles: list[dict[str, Any]] = bundle.get("roles") or []

    name: str = entity.get("navn") or bundle.get("legal_name") or orgnr
    if not orgnr or not name:
        return

    source_url = _company_url_brreg(orgnr)

    # ── 1. Entity statement ───────────────────────────────────────────────
    founding_date = (
        entity.get("stiftelsesdato")
        or entity.get("registreringsdatoEnhetsregisteret")
        or None
    )
    address = _brreg_address(
        entity.get("forretningsadresse") or entity.get("postadresse")
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": orgnr,
            "scheme": "NO-BRC",
            "schemeName": "Brønnøysundregistrene Enhetsregisteret",
        }
    ]

    company_stmt = make_entity_statement(
        source_id="brreg",
        local_id=orgnr,
        name=name,
        jurisdiction=("Norway", "NO"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        source_url=source_url,
    )
    # Enhetsregisteret flags (Phase 151): ``slettedato`` is the deletion
    # date (terminal); ``konkurs`` / ``underAvvikling`` /
    # ``underTvangsavviklingEllerTvangsopplosning`` are pending processes;
    # an entity with none of these is live in the register's eyes.
    no_deleted = str(entity.get("slettedato") or "")[:10] or None
    no_pending = [
        label
        for key, label in (
            ("konkurs", "konkurs"),
            ("underAvvikling", "under avvikling"),
            ("underTvangsavviklingEllerTvangsopplosning", "under tvangsavvikling eller tvangsoppløsning"),
        )
        if entity.get(key) is True
    ]
    if no_deleted:
        no_liveness, no_raw = _liveness.TERMINAL, "slettet"
    elif no_pending:
        no_liveness, no_raw = _liveness.PENDING, ", ".join(no_pending)
    elif any(k in entity for k in ("konkurs", "underAvvikling")):
        no_liveness, no_raw = _liveness.LIVE, "registrert"
    else:
        no_liveness, no_raw = _liveness.UNKNOWN, None
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["brreg"],
        liveness=no_liveness,
        raw=no_raw,
        since=no_deleted,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. Role-holder statements ─────────────────────────────────────────
    seen_person_ids: set[str] = set()

    for idx, role in enumerate(roles):
        # Role type: use the individual role type on the record.
        role_type_block = role.get("type") or role.get("_group_type") or {}
        role_code = role_type_block.get("kode") or ""
        role_label_raw = role_type_block.get("beskrivelse") or ""

        if role_code not in _BRREG_ROLE_MAP:
            continue

        # Skip roles that have been terminated.
        if role.get("fratraadt") or role.get("avregistrert"):
            continue

        interest_type, role_label = _BRREG_ROLE_MAP[role_code]
        display_label = role_label_raw or role_label

        person: dict[str, Any] = role.get("person") or {}
        full_name = _brreg_full_name(person)
        if not full_name:
            continue

        person_local_id = _brreg_person_local_id(person, idx)
        dob = person.get("fodselsdato") or None
        # sistEndret on the role group: when Enhetsregisteret last changed
        # these role records. The register's declaration date, not a role
        # start date — brreg publishes no per-role appointment date.
        role_last_changed = role.get("_group_last_changed") or None

        if person_local_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="brreg",
                local_id=person_local_id,
                full_name=full_name,
                birth_date=dob,
                source_url=source_url,
                statement_date=role_last_changed,
            )
            yield person_stmt
            seen_person_ids.add(person_local_id)
        else:
            person_stmt = {
                "statementId": _stable_id("brreg", "person", person_local_id)
            }

        interests: list[dict[str, Any]] = [
            {
                "type": interest_type,
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": display_label,
            }
        ]

        yield make_relationship_statement(
            source_id="brreg",
            local_id=f"{orgnr}:role:{idx}:{person_local_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
            statement_date=role_last_changed,
        )
