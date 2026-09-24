"""Latvia — Register of Enterprises (UR) → BODS v0.4.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

import pycountry

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _addr,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ----------------------------------------------------------------------
# Latvia Register of Enterprises (UR) → BODS v0.4
# ----------------------------------------------------------------------

# BODS entity type for each Latvian legal form code.
_LV_ENTITY_TYPES: dict[str, str] = {
    "SIA": "registeredEntity",   # Sabiedrība ar ierobežotu atbildību (LLC)
    "AS": "registeredEntity",    # Akciju sabiedrība (JSC)
    "IK": "registeredEntity",    # Individuālais komersants (Sole trader)
    "IND": "registeredEntity",   # Individuālais uzņēmums (Private enterprise)
    "ZEM": "registeredEntity",   # Zemnieku saimniecība (Farm enterprise)
    "PS": "registeredEntity",    # Pilnsabiedrība (General partnership)
    "KS": "registeredEntity",    # Komandītsabiedrība (Limited partnership)
    "KB": "registeredEntity",    # Kooperatīvā sabiedrība (Cooperative)
    "BDR": "legalEntity",        # Biedrība (Association)
    "NOD": "legalEntity",        # Nodibinājums (Foundation)
    "VU": "registeredEntity",    # Valsts uzņēmums (State enterprise)
    "PSV": "registeredEntity",   # Pašvaldības uzņēmums (Municipal enterprise)
    "FIL": "registeredEntity",   # Filiāle (Branch)
    "AKF": "registeredEntity",   # Ārvalsts komersanta filiāle (Foreign branch)
    "PAR": "registeredEntity",   # Ārvalsts komersanta pārstāvniecība
    "DRZ": "legalEntity",        # Draudze (Religious congregation)
    "MIL": "registeredEntity",   # Masu informācijas līdzeklis
    "SPO": "legalEntity",        # Sporta organizācija (Sports organisation)
    "SAB": "legalEntity",        # Sabiedriskā organizācija (Public organisation)
    "ASF": "registeredEntity",   # AS filiāle (JSC branch)
}

# Map CKAN governing_body values to BODS interest types for officer OOC stmts.
_LV_GOVERNING_BODY_INTEREST: dict[str, str] = {
    "EXECUTIVE_BOARD": "boardMember",
    "SUPERVISORY_BOARD": "boardMember",
    "COUNCIL": "boardMember",
    "AUDIT_COMMISSION": "boardMember",
    "LIQUIDATOR": "otherInfluenceOrControl",
    "ADMINISTRATOR": "otherInfluenceOrControl",
    "TRUSTEE_IN_BANKRUPTCY": "otherInfluenceOrControl",
}


def _lv_date(dt_str: str | None) -> str | None:
    """Return ISO-8601 date (YYYY-MM-DD) from a CKAN datetime string, or None."""
    if not dt_str:
        return None
    return str(dt_str)[:10] or None


def _lv_nationality(code: str | None) -> list[dict[str, str]]:
    """Convert a 2-letter ISO country code to a BODS nationality entry."""
    if not code:
        return []
    try:
        country = pycountry.countries.get(alpha_2=code.upper())
        if country:
            return [{"name": country.name, "code": code.upper()}]
    except Exception:
        pass
    return [{"name": code, "code": code}]


def map_ur_latvia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a UrLatviaAdapter fetch bundle to BODS v0.4 statements.

    Yields, in order:
    1. One entity statement for the registered company.
    2. Person or entity statements for each beneficial owner declared in the
       UR BO register, followed by the corresponding OOC relationship.
    3. Person or entity statements for each officer (executive/supervisory board
       member, liquidator, etc.), followed by the OOC relationship.
    4. Person or entity statements for each SIA shareholder (member), followed
       by the OOC relationship.

    Historical names from the UR historical-names dataset are attached as
    ``alternateNames`` on the entity statement.
    """
    if not bundle or bundle.get("is_stub"):
        return

    regcode: str = bundle.get("lv_regcode") or ""
    entity_rec: dict[str, Any] = bundle.get("entity") or {}
    if not regcode or not entity_rec:
        return

    # ------------------------------------------------------------------
    # 1.  Entity statement
    # ------------------------------------------------------------------

    name: str = (
        (entity_rec.get("name") or "").strip()
        or bundle.get("legal_name")
        or f"LV-{regcode}"
    )
    type_code: str = (entity_rec.get("type") or "").strip()
    entity_type: str = _LV_ENTITY_TYPES.get(type_code, "registeredEntity")

    # Parse registration / dissolution dates (datetime → date string).
    founding_date = _lv_date(entity_rec.get("registered"))
    # ``terminated`` is the register's termination date; ``closed`` a closure
    # marker. Either ends the entity (the adapter's own search summary already
    # reads them as "inactive"); the parsed date was previously computed here
    # and discarded (Phase 151).
    dissolution_date = _lv_date(entity_rec.get("terminated"))
    lv_closed = str(entity_rec.get("closed") or "").strip()

    # Alternate names from historical_names table.
    hist_names: list[dict[str, Any]] = bundle.get("historical_names") or []
    alternate_names: list[str] = [
        (h.get("name") or "").strip()
        for h in hist_names
        if (h.get("name") or "").strip()
    ]

    # Address — pre-formatted string from the business register.
    raw_address: str = (entity_rec.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "LV")]
        if raw_address
        else []
    )

    # Identifiers.
    identifiers: list[dict[str, str]] = [
        {
            "id": regcode,
            "scheme": "LV-UR",
            "schemeName": "Latvian Register of Enterprises (UR)",
        }
    ]
    sepa: str = (entity_rec.get("sepa") or "").strip()
    if sepa:
        identifiers.append({
            "id": sepa,
            "scheme": "LV-SEPA",
            "schemeName": "Latvian SEPA Account Identifier",
        })

    source_url = f"https://www.ur.gov.lv/lv/registri/komercregistrs/{regcode}/"

    entity_stmt = make_entity_statement(
        source_id="ur_latvia",
        local_id=regcode,
        name=name,
        jurisdiction=("Latvia", "LV"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        alternate_names=alternate_names,
        entity_type=entity_type,
        source_url=source_url,
    )
    _liveness.apply_register_status(
        entity_stmt,
        source_label=SOURCE_NAMES["ur_latvia"],
        liveness=_liveness.TERMINAL if (dissolution_date or lv_closed) else _liveness.LIVE,
        raw=lv_closed or ("terminated" if dissolution_date else "registered"),
        since=dissolution_date,
    )
    yield entity_stmt
    entity_stmt_id: str = entity_stmt["statementId"]

    # ------------------------------------------------------------------
    # 2.  Beneficial owners (UBO declarations)
    # ------------------------------------------------------------------

    bowners: list[dict[str, Any]] = bundle.get("beneficial_owners") or []
    for bo in bowners:
        bo_id = str(bo.get("id") or "")
        forename = (bo.get("forename") or "").strip()
        surname = (bo.get("surname") or "").strip()
        full_name = " ".join(filter(None, [forename, surname])).strip()
        if not full_name:
            continue

        nationality_code = (bo.get("nationality") or "").strip()
        nationalities = _lv_nationality(nationality_code)
        birth_date = _lv_date(bo.get("birth_date"))
        registered_on = _lv_date(bo.get("registered_on"))

        person_local_id = f"bo-{regcode}-{bo_id or full_name}"
        person_stmt = make_person_statement(
            source_id="ur_latvia",
            local_id=person_local_id,
            full_name=full_name,
            nationalities=nationalities,
            birth_date=birth_date or None,
            source_url=source_url,
        )
        yield person_stmt

        # Declared BO records are the Latvian BO declarations — the flag
        # comes from the regimes registry (ur_latvia/beneficial_owner -> true).
        interests = [
            set_beneficial_ownership(
                {
                    "type": "otherInfluenceOrControl",
                    "directOrIndirect": "unknown",
                    "details": "Declared beneficial owner per Latvian UR register",
                    **({"startDate": registered_on} if registered_on else {}),
                },
                "ur_latvia",
                record_kind="beneficial_owner",
            )
        ]
        rel_local_id = f"bo-rel-{regcode}-{bo_id or full_name}"
        yield make_relationship_statement(
            source_id="ur_latvia",
            local_id=rel_local_id,
            subject_statement_id=entity_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )

    # ------------------------------------------------------------------
    # 3.  Officers (board members, representatives, liquidators, etc.)
    # ------------------------------------------------------------------

    officers: list[dict[str, Any]] = bundle.get("officers") or []
    for officer in officers:
        off_id = str(officer.get("id") or "")
        off_name = (officer.get("name") or "").strip()
        if not off_name:
            continue

        entity_type_flag = (officer.get("entity_type") or "NATURAL_PERSON").upper()
        governing_body = (officer.get("governing_body") or "").strip()
        position = (officer.get("position") or "").strip()
        interest_type = _LV_GOVERNING_BODY_INTEREST.get(governing_body, "boardMember")
        registered_on = _lv_date(officer.get("registered_on"))
        last_modified = _lv_date(officer.get("last_modified_at"))

        off_local_id = f"officer-{regcode}-{off_id or off_name}"

        if entity_type_flag == "NATURAL_PERSON":
            off_stmt = make_person_statement(
                source_id="ur_latvia",
                local_id=off_local_id,
                full_name=off_name,
                source_url=source_url,
                statement_date=last_modified or registered_on,
            )
            ip_type = "person"
        else:
            # Corporate officer — create an entity statement.
            corp_regcode = str(officer.get("legal_entity_registration_number") or "").strip()
            corp_identifiers: list[dict[str, str]] = []
            if corp_regcode:
                corp_identifiers.append({
                    "id": corp_regcode,
                    "scheme": "LV-UR",
                    "schemeName": "Latvian Register of Enterprises (UR)",
                })
            off_stmt = make_entity_statement(
                source_id="ur_latvia",
                local_id=f"corp-officer-{regcode}-{off_id}",
                name=off_name,
                jurisdiction=("Latvia", "LV") if corp_regcode else None,
                identifiers=corp_identifiers,
                source_url=source_url,
                statement_date=last_modified or registered_on,
            )
            ip_type = "entity"

        yield off_stmt

        role_desc_parts = list(filter(None, [position, governing_body.replace("_", " ").title()]))
        interests = [
            {
                "type": interest_type,
                "directOrIndirect": "direct",
                # BOC unset ("not stated"): an officer record is not a BO
                # declaration — bo_regimes: ur_latvia/officer -> omit
                # (decision 2026-08-28; was an over-claiming explicit False).
                "details": ", ".join(role_desc_parts) if role_desc_parts else governing_body,
                # registered_on is the UR entry date for this officer record —
                # the closest thing UR gives us to when the role began.
                # last_modified_at is when UR last REVISED the record, which is
                # a declaration date, not a start date: using it here would put
                # clock two into clock one. It goes to statementDate instead.
                **({"startDate": registered_on} if registered_on else {}),
            }
        ]
        rel_local_id = f"officer-rel-{regcode}-{off_id or off_name}"
        yield make_relationship_statement(
            source_id="ur_latvia",
            local_id=rel_local_id,
            subject_statement_id=entity_stmt_id,
            interested_party_statement_id=off_stmt["statementId"],
            interested_party_type=ip_type,
            interests=interests,
            source_url=source_url,
            statement_date=last_modified or registered_on,
        )

    # ------------------------------------------------------------------
    # 4.  SIA shareholders / members
    # ------------------------------------------------------------------

    members: list[dict[str, Any]] = bundle.get("members") or []
    for member in members:
        mem_id = str(member.get("id") or "")
        mem_name = (member.get("name") or "").strip()
        if not mem_name:
            continue

        entity_type_flag = (member.get("entity_type") or "NATURAL_PERSON").upper()
        num_shares = member.get("number_of_shares")
        nominal_value = member.get("share_nominal_value")
        currency = (member.get("share_currency") or "").strip()
        date_from = _lv_date(member.get("date_from"))

        mem_local_id = f"member-{regcode}-{mem_id or mem_name}"

        if entity_type_flag == "NATURAL_PERSON":
            mem_stmt = make_person_statement(
                source_id="ur_latvia",
                local_id=mem_local_id,
                full_name=mem_name,
                source_url=source_url,
            )
            ip_type = "person"
        else:
            corp_regcode = str(member.get("legal_entity_registration_number") or "").strip()
            corp_identifiers_m: list[dict[str, str]] = []
            if corp_regcode:
                corp_identifiers_m.append({
                    "id": corp_regcode,
                    "scheme": "LV-UR",
                    "schemeName": "Latvian Register of Enterprises (UR)",
                })
            mem_stmt = make_entity_statement(
                source_id="ur_latvia",
                local_id=f"corp-member-{regcode}-{mem_id}",
                name=mem_name,
                jurisdiction=("Latvia", "LV") if corp_regcode else None,
                identifiers=corp_identifiers_m,
                source_url=source_url,
            )
            ip_type = "entity"

        yield mem_stmt

        # BOC unset ("not stated"): UR's member list is a registered-holding
        # record, not a BO declaration; the same person's BO record (if any)
        # carries true — bo_regimes: ur_latvia/member_shareholder -> omit
        # (decision 2026-08-28; was an over-claiming explicit False).
        interest: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
        }
        if date_from:
            interest["startDate"] = date_from
        if num_shares is not None:
            share_block: dict[str, Any] = {"exact": int(num_shares)}
            if nominal_value is not None and currency:
                share_block["nominalValue"] = {
                    "amount": float(nominal_value),
                    "currency": currency,
                }
            interest["share"] = share_block

        rel_local_id = f"member-rel-{regcode}-{mem_id or mem_name}"
        yield make_relationship_statement(
            source_id="ur_latvia",
            local_id=rel_local_id,
            subject_statement_id=entity_stmt_id,
            interested_party_statement_id=mem_stmt["statementId"],
            interested_party_type=ip_type,
            interests=[interest],
            source_url=source_url,
        )
