"""Czechia — ARES → BODS.

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
)


# ----------------------------------------------------------------------
# ARES (Czechia) → BODS
# ----------------------------------------------------------------------


def map_ares(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AresAdapter fetch bundle to BODS v0.4 statements.

    Yields, in order:
    1. One entity statement for the registered company.
    2. For each current shareholder / partner (from VR akcionari / spolecnici):
       a person or entity statement, then the OOC relationship statement.
    3. For each current director (from VR statutarniOrgany):
       a person or entity statement, then the OOC relationship statement.

    Ownership data is only present when the entity is registered in the
    Czech commercial register (VR); entities registered only in other
    sub-registers (ROS, RZP, RES, etc.) will have an entity statement only.
    """
    if not bundle or bundle.get("is_stub"):
        return

    ico: str = (bundle.get("cz_ico") or bundle.get("hit_id") or "").strip()
    entity_rec: dict[str, Any] = bundle.get("entity") or {}
    if not ico:
        return

    name: str = (entity_rec.get("name") or bundle.get("name") or "").strip() or f"CZ-{ico}"

    # ----------------------------------------------------------------
    # 1.  Entity statement
    # ----------------------------------------------------------------

    entity_type_label: str = (entity_rec.get("entity_type") or "").strip()
    # Map common Czech legal-form labels to BODS entity type vocabulary.
    _BODS_ENTITY_TYPE: dict[str, str] = {
        "s.r.o.": "registeredEntity",
        "a.s.": "registeredEntity",
        "v.o.s.": "registeredEntity",
        "k.s.": "registeredEntity",
        "state enterprise": "stateBody",
        "organisational unit of state": "stateBody",
        "foundation": "arrangement",
    }
    entity_type: str = "registeredEntity"
    for kw, bods_type in _BODS_ENTITY_TYPE.items():
        if kw.lower() in entity_type_label.lower():
            entity_type = bods_type
            break

    raw_address: str = (entity_rec.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "CZ")] if raw_address else []
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": ico,
            "scheme": "CZ-ICO",
            "schemeName": "Czech ARES (Administrativní registr ekonomických subjektů)",
        }
    ]
    vat = (entity_rec.get("vat_number") or "").strip()
    if vat:
        identifiers.append({
            "id": vat,
            "scheme": "CZ-DIC",
            "schemeName": "Czech DIČ (daňové identifikační číslo)",
        })

    founding_date: str | None = entity_rec.get("incorporation_date")

    source_url: str = (
        entity_rec.get("link")
        or f"https://or.justice.cz/ias/ui/rejstrik-firma.vysledky?subjektId={ico}&typ=PLATNY"
    )

    # datumAktualizace — when ARES last refreshed the record (live-verified
    # 2026-08-14). Distinct from datumVzniku, the founding date carried above
    # as foundingDate.
    ares_last_updated = entity_rec.get("last_updated") or bundle.get("last_updated")

    entity_stmt = make_entity_statement(
        source_id="ares",
        local_id=ico,
        name=name,
        jurisdiction=("Czechia", "CZ"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        entity_type=entity_type,
        source_url=source_url,
        statement_date=ares_last_updated,
    )
    # ARES status, normalised by the adapter's ``_STATUS_MAP`` (Phase 151):
    # active; liquidation (pending); dissolved / dissolved-merger /
    # not-registered (terminal). ``datumZaniku`` is the dissolution date when
    # the adapter passes it through.
    ares_status = (entity_rec.get("status") or "").strip()
    _liveness.apply_register_status(
        entity_stmt,
        source_label=SOURCE_NAMES["ares"],
        liveness=_liveness.classify(
            ares_status,
            live=("active",),
            pending=("liquidation",),
            terminal=("dissolved", "dissolved-merger", "not-registered"),
        ),
        raw=ares_status or None,
        since=str(entity_rec.get("datumZaniku") or "")[:10] or None,
    )
    yield entity_stmt
    entity_stmt_id: str = entity_stmt["statementId"]

    # ----------------------------------------------------------------
    # 2.  Owners (shareholders / partners)
    # ----------------------------------------------------------------

    owners: list[dict[str, Any]] = bundle.get("owners") or []
    for idx, owner in enumerate(owners):
        o_name: str = (owner.get("name") or "").strip()
        if not o_name:
            continue

        role: str = owner.get("role", "shareholder")
        start_date: str | None = owner.get("start_date")
        stake: str | None = owner.get("stake_percent")

        if owner.get("type") == "person":
            given = (owner.get("given_name") or "").strip()
            family = (owner.get("family_name") or "").strip()
            birth_date: str | None = owner.get("birth_date")
            nat_code: str | None = owner.get("nationality")
            nationalities: list[dict[str, str]] = []
            if nat_code:
                try:
                    country = pycountry.countries.get(alpha_2=nat_code)
                    if country:
                        nationalities = [{"name": country.name, "code": nat_code}]
                except Exception:  # noqa: BLE001
                    pass

            person_local_id = f"owner-person-{ico}-{idx}"
            person_stmt = make_person_statement(
                source_id="ares",
                local_id=person_local_id,
                full_name=o_name,
                nationalities=nationalities,
                birth_date=birth_date,
                source_url=source_url,
            )
            yield person_stmt
            interested_party_id = person_stmt["statementId"]
            interested_party_type = "person"

        else:
            # Legal entity shareholder / partner
            o_ico: str | None = owner.get("ico")
            o_country: str | None = owner.get("country")
            o_address: str | None = owner.get("address")

            o_identifiers: list[dict[str, str]] = []
            if o_ico:
                o_identifiers.append({
                    "id": str(o_ico).strip().zfill(8),
                    "scheme": "CZ-ICO",
                    "schemeName": "Czech ARES",
                })

            o_addresses: list[dict[str, Any]] = (
                [_addr("registered", o_address, o_country or "CZ")]
                if o_address
                else []
            )

            o_jur: tuple[str, str] | None = None
            if o_country:
                try:
                    _c = pycountry.countries.get(alpha_2=o_country)
                    o_jur = (_c.name if _c else o_country, o_country)
                except Exception:  # noqa: BLE001
                    o_jur = (o_country, o_country)

            entity_local_id = f"owner-entity-{ico}-{idx}"
            o_entity_stmt = make_entity_statement(
                source_id="ares",
                local_id=entity_local_id,
                name=o_name,
                jurisdiction=o_jur,
                identifiers=o_identifiers,
                addresses=o_addresses,
                entity_type="registeredEntity",
                source_url=source_url,
            )
            yield o_entity_stmt
            interested_party_id = o_entity_stmt["statementId"]
            interested_party_type = "entity"

        # Relationship: owner → subject entity
        interests: list[dict[str, Any]] = []
        interest: dict[str, Any] = {
            "type": "shareholding" if role in ("shareholder", "partner") else "otherInfluenceOrControl",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if stake:
            interest["details"] = f"Stake: {stake}%"
        if start_date:
            interest["startDate"] = start_date
        interests.append(interest)

        rel_local_id = f"owner-rel-{ico}-{idx}"
        yield make_relationship_statement(
            source_id="ares",
            local_id=rel_local_id,
            subject_statement_id=entity_stmt_id,
            interested_party_statement_id=interested_party_id,
            interested_party_type=interested_party_type,
            interests=interests,
            source_url=source_url,
            statement_date=ares_last_updated,
        )

    # ----------------------------------------------------------------
    # 3.  Directors
    # ----------------------------------------------------------------

    directors: list[dict[str, Any]] = bundle.get("directors") or []
    for idx, director in enumerate(directors):
        d_name: str = (director.get("name") or "").strip()
        if not d_name:
            continue

        role_label: str = director.get("role_label") or "Director"
        start_date_d: str | None = director.get("start_date")

        if director.get("type") == "person":
            birth_date_d: str | None = director.get("birth_date")
            nat_code_d: str | None = director.get("nationality")
            nats_d: list[dict[str, str]] = []
            if nat_code_d:
                try:
                    country_d = pycountry.countries.get(alpha_2=nat_code_d)
                    if country_d:
                        nats_d = [{"name": country_d.name, "code": nat_code_d}]
                except Exception:  # noqa: BLE001
                    pass

            dir_local_id = f"director-person-{ico}-{idx}"
            dir_stmt = make_person_statement(
                source_id="ares",
                local_id=dir_local_id,
                full_name=d_name,
                nationalities=nats_d,
                birth_date=birth_date_d,
                source_url=source_url,
            )
        else:
            d_ico: str | None = director.get("ico")
            d_country: str | None = director.get("country")
            d_ids: list[dict[str, str]] = []
            if d_ico:
                d_ids.append({
                    "id": str(d_ico).strip().zfill(8),
                    "scheme": "CZ-ICO",
                    "schemeName": "Czech ARES",
                })
            d_jur: tuple[str, str] | None = None
            if d_country:
                try:
                    _dc = pycountry.countries.get(alpha_2=d_country)
                    d_jur = (_dc.name if _dc else d_country, d_country)
                except Exception:  # noqa: BLE001
                    d_jur = (d_country, d_country)

            dir_local_id = f"director-entity-{ico}-{idx}"
            dir_stmt = make_entity_statement(
                source_id="ares",
                local_id=dir_local_id,
                name=d_name,
                jurisdiction=d_jur,
                identifiers=d_ids,
                entity_type="registeredEntity",
                source_url=source_url,
            )

        yield dir_stmt

        dir_interest: dict[str, Any] = {
            "type": "appointmentOfBoard",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
            "details": role_label,
        }
        if start_date_d:
            dir_interest["startDate"] = start_date_d

        dir_rel_local_id = f"director-rel-{ico}-{idx}"
        yield make_relationship_statement(
            source_id="ares",
            local_id=dir_rel_local_id,
            subject_statement_id=entity_stmt_id,
            interested_party_statement_id=dir_stmt["statementId"],
            interested_party_type="person" if director.get("type") == "person" else "entity",
            interests=[dir_interest],
            source_url=source_url,
            statement_date=ares_last_updated,
        )
