"""Estonia — e-Business Register (ariregister) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ----------------------------------------------------------------------
# Estonian e-Business Register (ariregister) → BODS
# ----------------------------------------------------------------------
#
# Maps a bundle from AriregisterAdapter.fetch() to BODS v0.4 statements.
#
# Emitted statements:
#   1. One entityStatement for the company itself.
#   2. One personStatement  + ownershipOrControlStatement per shareholder
#      (osanikud). For corporate shareholders (isiku_tyyp == "J") with an
#      Estonian registry code, an entityStatement is emitted instead of a
#      personStatement, and the interest type is "shareholding".
#   3. One personStatement  + ownershipOrControlStatement per officer on the
#      registry card (kaardile_kantud_isikud), role-mapped to BODS interest
#      types (boardMember, seniorManagingOfficial, etc.).
#   4. One personStatement  + ownershipOrControlStatement per declared
#      beneficial owner (kasusaajad), interest type "beneficialOwner".
#      These statements are only emitted when the bundle contains BO data
#      (controlled by include_beneficial_owners in the adapter).
#
# Personal identity: since November 2024 the open data files no longer
# contain personal identification numbers (isikukood_registrikood is null
# for natural persons). The `isikukood_hash` UUID field is used as a stable
# cross-file identifier and is surfaced as an identifier with scheme
# "EE-ARIREGISTER-HASH" when present.
#
# Date format in source: DD.MM.YYYY — converted to ISO YYYY-MM-DD here.

_EE_OFFICER_ROLE_MAP: dict[str, tuple[str, str]] = {
    # (BODS interest type, descriptive label)
    "JUHL":   ("boardMember",              "Board member (juhatuse liige)"),
    "PROK":   ("seniorManagingOfficial",   "Procurist (prokurist)"),
    "LIKV":   ("seniorManagingOfficial",   "Liquidator (likvideerija)"),
    "LIKVJ":  ("boardMember",              "Liquidator (board member)"),
    "TOSAN":  ("boardMember",              "General partner (täisosanik)"),
    "UOSAN":  ("boardMember",              "Limited partner (usaldusosanik)"),
    "ASES":   ("seniorManagingOfficial",   "Authorised representative"),
    "SJESI":  ("seniorManagingOfficial",   "Legal representative"),
    "VFILJ":  ("seniorManagingOfficial",   "Branch manager (filiaali juhataja)"),
    "FV":     ("seniorManagingOfficial",   "Fund manager (fondivalitseja)"),
}

# Maps Estonian BO control-mechanism code → (BODS interest type, human-readable detail).
# BODS v0.4 does not have a "beneficialOwner" interest type; BO is expressed via
# beneficialOwnershipOrControl=True on a typed interest.
_EE_BO_CONTROL_MAP: dict[str, tuple[str, str]] = {
    "O": ("shareholding",            "Direct participation"),
    "K": ("otherInfluenceOrControl", "Indirect participation"),
    "H": ("votingRights",            "Through voting rights"),
    "M": ("otherInfluenceOrControl", "Other means"),
    "F": ("otherInfluenceOrControl", "Other means of control or influence"),
}


def _ee_date(s: str | None) -> str | None:
    """Convert an Estonian date string to ISO YYYY-MM-DD.

    Handles two input formats:
    * ``DD.MM.YYYY`` — bulk open-data exports
    * ``YYYY-MM-DD[Z]`` / ``YYYY-MM-DDTHH:MM:SSZ`` — live API responses
    """
    if not s:
        return None
    s = str(s).strip()
    # Already ISO (or API format with optional time/timezone suffix).
    if len(s) >= 10 and s[4] == "-":
        return s[:10].rstrip("Z")
    # Estonian bulk-data: DD.MM.YYYY
    parts = s.split(".")
    if len(parts) == 3:
        d, m, y = parts
        if len(y) == 4 and d.isdigit() and m.isdigit():
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None


def _ee_person_id(person: dict[str, Any]) -> str:
    """Derive a stable local ID for a person record.

    Prefers isikukood_hash (UUID present in all modern records) then falls
    back to combining kirje_id with eesnimi+nimi to avoid collisions.
    """
    h = person.get("isikukood_hash")
    if h:
        return h
    kirje = person.get("kirje_id")
    first = person.get("eesnimi") or ""
    last = person.get("nimi_arinimi") or person.get("nimi") or ""
    return f"{kirje or 'x'}-{first}-{last}"


def _ee_full_name(person: dict[str, Any]) -> str:
    first = (person.get("eesnimi") or "").strip()
    last = (person.get("nimi_arinimi") or person.get("nimi") or "").strip()
    if first and last:
        return f"{first} {last}"
    return last or first or "Unknown"


def map_ariregister(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AriregisterAdapter fetch bundle to BODS v0.4 statements.

    Returns an empty iterable for stub bundles or missing company data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    registry_code: str = bundle.get("registry_code") or ""
    name: str = bundle.get("name") or ""
    if not registry_code or not name:
        return

    source_url = bundle.get("link") or (
        f"https://ariregister.rik.ee/eng/company/{registry_code}"
    )

    # ── 1. Entity statement for the company ─────────────────────────────
    reg_date = bundle.get("registration_date")
    if reg_date and len(reg_date) == 10 and reg_date[4] == "-":
        founding_date = reg_date  # already ISO
    else:
        founding_date = _ee_date(reg_date)

    vat = bundle.get("vat_number") or ""
    identifiers: list[dict[str, str]] = [
        {
            # The registry code (registrikood). EE-KMKR until Phase 239 —
            # but KMKR is Estonia's VAT number, which the next identifier
            # carries; the registry code is the one this mapper already
            # writes as EE-ARIREGISTER for corporate shareholders, and the
            # scheme GLEIF's RA000181 now maps to.
            "id": registry_code,
            "scheme": "EE-ARIREGISTER",
            "schemeName": "Estonian e-Business Register (Äriregister)",
        }
    ]
    if vat:
        identifiers.append({
            "id": vat,
            "scheme": "EE-KMKR",
            "schemeName": "Estonian VAT number (Äriregister)",
        })

    address_str = bundle.get("address") or ""
    addresses = (
        [_addr("registered", address_str, "EE")]
        if address_str
        else []
    )

    company_stmt = make_entity_statement(
        source_id="ariregister",
        local_id=registry_code,
        name=name,
        jurisdiction=("Estonia", "EE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # e-Business Register status label as shown on the company page
    # (Phase 151): "Entered into the register" is live; liquidation /
    # bankruptcy are pending; "Deleted from the register" is terminal.
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["ariregister"],
        liveness=_liveness.classify(
            bundle.get("status"),
            live=("Entered into the register", "Registered"),
            pending=("In liquidation", "Liquidation", "Bankrupt", "In bankruptcy", "Being deleted"),
            terminal=("Deleted from the register", "Deleted", "Removed from the register"),
        ),
        raw=bundle.get("status") or None,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. Shareholders ──────────────────────────────────────────────────
    seen_person_ids: set[str] = set()

    for sh in bundle.get("shareholders") or []:
        isiku_tyyp = sh.get("isiku_tyyp") or "F"
        pct_str = sh.get("osaluse_protsent") or ""
        share_size = sh.get("osaluse_suurus") or ""
        currency = sh.get("osaluse_valuuta") or ""
        start_date = _ee_date(sh.get("algus_kpv"))
        end_date = _ee_date(sh.get("lopp_kpv"))
        kirje_id = str(sh.get("kirje_id") or "")

        interests: list[dict[str, Any]] = [{"type": "shareholding"}]
        try:
            pct = float(pct_str) if pct_str else None
        except ValueError:
            pct = None
        if pct is not None:
            # BODS v0.4: ``share.exclusiveMinimum``/``exclusiveMaximum`` are
            # *numbers* (exclusive percentage bounds), not booleans as in v0.3.
            # With an exact value known, the inclusive bounds suffice.
            interests[0]["share"] = {
                "exact": pct,
                "minimum": pct,
                "maximum": pct,
            }
        if share_size:
            interests[0]["details"] = (
                f"Share value: {share_size} {currency}".strip()
            )
        if start_date:
            interests[0]["startDate"] = start_date
        if end_date:
            interests[0]["endDate"] = end_date

        if isiku_tyyp == "J":
            # Corporate shareholder — emit an entity statement
            corp_code = sh.get("isikukood_registrikood") or ""
            corp_name = (sh.get("nimi_arinimi") or "").strip()
            if not corp_name:
                continue
            corp_local_id = corp_code if corp_code else f"sh-corp-{kirje_id}"
            corp_ids: list[dict[str, str]] = []
            if corp_code:
                corp_ids.append({
                    "id": corp_code,
                    "scheme": "EE-ARIREGISTER",
                    "schemeName": "Estonian e-Business Register",
                })
            corp_stmt = make_entity_statement(
                source_id="ariregister",
                local_id=corp_local_id,
                name=corp_name,
                jurisdiction=("Estonia", "EE") if corp_code else None,
                identifiers=corp_ids,
                source_url=(
                    f"https://ariregister.rik.ee/eng/company/{corp_code}"
                    if corp_code
                    else None
                ),
            )
            yield corp_stmt
            yield make_relationship_statement(
                source_id="ariregister",
                local_id=f"sh-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=corp_stmt["statementId"],
                interested_party_type="entity",
                interests=interests,
                source_url=source_url,
            )
        else:
            # Natural person shareholder
            person_id = _ee_person_id(sh)
            full_name = _ee_full_name(sh)
            if not full_name or full_name == "Unknown":
                continue
            birth_date = _ee_date(sh.get("synniaeg"))
            country_code = sh.get("valis_kood_riik") or ""
            nationalities = (
                [{"code": country_code}]
                if country_code and country_code not in ("XXX", "EST")
                else []
            )
            p_ids: list[dict[str, str]] = []
            if sh.get("isikukood_hash"):
                p_ids.append({
                    "id": sh["isikukood_hash"],
                    "scheme": "EE-ARIREGISTER-HASH",
                    "schemeName": "Estonian e-Business Register person hash",
                })
            if person_id not in seen_person_ids:
                person_stmt = make_person_statement(
                    source_id="ariregister",
                    local_id=person_id,
                    full_name=full_name,
                    nationalities=nationalities,
                    birth_date=birth_date,
                    identifiers=p_ids,
                    source_url=source_url,
                )
                yield person_stmt
                seen_person_ids.add(person_id)
            else:
                person_stmt = {
                    "statementId": _stable_id("ariregister", "person", person_id)
                }
            yield make_relationship_statement(
                source_id="ariregister",
                local_id=f"sh-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=person_stmt["statementId"],
                interested_party_type="person",
                interests=interests,
                source_url=source_url,
            )

    # ── 3. Officers (kaardile_kantud_isikud) ─────────────────────────────
    for officer in bundle.get("officers") or []:
        role_code = officer.get("isiku_roll") or ""
        if role_code not in _EE_OFFICER_ROLE_MAP:
            continue  # skip roles we don't map (e.g. KISIK contact, ORP share registrar)
        interest_type, role_label = _EE_OFFICER_ROLE_MAP[role_code]
        start_date = _ee_date(officer.get("algus_kpv"))
        end_date = _ee_date(officer.get("lopp_kpv"))
        kirje_id = str(officer.get("kirje_id") or "")

        interests = [{"type": interest_type, "details": role_label}]
        if start_date:
            interests[0]["startDate"] = start_date
        if end_date:
            interests[0]["endDate"] = end_date

        person_id = _ee_person_id(officer)
        full_name = _ee_full_name(officer)
        if not full_name or full_name == "Unknown":
            continue

        birth_date = _ee_date(officer.get("synniaeg"))
        country_code = officer.get("valis_kood_riik") or ""
        nationalities = (
            [{"code": country_code}]
            if country_code and country_code not in ("XXX", "EST")
            else []
        )
        p_ids = []
        if officer.get("isikukood_hash"):
            p_ids.append({
                "id": officer["isikukood_hash"],
                "scheme": "EE-ARIREGISTER-HASH",
                "schemeName": "Estonian e-Business Register person hash",
            })
        if person_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="ariregister",
                local_id=person_id,
                full_name=full_name,
                nationalities=nationalities,
                birth_date=birth_date,
                identifiers=p_ids,
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_id)
        else:
            person_stmt = {
                "statementId": _stable_id("ariregister", "person", person_id)
            }
        yield make_relationship_statement(
            source_id="ariregister",
            local_id=f"off-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )

    # ── 4. Beneficial owners (kasusaajad) ─────────────────────────────────
    # NOTE: Include only while Estonian law makes this data publicly available.
    # Set include_beneficial_owners=False in the adapter call to suppress.
    for bo in bundle.get("beneficial_owners") or []:
        kirje_id = str(bo.get("kirje_id") or "")
        start_date = _ee_date(bo.get("algus_kpv"))
        end_date = _ee_date(bo.get("lopp_kpv"))
        control_code = bo.get("kontrolli_teostamise_viis") or ""
        interest_type, control_label = _EE_BO_CONTROL_MAP.get(
            control_code, ("otherInfluenceOrControl", "")
        )

        # kasusaajad records ARE the Estonian BO declarations — the flag
        # comes from the regimes registry (ariregister/kasusaaja_bo -> true).
        interest: dict[str, Any] = set_beneficial_ownership(
            {"type": interest_type}, "ariregister", record_kind="kasusaaja_bo"
        )
        if control_label:
            interest["details"] = control_label
        if start_date:
            interest["startDate"] = start_date
        if end_date:
            interest["endDate"] = end_date
        interests: list[dict[str, Any]] = [interest]

        first = (bo.get("eesnimi") or "").strip()
        last = (bo.get("nimi") or "").strip()
        full_name = f"{first} {last}".strip() if first or last else ""
        if not full_name:
            continue

        person_id = _ee_person_id({
            "isikukood_hash": bo.get("isikukood_hash"),
            "kirje_id": kirje_id,
            "eesnimi": first,
            "nimi_arinimi": last,
        })
        birth_date = _ee_date(bo.get("synniaeg"))
        country_code = bo.get("valis_kood_riik") or ""
        res_country = bo.get("aadress_riik") or ""
        nationalities = (
            [{"code": country_code}]
            if country_code and country_code not in ("XXX",)
            else []
        )
        addresses = (
            [_addr("residence", "", res_country)]
            if res_country
            else []
        )
        p_ids = []
        if bo.get("isikukood_hash"):
            p_ids.append({
                "id": bo["isikukood_hash"],
                "scheme": "EE-ARIREGISTER-HASH",
                "schemeName": "Estonian e-Business Register person hash",
            })
        if person_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="ariregister",
                local_id=person_id,
                full_name=full_name,
                nationalities=nationalities,
                birth_date=birth_date,
                addresses=addresses,
                identifiers=p_ids,
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_id)
        else:
            person_stmt = {
                "statementId": _stable_id("ariregister", "person", person_id)
            }
        yield make_relationship_statement(
            source_id="ariregister",
            local_id=f"bo-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )
