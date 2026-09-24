"""Slovakia — RPO (legal entities) and RPVS (public-sector partners) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

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
# RPO Slovakia (Register právnických osôb) → BODS
# ----------------------------------------------------------------------


def map_rpo_slovakia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an RpoSlovakiaAdapter fetch bundle to a BODS v0.4 entity statement.

    Only entity-level data is available from RPO (name, IČO, address,
    establishment date, registration number, court).  No beneficial
    ownership or officer data is available via this API; the RPVS adapter
    provides public-procurement beneficial ownership data separately.

    Identifiers emitted:
      • IČO  — scheme "SK-RPO"
    """
    if not bundle or bundle.get("is_stub"):
        return

    ico: str = (bundle.get("sk_ico") or bundle.get("hit_id") or "").strip()
    if not ico:
        return

    name: str = (bundle.get("name") or "").strip() or f"SK-{ico}"

    # Address — pre-formatted string from the RPO response.
    raw_address: str = (bundle.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "SK")]
        if raw_address
        else []
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": ico,
            "scheme": "SK-RPO",
            "schemeName": "Register právnických osôb (Slovak Register of Legal Persons)",
        }
    ]

    # Registration number in source register (e.g. Obchodný register number).
    reg_numbers: list[str] = bundle.get("registration_numbers") or []
    if reg_numbers:
        identifiers.append({
            "id": reg_numbers[0],
            "scheme": "SK-OR",
            "schemeName": "Obchodný register SR (Slovak Commercial Register)",
        })

    founding_date: str | None = bundle.get("establishment")

    source_url: str = bundle.get("link") or f"https://rpo.statistics.sk/"

    rpo_entity = make_entity_statement(
        source_id="rpo_slovakia",
        local_id=ico,
        name=name,
        jurisdiction=("Slovakia", "SK"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        entity_type="registeredEntity",
        source_url=source_url,
    )
    # ``termination`` is the RPO's dissolution date (null = active); the
    # adapter derives ``status`` from it (Phase 151).
    rpo_termination = bundle.get("termination") or None
    _liveness.apply_register_status(
        rpo_entity,
        source_label=SOURCE_NAMES["rpo_slovakia"],
        liveness=_liveness.TERMINAL if rpo_termination else _liveness.LIVE,
        raw=bundle.get("status"),
        since=rpo_termination,
    )
    yield rpo_entity


# ---------------------------------------------------------------------------
# RPVS Slovakia
# ---------------------------------------------------------------------------


def map_rpvs_slovakia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an RpvsSlovakiaAdapter fetch bundle to BODS v0.4 statements.

    Produces:
    * One **entity statement** for the public sector partner (identified by IČO).
    * One **person or entity statement** per active KUV (beneficial owner).
    * One **ownership-or-control relationship statement** per active KUV,
      linking the KUV to the partner entity.

    The RPVS does not disclose the specific mechanism of beneficial ownership
    (share percentage, voting rights, etc.) — only that the named individual
    or entity is the KUV of this public sector partner.  Interest type is
    therefore ``unknownInterest`` with ``beneficialOwnershipOrControl: true``
    and a details note explaining the RPVS declaration.

    Identifiers emitted:
    • Entity  — scheme "SK-RPVS" (IČO within the RPVS context)
    • Persons  — no external scheme available from the API
    """
    if not bundle or bundle.get("is_stub"):
        return

    ico: str = (bundle.get("sk_ico") or bundle.get("hit_id") or "").strip()
    if not ico:
        return

    partner_id: int | None = bundle.get("partner_id")
    name: str = (bundle.get("name") or "").strip() or f"SK-RPVS-{ico}"
    source_url: str = bundle.get("link") or "https://rpvs.gov.sk/rpvs"

    # ------------------------------------------------------------------
    # 1. Entity statement for the public sector partner
    # ------------------------------------------------------------------
    entity_stmt = make_entity_statement(
        source_id="rpvs_slovakia",
        local_id=ico,
        name=name,
        jurisdiction=("Slovakia", "SK"),
        identifiers=[
            {
                "id": ico,
                "scheme": "SK-RPVS",
                "schemeName": (
                    "Register partnerov verejného sektora "
                    "(Slovak Public Sector Partners Register)"
                ),
            }
        ],
        source_url=source_url,
    )
    yield entity_stmt
    entity_sid = entity_stmt["statementId"]

    # ------------------------------------------------------------------
    # 2 & 3. Person/entity statements + relationship statements for KUVs
    # ------------------------------------------------------------------
    active_kuvs: list[dict[str, Any]] = bundle.get("active_kuvs") or []

    # Fall back to all KUVs if active list is empty (e.g. all have PlatnostDo).
    if not active_kuvs:
        active_kuvs = bundle.get("kuvs") or []

    for kuv in active_kuvs:
        kuv_id_raw: int = kuv.get("Id") or 0
        kuv_ico: str | None = None
        raw_ico = (kuv.get("Ico") or "").strip()
        if raw_ico:
            # Zero-pad to 8 digits if it looks like a numeric IČO.
            try:
                kuv_ico = str(int(raw_ico)).zfill(8)
            except ValueError:
                kuv_ico = raw_ico

        is_legal_person: bool = bool(kuv.get("ObchodneMeno") and not kuv.get("Meno"))

        # --- 2. Interested party statement (person or entity) ---
        if is_legal_person:
            lp_name = (kuv.get("ObchodneMeno") or "").strip() or f"KUV-{kuv_id_raw}"
            ip_identifiers = []
            if kuv_ico:
                ip_identifiers.append({
                    "id": kuv_ico,
                    "scheme": "SK-RPO",
                    "schemeName": "Register právnických osôb (Slovak Register of Legal Persons)",
                })

            ip_stmt = make_entity_statement(
                source_id="rpvs_slovakia",
                local_id=f"kuv_entity:{kuv_id_raw}",
                name=lp_name,
                jurisdiction=("Slovakia", "SK"),
                identifiers=ip_identifiers,
                source_url=source_url,
            )
            ip_type = "entity"
        else:
            # Natural person KUV
            first = (kuv.get("Meno") or "").strip()
            last = (kuv.get("Priezvisko") or "").strip()
            full_name = " ".join(p for p in [
                kuv.get("TitulPred", ""), first, last, kuv.get("TitulZa", "")
            ] if p and p.strip()) or f"KUV-{kuv_id_raw}"

            person_dob_raw: str | None = kuv.get("DatumNarodenia")
            person_dob: str | None = None
            if person_dob_raw:
                # DateTimeOffset like "1969-12-15T00:00:00+01:00"
                person_dob = person_dob_raw[:10]

            nationalities = []
            obcanstvo = kuv.get("Obcanstvo") or kuv.get("statObcanstva")
            if obcanstvo:
                nationalities.append({"name": obcanstvo})

            addresses = []
            adresa = kuv.get("Adresa")
            if adresa:
                addresses.append(_addr("service", adresa))

            is_pep: bool = bool(kuv.get("JeVerejnyCinitel"))
            pep_exposure: dict[str, Any] | None = (
                {
                    "status": "isPep",
                    "details": [{"type": "existingRelationship", "jurisdiction": {"name": "Slovakia", "code": "SK"}}],
                }
                if is_pep
                else None
            )

            ip_stmt = make_person_statement(
                source_id="rpvs_slovakia",
                local_id=f"kuv_person:{kuv_id_raw}",
                full_name=full_name,
                person_type="knownPerson",
                nationalities=nationalities,
                birth_date=person_dob,
                addresses=addresses,
                source_url=source_url,
                political_exposure=pep_exposure,
            )
            ip_type = "person"

        yield ip_stmt
        ip_sid = ip_stmt["statementId"]

        # --- 3. Ownership-or-control relationship statement ---
        kuv_valid_from: str | None = (kuv.get("PlatnostOd") or "")[:10] or None
        kuv_valid_to: str | None = (kuv.get("PlatnostDo") or "")[:10] or None

        # KUV records are verified Slovak BO declarations — the flag comes
        # from the regimes registry (rpvs_slovakia/kuv -> true).
        interest: dict[str, Any] = set_beneficial_ownership(
            {
                "type": "unknownInterest",
                "directOrIndirect": "unknown",
                "details": (
                    "Disclosed as konečný užívateľ výhod (KUV) in the Slovak "
                    "Public Sector Partners Register (RPVS).  The specific "
                    "mechanism of beneficial ownership is not published."
                ),
            },
            "rpvs_slovakia",
            record_kind="kuv",
        )
        if kuv_valid_from or kuv_valid_to:
            interest["startDate"] = kuv_valid_from
            if kuv_valid_to:
                interest["endDate"] = kuv_valid_to

        rel_stmt = make_relationship_statement(
            source_id="rpvs_slovakia",
            local_id=f"rel:{kuv_id_raw}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=ip_sid,
            interested_party_type=ip_type,
            interests=[interest],
            source_url=source_url,
        )
        yield rel_stmt
