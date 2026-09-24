"""Pooled EITI national beneficial ownership registers (eiti_bo) → BODS v0.4.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from ..annotations import annotate, commenting, pointer, transformation
from ..statements import (
    _addr,
    _country_obj,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)
from .nigeria import _cac_owner_statements


# ----------------------------------------------------------------------
# Pooled EITI national BO registers (eiti_bo) → BODS v0.4
# ----------------------------------------------------------------------

#: French nationality adjectives (as filed in the ITIE-RDC register, incl.
#: observed typos) → ISO 3166-1 alpha-2. Values the map misses fall back to a
#: name-only Country object so nothing is silently lost.
_DRC_NATIONALITIES: dict[str, str] = {
    "CHINOISE": "CN",
    "CHINOSE": "CN",  # register typo
    "INDIENNE": "IN",
    "INDIA": "IN",
    "CONGOLAISE": "CD",
    "CONGOLAISE (RDC)": "CD",
    "CANADIENNE": "CA",
    "LIBANAISE": "LB",
    "BELGE": "BE",
    "TAIWANAISE": "TW",
    "GRECQUE": "GR",
    "SUD AFRICAINE": "ZA",
    "TURQUE": "TR",
    "ANGLAISE": "GB",
    "SÉNÉGALAISE": "SN",
    "FRANÇAISE": "FR",
}

_DRC_HONORIFIC_RE = re.compile(r"^(?:MR|MRS|MS|MME|M|HON|DR)\s*\.?\s+", re.I)

#: BODS v0.2 interest types (as published by the Armenian register) → v0.4
#: codelist. Identity for codes that survived unchanged; renames per the
#: v0.4 changelog.
_BODS02_INTEREST_TYPES: dict[str, str] = {
    "shareholding": "shareholding",
    "voting-rights": "votingRights",
    "votingRights": "votingRights",
    "appointment-of-board": "appointmentOfBoard",
    "appointmentOfBoard": "appointmentOfBoard",
    "senior-managing-official": "seniorManagingOfficial",
    "seniorManagingOfficial": "seniorManagingOfficial",
    "other-influence-or-control": "otherInfluenceOrControl",
    "otherInfluenceOrControl": "otherInfluenceOrControl",
    "rights-to-surplus-assets-on-dissolution": "rightsToSurplusAssetsOnDissolution",
    "rights-to-profit-or-income": "rightsToProfitOrIncome",
    "rightsToProfitOrIncome": "rightsToProfitOrIncome",
    "influence-or-control": "otherInfluenceOrControl",
    "unknownInterest": "unknownInterest",
}


def _eiti_bo_date(value: Any) -> str | None:
    """Best-effort ISO date from register date strings (DD/MM/YYYY or ISO)."""
    s = str(value or "").strip()
    if not s:
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def map_eiti_bo(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a pooled EITI BO registers bundle to BODS v0.4 statements.

    The pooled index carries one register-specific payload per record;
    each register gets its own sub-mapper:

    * ``drc_itie`` — tabular owner rows from the ITIE-RDC XLSX export.
    * ``armenia_eregister`` — the register's own **BODS v0.2** declaration,
      upconverted statement-by-statement to v0.4 (deterministic IDs; the
      original statement IDs, dates and values are preserved via annotations
      where the transformation is lossy or non-obvious).
    * ``nigeria_cac`` — CAC PSC rows (the cac_nigeria shape), NEITI
      solid-minerals subset; reuses the CAC interest mapping.
    """
    if not bundle or bundle.get("is_stub"):
        return
    record: dict[str, Any] = bundle.get("record") or {}
    register_id = str(record.get("register_id") or "")
    if register_id == "drc_itie":
        yield from _eiti_bo_map_drc(bundle, record)
    elif register_id == "armenia_eregister":
        yield from _eiti_bo_map_armenia(bundle, record)
    elif register_id == "nigeria_cac":
        yield from _eiti_bo_map_nigeria(bundle, record)


def _eiti_bo_map_drc(
    bundle: dict[str, Any], record: dict[str, Any]
) -> Iterable[dict[str, Any]]:
    comp: dict[str, Any] = record.get("drc") or {}
    nif = str((record.get("local_ids") or {}).get("cd_nif") or comp.get("nif") or "")
    name = (comp.get("name") or record.get("company") or "").strip()
    if not name:
        return
    source_url = bundle.get("register_url") or "https://www.itierdc.net/donnees/"
    statement_date = record.get("source_date")  # register export date (ISO)

    identifiers = []
    if nif:
        identifiers.append({
            "id": nif,
            "scheme": "CD-NIF",
            "schemeName": "DRC Numéro d'Identification Fiscale (NIF)",
        })
    subject_stmt = make_entity_statement(
        source_id="eiti_bo",
        local_id=f"drc:{nif or name}",
        name=name,
        jurisdiction=("Congo, The Democratic Republic of the", "CD"),
        identifiers=identifiers,
        alternate_names=[comp["acronym"]] if comp.get("acronym") else [],
        source_url=source_url,
        statement_date=statement_date,
    )
    annotate(
        subject_stmt,
        commenting(
            pointer("recordDetails"),
            "From the ITIE-RDC Registre des propriétaires effectifs "
            f"(register export dated {statement_date}); sector "
            f"{comp.get('sector') or 'unknown'}.",
        ),
    )
    yield subject_stmt
    subject_id: str = subject_stmt["statementId"]

    fraction = comp.get("pct_semantics") == "fraction-of-1"
    for owner in comp.get("owners") or []:
        raw_name = (owner.get("name") or "").strip()
        if not raw_name:
            continue
        display = _DRC_HONORIFIC_RE.sub("", raw_name).strip() or raw_name

        nationalities = []
        nat_fr = (owner.get("nationality_fr") or "").strip().upper()
        if nat_fr:
            code = _DRC_NATIONALITIES.get(nat_fr)
            co = _country_obj(code) if code else {"name": nat_fr.title()}
            if co:
                nationalities.append(co)

        pep_exposure: dict[str, Any] | None = None
        if owner.get("pep"):
            detail: dict[str, Any] = {
                "jurisdiction": {
                    "name": "Congo, The Democratic Republic of the",
                    "code": "CD",
                },
            }
            if owner.get("pep_role"):
                detail["reason"] = str(owner["pep_role"])
            pep_exposure = {"status": "isPep", "details": [detail]}

        local_id = f"drc:person:{nif}:{raw_name}"
        person_stmt = make_person_statement(
            source_id="eiti_bo",
            local_id=local_id,
            full_name=display,
            nationalities=nationalities,
            source_url=source_url,
            statement_date=statement_date,
            political_exposure=pep_exposure,
        )
        if display != raw_name:
            annotate(
                person_stmt,
                transformation(
                    pointer("recordDetails", "names", 0, "fullName"),
                    f"Register publishes the name as “{raw_name}”; honorific "
                    "prefix removed by OpenCheck.",
                    transformed_content=display,
                ),
            )
        yield person_stmt

        interests: list[dict[str, Any]] = []
        start = _eiti_bo_date(owner.get("acquired"))

        def _mk(itype: str, share: Any = None, details: str | None = None) -> dict[str, Any]:
            i: dict[str, Any] = {
                "type": itype,
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
            }
            if isinstance(share, (int, float)) and 0 < share <= 100:
                i["share"] = {"exact": share}
            if details:
                i["details"] = details
            if start:
                i["startDate"] = start
            return i

        if owner.get("pct_shares"):
            interests.append(_mk("shareholding", owner["pct_shares"]))
        if owner.get("pct_voting"):
            interests.append(_mk("votingRights", owner["pct_voting"]))
        if owner.get("control_type"):
            interests.append(_mk(
                "seniorManagingOfficial",
                details=f"Type de contrôle (as filed): {owner['control_type']}",
            ))
        if not interests:
            interests.append({
                "type": "unknownInterest",
                "directOrIndirect": "unknown",
                "beneficialOwnershipOrControl": True,
            })

        rel_stmt = make_relationship_statement(
            source_id="eiti_bo",
            local_id=f"drc:{nif}:{raw_name}",
            subject_statement_id=subject_id,
            interested_party_statement_id=person_stmt["statementId"],
            interests=interests,
            source_url=source_url,
            statement_date=statement_date,
        )
        # The register mixes share semantics per filer (fractions of 1 vs
        # literal percentages) — annotate what the register actually said.
        for idx, i in enumerate(interests):
            share = i.get("share", {}).get("exact")
            raw = owner.get("pct_shares_raw") if i["type"] == "shareholding" else (
                owner.get("pct_voting_raw") if i["type"] == "votingRights" else None
            )
            if share is not None and raw is not None:
                annotate(
                    rel_stmt,
                    transformation(
                        pointer("recordDetails", "interests", idx, "share", "exact"),
                        f"Register value {raw!r} read as "
                        f"{'a fraction of 1' if fraction else 'a literal percentage'} "
                        "(per-company sum heuristic; see build_eiti_bo_index.py).",
                        transformed_content=str(share),
                    ),
                )
        yield rel_stmt


def _eiti_bo_map_armenia(
    bundle: dict[str, Any], record: dict[str, Any]
) -> Iterable[dict[str, Any]]:
    arm: dict[str, Any] = record.get("armenia") or {}
    statements: list[dict[str, Any]] = arm.get("bods_v02") or []
    if not statements:
        return
    source_url = arm.get("declaration_url") or bundle.get("register_url") or (
        "https://old.e-register.am/"
    )
    decl_date = (arm.get("declaration_date") or "")[:10] or None
    decl_uuid = arm.get("declaration_uuid")

    # Map original v0.2 statementIDs → upconverted v0.4 statementIds so
    # relationship references stay intact.
    id_map: dict[str, str] = {}
    for s in statements:
        orig = str(s.get("statementID") or "")
        kind = s.get("statementType")
        if not orig:
            continue
        if kind == "entityStatement":
            id_map[orig] = _stable_id("eiti_bo", "entity", f"am:{orig}")
        elif kind == "personStatement":
            id_map[orig] = _stable_id("eiti_bo", "person", f"am:{orig}")

    regnum = (record.get("local_ids") or {}).get("am_regnum")
    tin = (record.get("local_ids") or {}).get("am_tin")
    subject_annotated = False

    for s in statements:
        kind = s.get("statementType")
        orig = str(s.get("statementID") or "")
        stmt_date = (s.get("statementDate") or "")[:10] or decl_date

        if kind == "entityStatement":
            names = [s.get("name") or ""] + list(s.get("alternateNames") or [])
            names = [n for n in names if n]
            primary = names[0] if names else f"AM-{orig[:8]}"
            identifiers = [
                {"id": str(i.get("id")), "scheme": str(i.get("scheme"))}
                for i in (s.get("identifiers") or [])
                if i.get("id") and i.get("scheme")
            ]
            addresses = [
                _addr(
                    str(a.get("type") or "registered"),
                    str(a.get("address") or ""),
                    str(a.get("country") or ""),
                )
                for a in (s.get("addresses") or [])
                if a.get("address")
            ]
            is_subject = not subject_annotated and _norm_or_empty(primary) and (
                primary == (record.get("company") or "")
                or orig
                == _eiti_bo_armenia_subject_orig_id(statements)
            )
            if is_subject:
                # The declaring company: assert the identifiers the register
                # publishes on its company page as well.
                if regnum:
                    identifiers.append({
                        "id": str(regnum),
                        "scheme": "AM-REG",
                        "schemeName": "Armenia State Register registration number",
                    })
                if tin:
                    identifiers.append({
                        "id": str(tin),
                        "scheme": "AM-TIN",
                        "schemeName": "Armenia taxpayer identification number (ՀՎՀՀ)",
                    })
            stmt = make_entity_statement(
                source_id="eiti_bo",
                local_id=f"am:{orig}",
                name=primary,
                jurisdiction=("Armenia", "AM") if is_subject else None,
                identifiers=identifiers,
                alternate_names=names[1:],
                addresses=addresses,
                source_url=source_url,
                statement_date=stmt_date,
            )
            annotate(
                stmt,
                commenting(
                    pointer("recordDetails"),
                    "Upconverted by OpenCheck from the register's BODS v0.2 "
                    f"statement {orig} (declaration {decl_uuid}, "
                    f"approved {decl_date}).",
                ),
            )
            if is_subject:
                subject_annotated = True
            yield stmt

        elif kind == "personStatement":
            names02 = s.get("names") or []
            legal = next(
                (n for n in names02 if n.get("type") in (None, "individual", "legal")),
                names02[0] if names02 else {},
            )
            full_name = (legal.get("fullName") or "").strip() or f"AM-{orig[:8]}"
            nationalities = []
            for n in s.get("nationalities") or []:
                co = _country_obj(str(n.get("code") or n.get("name") or ""))
                if co:
                    nationalities.append(co)
            birth = (s.get("birthDate") or "").strip() or None
            birth_out = None
            if birth:
                # Register publishes full dates of birth; OpenCheck truncates
                # to YYYY-MM (the UK PSC convention) and says so.
                birth_out = birth[:7] if len(birth) >= 7 else birth
            pep_exposure = None
            if s.get("hasPepStatus"):
                pep_exposure = {"status": "isPep", "details": s.get("pepStatusDetails") or []}
            stmt = make_person_statement(
                source_id="eiti_bo",
                local_id=f"am:{orig}",
                full_name=full_name,
                nationalities=nationalities,
                birth_date=birth_out,
                source_url=source_url,
                statement_date=stmt_date,
                political_exposure=pep_exposure,
            )
            # Preserve the register's own transliteration entries (the factory
            # only auto-transliterates Cyrillic/Greek, not Armenian script).
            existing = {
                n.get("fullName") for n in stmt["recordDetails"].get("names", [])
            }
            for n in names02:
                if n.get("type") == "transliteration" and n.get("fullName") not in existing:
                    stmt["recordDetails"]["names"].append({
                        "type": "transliteration",
                        "fullName": str(n.get("fullName")),
                    })
            if birth and birth_out != birth:
                annotate(
                    stmt,
                    commenting(
                        pointer("recordDetails", "birthDate"),
                        "Register publishes a full date of birth; truncated "
                        "to year-month by OpenCheck.",
                    ),
                )
            # Residential addresses are published by the register but omitted
            # here (data-minimisation; matches the cac_nigeria precedent).
            yield stmt

        elif kind == "ownershipOrControlStatement":
            subj02 = (s.get("subject") or {}).get("describedByEntityStatement")
            subject_id = id_map.get(str(subj02 or ""))
            ip02 = s.get("interestedParty") or {}
            ip_id = id_map.get(
                str(
                    ip02.get("describedByPersonStatement")
                    or ip02.get("describedByEntityStatement")
                    or ""
                )
            )
            if not subject_id:
                continue
            interests_out: list[dict[str, Any]] = []
            for i in s.get("interests") or []:
                itype02 = str(i.get("type") or "unknownInterest")
                itype = _BODS02_INTEREST_TYPES.get(itype02)
                out: dict[str, Any] = {
                    "type": itype or "otherInfluenceOrControl",
                    "directOrIndirect": (
                        str(i.get("interestLevel") or "unknown")
                        if str(i.get("interestLevel")) in ("direct", "indirect")
                        else "unknown"
                    ),
                    "beneficialOwnershipOrControl": bool(
                        i.get("beneficialOwnershipOrControl")
                    ),
                }
                if itype is None:
                    out["details"] = f"BODS v0.2 interest type (as filed): {itype02}"
                share = (i.get("share") or {}).get("exact")
                try:
                    share_f = float(share) if share is not None else None
                except (TypeError, ValueError):
                    share_f = None
                if share_f is not None and 0 < share_f <= 100:
                    out["share"] = {"exact": share_f}
                if i.get("startDate"):
                    out["startDate"] = str(i["startDate"])[:10]
                if i.get("endDate"):
                    out["endDate"] = str(i["endDate"])[:10]
                if i.get("details"):
                    out["details"] = str(i["details"])
                interests_out.append(out)

            kwargs: dict[str, Any] = {}
            if ip_id:
                kwargs["interested_party_statement_id"] = ip_id
                kwargs["interested_party_type"] = (
                    "person" if ip02.get("describedByPersonStatement") else "entity"
                )
            else:
                unspec = ip02.get("unspecified") or {
                    "reason": "unknown",
                    "description": "Interested party not resolvable from the v0.2 declaration",
                }
                kwargs["interested_party_unspecified"] = unspec
            yield make_relationship_statement(
                source_id="eiti_bo",
                local_id=f"am:{orig}",
                subject_statement_id=subject_id,
                interests=interests_out,
                source_url=source_url,
                statement_date=stmt_date,
                **kwargs,
            )


def _norm_or_empty(value: str | None) -> str:
    return (value or "").strip()


def _eiti_bo_armenia_subject_orig_id(statements: list[dict[str, Any]]) -> str | None:
    """Original v0.2 statementID of the declaring (root subject) entity."""
    subj: list[str] = []
    ip: set[str] = set()
    ents: set[str] = {
        str(s.get("statementID"))
        for s in statements
        if s.get("statementType") == "entityStatement"
    }
    for s in statements:
        if s.get("statementType") != "ownershipOrControlStatement":
            continue
        sid = (s.get("subject") or {}).get("describedByEntityStatement")
        if sid:
            subj.append(str(sid))
        pid = (s.get("interestedParty") or {}).get("describedByEntityStatement")
        if pid:
            ip.add(str(pid))
    for sid in dict.fromkeys(subj):
        if sid not in ip and sid in ents:
            return sid
    return subj[0] if subj else None


def _eiti_bo_map_nigeria(
    bundle: dict[str, Any], record: dict[str, Any]
) -> Iterable[dict[str, Any]]:
    """CAC PSC rows for the NEITI solid-minerals subset — the cac_nigeria
    owner grouping (``_cac_owner_statements``), with the (dated) NEITI filter
    evidence annotated on the subject so the extractives scoping is
    auditable."""
    cac_record: dict[str, Any] = record.get("nigeria") or {}
    rc: str = str(cac_record.get("rc") or "")
    name: str = (cac_record.get("company") or "").strip()
    if not name or not rc:
        return
    source_url = bundle.get("register_url") or "https://bor.cac.gov.ng"
    statement_date = (record.get("source_date") or "")[:10] or None

    subject_stmt = make_entity_statement(
        source_id="eiti_bo",
        local_id=f"ng:{rc}",
        name=name,
        jurisdiction=("Nigeria", "NG"),
        identifiers=[{
            "id": rc,
            "scheme": "NG-CAC",
            "schemeName": "Nigeria Corporate Affairs Commission",
        }],
        source_url=source_url,
        statement_date=statement_date,
    )
    evidence = record.get("neiti_filter_evidence")
    if evidence:
        annotate(
            subject_stmt,
            commenting(
                pointer("recordDetails"),
                f"In the EITI pool as a NEITI-covered extractive company: {evidence}",
            ),
        )
    yield subject_stmt
    subject_id: str = subject_stmt["statementId"]

    # Phase 231: the same grouping as the cac_nigeria card beside it —
    # current vs closed owners, unnamed corporate owners kept — so one report
    # cannot show two different CAC pictures of the same company.
    yield from _cac_owner_statements(
        source_id="eiti_bo",
        id_prefix="ng:",
        rc=rc,
        subject_id=subject_id,
        pscs=cac_record.get("pscs") or [],
        source_url=source_url,
        statement_date=statement_date,
    )
