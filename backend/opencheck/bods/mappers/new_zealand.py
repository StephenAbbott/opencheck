"""New Zealand — Companies Office register → BODS.

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


def _nz_ids(nzbn: str | None, number: str | None) -> list[dict[str, str]]:
    """Build BODS identifiers for an NZ entity from its NZBN and/or company number."""
    ids: list[dict[str, str]] = []
    if nzbn:
        ids.append({
            "id": nzbn, "scheme": "NZ-NZBN", "schemeName": "New Zealand Business Number",
        })
    if number:
        ids.append({
            "id": number, "scheme": "NZ-COH", "schemeName": "New Zealand Companies Register",
        })
    return ids


def map_nz_companies(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an NzCompaniesAdapter bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the New Zealand company (NZBN + company number).
    * Per director (``roles``): a person/entity statement + a
      ``seniorManagingOfficial`` relationshipStatement.
    * Per shareholder: a person/entity statement + a ``shareholding``
      relationshipStatement carrying ``share.exact`` (the allocation's percent).
    * The ultimate holding company, if any: an entityStatement + an
      ``otherInfluenceOrControl`` (indirect) relationshipStatement.
    """
    if not bundle or bundle.get("is_stub"):
        return

    number: str = str(bundle.get("nz_company_number") or "")
    nzbn: str = str(bundle.get("nzbn") or "")
    company: dict[str, Any] = bundle.get("company") or {}
    name: str = (
        (company.get("name") or "").strip()
        or bundle.get("legal_name")
        or (f"NZBN {nzbn}" if nzbn else "")
    )
    if not name or not (number or nzbn):
        return

    source_url = bundle.get("link") or (f"https://www.nzbn.govt.nz/mynzbn/nzbndetails/{nzbn}/" if nzbn else None)
    local_company_id = number or nzbn

    alt_names = list(company.get("trading_names") or []) + list(company.get("previous_names") or [])

    # ── 1. Company entity statement ───────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="nz_companies",
        local_id=local_company_id,
        name=name,
        jurisdiction=("New Zealand", "NZ"),
        identifiers=_nz_ids(nzbn, number),
        founding_date=company.get("founding_date"),
        addresses=([_addr("registered", company["address"], "NZ")] if company.get("address") else []),
        alternate_names=[a for a in alt_names if a],
        entity_details=company.get("entity_type"),
        source_url=source_url,
    )
    # NZBN entity status (Phase 151): "Registered" is live; the insolvency
    # states are pending; "Removed" is the register's terminal state. The
    # lookup bundle carries no status date (the entity-statuses history is
    # fetched only for the Time Machine), so none is stated.
    nz_status = (company.get("status") or "").strip()
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["nz_companies"],
        liveness=_liveness.classify(
            nz_status,
            live=("registered",),
            pending=(
                "in liquidation",
                "in receivership",
                "in voluntary administration",
                "in statutory management",
                "registered (in liquidation)",
                "registered (in receivership)",
            ),
            terminal=("removed", "struck off", "deregistered", "amalgamated"),
        ),
        raw=nz_status or None,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    seen: set[str] = set()

    def _emit_party(kind: str, name_: str, *, nzbn_: str | None = None,
                    number_: str | None = None, idx: int = 0) -> tuple[str, str]:
        """Emit a person/entity statement (deduped) and return (ip_id, ip_type)."""
        if kind == "entity":
            local = nzbn_ or number_ or f"{local_company_id}:e:{idx}"
            if local not in seen:
                yield_stmt = make_entity_statement(
                    source_id="nz_companies", local_id=local, name=name_,
                    jurisdiction=("New Zealand", "NZ"),
                    identifiers=_nz_ids(nzbn_, number_), source_url=source_url,
                )
                _emit_party.pending.append(yield_stmt)
                seen.add(local)
            return _stable_id("nz_companies", "entity", local), "entity"
        local = f"{local_company_id}:p:{name_}"
        if local not in seen:
            yield_stmt = make_person_statement(
                source_id="nz_companies", local_id=local, full_name=name_, source_url=source_url,
            )
            _emit_party.pending.append(yield_stmt)
            seen.add(local)
        return _stable_id("nz_companies", "person", local), "person"

    _emit_party.pending = []  # type: ignore[attr-defined]

    rel_idx = 0

    # ── 2. Directors / role-holders → seniorManagingOfficial ──────────────
    for i, role in enumerate(bundle.get("roles") or []):
        rname = (role.get("name") or "").strip()
        if not rname:
            continue
        ip_id, ip_type = _emit_party(
            role.get("kind", "person"), rname, nzbn_=role.get("nzbn"), idx=i,
        )
        for s in _emit_party.pending:
            yield s
        _emit_party.pending = []  # type: ignore[attr-defined]
        interest: dict[str, Any] = {
            "type": "seniorManagingOfficial",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if role.get("role_type"):
            interest["details"] = role["role_type"]
        if role.get("start"):
            interest["startDate"] = role["start"]
        if role.get("end"):
            interest["endDate"] = role["end"]
        yield make_relationship_statement(
            source_id="nz_companies", local_id=f"{local_company_id}:rel:{rel_idx}",
            subject_statement_id=company_stmt_id, interested_party_statement_id=ip_id,
            interested_party_type=ip_type, interests=[interest], source_url=source_url,
        )
        rel_idx += 1

    # ── 3. Shareholders → shareholding (with share.exact) ─────────────────
    for i, sh in enumerate(bundle.get("shareholders") or []):
        sname = (sh.get("name") or "").strip()
        if not sname:
            continue
        kind = sh.get("kind", "person")
        ip_id, ip_type = _emit_party(
            kind, sname, nzbn_=sh.get("nzbn"), number_=sh.get("company_number"), idx=1000 + i,
        )
        for s in _emit_party.pending:
            yield s
        _emit_party.pending = []  # type: ignore[attr-defined]
        # Being a natural person is not a beneficial-ownership declaration.
        # NZ files share allocations; whether the holder benefits is a separate
        # question the register does not answer. (Open: whether NZBN exposes a
        # beneficiallyHeld-equivalent — needs a live probe.)
        interest = {
            "type": "shareholding",
            "directOrIndirect": "direct",
        }
        set_beneficial_ownership(interest, "nz_companies")
        if sh.get("percent") is not None:
            interest["share"] = {"exact": sh["percent"]}
        if sh.get("jointly_held"):
            interest["details"] = "Jointly held"
        if sh.get("start"):
            interest["startDate"] = sh["start"]
        yield make_relationship_statement(
            source_id="nz_companies", local_id=f"{local_company_id}:rel:{rel_idx}",
            subject_statement_id=company_stmt_id, interested_party_statement_id=ip_id,
            interested_party_type=ip_type, interests=[interest], source_url=source_url,
        )
        rel_idx += 1

    # ── 4. Ultimate holding company → otherInfluenceOrControl (indirect) ──
    uhc = bundle.get("ultimate_holding_company")
    if isinstance(uhc, dict) and (uhc.get("name") or "").strip():
        ip_id, ip_type = _emit_party(
            "entity", uhc["name"].strip(), nzbn_=uhc.get("nzbn"),
            number_=uhc.get("number"), idx=9000,
        )
        for s in _emit_party.pending:
            yield s
        _emit_party.pending = []  # type: ignore[attr-defined]
        yield make_relationship_statement(
            source_id="nz_companies", local_id=f"{local_company_id}:rel:{rel_idx}",
            subject_statement_id=company_stmt_id, interested_party_statement_id=ip_id,
            interested_party_type=ip_type,
            interests=[{
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "indirect",
                "beneficialOwnershipOrControl": False,
                "details": "Ultimate holding company",
            }],
            source_url=source_url,
        )
