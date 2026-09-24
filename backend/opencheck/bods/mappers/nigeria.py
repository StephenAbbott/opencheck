"""Nigeria — CAC Persons with Significant Control → BODS v0.4.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _country_obj,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ----------------------------------------------------------------------
# Nigeria CAC — Persons with Significant Control → BODS v0.4
# ----------------------------------------------------------------------


def _cac_interests(psc: dict[str, Any], record_kind: str) -> list[dict[str, Any]]:
    """Map the five statutory CAMA PSC conditions to BODS interest types.

    ``record_kind`` routes beneficialOwnershipOrControl through the regimes
    registry (bo_regimes.py): ``psc_natural_person`` -> true (the ultimate
    beneficial owners), ``psc_corporate`` -> false (legal owners in the chain
    — BODS guidance: do not assert beneficial ownership on an entity party).
    """
    out: list[dict[str, Any]] = []

    def _valid(v: Any) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool) and 0 < v <= 100

    start = psc.get("notified") or None

    def _mk(
        itype: str,
        pct_direct: Any = None,
        pct_indirect: Any = None,
        details: str | None = None,
    ) -> dict[str, Any]:
        # CAMA's conditions read "directly or indirectly", so the flag alone
        # says neither. Only a usable percentage in one column says which;
        # without one the interest is "unknown", never assumed direct.
        # A percentage over 100 (NIPCO files 120%) is carried as no share.
        if _valid(pct_direct):
            how, share_val = "direct", pct_direct
        elif _valid(pct_indirect):
            how, share_val = "indirect", pct_indirect
        else:
            how, share_val = "unknown", None
        i: dict[str, Any] = set_beneficial_ownership(
            {"type": itype, "directOrIndirect": how},
            "cac_nigeria",
            record_kind=record_kind,
        )
        if share_val is not None:
            i["share"] = {"exact": share_val}
        if details:
            i["details"] = details
        if start:
            i["startDate"] = start
        return i

    if psc.get("shares"):
        out.append(_mk(
            "shareholding", psc.get("share_pct_direct"), psc.get("share_pct_indirect"),
        ))
    if psc.get("voting"):
        out.append(_mk(
            "votingRights", psc.get("voting_pct_direct"), psc.get("voting_pct_indirect"),
        ))
    if psc.get("appoint_board"):
        out.append(_mk("appointmentOfBoard"))
    if psc.get("sig_influence_company"):
        out.append(_mk(
            "otherInfluenceOrControl",
            details="Significant influence or control over the company/LLP (CAMA condition 4)",
        ))
    if psc.get("sig_influence_trust_firm"):
        out.append(_mk(
            "otherInfluenceOrControl",
            details="Significant influence or control over a trust or firm (CAMA condition 5)",
        ))
    if not out:
        out.append(set_beneficial_ownership(
            {"type": "unknownInterest", "directOrIndirect": "unknown"},
            "cac_nigeria",
            record_kind=record_kind,
        ))
    return out


def _cac_merge_interests(lists: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Combine interests from several PSC rows for the same (subject, owner):
    dedupe by (type, details), keeping the variant with the largest share."""
    best: dict[tuple[str, str | None], dict[str, Any]] = {}
    order: list[tuple[str, str | None]] = []
    for lst in lists:
        for i in lst:
            key = (i["type"], i.get("details"))
            if key not in best:
                best[key] = i
                order.append(key)
            else:
                new_s = i.get("share", {}).get("exact")
                old_s = best[key].get("share", {}).get("exact")
                if new_s is not None and (old_s is None or new_s > old_s):
                    best[key] = i
    return [best[k] for k in order]


def _cac_owner_statements(
    *,
    source_id: str,
    id_prefix: str,
    rc: str,
    subject_id: str,
    pscs: list[dict[str, Any]],
    source_url: str,
    statement_date: str | None = None,
) -> Iterable[dict[str, Any]]:
    """Owners and relationships for a set of CAC PSC rows — the one grouping
    both ``map_cac_nigeria`` and the ``eiti_bo`` Nigeria path use (Phase 231).

    ``eiti_bo`` used to carry its own copy of the pre-Phase-213 grouping, so
    after the CAC re-harvest the same company showed two different pictures
    on one report: superseded INACTIVE filings merged into current ownership,
    and a blank-name owner silently dropped. One function means they cannot
    drift again.

    ``id_prefix`` keeps each source's local ids as they were (``""`` for
    ``cac_nigeria``, ``"ng:"`` for ``eiti_bo``), so every statementId either
    source published before is unchanged.

    The register keeps every filing with a status. A row is current when it
    is ACTIVE (or carries no status — the pre-Phase-213 index shape). An
    owner with any current row gets one relationship built from its current
    rows only; its earlier INACTIVE filings are superseded declarations of
    the same holding, not a second one. An owner with no current row left
    the register: one ``closed`` relationship from all its rows. The CAC
    publishes no cessation date, so none is invented (no ``endDate``). A
    filing with no name at all gets its own ``unknownEntity`` party rather
    than being dropped.
    """
    def _is_current(psc: dict[str, Any]) -> bool:
        return (psc.get("psc_status") or "ACTIVE").upper() == "ACTIVE"

    groups: dict[str, dict[str, Any]] = {}
    for psc in pscs:
        owner = (psc.get("owner_name") or "").strip()
        if owner:
            key = f"named:{owner}"
        elif psc.get("owner_named") is False:
            # A corporate PSC whose name the register does not publish: its
            # own party per row — two blank rows are not known to be one owner.
            key = f"unnamed:{psc.get('psc_id')}"
        else:
            continue
        kind = psc.get("owner_kind") or "entity"
        g = groups.setdefault(key, {
            "owner": owner, "kind": kind, "psc": psc, "rows": [],
        })
        g["rows"].append(psc)

    emitted: set[str] = set()
    for key, g in groups.items():
        owner = g["owner"]
        kind = g["kind"]
        psc = g["psc"]
        owner_rc = psc.get("owner_rc") or None
        juris = psc.get("owner_jurisdiction") or None
        current_rows = [r for r in g["rows"] if _is_current(r)]
        closed = not current_rows
        rows = g["rows"] if closed else current_rows
        record_kind = "psc_natural_person" if kind == "person" else "psc_corporate"
        interests = _cac_merge_interests([
            _cac_interests(r, record_kind=record_kind) for r in rows
        ])

        if not owner:
            local_id = f"{id_prefix}entity:{key}:{rc}"
            if local_id not in emitted:
                yield make_entity_statement(
                    source_id=source_id,
                    local_id=local_id,
                    name=_CAC_UNNAMED_OWNER,
                    entity_type="unknownEntity",
                    entity_details=_CAC_UNNAMED_DETAILS,
                    source_url=source_url,
                    statement_date=statement_date,
                )
                emitted.add(local_id)
            ip_id = _stable_id(source_id, "entity", local_id)
        elif kind == "person":
            local_id = f"{id_prefix}person:{owner}"
            nationalities = []
            nat = psc.get("nationality") or ""
            co = _country_obj(nat) if nat else None
            if co:
                nationalities = [co]
            if local_id not in emitted:
                yield make_person_statement(
                    source_id=source_id,
                    local_id=local_id,
                    full_name=owner,
                    nationalities=nationalities,
                    source_url=source_url,
                    statement_date=statement_date,
                )
                emitted.add(local_id)
            ip_id = _stable_id(source_id, "person", local_id)
        else:
            entity_type = {
                "arrangement": "arrangement",
                "unknown": "unknownEntity",
            }.get(kind, "registeredEntity")
            local_id = f"{id_prefix}entity:{owner_rc or owner}"
            idents = []
            if owner_rc:
                idents = [{
                    "id": str(owner_rc),
                    "scheme": "NG-CAC",
                    "schemeName": "Nigeria Corporate Affairs Commission",
                }]
            if local_id not in emitted:
                yield make_entity_statement(
                    source_id=source_id,
                    local_id=local_id,
                    name=owner,
                    jurisdiction=("Nigeria", "NG") if juris == "NG" else None,
                    identifiers=idents,
                    entity_type=entity_type,
                    source_url=source_url,
                    statement_date=statement_date,
                )
                emitted.add(local_id)
            ip_id = _stable_id(source_id, "entity", local_id)

        # The pre-Phase-213 local id (``rc:owner``) is kept for a named current
        # owner, so its relationship statementId is unchanged by the rebuild.
        rel_local = f"{id_prefix}{rc}:{owner}" if owner else f"{id_prefix}{rc}:{key}"
        if closed:
            rel_local += ":closed"
        yield make_relationship_statement(
            source_id=source_id,
            local_id=rel_local,
            subject_statement_id=subject_id,
            interested_party_statement_id=ip_id,
            interested_party_type="person" if kind == "person" else "entity",
            interests=interests,
            source_url=source_url,
            statement_date=statement_date,
            record_status="closed" if closed else "new",
        )


def map_cac_nigeria(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CacNigeriaAdapter bundle to BODS v0.4 statements.

    Yields one entityStatement for the Nigerian company, and per beneficial
    owner (person / entity / arrangement / unknown) a person- or
    entityStatement plus an ownership-or-control relationshipStatement. Owners
    are deduped by canonical name so a shared owner (e.g. Dangote Industries)
    reuses one statement. The five CAMA PSC conditions map to BODS interest
    types (see ``_cac_interests``).

    Phase 213: an owner whose every filing is INACTIVE or CEASED gets a
    ``closed`` relationship, and a filing with no name at all gets its own
    ``unknownEntity`` party rather than being dropped.
    """
    if not bundle or bundle.get("is_stub"):
        return

    record: dict[str, Any] = bundle.get("record") or {}
    rc: str = str(record.get("rc") or "")
    name: str = (record.get("company") or "").strip()
    if not name or not rc:
        return

    source_url = "https://bor.cac.gov.ng"
    cac_id = {
        "id": rc,
        "scheme": "NG-CAC",
        "schemeName": "Nigeria Corporate Affairs Commission",
    }

    # ── 1. Subject company entity statement ───────────────────────────────
    subject_stmt = make_entity_statement(
        source_id="cac_nigeria",
        local_id=rc,
        name=name,
        jurisdiction=("Nigeria", "NG"),
        identifiers=[cac_id],
        source_url=source_url,
    )
    # CAC public-search status (Phase 151). "ACTIVE" is live. "INACTIVE" at
    # the CAC means annual returns are outstanding, not that the company has
    # ceased, so it is deliberately left unclassified rather than read as
    # dissolved.
    _liveness.apply_register_status(
        subject_stmt,
        source_label=SOURCE_NAMES["cac_nigeria"],
        liveness=_liveness.classify(
            record.get("status"), live=("ACTIVE",), terminal=("DISSOLVED", "STRUCK OFF", "WOUND UP")
        ),
        raw=(record.get("status") or "").strip() or None,
    )
    yield subject_stmt
    subject_id: str = subject_stmt["statementId"]

    # ── 2. Owners + relationships (shared with eiti_bo — Phase 231) ──────
    yield from _cac_owner_statements(
        source_id="cac_nigeria",
        id_prefix="",
        rc=rc,
        subject_id=subject_id,
        pscs=record.get("pscs") or [],
        source_url=source_url,
    )


#: Display name and entityType details for a PSC row with every name field
#: blank. ``unknownEntity``, not ``anonymousEntity``: the gap is in what the
#: register's public API returns, not a withholding by the company, so it must
#: not reach the opaque-ownership risk signal (risk.py fires on anonymous* only).
_CAC_UNNAMED_OWNER = "Unnamed corporate owner"
_CAC_UNNAMED_DETAILS = (
    "The CAC register publishes this PSC filing with every name field blank; "
    "the filing's governing-law and register fields are those of a corporate PSC."
)
