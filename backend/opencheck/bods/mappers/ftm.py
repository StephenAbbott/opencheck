"""FollowTheMoney (OpenSanctions / OpenAleph) → BODS.

Split out of ``mapper.py`` in Phase 168, unchanged, together with the
vendored FtM edge-schema table it carries — a model-driven map of every
schema that declares edge properties, which is the largest single block of
data in the mapping layer and has nothing to do with any other source.
"""

from __future__ import annotations

from typing import Any

import pycountry

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    BODSBundle,
    _addr,
    _country_code,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)

# ----------------------------------------------------------------------
# FtM (OpenSanctions / OpenAleph) → BODS
# ----------------------------------------------------------------------
#
# FollowTheMoney (FtM) is the shared schema behind both OpenSanctions
# and OpenAleph. For Phase 2 we map the search-time properties into a
# single-statement BODS bundle: one entity or person statement with
# whatever cross-identifiers FtM carried. Ownership relationships
# embedded in richer FtM payloads (Ownership/Directorship interval
# schemas) get picked up when their child entities are present via
# ``related_entities``.

# FtM schemas we treat as "entity-like" rather than "person-like".
_FTM_ENTITY_SCHEMAS = {
    "Company",
    "Organization",
    "LegalEntity",
    "PublicBody",
    "Asset",
    "Airplane",
    "Vessel",
}
_FTM_PERSON_SCHEMAS = {"Person"}

# FtM topics (sanction, role.pep, etc.) are intentionally NOT converted into
# BODS interests here — they are risk signals handled by the risk engine, not
# ownership or control relationships.

# Role strings from Directorship/Employment FtM entities → BODS interest types.
# Only the most common values are listed; anything not found defaults to
# "seniorManagingOfficial".
_FTM_ROLE_TO_INTEREST_TYPE: dict[str, str] = {
    "board member": "boardMember",
    "boardmember": "boardMember",
    "director": "boardMember",
    "board chair": "boardChair",
    "boardchair": "boardChair",
    "chair": "boardChair",
    "chairman": "boardChair",
    "ceo": "seniorManagingOfficial",
    "cfo": "seniorManagingOfficial",
    "coo": "seniorManagingOfficial",
    "president": "seniorManagingOfficial",
    "trustee": "trustee",
    "protector": "protector",
    "nominee": "nominee",
    "beneficiary": "beneficiaryOfLegalArrangement",
    "settlor": "settlor",
}


def _ftm_percentage(op: dict[str, Any]) -> float | None:
    """Parse ``percentage`` from a FtM Ownership properties dict."""
    raw = (op.get("percentage") or [None])[0]
    if raw is None:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


# ----------------------------------------------------------------------
# Vendored FtM edge-schema table (model-driven edge handling)
# ----------------------------------------------------------------------
# Vendored from followthemoney 4.10.0: every schema declared with
# ``edge: true`` in the FtM model, with its declared source/target property
# names. Nested API payloads (yente ``GET /entities/{id}``) embed these edge
# entities under the *reverse* property names on adjacent entities — on BOTH
# sides of the edge. A Company carries Ownership edges under
# ``ownershipOwner``/``ownershipAsset``; a Person carries Directorship edges
# under ``directorshipDirector``, Family under ``familyPerson``, Associate
# under ``associates``, Occupancy under ``positionOccupancies``. The mapper
# must therefore recognise an edge entity wherever it appears and never treat
# the edge wrapper itself as a party (doing so emitted phantom entity
# statements named after edge captions, e.g. "director of Acme Ltd").
#
# Drift guard: ``scripts/check_ftm_edges.py`` (CI: vendored-enum-drift.yml,
# ``ftm-edges`` job) compares this table against the installed followthemoney
# model, so upstream schema changes fail the build instead of silently
# desynchronising — same pattern as the vendored CH enums.
#
# The third element is OpenCheck's own BODS mapping policy, not FtM data:
#   "ownership"      → shareholding (+ percentage → share.exact)
#   "directorship"   → role-based interest (seniorManagingOfficial default)
#   "membership"     → otherInfluenceOrControl
#   "representation" → nominee (the agent acts for the client)
#   "unknown"        → unknownInterest
#   None             → no BODS relationship statement. Family/Associate are
#                      screening context, not ownership-or-control; Occupancy
#                      is consumed by person_check (PEP positions); the rest
#                      are out of BODS scope.
_FTM_EDGE_SCHEMAS: dict[str, tuple[str, str, str | None]] = {
    # schema:           (source_prop,   target_prop,    bods_policy)
    "Ownership":         ("owner",       "asset",        "ownership"),
    "Directorship":      ("director",    "organization", "directorship"),
    "Membership":        ("member",      "organization", "membership"),
    "Representation":    ("agent",       "client",       "representation"),
    "UnknownLink":       ("subject",     "object",       "unknown"),
    "Family":            ("person",      "relative",     None),
    "Associate":         ("person",      "associate",    None),
    "Employment":        ("employee",    "employer",     None),
    "Occupancy":         ("holder",      "post",         None),
    "Succession":        ("predecessor", "successor",    None),
    "Payment":           ("payer",       "beneficiary",  None),
    "Debt":              ("debtor",      "creditor",     None),
    "ContractAward":     ("contract",    "supplier",     None),
    "CourtCaseParty":    ("party",       "case",         None),
    "Documentation":     ("document",    "entity",       None),
    "ProjectParticipant": ("participant", "project",     None),
}




# FtM datasets whose ``Ownership`` edges express genuine *beneficial* ownership
# rather than legal/registered ownership. FtM ``Ownership`` is by default a
# registered-ownership relation (a company-registry shareholder, a GLEIF RR
# parent) — NOT necessarily a beneficial owner — so OpenSanctions'
# followthemoney-graph makes no BO claim when loading it, and neither should
# we by default. We only assert ``beneficialOwnershipOrControl: true`` when the
# edge (or its subject entity) carries a dataset that publishes actual
# beneficial-ownership declarations. Conservative allow-list — extend only with
# a dataset whose ownership edges are demonstrably beneficial ownership.
#   * ``openownership`` — the Open Ownership register republished in FtM
#     (https://www.opensanctions.org/datasets/openownership/); its entire
#     purpose is beneficial-ownership data.
_FTM_BO_ASSERTING_DATASETS: frozenset[str] = frozenset({"openownership"})


def _ftm_asserts_beneficial_ownership(datasets: set[str]) -> bool:
    """True when a FtM ownership edge's datasets assert *beneficial* ownership."""
    return bool(datasets & _FTM_BO_ASSERTING_DATASETS)


def _ftm_edge_interest(
    policy: str, edge_props: dict[str, Any], datasets: set[str] | None = None
) -> dict[str, Any]:
    """Build the BODS interest dict for a mapped FtM edge entity.

    ``datasets`` is the set of FtM dataset names carried by the edge / subject;
    it decides whether an ``Ownership`` edge may assert
    ``beneficialOwnershipOrControl`` (see ``_FTM_BO_ASSERTING_DATASETS``).
    """
    role = (edge_props.get("role") or [""])[0] or ""
    if policy == "ownership":
        # ``directOrIndirect: "direct"`` is retained: FtM ownership edges are
        # direct links in the graph (the immediate holder), even when the BO
        # claim itself is unknown.
        interest: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
        }
        # Only claim beneficial ownership when the source dataset actually
        # publishes it; otherwise leave the flag unset ("not stated") rather
        # than asserting a registered holding is a beneficial one.
        if _ftm_asserts_beneficial_ownership(datasets or set()):
            interest["beneficialOwnershipOrControl"] = True
        pct = _ftm_percentage(edge_props)
        if pct is not None:
            interest["share"] = {"exact": pct}
        return interest

    if policy == "directorship":
        interest = {
            "type": _FTM_ROLE_TO_INTEREST_TYPE.get(
                role.lower().strip(), "seniorManagingOfficial"
            )
        }
        if role:
            interest["details"] = role
    elif policy == "membership":
        interest = {"type": "otherInfluenceOrControl", "details": role or "FtM Membership"}
    elif policy == "representation":
        interest = {"type": "nominee"}
        if role:
            interest["details"] = role
    else:  # "unknown"
        interest = {"type": "unknownInterest"}
        if role:
            interest["details"] = role

    start = (edge_props.get("startDate") or [None])[0]
    if start:
        interest["startDate"] = start
    end = (edge_props.get("endDate") or [None])[0]
    if end:
        interest["endDate"] = end
    return interest


_FTM_SUBJECT = object()  # sentinel: this edge endpoint is the payload subject


def _ftm_edge_relationships(
    payload: dict[str, Any],
    subject_sid: str,
    subject_type: str,
    source_id: str,
    source_url_builder: Any,
    result: BODSBundle,
) -> None:
    """Map nested FtM *edge entities* to BODS relationship statements.

    Scans every property value of ``payload`` for dicts whose ``schema`` is in
    the vendored ``_FTM_EDGE_SCHEMAS`` table (rather than hand-listing reverse
    property keys), then resolves the edge's declared source/target sides:

    * a nested party dict on a side → its own entity/person statement;
    * the subject's own id (string or re-nested dict), or an otherwise-empty
      side opposite a populated one → the subject statement (yente renders the
      subject's side of an edge as a bare string id, or omits it);
    * edge schemas with policy ``None`` emit nothing — and, crucially, the
      edge wrapper itself is never mistaken for a party.

    Direction rule: the BODS ``interestedParty`` is the edge *source*
    (owner / director / member / agent) and the BODS ``subject`` is the edge
    *target* (asset / organization / client) — this matches FtM edge
    semantics for every mapped schema.
    """
    subject_ftm_id = payload.get("id")
    props = payload.get("properties") or {}
    subject_url: str | None = (
        source_url_builder(payload.get("id", ""))
        if callable(source_url_builder)
        else None
    )

    seen_edge_ids: set[str] = set()
    for values in props.values():
        if not isinstance(values, list):
            continue
        for entry in values:
            if not isinstance(entry, dict):
                continue
            schema = entry.get("schema") or ""
            spec = _FTM_EDGE_SCHEMAS.get(schema)
            if spec is None:
                continue
            source_prop, target_prop, policy = spec
            edge_id = entry.get("id")
            if edge_id:
                if edge_id in seen_edge_ids:
                    continue  # same edge nested under two property keys
                seen_edge_ids.add(edge_id)
            if policy is None:
                continue
            edge_props = entry.get("properties") or {}

            def _side(prop_name: str) -> tuple[list[dict[str, Any]], bool]:
                parties: list[dict[str, Any]] = []
                has_subject = False
                for value in edge_props.get(prop_name) or []:
                    if isinstance(value, dict):
                        if value.get("id") and value.get("id") == subject_ftm_id:
                            has_subject = True
                        elif (value.get("schema") or "") in _FTM_EDGE_SCHEMAS:
                            continue  # never treat an edge entity as a party
                        else:
                            parties.append(value)
                    elif isinstance(value, str) and value == subject_ftm_id:
                        has_subject = True
                return parties, has_subject

            src_parties, src_is_subject = _side(source_prop)
            tgt_parties, tgt_is_subject = _side(target_prop)
            # yente often omits the subject's side entirely — default an
            # empty side to the subject when the opposite side has parties.
            if not src_parties and not src_is_subject and tgt_parties:
                src_is_subject = True
            if not tgt_parties and not tgt_is_subject and src_parties:
                tgt_is_subject = True

            src_nodes: list[Any] = list(src_parties)
            if src_is_subject:
                src_nodes.append(_FTM_SUBJECT)
            tgt_nodes: list[Any] = list(tgt_parties)
            if tgt_is_subject:
                tgt_nodes.append(_FTM_SUBJECT)
            if not src_nodes or not tgt_nodes:
                continue

            edge_datasets = set(entry.get("datasets") or []) | set(
                payload.get("datasets") or []
            )
            interest = _ftm_edge_interest(policy, edge_props, edge_datasets)

            for src in src_nodes:
                for tgt in tgt_nodes:
                    if src is _FTM_SUBJECT and tgt is _FTM_SUBJECT:
                        continue
                    src_ref = (
                        subject_ftm_id if src is _FTM_SUBJECT else src.get("id", "?")
                    )
                    tgt_ref = (
                        subject_ftm_id if tgt is _FTM_SUBJECT else tgt.get("id", "?")
                    )
                    if src_ref == tgt_ref:
                        continue

                    if src is _FTM_SUBJECT:
                        ip_sid, ip_type = subject_sid, subject_type
                    else:
                        stmt = _ftm_statement(
                            src,
                            source_id=source_id,
                            source_url_builder=source_url_builder,
                        )
                        if stmt is None:
                            continue
                        result.statements.append(stmt)
                        ip_sid = stmt["statementId"]
                        ip_type = (
                            "entity" if stmt["recordType"] == "entity" else "person"
                        )
                    if tgt is _FTM_SUBJECT:
                        subj_sid = subject_sid
                    else:
                        stmt = _ftm_statement(
                            tgt,
                            source_id=source_id,
                            source_url_builder=source_url_builder,
                        )
                        if stmt is None:
                            continue
                        result.statements.append(stmt)
                        subj_sid = stmt["statementId"]

                    # Composite local id: stays stable per edge AND unique per
                    # endpoint pair when one edge fans out to several parties.
                    local_id = f"{edge_id or schema.lower()}:{src_ref}:{tgt_ref}"
                    rel = make_relationship_statement(
                        source_id=source_id,
                        local_id=local_id,
                        subject_statement_id=subj_sid,
                        interested_party_statement_id=ip_sid,
                        interested_party_type=ip_type,
                        interests=[dict(interest)],
                        source_url=subject_url,
                    )
                    result.statements.append(rel)


def map_ftm(
    payload: dict[str, Any],
    *,
    source_id: str,
    source_url_builder: Any = None,
) -> BODSBundle:
    """Map a FtM-shaped entity payload (OpenSanctions/OpenAleph) to BODS.

    ``payload`` is the single FtM record (the ``entity`` block from the
    adapter's ``fetch`` output, or a hit's ``raw``). ``source_url_builder``
    is an optional callable ``(ftm_id) -> url`` for populating the BODS
    source block.
    """
    result = BODSBundle()

    subject = _ftm_statement(
        payload, source_id=source_id, source_url_builder=source_url_builder
    )
    if subject is None:
        return result
    result.statements.append(subject)
    subject_sid = subject["statementId"]
    subject_type = "entity" if subject["recordType"] == "entity" else "person"

    # --- Legacy flat-property relationships (older FtM API shape) -----------
    # Some sources still use flat property arrays (``ownersOf``, ``owners``)
    # whose entries are the related party dicts themselves. Handle them here.
    # Anything whose entries are nested *edge entities* (Ownership,
    # Directorship, Associate, ...) is handled generically by
    # _ftm_edge_relationships below via the vendored _FTM_EDGE_SCHEMAS table —
    # do NOT add reverse-property keys like ``directorshipDirector`` back to
    # this table (doing so emitted phantom entities named after edge captions).
    props = payload.get("properties") or {}
    legacy_control_props = {
        "ownersOf": "shareholding",
        "owners": "shareholding",
    }
    for key, interest_type in legacy_control_props.items():
        for related in props.get(key) or []:
            # FtM emits either string IDs or nested entity dicts.
            if not isinstance(related, dict):
                continue
            if (related.get("schema") or "") in _FTM_EDGE_SCHEMAS:
                continue  # edge entity — handled by _ftm_edge_relationships
            related_stmt = _ftm_statement(
                related,
                source_id=source_id,
                source_url_builder=source_url_builder,
            )
            if related_stmt is None:
                continue
            result.statements.append(related_stmt)
            related_type = "entity" if related_stmt["recordType"] == "entity" else "person"

            # When the FtM property expresses "owner of X", the related
            # record is the *subject* and `payload` is the interested party.
            if key == "ownersOf":
                rel_subject_sid = related_stmt["statementId"]
                rel_ip_sid = subject_sid
                rel_ip_type = subject_type
            else:
                rel_subject_sid = subject_sid
                rel_ip_sid = related_stmt["statementId"]
                rel_ip_type = related_type

            legacy_interest: dict[str, Any] = {
                "type": interest_type,
                "directOrIndirect": "direct",
                "details": f"FtM property '{key}'",
            }
            # As with nested Ownership edges: only assert beneficial ownership
            # when the dataset publishes it, otherwise leave the flag unset.
            legacy_datasets = set(payload.get("datasets") or []) | set(
                related.get("datasets") or []
            )
            if interest_type == "shareholding" and _ftm_asserts_beneficial_ownership(
                legacy_datasets
            ):
                legacy_interest["beneficialOwnershipOrControl"] = True
            rel = make_relationship_statement(
                source_id=source_id,
                local_id=f"{payload.get('id', '?')}:{key}:{related.get('id', '?')}",
                subject_statement_id=rel_subject_sid,
                interested_party_statement_id=rel_ip_sid,
                interested_party_type=rel_ip_type,
                interests=[legacy_interest],
                source_url=subject.get("source", {}).get("url"),
            )
            result.statements.append(rel)

    # --- Nested edge entities (current FtM/yente shape) ----------------------
    # Any edge schema from _FTM_EDGE_SCHEMAS, wherever it is nested
    # (ownershipOwner, ownershipAsset, directorshipOrganization,
    # directorshipDirector, membershipMember, agencyClient, ...).
    _ftm_edge_relationships(
        payload,
        subject_sid=subject_sid,
        subject_type=subject_type,
        source_id=source_id,
        source_url_builder=source_url_builder,
        result=result,
    )

    return result


def _ftm_statement(
    payload: dict[str, Any],
    *,
    source_id: str,
    source_url_builder: Any,
) -> dict[str, Any] | None:
    ftm_id = payload.get("id")
    if not ftm_id:
        return None
    schema = payload.get("schema") or ""
    props = payload.get("properties") or {}

    source_url = source_url_builder(ftm_id) if callable(source_url_builder) else None

    if schema in _FTM_PERSON_SCHEMAS:
        return _ftm_person_statement(payload, source_id, source_url)
    # Everything else — including unknown schemas — becomes an entity.
    return _ftm_entity_statement(payload, source_id, source_url)


def _ftm_entity_statement(
    payload: dict[str, Any], source_id: str, source_url: str | None
) -> dict[str, Any]:
    ftm_id = payload.get("id") or ""
    props = payload.get("properties") or {}
    name = (
        (props.get("name") or [None])[0]
        or payload.get("caption")
        or f"Entity {ftm_id}"
    )

    jurisdiction = _ftm_jurisdiction(props)
    identifiers = _ftm_identifiers(ftm_id, source_id, props)
    addresses = _ftm_addresses(props)
    founding_date = (props.get("incorporationDate") or [None])[0]

    stmt = make_entity_statement(
        source_id=source_id,
        local_id=ftm_id,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        addresses=addresses,
        founding_date=founding_date,
        source_url=source_url,
    )
    # FtM ``dissolutionDate`` / ``status`` (Phase 151). Only a dissolution date
    # is a positive statement; FtM ``status`` is free text from whichever
    # upstream dataset produced the record, so it travels as the raw label
    # but is not classified on its own.
    ftm_dissolved = (props.get("dissolutionDate") or [None])[0]
    if ftm_dissolved:
        _liveness.apply_register_status(
            stmt,
            source_label=SOURCE_NAMES.get(source_id, source_id),
            liveness=_liveness.TERMINAL,
            raw=(props.get("status") or [None])[0],
            since=str(ftm_dissolved)[:10],
        )
    return stmt


def _ftm_person_statement(
    payload: dict[str, Any], source_id: str, source_url: str | None
) -> dict[str, Any]:
    ftm_id = payload.get("id") or ""
    props = payload.get("properties") or {}
    full_name = (
        (props.get("name") or [None])[0]
        or payload.get("caption")
        or f"Person {ftm_id}"
    )
    nationalities = [_ftm_resolve_nationality(n) for n in (props.get("nationality") or [])]
    birth_date = (props.get("birthDate") or [None])[0]
    addresses = _ftm_addresses(props)
    identifiers = _ftm_identifiers(ftm_id, source_id, props)

    return make_person_statement(
        source_id=source_id,
        local_id=ftm_id,
        full_name=full_name,
        nationalities=nationalities,
        birth_date=birth_date,
        addresses=addresses,
        identifiers=identifiers,
        source_url=source_url,
    )


def _ftm_resolve_nationality(raw: str) -> dict[str, str]:
    """Resolve a FtM nationality string (ISO code or full name) to a BODS entry.

    Returns ``{"name": ..., "code": ...}`` when pycountry can resolve the
    value, or ``{"name": raw}`` as a safe fallback.
    """
    try:
        country = pycountry.countries.lookup(raw.strip())
        return {"name": country.name, "code": country.alpha_2}
    except LookupError:
        return {"name": raw.strip()}


def _ftm_jurisdiction(props: dict[str, Any]) -> tuple[str, str] | None:
    """Resolve a FtM jurisdiction/country property array to ``(name, alpha-2)``.

    FtM stores jurisdiction as an array of strings that may be ISO 3166-1
    alpha-2 codes (``"RU"``), lowercase codes (``"ru"``), or full country
    names. We resolve all forms via pycountry so the BODS
    ``jurisdiction.name`` is always a human-readable string.
    """
    jur = (props.get("jurisdiction") or props.get("country") or [None])[0]
    if not jur:
        return None
    try:
        country = pycountry.countries.lookup(jur.strip())
        return (country.name, country.alpha_2)
    except LookupError:
        # Unknown/custom jurisdiction — surface as-is so it's not silently lost.
        return (jur.strip(), _country_code(jur) or jur.strip())


def _ftm_identifiers(
    ftm_id: str, source_id: str, props: dict[str, Any]
) -> list[dict[str, str]]:
    scheme_name = "OpenSanctions" if source_id == "opensanctions" else "OpenAleph"
    scheme_code = "OPENSANCTIONS" if source_id == "opensanctions" else "OPENALEPH"
    identifiers: list[dict[str, str]] = [
        {"id": ftm_id, "scheme": scheme_code, "schemeName": scheme_name}
    ]

    # Resolve jurisdiction so registrationNumber gets a country-qualified scheme
    # (e.g. "REG-RU" instead of the generic "REG") when the entity's
    # jurisdiction is known. This lets reconcilers bridge to other sources on
    # the same identifier without guessing the registry.
    jur_raw = (props.get("jurisdiction") or props.get("country") or [None])[0]
    reg_scheme = "REG"
    if jur_raw:
        try:
            alpha2 = pycountry.countries.lookup(jur_raw.strip()).alpha_2
            reg_scheme = f"REG-{alpha2}"
        except LookupError:
            pass

    for key, scheme, name in (
        ("leiCode", "XI-LEI", "Legal Entity Identifier"),
        ("wikidataId", "WIKIDATA", "Wikidata"),
        ("registrationNumber", reg_scheme, "Local registry identifier"),
        ("ogrnCode", "RU-OGRN", "Russian OGRN"),
        ("innCode", "RU-INN", "Russian INN"),
    ):
        values = props.get(key) or []
        if values:
            identifiers.append(
                {"id": values[0], "scheme": scheme, "schemeName": name}
            )
    return identifiers


def _ftm_addresses(props: dict[str, Any]) -> list[dict[str, Any]]:
    raw = props.get("address") or props.get("addressEntity") or []
    result: list[dict[str, Any]] = []
    for entry in raw:
        if isinstance(entry, str):
            # No country available — omit country key.
            result.append({"type": "registered", "address": entry})
        elif isinstance(entry, dict):
            p = entry.get("properties") or {}
            parts = [
                *(p.get("street") or []),
                *(p.get("city") or []),
                *(p.get("region") or []),
                *(p.get("postalCode") or []),
                *(p.get("country") or []),
            ]
            joined = ", ".join([str(x) for x in parts if x])
            if joined:
                result.append(
                    _addr("registered", joined, (p.get("country") or [""])[0])
                )
    return result


def map_opensanctions(bundle: dict[str, Any]) -> BODSBundle:
    """Convenience wrapper: ``bundle`` is the adapter's fetch output."""
    entity = bundle.get("entity") or bundle
    return map_ftm(
        entity,
        source_id="opensanctions",
        source_url_builder=lambda _id: f"https://www.opensanctions.org/entities/{_id}/",
    )


def map_openaleph(bundle: dict[str, Any]) -> BODSBundle:
    """Convenience wrapper: ``bundle`` is the adapter's fetch output."""
    entity = bundle.get("entity") or bundle
    return map_ftm(
        entity,
        source_id="openaleph",
        source_url_builder=lambda _id: f"https://search.openaleph.org/entities/{_id}",
    )


def map_everypolitician(bundle: dict[str, Any]) -> BODSBundle:
    """Convenience wrapper for EveryPolitician — same FtM shape as OpenSanctions.

    Politicians never carry ownership data, so the mapper simply emits
    a single ``personStatement``. ``positions held`` is intentionally
    *not* converted to BODS interests — those are PEP signals, surfaced
    separately by the risk engine.
    """
    entity = bundle.get("entity") or bundle
    return map_ftm(
        entity,
        source_id="everypolitician",
        source_url_builder=lambda _id: f"https://www.opensanctions.org/entities/{_id}/",
    )
