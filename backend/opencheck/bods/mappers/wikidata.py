"""Wikidata → BODS.

Split out of ``mapper.py`` in Phase 168, unchanged. The largest per-source
section in the file, and entirely self-contained: it reads Wikidata claims
and emits statements through the shared factories.
"""

from __future__ import annotations

from typing import Any

import pycountry

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    BODSBundle,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)

# ----------------------------------------------------------------------
# Wikidata → BODS
# ----------------------------------------------------------------------


def _emit_wikidata_owner(
    result: BODSBundle,
    subject_qid: str,
    subject_statement_id: str,
    owner: dict[str, Any],
    subject_source_url: str | None,
) -> None:
    """Emit a controlling-owner statement + relationship from the Wikidata
    ownership extraction (see docs/wikidata-ownership.md).

    The owner is already classified (person / foundation / arrangement / company /
    glie / state / stateBody) with family owners dropped upstream, so this maps it
    to the correct BODS ``entityType`` (or a personStatement), records the
    **indicative** share as ``share.exact``, and points the BODS ``source`` at the
    underlying reference where Wikidata carries one.
    """
    oqid = owner.get("qid") or ""
    oname = owner.get("name") or oqid
    if not oqid:
        return

    identifiers = [
        {
            "id": oqid,
            "scheme": "WIKIDATA",
            "schemeName": "Wikidata Q identifier",
            "uri": f"https://www.wikidata.org/wiki/{oqid}",
        }
    ]
    owner_url = f"https://www.wikidata.org/wiki/{oqid}"

    if owner.get("bods_kind") == "person":
        owner_stmt = make_person_statement(
            source_id="wikidata", local_id=oqid, full_name=oname,
            identifiers=identifiers, source_url=owner_url,
        )
        ip_type = "person"
    else:
        owner_stmt = make_entity_statement(
            source_id="wikidata", local_id=oqid, name=oname, identifiers=identifiers,
            entity_type=owner.get("entity_type") or "registeredEntity",
            source_url=owner_url,
        )
        ip_type = "entity"

    result.statements.append(owner_stmt)

    share = owner.get("share_percent")
    refs = owner.get("references") or []
    ref0 = refs[0] if refs else {}
    ref_src = ref0.get("stated_in") or ref0.get("url")
    via = "/".join(owner.get("via") or []) or "P127/P749"

    # beneficialOwnershipOrControl comes from the regimes registry: Wikidata
    # publishes no BO declaration, so a person owner gets NO flag ("not
    # stated" — the old hard-coded True over-claimed, audit finding 4), while
    # an entity party is never the beneficial owner (definitional -> false).
    interest: dict[str, Any] = set_beneficial_ownership(
        {},
        "wikidata",
        record_kind=(
            "owner_natural_person" if ip_type == "person" else "owner_entity"
        ),
    )
    if share is not None:
        interest["type"] = "shareholding"
        interest["share"] = {"exact": share}
        detail = f"Ownership declared on Wikidata ({via}); {share}% (indicative)"
    else:
        interest["type"] = "otherInfluenceOrControl"
        detail = f"Controlling owner declared on Wikidata ({via})"
    if ref_src:
        detail += f"; source: {ref_src}"
    interest["details"] = detail

    relationship = make_relationship_statement(
        source_id="wikidata",
        local_id=f"{subject_qid}-owner-{oqid}",
        subject_statement_id=subject_statement_id,
        interested_party_statement_id=owner_stmt["statementId"],
        interested_party_type=ip_type,
        interests=[interest],
        source_url=(ref0.get("url") or subject_source_url),
    )
    result.statements.append(relationship)


def map_wikidata(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Wikidata fetch bundle to BODS statements.

    Wikidata's role in OpenCheck is identifier-bridging — its records
    carry cross-source identifiers and, for well-documented entities,
    declared parent organisations.  We emit:

    * One person or entity statement (decided by P31) for the subject,
      carrying ``WIKIDATA`` as the primary scheme identifier plus any
      cross-source bridge identifiers (``XI-LEI``, ``OPENCORPORATES``,
      ``ISIN``).
    * For entity subjects only: one stub entity statement per distinct
      parent declared via P749 (parent organization) or P127 (owned by),
      plus one ``relationship`` statement linking the subject to each
      parent with an ``otherInfluenceOrControl`` interest and
      ``beneficialOwnershipOrControl: false``.

    Positions held (``positions``) are intentionally not converted to
    BODS interests — they are PEP signals, surfaced separately by the
    risk engine.
    """
    summary = bundle.get("summary") or {}
    qid = summary.get("qid") or bundle.get("qid") or "Q0"
    label = summary.get("label") or qid
    source_url = f"https://www.wikidata.org/wiki/{qid}"

    base_identifiers: list[dict[str, str]] = [
        {
            "id": qid,
            "scheme": "WIKIDATA",
            "schemeName": "Wikidata Q identifier",
            "uri": f"https://www.wikidata.org/wiki/{qid}",
        }
    ]
    cross_ids = summary.get("identifiers") or {}
    if cross_ids.get("lei"):
        base_identifiers.append(
            {
                "id": cross_ids["lei"],
                "scheme": "XI-LEI",
                "schemeName": "Global Legal Entity Identifier Index",
            }
        )
    if cross_ids.get("opencorporates"):
        base_identifiers.append(
            {
                "id": cross_ids["opencorporates"],
                "scheme": "OpenCorporates",
                "schemeName": "OpenCorporates company ID",
            }
        )
    if cross_ids.get("isin"):
        base_identifiers.append(
            {
                "id": cross_ids["isin"],
                "scheme": "ISIN",
                "schemeName": "International Securities Identification Number",
            }
        )

    result = BODSBundle()

    if summary.get("is_person"):
        nationalities: list[dict[str, str]] = []
        for citizenship in summary.get("citizenships") or []:
            country_qid = citizenship.get("qid")
            country_label = citizenship.get("label") or country_qid
            if country_qid and country_label:
                nationalities.append(
                    {"name": country_label, "code": country_qid}
                )

        person = make_person_statement(
            source_id="wikidata",
            local_id=qid,
            full_name=label,
            nationalities=nationalities,
            birth_date=_normalise_wikidata_date(summary.get("dob")),
            identifiers=base_identifiers,
            source_url=source_url,
        )
        result.statements.append(person)
        return result

    # Anything that's not a Q5 we treat as an entity. If P31 was empty
    # entirely (rare for live data) we still emit an entity statement —
    # the BODS validator accepts ``unknownEntity`` as the entityType for
    # such cases.
    entity_type = "registeredEntity" if summary.get("is_entity") else "unknownEntity"
    jurisdiction = _wikidata_jurisdiction(summary.get("country") or {})
    # Phase E (rigour adoption): multilingual labels captured by the adapter
    # (previously pinned to @en) become alternate names — the display name
    # stays the English label. Order: sorted by language tag, deduped.
    other_labels = list(dict.fromkeys(
        e["label"]
        for e in summary.get("labels") or []
        if e.get("label") and e["label"] != label
    ))
    entity = make_entity_statement(
        source_id="wikidata",
        local_id=qid,
        name=label,
        jurisdiction=jurisdiction,
        identifiers=base_identifiers,
        founding_date=_normalise_wikidata_date(summary.get("inception")),
        alternate_names=other_labels,
        entity_type=entity_type,
        source_url=source_url,
    )
    result.statements.append(entity)

    # Controlling owners (P127 owned-by / P749 parent). The richer extraction
    # classifies each owner and maps it to the correct BODS entityType
    # (registeredEntity / arrangement / state / stateBody) or a personStatement,
    # with an indicative share and the underlying reference (see
    # docs/wikidata-ownership.md). Falls back to the legacy parent_orgs path for
    # bundles produced before the ownership query existed.
    subject_statement_id: str = entity["statementId"]
    controlling_owners = summary.get("controlling_owners")
    if controlling_owners:
        for owner in controlling_owners:
            _emit_wikidata_owner(result, qid, subject_statement_id, owner, source_url)
    else:
        # Legacy fallback: parent Q-ID + label only → unknownEntity parent.
        for parent in summary.get("parent_orgs") or []:
            parent_qid = parent.get("qid") or ""
            parent_name = parent.get("label") or parent_qid
            if not parent_qid:
                continue

            parent_identifiers = [
                {
                    "id": parent_qid,
                    "scheme": "WIKIDATA",
                    "schemeName": "Wikidata Q identifier",
                    "uri": f"https://www.wikidata.org/wiki/{parent_qid}",
                }
            ]
            parent_entity = make_entity_statement(
                source_id="wikidata",
                local_id=parent_qid,
                name=parent_name,
                identifiers=parent_identifiers,
                entity_type="unknownEntity",
                source_url=f"https://www.wikidata.org/wiki/{parent_qid}",
            )
            result.statements.append(parent_entity)

            relationship = make_relationship_statement(
                source_id="wikidata",
                local_id=f"{qid}-parent-{parent_qid}",
                subject_statement_id=subject_statement_id,
                interested_party_statement_id=parent_entity["statementId"],
                interested_party_type="entity",
                interests=[
                    {
                        "type": "otherInfluenceOrControl",
                        "beneficialOwnershipOrControl": False,
                        "details": "Parent organisation declared on Wikidata (P749/P127)",
                    }
                ],
                source_url=source_url,
            )
            result.statements.append(relationship)

    # Emit person + relationship statements for current roleholders
    # (P169 CEO, P488 chair, P3320 board member, P6346 treasurer, P1037 director).
    # Each person gets one personStatement and one relationshipStatement whose
    # interests list carries one seniorManagingOfficial entry per role they hold,
    # with the role title in the ``details`` field.
    for roleholder in summary.get("roleholders") or []:
        person_qid  = roleholder.get("qid") or ""
        person_name = roleholder.get("name") or person_qid
        if not person_qid:
            continue

        person_identifiers = [
            {
                "id": person_qid,
                "scheme": "WIKIDATA",
                "schemeName": "Wikidata Q identifier",
                "uri": f"https://www.wikidata.org/wiki/{person_qid}",
            }
        ]
        person_stmt = make_person_statement(
            source_id="wikidata",
            local_id=person_qid,
            full_name=person_name,
            identifiers=person_identifiers,
            source_url=f"https://www.wikidata.org/wiki/{person_qid}",
        )
        result.statements.append(person_stmt)

        # One BODS interest per role; normalise Wikidata date strings.
        roles = roleholder.get("roles") or []
        interests: list[dict[str, Any]] = []
        for role in roles:
            role_label = role.get("label") or "officeholder"
            start_raw  = role.get("start")
            interest: dict[str, Any] = {
                "type": "seniorManagingOfficial",
                "details": role_label,
                "beneficialOwnershipOrControl": False,
            }
            start_date = _normalise_wikidata_date(start_raw)
            if start_date:
                interest["startDate"] = start_date
            interests.append(interest)

        if not interests:
            interests = [
                {
                    "type": "seniorManagingOfficial",
                    "details": "officeholder",
                    "beneficialOwnershipOrControl": False,
                }
            ]

        roleholder_rel = make_relationship_statement(
            source_id="wikidata",
            local_id=f"{qid}-role-{person_qid}",
            subject_statement_id=subject_statement_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )
        result.statements.append(roleholder_rel)

    return result


def _normalise_wikidata_date(value: str | None) -> str | None:
    """Convert ``+1952-10-07T00:00:00Z`` → ``1952-10-07``.

    Wikidata's SPARQL service returns dates as XSD dateTime strings
    (sometimes with a ``+`` sign prefix); BODS expects ISO date.
    """
    if not value:
        return None
    cleaned = value.lstrip("+")
    if "T" in cleaned:
        cleaned = cleaned.split("T", 1)[0]
    return cleaned or None


def _wikidata_jurisdiction(country: dict[str, Any]) -> tuple[str, str] | None:
    """Resolve a Wikidata ``country`` object to a ``(name, ISO code)`` tuple.

    Wikidata's P17 returns a Q-ID — we use the country's English label
    and pass it through pycountry to recover the alpha-2 code so the
    BODS jurisdiction block carries an ISO code (matching every other
    source). When the lookup fails we fall back to the raw label/Q-ID.
    """
    if not country:
        return None
    name = country.get("label")
    if not name:
        return None
    try:
        match = pycountry.countries.lookup(name)
    except LookupError:
        return (name, country.get("qid", name))
    return (match.name, match.alpha_2)


_OC_POSITION_TO_INTEREST_TYPE: dict[str, str] = {
    # Board-level appointments
    "director": "appointmentOfBoard",
    "managing director": "appointmentOfBoard",
    "executive director": "appointmentOfBoard",
    "non-executive director": "appointmentOfBoard",
    "alternate director": "appointmentOfBoard",
    "shadow director": "appointmentOfBoard",
    "de facto director": "appointmentOfBoard",
    "deputy director": "appointmentOfBoard",
    "associate director": "appointmentOfBoard",
    "joint director": "appointmentOfBoard",
    "directeur": "appointmentOfBoard",
    "directeur general": "appointmentOfBoard",
    "geschaeftsfuehrer": "appointmentOfBoard",
    "direktor": "appointmentOfBoard",
    "bestuurder": "appointmentOfBoard",
    "amministratore": "appointmentOfBoard",
    "administrador": "appointmentOfBoard",
    # Board membership (non-chair)
    "board member": "boardMember",
    "member of the board": "boardMember",
    "supervisory board member": "boardMember",
    "aufsichtsratsmitglied": "boardMember",
    "bestuurslid": "boardMember",
    "vice president": "boardMember",
    "vorsitzender": "boardMember",
    "voorzitter": "boardMember",
    "presidente": "boardMember",
    # Board chair (BODS v0.4 has a separate boardChair type)
    "chairman": "boardChair",
    "chairwoman": "boardChair",
    "chairperson": "boardChair",
    "chair": "boardChair",
    "president": "boardChair",
    "vice chairman": "boardChair",
    "deputy chairman": "boardChair",
    # Senior management / officers
    "secretary": "seniorManagingOfficial",
    "company secretary": "seniorManagingOfficial",
    "corporate secretary": "seniorManagingOfficial",
    "assistant secretary": "seniorManagingOfficial",
    "joint secretary": "seniorManagingOfficial",
    "chief executive": "seniorManagingOfficial",
    "chief executive officer": "seniorManagingOfficial",
    "ceo": "seniorManagingOfficial",
    "chief financial officer": "seniorManagingOfficial",
    "cfo": "seniorManagingOfficial",
    "chief operating officer": "seniorManagingOfficial",
    "coo": "seniorManagingOfficial",
    "chief technology officer": "seniorManagingOfficial",
    "cto": "seniorManagingOfficial",
    "treasurer": "seniorManagingOfficial",
    "manager": "seniorManagingOfficial",
    "general manager": "seniorManagingOfficial",
    "partner": "seniorManagingOfficial",
    "general partner": "seniorManagingOfficial",
    "limited partner": "seniorManagingOfficial",
    "managing partner": "seniorManagingOfficial",
    "member": "seniorManagingOfficial",
    "managing member": "seniorManagingOfficial",
    "liquidator": "seniorManagingOfficial",
    "receiver": "seniorManagingOfficial",
    "administrator": "seniorManagingOfficial",
    "gerant": "seniorManagingOfficial",
    # Nominees / agents
    "nominee": "nominee",
    "nominee director": "nominee",
    "nominee shareholder": "nominee",
    "nominee secretary": "nominee",
    "agent": "otherInfluenceOrControl",
    "authorized representative": "otherInfluenceOrControl",
    "authorised representative": "otherInfluenceOrControl",
    "representative": "otherInfluenceOrControl",
    "legal representative": "otherInfluenceOrControl",
    "proxy": "otherInfluenceOrControl",
    "power of attorney": "otherInfluenceOrControl",
    # Trust roles
    "trustee": "trustee",
    "co-trustee": "trustee",
    "settlor": "settlor",
    "protector": "protector",
    "beneficiary": "beneficiaryOfLegalArrangement",
    "guardian": "otherInfluenceOrControl",
    # Ownership
    "shareholder": "shareholding",
    "owner": "shareholding",
    "subscriber": "shareholding",
    "incorporator": "otherInfluenceOrControl",
    "founder": "otherInfluenceOrControl",
}

# Relationship types from the OC Relationships Supplement → BODS interest type.
_OC_RELATIONSHIP_TYPE_TO_INTEREST: dict[str, str] = {
    "control_statement": "otherInfluenceOrControl",
    "control": "otherInfluenceOrControl",
    "subsidiary": "shareholding",
    "parent": "shareholding",
    "branch": "otherInfluenceOrControl",
    "share_parcel": "shareholding",
    "share": "shareholding",
}


def _oc_match_position(position: str) -> str:
    """Map an OC officer position string to a BODS interestType.

    Strategy: exact match → substring match → regex patterns → default.
    Officer positions never carry beneficialOwnershipOrControl=True
    (they represent governance roles, not ownership claims).
    """
    if not position:
        return "otherInfluenceOrControl"
    norm = position.strip().lower()
    if norm in _OC_POSITION_TO_INTEREST_TYPE:
        return _OC_POSITION_TO_INTEREST_TYPE[norm]
    for known, itype in _OC_POSITION_TO_INTEREST_TYPE.items():
        if known in norm:
            return itype
    # Regex fallbacks for multilingual variants
    import re as _re
    if _re.search(r"\bdirect(or|eur|ör)\b", norm):
        return "appointmentOfBoard"
    if _re.search(r"\bsecretar", norm):
        return "seniorManagingOfficial"
    if _re.search(r"\bmanag", norm):
        return "seniorManagingOfficial"
    if _re.search(r"\bchair", norm):
        return "boardChair"
    if _re.search(r"\btrustee", norm):
        return "trustee"
    if _re.search(r"\bnominee", norm):
        return "nominee"
    return "otherInfluenceOrControl"


def _oc_parse_network_relationships(
    network: dict[str, Any],
    focal_ocid: str,
) -> list[dict[str, Any]]:
    """Extract a list of normalised relationship dicts from a raw OC network payload.

    The OC ``/network`` endpoint (Relationships Supplement) is a premium
    API product.  Its exact JSON shape is not publicly documented, so we
    probe multiple plausible structures and normalise to a common internal
    dict::

        {
          "relationship_type": str,
          "source": {"name": str, "jurisdiction_code": str, "company_number": str},
          "target": {"name": str, "jurisdiction_code": str, "company_number": str},
          "percentage_min_share_ownership": float | None,
          "percentage_max_share_ownership": float | None,
          "percentage_min_voting_rights": float | None,
          "percentage_max_voting_rights": float | None,
          "start_date": str | None,
          "end_date": str | None,
        }

    Relationships with ``end_date`` set are skipped (historical only).
    """

    def _extract_company(obj: dict[str, Any]) -> dict[str, str]:
        """Unwrap a possibly-nested company dict → {name, jurisdiction_code, company_number}."""
        if "company" in obj and isinstance(obj["company"], dict):
            obj = obj["company"]
        return {
            "name": str(obj.get("name") or ""),
            "jurisdiction_code": str(obj.get("jurisdiction_code") or ""),
            "company_number": str(obj.get("company_number") or ""),
        }

    def _float_or_none(val: Any) -> float | None:
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None

    results: list[dict[str, Any]] = []

    # --- Try to locate the relationships list inside the network payload ---
    # Possible structures:
    #   A) network["relationships"] → list of {"relationship": {...}}
    #   B) network["network"] → list of {"relationship": {...}} or flat dicts
    #   C) network["edges"] → list of flat relationship dicts
    #   D) network is itself a list
    candidates: list[Any] = []
    if isinstance(network, list):
        candidates = network
    elif isinstance(network, dict):
        for key in ("relationships", "network", "edges"):
            val = network.get(key)
            if isinstance(val, list):
                candidates = val
                break

    for item in candidates:
        # Unwrap {"relationship": {...}} wrapper if present
        rel = item.get("relationship", item) if isinstance(item, dict) else item
        if not isinstance(rel, dict):
            continue

        end_date = rel.get("end_date")
        if end_date:
            continue  # skip historical relationships

        rel_type = (rel.get("relationship_type") or rel.get("type") or "").strip()

        # Source / target — OC may use source/target, subject/object, or from/to
        src_raw = (
            rel.get("source")
            or rel.get("subject")
            or rel.get("from")
            or {}
        )
        tgt_raw = (
            rel.get("target")
            or rel.get("object")
            or rel.get("to")
            or {}
        )

        # For endpoints that return a flat list of related companies (no
        # explicit source/target), the focal company is always the subject.
        if not src_raw and not tgt_raw:
            tgt_raw = rel  # the item itself is the related company
            jc, num = focal_ocid.split("/", 1) if "/" in focal_ocid else ("", focal_ocid)
            src_raw = {"jurisdiction_code": jc, "company_number": num, "name": ""}

        results.append({
            "relationship_type": rel_type,
            "source": _extract_company(src_raw) if isinstance(src_raw, dict) else {},
            "target": _extract_company(tgt_raw) if isinstance(tgt_raw, dict) else {},
            "percentage_min_share_ownership": _float_or_none(
                rel.get("percentage_min_share_ownership")
                or rel.get("percentage_min")
                or rel.get("min_percentage")
            ),
            "percentage_max_share_ownership": _float_or_none(
                rel.get("percentage_max_share_ownership")
                or rel.get("percentage_max")
                or rel.get("max_percentage")
            ),
            "percentage_min_voting_rights": _float_or_none(
                rel.get("percentage_min_voting_rights")
            ),
            "percentage_max_voting_rights": _float_or_none(
                rel.get("percentage_max_voting_rights")
            ),
            "start_date": rel.get("start_date"),
            "end_date": end_date,
        })

    return results


def _oc_build_interests_from_relationship(rel: dict[str, Any]) -> list[dict[str, Any]]:
    """Produce a BODS interests list from a normalised OC network relationship dict."""
    interests: list[dict[str, Any]] = []
    pmin_own = rel.get("percentage_min_share_ownership")
    pmax_own = rel.get("percentage_max_share_ownership")
    pmin_vot = rel.get("percentage_min_voting_rights")
    pmax_vot = rel.get("percentage_max_voting_rights")
    start = rel.get("start_date")

    def _share_obj(mn: float | None, mx: float | None) -> dict[str, float]:
        if mn is not None and mx is not None:
            return {"exact": mn} if mn == mx else {"minimum": mn, "maximum": mx}
        if mn is not None:
            return {"minimum": mn}
        if mx is not None:
            return {"maximum": mx}
        return {}

    # A percentage on an OpenCorporates network relationship makes the holding
    # more precise, not more beneficial: it is still a registered stake between
    # two companies, and OC publishes no BO declaration. Same rule as the
    # type-inferred fallback below.
    if pmin_own is not None or pmax_own is not None:
        entry: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
            "share": _share_obj(pmin_own, pmax_own),
        }
        set_beneficial_ownership(entry, "opencorporates")
        if start:
            entry["startDate"] = start
        interests.append(entry)

    if pmin_vot is not None or pmax_vot is not None:
        entry = {
            "type": "votingRights",
            "directOrIndirect": "direct",
            "share": _share_obj(pmin_vot, pmax_vot),
        }
        set_beneficial_ownership(entry, "opencorporates")
        if start:
            entry["startDate"] = start
        interests.append(entry)

    # Fallback: no percentage data — use the relationship type to pick an interest
    if not interests:
        rel_type = rel.get("relationship_type", "")
        interest_type = _OC_RELATIONSHIP_TYPE_TO_INTEREST.get(
            rel_type.lower(), "otherInfluenceOrControl"
        )
        # OpenCorporates network relationships are structural links between
        # registered entities, not beneficial-ownership declarations. Inferring
        # the flag from the interest TYPE asserted beneficial ownership that no
        # register had stated — see set_beneficial_ownership.
        entry = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "details": f"OpenCorporates relationship: {rel_type}" if rel_type else "OpenCorporates network relationship",
        }
        set_beneficial_ownership(entry, "opencorporates")
        if start:
            entry["startDate"] = start
        interests.append(entry)

    return interests


def map_opencorporates(bundle: dict[str, Any]) -> BODSBundle:
    """Map an OpenCorporates fetch bundle to BODS v0.4 statements.

    Produces:
    * One entity statement for the company itself.
    * One person or entity statement + relationship per current officer.
    * When the ``network`` key is present (OC Relationships Supplement),
      additional entity statements for related companies and ownership-or-
      control relationship statements for each active network relationship.

    Officers are sourced from ``/companies/{j}/{n}/officers`` (``position``,
    optional start/end dates).  Network relationships come from the premium
    ``/companies/{j}/{n}/network`` endpoint and cover ``control_statement``,
    ``subsidiary``, ``branch``, and ``share_parcel`` types.
    """
    result = BODSBundle()
    company = bundle.get("company") or {}
    ocid = bundle.get("ocid") or bundle.get("hit_id") or ""
    # The dedicated /officers endpoint requires a premium API tier and returns
    # null (402/403) for standard keys. Fall back to the officers list embedded
    # in the company profile endpoint response, which is available on all tiers
    # (typically up to 50 officers, wrapped as {"officer": {...}} items).
    officers = bundle.get("officers") or company.get("officers") or []
    network_raw = bundle.get("network")  # None when Supplement not available

    if not company:
        return result

    # --- Entity statement for the focal company ---------------------------

    name = company.get("name") or "Unknown company"
    jurisdiction_code = (company.get("jurisdiction_code") or "").upper()
    company_number = company.get("company_number") or ""
    incorporation_date = company.get("incorporation_date")
    oc_url = company.get("opencorporates_url") or (
        f"https://opencorporates.com/companies/{ocid}" if ocid else None
    )

    jurisdiction: tuple[str, str] | None = None
    if jurisdiction_code:
        # OC uses ISO 3166-1 alpha-2 lower, with sub-national variants like
        # "us_de".  Use the top-level alpha-2 code for display.
        top_code = jurisdiction_code.split("_")[0].upper()
        try:
            country = pycountry.countries.get(alpha_2=top_code)
            country_name = country.name if country else top_code
            jurisdiction = (country_name, top_code)
        except Exception:  # noqa: BLE001
            jurisdiction = (top_code, top_code)

    identifiers: list[dict[str, str]] = []
    if ocid:
        identifiers.append(
            {
                "id": ocid,
                "scheme": "OpenCorporates",
                "schemeName": "OpenCorporates company ID",
                "uri": oc_url or "",
            }
        )
    if company_number and jurisdiction_code:
        # Map OC jurisdiction codes to org.ids scheme codes.
        # OC uses ISO alpha-2 (lower) for country-level jurisdictions and
        # "{alpha2}_{state}" for sub-national ones (e.g. "us_de" for Delaware).
        # Fall back to "{ALPHA2}-COA" (generic companies register) if no exact
        # match is known.
        _OC_JUR_TO_ORGID: dict[str, str] = {
            "gb": "GB-COH", "nl": "NL-KVK", "se": "SE-BLV", "no": "NO-BRC",
            "fr": "FR-RCS", "be": "BE-BCE_KBO", "at": "AT-FB", "ch": "CH-FDJP",
            "pl": "PL-KRS", "cz": "CZ-ICO", "sk": "SK-ORSR", "lt": "LT-RC",
            "lv": "LV-RE", "ee": "EE-KMKR", "sg": "SG-ACRA", "ca": "CA-CC",
        }
        top_jur = jurisdiction_code.split("_")[0].lower()
        jur_scheme = _OC_JUR_TO_ORGID.get(top_jur) or f"{top_jur.upper()}-COA"
        identifiers.append(
            {
                "id": company_number,
                "scheme": jur_scheme,
                "schemeName": f"OpenCorporates {jurisdiction_code.upper()} company number",
            }
        )

    subject_stmt = make_entity_statement(
        source_id="opencorporates",
        local_id=ocid or company_number,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        founding_date=incorporation_date,
        entity_type="registeredEntity",
        source_url=oc_url,
    )
    # OpenCorporates normalises every register's status into ``inactive``
    # (bool) and carries the register's own words in ``current_status`` and,
    # where published, a ``dissolution_date``. The bool is OC's classification
    # and is what we key on; the label travels verbatim (Phase 151).
    oc_inactive = company.get("inactive")
    oc_dissolved = company.get("dissolution_date")
    if oc_dissolved or oc_inactive is True:
        oc_liveness = _liveness.TERMINAL
    elif oc_inactive is False:
        oc_liveness = _liveness.LIVE
    else:
        oc_liveness = _liveness.UNKNOWN
    _liveness.apply_register_status(
        subject_stmt,
        source_label=SOURCE_NAMES["opencorporates"],
        liveness=oc_liveness,
        raw=company.get("current_status") or company.get("company_status"),
        since=oc_dissolved,
    )
    subject_stmt_id: str = subject_stmt["statementId"]
    result.extend([subject_stmt])

    # Track emitted entity statementIds to avoid duplicates across officers
    # and network relationships.
    seen_entity_sids: set[str] = {subject_stmt_id}

    # --- Officer statements -----------------------------------------------
    # OC officers carry a ``position`` string (e.g. "director"), optional
    # ``start_date`` / ``end_date``, and a nested ``officer`` sub-object.
    # We only surface current officers (no end_date set).
    for officer_item in officers:
        officer_data = officer_item.get("officer") or officer_item
        position = (officer_data.get("position") or "").strip()
        end_date = officer_data.get("end_date")
        if end_date:
            continue  # skip resigned officers

        officer_name = officer_data.get("name") or ""
        if not officer_name:
            continue

        officer_id = str(officer_data.get("id") or officer_data.get("uid") or "")
        local_key = f"{ocid}/{officer_id or _stable_id('oc', 'officer', officer_name)}"

        officer_type = (officer_data.get("type") or "").lower()
        is_corporate = officer_type == "company"

        if is_corporate:
            # Corporate officer → entity statement
            corp_stmt = make_entity_statement(
                source_id="opencorporates",
                local_id=local_key,
                name=officer_name,
                source_url=oc_url,
            )
            ip_sid: str = corp_stmt["statementId"]
            if ip_sid not in seen_entity_sids:
                result.extend([corp_stmt])
                seen_entity_sids.add(ip_sid)
            ip_type = "entity"
        else:
            # Natural person → person statement.
            # OC returns date_of_birth as "YYYY-MM" from the company profile
            # endpoint. Extract it so the cross-check's birth-year filter
            # can disambiguate common names against OpenSanctions / EP records.
            dob_raw = officer_data.get("date_of_birth") or ""
            birth_date: str | None = dob_raw if dob_raw else None

            # Nationality comes back as a plain string (e.g. "ITALIAN").
            nationality_str = (officer_data.get("nationality") or "").strip().title()
            nationalities = [{"name": nationality_str}] if nationality_str else []

            person_stmt = make_person_statement(
                source_id="opencorporates",
                local_id=local_key,
                full_name=officer_name,
                birth_date=birth_date,
                nationalities=nationalities,
                source_url=oc_url,
            )
            ip_sid = person_stmt["statementId"]
            result.extend([person_stmt])
            ip_type = "person"

        interest_type = _oc_match_position(position)
        start_date = officer_data.get("start_date")
        interest_entry: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,  # officer roles ≠ ownership
        }
        if interest_type == "otherInfluenceOrControl" and position:
            interest_entry["details"] = f"Officer position: {position}"
        if start_date:
            interest_entry["startDate"] = start_date

        rel_stmt = make_relationship_statement(
            source_id="opencorporates",
            local_id=f"rel/{local_key}",
            subject_statement_id=subject_stmt_id,
            interested_party_statement_id=ip_sid,
            interested_party_type=ip_type,
            interests=[interest_entry],
            source_url=oc_url,
        )
        result.extend([rel_stmt])

    # --- Network relationship statements ----------------------------------
    # These come from the OC Relationships Supplement (premium API tier).
    # Absent when the API key does not have access.
    if network_raw:
        parsed_rels = _oc_parse_network_relationships(network_raw, focal_ocid=ocid)
        for rel in parsed_rels:
            src = rel["source"]
            tgt = rel["target"]

            def _entity_for_company(co: dict[str, str]) -> dict[str, Any] | None:
                co_number = co.get("company_number") or ""
                co_jur = co.get("jurisdiction_code") or ""
                co_name = co.get("name") or "Unknown entity"
                if not co_number and not co_jur:
                    return None
                co_ocid = f"{co_jur}/{co_number}" if co_jur and co_number else co_number
                co_url = f"https://opencorporates.com/companies/{co_ocid}" if co_ocid else None
                co_jur_upper = co_jur.upper().split("_")[0]
                co_jurisdiction: tuple[str, str] | None = None
                if co_jur_upper:
                    try:
                        c = pycountry.countries.get(alpha_2=co_jur_upper)
                        co_jurisdiction = (c.name if c else co_jur_upper, co_jur_upper)
                    except Exception:  # noqa: BLE001
                        co_jurisdiction = (co_jur_upper, co_jur_upper)
                co_ids: list[dict[str, str]] = []
                if co_ocid:
                    co_ids.append({
                        "id": co_ocid,
                        "scheme": "OpenCorporates",
                        "schemeName": "OpenCorporates company ID",
                        "uri": co_url or "",
                    })
                return make_entity_statement(
                    source_id="opencorporates",
                    local_id=co_ocid or co_number,
                    name=co_name,
                    jurisdiction=co_jurisdiction,
                    identifiers=co_ids,
                    entity_type="registeredEntity",
                    source_url=co_url,
                )

            src_stmt = _entity_for_company(src)
            tgt_stmt = _entity_for_company(tgt)
            if not src_stmt or not tgt_stmt:
                continue

            # In BODS, the relationship is: subject (the company being
            # controlled/owned) ← interestedParty (the owner/controller).
            # OC relationship direction: source controls/owns target.
            # → subject = target, interestedParty = source.
            subj_sid = tgt_stmt["statementId"]
            party_sid = src_stmt["statementId"]

            if subj_sid not in seen_entity_sids:
                result.extend([tgt_stmt])
                seen_entity_sids.add(subj_sid)
            if party_sid not in seen_entity_sids:
                result.extend([src_stmt])
                seen_entity_sids.add(party_sid)

            interests = _oc_build_interests_from_relationship(rel)
            rel_local_id = (
                f"network-rel/{src.get('company_number','?')}/"
                f"{tgt.get('company_number','?')}/"
                f"{rel.get('relationship_type','?')}"
            )
            network_rel_stmt = make_relationship_statement(
                source_id="opencorporates",
                local_id=rel_local_id,
                subject_statement_id=subj_sid,
                interested_party_statement_id=party_sid,
                interested_party_type="entity",
                interests=interests,
                source_url=oc_url,
            )
            result.extend([network_rel_stmt])

    return result
