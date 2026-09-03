"""Map source payloads to BODS v0.4 statements.

BODS v0.4 statements come in three kinds — entity, person, relationship —
each wrapped in a ``recordDetails`` object. OpenCheck uses deterministic
statement IDs derived from the source adapter ID plus a stable local key,
so re-mapping the same payload always produces the same IDs. This matters
for deduplication across runs and for the visualisation library, which
keys on statement IDs.

Reference: https://standard.openownership.org/en/0.4.0/
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable

import pycountry

from .. import names as _names_mod
from .. import provenance as _provenance
from ..elf import resolve_elf
from . import liveness as _liveness
from .annotations import annotate, commenting, pointer, transformation
from .ch_constants import describe_company_type, describe_officer_role
from .psc_natures import describe_nature, describe_statement, describe_super_secure

# Phase 168: the statement factories and the two largest per-source sections
# now live in their own modules. They are re-exported here — including the
# private helpers — because 62 files import them from `bods.mapper`, and
# `sources/probes.py` resolves a mapper by `getattr(mapper, name)`. Moving
# code is not the same as moving its address.
from .statements import (  # noqa: F401  (re-exported: see the module docstring)
    BODSBundle,
    SOURCE_NAMES,
    _BO_ASSERTING_SOURCES,
    _CH_UK_COUNTRY_STRINGS,
    _INTEREST_PREFIX,
    _SHARE_BAND_RE,
    _addr,
    _birth_date_precision_note,
    _country_code,
    _country_obj,
    _parse_nature,
    _publication_details_block,
    _source_block,
    _stable_id,
    _statement_date,
    _today,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
    source_may_assert_beneficial_ownership,
)

from .mappers.ftm import (  # noqa: F401  (re-exported: see the module docstring)
    _FTM_BO_ASSERTING_DATASETS,
    _FTM_EDGE_SCHEMAS,
    _FTM_ENTITY_SCHEMAS,
    _FTM_PERSON_SCHEMAS,
    _FTM_ROLE_TO_INTEREST_TYPE,
    _FTM_SUBJECT,
    _ftm_addresses,
    _ftm_asserts_beneficial_ownership,
    _ftm_edge_interest,
    _ftm_edge_relationships,
    _ftm_entity_statement,
    _ftm_identifiers,
    _ftm_jurisdiction,
    _ftm_percentage,
    _ftm_person_statement,
    _ftm_resolve_nationality,
    _ftm_statement,
    map_everypolitician,
    map_ftm,
    map_openaleph,
    map_opensanctions,
)

from .mappers.wikidata import (  # noqa: F401  (re-exported: see the module docstring)
    _OC_POSITION_TO_INTEREST_TYPE,
    _OC_RELATIONSHIP_TYPE_TO_INTEREST,
    _emit_wikidata_owner,
    _normalise_wikidata_date,
    _oc_build_interests_from_relationship,
    _oc_match_position,
    _oc_parse_network_relationships,
    _wikidata_jurisdiction,
    map_opencorporates,
    map_wikidata,
)


def map_companies_house(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Companies House bundle to BODS.

    Two dispatch shapes:

    * ``{"company_number": ..., "profile": ..., "officers": ..., "pscs": ...,
         "related_companies": {...}}``
      — produced by ``_fetch_company_bundle``. Yields the company entity
      + a personStatement / entityStatement per active PSC, plus an
      ownership-or-control relationship per PSC. UK corporate PSC chains
      (up to ``max_depth`` hops, fetched recursively by the adapter) are
      emitted from ``related_companies``.
    * ``{"officer_id": ..., "appointments": {...}}`` — produced by
      ``_fetch_officer_bundle``. Yields the officer as a
      personStatement, plus a "boardMember" relationship for every
      appointment (both current and historical).
    """
    if "officer_id" in bundle:
        return _map_companies_house_officer(bundle)

    result = BODSBundle()
    # Track statement IDs emitted so far to avoid duplicates when a UK
    # corporate PSC appears both as a PSC reference and as a related company.
    seen_sids: set[str] = set()

    _emit_company_statements(bundle, result, seen_sids)

    for sub_bundle in (bundle.get("related_companies") or {}).values():
        _emit_company_statements(sub_bundle, result, seen_sids)

    return result


def _ch_officer_local_id(company_number: str, officer: dict[str, Any]) -> str:
    """Derive a stable local_id for a Companies House officer from the company bundle.

    Extracts the officer id from ``links.officer.appointments`` when present
    (the path has the form ``/officers/{id}/appointments``), falling back to a
    SHA-256 digest of ``{company_number}|{name}|{appointed_on}`` so IDs remain
    stable even when the links block is absent.
    """
    links_path: str = (
        (officer.get("links") or {})
        .get("officer", {})
        .get("appointments", "")
    )
    # Extract id from "/officers/{id}/appointments"
    parts = [p for p in links_path.split("/") if p]
    if "officers" in parts:
        idx = parts.index("officers")
        if idx + 1 < len(parts):
            officer_id = parts[idx + 1]
            return f"{company_number}:director:{officer_id}"
    # Fallback: hash of stable fields
    name = officer.get("name") or ""
    appointed_on = officer.get("appointed_on") or ""
    digest = hashlib.sha256(
        f"{company_number}|{name}|{appointed_on}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{company_number}:director:{digest}"


# Officer roles that constitute a senior managing official, mapped from the
# official CH officer_role enumeration (constants.yml). Excludes purely
# administrative or non-managing roles — secretaries, limited partners,
# supervisory-organ members, and persons merely authorised to accept/represent.
_MANAGING_OFFICIAL_ROLES = frozenset({
    "director",
    "corporate-director",
    "nominee-director",
    "corporate-nominee-director",
    "managing-officer",
    "corporate-managing-officer",
    "member-of-a-management-organ",
    "corporate-member-of-a-management-organ",
    "member-of-an-administrative-organ",
    "corporate-member-of-an-administrative-organ",
    "manager-of-an-eeig",
    "corporate-manager-of-an-eeig",
    "cic-manager",
    "llp-member",
    "corporate-llp-member",
    "llp-designated-member",
    "corporate-llp-designated-member",
    "general-partner-in-a-limited-partnership",
    "corporate-general-partner-in-a-limited-partnership",
    "judicial-factor",
    "receiver-and-manager",
})


def _ch_director_statements(
    company_number: str,
    officers_payload: dict[str, Any],
    entity_sid: str,
    company_url: str,
    seen_sids: set[str],
) -> list[dict[str, Any]]:
    """Emit person + relationship statements for active managing-official officers.

    Includes active officers whose ``officer_role`` is a senior managing official
    per the official CH enumeration (``_MANAGING_OFFICIAL_ROLES``, from
    constants.yml) — directors, managing officers, LLP (designated) members,
    general partners, management/administrative-organ members, etc. Resigned
    officers and non-managing roles (secretary, limited partner, …) are skipped.
    Each becomes:

    * A ``personStatement`` (``knownPerson``) with name, DOB, nationality and
      service address.
    * A ``relationship`` statement with:
      - ``type: seniorManagingOfficial``
      - ``beneficialOwnershipOrControl: false``
      - ``startDate`` = ``appointed_on`` (when present)

    Already-seen ``statementId``\\s are skipped so duplicates are suppressed
    when the same director appears across the root + related-company passes.
    """
    stmts: list[dict[str, Any]] = []
    items = officers_payload.get("items") or []

    for officer in items:
        # Only active directors — skip resignations and non-director roles.
        if officer.get("resigned_on"):
            continue
        role = (officer.get("officer_role") or "").lower()
        if role not in _MANAGING_OFFICIAL_ROLES:
            continue
        role_label = describe_officer_role(role) or "Managing official"

        name: str = officer.get("name") or "Unknown director"

        # Date of birth: CH returns {"year": int, "month": int} or {"year": int}.
        dob = officer.get("date_of_birth")
        birth_date: str | None = None
        if isinstance(dob, dict) and "year" in dob:
            if "month" in dob:
                birth_date = f"{dob['year']:04d}-{dob['month']:02d}"
            else:
                birth_date = f"{dob['year']:04d}"

        nationalities: list[dict[str, str]] = []
        if officer.get("nationality"):
            nationalities.append({"name": officer["nationality"]})

        # Service address — same structure as individual PSC address.
        address_block = officer.get("address") or {}
        addresses: list[dict[str, str]] = []
        if address_block:
            addr_parts = [
                address_block.get("premises"),
                address_block.get("address_line_1"),
                address_block.get("address_line_2"),
                address_block.get("locality"),
                address_block.get("region"),
                address_block.get("postal_code"),
                address_block.get("country"),
            ]
            joined = ", ".join([p for p in addr_parts if p])
            if joined:
                addresses.append(
                    _addr("service", joined, address_block.get("country", ""))
                )

        local_id = _ch_officer_local_id(company_number, officer)
        person = make_person_statement(
            source_id="companies_house",
            local_id=local_id,
            full_name=name,
            person_type="knownPerson",
            nationalities=nationalities,
            birth_date=birth_date,
            addresses=addresses,
            source_url=company_url,
        )
        annotate(person, _birth_date_precision_note(birth_date))
        person_sid = person["statementId"]
        if person_sid not in seen_sids:
            stmts.append(person)
            seen_sids.add(person_sid)

        appointed_on = officer.get("appointed_on")
        details = role_label + (f", from {appointed_on}" if appointed_on else "")
        # No beneficialOwnershipOrControl key: the officers register is not
        # a BO declaration, so the flag stays unset ("not stated") —
        # bo_regimes: companies_house/officer_director -> omit
        # (decision 2026-08-28; was an over-claiming explicit False).
        interest: dict[str, Any] = {
            "type": "seniorManagingOfficial",
            "directOrIndirect": "direct",
            "details": details,
        }
        if appointed_on:
            interest["startDate"] = appointed_on

        rel = make_relationship_statement(
            source_id="companies_house",
            local_id=f"{local_id}:rel",
            subject_statement_id=entity_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=[interest],
            source_url=company_url,
            # appointed_on is when the appointment BEGAN — already carried as
            # interest.startDate above. It is neither when the register
            # declared it (Companies House publishes no per-officer
            # notification date) nor when OpenCheck published, so it belongs in
            # neither statementDate nor publicationDate. Emitting it as a
            # publication date meant a director appointed in 1998 produced a
            # statement OpenCheck "published" in 1998. statementDate falls back
            # to the retrieval date; publicationDate is today.
        )
        rel_sid = rel["statementId"]
        if rel_sid not in seen_sids:
            stmts.append(rel)
            seen_sids.add(rel_sid)

    return stmts


# CH PSC statement code → BODS unspecifiedReason. Only codes that represent
# *missing* beneficial-ownership information are mapped; "positive" update-period
# declarations (e.g. all-beneficial-owners-identified, no-change-…) are absent
# and produce no statement. See data-standard issue #389.
_PSC_STATEMENT_NO_BO = "noBeneficialOwners"
_PSC_STATEMENT_REASON: dict[str, str] = {
    # "There is no beneficial owner" — no party, no interest.
    "no-individual-or-entity-with-signficant-control": _PSC_STATEMENT_NO_BO,
    "no-individual-or-entity-with-signficant-control-partnership": _PSC_STATEMENT_NO_BO,
    "no-beneficial-owner-identified": _PSC_STATEMENT_NO_BO,
    # "A PSC exists but the company cannot identify/confirm them."
    "psc-exists-but-not-identified": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "psc-exists-but-not-identified-partnership": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "psc-details-not-confirmed": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "psc-details-not-confirmed-partnership": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "steps-to-find-psc-not-yet-completed": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "steps-to-find-psc-not-yet-completed-partnership": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "awaiting-confirmation-from-psc": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "at-least-one-beneficial-owner-unidentified": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    # "A PSC was contacted/required to disclose but the information wasn't provided."
    "psc-contacted-but-no-response": "interestedPartyHasNotProvidedInformation",
    "psc-contacted-but-no-response-partnership": "interestedPartyHasNotProvidedInformation",
    "psc-has-failed-to-confirm-changed-details": "interestedPartyHasNotProvidedInformation",
    "psc-has-failed-to-confirm-changed-details-partnership": "interestedPartyHasNotProvidedInformation",
    "restrictions-notice-issued-to-psc": "interestedPartyHasNotProvidedInformation",
    "restrictions-notice-issued-to-psc-partnership": "interestedPartyHasNotProvidedInformation",
    "information-not-provided-for-at-least-one-beneficial-owner": "interestedPartyHasNotProvidedInformation",
    "at-least-one-beneficial-owner-unidentified-and-information-not-provided-for-at-least-one-beneficial-owner": "interestedPartyHasNotProvidedInformation",
}


def _ch_psc_statement_statements(
    number: str,
    statements: list[dict[str, Any]],
    entity_sid: str,
    company_url: str,
) -> list[dict[str, Any]]:
    """Map CH PSC *statements* to BODS ownership-or-control statements with an
    unspecified ``interestedParty`` (BODS missing-information modelling, #389).

    "No beneficial owner" cases carry **no** interest (there is no owner, so no
    phantom party is invented); "exists but unidentified/undisclosed" cases carry
    a single ``unknownInterest`` (an owner exists, identity unknown). The CH
    statement code becomes the ``reason`` and its official text the ``description``.
    """
    out: list[dict[str, Any]] = []
    for item in statements:
        code = (item.get("statement") or "").strip()
        reason = _PSC_STATEMENT_REASON.get(code.lower())
        if not reason:
            continue  # positive / update-period declaration → no missing info
        description = describe_statement(code) or code
        linked = item.get("linked_psc_name")
        if linked and "{linked_psc_name}" in description:
            description = description.replace("{linked_psc_name}", linked)

        ceased_on = item.get("ceased_on")
        interests: list[dict[str, Any]] = []
        if reason != _PSC_STATEMENT_NO_BO:
            # Deliberately NOT routed through the regimes registry: PSC
            # statements are per-code decisions (bo_regimes:
            # companies_house/psc_statement -> per_statement_code) — this
            # branch IS the policy: any statement short of "no BO exists"
            # asserts that a beneficial owner exists but is not identified.
            interests = [{
                "type": "unknownInterest",
                "directOrIndirect": "unknown",
                "beneficialOwnershipOrControl": True,
            }]
        if ceased_on:
            for interest in interests:
                interest["endDate"] = ceased_on

        out.append(
            make_relationship_statement(
                source_id="companies_house",
                local_id=f"{number}:psc-statement:{code}:{item.get('etag', '0')}",
                subject_statement_id=entity_sid,
                interested_party_unspecified={"reason": reason, "description": description},
                interests=interests,
                source_url=company_url,
                # When the register was told, not when we published. A closed
                # record is declared at cessation, so ceased_on wins.
                statement_date=(ceased_on or item.get("notified_on") or None),
                record_status="closed" if ceased_on else "new",
            )
        )
    return out


def _emit_company_statements(
    bundle: dict[str, Any],
    result: BODSBundle,
    seen_sids: set[str],
) -> None:
    """Emit entity + PSC + director statements for one company bundle into *result*.

    *seen_sids* is updated in place; statements whose ``statementId`` is
    already present are silently skipped so the same entity/relationship is
    never duplicated across the root + related-company passes.
    """
    number = str(bundle.get("company_number", ""))
    profile = bundle.get("profile") or {}
    pscs = (bundle.get("pscs") or {}).get("items") or []
    officers_payload = bundle.get("officers") or {}

    company_url = (
        f"https://find-and-update.company-information.service.gov.uk/company/{number}"
    )
    company_name = profile.get("company_name", f"Company {number}")

    # Previous names the company traded under → BODS alternateNames.
    # Companies House publishes these in ``profile.previous_company_names``
    # as ``[{"name": ..., "effective_from": ..., "ceased_on": ...}, ...]``.
    seen_names: set[str] = {company_name}
    alternate_names: list[str] = []
    for prev in profile.get("previous_company_names") or []:
        prev_name = (prev.get("name") or "").strip()
        if prev_name and prev_name not in seen_names:
            seen_names.add(prev_name)
            alternate_names.append(prev_name)

    entity = make_entity_statement(
        source_id="companies_house",
        local_id=number,
        name=company_name,
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[
            {"id": number, "scheme": "GB-COH", "schemeName": "Companies House"}
        ],
        founding_date=profile.get("date_of_creation"),
        # Official company-type label (constants.yml) on entityType.details; the
        # type stays registeredEntity (all CH companies are registered entities).
        # CH profiles carry the type code in ``type`` (e.g. "ltd", "plc", "llp").
        entity_details=describe_company_type(profile.get("type") or profile.get("company_type")),
        addresses=_profile_addresses(profile),
        alternate_names=alternate_names,
        source_url=company_url,
    )
    # Register status → liveness (Phase 151). The Companies House
    # ``company_status`` codelist (constants.yml) distinguishes the closed
    # states from the insolvency processes; only the former end the company.
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["companies_house"],
        liveness=_liveness.classify(
            profile.get("company_status"),
            live=("active", "open", "registered"),
            pending=(
                "liquidation",
                "receivership",
                "administration",
                "voluntary-arrangement",
                "insolvency-proceedings",
            ),
            terminal=("dissolved", "converted-closed", "closed", "removed"),
        ),
        raw=profile.get("company_status"),
        since=profile.get("date_of_cessation"),
    )
    entity_sid = entity["statementId"]
    if entity_sid not in seen_sids:
        result.statements.append(entity)
        seen_sids.add(entity_sid)

    for psc in pscs:
        # Ceased PSCs are no longer dropped: per the BODS Information updates
        # modelling requirements a no-longer-current element is represented by a
        # statement with recordStatus 'closed' (stable recordId, distinct
        # statementId), not by omission.
        ceased_on = psc.get("ceased_on")
        psc_kind = (psc.get("kind") or "").lower()

        if "corporate-entity" in psc_kind or "legal-person" in psc_kind:
            # Detect UK CH registration numbers so the entity statementId
            # produced here aligns with the statementId the related-company
            # pass emits for the same company (both use local_id = reg_no).
            ident = psc.get("identification") or {}
            reg_no = (ident.get("registration_number") or "").strip()
            reg_country = (ident.get("country_registered") or "").lower().strip()
            uk_number = (
                reg_no
                if (
                    len(reg_no) == 8
                    and reg_no.isalnum()
                    and reg_country in _CH_UK_COUNTRY_STRINGS
                )
                else None
            )
            ip = _map_corporate_psc(number, psc, company_url, uk_number=uk_number)
            ip_type = "entity"
        elif "individual" in psc_kind:
            ip = _map_individual_psc(number, psc, company_url)
            ip_type = "person"
        else:
            # super-secure-person / unknown — a known person whose particulars are
            # withheld by court order → anonymousPerson. The official explanation
            # rides on the relationship interest (below), not a placeholder name.
            ip = make_person_statement(
                source_id="companies_house",
                local_id=f"{number}:anon:{psc.get('etag', '0')}",
                full_name="Super-secure person",
                person_type="anonymousPerson",
                source_url=company_url,
            )
            ip_type = "person"

        ip_sid = ip["statementId"]
        if ip_sid not in seen_sids:
            result.statements.append(ip)
            seen_sids.add(ip_sid)

        natures = psc.get("natures_of_control") or []
        if "super-secure" in psc_kind:
            # Particulars withheld by court order → a single `unpublishedInterest`
            # carrying CH's official explanatory text, not a bare unknownInterest.
            ss_code = psc.get("description") or next(
                (n for n in natures if "super-secure" in (n or "").lower()), None
            )
            interests = [
                {
                    "type": "unpublishedInterest",
                    "directOrIndirect": "unknown",
                    "details": describe_super_secure(ss_code),
                }
            ]
        else:
            interests = [_parse_nature(n) for n in natures] or [
                {
                    "type": "unknownInterest",
                    "directOrIndirect": "unknown",
                }
            ]
        # beneficialOwnershipOrControl comes from the regimes registry
        # (bo_regimes.py), not from per-interest literals: an individual PSC
        # record is a BO declaration under the UK regime (psc_individual ->
        # true); a corporate / legal-person PSC (RLE) is an entity interested
        # party and never the beneficial owner (psc_corporate_rle -> false) —
        # the BO relationship is further up the chain.
        psc_record_kind = (
            "psc_corporate_rle" if ip_type == "entity" else "psc_individual"
        )
        for interest in interests:
            set_beneficial_ownership(
                interest, "companies_house", record_kind=psc_record_kind
            )

        # A ceased PSC closes the relationship: stamp each interest with the
        # cessation date and emit the statement with recordStatus 'closed'. The
        # closed statement shares the original's stable recordId — which is how
        # BODS 0.4 links a record's versions (replacesStatements was removed).
        if ceased_on:
            for interest in interests:
                interest["endDate"] = ceased_on

        rel = make_relationship_statement(
            source_id="companies_house",
            local_id=f"{number}:{ip_sid}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=ip_sid,
            interested_party_type=ip_type,
            interests=interests,
            source_url=company_url,
            # When the register was told, not when we published. A closed
            # record is declared at cessation, so ceased_on wins.
            statement_date=(ceased_on or psc.get("notified_on") or None),
            record_status="closed" if ceased_on else "new",
        )
        # The register's own nature-of-control codes. mapper._INTEREST_PREFIX
        # deliberately does not model them as BODS interest types (nominee
        # arrangements need an intermediary `arrangement` entity, which is not
        # implemented), so the code identity survived only inside an English
        # prose descriptor. That made the NOMINEE risk signal depend on the word
        # "nominee" appearing in a sentence. Recording the codes machine-readably
        # costs one annotation and does not pre-empt the arrangement modelling.
        for interest_idx, nature in enumerate(natures):
            if interest_idx >= len(rel["recordDetails"].get("interests", [])):
                break
            emitted = rel["recordDetails"]["interests"][interest_idx]
            annotate(
                rel,
                transformation(
                    pointer("recordDetails", "interests", interest_idx, "type"),
                    (
                        "Companies House nature-of-control code "
                        f"'{nature}', mapped to the closest BODS interest type."
                    ),
                    transformed_content=emitted.get("type"),
                    creation_date=_today(),
                ),
            )

        rel_sid = rel["statementId"]
        if rel_sid not in seen_sids:
            result.statements.append(rel)
            seen_sids.add(rel_sid)

    # PSC statements ("no PSC exists", "PSC not yet identified", …) → ownership-
    # or-control statements with an unspecified interestedParty (BODS missing-
    # information modelling, data-standard issue #389).
    statement_items = (bundle.get("psc_statements") or {}).get("items") or []
    for stmt in _ch_psc_statement_statements(number, statement_items, entity_sid, company_url):
        if stmt["statementId"] not in seen_sids:
            result.statements.append(stmt)
            seen_sids.add(stmt["statementId"])

    # Directors → seniorManagingOfficial person + relationship statements.
    director_stmts = _ch_director_statements(
        number, officers_payload, entity_sid, company_url, seen_sids
    )
    result.statements.extend(director_stmts)


def _profile_addresses(profile: dict[str, Any]) -> list[dict[str, str]]:
    ra = profile.get("registered_office_address")
    if not ra:
        return []
    parts = [
        ra.get("care_of"),
        ra.get("po_box"),
        ra.get("address_line_1"),
        ra.get("address_line_2"),
        ra.get("locality"),
        ra.get("region"),
        ra.get("postal_code"),
        ra.get("country"),
    ]
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return []
    return [_addr("registered", joined, ra.get("country", ""))]


def _map_individual_psc(
    company_number: str, psc: dict[str, Any], source_url: str
) -> dict[str, Any]:
    nd = psc.get("name_elements") or {}
    full_name = psc.get("name") or " ".join(
        [nd.get("forename", ""), nd.get("middle_name", ""), nd.get("surname", "")]
    ).strip()

    dob = psc.get("date_of_birth")
    birth_date = None
    if isinstance(dob, dict) and "year" in dob:
        # Companies House exposes month/year only — emit YYYY-MM or YYYY.
        if "month" in dob:
            birth_date = f"{dob['year']:04d}-{dob['month']:02d}"
        else:
            birth_date = f"{dob['year']:04d}"

    nationalities = []
    if psc.get("nationality"):
        nationalities.append({"name": psc["nationality"]})

    # Companies House returns addresses for PSCs under "address".
    address_block = psc.get("address") or {}
    addresses: list[dict[str, str]] = []
    if address_block:
        parts = [
            address_block.get("premises"),
            address_block.get("address_line_1"),
            address_block.get("address_line_2"),
            address_block.get("locality"),
            address_block.get("region"),
            address_block.get("postal_code"),
            address_block.get("country"),
        ]
        joined = ", ".join([p for p in parts if p])
        if joined:
            addresses.append(_addr("service", joined, address_block.get("country", "")))

    etag = psc.get("etag") or psc.get("name", "")
    local_id = f"{company_number}:psc:{etag}"

    return annotate(
        make_person_statement(
            source_id="companies_house",
            local_id=local_id,
            full_name=full_name,
            person_type="knownPerson",
            nationalities=nationalities,
            birth_date=birth_date,
            addresses=addresses,
            source_url=source_url,
        ),
        _birth_date_precision_note(birth_date),
    )


def _map_companies_house_officer(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Companies House officer-appointments bundle to BODS.

    The officer becomes a single ``personStatement``; each appointment
    becomes an ``entityStatement`` (the company appointed-to) plus a
    ``relationship`` statement with a ``boardMember`` interest. Resigned
    appointments carry ``endDate`` so consumers can distinguish current
    from historical board membership.

    The Companies House appointments endpoint returns the officer's
    canonical name + DOB + nationality + occupation + country of
    residence on the *appointments envelope* — those fields are used
    for the personStatement; the per-appointment block carries
    appointment-specific data.
    """
    result = BODSBundle()

    officer_id = str(bundle.get("officer_id", ""))
    appointments = bundle.get("appointments") or {}
    items = appointments.get("items") or []

    full_name = appointments.get("name") or "Unknown officer"
    dob = appointments.get("date_of_birth")
    birth_date = None
    if isinstance(dob, dict) and "year" in dob:
        if "month" in dob:
            birth_date = f"{dob['year']:04d}-{dob['month']:02d}"
        else:
            birth_date = f"{dob['year']:04d}"

    nationalities: list[dict[str, str]] = []
    nationality = appointments.get("nationality")
    if nationality:
        nationalities.append({"name": nationality})

    person_url = (
        f"https://find-and-update.company-information.service.gov.uk/officers/"
        f"{officer_id}/appointments"
    )

    person = make_person_statement(
        source_id="companies_house",
        local_id=f"officer:{officer_id}",
        full_name=full_name,
        person_type="knownPerson",
        nationalities=nationalities,
        birth_date=birth_date,
        identifiers=[
            {
                "id": officer_id,
                "scheme": "GB-COH-OFFICER",
                "schemeName": "Companies House officer id",
            }
        ],
        source_url=person_url,
    )
    annotate(person, _birth_date_precision_note(birth_date))
    result.statements.append(person)
    person_sid = person["statementId"]

    for idx, appointment in enumerate(items):
        appointed_to = appointment.get("appointed_to") or {}
        company_number = str(appointed_to.get("company_number") or f"unknown-{idx}")
        company_name = (
            appointed_to.get("company_name")
            or f"Company {company_number}"
        )
        company_url = (
            f"https://find-and-update.company-information.service.gov.uk/company/"
            f"{company_number}"
        )

        entity = make_entity_statement(
            source_id="companies_house",
            local_id=f"officer:{officer_id}:co:{company_number}",
            name=company_name,
            jurisdiction=("United Kingdom", "GB"),
            identifiers=[
                {
                    "id": company_number,
                    "scheme": "GB-COH",
                    "schemeName": "Companies House",
                }
            ],
            source_url=company_url,
        )
        result.statements.append(entity)
        entity_sid = entity["statementId"]

        # Map the officer role to a BODS interest. Directors and
        # secretaries become boardMember; LLP members are otherInfluence
        # (no board) — but everyone gets the appointment surfaced.
        role = (appointment.get("officer_role") or "").lower()
        if "director" in role:
            interest_type = "boardMember"
        elif "chair" in role:
            interest_type = "boardChair"
        else:
            interest_type = "otherInfluenceOrControl"

        # Use the official CH label rather than passing the raw role code through.
        details_bits = [describe_officer_role(role) or appointment.get("officer_role") or "appointment"]
        if appointment.get("appointed_on"):
            details_bits.append(f"from {appointment['appointed_on']}")
        if appointment.get("resigned_on"):
            details_bits.append(f"to {appointment['resigned_on']}")

        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "details": " ".join(details_bits),
        }
        if appointment.get("appointed_on"):
            interest["startDate"] = appointment["appointed_on"]
        if appointment.get("resigned_on"):
            interest["endDate"] = appointment["resigned_on"]

        rel = make_relationship_statement(
            source_id="companies_house",
            local_id=f"officer-rel:{officer_id}:{company_number}:{idx}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=[interest],
            source_url=person_url,
        )
        result.statements.append(rel)

    return result


def _map_corporate_psc(
    company_number: str,
    psc: dict[str, Any],
    source_url: str,
    *,
    uk_number: str | None = None,
) -> dict[str, Any]:
    """Map a corporate / legal-person PSC to a BODS entityStatement.

    When *uk_number* is provided it is used as the ``local_id`` so that the
    ``statementId`` produced here matches the one emitted when the same
    company is processed as a related-company root (both sides use
    ``local_id = company_number``).  Without this alignment, the dagre
    visualiser can't connect the PSC node to the full ownership subgraph.
    """
    identification = psc.get("identification") or {}
    identifiers: list[dict[str, str]] = []
    reg_number = identification.get("registration_number")
    reg_country = identification.get("country_registered")
    if reg_number:
        alpha2 = _country_code(reg_country)
        place = (identification.get("place_registered") or "").lower()
        # Map well-known registries to their canonical BODS scheme codes;
        # fall back to REG-{alpha2} (2-letter, not the old 3-letter truncation)
        # so reconcilers can bridge to other sources on the same identifier.
        if alpha2 == "GB" and ("companies house" in place or not place):
            scheme = "GB-COH"
            scheme_name = "UK Companies House"
        elif alpha2:
            scheme = f"REG-{alpha2}"
            scheme_name = identification.get("place_registered") or f"{alpha2} company register"
        else:
            scheme = "REG"
            scheme_name = identification.get("place_registered") or "Company register"
        identifiers.append(
            {
                "id": reg_number,
                "scheme": scheme,
                "schemeName": scheme_name,
            }
        )

    # Use the UK company number as local_id when available so that the
    # statementId here aligns with the entity statement emitted by the
    # related-company pass for the same company.
    if uk_number:
        local_id = uk_number
    else:
        etag = psc.get("etag") or psc.get("name", "")
        local_id = f"{company_number}:psc-corp:{etag}"

    return make_entity_statement(
        source_id="companies_house",
        local_id=local_id,
        name=psc.get("name", "Corporate PSC"),
        jurisdiction=(
            (reg_country, _country_code(reg_country))
            if reg_country
            else None
        ),
        identifiers=identifiers,
        source_url=source_url,
    )



# ----------------------------------------------------------------------
# GLEIF → BODS
# ----------------------------------------------------------------------
#
# Mirrors OpenOwnership's canonical GLEIF → BODS pipeline
# (https://github.com/openownership/bods-gleif-pipeline):
#
# * Subject entity: one ``registeredEntity`` statement, identified by LEI
#   (``XI-LEI``) and by the GLEIF ``RegistrationAuthority`` scheme when
#   the record carries a ``registeredAt.id`` (e.g. ``RA000585`` for UK
#   Companies House).
# * Each accounting consolidation parent (direct / ultimate) → one entity
#   statement for the parent + one relationship statement with an
#   ``otherInfluenceOrControl`` interest. ``beneficialOwnershipOrControl``
#   is always ``false`` — LEI-RR captures accounting consolidation, not
#   beneficial ownership.
# * Reporting exceptions (``NO_LEI``, ``NATURAL_PERSONS``,
#   ``NON_CONSOLIDATING`` etc.) produce a bridging statement
#   (``anonymousEntity`` or ``unknownPerson``) plus a relationship whose
#   interest ``details`` carry the GLEIF exception reason — so companies
#   that report "my parent is a natural person" don't silently disappear.

# ---------------------------------------------------------------------------
# RA code → org-id.guide scheme code
#
# Maps GLEIF Registration Authority codes to org-id.guide list identifiers so
# that national company numbers are emitted with a proper ``scheme`` in BODS
# entity statements rather than with ``scheme: ""``.
#
# Source of truth for RA codes: https://api.gleif.org/api/v1/registration-authorities/<RA>
# Source of truth for org-id codes: https://org-id.guide
#
# Extend this dict as further jurisdictions are confirmed.  Unknown RA codes
# fall through to a blank scheme with schemeName "GLEIF Registration Authorities List".
# ---------------------------------------------------------------------------
_GLEIF_RA_TO_ORG_ID: dict[str, tuple[str, str]] = {
    # Estonia — Centre of Registers and Information Systems (RIK)
    "RA000181": ("EE-RIK", "Centre of Registers and Information Systems (Estonia)"),
    # France — Sirene (INSEE)
    "RA000189": ("FR-INSEE", "Sirene — Institut National de la Statistique et des Études Économiques (France)"),
    # Netherlands — Kamer van Koophandel (KvK)
    "RA000463": ("NL-KVK", "Netherlands Chamber of Commerce (KvK)"),
    # Sweden — Bolagsverket (Swedish Companies Registration Office)
    "RA000544": ("SE-ON", "Swedish Companies Registration Office (Bolagsverket)"),
    # Switzerland — Federal Statistical Office UID Register (uid.admin.ch)
    "RA000548": ("CH-FDJP", "Swiss Commercial Register (Federal Office of Justice)"),
    # Switzerland — Handelsregister / ZEFIX (Federal Office of Justice)
    "RA000549": ("CH-FDJP", "Swiss Commercial Register (Federal Office of Justice)"),
    # United Kingdom — Companies House (England & Wales)
    "RA000585": ("GB-COH", "Companies House"),
    # United Kingdom — Companies House (Northern Ireland)
    "RA000586": ("GB-COH", "Companies House"),
    # United Kingdom — Companies House (Scotland). Missing until 2026-08-28,
    # so Scottish entities carried no scheme code on their BODS identifiers.
    "RA000587": ("GB-COH", "Companies House"),
    # RA000591 = The Pensions Regulator (UK) — not a company registry; no org-id code.
    # Belgium — Crossroads Bank for Enterprises (BCE / KBO)
    "RA000025": ("BE-BCE_KBO", "Crossroads Bank for Enterprises (Belgium)"),
}

# ---------------------------------------------------------------------------
# US state company registries
#
# org-id.guide does not carry per-state entries for US company registries.
# GLEIF uses state-level ISO 3166-2 subdivision codes as jurisdiction codes
# (e.g. "US-DE" for Delaware) and assigns a separate RA code to each state
# registry (e.g. RA000602 for Delaware Division of Corporations).
#
# Rather than enumerate all 50+ GLEIF RA codes in _GLEIF_RA_TO_ORG_ID, we
# resolve US-jurisdiction entities by cross-referencing the entity's
# ``jurisdiction`` field: when the RA code is unknown and the jurisdiction
# starts with "US-", we use the ISO 3166-2 subdivision code itself as the
# BODS identifier ``scheme`` and look up the official registry name here.
#
# Scheme example:  {"id": "3954875", "scheme": "US-DE",
#                   "schemeName": "Delaware Division of Corporations"}
# ---------------------------------------------------------------------------
_US_STATE_REGISTRY_NAMES: dict[str, str] = {
    "US-AL": "Alabama Secretary of State",
    "US-AK": "Alaska Division of Corporations, Business & Professional Licensing",
    "US-AZ": "Arizona Corporation Commission",
    "US-AR": "Arkansas Secretary of State",
    "US-CA": "California Secretary of State",
    "US-CO": "Colorado Secretary of State",
    "US-CT": "Connecticut Secretary of State",
    "US-DC": "District of Columbia Department of Licensing and Consumer Protection",
    "US-DE": "Delaware Division of Corporations",
    "US-FL": "Florida Division of Corporations",
    "US-GA": "Georgia Secretary of State",
    "US-HI": "Hawaii Department of Commerce and Consumer Affairs",
    "US-ID": "Idaho Secretary of State",
    "US-IL": "Illinois Secretary of State",
    "US-IN": "Indiana Secretary of State",
    "US-IA": "Iowa Secretary of State",
    "US-KS": "Kansas Secretary of State",
    "US-KY": "Kentucky Secretary of State",
    "US-LA": "Louisiana Secretary of State",
    "US-ME": "Maine Secretary of State",
    "US-MD": "Maryland Department of Assessments and Taxation",
    "US-MA": "Massachusetts Secretary of State",
    "US-MI": "Michigan Department of Licensing and Regulatory Affairs",
    "US-MN": "Minnesota Secretary of State",
    "US-MS": "Mississippi Secretary of State",
    "US-MO": "Missouri Secretary of State",
    "US-MT": "Montana Secretary of State",
    "US-NE": "Nebraska Secretary of State",
    "US-NV": "Nevada Secretary of State",
    "US-NH": "New Hampshire Secretary of State",
    "US-NJ": "New Jersey Division of Revenue and Enterprise Services",
    "US-NM": "New Mexico Secretary of State",
    "US-NY": "New York Department of State",
    "US-NC": "North Carolina Secretary of State",
    "US-ND": "North Dakota Secretary of State",
    "US-OH": "Ohio Secretary of State",
    "US-OK": "Oklahoma Secretary of State",
    "US-OR": "Oregon Secretary of State",
    "US-PA": "Pennsylvania Department of State",
    "US-PR": "Puerto Rico Department of State",
    "US-RI": "Rhode Island Department of State",
    "US-SC": "South Carolina Secretary of State",
    "US-SD": "South Dakota Secretary of State",
    "US-TN": "Tennessee Secretary of State",
    "US-TX": "Texas Secretary of State",
    "US-UT": "Utah Division of Corporations and Commercial Code",
    "US-VT": "Vermont Secretary of State",
    "US-VA": "Virginia State Corporation Commission",
    "US-WA": "Washington Secretary of State",
    "US-WV": "West Virginia Secretary of State",
    "US-WI": "Wisconsin Department of Financial Institutions",
    "US-WY": "Wyoming Secretary of State",
}

# Reporting-exception reasons — GLEIF Level 2 Reporting Exceptions Format 2.1
# (https://www.gleif.org/en/lei-data/access-and-use-lei-data/level-2-data-reporting-exceptions-2-1-format)
# and the GLEIF Reporting Exception Ontology
# (https://www.gleif.org/ontology/v1.0/ReportingException/).
#
# Modelling follows the Open Ownership / Open Data Services GLEIF → BODS
# pipeline (https://github.com/openownership/bods-gleif-pipeline): each
# exception produces a bridging person/entity statement plus a relationship
# whose interest ``details`` carry the exception reason, and every statement
# created from an exception carries a ``commenting`` annotation naming the
# reason — so companies that report "my parent is a natural person" don't
# silently disappear, and consumers can tell an exception bridge from a
# real party.
#
# BODS types are chosen per reason:
# * ``unknownPerson`` — NATURAL_PERSONS / NO_KNOWN_PERSON: any controlling
#   party is a natural person GLEIF does not identify (only entities carry
#   LEIs), or no controlling person is known at all.
# * ``unknownEntity`` — NO_LEI / NON_CONSOLIDATING: a parent entity exists
#   but is not identified in GLEIF (no LEI, or outside the accounting
#   consolidation net). Not *anonymised* — merely not identified here.
# * ``anonymousEntity`` — NON_PUBLIC and its five deprecated variants: a
#   consolidating parent exists and is known but is deliberately withheld
#   from publication. This is the only genuinely opacity-relevant family
#   (see ``risk._opaque_ownership_signals``).
#
# Exception reason → (interested_party_type, person_type or entity_type,
#                     bridge display name, human-readable details).
_GLEIF_EXCEPTION_REASONS = {
    "NATURAL_PERSONS": (
        "person",
        "unknownPerson",
        "Natural person(s) (GLEIF reporting exception)",
        "GLEIF reporting exception NATURAL_PERSONS: the entity is controlled"
        " by natural person(s) without any intermediate legal entity meeting"
        " the definition of accounting consolidating parent",
    ),
    "NO_KNOWN_PERSON": (
        "person",
        "unknownPerson",
        "No known controlling person (GLEIF reporting exception)",
        "GLEIF reporting exception NO_KNOWN_PERSON: there is no known person"
        " controlling the entity (e.g. diversified shareholding)",
    ),
    "NO_LEI": (
        "entity",
        "unknownEntity",
        "Parent without an LEI (GLEIF reporting exception)",
        "GLEIF reporting exception NO_LEI: a parent exists but does not"
        " consent to have an LEI, so it is not identified in GLEIF",
    ),
    "NON_CONSOLIDATING": (
        "entity",
        "unknownEntity",
        "Non-consolidating parent (GLEIF reporting exception)",
        "GLEIF reporting exception NON_CONSOLIDATING: the entity is"
        " controlled by legal entities not subject to preparing consolidated"
        " financial statements",
    ),
    "NON_PUBLIC": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception NON_PUBLIC: a parent exists but the"
        " relationship is non-public and is not disclosed",
    ),
    # The five reasons below were deprecated in Reporting Exceptions Format
    # 2.1 and consolidated under NON_PUBLIC from 2022-03-01; they are kept
    # for records that still carry the old codes.
    "BINDING_LEGAL_COMMITMENTS": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception BINDING_LEGAL_COMMITMENTS (deprecated,"
        " now NON_PUBLIC): binding legal commitments prevent disclosure of"
        " the parent",
    ),
    "LEGAL_OBSTACLES": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception LEGAL_OBSTACLES (deprecated, now"
        " NON_PUBLIC): obstacles in laws or regulations prevent disclosure"
        " of the parent",
    ),
    "DISCLOSURE_DETRIMENTAL": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception DISCLOSURE_DETRIMENTAL (deprecated, now"
        " NON_PUBLIC): disclosure would be detrimental to the entity or its"
        " parent",
    ),
    "DETRIMENT_NOT_EXCLUDED": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception DETRIMENT_NOT_EXCLUDED (deprecated, now"
        " NON_PUBLIC): detriment to the parent from disclosure could not be"
        " excluded",
    ),
    "CONSENT_NOT_OBTAINED": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception CONSENT_NOT_OBTAINED (deprecated, now"
        " NON_PUBLIC): the parent's consent to disclose the relationship"
        " was not obtained",
    ),
}

#: Reasons whose bridge party is deliberately withheld (a parent exists,
#: consolidates, and is known — but is not published). Imported by
#: ``risk.py`` to drive the OPAQUE_OWNERSHIP classification.
GLEIF_UNDISCLOSED_REASONS: frozenset[str] = frozenset(
    {
        "NON_PUBLIC",
        "BINDING_LEGAL_COMMITMENTS",
        "LEGAL_OBSTACLES",
        "DISCLOSURE_DETRIMENTAL",
        "DETRIMENT_NOT_EXCLUDED",
        "CONSENT_NOT_OBTAINED",
    }
)



# ---------------------------------------------------------------------------
# Greek General Commercial Registry (ΓΕΜΗ)
# ---------------------------------------------------------------------------

def _gemi() -> Any:
    """The ΓΕΜΗ adapter module — local import avoids a circular import.

    Several adapters import this mapper, so the mapper cannot import the
    sources package at module scope. Same pattern as ``_role_to_interest``
    (firmenbuch) and ``_cy_field`` (cyprus_drcor) below.
    """
    from ..sources import gemi_greece  # local import avoids circular

    return gemi_greece


#: ``persons[].category`` → how to read that person's relationship to the
#: company. The **category** decides this, not the free-text ``role``:
#: ``Εταίροι`` (partners) carry real ``percentage`` values, while
#: ``Διοικητικό συμβούλιο`` (board of directors) always carry ``"-"``.
#: Verified against live records for an ΑΕ, an ΙΚΕ and an ΕΕ on 2026-08-28.
_GEMI_CATEGORY_INTERESTS: dict[str, str] = {
    "Εταίροι": "shareholding",              # gemi_greece.CATEGORY_PARTNERS
    "Διοικητικό συμβούλιο": "seniorManagingOfficial",  # …CATEGORY_BOARD
}

#: Role fragments that indicate management or representation *in addition to*
#: whatever the category says. A partner who is also a Διαχειριστής holds two
#: distinct interests — a shareholding and a management role — and flattening
#: them into one would lose a fact the register published.
_GEMI_MANAGER_TOKENS = ("διαχειριστ", "εκπρόσωπ", "εκπροσωπ")

#: Greek partnership membership classes. The distinction is substantive —
#: an ομόρρυθμος partner has unlimited liability, an ετερόρρυθμος one does
#: not — so it is preserved rather than flattened to "partner".
_GEMI_PARTNER_CLASSES: dict[str, str] = {
    "ομόρρυθμο": "General partner (ομόρρυθμο μέλος) — unlimited liability",
    "ετερόρρυθμο": "Limited partner (ετερόρρυθμο μέλος) — limited liability",
}

#: ΓΕΜΗ emits already-ISO dates (``"1998-09-16"``). Declared here rather than
#: reused from ``findings`` — that module imports from ``sources``, which
#: imports adapters that import this mapper.
_GEMI_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _gemi_date(value: Any) -> str | None:
    """A ΓΕΜΗ date as ``YYYY-MM-DD``, or None.

    Dates arrive already ISO-formatted (``"1998-09-16"``); anything else is
    dropped rather than guessed at.
    """
    text = str(value or "").strip()
    return text[:10] if _GEMI_ISO_DATE.match(text) else None


def _gemi_is_past(value: str | None) -> bool:
    """True when a date is strictly in the past.

    Board appointments carry a **fixed future** ``dtTo`` (a term expiry, e.g.
    2023-10-27 → 2028-10-27). Treating that as a closed record would report
    every sitting Greek director as departed, so only a past ``dtTo`` closes a
    relationship.
    """
    return bool(value) and value < _today()


def _gemi_addresses(company: dict[str, Any]) -> list[dict[str, str]]:
    street = " ".join(
        part for part in (
            str(company.get("street") or "").strip(),
            str(company.get("streetNumber") or "").strip(),
        ) if part
    )
    municipality = _gemi().english_label("municipalities", company.get("municipality"))
    parts = [
        street,
        str(company.get("city") or "").strip(),
        municipality,
        str(company.get("zipCode") or "").strip(),
    ]
    address = ", ".join(part for part in parts if part)
    return [_addr("registered", address, "GR")] if address else []


def _gemi_identifiers(company: dict[str, Any]) -> list[dict[str, str]]:
    """Only identifiers ΓΕΜΗ itself publishes — never the LEI we arrived by."""
    identifiers: list[dict[str, str]] = []
    argemi = str(company.get("arGemi") or "").strip()
    if argemi:
        identifiers.append({
            "id": argemi,
            "scheme": "GR-GEMI",
            "schemeName": "General Commercial Registry (ΓΕΜΗ)",
        })
    afm = str(company.get("afm") or "").strip()
    if afm:
        identifiers.append({
            "id": afm,
            "scheme": "GR-AFM",
            "schemeName": "Greek Tax Registry Number (ΑΦΜ)",
        })
    return identifiers


def _gemi_person_local_id(argemi: str, person: dict[str, Any], index: int) -> str:
    """A stable local id for one person entry.

    ΓΕΜΗ publishes no personal identifier — no birth date, no tax number, just
    a name — so the id is derived from the name plus the role and start date
    that distinguish two entries for the same name. ``index`` is the final
    tie-breaker so two byte-identical rows still get distinct statements
    rather than silently collapsing into one.
    """
    name = str(person.get("personName") or person.get("businessName") or "").strip()
    return "|".join([
        argemi,
        name.casefold(),
        str(person.get("role") or "").strip().casefold(),
        str(person.get("dtFrom") or ""),
        str(index),
    ])


def _gemi_interests(person: dict[str, Any], source_id: str) -> list[dict[str, Any]]:
    """Interests for one ``persons[]`` entry.

    Branches on ``category`` and then refines with ``role``. A partner who is
    also a manager gets two interests, not one.

    ``beneficialOwnershipOrControl`` is deliberately never set: ΓΕΜΗ is a
    commercial register recording legal and registered holdings, and Greece's
    beneficial ownership register (Κεντρικό Μητρώο Πραγματικών Δικαιούχων) is
    a separate, non-public register. ``set_beneficial_ownership`` with
    ``asserted=None`` omits the flag for any source not in
    ``_BO_ASSERTING_SOURCES``, which ΓΕΜΗ is not — BODS reads the absence as
    "not stated", which is the honest claim.
    """
    category = str(person.get("category") or "").strip()
    role = str(person.get("role") or "").strip()
    role_folded = role.casefold()
    start = _gemi_date(person.get("dtFrom"))
    end = _gemi_date(person.get("dtTo"))

    def _build(interest_type: str, *, share: float | None = None) -> dict[str, Any]:
        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
        }
        if share is not None:
            interest["share"] = {"exact": share}
        if start:
            interest["startDate"] = start
        if end:
            interest["endDate"] = end
        if role:
            interest["details"] = role
        return set_beneficial_ownership(interest, source_id, asserted=None)

    interests: list[dict[str, Any]] = []
    primary = _GEMI_CATEGORY_INTERESTS.get(category)

    if primary == "shareholding":
        interests.append(_build("shareholding", share=_gemi().parse_percentage(person.get("percentage"))))
        # A partner who also manages or represents the company holds a second,
        # different interest. Board members already map to the management
        # interest, so this only applies on the partner branch.
        if any(token in role_folded for token in _GEMI_MANAGER_TOKENS):
            interests.append(_build("seniorManagingOfficial"))
    elif primary:
        interests.append(_build(primary))
    else:
        # An unrecognised category is recorded, not dropped: the raw Greek
        # survives in ``details`` so a reviewer can see what the register said.
        interests.append(_build("otherInfluenceOrControl"))

    return interests


def _gemi_partner_class(role: str) -> str | None:
    """The general/limited partnership class named in a role string, if any."""
    folded = role.casefold()
    for token, description in _GEMI_PARTNER_CLASSES.items():
        if token in folded:
            return description
    return None


def map_gemi_greece(bundle: dict[str, Any]) -> BODSBundle:
    """Map a ΓΕΜΗ fetch bundle to BODS v0.4 statements.

    Produces one entity statement for the company, then a person or entity
    statement plus a relationship statement for each ``persons[]`` entry.

    An **ΑΕ (société anonyme) yields officers but no owners** — its share
    register is not part of ΓΕΜΗ publicity. That is the Greek regime, not an
    absence of data, and callers must not present it as ownership being
    withheld.
    """
    statements = BODSBundle()
    company = bundle.get("company")
    if not isinstance(company, dict):
        return statements

    argemi = str(company.get("arGemi") or bundle.get("gr_argemi") or "").strip()
    if not argemi:
        return statements

    source_id = bundle.get("source_id") or "gemi_greece"
    url = _gemi().company_url(argemi)

    name = str(company.get("coNameEl") or bundle.get("legal_name") or "").strip()
    # Deduplicated: the Latin name and the Latin distinctive title are often
    # the same string (BUTTON P.C. carries "BUTTON" as both coTitlesEl and
    # coTitlesEn), and BODS alternateNames should not repeat itself.
    alternates: list[str] = []
    for key in ("coNamesEn", "coTitlesEl", "coTitlesEn"):
        for value in (company.get(key) or []):
            text = str(value).strip()
            if text and text != name and text not in alternates:
                alternates.append(text)

    # A dissolved company's ``lastStatusChange`` is when it left the register.
    # It is only a dissolution date when the status is actually inactive —
    # for a live company the same field is just the last time anything moved.
    is_active = _gemi().status_is_active(company.get("status"))
    last_change = _gemi_date(company.get("lastStatusChange"))
    dissolution = last_change if is_active is False else None

    status_label = _gemi().english_label("companyStatuses", company.get("status"))
    legal_form = _gemi().english_label("legalTypes", company.get("legalType"))
    details = " · ".join(part for part in (legal_form, status_label) if part) or None

    entity = make_entity_statement(
        source_id=source_id,
        local_id=argemi,
        name=name,
        jurisdiction=("Greece", "GR"),
        identifiers=_gemi_identifiers(company),
        founding_date=_gemi_date(company.get("incorporationDate")),
        addresses=_gemi_addresses(company),
        alternate_names=alternates,
        entity_details=details,
        source_url=url,
    )
    # Shared liveness path (Phase 151): the codelist's ``isActive`` is the
    # classification; an unknown status stays unknown, never "inactive".
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["gemi_greece"],
        liveness=(
            _liveness.LIVE if is_active is True
            else _liveness.TERMINAL if is_active is False
            else _liveness.UNKNOWN
        ),
        raw=status_label,
        since=dissolution,
    )
    statements.extend([entity])

    for index, person in enumerate(company.get("persons") or []):
        if not isinstance(person, dict):
            continue
        person_name = str(person.get("personName") or "").strip()
        business_name = str(person.get("businessName") or "").strip()
        if not person_name and not business_name:
            continue

        local_id = _gemi_person_local_id(argemi, person, index)
        role = str(person.get("role") or "").strip()

        if business_name:
            # A partner that is itself a company. ΓΕΜΗ gives only its name
            # here — no ΓΕΜΗ number for the holder — so it is an entity
            # statement with no identifiers rather than a resolvable link.
            party = make_entity_statement(
                source_id=source_id,
                local_id=f"party:{local_id}",
                name=business_name,
                entity_type="legalEntity",
                entity_details=_gemi_partner_class(role),
                source_url=url,
            )
            party_type = "entity"
        else:
            party = make_person_statement(
                source_id=source_id,
                local_id=f"party:{local_id}",
                full_name=person_name,
                source_url=url,
            )
            party_type = "person"
        statements.extend([party])

        interests = _gemi_interests(person, source_id)
        ended = _gemi_is_past(_gemi_date(person.get("dtTo")))
        statements.extend([
            make_relationship_statement(
                source_id=source_id,
                local_id=local_id,
                subject_statement_id=entity["recordId"],
                interested_party_statement_id=party["recordId"],
                interested_party_type=party_type,
                interests=interests,
                source_url=url,
                record_status="closed" if ended else "new",
            )
        ])

    return statements

def map_gleif(bundle: dict[str, Any]) -> BODSBundle:
    """Map a GLEIF adapter bundle to BODS v0.4 statements.

    Input shape matches ``GleifAdapter.fetch`` output:

        {
          "lei": ...,
          "record": {...},                            # Level 1 CDF
          "direct_parent": {...} | None,              # Level 2 RR
          "ultimate_parent": {...} | None,            # Level 2 RR
          "direct_parent_exception": {...} | None,    # Reporting exception
          "ultimate_parent_exception": {...} | None,  # Reporting exception
        }
    """
    result = BODSBundle()

    record = bundle.get("record") or {}
    subject_attrs = record.get("attributes") or record
    subject_entity_block = subject_attrs.get("entity") or {}
    lei = (
        bundle.get("lei")
        or subject_attrs.get("lei")
        or record.get("id")
        or ""
    )
    if not lei:
        return result

    subject_url = f"https://www.gleif.org/lei/{lei}"
    # The subject record's own last-update date, reused for the Level 2
    # relationship statements it reports.
    subject_statement_date = _gleif_registration_date(subject_attrs)
    subject_statement = _gleif_entity_statement(
        lei, subject_entity_block, subject_url, attrs=subject_attrs
    )
    result.statements.append(subject_statement)
    subject_sid = subject_statement["statementId"]

    for kind, parent, exception in (
        (
            "direct",
            bundle.get("direct_parent"),
            bundle.get("direct_parent_exception"),
        ),
        (
            "ultimate",
            bundle.get("ultimate_parent"),
            bundle.get("ultimate_parent_exception"),
        ),
    ):
        if parent:
            result.extend(
                _gleif_parent_statements(
                    lei, subject_sid, kind, parent, subject_statement_date
                )
            )
        elif exception:
            result.extend(
                _gleif_exception_statements(
                    lei, subject_sid, kind, exception, subject_statement_date
                )
            )

    for child in bundle.get("direct_children") or []:
        result.extend(
            _gleif_child_statements(lei, subject_sid, child, subject_statement_date)
        )

    return result


def _gleif_parent_statements(
    lei: str,
    subject_sid: str,
    kind: str,
    parent: dict[str, Any],
    subject_statement_date: str | None = None,
) -> list[dict[str, Any]]:
    """Emit entity + relationship statements for one GLEIF Level 2 parent.

    ``subject_statement_date`` is the subject LEI record's
    ``registration.lastUpdateDate``. GLEIF's parent endpoints return the
    *parent's* Level 1 record, not the relationship (RR) record, so the RR's own
    update date is not available to us; the subject's is the closest thing we
    genuinely hold, since the Level 2 relationship is reported by the subject.
    """
    parent_attrs = parent.get("attributes") or parent
    parent_entity_block = parent_attrs.get("entity") or {}
    parent_lei = parent_attrs.get("lei") or parent.get("id") or ""
    if not parent_lei:
        return []

    parent_url = f"https://www.gleif.org/lei/{parent_lei}"
    parent_statement = _gleif_entity_statement(
        parent_lei, parent_entity_block, parent_url, attrs=parent_attrs
    )
    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:{kind}-parent:{parent_lei}",
        subject_statement_id=subject_sid,
        interested_party_statement_id=parent_statement["statementId"],
        interested_party_type="entity",
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct" if kind == "direct" else "indirect",
                "beneficialOwnershipOrControl": False,
                "details": (
                    f"GLEIF Level 2 {kind}-parent (accounting consolidation)"
                ),
            }
        ],
        source_url=parent_url,
        statement_date=subject_statement_date,
    )
    return [parent_statement, rel]


def _gleif_child_statements(
    lei: str,
    subject_sid: str,
    child: dict[str, Any],
    subject_statement_date: str | None = None,
) -> list[dict[str, Any]]:
    """Emit entity + relationship statements for one GLEIF direct subsidiary.

    Relationship direction (mirrors the parent case but inverted):
    * ``subject``           = child entity  (the one being controlled)
    * ``interestedParty``   = queried entity (the one doing the controlling)

    Only the first page of children is passed in here; the total count is
    surfaced separately via the bundle's ``direct_children_total`` field
    (stored in the GLEIF hit's ``raw`` dict for the frontend to read).
    """
    child_attrs = child.get("attributes") or child
    child_entity_block = child_attrs.get("entity") or {}
    child_lei = child_attrs.get("lei") or child.get("id") or ""
    if not child_lei:
        return []

    child_url = f"https://www.gleif.org/lei/{child_lei}"
    child_statement = _gleif_entity_statement(
        child_lei, child_entity_block, child_url, attrs=child_attrs
    )
    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:direct-child:{child_lei}",
        subject_statement_id=child_statement["statementId"],
        interested_party_statement_id=subject_sid,
        interested_party_type="entity",
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": "GLEIF Level 2 direct-child (accounting consolidation)",
            }
        ],
        source_url=child_url,
        statement_date=subject_statement_date,
    )
    return [child_statement, rel]


def map_gleif_subsidiaries(
    subject_lei: str,
    subject_attrs: dict[str, Any],
    children: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Map a subject entity + its merged direct/ultimate children to BODS.

    Used by the lazy ``/subsidiaries`` reveal. ``children`` is a list of
    ``{"record": <GLEIF L1 data object>, "relations": ["direct"|"ultimate", …]}``.
    A child that is **both** a direct and an ultimate child gets **two**
    relationshipStatements (``directOrIndirect`` ``direct`` and ``indirect``) —
    the graph merges them into one annotated edge, but the statements stay
    distinct in the data and the export.
    """
    if not subject_lei:
        return []
    subj_url = f"https://www.gleif.org/lei/{subject_lei}"
    subj = _gleif_entity_statement(
        subject_lei, (subject_attrs or {}).get("entity") or {}, subj_url,
        attrs=subject_attrs,
    )
    out: list[dict[str, Any]] = [subj]
    subj_sid = subj["statementId"]
    seen: set[str] = set()
    for c in children:
        rec = c.get("record") or {}
        attrs = rec.get("attributes") or rec
        child_lei = attrs.get("lei") or rec.get("id") or ""
        if not child_lei or child_lei in seen:
            continue
        seen.add(child_lei)
        child_url = f"https://www.gleif.org/lei/{child_lei}"
        child_stmt = _gleif_entity_statement(
            child_lei, attrs.get("entity") or {}, child_url, attrs=attrs
        )
        out.append(child_stmt)
        for kind in sorted(set(c.get("relations") or [])):
            out.append(make_relationship_statement(
                source_id="gleif",
                local_id=f"{subject_lei}:{kind}-child:{child_lei}",
                subject_statement_id=child_stmt["statementId"],
                interested_party_statement_id=subj_sid,
                interested_party_type="entity",
                interests=[{
                    "type": "otherInfluenceOrControl",
                    "directOrIndirect": "direct" if kind == "direct" else "indirect",
                    "beneficialOwnershipOrControl": False,
                    "details": f"GLEIF Level 2 {kind}-child (accounting consolidation)",
                }],
                source_url=child_url,
            ))
    return out


def _gleif_exception_statements(
    lei: str,
    subject_sid: str,
    kind: str,
    exception: dict[str, Any],
    subject_statement_date: str | None = None,
) -> list[dict[str, Any]]:
    """Emit a bridging person/entity statement + relationship for an exception.

    Mirrors the Open Ownership GLEIF pipeline's reporting-exception handling:
    the bridge and relationship each carry a ``commenting`` annotation naming
    the exception reason, and the relationship interest ``details`` carry the
    reason's meaning plus the exception category (and the legal
    ``ExceptionReference`` when the entity supplied one).
    """
    attrs = exception.get("attributes") or exception
    # Live GLEIF API uses "reason"; OO SQLite dump uses "exceptionReason".
    reason = (attrs.get("reason") or attrs.get("exceptionReason") or "").upper()
    category = (attrs.get("category") or attrs.get("exceptionCategory") or "").upper()
    reference = attrs.get("reference") or attrs.get("exceptionReference") or ""
    ip_type, ip_subtype, bridge_name, details = _GLEIF_EXCEPTION_REASONS.get(
        reason,
        (
            "entity",
            "unknownEntity",
            "Unknown parent (GLEIF reporting exception)",
            f"GLEIF reporting exception: {reason or 'unspecified reason'}",
        ),
    )
    if category:
        details += f" (ExceptionCategory: {category})"
    if reference:
        details += f"; legal reference: {reference}"

    exception_note = commenting(
        "/",
        (
            f"This statement was created due to a {reason or 'GLEIF'}"
            f" GLEIF Reporting Exception for {lei}. Reporting exceptions are"
            " permitted reasons, defined by the LEI ROC policy, for an entity"
            " not to report an accounting consolidation parent."
        ),
        creation_date=_today(),
    )

    bridge_local_id = f"{lei}:{kind}-parent-exception:{reason or 'unspecified'}"
    if ip_type == "person":
        bridge = make_person_statement(
            source_id="gleif",
            local_id=bridge_local_id,
            full_name=bridge_name,
            person_type=ip_subtype,
            source_url=f"https://www.gleif.org/lei/{lei}",
        )
    else:
        bridge = make_entity_statement(
            source_id="gleif",
            local_id=bridge_local_id,
            name=bridge_name,
            entity_type=ip_subtype,
            source_url=f"https://www.gleif.org/lei/{lei}",
        )
    annotate(bridge, dict(exception_note))

    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:{kind}-parent-exception-rel:{reason or 'unspecified'}",
        subject_statement_id=subject_sid,
        interested_party_statement_id=bridge["statementId"],
        interested_party_type=ip_type,
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct" if kind == "direct" else "indirect",
                "beneficialOwnershipOrControl": False,
                "details": details,
            }
        ],
        source_url=f"https://www.gleif.org/lei/{lei}",
        statement_date=subject_statement_date,
    )
    annotate(rel, dict(exception_note))
    return [bridge, rel]


def _gleif_scalar(value: Any) -> str:
    """Coerce a GLEIF attribute expected to be a single string.

    GLEIF returns ``ocid`` / ``qcc`` as scalar strings. Guard against a list
    (take the first non-empty element) or ``None`` so a schema quirk can never
    put a Python list into a BODS identifier ``id``. Returns ``""`` when empty.
    """
    if isinstance(value, list):
        value = next((v for v in value if v), None)
    if value is None:
        return ""
    return str(value).strip()


def _gleif_id_values(value: Any) -> list[str]:
    """Normalise a GLEIF multi-valued identifier field to a list of strings.

    ``bic`` / ``mic`` / ``spglobal`` are arrays in the live GLEIF API — an
    entity can hold many BICs (Deutsche Bank carries 70+) and an exchange
    operator several MICs (London Stock Exchange has ``["ECHO", "XLON"]``) —
    but GLEIF has historically returned a bare string for single-valued cases.
    Accept ``str``, ``list`` or ``None``; return de-duplicated,
    order-preserving, non-empty trimmed strings so *every* available
    identifier is linked to the LEI, not just the first.
    """
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item is None:
            continue
        s = str(item).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _gleif_registration_date(attrs: dict[str, Any] | None) -> str | None:
    """GLEIF ``registration.lastUpdateDate`` as a plain ISO date.

    GLEIF publishes it with a time component (e.g. "2023-03-31T07:01:00Z");
    BODS date fields want ``YYYY-MM-DD``, so take the date portion.
    """
    registration = (attrs or {}).get("registration") or {}
    last_update = registration.get("lastUpdateDate") or ""
    return last_update[:10] or None


def _gleif_entity_statement(
    lei: str,
    entity_block: dict[str, Any],
    source_url: str,
    *,
    attrs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a BODS entity statement from a GLEIF Level 1 entity block.

    ``attrs`` is the full ``record.attributes`` dict (one level above
    ``entity``). It carries the cross-reference identifiers that GLEIF
    publishes via its LEI Mapping programme:

    * ``ocid``     — OpenCorporates identifier (e.g. ``"gb/00102498"``)
    * ``qcc``      — QCC Global Enterprise Identifier / QCC Code (e.g. ``"QGBVC89DTN"``)
    * ``mic``      — Market Identifier Code ISO 10383 (e.g. ``"XLON"``)
    * ``bic``      — Bank Identifier Code ISO 9362 (e.g. ``"BARCGB22"``)
    * ``spglobal`` — S&P CIQ Company ID (e.g. ``"32307"`` for NVIDIA). Published
                     via GLEIF's LEI Mapping programme; S&P Global is not currently
                     listed on org-id.guide so the scheme is recorded as
                     ``"S&P CIQ Company ID"``.

    These are mapped to BODS identifiers when non-null, enabling
    downstream adapters to use them for additional cross-source queries.
    """
    legal_name = (entity_block.get("legalName") or {}).get("name") or f"LEI {lei}"
    jurisdiction_code = entity_block.get("jurisdiction")
    jurisdiction: tuple[str, str] | None = None
    if jurisdiction_code:
        jurisdiction = _gleif_jurisdiction(jurisdiction_code)

    identifiers: list[dict[str, str]] = [
        {
            "id": lei,
            "scheme": "XI-LEI",
            "schemeName": "Global Legal Entity Identifier Index",
        }
    ]

    # GLEIF records the registration authority in ``entity.registeredAt``:
    #   {"id": "RA000585", "other": null}   # standard RA code
    #   {"id": "RA999999", "other": "My Authority"}   # free-text authority
    #
    # Resolution priority:
    #  1. Known RA code in _GLEIF_RA_TO_ORG_ID → use the mapped org-id scheme.
    #  2. Unknown RA code + US-* jurisdiction → use the ISO 3166-2 subdivision
    #     code as scheme (e.g. "US-DE") and look up the registry name in
    #     _US_STATE_REGISTRY_NAMES.  org-id.guide has no per-state US entries
    #     but ISO 3166-2 codes are unambiguous and machine-readable.
    #  3. Anything else → blank scheme, "GLEIF Registration Authorities List".
    registered_as = entity_block.get("registeredAs")
    registered_at = entity_block.get("registeredAt") or {}
    ra_id = registered_at.get("id")
    if registered_as and ra_id:
        if ra_id in _GLEIF_RA_TO_ORG_ID:
            org_id_scheme, org_id_name = _GLEIF_RA_TO_ORG_ID[ra_id]
        elif jurisdiction_code and jurisdiction_code.upper().startswith("US-"):
            state_code = jurisdiction_code.upper()
            org_id_scheme = state_code
            org_id_name = _US_STATE_REGISTRY_NAMES.get(
                state_code,
                f"{state_code} company registry",
            )
        else:
            org_id_scheme, org_id_name = "", "GLEIF Registration Authorities List"
        identifiers.append(
            {
                "id": registered_as,
                "scheme": org_id_scheme,
                "schemeName": org_id_name,
            }
        )

    # GLEIF LEI Mapping cross-reference identifiers (from ``record.attributes``).
    # GLEIF surfaces its BIC-to-LEI, MIC-to-LEI and OpenCorporates / S&P Global /
    # QCC mapping programmes inline on every LEI record. ``ocid`` and ``qcc`` are
    # single strings; ``bic``, ``mic`` and ``spglobal`` are arrays (an entity can
    # hold dozens of BICs and an exchange operator several MICs). We emit one
    # BODS identifier per value so *all* available identifiers are linked to the
    # LEI — the richer the identifier graph, the more datasets the LEI connects.
    # Each is only included when the GLEIF API returns a non-null value.
    if attrs:
        ocid = _gleif_scalar(attrs.get("ocid"))
        if ocid:
            identifiers.append(
                {
                    "id": ocid,
                    "scheme": "OpenCorporates",
                    "schemeName": "OpenCorporates company ID",
                    "uri": f"https://opencorporates.com/companies/{ocid}",
                }
            )

        qcc = _gleif_scalar(attrs.get("qcc"))
        if qcc:
            identifiers.append(
                {
                    "id": qcc,
                    "scheme": "QCC Code",
                    "schemeName": "QCC Global Enterprise Identifier (QCC Code)",
                }
            )

        # MIC — one identifier per Market Identifier Code (ISO 10383).
        for mic_val in _gleif_id_values(attrs.get("mic")):
            identifiers.append(
                {
                    "id": mic_val,
                    "scheme": "ISO-10383",
                    "schemeName": "Market Identifier Code (ISO 10383)",
                }
            )

        # BIC — one identifier per Bank Identifier Code (ISO 9362).
        for bic_val in _gleif_id_values(attrs.get("bic")):
            identifiers.append(
                {
                    "id": bic_val,
                    "scheme": "ISO-9362",
                    "schemeName": "Bank Identifier Code (ISO 9362)",
                }
            )

        # S&P CIQ Company ID — one identifier per value. S&P Global is not
        # currently listed on org-id.guide so the scheme is recorded as a
        # descriptive string per BODS v0.4 guidance.
        for spglobal_val in _gleif_id_values(attrs.get("spglobal")):
            identifiers.append(
                {
                    "id": spglobal_val,
                    "scheme": "S&P CIQ Company ID",
                    "schemeName": "S&P CIQ Company ID",
                }
            )

    addresses = _gleif_addresses(entity_block)

    # Collect alternate names from otherNames and transliteratedOtherNames,
    # deduplicating and excluding the primary legal name.
    seen_names: set[str] = {legal_name}
    alternate_names: list[str] = []
    for name_block in (
        *(entity_block.get("otherNames") or []),
        *(entity_block.get("transliteratedOtherNames") or []),
    ):
        n = (name_block.get("name") or "").strip()
        if n and n not in seen_names:
            seen_names.add(n)
            alternate_names.append(n)

    # GLEIF's registration.lastUpdateDate is when GLEIF last asserted this
    # record's contents — the source's own declaration date, so it is a
    # statementDate. It is NOT a publicationDate: publicationDetails describes
    # OpenCheck's publication of this statement, and OpenCheck published it now.
    gleif_statement_date = _gleif_registration_date(attrs)

    # entity.creationDate → foundingDate (ISO 8601 date or datetime; take date part).
    creation_date_raw = entity_block.get("creationDate") or ""
    founding_date = creation_date_raw[:10] if creation_date_raw else None

    # entity.status (ACTIVE / INACTIVE) is the LEI system's view of whether the
    # legal entity still exists; entity.expiration.date/.reason say when and
    # why it stopped (dissolved, merged, ...). Both go through the shared
    # liveness path (Phase 151): INACTIVE without an expiration date is now
    # visible as a status annotation rather than lost. ``registration.status``
    # (ISSUED / LAPSED / RETIRED / ...) is a different question — whether the
    # LEI *record* is maintained — and is deliberately not read as liveness.
    expiration = entity_block.get("expiration") or {}
    expiration_date_raw = expiration.get("date") or ""
    expiration_date = expiration_date_raw[:10] if expiration_date_raw else None
    entity_status = str(entity_block.get("status") or "")

    stmt = make_entity_statement(
        source_id="gleif",
        local_id=lei,
        name=legal_name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        addresses=addresses,
        alternate_names=alternate_names,
        founding_date=founding_date,
        source_url=source_url,
        statement_date=gleif_statement_date,
    )
    gleif_liveness = _liveness.classify(
        entity_status, live=("ACTIVE",), terminal=("INACTIVE",)
    )
    if gleif_liveness == _liveness.UNKNOWN and expiration_date:
        # An expiration date with no status field (older cached records)
        # still means the entity ended.
        gleif_liveness = _liveness.TERMINAL
    raw_status = entity_status
    if expiration.get("reason"):
        raw_status = f"{entity_status} ({expiration['reason']})" if entity_status else str(expiration["reason"])
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["gleif"],
        liveness=gleif_liveness,
        raw=raw_status or None,
        since=expiration_date,
    )

    # Resolve GLEIF's ISO 20275 legal-form code (entity.legalForm.id, e.g.
    # "2JZ4" = "Foundation") to a human label carried as the non-schema
    # `legalFormLabel` annotation. This is what the AMLA trust/arrangement risk
    # signal keys off, so a GLEIF-only foundation/trust (no national-register
    # hit) is still caught — matching the legal form, never the entity name.
    legal_form_label = resolve_elf((entity_block.get("legalForm") or {}).get("id"))
    if legal_form_label:
        stmt["recordDetails"]["legalFormLabel"] = legal_form_label

    return stmt


def _gleif_jurisdiction(code: str) -> tuple[str, str]:
    """Resolve a GLEIF jurisdiction code to ``(name, code)``.

    GLEIF uses ISO 3166-1 alpha-2 codes at the country level and
    ISO 3166-2 codes (e.g. ``GB-ENG``) at the subdivision level.
    """
    upper = code.upper()
    alpha_2 = upper.split("-")[0]
    country = pycountry.countries.get(alpha_2=alpha_2)
    if not country:
        return (code, code)
    if "-" in upper:
        subdivision = pycountry.subdivisions.get(code=upper)
        if subdivision:
            return (f"{subdivision.name}, {country.name}", upper)
    return (country.name, alpha_2)


def _gleif_addresses(entity_block: dict[str, Any]) -> list[dict[str, str]]:
    addresses: list[dict[str, str]] = []
    legal_address = entity_block.get("legalAddress")
    if legal_address:
        addresses.append(_gleif_address(legal_address, address_type="registered"))
    hq_address = entity_block.get("headquartersAddress")
    if hq_address:
        addresses.append(_gleif_address(hq_address, address_type="business"))
    return addresses


def _gleif_address(block: dict[str, Any], *, address_type: str) -> dict[str, Any]:
    parts = [
        *(block.get("addressLines") or []),
        block.get("city"),
        block.get("region"),
        block.get("postalCode"),
        block.get("country"),
    ]
    joined = ", ".join([p for p in parts if p])
    return _addr(address_type, joined, block.get("country", ""))


# ----------------------------------------------------------------------
# Zefix (Swiss Federal Commercial Registry) → BODS
# ----------------------------------------------------------------------
#
# Zefix exposes the ``CompanyFull`` schema via ``GET /api/v1/company/uid/{uid}``.
# We map the entity-level fields to a BODS entity statement.  Zefix does not
# expose natural persons through this API, so only entity statements are emitted.
#
# Identifier scheme: ``CH-FDJP`` for the Swiss Unternehmens-Identifikationsnummer
# (UID), issued by the Federal Department of Justice and Police (FDJP) via
# the commercial register.  Format used in BODS identifiers is ``CHE-NNN.NNN.NNN``
# (the official display format).
#
# The ``bundle`` shape expected here matches what ZefixAdapter.fetch() returns:
#   {
#     "source_id": "zefix",
#     "uid": "CHE313550547",        # normalised (no separators)
#     "company": {<CompanyFull>},   # from Zefix API
#     "is_stub": False,
#   }

import re as _re

_ZEFIX_UID_RE = _re.compile(r"CHE(\d{3})(\d{3})(\d{3})", _re.IGNORECASE)

_ZEFIX_CANTON_TO_NAME: dict[str, str] = {
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Geneva", "GL": "Glarus", "GR": "Graubünden",
    "JU": "Jura", "LU": "Lucerne", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen", "SO": "Solothurn",
    "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino", "UR": "Uri",
    "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zurich",
}


def _zefix_format_uid(uid: str) -> str:
    """``CHE313550547`` → ``CHE-313.550.547`` (official display format)."""
    m = _ZEFIX_UID_RE.match(uid.strip())
    if m:
        return f"CHE-{m.group(1)}.{m.group(2)}.{m.group(3)}"
    return uid


def map_zefix(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Zefix fetch bundle to BODS v0.4 entity statements.

    Returns an empty iterable for stub bundles or missing company data.
    Only entity statements are emitted — Zefix does not expose natural persons.
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    uid_raw: str = company.get("uid") or bundle.get("uid") or ""
    name: str = company.get("name") or ""
    if not uid_raw or not name:
        return

    uid_display = _zefix_format_uid(uid_raw)
    canton: str = company.get("canton") or ""

    # Jurisdiction: use canton subdivision code where available (e.g. CH-ZH),
    # falling back to country-level CH.
    if canton and canton.upper() in _ZEFIX_CANTON_TO_NAME:
        jur_code = f"CH-{canton.upper()}"
        jur_name = f"{_ZEFIX_CANTON_TO_NAME[canton.upper()]}, Switzerland"
    else:
        jur_code = "CH"
        jur_name = "Switzerland"

    # Identifiers — Swiss UID is the primary cross-reference.
    identifiers: list[dict[str, str]] = [
        {
            "id": uid_display,
            "scheme": "CH-FDJP",
            "schemeName": "Swiss commercial register (Federal Department of Justice and Police)",
        }
    ]
    # EHRA-ID (internal Zefix identifier) as a secondary cross-reference.
    # Uses CH-COA (generic Swiss company register) since the EHRAID is a
    # Zefix-internal key distinct from the publicly-issued UID.
    ehraid = company.get("ehraid")
    if ehraid is not None:
        identifiers.append(
            {
                "id": str(ehraid),
                "scheme": "CH-COA",
                "schemeName": "Zefix (FCRO/EHRA) internal ID",
            }
        )

    # Address
    addr_block = company.get("address") or {}
    addresses = _zefix_address(addr_block)

    source_url = (
        ((company.get("zefixDetailWeb") or {}).get("en"))
        or company.get("cantonalExcerptWeb")
        or f"https://www.zefix.ch/en/search/entity/list/firm/{company.get('ehraid', '')}"
    )

    entity = make_entity_statement(
        source_id="zefix",
        local_id=uid_raw,
        name=name,
        jurisdiction=(jur_name, jur_code),
        identifiers=identifiers,
        addresses=addresses,
        source_url=source_url or None,
    )
    # Zefix ``status``: ACTIVE / BEING_CANCELLED / CANCELLED (Phase 151).
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["zefix"],
        liveness=_liveness.classify(
            company.get("status"),
            live=("ACTIVE",),
            pending=("BEING_CANCELLED",),
            terminal=("CANCELLED",),
        ),
        raw=company.get("status") or None,
    )

    # Carry the Swiss legal form (e.g. "Foundation"/"Stiftung", "Corporation")
    # as the non-schema `legalFormLabel` annotation. This is what the AMLA
    # trust/arrangement risk signal keys off — matching the *legal form*, not
    # the entity name (a name like "…Foundation" must not trip the signal on
    # its own). BODS v0.4 entityType.subtype is a restricted enum that does not
    # accept arbitrary legal-form text, so the label lives alongside it.
    legal_form = company.get("legalForm") or {}
    lf_names = legal_form.get("name") if isinstance(legal_form, dict) else None
    legal_form_text = ""
    if isinstance(lf_names, dict):
        legal_form_text = (lf_names.get("en") or lf_names.get("de") or "").strip()
    if legal_form_text:
        entity["recordDetails"]["legalFormLabel"] = legal_form_text

    yield entity


def _zefix_address(block: dict[str, Any]) -> list[dict[str, str]]:
    if not block:
        return []
    parts = [
        block.get("organisation"),
        block.get("careOf"),
        " ".join(filter(None, [block.get("street"), block.get("houseNumber")])),
        block.get("addon"),
        block.get("poBox"),
        " ".join(filter(None, [block.get("swissZipCode"), block.get("city")])),
    ]
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return []
    return [_addr("registered", joined, "CH")]


# ----------------------------------------------------------------------
# KvK (Netherlands Chamber of Commerce) → BODS
# ----------------------------------------------------------------------
#
# The KvK open-data endpoint returns limited fields: registration status,
# legal form code (rechtsvormCode), SBI activity codes, start date, and a
# 2-digit postal-code region.  Company name is NOT available from this API
# tier; it is passed via bundle["legal_name"] (sourced from GLEIF).
#
# Identifier scheme: "NL-KVK"  (follows the GB-COH / CH-FDJP pattern)
# Jurisdiction: Netherlands ("NL")
# Source: https://developers.kvk.nl/nl/documentation/open-dataset-basis-bedrijfsgegevens-api


def map_kvk(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a KvK fetch bundle to a BODS v0.4 entity statement.

    Returns an empty iterable for stub bundles, missing company data,
    or missing entity name.  KvK open data does not expose natural
    persons, so only entity statements are emitted.

    Bundle shape (as returned by KvKAdapter.fetch):

    .. code-block:: python

        {
            "source_id": "kvk",
            "kvk_number": "96332751",
            "company": {          # raw KvK open-data API response
                "datumAanvang": "20250202",
                "actief": "J",
                "rechtsvormCode": "BV",
                "postcodeRegio": 10,
                "activiteiten": [{"sbiCode": "6201", "soortActiviteit": "Hoofdactiviteit"}],
                "lidstaat": "NL",
            },
            "legal_name": "Splitty B.V.",   # from GLEIF, may be empty
            "is_stub": False,
        }
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    kvk_number: str = bundle.get("kvk_number") or ""
    name: str = bundle.get("legal_name") or ""
    if not kvk_number or not name:
        return

    # Founding date: datumAanvang is YYYYMMDD — convert to ISO format.
    raw_date = str(company.get("datumAanvang") or "").strip()
    founding_date: str | None = None
    if len(raw_date) == 8 and raw_date.isdigit():
        founding_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    identifiers: list[dict[str, str]] = [
        {
            "id": kvk_number,
            "scheme": "NL-KVK",
            "schemeName": "Netherlands Chamber of Commerce (KvK) registration number",
        }
    ]

    entity = make_entity_statement(
        source_id="kvk",
        local_id=kvk_number,
        name=name,
        jurisdiction=("Netherlands", "NL"),
        identifiers=identifiers,
        founding_date=founding_date,
        source_url=f"https://www.kvk.nl/zoeken/handelsnaam/?q={kvk_number}",
    )
    # datumEinde (YYYYMMDD) is the end of the registration — the register's
    # only liveness signal in the open-data profile (Phase 151). No end date
    # says nothing either way, so no annotation is written.
    raw_end = str(company.get("datumEinde") or "").strip()
    if len(raw_end) == 8 and raw_end.isdigit():
        _liveness.apply_register_status(
            entity,
            source_label=SOURCE_NAMES["kvk"],
            liveness=_liveness.TERMINAL,
            raw="datumEinde",
            since=f"{raw_end[:4]}-{raw_end[4:6]}-{raw_end[6:]}",
        )
    yield entity


# ----------------------------------------------------------------------
# INPI (France — Registre National des Entreprises) → BODS
# ----------------------------------------------------------------------
#
# The RNE API returns a rich JSON document keyed under ``content``.
# Only ``personneMorale`` companies are handled here; ``personnePhysique``
# (sole traders / auto-entrepreneurs) are out of scope for Phase 1.
#
# Identifier scheme: "FR-INSEE"  (follows GB-COH / CH-FDJP / NL-KVK pattern)
# Jurisdiction: France ("FR")
# Source: https://registre-national-entreprises.inpi.fr/
#
# ⚠️  BO restriction (Loi Sapin II / décret 2017-1094): any pouvoir entry
# where ``beneficiaireEffectif == true`` is a beneficial-ownership record
# and MUST NOT be republished without legitimate-interest authorisation.
# These are silently skipped.
#
# Non-BO individuals (``typeDePersonne == "INDIVIDU"`` AND
# ``beneficiaireEffectif == false``) are management / governance officers
# and ARE emitted as BODS person + relationship statements.  The
# ``roleEntreprise`` integer code from ``individu.descriptionPersonne``
# drives the BODS interest type (see ``_inpi_role_interest_type``) and
# its French Libellé from the INPI data dictionary is passed into the
# ``details`` field of the interest so the source register's precise role
# description is preserved.
#
# Field mapping for the relationship interest:
#   roleEntreprise code → interest type (seniorManagingOfficial or
#                         otherInfluenceOrControl)
#   roleEntreprise label (French) → details
#   dateEffetRoleDeclarant → startDate   (ISO date string; the sibling
#                                          boolean dateEffetRoleDeclarantPresent
#                                          indicates whether this date is present)

# ---------------------------------------------------------------------------
# INPI roleEntreprise codelist (source: INPI data dictionary, tab roleEntreprise)
# ---------------------------------------------------------------------------

_INPI_ROLE_LABELS: dict[int, str] = {
    11: "Membre",
    13: "Contrôleur de gestion",
    14: "Contrôleur des comptes",
    23: "Autre associé majoritaire",
    28: "Gérant et associé indéfiniment et solidairement responsable",
    29: "Gérant et associé indéfiniment responsable",
    30: "Gérant",
    40: "Liquidateur",
    41: "Associé unique (qui participe à l'activité EURL)",
    51: "Président du conseil d'administration",
    52: "Président du directoire",
    53: "Directeur Général",
    55: "Dirigeant à l'étranger d'une personne morale étrangère",
    56: "Dirigeant en France d'une personne morale étrangère",
    60: "Président du conseil d'administration et directeur général",
    61: "Président du conseil de surveillance",
    63: "Membre du directoire",
    64: "Membre du conseil de surveillance",
    65: "Administrateur",
    66: "Personne ayant le pouvoir d'engager à titre habituel la société",
    67: "Personne ayant le pouvoir d'engager l'établissement",
    69: "Directeur général unique de SA à directoire",
    70: "Directeur général délégué",
    71: "Commissaire aux comptes titulaire",
    72: "Commissaire aux comptes suppléant",
    73: "Président de SAS",
    74: "Associé indéfiniment et solidairement responsable",
    75: "Associé indéfiniment responsable",
    76: "Représentant social d'une entreprise personne étrangère sans établissement en France",
    77: "Représentant fiscal d'une entreprise personne étrangère sans établissement en France",
    82: "Indivisaire",
    86: "Exploitant pour le compte de l'indivision",
    90: "Personne physique, exploitant en commun",
    94: "Membre non salarié participant aux travaux",
    95: "Associé qui participe à la gestion",
    96: "Associé non salarié",
    97: "Mandataire ad hoc",
    98: "Administrateur provisoire",
    99: "Autre",
    100: "Repreneur",
    101: "Entrepreneur",
    103: "Suppléant",
    104: "Personne chargée du contrôle",
    105: "Personne décisionnaire désignée",
    106: "Comptable",
    107: "Héritier indivisaire",
    108: "Loueur",
    109: "Mandataire fiscal",
    110: "Vice-Président",
    111: "Vice-Président du Directoire",
    120: "Vice-Président du Conseil d'Orientation et de Surveillance",
    121: "Président du Conseil d'Orientation et de Surveillance",
    122: "Membre du Conseil d'Orientation et de Surveillance",
    130: "Associé unique qui récupère le patrimoine",
    131: "Associé commandité",
    132: "Associé commanditaire",
    135: "Administrateurs représentant les salariés",
    140: "Président de l'EPIC",
    150: "Avocat",
    200: "Fiduciaire",
    201: "Dirigeant",
    202: "Représentant de l'assujetti unique",
    203: "Membre bénéficiant d'un mandat général d'administration",
    204: "Personne capable d'engager l'entité",
    205: "Président",
    206: "Directeur",
    207: "Trésorier",
    208: "Secrétaire",
    209: "Secrétaire général",
    210: "Membre du conseil syndical",
    211: "Président du conseil syndical",
    212: "Personne désignée par les statuts",
    213: "Vice-Président du conseil de surveillance",
    214: "Personne ayant le pouvoir d'engager à titre habituel l'entité",
    215: "Personne ayant le pouvoir de représenter l'entité",
    216: "Trésorier adjoint",
    217: "Secrétaire adjoint",
    218: "Membre de l'organe collégial de contrôle",
    219: "Président de l'organe collégial de contrôle",
    220: "Auditeur de durabilité",
    231: "Gouverneur",
    232: "Premier sous-gouverneur",
    233: "Deuxième sous-gouverneur",
    234: "Conseiller général",
}

# External professional service providers — mapped to otherInfluenceOrControl.
# All other roleEntreprise codes default to seniorManagingOfficial.
_INPI_OTHER_INFLUENCE_CODES: frozenset[int] = frozenset({
    14,   # Contrôleur des comptes
    71,   # Commissaire aux comptes titulaire
    72,   # Commissaire aux comptes suppléant
    77,   # Représentant fiscal d'une entreprise personne étrangère sans établissement en France
    109,  # Mandataire fiscal
    150,  # Avocat
    220,  # Auditeur de durabilité
})


def _inpi_role_interest_type(role_code: int | str | None) -> str:
    """Return the BODS v0.4 interest type for an INPI roleEntreprise code.

    External professional service providers (auditors, lawyers, fiscal
    representatives) → ``otherInfluenceOrControl``.
    All other roles (and unknown codes) → ``seniorManagingOfficial``.
    """
    try:
        code = int(role_code) if role_code is not None else None
    except (ValueError, TypeError):
        code = None
    return (
        "otherInfluenceOrControl"
        if code in _INPI_OTHER_INFLUENCE_CODES
        else "seniorManagingOfficial"
    )


def map_inpi(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an INPI RNE fetch bundle to BODS v0.4 statements.

    Yields an entity statement for the company, followed by person +
    relationship statements for each active non-BO individual in
    ``composition.pouvoirs`` (``typeDePersonne == "INDIVIDU"`` and
    ``beneficiaireEffectif == false``).

    Pouvoirs where ``beneficiaireEffectif == true`` are BO records subject
    to the Loi Sapin II / décret 2017-1094 redistribution restriction and
    are silently skipped.

    Returns an empty iterable for stub bundles (including non-diffusable
    companies), missing company data, or missing entity name.

    The RNE API response wraps the rich company data under ``formality.content``
    and exposes a condensed ``identite`` block at the top level.  Actual
    structure (abbreviated):

    .. code-block:: python

        {
            "source_id": "inpi",
            "siren": "055804124",
            "company": {
                "diffusionINSEE": "O",
                "identite": {               # top-level condensed block
                    "entreprise": {"denomination": "BOLLORE SE", ...}
                },
                "formality": {
                    "content": {
                        "personneMorale": {
                            "adresseEntreprise": {
                                "adresse": {
                                    "typeVoie": "QUAI", "voie": "DE DION BOUTON",
                                    "numVoie": "31", "codePostal": "92800",
                                    "commune": "PUTEAUX", ...
                                }
                            },
                            "composition": {
                                "pouvoirs": [
                                    {
                                        "typeDePersonne": "INDIVIDU",
                                        "beneficiaireEffectif": False,
                                        "individu": {
                                            "descriptionPersonne": {
                                                "nom": "DUPONT",
                                                "prenoms": ["JEAN"],
                                                "nationalite": "Française",
                                                "roleEntreprise": 30,
                                                "dateEffetRoleDeclarant": "2020-01-15",
                                                "dateEffetRoleDeclarantPresent": True,
                                            }
                                        },
                                    }
                                ]
                            },
                        },
                        "natureCreation": {"dateCreation": "1990-09-13", ...},
                    }
                },
            },
            "is_stub": False,
        }
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    # The normalised SIREN is always put in the bundle by InpiAdapter.fetch;
    # do not fall back to company["siren"] so that the early-exit is reliable.
    siren: str = bundle.get("siren") or ""
    if not siren:
        return

    # The RNE API nests the full company data under formality.content.
    formality: dict[str, Any] = company.get("formality") or {}
    content: dict[str, Any] = formality.get("content") or {}
    pm: dict[str, Any] = content.get("personneMorale") or {}
    if not pm:
        # personnePhysique (sole trader) — out of scope for Phase 1.
        return

    # Company name — prefer the top-level identite block (condensed but stable),
    # fall back to the nested personneMorale.identite path.
    top_identite: dict[str, Any] = company.get("identite") or {}
    name: str = (
        (top_identite.get("entreprise") or {}).get("denomination")
        or (pm.get("identite") or {}).get("entreprise", {}).get("denomination")
        or ""
    )
    if not name:
        return

    # Founding date — dateCreation is ISO 8601 (YYYY-MM-DD) from the RNE.
    nature_creation: dict[str, Any] = content.get("natureCreation") or {}
    founding_date: str | None = nature_creation.get("dateCreation") or None

    # Identifier: FR-INSEE (SIREN — Système d'Identification du Répertoire des Entreprises)
    identifiers: list[dict[str, str]] = [
        {
            "id": siren,
            "scheme": "FR-INSEE",
            "schemeName": "INSEE — Système d'Identification du Répertoire des Entreprises",
        }
    ]

    # Address from the first registered address block.
    addr_block: dict[str, Any] = (pm.get("adresseEntreprise") or {}).get("adresse") or {}
    addresses = _inpi_address(addr_block)

    source_url = (
        f"https://registre-national-entreprises.inpi.fr/api/companies/{siren}"
    )

    entity = make_entity_statement(
        source_id="inpi",
        local_id=siren,
        name=name,
        jurisdiction=("France", "FR"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    yield entity
    entity_sid = entity["statementId"]

    # Emit person + relationship statements for non-BO INDIVIDU pouvoirs.
    seen_sids: set[str] = {entity_sid}
    pouvoirs = (pm.get("composition") or {}).get("pouvoirs") or []
    for pouvoir in pouvoirs:
        if (pouvoir.get("typeDePersonne") or "").upper() != "INDIVIDU":
            continue
        if pouvoir.get("beneficiaireEffectif") is True:
            # BO record — redistribution restricted; skip.
            continue
        for stmt in _inpi_individu_statements(siren, pouvoir, entity_sid, source_url, seen_sids):
            seen_sids.add(stmt["statementId"])
            yield stmt


def _inpi_individu_statements(
    siren: str,
    pouvoir: dict[str, Any],
    entity_sid: str,
    source_url: str,
    seen_sids: set[str],
) -> list[dict[str, Any]]:
    """Emit person + relationship statements for one non-BO INDIVIDU pouvoir.

    Returns an empty list if the person's name cannot be determined.

    The ``roleEntreprise`` code drives the BODS interest type
    (``seniorManagingOfficial`` or ``otherInfluenceOrControl``) and its
    French Libellé from the INPI data dictionary is passed into ``details``.

    ``dateEffetRoleDeclarant`` (ISO date string) maps to ``startDate``.
    The sibling boolean ``dateEffetRoleDeclarantPresent`` simply indicates
    whether that date field is populated; it is not mapped directly.

    The same person may hold multiple roles: a single person statement is
    emitted (keyed on siren + name), but a separate relationship statement
    is emitted per role so all roles are captured.
    """
    individu: dict[str, Any] = pouvoir.get("individu") or {}
    dp: dict[str, Any] = individu.get("descriptionPersonne") or {}

    nom = (dp.get("nom") or "").strip()
    prenoms_raw = dp.get("prenoms") or []
    prenoms: list[str] = prenoms_raw if isinstance(prenoms_raw, list) else [str(prenoms_raw)]
    prenom_str = " ".join(p for p in prenoms if p).strip()

    # Build full name: "PRENOMS NOM" (French convention in the RNE).
    full_name = f"{prenom_str} {nom}".strip() if prenom_str else nom
    if not full_name:
        return []

    nom_usage = (dp.get("nomUsage") or "").strip()
    nationalite = (dp.get("nationalite") or "").strip()

    # roleEntreprise may be an int or a string in the raw payload;
    # fall back to the top-level pouvoir dict if not on descriptionPersonne.
    role_raw = dp.get("roleEntreprise") if "roleEntreprise" in dp else pouvoir.get("roleEntreprise")
    try:
        role_code: int | None = int(role_raw) if role_raw is not None else None
    except (ValueError, TypeError):
        role_code = None

    role_label = _INPI_ROLE_LABELS.get(role_code, "") if role_code is not None else ""
    interest_type = _inpi_role_interest_type(role_code)

    # dateEffetRoleDeclarant → BODS startDate.
    start_date: str | None = (dp.get("dateEffetRoleDeclarant") or "").strip() or None

    # Person local_id: stable per (siren, nom, prenoms) across multiple roles.
    person_key = f"{siren}|individu|{nom}|{prenom_str}"
    person_local_id = (
        f"inpi:{siren}:individu:{hashlib.sha256(person_key.encode()).hexdigest()[:16]}"
    )

    nationalities: list[dict[str, str]] = [{"name": nationalite}] if nationalite else []
    person = make_person_statement(
        source_id="inpi",
        local_id=person_local_id,
        full_name=full_name,
        person_type="knownPerson",
        nationalities=nationalities,
        source_url=source_url,
    )
    person_sid = person["statementId"]

    stmts: list[dict[str, Any]] = []
    if person_sid not in seen_sids:
        stmts.append(person)

    # Relationship local_id: stable per (siren, nom, prenoms, role_code) so
    # the same person with multiple roles produces distinct relationships.
    rel_key = f"{siren}|individu|{nom}|{prenom_str}|{role_code}"
    rel_local_id = (
        f"inpi:{siren}:individu-rel:{hashlib.sha256(rel_key.encode()).hexdigest()[:16]}"
    )

    # details: French role label + nom d'usage if present.
    details_parts: list[str] = []
    if role_label:
        details_parts.append(role_label)
    elif role_code is not None:
        details_parts.append(f"Code {role_code}")
    if nom_usage:
        details_parts.append(f"Nom d'usage : {nom_usage}")
    details = "; ".join(details_parts) if details_parts else "Pouvoir (INPI RNE)"

    interest: dict[str, Any] = {
        "type": interest_type,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": False,
        "details": details,
    }
    if start_date:
        interest["startDate"] = start_date

    rel = make_relationship_statement(
        source_id="inpi",
        local_id=rel_local_id,
        subject_statement_id=entity_sid,
        interested_party_statement_id=person_sid,
        interested_party_type="person",
        interests=[interest],
        source_url=source_url,
        # start_date is when the role began (already interest.startDate), not
        # when the RNE declared it or when OpenCheck published — same class of
        # error as the Companies House officer path above.
    )
    rel_sid = rel["statementId"]
    if rel_sid not in seen_sids:
        stmts.append(rel)

    return stmts


def _inpi_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a raw RNE adresse block.

    The actual API field for the street name is ``voie``, not ``libelleVoie``
    (which was documented but not present in live responses).
    """
    if not block:
        return []
    parts = [
        block.get("numVoie"),
        block.get("indiceRepetition"),
        block.get("typeVoie"),
        block.get("voie") or block.get("libelleVoie"),  # live field is "voie"
        block.get("complementLocalisation"),
        block.get("codePostal"),
        block.get("commune") or block.get("libelleCommune"),
    ]
    joined = " ".join(p for p in parts if p)
    if not joined:
        return []
    return [_addr("registered", joined, "FR")]


# ----------------------------------------------------------------------
# Bolagsverket (Swedish Companies Registration Office) → BODS
# ----------------------------------------------------------------------
#
# Bolagsverket publishes company information via a WSO2 API gateway.
# The register is fully public for officer/board data — unlike INPI,
# there is no BO restriction; board members, CEO, and signatories are
# safe to republish as BODS person statements.
#
# Identifier scheme: "SE-BLV"  (follows GB-COH / CH-FDJP / NL-KVK / FR-INSEE)
# Jurisdiction: Sweden ("SE")
# Source: https://www.bolagsverket.se/
#
# Response shape confirmed from Bolagsverket API documentation.
# POST /organisationer → {"organisationer": [{...}]}
# The mapper receives the first element of that array as bundle["company"].
#
# Confirmed bundle shape:
#
#   {
#     "source_id": "bolagsverket",
#     "org_number": "5299999994",
#     "company": {
#       "organisationsidentitet": {"identitetsbeteckning": "5299999994"},
#       "organisationsnamn": {
#         "organisationsnamnLista": [
#           {"namn": "Cykelbolaget AB", "registreringsdatum": "2024-03-15"}
#         ]
#       },
#       "organisationsdatum": {"registreringsdatum": "2000-01-23"},
#       "organisationsform": {"kod": "AB", "klartext": "Aktiebolag"},
#       "juridiskForm": {"kod": "49", "klartext": "Övriga aktiebolag"},
#       "postadressOrganisation": {
#         "postadress": {
#           "utdelningsadress": "Jobbstigen 2",
#           "postnummer": "12345",
#           "postort": "Grönköping",
#           "land": "Sverige",
#           "coAdress": "C/o Annat företag"
#         }
#       },
#       "verksamhetsbeskrivning": {"beskrivning": "Handel med skor"},
#       "verksamOrganisation": {"kod": "JA"},   # JA = active
#       "avregistreradOrganisation": {"avregistreringsdatum": "2023-05-05T..."},
#       "avregistreringsorsak": {"klartext": "Likvidation"},
#       "pagaendeAvvecklingsEllerOmstruktureringsforfarande": {
#         "pagaendeAvvecklingsEllerOmstruktureringsforfarandeLista": [
#           {"kod": "KK", "klartext": "Konkurs", "fromDatum": "..."}
#         ]
#       }
#     },
#     "legal_name": "Cykelbolaget AB",
#     "is_stub": False,
#   }
#
# Note: Officer/board member data is NOT returned by /organisationer.
# This endpoint covers the EU high-value company dataset only.


def map_bolagsverket(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Bolagsverket fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the registered company. Officer data
    is not available from the /organisationer endpoint so no person or
    relationship statements are emitted.

    Returns an empty iterable for stub bundles or missing company data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    org_number: str = bundle.get("org_number") or ""
    if not org_number:
        return

    # Company name: organisationsnamn.organisationsnamnLista[0].namn
    # Fall back to the GLEIF-supplied legal_name if missing.
    namn_lista: list[dict[str, Any]] = (
        (company.get("organisationsnamn") or {}).get("organisationsnamnLista") or []
    )
    name: str = ""
    if namn_lista:
        # The list may contain multiple names (trading names, historical).
        # Take the first entry — the API returns the current registered name first.
        name = (namn_lista[0].get("namn") or "").strip()
    if not name:
        name = (bundle.get("legal_name") or "").strip()
    if not name:
        return

    # Format org number for display: NNNNNN-NNNN
    org_display = f"{org_number[:6]}-{org_number[6:]}" if len(org_number) == 10 else org_number

    # Founding / registration date: organisationsdatum.registreringsdatum (YYYY-MM-DD)
    founding_date: str | None = (
        (company.get("organisationsdatum") or {}).get("registreringsdatum") or None
    )
    # Guard against non-ISO or timestamp strings
    if founding_date and len(founding_date) != 10:
        founding_date = None

    identifiers: list[dict[str, str]] = [
        {
            "id": org_display,
            "scheme": "SE-BLV",
            "schemeName": "Bolagsverket — Swedish Companies Registration Office",
        }
    ]

    # Address: postadressOrganisation.postadress
    addr_block: dict[str, Any] = (
        (company.get("postadressOrganisation") or {}).get("postadress") or {}
    )
    addresses = _bv_address(addr_block)

    source_url = "https://www.bolagsverket.se/"

    entity = make_entity_statement(
        source_id="bolagsverket",
        local_id=org_number,
        name=name,
        jurisdiction=("Sweden", "SE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # Register status (Phase 151): ``avregistreradOrganisation`` carries the
    # deregistration date and ``avregistreringsorsak`` the reason; a pending
    # winding-up / restructuring list (konkurs, likvidation, ...) is the
    # pending class; ``verksamOrganisation.kod`` JA is live.
    dereg = (company.get("avregistreradOrganisation") or {}).get("avregistreringsdatum") or ""
    dereg_reason = (company.get("avregistreringsorsak") or {}).get("klartext") or ""
    pending_list = (
        (company.get("pagaendeAvvecklingsEllerOmstruktureringsforfarande") or {}).get(
            "pagaendeAvvecklingsEllerOmstruktureringsforfarandeLista"
        )
        or []
    )
    active_code = str((company.get("verksamOrganisation") or {}).get("kod") or "").upper()
    if dereg:
        se_liveness, se_raw, se_since = _liveness.TERMINAL, dereg_reason or "avregistrerad", str(dereg)[:10]
    elif pending_list:
        first = pending_list[0] if isinstance(pending_list[0], dict) else {}
        se_liveness = _liveness.PENDING
        se_raw = str(first.get("klartext") or first.get("kod") or "pågående avveckling")
        se_since = str(first.get("fromDatum") or "")[:10] or None
    elif active_code == "JA":
        se_liveness, se_raw, se_since = _liveness.LIVE, "verksam", None
    else:
        se_liveness, se_raw, se_since = _liveness.UNKNOWN, None, None
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["bolagsverket"],
        liveness=se_liveness,
        raw=se_raw,
        since=se_since,
    )
    yield entity


def _bv_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a Bolagsverket postadress block.

    Field names confirmed from API documentation:
    utdelningsadress (street), postnummer, postort (city), land (country),
    coAdress (c/o line).
    """
    if not block:
        return []
    parts = [
        block.get("coAdress"),
        block.get("utdelningsadress"),
        block.get("postnummer"),
        block.get("postort"),
        block.get("land"),
    ]
    joined = ", ".join(p for p in parts if p)
    if not joined:
        return []
    country = block.get("land") or "SE"
    return [_addr("registered", joined, country)]


# ----------------------------------------------------------------------
# Croatian Court Register (Sudski registar) → BODS
# ----------------------------------------------------------------------


def map_sudreg_croatia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Sudski registar fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the registered company. Officer and
    beneficial-ownership data are not exposed by the public API, so no
    person or relationship statements are emitted.

    Returns an empty iterable for stub bundles or missing subject data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    subject: dict[str, Any] = bundle.get("subject") or {}
    if not subject:
        return

    mbs: str = bundle.get("mbs") or ""
    if not mbs:
        return

    # Name: tvrtka.ime (full legal name); fall back to the short name, then
    # to the GLEIF-supplied legal_name.
    name: str = ((subject.get("tvrtka") or {}).get("ime") or "").strip()
    if not name:
        name = ((subject.get("skracena_tvrtka") or {}).get("ime") or "").strip()
    if not name:
        name = (bundle.get("legal_name") or "").strip()
    if not name:
        return

    short_name = ((subject.get("skracena_tvrtka") or {}).get("ime") or "").strip()
    alternate_names = [short_name] if short_name and short_name != name else []

    # Founding date: datum_osnivanja is an ISO timestamp ("1990-10-31T00:00:00");
    # keep the YYYY-MM-DD prefix only.
    founding_date: str | None = None
    raw_date = subject.get("datum_osnivanja") or ""
    if isinstance(raw_date, str) and len(raw_date) >= 10:
        candidate = raw_date[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
            founding_date = candidate

    # Identifiers: MBS (court register number) and OIB. Both are
    # independently published by the Sudski registar for this entity, so
    # both are asserted on the statement.
    mbs_display = (subject.get("potpuni_mbs") or mbs).strip()
    identifiers: list[dict[str, str]] = [
        {
            "id": mbs_display,
            "scheme": "HR-MBS",
            "schemeName": "Sudski registar — Croatian Court Register (MBS)",
        }
    ]
    oib = (bundle.get("oib") or subject.get("potpuni_oib") or "").strip()
    if oib:
        identifiers.append(
            {
                "id": oib,
                "scheme": "HR-OIB",
                "schemeName": "OIB — Croatian personal/company identification number",
            }
        )

    addresses = _sudreg_address(subject.get("sjediste") or {})

    entity = make_entity_statement(
        source_id="sudreg_croatia",
        local_id=mbs,
        name=name,
        jurisdiction=("Croatia", "HR"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        alternate_names=alternate_names,
        source_url=_SUDREG_SOURCE_URL,
    )
    yield entity


_SUDREG_SOURCE_URL = "https://sudreg.pravosudje.hr"


# ----------------------------------------------------------------------
# EITI — Extractive Industries Transparency Initiative
# ----------------------------------------------------------------------

# National identifier schemes for the countries whose EITI identification
# format has been verified against an org-id.guide scheme OpenCheck already
# emits. Other countries carry the identification without a scheme code.
_EITI_SCHEME_BY_COUNTRY: dict[str, tuple[str, str]] = {
    "GB": ("GB-COH", "Companies House"),
    "NO": ("NO-BRC", "Brønnøysundregistrene"),
    "NL": ("NL-KVK", "Kamer van Koophandel"),
}


def map_eiti(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an EITI fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the disclosing company. EITI payment
    data describes fiscal flows, not ownership or control, so no person
    or relationship statements are emitted. (When EITI's data strategy
    delivers BODS-native company/SOE publication, this mapper is the
    natural place to consume it.)
    """
    if not bundle or bundle.get("is_stub"):
        return

    identification: str = (bundle.get("identification") or "").strip()
    country: str = (bundle.get("country") or "").strip().upper()
    name: str = (bundle.get("entity_name") or "").strip()
    if not identification or not name:
        return

    scheme = _EITI_SCHEME_BY_COUNTRY.get(country)
    identifier: dict[str, str] = {"id": identification}
    if scheme:
        identifier["scheme"] = scheme[0]
        identifier["schemeName"] = f"{scheme[1]} (via EITI disclosure)"
    else:
        identifier["schemeName"] = "National registry identifier (via EITI disclosure)"

    jurisdiction_obj = _country_obj(country) if country else None
    jur_tuple: tuple[str, str] | None = (
        (jurisdiction_obj["name"], jurisdiction_obj["code"])
        if jurisdiction_obj
        else None
    )

    entity = make_entity_statement(
        source_id="eiti",
        local_id=f"{country}:{identification}",
        name=name,
        jurisdiction=jur_tuple,
        identifiers=[identifier],
        source_url="https://eiti.org/",
    )
    yield entity


def map_eiti_soe(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an EITI SOE Database bundle to BODS v0.4 statements.

    Emits the state-owned enterprise as an entity, the controlling government
    body as a ``stateBody`` entity, and a ``controlByLegalFramework``
    relationship between them. That relationship shape is exactly what the risk
    engine's ``_state_controlled_signals`` reads to raise ``STATE_CONTROLLED`` —
    so the state-ownership signal falls out of the BODS graph with no bespoke
    risk rule (and EITI is a far more authoritative source for it than the
    existing Wikidata path).

    The SOE database does **not** publish the LEI (OpenCheck derives it at
    index-build time), so ``lei`` is deliberately NOT asserted as a BODS
    identifier here — only the identifiers EITI itself publishes (its EITI id
    and, where present, the OpenCorporates id).

    When EITI ships its planned BODS-native SOE dataset, this mapper is the
    natural place to consume it — the graph shape emitted here already matches.
    """
    if not bundle or bundle.get("is_stub"):
        return

    lei: str = (bundle.get("lei") or "").strip().upper()
    name: str = (bundle.get("entity_name") or "").strip()
    if not lei or not name:
        return

    country: str = (bundle.get("country") or "").strip().upper()
    jurisdiction_obj = _country_obj(country) if country else None
    jur_tuple: tuple[str, str] | None = (
        (jurisdiction_obj["name"], jurisdiction_obj["code"])
        if jurisdiction_obj
        else None
    )

    identifiers: list[dict[str, str]] = []
    eiti_id = (bundle.get("eiti_id_company") or "").strip()
    if eiti_id:
        identifiers.append(
            {
                "id": eiti_id,
                "scheme": "XI-EITI",
                "schemeName": "EITI State-Owned Enterprises Database",
            }
        )
    oc_id = (bundle.get("opencorporates_id") or "").strip()
    if oc_id:
        identifiers.append(
            {
                "id": oc_id,
                "schemeName": "OpenCorporates company number (via EITI SOE database)",
            }
        )

    soe = make_entity_statement(
        source_id="eiti_soe",
        local_id=lei,
        name=name,
        jurisdiction=jur_tuple,
        identifiers=identifiers,
        entity_type="registeredEntity",
        entity_details="State-owned enterprise (EITI SOE database)",
        source_url="https://soe-database.eiti.org/",
    )
    yield soe

    gov_name = (bundle.get("government_entity") or "").strip()
    # Only assert state control (which raises the STATE_CONTROLLED signal) when
    # the LEI match is reasonably trustworthy. A low-confidence name match still
    # surfaces the SOE entity and its enrichment, but must not raise a
    # state-control signal on a possibly-wrong entity.
    if not gov_name or (bundle.get("match_confidence") or "medium").lower() == "low":
        return

    gov_local = f"{lei}:gov:{(bundle.get('eiti_id_government') or gov_name)}"
    government = make_entity_statement(
        source_id="eiti_soe",
        local_id=gov_local,
        name=gov_name,
        jurisdiction=jur_tuple,
        entity_type="stateBody",
        source_url="https://soe-database.eiti.org/",
    )
    yield government

    yield make_relationship_statement(
        source_id="eiti_soe",
        local_id=f"{lei}:state-control",
        subject_statement_id=soe["statementId"],
        interested_party_statement_id=government["statementId"],
        interested_party_type="entity",
        interests=[
            {
                "type": "controlByLegalFramework",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
                "details": (
                    f"State-owned enterprise controlled by {gov_name} "
                    "(EITI SOE database)."
                ),
            }
        ],
        source_url="https://soe-database.eiti.org/",
    )


# ----------------------------------------------------------------------
# Wikirate — open ESG metric answers
# ----------------------------------------------------------------------

# Wikirate Company-card identifier fields → BODS identifier schemes.
# Only fields Wikirate itself publishes are asserted (corroboration rule).
_WIKIRATE_IDENTIFIER_SCHEMES: dict[str, tuple[str | None, str]] = {
    "legal_entity_identifier": ("XI-LEI", "Legal Entity Identifier"),
    "wikidata_id": ("WIKIDATA", "Wikidata"),
    "uk_company_number": ("GB-COH", "Companies House"),
    "sec_central_index_key": ("US-SEC-CIK", "SEC EDGAR CIK"),
    "australian_business_number": ("AU-ABN", "Australian Business Number"),
    "open_corporates_id": (None, "OpenCorporates company number"),
}


def map_wikirate(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Wikirate fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the company, carrying the identifiers
    Wikirate independently publishes on the Company card. Metric answers
    describe ESG performance, not ownership or control, so no person or
    relationship statements are emitted.
    """
    if not bundle or bundle.get("is_stub"):
        return

    card_id = bundle.get("card_id")
    name: str = (bundle.get("name") or "").strip()
    if not card_id or not name:
        return

    identifiers: list[dict[str, str]] = []
    raw_identifiers: dict[str, Any] = bundle.get("identifiers") or {}
    for field, (scheme, scheme_name) in _WIKIRATE_IDENTIFIER_SCHEMES.items():
        value = raw_identifiers.get(field)
        if isinstance(value, list):
            value = value[0] if value else None
        if not value:
            continue
        identifier = {"id": str(value), "schemeName": f"{scheme_name} (via Wikirate)"}
        if scheme:
            identifier["scheme"] = scheme
        identifiers.append(identifier)

    entity = make_entity_statement(
        source_id="wikirate",
        local_id=str(card_id),
        name=name,
        identifiers=identifiers,
        source_url=bundle.get("wikirate_url") or "https://wikirate.org/",
    )
    yield entity


def map_ted_eu(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a TED (Tenders Electronic Daily) fetch bundle to BODS statements.

    Emits one entity statement for the subject company. Procurement awards
    describe economic activity, not ownership or control, so no person or
    relationship statements are emitted (the EITI precedent). The award
    details ride in the adapter's raw bundle for the frontend card.

    Identifier corroboration: only what TED itself published is asserted —
    the eForms BT-501 values that actually matched on the returned notices.
    Their scheme is not machine-readable in eForms (no scheme attribute on
    ``cbc:CompanyID``), so national numbers get a ``schemeName`` only; the
    LEI gets ``scheme: "XI-LEI"`` when a notice carried the LEI string
    (fill rate is zero as of 2026-08 — this arms automatically as LEI
    adoption in eForms grows).
    """
    if not bundle or bundle.get("is_stub"):
        return

    total = int(bundle.get("total_notice_count") or 0)
    if total <= 0:
        return

    lei: str = (bundle.get("lei") or "").strip().upper()
    name: str = (bundle.get("legal_name") or "").strip()
    local_id = lei or "|".join(bundle.get("identifiers_queried") or [])
    if not local_id:
        return

    identifiers: list[dict[str, str]] = []
    for value in bundle.get("matched_company_ids") or []:
        value = str(value).strip()
        if not value:
            continue
        if lei and value.upper() == lei:
            identifiers.append(
                {
                    "id": lei,
                    "scheme": "XI-LEI",
                    "schemeName": "Legal Entity Identifier (via TED notice)",
                }
            )
        else:
            identifiers.append(
                {
                    "id": value,
                    "schemeName": (
                        "Organisation identifier — eForms BT-501 (via TED notice)"
                    ),
                }
            )

    notices = bundle.get("notices") or []
    wins = int(bundle.get("confirmed_wins") or 0)
    details_parts = [
        f"Named as tenderer on {total} EU procurement award notice"
        f"{'s' if total != 1 else ''} (TED, eForms era)"
    ]
    if wins:
        details_parts.append(f"{wins} confirmed win{'s' if wins != 1 else ''}")
    source_url = next(
        (n.get("url") for n in notices if n.get("url")), "https://ted.europa.eu/"
    )

    # The most recent notice publication date. TED publishes the notice; that
    # publication is the declaration. Same precedent as SEC issuer details
    # taking the latest filing date (Phase 100). Deliberately not
    # contract_conclusion_date or award_date — those are event dates about the
    # contract, not TED's assertion about this record.
    latest_notice = max(
        (n.get("publication_date") or "" for n in notices), default=""
    ) or None

    entity = make_entity_statement(
        source_id="ted_eu",
        local_id=local_id,
        name=name or local_id,
        identifiers=identifiers,
        entity_details="; ".join(details_parts),
        source_url=source_url,
        statement_date=latest_notice,
    )
    yield entity


def _sudreg_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a Sudski registar ``sjediste`` block.

    Fields: ulica (street), kucni_broj (house number), naziv_naselja
    (settlement), naziv_opcine (municipality), naziv_zupanije (county).
    """
    if not block:
        return []
    street = (block.get("ulica") or "").strip()
    house = block.get("kucni_broj")
    if street and house not in (None, ""):
        street = f"{street} {house}"
    parts = [
        street,
        block.get("naziv_naselja"),
        block.get("naziv_opcine"),
        block.get("naziv_zupanije"),
    ]
    joined = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
    if not joined:
        return []
    return [_addr("registered", joined, "HR")]


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
            "id": registry_code,
            "scheme": "EE-KMKR",
            "schemeName": "Estonian e-Business Register (Äriregister / KMKR)",
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


# ----------------------------------------------------------------------
# BrightQuery / OpenData.org → BODS
# ----------------------------------------------------------------------
#
# BrightQuery's COMPANY dataset provides US entities; PEOPLE_BUSINESS
# provides their executives / contacts.  Because BQ records executive
# affiliations rather than beneficial ownership, all people relationships
# are mapped to ``otherInfluenceOrControl`` with
# ``beneficialOwnershipOrControl = false`` — mirroring the approach taken
# by the reference bods-brightquery adapter.
#
# Identifier mapping (OTHER_ID_TYPE → BODS scheme):
#   CIK          → US-SEC
#   PERMID       → PERMID
#   SAM_UEI      → US-SAM-UEI
#   SAM_CAGE     → US-SAM-CAGE
#   CAPIQ        → CAPIQ
#   PITCHBOOK_ID → PITCHBOOK
#   NPI          → US-NPI
#   OPEN_FIGI    → OPENFIGI
#   ISIN         → ISIN
#   TICKER       → TICKER

_BQ_IDENTIFIER_MAP: list[tuple[str, str, str]] = [
    # (OTHER_ID_TYPE, BODS scheme code, human name)
    ("CIK",          "US-SEC",    "US SEC Central Index Key"),
    ("PERMID",       "PERMID",    "Refinitiv PermID"),
    ("SAM_UEI",      "US-SAM-UEI","US SAM Unique Entity Identifier"),
    ("SAM_CAGE",     "US-SAM-CAGE","US SAM CAGE Code"),
    ("CAPIQ",        "CAPIQ",     "S&P Capital IQ"),
    ("PITCHBOOK_ID", "PITCHBOOK", "PitchBook"),
    ("NPI",          "US-NPI",    "US National Provider Identifier"),
    ("OPEN_FIGI",    "OPENFIGI",  "OpenFIGI"),
    ("ISIN",         "ISIN",      "International Securities Identification Number"),
    ("TICKER",       "TICKER",    "Stock Ticker"),
]

_BQ_SOURCE_URL = "https://opendata.org/"


def _bq_features(record: dict[str, Any]) -> list[dict[str, Any]]:
    return record.get("FEATURES") or []


def _bq_get_feature(feats: list[dict], key: str) -> dict | None:
    """Return the first feature dict that contains *key*."""
    for f in feats:
        if key in f:
            return f
    return None


def _bq_get_value(feats: list[dict], key: str, default: str = "") -> str:
    f = _bq_get_feature(feats, key)
    return str(f[key]).strip() if f and f.get(key) is not None else default


def _bq_other_ids(feats: list[dict]) -> dict[str, str]:
    """Return all OTHER_ID_TYPE → OTHER_ID_NUMBER pairs from FEATURES."""
    result: dict[str, str] = {}
    for f in feats:
        id_type = f.get("OTHER_ID_TYPE")
        id_number = f.get("OTHER_ID_NUMBER")
        if id_type and id_number:
            result[str(id_type)] = str(id_number).strip()
    return result


def map_brightquery(bundle: dict[str, Any]) -> BODSBundle:
    """Map a BrightQuery bundle to BODS v0.4 statements.

    ``bundle`` shape (as returned by ``BrightQueryAdapter.fetch()``):

    .. code-block:: python

        {
            "source_id": "brightquery",
            "hit_id": "<LEI>",
            "lei": "<LEI>",
            "bq_id": "<RECORD_ID>",
            "name": "<primary name string>",
            "company": {<Senzing COMPANY record>},
            "people":  [{<Senzing PEOPLE_BUSINESS record>}, ...],
        }

    Produces:
    * One ``entity`` statement for the company.
    * One ``person`` + one ``relationship`` statement per named executive.
    """
    result = BODSBundle()

    company = bundle.get("company") or {}
    people = bundle.get("people") or []
    lei = bundle.get("lei") or bundle.get("hit_id") or ""
    bq_id = bundle.get("bq_id") or str(company.get("RECORD_ID") or "")

    if not company or not bq_id:
        return result

    feats = _bq_features(company)
    name = _bq_get_value(feats, "NAME_ORG") or bundle.get("name") or f"BrightQuery {bq_id}"
    other_ids = _bq_other_ids(feats)

    # --- Entity identifiers ---
    identifiers: list[dict[str, str]] = [
        {"id": bq_id, "scheme": "BRIGHTQUERY", "schemeName": "BrightQuery"},
    ]
    if lei:
        identifiers.append(
            {"id": lei, "scheme": "XI-LEI", "schemeName": "Legal Entity Identifier"}
        )
    for id_type, scheme, scheme_name in _BQ_IDENTIFIER_MAP:
        val = other_ids.get(id_type)
        if val:
            identifiers.append({"id": val, "scheme": scheme, "schemeName": scheme_name})

    # --- Business address ---
    addresses: list[dict[str, str]] = []
    addr_feat = _bq_get_feature(feats, "ADDR_LINE1") or _bq_get_feature(feats, "ADDR_CITY")
    if addr_feat:
        parts = [
            addr_feat.get("ADDR_LINE1"),
            addr_feat.get("ADDR_CITY"),
            addr_feat.get("ADDR_STATE"),
            addr_feat.get("ADDR_POSTAL_CODE"),
            addr_feat.get("ADDR_COUNTRY"),
        ]
        joined = ", ".join(p for p in parts if p)
        if joined:
            country = addr_feat.get("ADDR_COUNTRY", "")
            # Normalise "USA" → "US" for BODS country field.
            if country.upper() == "USA":
                country = "US"
            addresses.append(_addr("registered", joined, country))

    entity = make_entity_statement(
        source_id="brightquery",
        local_id=bq_id,
        name=name,
        jurisdiction=("United States", "US"),
        identifiers=identifiers,
        addresses=addresses,
        source_url=_BQ_SOURCE_URL,
    )
    result.statements.append(entity)
    entity_sid = entity["statementId"]

    # --- Executives (PEOPLE_BUSINESS records) ---
    for person_record in people:
        pfeats = _bq_features(person_record)
        person_id = str(person_record.get("RECORD_ID") or "").strip()
        if not person_id:
            continue

        # Build a display name; skip truly nameless records.
        full_name = _bq_get_value(pfeats, "NAME_FULL")
        if not full_name:
            first = _bq_get_value(pfeats, "NAME_FIRST")
            last = _bq_get_value(pfeats, "NAME_LAST")
            full_name = f"{first} {last}".strip()
        if not full_name:
            continue

        # Role from REL_POINTER_ROLE (e.g. "Executive", "Director").
        role = ""
        for f in pfeats:
            if "REL_POINTER_ROLE" in f:
                role = str(f["REL_POINTER_ROLE"]).strip()
                break

        local_person_id = f"{bq_id}:person:{person_id}"

        person = make_person_statement(
            source_id="brightquery",
            local_id=local_person_id,
            full_name=full_name.title(),
            source_url=_BQ_SOURCE_URL,
        )
        result.statements.append(person)
        person_sid = person["statementId"]

        interest: dict[str, Any] = {
            "type": "otherInfluenceOrControl",
            "directOrIndirect": "unknown",
            "beneficialOwnershipOrControl": False,
        }
        if role:
            interest["details"] = role

        rel = make_relationship_statement(
            source_id="brightquery",
            local_id=f"{bq_id}:rel:{person_id}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=[interest],
            source_url=_BQ_SOURCE_URL,
        )
        result.statements.append(rel)

    return result


# ----------------------------------------------------------------------
# SEC EDGAR (Schedule 13D/13G) → BODS
# ----------------------------------------------------------------------

try:
    import pycountry as _pycountry
except ImportError:  # pragma: no cover
    _pycountry = None  # type: ignore[assignment]


def _iso2_to_country_name(iso2: str) -> str:
    """Return a human-readable country name for an ISO 3166-1 alpha-2 code.

    Falls back to the code itself when pycountry is unavailable or the
    code is not found (which should not happen for codes from EDGAR's
    controlled vocabulary, but is defensive).
    """
    if not iso2:
        return ""
    if _pycountry is None:
        return iso2
    country = _pycountry.countries.get(alpha_2=iso2.upper())
    return country.name if country else iso2


# SEC Schedule 13D/13G ``typeOfReportingPerson`` codes that describe a filer
# acting in a CUSTODIAL or ADVISORY capacity rather than as the beneficial
# owner. A 13D/13G "beneficial owner" is an SEC-rules term meaning voting or
# dispositive power — an investment adviser voting client shares has it without
# any economic interest in the shares. The evidence to tell the two apart was
# already in the bundle (sec_edgar.py parses type_code alongside sole/shared
# voting power) and was simply never read: the mapper hard-coded
# beneficialOwnershipOrControl: True for every filer.
_SEC_CUSTODIAL_REPORTER_CODES: frozenset[str] = frozenset({
    "IA",  # Investment adviser
    "BD",  # Broker-dealer
    "IC",  # Investment company
    "EP",  # Employee benefit plan / ERISA
    "SA",  # Savings association
    "BK",  # Bank
    "IN",  # (see below — natural persons are NOT custodial; excluded in code)
})
# "IN" is a natural person and is emphatically not custodial; it is listed above
# only to make the omission deliberate rather than accidental.
_SEC_CUSTODIAL_REPORTER_CODES = _SEC_CUSTODIAL_REPORTER_CODES - {"IN"}


def _sec_beneficial_ownership(
    reporter: dict[str, Any], party_type: str = "person"
) -> bool | None:
    """What, if anything, a 13D/13G filing says about beneficial ownership.

    Returns ``None`` — "not stated" — when the filer reports in a custodial or
    advisory capacity, because the filing then asserts voting/dispositive power
    without asserting that the filer benefits. Returns ``True`` for an ordinary
    NATURAL-PERSON filer, where the SEC's own beneficial-ownership test (Rule
    13d-3: voting and/or dispositive power) has been met. Returns ``False`` for
    an ordinary ENTITY filer: Rule 13d-3 admits entities, but a BODS beneficial
    owner is a natural person, so an entity interested party never carries
    ``true`` — bo_regimes: sec_edgar/filer_entity -> assert_false, matching
    Open Ownership's entity-party convention (2026-08 audit).
    """
    code = (reporter.get("type_code") or "").strip().upper()
    if code in _SEC_CUSTODIAL_REPORTER_CODES:
        return None
    return party_type == "person"


def map_sec_edgar(bundle: dict[str, Any]) -> BODSBundle:
    """Map a SEC EDGAR 13D/13G bundle to BODS v0.4.

    Input shape (produced by ``SecEdgarAdapter.fetch``):
    ``{
        "source_id": "sec_edgar",
        "issuer_cik": "<cik>",
        "filings": [
            {
                "issuer": {"cik": ..., "name": ..., "cusip": ..., "address": {...}},
                "reporter": {
                    "reporter_cik": ...,
                    "name": ...,
                    "type_code": ...,
                    "is_individual": bool,
                    "citizenship_iso": ...,
                    "percent_of_class": float | None,
                    ...
                },
                "filing_url": ...,
                "form_type": ...,
                "filed": "YYYY-MM-DD",
            }, ...
        ]
    }``

    Output: one entity statement (the listed issuer) + one person/entity
    statement per unique reporter + one relationship statement per reporter,
    carrying a shareholding interest with ``share.exact`` = the percent of
    class reported in the filing.
    """
    result = BODSBundle()

    filings: list[dict[str, Any]] = bundle.get("filings") or []
    if not filings:
        return result

    # --- Subject (listed issuer) entity ---
    # Use the first filing's issuer block; all filings share the same subject.
    issuer: dict[str, Any] = filings[0].get("issuer") or {}
    issuer_cik: str = issuer.get("cik") or bundle.get("issuer_cik", "")
    issuer_name: str = issuer.get("name") or ""
    if not issuer_name or not issuer_cik:
        return result

    issuer_identifiers: list[dict[str, str]] = [
        {"id": issuer_cik, "scheme": "US-SEC-CIK", "schemeName": "SEC EDGAR CIK"},
    ]
    cusip = issuer.get("cusip") or ""
    if cusip:
        issuer_identifiers.append(
            {"id": cusip, "scheme": "CUSIP", "schemeName": "CUSIP"}
        )

    addr_raw: dict[str, str] = issuer.get("address") or {}
    issuer_addresses: list[dict[str, Any]] = []
    if addr_raw:
        parts = [
            addr_raw.get("street1", ""),
            addr_raw.get("street2", ""),
            addr_raw.get("city", ""),
            addr_raw.get("stateOrCountry", ""),
            addr_raw.get("zipCode", ""),
        ]
        address_str = ", ".join(p for p in parts if p)
        if address_str:
            issuer_addresses = [_addr("registered", address_str, "US")]

    subject_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={issuer_cik}&type=SCHEDULE+13D"
    )
    # The issuer block is read off the filings, so the latest filing date is
    # when the SEC last published these details.
    latest_filed = max(
        (f.get("filed") or "" for f in filings),
        default="",
    ) or None
    subject_entity = make_entity_statement(
        source_id="sec_edgar",
        local_id=issuer_cik,
        name=issuer_name,
        jurisdiction=("United States", "US"),
        identifiers=issuer_identifiers,
        addresses=issuer_addresses,
        source_url=subject_url,
        statement_date=latest_filed,
    )
    result.statements.append(subject_entity)
    subject_sid = subject_entity["statementId"]

    # --- Reporters ---
    for filing in filings:
        reporter: dict[str, Any] = filing.get("reporter") or {}
        name = reporter.get("name") or ""
        if not name:
            continue

        reporter_cik = reporter.get("reporter_cik") or ""
        is_individual = reporter.get("is_individual", False)
        citizenship_iso = reporter.get("citizenship_iso") or ""
        percent = reporter.get("percent_of_class")
        filing_url = filing.get("filing_url") or subject_url

        # Stable local ID: prefer CIK, fall back to a hash of the name.
        local_id = reporter_cik or _stable_id("sec_edgar_name", name)

        reporter_identifiers: list[dict[str, str]] = []
        if reporter_cik:
            reporter_identifiers.append(
                {
                    "id": reporter_cik,
                    "scheme": "US-SEC-CIK",
                    "schemeName": "SEC EDGAR CIK",
                }
            )

        if is_individual:
            nationalities: list[dict[str, str]] = []
            if citizenship_iso:
                country_name = _iso2_to_country_name(citizenship_iso)
                nationalities = [{"name": country_name, "code": citizenship_iso}]

            reporter_stmt = make_person_statement(
                source_id="sec_edgar",
                local_id=local_id,
                full_name=name,
                nationalities=nationalities,
                identifiers=reporter_identifiers,
                source_url=filing_url,
                statement_date=filing.get("filed") or None,
            )
            party_type = "person"
        else:
            jur: tuple[str, str] | None = None
            if citizenship_iso:
                country_name = _iso2_to_country_name(citizenship_iso)
                jur = (country_name, citizenship_iso)

            reporter_stmt = make_entity_statement(
                source_id="sec_edgar",
                local_id=local_id,
                name=name,
                jurisdiction=jur,
                identifiers=reporter_identifiers,
                source_url=filing_url,
                statement_date=filing.get("filed") or None,
            )
            party_type = "entity"

        result.statements.append(reporter_stmt)
        party_sid = reporter_stmt["statementId"]

        # Build interests — always at least a bare shareholding entry.
        shareholding: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
        }
        set_beneficial_ownership(
            shareholding,
            "sec_edgar",
            asserted=_sec_beneficial_ownership(reporter, party_type),
        )
        if shareholding.get("beneficialOwnershipOrControl") is True:
            # Name WHICH definition the flag is true under — Rule 13d-3 is a
            # securities-disclosure concept, not AML beneficial ownership
            # (bo_regimes: sec_edgar).
            shareholding["details"] = (
                "Beneficial owner under SEC Rule 13d-3 (voting and/or "
                "dispositive power) — a securities-disclosure concept distinct "
                "from AML beneficial ownership"
            )
        # Sole vs shared power is a materially different claim and was being
        # discarded; where the filing distinguishes them, say so.
        sole = reporter.get("sole_voting_power")
        shared = reporter.get("shared_voting_power")
        if sole is not None or shared is not None:
            power_parts = []
            if sole:
                power_parts.append(f"sole voting power over {sole:,.0f} shares")
            if shared:
                power_parts.append(f"shared voting power over {shared:,.0f} shares")
            if power_parts:
                shareholding["details"] = "; ".join(
                    filter(None, [shareholding.get("details"), *power_parts])
                )
        type_code = (reporter.get("type_code") or "").strip().upper()
        if type_code in _SEC_CUSTODIAL_REPORTER_CODES:
            note = (
                f"Filed as reporting-person type {type_code} "
                "(custodial/advisory capacity); the filing asserts voting or "
                "dispositive power, not that the filer is the beneficiary."
            )
            shareholding["details"] = (
                f"{shareholding['details']}. {note}"
                if shareholding.get("details")
                else note
            )
        if percent is not None:
            shareholding["share"] = {"exact": percent}

        rel_stmt = make_relationship_statement(
            source_id="sec_edgar",
            local_id=f"{issuer_cik}:{local_id}",
            subject_statement_id=subject_sid,
            interested_party_statement_id=party_sid,
            interested_party_type=party_type,
            interests=[shareholding],
            source_url=filing_url,
            # The 13D/13G filing date — when this holding was declared to the
            # SEC. That is the source's declaration date, so it is the
            # statementDate.
            statement_date=filing.get("filed") or None,
        )
        result.statements.append(rel_stmt)

    return result


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


# ----------------------------------------------------------------------
# CRO (Companies Registration Office Ireland) → BODS
# ----------------------------------------------------------------------
#
# The CRO Open Data Portal provides entity-level data only — no
# officer or director records are available from the free tier. We
# therefore emit a single entityStatement per company. The Open
# Services API (key-gated) can extend this with officers in future.

# CRO company_type values → BODS entityType
# https://core.cro.ie (type codes seen in practice)
_CRO_ENTITY_TYPES: dict[str, str] = {
    # Registered entities (limited liability companies)
    "LTD": "registeredEntity",
    "DAC": "registeredEntity",
    "PLC": "registeredEntity",
    "UC":  "registeredEntity",     # Unlimited company
    "CLG": "registeredEntity",     # Company limited by guarantee
    "EEIG": "registeredEntity",    # European Economic Interest Grouping
    "SE":  "registeredEntity",     # Societas Europaea
    "ICAV": "registeredEntity",    # Investment limited partnership
    "ILP": "registeredEntity",
}


def _cro_entity_type(company_type: str) -> str:
    """Map a CRO company type string to a BODS entityType."""
    # The company_type field carries a full description, e.g.
    # "PLC - Public Limited Company" or "LTD - Private company limited by shares".
    # Extract the leading abbreviation.
    code = (company_type or "").split("-")[0].strip().split("(")[0].strip().upper()
    for prefix, bods_type in _CRO_ENTITY_TYPES.items():
        if code.startswith(prefix):
            return bods_type
    return "registeredEntity"


def _cro_address(rec: dict[str, Any]) -> dict[str, str] | None:
    """Build a BODS address dict from CRO company_address_1..4 fields."""
    lines = [
        (rec.get(f"company_address_{i}") or "").strip()
        for i in range(1, 5)
    ]
    non_empty = [l for l in lines if l]
    if not non_empty:
        return None
    eircode = (rec.get("eircode") or "").strip()
    if eircode:
        non_empty.append(eircode)
    return _addr("registered", ", ".join(non_empty), "IE")


def map_cro(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CroAdapter fetch bundle to a single BODS v0.4 entity statement.

    Only entity data is available from the CRO Open Data Portal. When the
    CRO Open Services API key is configured (future enhancement), officer
    records will be added here as person + relationship statements.
    """
    if not bundle or bundle.get("is_stub"):
        return

    crn: str = str(bundle.get("crn") or "")
    company: dict[str, Any] = bundle.get("company") or {}

    name: str = (
        (company.get("company_name") or "").strip()
        or bundle.get("legal_name")
        or f"IE-CRN {crn}"
    )
    if not crn or not name:
        return

    source_url = f"https://core.cro.ie/company/{crn}"

    # Registration date comes as "1996-06-05T00:00:00" — take the date part.
    reg_date_raw = company.get("company_reg_date") or ""
    founding_date = reg_date_raw[:10] if reg_date_raw else None

    company_type = (company.get("company_type") or "").strip()
    entity_type = _cro_entity_type(company_type)

    identifiers: list[dict[str, str]] = [
        {
            "id": crn,
            "scheme": "IE-CRO",
            "schemeName": "Companies Registration Office Ireland",
        }
    ]

    nace = (company.get("nace_v2_code") or "").strip()
    if nace:
        identifiers.append({
            "id": nace,
            "scheme": "NACE2",
            "schemeName": "NACE Rev. 2 activity code",
        })

    address = _cro_address(company)

    cro_entity = make_entity_statement(
        source_id="cro",
        local_id=crn,
        name=name,
        jurisdiction=("Ireland", "IE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        entity_type=entity_type,
        source_url=source_url,
    )
    # CRO ``company_status`` (Phase 151): "Normal" is live; the insolvency
    # and strike-off-listed states are pending; dissolved / struck off end
    # the company. ``company_status_date`` when the open-data row has it.
    cro_status = (company.get("company_status") or "").strip()
    cro_liveness = _liveness.classify(
        cro_status,
        live=("Normal",),
        pending=("Liquidation", "Receivership", "Examinership", "Strike Off Listed", "Strike-off Listed"),
        terminal=("Dissolved", "Struck Off", "Struck-off", "Amalgamated", "Ceased"),
    )
    _liveness.apply_register_status(
        cro_entity,
        source_label=SOURCE_NAMES["cro"],
        liveness=cro_liveness,
        raw=cro_status or None,
        since=(
            str(company.get("company_status_date") or "")[:10] or None
            if cro_liveness != _liveness.LIVE
            else None
        ),
    )
    yield cro_entity


def map_malta_mbr(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a MaltaMbrAdapter fetch bundle to a single BODS v0.4 entity statement.

    The MBR Open Data API exposes entity data only (no officers, shareholders
    or beneficial owners), so just one entityStatement is produced — the legal
    form is carried in ``entityType.details``.
    """
    if not bundle or bundle.get("is_stub"):
        return

    reg: str = str(bundle.get("mt_crn") or "")
    company: dict[str, Any] = bundle.get("company") or {}

    name: str = (
        (company.get("name") or "").strip()
        or bundle.get("legal_name")
        or (f"MT {reg}" if reg else "")
    )
    if not reg or not name:
        return

    # A stable, dereferenceable URL for the record (space percent-encoded).
    source_url = (
        "https://openapi.baros.mbr.mt/api/v1/companies/" + reg.replace(" ", "%20")
    )

    reg_date = (company.get("registration_date") or "").strip()
    founding_date = reg_date[:10] if reg_date else None

    legal_form = (company.get("type") or "").strip() or None

    identifiers: list[dict[str, str]] = [
        {
            "id": reg,
            "scheme": "MT-MBR",
            "schemeName": "Malta Business Registry",
        }
    ]

    # Registered office — concatenate the non-empty address components.
    parts = [
        (company.get("street") or "").strip(),
        (company.get("address") or "").strip(),
        (company.get("locality") or "").strip(),
        (company.get("postcode") or "").strip(),
    ]
    addr_str = ", ".join(p for p in parts if p)
    address = _addr("registered", addr_str, "MT") if addr_str else None

    mbr_entity = make_entity_statement(
        source_id="malta_mbr",
        local_id=reg,
        name=name,
        jurisdiction=("Malta", "MT"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        entity_type="registeredEntity",
        entity_details=legal_form,
        source_url=source_url,
    )
    # MBR ``state`` + ``status_effective_date`` (Phase 151). Labels seen in
    # the open-data API: Active, Struck Off, Dissolved, In Liquidation,
    # Defunct, Removed.
    mbr_state = (company.get("state") or "").strip()
    mbr_liveness = _liveness.classify(
        mbr_state,
        live=("active",),
        pending=("in liquidation", "in dissolution", "under liquidation"),
        terminal=("struck off", "dissolved", "defunct", "removed", "liquidated"),
    )
    mbr_since = (company.get("status_effective_date") or "").strip()[:10] or None
    _liveness.apply_register_status(
        mbr_entity,
        source_label=SOURCE_NAMES["malta_mbr"],
        liveness=mbr_liveness,
        raw=mbr_state or None,
        since=mbr_since if mbr_liveness != _liveness.LIVE else None,
    )
    yield mbr_entity


# Brazil CNPJ — QSA qualification label → BODS interest type.
# Owner-type qualifications (sócio / acionista / quotista / titular) map to
# ``shareholding``; management & representation roles map to
# ``seniorManagingOfficial``.
_BR_OWNER_KEYWORDS = ("socio", "sócio", "acionista", "quotista", "cotista", "titular")


def _br_interest_type(qualificacao: str | None) -> str:
    q = (qualificacao or "").strip().lower()
    if any(k in q for k in _BR_OWNER_KEYWORDS):
        return "shareholding"
    return "seniorManagingOfficial"


def map_cnpj_brazil(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CnpjBrazilAdapter bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Brazilian company.
    * Per QSA partner: a personStatement (natural person / foreign) or an
      entityStatement (legal-entity partner, carrying its own CNPJ) plus an
      ownership-or-control relationshipStatement. Owner-type qualifications →
      ``shareholding``; administrators/directors → ``seniorManagingOfficial``.
    """
    if not bundle or bundle.get("is_stub"):
        return

    cnpj: str = str(bundle.get("br_cnpj") or "")
    company: dict[str, Any] = bundle.get("company") or {}
    partners: list[dict[str, Any]] = bundle.get("partners") or []

    name: str = (
        (company.get("name") or "").strip()
        or bundle.get("legal_name")
        or (f"CNPJ {cnpj}" if cnpj else "")
    )
    if not cnpj or not name:
        return

    source_url = bundle.get("link") or f"https://opencnpj.org/{cnpj}"

    def _cnpj_id(value: str) -> dict[str, str]:
        return {
            "id": value,
            "scheme": "BR-RFB",
            "schemeName": "Receita Federal do Brasil — CNPJ",
        }

    # ── 1. Company entity statement ───────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="cnpj_brazil",
        local_id=cnpj,
        name=name,
        jurisdiction=("Brazil", "BR"),
        identifiers=[_cnpj_id(cnpj)],
        founding_date=company.get("founding_date"),
        addresses=(
            [_addr("registered", company["address"], "BR")]
            if company.get("address")
            else []
        ),
        alternate_names=[company["trade_name"]] if company.get("trade_name") else [],
        entity_details=company.get("legal_nature"),
        source_url=source_url,
    )
    # Receita Federal ``situacao_cadastral`` (Phase 151): ATIVA is live;
    # BAIXADA (closed) and NULA (annulled) are terminal. SUSPENSA and INAPTA
    # are irregular-but-existing registrations and are left unclassified
    # rather than guessed. ``data_situacao_cadastral`` is the effective date.
    br_status = (company.get("status") or "").strip()
    br_liveness = _liveness.classify(
        br_status,
        live=("ATIVA", "02", "2"),
        terminal=("BAIXADA", "08", "8", "NULA", "01", "1"),
    )
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["cnpj_brazil"],
        liveness=br_liveness,
        raw=br_status or None,
        since=(company.get("status_date") if br_liveness == _liveness.TERMINAL else None),
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. QSA partners / administrators ──────────────────────────────────
    seen: set[str] = set()
    for idx, p in enumerate(partners):
        pname = (p.get("name") or "").strip()
        if not pname:
            continue

        interest = {
            "type": _br_interest_type(p.get("role")),
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if p.get("role"):
            interest["details"] = p["role"]
        if p.get("entry_date"):
            interest["startDate"] = p["entry_date"]

        if p.get("kind") == "entity":
            partner_cnpj = p.get("cnpj")
            local_id = partner_cnpj or f"{cnpj}:pj:{idx}"
            ip_type = "entity"
            if local_id not in seen:
                yield make_entity_statement(
                    source_id="cnpj_brazil",
                    local_id=local_id,
                    name=pname,
                    jurisdiction=("Brazil", "BR"),
                    identifiers=[_cnpj_id(partner_cnpj)] if partner_cnpj else [],
                    source_url=source_url,
                )
                seen.add(local_id)
            ip_id = _stable_id("cnpj_brazil", "entity", local_id)
        else:
            # Natural person (PF) or foreign individual — scope the local id to
            # the company so identical names across companies don't false-merge.
            local_id = f"{cnpj}:pf:{pname}"
            ip_type = "person"
            if local_id not in seen:
                yield make_person_statement(
                    source_id="cnpj_brazil",
                    local_id=local_id,
                    full_name=pname,
                    source_url=source_url,
                )
                seen.add(local_id)
            ip_id = _stable_id("cnpj_brazil", "person", local_id)

        yield make_relationship_statement(
            source_id="cnpj_brazil",
            local_id=f"{cnpj}:rel:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=ip_id,
            interested_party_type=ip_type,
            interests=[interest],
            source_url=source_url,
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


# ----------------------------------------------------------------------
# PRH — Finnish Patent and Registration Office (Patentti- ja rekisterihallitus)
# ----------------------------------------------------------------------

# Finnish company form codes → BODS entityType.
# PRH uses uppercase abbreviations from Finnish company law.
_PRH_ENTITY_TYPES: dict[str, str] = {
    "OY": "registeredEntity",    # Osakeyhtiö — private limited company
    "OYJ": "registeredEntity",   # Julkinen osakeyhtiö — public limited company
    "KY": "registeredEntity",    # Kommandiittiyhtiö — limited partnership
    "AY": "registeredEntity",    # Avoin yhtiö — general partnership
    "OOY": "registeredEntity",   # Osuuskunta — co-operative
    "OK": "registeredEntity",    # Osuuskunta — co-operative (alternate code)
    "SÄÄ": "registeredEntity",   # Säätiö — foundation
    "VOJ": "registeredEntity",   # Vakuutusosakeyhtiö — insurance company
    "MTY": "registeredEntity",   # Maatilatalouden yhtymä — farm association
    "ETS": "registeredEntity",   # Eurooppayhtiö (SE) — Societas Europaea
    "EOY": "registeredEntity",   # Eurooppaosuuskunta (SCE) — European co-op
}


def _prh_entity_type(company_form: str) -> str:
    """Map a PRH companyForm code to a BODS entityType."""
    code = (company_form or "").strip().upper()
    return _PRH_ENTITY_TYPES.get(code, "registeredEntity")


def _prh_current_name(names: list[dict[str, Any]]) -> str:
    """Extract the current primary name from the PRH names array."""
    active = [n for n in names if not n.get("endDate")]
    primary = [n for n in active if n.get("order") == 0]
    if primary:
        return (primary[0].get("name") or "").strip()
    if active:
        return (active[0].get("name") or "").strip()
    if names:
        return (names[0].get("name") or "").strip()
    return ""


def _prh_address(company: dict[str, Any]) -> dict[str, str] | None:
    """Build a BODS address dict from PRH address fields.

    PRH nests addresses in ``addresses[]`` or ``postAddress[]``. We
    prefer the first registered/visiting address, then postal.
    """
    for key in ("addresses", "postAddresses"):
        addrs = company.get(key) or []
        for addr in addrs:
            if addr.get("endDate"):
                continue
            parts = [
                (addr.get("street") or "").strip(),
                (addr.get("postCode") or "").strip(),
                (addr.get("city") or addr.get("postOffice") or "").strip(),
            ]
            non_empty = [p for p in parts if p]
            if non_empty:
                return _addr("registered", " ".join(non_empty), "FI")
    return None


def map_prh(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a PrhAdapter fetch bundle to a BODS v0.4 entity statement.

    Only entity data is available from the PRH YTJ Open Data API.
    Officer/role data requires the paid Virre service and is not
    included here.
    """
    if not bundle or bundle.get("is_stub"):
        return

    ytunnus: str = bundle.get("ytunnus") or ""
    company: dict[str, Any] = bundle.get("company") or {}

    names = company.get("names") or []
    name: str = (
        _prh_current_name(names)
        or bundle.get("legal_name")
        or f"FI-YTUNNUS {ytunnus}"
    )
    if not ytunnus or not name:
        return

    source_url = f"https://tietopalvelu.ytj.fi/yritystiedot.aspx?yavain={ytunnus}"

    company_form = (company.get("companyForm") or "").strip()
    entity_type = _prh_entity_type(company_form)

    # Registration date — look in businessId block.
    business_id = company.get("businessId") or {}
    founding_date = (business_id.get("registrationDate") or "")[:10] or None

    identifiers: list[dict[str, str]] = [
        {
            "id": ytunnus,
            "scheme": "FI-PRH",
            "schemeName": "Patentti- ja rekisterihallitus (PRH) — Finnish Trade Register",
        }
    ]

    # Business line code (TOL/NACE equivalent).
    biz_lines = company.get("mainBusinessLine") or []
    for bl in biz_lines:
        if bl.get("endDate"):
            continue
        code = (bl.get("code") or "").strip()
        if code:
            identifiers.append({
                "id": code,
                "scheme": "FI-TOL",
                "schemeName": "Finnish Standard Industrial Classification (TOL 2008)",
            })
        break  # Only the current primary business line.

    address = _prh_address(company)

    prh_entity = make_entity_statement(
        source_id="prh",
        local_id=ytunnus,
        name=name,
        jurisdiction=("Finland", "FI"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=[address] if address else [],
        entity_type=entity_type,
        source_url=source_url,
    )
    # YTJ open data (Phase 151): a ``liquidations`` block means a pending
    # process; an ``endDate`` on the company is its end. Otherwise the
    # register says nothing about liveness beyond listing it, so nothing is
    # asserted.
    prh_end = str(company.get("endDate") or "")[:10] or None
    if prh_end:
        _liveness.apply_register_status(
            prh_entity,
            source_label=SOURCE_NAMES["prh"],
            liveness=_liveness.TERMINAL,
            raw="endDate",
            since=prh_end,
        )
    elif company.get("liquidations"):
        liq = company.get("liquidations")
        first = liq[0] if isinstance(liq, list) and liq and isinstance(liq[0], dict) else {}
        _liveness.apply_register_status(
            prh_entity,
            source_label=SOURCE_NAMES["prh"],
            liveness=_liveness.PENDING,
            raw=str(first.get("type") or first.get("description") or "liquidation"),
            since=str(first.get("registrationDate") or "")[:10] or None,
        )
    yield prh_entity


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


# ----------------------------------------------------------------------
# Climate TRACE / Global Energy Monitor → BODS
# ----------------------------------------------------------------------


# GEM's Entity Type column follows BODS definitions by GEM's own documentation
# (August 2026 About sheet). "legal entity" maps to registeredEntity when the
# row carries a registry identifier (LEI), else legalEntity; blank or
# unrecognised values fall back the same way. "person" (2 rows in August 2026)
# has no honest entityStatement mapping and yields no statements at all.
_GEM_ENTITY_TYPE_MAP: dict[str, str] = {
    "state": "state",
    "state body": "stateBody",
    "arrangement": "arrangement",
    "unknown entity": "unknownEntity",
}


def _gem_entity_type(gem_row: dict[str, Any], lei: str) -> str | None:
    """BODS entityType.type for a GEM entities-CSV row, or None for 'person'.

    Deliberate consequence, accepted 2026-08-28: mapping GEM ``arrangement``
    honestly means the risk engine's TRUST_OR_ARRANGEMENT signal can fire
    from this ESG-category source.
    """
    raw = str(gem_row.get("Entity Type") or "").strip().lower()
    if raw == "person":
        return None
    mapped = _GEM_ENTITY_TYPE_MAP.get(raw)
    if mapped:
        return mapped
    return "registeredEntity" if len(lei) == 20 else "legalEntity"


def _gem_status_note(entity_status: dict[str, Any]) -> str:
    """One-sentence annotation text for a dissolved/amalgamated GEM entity."""
    status = entity_status.get("status")
    if status == "amalgamated":
        successor = (
            entity_status.get("merged_into_name")
            or entity_status.get("merged_into")
            or "another entity"
        )
        text = f"Global Energy Monitor records this entity as amalgamated into {successor}"
        if entity_status.get("merged_into_name") and entity_status.get("merged_into"):
            text += f" ({entity_status['merged_into']})"
    else:
        text = "Global Energy Monitor records this entity as dissolved"
    urls = entity_status.get("urls") or []
    if urls:
        text += ". Source: " + "; ".join(urls)
    return text + "."


def map_climatetrace(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Climate TRACE / GEM fetch bundle to BODS statements.

    Emits:
    * One entity statement for the subject company (GEM entity identifier),
      typed from GEM's Entity Type column (which follows BODS definitions);
      joint ventures are noted in ``entityType.details``.
    * For each declared GEM parent: one stub entity statement + one
      ``otherInfluenceOrControl`` relationship (``beneficialOwnershipOrControl``
      is ``False`` — parent declarations in GEM are corporate structure data,
      not beneficial ownership assertions).
    * For a dissolved or amalgamated entity (August 2026 GEOT fields): a
      ``commenting`` annotation on the subject's statement — never a
      ``dissolutionDate``, which requires a date GEM does not publish, and
      never ``recordStatus: "closed"``, which would misuse the record
      lifecycle on a first-and-only statement. An amalgamated entity's
      successor additionally gets a stub entity statement so it exists as a
      node; no relationship statement links them, because a merger is not an
      ownership or control interest and no BODS interest type fits.

    Emissions data is attached as an annotation via ``source.description``
    rather than as a BODS interest — BODS v0.4 has no concept of an
    "emissions interest" and the data is ESG context rather than ownership
    or control.
    """
    if not bundle or bundle.get("is_stub"):
        return BODSBundle()

    result = BODSBundle()

    entity_id: str = bundle.get("entity_id") or ""
    entity_name: str = bundle.get("entity_name") or entity_id
    lei: str = (bundle.get("lei") or "").strip().upper()

    if not entity_id:
        return result

    source_url = f"https://globalenergymonitor.org/"

    # Build identifiers list.
    identifiers: list[dict[str, str]] = [
        {
            "id": entity_id,
            "scheme": "GEM-ENTITY",
            "schemeName": "Global Energy Monitor Entity ID",
            "uri": f"https://globalenergymonitor.org/",
        }
    ]
    if len(lei) == 20:
        identifiers.append(
            {
                "id": lei,
                "scheme": "XI-LEI",
                "schemeName": "Global Legal Entity Identifier Index",
            }
        )

    # Determine jurisdiction from GEM row if available.
    gem_row: dict[str, str] = bundle.get("gem_row") or {}
    # GEM CSV uses "Headquarters Country" (ISO 3166-1 alpha-3), not "Country".
    country_raw: str = (
        gem_row.get("Headquarters Country")
        or gem_row.get("Registration Country")
        or gem_row.get("Country")
        or ""
    ).strip()
    jurisdiction = _country_obj(country_raw) if country_raw else None
    jur_tuple: tuple[str, str] | None = (
        (jurisdiction["name"], jurisdiction["code"]) if jurisdiction else None
    )

    entity_type = _gem_entity_type(gem_row, lei)
    if entity_type is None:
        # GEM types a handful of records as natural persons — an
        # entityStatement would misdescribe them, so emit nothing.
        return result

    entity_status: dict[str, Any] = bundle.get("entity_status") or {}

    entity = make_entity_statement(
        source_id="climatetrace",
        local_id=entity_id,
        name=entity_name,
        jurisdiction=jur_tuple,
        identifiers=identifiers,
        entity_type=entity_type,
        entity_details=(
            "Joint venture (per Global Energy Monitor)"
            if entity_status.get("jv")
            else None
        ),
        source_url=source_url,
    )
    if entity_status.get("status") in ("dissolved", "amalgamated"):
        annotate(
            entity,
            commenting(pointer("recordDetails"), _gem_status_note(entity_status)),
        )
    result.statements.append(entity)
    subject_statement_id: str = entity["statementId"]

    # A stub statement for the amalgamation successor, so "merged into X"
    # names a node that exists in the bundle. Deliberately NO relationship
    # statement: a merger is not an ownership or control interest.
    successor_id = (entity_status.get("merged_into") or "").strip()
    if successor_id:
        successor_lei = (entity_status.get("merged_into_lei") or "").strip().upper()
        successor_identifiers: list[dict[str, str]] = [
            {
                "id": successor_id,
                "scheme": "GEM-ENTITY",
                "schemeName": "Global Energy Monitor Entity ID",
            }
        ]
        if len(successor_lei) == 20:
            successor_identifiers.append(
                {
                    "id": successor_lei,
                    "scheme": "XI-LEI",
                    "schemeName": "Global Legal Entity Identifier Index",
                }
            )
        successor = make_entity_statement(
            source_id="climatetrace",
            local_id=successor_id,
            name=entity_status.get("merged_into_name") or successor_id,
            identifiers=successor_identifiers,
            entity_type="registeredEntity" if len(successor_lei) == 20 else "unknownEntity",
            source_url=source_url,
        )
        annotate(
            successor,
            commenting(
                pointer("recordDetails"),
                (
                    f"Successor entity: Global Energy Monitor records "
                    f"{entity_name} as merged into this entity."
                ),
            ),
        )
        result.statements.append(successor)

    # Emit stub entity + relationship for each declared parent.
    for parent in bundle.get("parents") or []:
        parent_eid = (parent.get("entity_id") or "").strip()
        parent_name = (parent.get("name") or parent_eid).strip()
        if not parent_eid:
            continue

        parent_entity = make_entity_statement(
            source_id="climatetrace",
            local_id=parent_eid,
            name=parent_name,
            identifiers=[
                {
                    "id": parent_eid,
                    "scheme": "GEM-ENTITY",
                    "schemeName": "Global Energy Monitor Entity ID",
                }
            ],
            entity_type="unknownEntity",
            source_url=source_url,
        )
        result.statements.append(parent_entity)

        interest: dict[str, Any] = {
            "type": "otherInfluenceOrControl",
            "beneficialOwnershipOrControl": False,
            "details": (
                "Parent organisation declared in GEM ownership tracker "
                "(not a beneficial ownership assertion)"
            ),
        }
        # GEM publishes the parent's share for most entities (parsed from the
        # "Gem parents IDs" column by the adapter, e.g. "E1000… [55.0%]").
        share = parent.get("share")
        if isinstance(share, (int, float)):
            interest["share"] = {"exact": float(share)}

        relationship = make_relationship_statement(
            source_id="climatetrace",
            local_id=f"{entity_id}-parent-{parent_eid}",
            subject_statement_id=subject_statement_id,
            interested_party_statement_id=parent_entity["statementId"],
            interested_party_type="entity",
            interests=[interest],
            source_url=source_url,
        )
        result.statements.append(relationship)

    return result


# ----------------------------------------------------------------------
# Firmenbuch — Austrian Commercial Register
# ----------------------------------------------------------------------


def _at_date_iso(raw: str) -> str | None:
    """Convert Austrian DD.MM.YYYY date to ISO 8601 (YYYY-MM-DD), or None."""
    raw = (raw or "").strip()
    if not raw:
        return None
    parts = raw.split(".")
    if len(parts) == 3:
        day, month, year = parts
        try:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass
    return raw  # already ISO or unrecognised — pass through


def map_firmenbuch(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a FirmenbuchAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Austrian company.
    * One personStatement per unique officer or individual shareholder.
    * One entityStatement per unique corporate shareholder.
    * One relationshipStatement per officer role.
    * One relationshipStatement per shareholder / partner record.
    """
    if not bundle or bundle.get("is_stub"):
        return

    fn: str = bundle.get("fn") or ""
    extract: dict[str, Any] = bundle.get("extract") or {}
    if not fn or not extract:
        return

    name: str = extract.get("name") or bundle.get("legal_name") or fn
    uid: str = extract.get("uid") or ""
    founding_date_iso = _at_date_iso(extract.get("founding_date") or "")
    address_str: str = extract.get("address") or ""
    stamm_kapital: float | None = extract.get("stamm_kapital")
    officers: list[dict[str, Any]] = extract.get("officers") or []
    shareholders: list[dict[str, Any]] = extract.get("shareholders") or []

    source_url = f"https://justizonline.gv.at/jop/web/firmenbuchabfrage?firmennummer={fn}"

    # ── Identifiers ────────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = [
        {"id": fn, "scheme": "AT-FB", "schemeName": "Firmenbuch (Austrian Commercial Register)"},
    ]
    if uid:
        identifiers.append(
            {"id": uid, "scheme": "AT-UID", "schemeName": "Umsatzsteuer-Identifikationsnummer (Austrian VAT ID)"}
        )

    # ── 1. Subject entity statement ────────────────────────────────────────
    addresses = [{"address": address_str, "country": "AT", "type": "registered"}] if address_str else []

    company_stmt = make_entity_statement(
        source_id="firmenbuch",
        local_id=fn,
        name=name,
        jurisdiction=("Austria", "AT"),
        identifiers=identifiers,
        founding_date=founding_date_iso,
        addresses=addresses,
        source_url=source_url,
    )
    # Firmenbuch AUFRECHT attribute → "aktiv" / "gelöscht" (Phase 151).
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["firmenbuch"],
        liveness=_liveness.classify(
            extract.get("status"),
            live=("aktiv", "aufrecht"),
            terminal=("gelöscht", "geloescht", "gelöscht (amtswegig)"),
        ),
        raw=extract.get("status") or None,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # Track already-emitted local IDs to avoid duplicate statements.
    seen_ids: set[str] = set()

    # ── 2. Officer statements ──────────────────────────────────────────────
    from opencheck.sources.firmenbuch import _role_to_interest  # local import avoids circular

    for idx, officer in enumerate(officers):
        full_name: str = officer.get("full_name") or ""
        if not full_name:
            continue

        role_code: str = officer.get("role_code") or ""
        role_name: str = officer.get("role_name") or role_code
        interest_type, display_label = _role_to_interest(role_code, role_name)

        dob = _at_date_iso(officer.get("dob") or "")
        person_local_id = f"firmenbuch:officer:{full_name.lower().replace(' ', '_')}:{officer.get('dob', '')}"

        if person_local_id not in seen_ids:
            person_stmt = make_person_statement(
                source_id="firmenbuch",
                local_id=person_local_id,
                full_name=full_name,
                birth_date=dob,
                source_url=source_url,
            )
            yield person_stmt
            seen_ids.add(person_local_id)
        else:
            person_stmt = {"statementId": _stable_id("firmenbuch", "person", person_local_id)}

        yield make_relationship_statement(
            source_id="firmenbuch",
            local_id=f"{fn}:officer:{idx}:{person_local_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=[{
                "type": interest_type,
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": display_label,
            }],
            source_url=source_url,
        )

    # ── 3. Shareholder statements ──────────────────────────────────────────
    for idx, sh in enumerate(shareholders):
        display_name: str = sh.get("display_name") or ""
        if not display_name:
            continue

        is_person: bool = sh.get("is_person", True)
        einlage: float | None = sh.get("einlage")
        sh_fn: str = sh.get("fn") or ""
        kind: str = sh.get("kind") or "gesellschafter"

        if kind == "komplementaer":
            interest_type = "otherInfluenceOrControl"
            detail_label = "Komplementär (General Partner)"
        elif kind == "kommanditist":
            interest_type = "shareholding"
            detail_label = "Kommanditist (Limited Partner)"
        else:
            interest_type = "shareholding"
            detail_label = "Gesellschafter (Shareholder)"

        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
            "details": detail_label,
        }

        if interest_type == "shareholding" and einlage is not None and stamm_kapital:
            try:
                pct = round(einlage / stamm_kapital * 100, 4)
                interest["share"] = {"exact": pct}
            except ZeroDivisionError:
                pass

        if is_person:
            dob_sh = _at_date_iso(sh.get("dob") or "")
            sh_local_id = f"firmenbuch:sh:{display_name.lower().replace(' ', '_')}:{sh.get('dob', '')}"
            if sh_local_id not in seen_ids:
                sh_stmt = make_person_statement(
                    source_id="firmenbuch",
                    local_id=sh_local_id,
                    full_name=display_name,
                    birth_date=dob_sh,
                    source_url=source_url,
                )
                yield sh_stmt
                seen_ids.add(sh_local_id)
            else:
                sh_stmt = {"statementId": _stable_id("firmenbuch", "person", sh_local_id)}
            interested_party_type = "person"
        else:
            sh_local_id = f"firmenbuch:sh_entity:{sh_fn or display_name.lower().replace(' ', '_')}"
            sh_identifiers: list[dict[str, str]] = []
            if sh_fn:
                sh_identifiers.append(
                    {"id": sh_fn, "scheme": "AT-FB", "schemeName": "Firmenbuch (Austrian Commercial Register)"}
                )
            if sh_local_id not in seen_ids:
                sh_stmt = make_entity_statement(
                    source_id="firmenbuch",
                    local_id=sh_local_id,
                    name=display_name,
                    jurisdiction=None,
                    identifiers=sh_identifiers,
                    source_url=source_url,
                )
                yield sh_stmt
                seen_ids.add(sh_local_id)
            else:
                sh_stmt = {"statementId": _stable_id("firmenbuch", "entity", sh_local_id)}
            interested_party_type = "entity"

        yield make_relationship_statement(
            source_id="firmenbuch",
            local_id=f"{fn}:sh:{idx}:{sh_local_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=sh_stmt["statementId"],
            interested_party_type=interested_party_type,
            interests=[interest],
            source_url=source_url,
        )


# ----------------------------------------------------------------------
# Open Ownership BODS bulk data — passthrough mappers
# ----------------------------------------------------------------------
# The bods_gleif and bods_uk_psc adapters already reconstruct full BODS
# v0.4 statements inside their fetch() method and return them under the
# ``bods_statements`` key. These mapper functions are simple passthroughs
# so the _MAPPERS dispatch in app.py can route to them uniformly.
# ----------------------------------------------------------------------


def map_bods_gleif(bundle: dict[str, Any]) -> BODSBundle:
    """Passthrough mapper for the Open Ownership GLEIF bulk data adapter.

    The adapter returns ``{"bods_statements": [...], ...}`` directly from
    its Parquet reconstruction step; we just yield those statements.
    """
    return iter(bundle.get("bods_statements", []))


def map_bods_uk_psc(bundle: dict[str, Any]) -> BODSBundle:
    """Passthrough mapper for the Open Ownership UK PSC bulk data adapter.

    Same pattern as map_bods_gleif — statements are pre-built by the
    adapter; this function makes them visible to the _MAPPERS dispatch.
    """
    return iter(bundle.get("bods_statements", []))


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

    def _share(v: Any) -> dict[str, Any] | None:
        return {"exact": v} if isinstance(v, (int, float)) and 0 < v <= 100 else None

    start = psc.get("notified") or None

    def _mk(itype: str, share_val: Any = None, details: str | None = None) -> dict[str, Any]:
        i: dict[str, Any] = set_beneficial_ownership(
            {"type": itype, "directOrIndirect": "direct"},
            "cac_nigeria",
            record_kind=record_kind,
        )
        s = _share(share_val)
        if s:
            i["share"] = s
        if details:
            i["details"] = details
        if start:
            i["startDate"] = start
        return i

    if psc.get("shares"):
        out.append(_mk("shareholding", psc.get("share_pct_direct")))
    if psc.get("voting"):
        out.append(_mk("votingRights", psc.get("voting_pct_direct")))
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


def map_cac_nigeria(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CacNigeriaAdapter bundle to BODS v0.4 statements.

    Yields one entityStatement for the Nigerian company, and per beneficial
    owner (person / entity / arrangement / unknown) a person- or
    entityStatement plus an ownership-or-control relationshipStatement. Owners
    are deduped by canonical name so a shared owner (e.g. Dangote Industries)
    reuses one statement. The five CAMA PSC conditions map to BODS interest
    types (see ``_cac_interests``).
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

    # ── 2. Group PSC rows by canonical owner, emit owners + relationships ──
    groups: dict[str, dict[str, Any]] = {}
    for psc in record.get("pscs") or []:
        owner = (psc.get("owner_name") or "").strip()
        if not owner:
            continue
        kind = psc.get("owner_kind") or "entity"
        g = groups.setdefault(owner, {"kind": kind, "psc": psc, "ilists": []})
        g["ilists"].append(_cac_interests(
            psc,
            record_kind=(
                "psc_natural_person" if kind == "person" else "psc_corporate"
            ),
        ))

    emitted: set[str] = set()
    for owner, g in groups.items():
        kind = g["kind"]
        psc = g["psc"]
        owner_rc = psc.get("owner_rc") or None
        juris = psc.get("owner_jurisdiction") or None

        if kind == "person":
            local_id = f"person:{owner}"
            nationalities = []
            nat = psc.get("nationality") or ""
            co = _country_obj(nat) if nat else None
            if co:
                nationalities = [co]
            if local_id not in emitted:
                yield make_person_statement(
                    source_id="cac_nigeria",
                    local_id=local_id,
                    full_name=owner,
                    nationalities=nationalities,
                    source_url=source_url,
                )
                emitted.add(local_id)
            ip_id = _stable_id("cac_nigeria", "person", local_id)
        else:
            entity_type = {
                "arrangement": "arrangement",
                "unknown": "unknownEntity",
            }.get(kind, "registeredEntity")
            local_id = f"entity:{owner_rc or owner}"
            idents = []
            if owner_rc:
                idents = [{
                    "id": str(owner_rc),
                    "scheme": "NG-CAC",
                    "schemeName": "Nigeria Corporate Affairs Commission",
                }]
            if local_id not in emitted:
                yield make_entity_statement(
                    source_id="cac_nigeria",
                    local_id=local_id,
                    name=owner,
                    jurisdiction=("Nigeria", "NG") if juris == "NG" else None,
                    identifiers=idents,
                    entity_type=entity_type,
                    source_url=source_url,
                )
                emitted.add(local_id)
            ip_id = _stable_id("cac_nigeria", "entity", local_id)

        yield make_relationship_statement(
            source_id="cac_nigeria",
            local_id=f"{rc}:{owner}",
            subject_statement_id=subject_id,
            interested_party_statement_id=ip_id,
            interested_party_type="person" if kind == "person" else "entity",
            interests=_cac_merge_interests(g["ilists"]),
            source_url=source_url,
        )


# ----------------------------------------------------------------------
# Lithuanian Register of Legal Entities (JAR) → BODS v0.4
# ----------------------------------------------------------------------

# Map common Lithuanian legal form abbreviations to BODS entity types.
_LT_ENTITY_TYPES: dict[str, str] = {
    "UAB": "registeredEntity",          # Private limited company
    "AB": "registeredEntity",           # Public limited company
    "MB": "registeredEntity",           # Small partnership
    "IĮ": "registeredEntity",           # Sole proprietorship
    "TŪB": "registeredEntity",          # General partnership
    "KŪB": "registeredEntity",          # Limited partnership
    "VšĮ": "legalEntity",               # Public institution (non-profit)
    "Asociacija": "legalEntity",        # Association
    "Valstybės įmonė": "stateBody",     # State enterprise
    "Savivaldybės įmonė": "stateBody",  # Municipal enterprise
    "Biudžetinė įstaiga": "stateBody",  # Budget institution
    "Labdaros ir paramos fondas": "legalEntity",  # Charitable foundation
    "Kooperatinė bendrovė": "registeredEntity",   # Cooperative
}


def map_jar_lithuania(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a JarLithuaniaAdapter fetch bundle to a BODS v0.4 entity statement.

    Only entity-level data (name, code, address, legal form, status) is
    available from the JAR public search interface.  Participant / beneficial
    ownership data from the former JADIS system has been migrated to JANGIS,
    which is restricted to legitimate-interest access; it is intentionally
    excluded from this adapter.
    """
    if not bundle or bundle.get("is_stub"):
        return

    lt_code: str = (bundle.get("lt_code") or bundle.get("hit_id") or "").strip()
    name: str = (bundle.get("name") or "").strip() or f"LT-{lt_code}"
    if not lt_code or not name:
        return

    legal_form: str = (bundle.get("legal_form") or "").strip()
    entity_type: str = _LT_ENTITY_TYPES.get(legal_form, "registeredEntity")

    # Address — pre-formatted string from the JAR HTML.
    raw_address: str = (bundle.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "LT")]
        if raw_address
        else []
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": lt_code,
            "scheme": "LT-JAR",
            "schemeName": "Juridinių Asmenų Registras (Lithuanian Register of Legal Entities)",
        }
    ]

    source_url = f"https://www.registrucentras.lt/jar/p/index.php?kod={lt_code}"

    jar_entity = make_entity_statement(
        source_id="jar_lithuania",
        local_id=lt_code,
        name=name,
        jurisdiction=("Lithuania", "LT"),
        identifiers=identifiers,
        addresses=addresses,
        entity_type=entity_type,
        source_url=source_url,
    )
    # JAR status (Phase 151), in the register's Lithuanian: Veikiantis =
    # operating; Likviduojamas / Bankrutuojantis / Bankrotas /
    # Reorganizuojamas = a process under way; Išregistruotas = deregistered.
    # Sustabdyta (suspended) is left unclassified.
    _liveness.apply_register_status(
        jar_entity,
        source_label=SOURCE_NAMES["jar_lithuania"],
        liveness=_liveness.classify(
            bundle.get("status"),
            live=("Veikiantis", "Veikianti", "Veikiantys"),
            pending=("Likviduojamas", "Likviduojama", "Bankrutuojantis", "Bankrutuojanti", "Bankrotas", "Reorganizuojamas", "Reorganizuojama"),
            terminal=("Išregistruotas", "Išregistruota", "Išregistruotas iš registro"),
        ),
        raw=(bundle.get("status") or "").strip() or None,
    )
    yield jar_entity


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


# ----------------------------------------------------------------------
# KRS Poland (Krajowy Rejestr Sądowy) → BODS
# ----------------------------------------------------------------------


def map_krs_poland(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a KrsPolandAdapter fetch bundle to BODS v0.4 statements.

    Yields a single entity statement.  No person or ownership-or-control
    statements are emitted because the KRS public API masks personal data
    (names appear as "Ł*******", PESEL as "7**********").  The CRBR adapter
    (Phase 32) provides the unmasked beneficial ownership data.

    Identifiers emitted:
      • KRS number  — scheme "PL-KRS"
      • NIP         — scheme "PL-NIP"  (if present)
      • REGON       — scheme "PL-REGON" (if present; 9-digit entity-level)
    """
    if not bundle or bundle.get("is_stub"):
        return

    krs: str = (bundle.get("pl_krs") or bundle.get("hit_id") or "").strip()
    if not krs:
        return

    name: str = (bundle.get("name") or "").strip() or f"PL-KRS-{krs}"

    # ----------------------------------------------------------------
    # Entity type
    # ----------------------------------------------------------------
    legal_form: str = (bundle.get("legal_form") or "").upper()
    _BODS_ENTITY_TYPE: dict[str, str] = {
        "FUNDACJA": "arrangement",
        "STOWARZYSZENIE": "arrangement",
        "PRZEDSIĘBIORSTWO PAŃSTWOWE": "stateBody",
        "AGENCJA": "stateBody",
    }
    entity_type = "registeredEntity"
    for kw, bods_type in _BODS_ENTITY_TYPE.items():
        if kw in legal_form:
            entity_type = bods_type
            break

    # ----------------------------------------------------------------
    # Address
    # ----------------------------------------------------------------
    raw_address: str = (bundle.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "PL")] if raw_address else []
    )

    # ----------------------------------------------------------------
    # Identifiers
    # ----------------------------------------------------------------
    identifiers: list[dict[str, str]] = [
        {
            "id": krs,
            "scheme": "PL-KRS",
            "schemeName": "Polish National Court Register (Krajowy Rejestr Sądowy)",
        }
    ]
    nip: str = (bundle.get("nip") or "").strip()
    if nip:
        identifiers.append({
            "id": nip,
            "scheme": "PL-NIP",
            "schemeName": "Polish Tax Identification Number (Numer Identyfikacji Podatkowej)",
        })
    regon: str = (bundle.get("regon") or "").strip()
    if regon:
        identifiers.append({
            "id": regon,
            "scheme": "PL-REGON",
            "schemeName": "Polish Statistical Number (Rejestr Gospodarki Narodowej)",
        })

    founding_date: str | None = bundle.get("registration_date")
    source_url: str = bundle.get("link") or f"https://ekrs.ms.gov.pl/"

    entity_stmt = make_entity_statement(
        source_id="krs_poland",
        local_id=krs,
        name=name,
        jurisdiction=("Poland", "PL"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        entity_type=entity_type,
        source_url=source_url,
        # dataOstatniegoWpisu — "date of the last entry" in the KRS. When the
        # court register last revised this record, i.e. its declaration date.
        # Not dataRejestracjiWKRS, which is the original registration.
        statement_date=bundle.get("last_change_date"),
    )
    yield entity_stmt


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


# ----------------------------------------------------------------------
# BCE Belgium → BODS
# ----------------------------------------------------------------------


def map_bce_belgium(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a BceBelgiumAdapter fetch bundle to BODS v0.4 statements.

    Returns a single entity statement.  No beneficial ownership data is
    available from the BCE/KBO open data publication — the Belgian UBO
    register is not openly accessible.

    Identifiers emitted:
      * BE-BCE_KBO  — 10-digit enterprise number, no dots (e.g. "0433795975")
      * XI-VAT      — Belgian VAT number derived from the enterprise number
                      by prepending "BE0" for numbers beginning with 0 or
                      "BE" for numbers beginning with 1.
    """
    if not bundle or bundle.get("is_stub"):
        return

    enterprise_number: str = bundle.get("enterprise_number") or ""
    dotted: str = bundle.get("dotted") or ""
    name: str = (
        bundle.get("name")
        or bundle.get("name_nl")
        or bundle.get("name_fr")
        or bundle.get("name_de")
        or ""
    )
    if not enterprise_number or not name:
        return

    source_url = bundle.get("link") or (
        f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
        f"?ondernemingsnummer={enterprise_number}"
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": enterprise_number,
            "scheme": "BE-BCE_KBO",
            "schemeName": "Crossroads Bank for Enterprises (Belgium)",
        }
    ]

    # Belgian VAT numbers are the enterprise number prefixed with "BE":
    # enterprises starting with 0 → e.g. BE0433795975
    # enterprises starting with 1 → e.g. BE1234567890 (natural persons — rare)
    # The VAT-registration status is NOT checked here (not all enterprises are
    # VAT-registered); we emit the potential VAT number as an identifier hint.
    if enterprise_number.isdigit() and len(enterprise_number) == 10:
        identifiers.append({
            "id": f"BE{enterprise_number}",
            "scheme": "XI-VAT",
            "schemeName": "VAT number",
        })

    # Founding date from BCE start_date (may be YYYY-MM-DD or blank).
    founding_date: str | None = bundle.get("start_date") or None
    if founding_date and not re.match(r"^\d{4}-\d{2}-\d{2}$", founding_date):
        founding_date = None

    address_str = bundle.get("address") or ""
    addresses = (
        [_addr("registered", address_str, "BE")]
        if address_str
        else []
    )

    entity_stmt = make_entity_statement(
        source_id="bce_belgium",
        local_id=enterprise_number,
        name=name,
        jurisdiction=("Belgium", "BE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # KBO/BCE open-data ``Status``: AC (active) / ST (stopped) (Phase 151).
    be_status = (bundle.get("status") or "").strip()
    _liveness.apply_register_status(
        entity_stmt,
        source_label=SOURCE_NAMES["bce_belgium"],
        liveness=_liveness.classify(be_status, live=("AC", "active"), terminal=("ST", "stopped")),
        raw=be_status or None,
    )
    yield entity_stmt


# ----------------------------------------------------------------------
# Corporations Canada — ISED federal register
# ----------------------------------------------------------------------


def _cc_corp_url(corp_id: str) -> str:
    return (
        f"https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html"
        f"?corpId={corp_id}&V_TOKEN=null&LANGUAGE_ID=1"
    )


def _cc_current_name(corp: dict[str, Any]) -> str:
    """Return the current primary corporation name."""
    names = corp.get("corporationNames") or []
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current") and (cn.get("nameType") or "").lower() == "primary":
            return (cn.get("name") or "").strip()
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current"):
            return (cn.get("name") or "").strip()
    if names:
        cn = (names[-1].get("CorporationName") or {})
        return (cn.get("name") or "").strip()
    return ""


def map_corporations_canada(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CorporationsCanadaAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Canadian federal corporation.
    * One personStatement per current director.
    * One relationshipStatement (seniorManagingOfficial) per director.
    """
    if not bundle or bundle.get("is_stub"):
        return

    corp_id: str = bundle.get("corp_id") or ""
    corp: dict[str, Any] = bundle.get("corporation") or {}
    directors: list[dict[str, Any]] = bundle.get("directors") or []

    name: str = _cc_current_name(corp) or bundle.get("legal_name") or corp_id
    if not corp_id or not name:
        return

    source_url = _cc_corp_url(corp_id)

    # ── Founding date from activities ─────────────────────────────────────
    founding_date: str | None = None
    dissolution_date: str | None = None
    for act_entry in corp.get("activities") or []:
        act = act_entry.get("activity") or {}
        act_type = (act.get("activity") or "").lower()
        if act_type in ("incorporation", "continuance", "amalgamation") and not founding_date:
            founding_date = act.get("date") or None
        # The same activity log records the end of the corporation (Phase 151).
        if any(k in act_type for k in ("dissolution", "dissolved", "discontinuance", "amalgamated into")):
            dissolution_date = act.get("date") or dissolution_date

    # ── Address from adresses (documented API typo) ───────────────────────
    addresses: list[dict[str, Any]] = []
    for addr_entry in corp.get("adresses") or []:
        addr = addr_entry.get("address") or {}
        lines = addr.get("addressLine") or []
        city = (addr.get("city") or "").strip()
        postal = (addr.get("postalCode") or "").strip()
        country = (addr.get("countryCode") or "CA").strip()
        parts = [ln.strip() for ln in lines if ln.strip()]
        if city:
            parts.append(city)
        if postal:
            parts.append(postal)
        if parts:
            addresses.append(_addr("registered", ", ".join(parts), country))
        break  # use only the first address entry

    # ── Identifiers ───────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = [
        {
            "id": corp_id,
            "scheme": "CA-CORP",
            "schemeName": "Corporations Canada — ISED federal register",
        }
    ]
    bn_block = corp.get("businessNumbers") or {}
    if isinstance(bn_block, dict):
        bn = (bn_block.get("businessNumber") or "").strip()
        if bn:
            identifiers.append({
                "id": bn,
                "scheme": "CA-BN",
                "schemeName": "Canada Revenue Agency Business Number",
            })

    # ── 1. Entity statement ───────────────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="corporations_canada",
        local_id=corp_id,
        name=name,
        jurisdiction=("Canada", "CA"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # Corporation ``status`` (Active / Dissolved / Amalgamated / Discontinued /
    # Inactive) plus any dissolution activity date (Phase 151).
    cc_status = (corp.get("status") or "").strip()
    cc_liveness = _liveness.classify(
        cc_status,
        live=("active",),
        pending=("dissolution pending", "pending dissolution", "in liquidation"),
        terminal=("dissolved", "amalgamated", "discontinued", "inactive", "revoked"),
    )
    if cc_liveness == _liveness.UNKNOWN and dissolution_date:
        cc_liveness = _liveness.TERMINAL
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["corporations_canada"],
        liveness=cc_liveness,
        raw=cc_status or None,
        since=dissolution_date,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. Director statements ────────────────────────────────────────────
    seen_person_ids: set[str] = set()

    for idx, director in enumerate(directors):
        first = (director.get("firstName") or "").strip()
        last = (director.get("lastName") or "").strip()
        full_name = " ".join(p for p in [first, last] if p)
        if not full_name:
            continue

        person_local_id = f"{corp_id}:director:{idx}:{full_name.lower()}"

        if person_local_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="corporations_canada",
                local_id=person_local_id,
                full_name=full_name,
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_local_id)
        else:
            person_stmt = {
                "statementId": _stable_id("corporations_canada", "person", person_local_id)
            }

        interests: list[dict[str, Any]] = [
            {
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": "Director",
            }
        ]

        yield make_relationship_statement(
            source_id="corporations_canada",
            local_id=f"{corp_id}:director:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )


# ---------------------------------------------------------------------------
# ACRA Singapore (data.gov.sg open data)
# ---------------------------------------------------------------------------
#
# ACRA publishes firmographic entity data only — no beneficial ownership
# information is included in the open dataset.  This mapper therefore
# produces a single entity statement.
#
# Fields mapped:
#   uen              → identifiers (SG-ACRA scheme)
#   entity_name      → name
#   uen_status_desc  → register status → liveness annotation (+ dissolutionDate never: no date published)
#   entity_type_desc → entity type label (stored in description)
#   uen_issue_date   → foundingDate
#   reg_street_name + reg_postal_code → registered address
#   link             → publicationDetails.publicationDate / source URL


def map_acra_singapore(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield BODS v0.4 statements for a Singapore ACRA entity.

    Only an entity statement is produced — the open dataset contains
    firmographic data only (no ownership / control relationships).
    """
    uen = (bundle.get("uen") or "").strip().upper()
    if not uen:
        return

    name = (bundle.get("entity_name") or "").strip()
    status_raw = (bundle.get("uen_status_desc") or "").strip().lower()
    entity_type = (bundle.get("entity_type_desc") or "").strip()
    issue_date = (bundle.get("uen_issue_date") or "").strip()
    street = (bundle.get("reg_street_name") or "").strip()
    postal = (bundle.get("reg_postal_code") or "").strip()
    source_url = bundle.get("link") or "https://data.gov.sg/datasets?query=acra"

    # Identifier block.
    identifiers: list[dict[str, str]] = [
        {
            "id": uen,
            "scheme": "SG-ACRA",
            "schemeName": "Singapore Unique Entity Number — Accounting and Corporate Regulatory Authority (ACRA)",
        }
    ]

    # Address block.
    addresses: list[dict[str, Any]] = []
    addr_parts = [p for p in [street, postal, "Singapore"] if p]
    if len(addr_parts) > 1:  # at least street or postal + country
        addresses.append(_addr("registered", ", ".join(addr_parts), "SG"))

    # Founding date from uen_issue_date.
    founding_date: str | None = None
    if issue_date and re.match(r"\d{4}-\d{2}-\d{2}", issue_date):
        founding_date = issue_date

    # The open dataset carries a status label but no date, so the status is
    # recorded as a liveness annotation and ``dissolutionDate`` is never set
    # (Phase 151 — it used to be written as JSON null, which the schema
    # forbids). Labels seen in the dataset: "Live", "Live Company", "Struck
    # Off", "Cancelled", "Ceased Registration", "Dissolved", "Amalgamated",
    # "In Liquidation", "Receivership", "Converted To LLP".
    if any(k in status_raw for k in ("struck off", "cancelled", "ceased", "dissolved", "amalgamated", "converted")):
        acra_liveness = _liveness.TERMINAL
    elif any(k in status_raw for k in ("liquidation", "receivership", "winding")):
        acra_liveness = _liveness.PENDING
    elif status_raw.startswith("live"):
        acra_liveness = _liveness.LIVE
    else:
        acra_liveness = _liveness.UNKNOWN

    # Entity statement.
    stmt = make_entity_statement(
        source_id="acra_singapore",
        local_id=uen,
        name=name or f"SG Entity {uen}",
        jurisdiction=("Singapore", "SG"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )

    # Annotate with entity type in the description field if available.
    if entity_type:
        record_details = stmt.get("recordDetails") or {}
        record_details["entityType"] = {
            "type": "registeredEntity",
            "subtype": entity_type,
        }
        stmt["recordDetails"] = record_details

    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["acra_singapore"],
        liveness=acra_liveness,
        raw=(bundle.get("uen_status_desc") or "").strip() or None,
    )

    yield stmt


# ---------------------------------------------------------------------------
# CVR Denmark → BODS
# ---------------------------------------------------------------------------


def map_cvr_denmark(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CVR Denmark bundle to BODS v0.4 statements.

    CVRPerson (natural persons) is a restricted entity not available without
    a separate access application, so this mapper produces entity statements
    only — no person or ownership-or-control statements.

    The entity identifier uses the "DK-CVR" scheme (CVR number).
    """
    cvr_number: str = bundle.get("cvr_number", "")
    if bundle.get("is_stub") or not cvr_number:
        return
    name: str = bundle.get("name", "") or cvr_number
    status: str = bundle.get("status", "unknown")
    start_date: str | None = bundle.get("start_date")
    end_date: str | None = bundle.get("end_date")
    legal_form_text: str | None = bundle.get("legal_form_text")
    branche_code: str | None = bundle.get("branche_code")
    source_url: str | None = bundle.get("source_url")
    addr_raw: dict[str, Any] | None = bundle.get("address")

    # Identifiers.
    identifiers: list[dict[str, str]] = [
        {
            "id": cvr_number,
            "scheme": "DK-CVR",
            "schemeName": (
                "Det Centrale Virksomhedsregister — Danish Central Business Register"
            ),
        }
    ]

    # Address block.
    addresses: list[dict[str, Any]] = []
    if addr_raw:
        parts = []
        street = addr_raw.get("CVRAdresse_vejnavn", "")
        house = addr_raw.get("CVRAdresse_husnummerFra", "")
        if street:
            parts.append(f"{street} {house}".strip())
        postal = addr_raw.get("CVRAdresse_postnummer", "")
        city = addr_raw.get("CVRAdresse_postdistrikt", "")
        if postal or city:
            parts.append(f"{postal} {city}".strip())
        country_code = addr_raw.get("CVRAdresse_landekode", "DK") or "DK"
        country_names = {"DK": "Denmark"}
        country_name = country_names.get(country_code, country_code)
        full_address = ", ".join(p for p in parts if p)
        if full_address:
            addresses.append(_addr("registered", full_address, country_code))

    # Jurisdiction.
    jurisdiction = ("Denmark", "DK")

    # Entity statement.
    stmt = make_entity_statement(
        source_id="cvr_denmark",
        local_id=cvr_number,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        founding_date=start_date,
        addresses=addresses,
        source_url=source_url,
    )

    # entityType block.  BODS v0.4 entityType.subtype is a restricted enum
    # (governmentDepartment, stateAgency, other, trust, nomination) and does
    # not accept arbitrary legal-form text.  The Danish legal form label is
    # stored in the non-schema annotation field "legalFormLabel" for
    # informational use only; libcovebods ignores unknown extra fields.
    record_details = stmt.get("recordDetails") or {}
    record_details["entityType"] = {"type": "registeredEntity"}
    if legal_form_text:
        record_details["legalFormLabel"] = legal_form_text

    # Register status → liveness (Phase 151). ``status`` is the adapter's
    # normalised English label (``_STATUS_MAP`` in sources/cvr_denmark.py,
    # falling back to the API's own text); ``end_date`` is
    # ``virksomhedOphoersdato``. A published end date is the register's own
    # statement that the company ended and outranks the label.
    cvr_liveness = _liveness.classify(
        status,
        live=("active",),
        pending=("in bankruptcy", "in forced dissolution", "in voluntary liquidation"),
        terminal=("dissolved", "dissolved (error registration)", "deleted"),
    )
    if end_date:
        cvr_liveness = _liveness.TERMINAL
    stmt["recordDetails"] = record_details
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["cvr_denmark"],
        liveness=cvr_liveness,
        raw=bundle.get("status_label") or (status if status != "unknown" else None),
        since=end_date,
    )

    # Industry code (DB07/NACE) as supplemental annotation.
    if branche_code:
        record_details["primaryIndustryCode"] = branche_code

    stmt["recordDetails"] = record_details

    yield stmt


# ---------------------------------------------------------------------------
# Cyprus DRCOR (data.gov.cy open data)
# ---------------------------------------------------------------------------
#
# DRCOR's open dataset carries company + role-holder data but no
# shareholders, so this mapper produces:
#   * one entityStatement for the organisation,
#   * one personStatement / entityStatement per official, and
#   * one relationshipStatement (seniorManagingOfficial) per official.


# Tokens that mark an "official" row as a corporate body rather than a person
# (Cyprus secretaries are frequently companies). Greek "ΛΤΔ" / "ΛΙΜΙΤΕΔ" and
# the Latin equivalents.
_CY_ORG_OFFICIAL_TOKENS = (
    "LTD", "LIMITED", "PLC", "ΛΤΔ", "ΛΙΜΙΤΕΔ", "SECRETARIAL", "SERVICES LIMITED",
)


def _cy_official_is_org(name: str) -> bool:
    up = (name or "").upper()
    return any(tok in up for tok in _CY_ORG_OFFICIAL_TOKENS)


def map_cyprus_drcor(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CyprusDrcorAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Cypriot organisation.
    * One person/entity statement per official (corporate officials such as
      secretarial companies are emitted as entityStatements).
    * One relationshipStatement (seniorManagingOfficial) per official.
    """
    # Local import to avoid a circular import at module load time.
    from ..sources.cyprus_drcor import _field as _cy_field

    if not bundle or bundle.get("is_stub"):
        return

    reg_no = (bundle.get("reg_no") or "").strip()
    organisation = bundle.get("organisation") or {}
    officials = bundle.get("officials") or []
    if not reg_no or not organisation:
        return

    name = _cy_field(organisation, "org_name") or bundle.get("name") or f"CY {reg_no}"
    source_url = bundle.get("link") or None

    # ── Identifiers ───────────────────────────────────────────────────────
    type_code = _cy_field(organisation, "org_type_code") or "HE"
    display_reg = f"{type_code}{reg_no}" if type_code else reg_no
    identifiers: list[dict[str, str]] = [
        {
            "id": display_reg,
            "scheme": "CY-DRCOR",
            "schemeName": (
                "Cyprus registration number — Department of Registrar of "
                "Companies and Intellectual Property"
            ),
        }
    ]

    # ── Founding date + address ───────────────────────────────────────────
    reg_date = _cy_field(organisation, "reg_date")
    founding_date: str | None = reg_date if re.match(r"\d{4}-\d{2}-\d{2}", reg_date) else None

    addresses: list[dict[str, Any]] = []
    addr = bundle.get("address") or {}
    addr_parts = [
        p for p in (
            _cy_field(addr, "street"),
            _cy_field(addr, "building"),
            _cy_field(addr, "territory"),
        ) if p
    ]
    if addr_parts:
        addresses.append(_addr("registered", ", ".join(addr_parts), "CY"))

    # ── 1. Entity statement ───────────────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="cyprus_drcor",
        local_id=reg_no,
        name=name,
        jurisdiction=("Cyprus", "CY"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # DRCOR organisation status (Phase 151).
    cy_status = (_cy_field(organisation, "org_status") or "").strip()
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["cyprus_drcor"],
        liveness=_liveness.classify(
            cy_status,
            live=("Active", "Εγγεγραμμένη"),
            pending=("Under Liquidation", "In Liquidation", "Under Strike Off", "Strike Off Pending"),
            terminal=("Dissolved", "Struck Off", "Deleted", "Liquidated"),
        ),
        raw=cy_status or None,
    )
    org_type_label = _cy_field(organisation, "org_type")
    if org_type_label:
        rd = company_stmt.get("recordDetails") or {}
        rd["entityType"] = {"type": "registeredEntity", "subtype": org_type_label}
        company_stmt["recordDetails"] = rd
    yield company_stmt
    company_stmt_id = company_stmt["statementId"]

    # ── 2. Officials ──────────────────────────────────────────────────────
    seen: set[str] = set()
    for idx, official in enumerate(officials):
        full_name = _cy_field(official, "official_name")
        if not full_name:
            continue
        position = _cy_field(official, "official_position") or "Official"
        is_org = _cy_official_is_org(full_name)
        party_local = f"{reg_no}:official:{idx}:{full_name.lower()}"

        if party_local not in seen:
            if is_org:
                party_stmt = make_entity_statement(
                    source_id="cyprus_drcor",
                    local_id=party_local,
                    name=full_name,
                    jurisdiction=("Cyprus", "CY"),
                    source_url=source_url,
                )
            else:
                party_stmt = make_person_statement(
                    source_id="cyprus_drcor",
                    local_id=party_local,
                    full_name=full_name,
                    source_url=source_url,
                )
            yield party_stmt
            seen.add(party_local)
            party_stmt_id = party_stmt["statementId"]
        else:
            kind = "entity" if is_org else "person"
            party_stmt_id = _stable_id("cyprus_drcor", kind, party_local)

        interests: list[dict[str, Any]] = [
            {
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": position,
            }
        ]
        yield make_relationship_statement(
            source_id="cyprus_drcor",
            local_id=f"{reg_no}:official:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=party_stmt_id,
            interested_party_type="entity" if is_org else "person",
            interests=interests,
            source_url=source_url,
        )


# ---------------------------------------------------------------------------
# Australian Business Register — ABN Lookup (data.gov.au, CC BY 3.0 AU)
# ---------------------------------------------------------------------------
#
# ABN Lookup publishes entity-level firmographic data only — no officers or
# beneficial owners — so this mapper produces a single entity statement.


def map_abr_australia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield a BODS v0.4 entity statement for an Australian ABR entity.

    Identifiers: ``AU-ABN`` (always) and ``AU-ACN`` (companies only).
    Business/trading names become ``alternateNames``; the registered state and
    postcode become a partial registered address.
    """
    if not bundle or bundle.get("is_stub"):
        return

    abn = (bundle.get("abn") or "").strip()
    acn = (bundle.get("acn") or "").strip()
    name = (bundle.get("name") or "").strip()
    if not (abn or acn) or not name:
        return

    local_id = abn or acn
    source_url = bundle.get("link") or "https://abr.business.gov.au/"

    # ── Identifiers ───────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = []
    if abn:
        identifiers.append({
            "id": abn,
            "scheme": "AU-ABN",
            "schemeName": "Australian Business Number — Australian Business Register",
        })
    if acn:
        identifiers.append({
            "id": acn,
            "scheme": "AU-ACN",
            "schemeName": "Australian Company Number — Australian Securities and Investments Commission",
        })

    # ── Partial registered address (state + postcode only) ────────────────
    addresses: list[dict[str, Any]] = []
    addr_parts = [p for p in (bundle.get("state"), bundle.get("postcode")) if p]
    if addr_parts:
        addresses.append(_addr("registered", " ".join(addr_parts), "AU"))

    # ── Business / trading names → alternateNames ─────────────────────────
    alternate_names = [
        b for b in (bundle.get("business_names") or []) if isinstance(b, str) and b.strip()
    ]

    stmt = make_entity_statement(
        source_id="abr_australia",
        local_id=local_id,
        name=name,
        jurisdiction=("Australia", "AU"),
        identifiers=identifiers,
        addresses=addresses,
        alternate_names=alternate_names,
        source_url=source_url,
    )

    # Entity type subtype + cancelled-status annotation.
    entity_type_name = (bundle.get("entity_type_name") or "").strip()
    abn_status = (bundle.get("abn_status") or "").strip().lower()
    record_details = stmt.get("recordDetails") or {}
    if entity_type_name:
        record_details["entityType"] = {"type": "registeredEntity", "subtype": entity_type_name}
    stmt["recordDetails"] = record_details
    # ABN Lookup publishes the ABN's status ("Active" / "Cancelled") and the
    # date it took effect. A cancelled ABN is the register's terminal state
    # for the registration OpenCheck resolved (Phase 151 — previously written
    # as the literal "unknown" when no date was given, which the schema
    # forbids; now the date is set only when ABR gives one).
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["abr_australia"],
        liveness=_liveness.classify(abn_status, live=("active",), terminal=("cancelled",)),
        raw=(bundle.get("abn_status") or "").strip() or None,
        since=bundle.get("abn_status_from") if abn_status and abn_status != "active" else None,
    )

    yield stmt


# ---------------------------------------------------------------------------
# India — Ministry of Corporate Affairs Company Master Data (data.gov.in, GODL)
# ---------------------------------------------------------------------------
#
# The MCA master data is entity-level firmographic data only — no officers or
# beneficial owners — so this mapper produces a single entity statement.

# CompanyStatus values that mean the company has ceased to exist on the
# register. Deliberately conservative: in-progress states ("Under Process of
# Striking Off", "Under Liquidation") are the ``pending`` class and
# inactive-but-registered states ("Dormant") are still ``live`` — the company
# exists.
_MCA_TERMINAL_STATUSES = frozenset({
    "strike off",
    "struck off",
    "dissolved",
    "amalgamated",
    "liquidated",
    "converted to llp",
    "converted to llp and dissolved",
})
_MCA_PENDING_STATUSES = frozenset({
    "under process of striking off",
    "under liquidation",
    "to be struck off",
})
_MCA_LIVE_STATUSES = frozenset({"active", "dormant", "active in progress", "dormant under section 455"})


def map_mca_india(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield a BODS v0.4 entity statement for an Indian MCA company.

    Identifier: ``IN-MCA`` (the 21-character CIN). The registered office
    address becomes a registered address; the registration date becomes
    ``foundingDate``; terminal register statuses set ``dissolutionDate``
    ("unknown" — the master data carries no event date).
    """
    if not bundle or bundle.get("is_stub"):
        return

    cin = (bundle.get("cin") or "").strip().upper()
    name = (bundle.get("name") or "").strip()
    if not cin or not name:
        return

    source_url = bundle.get("link") or (
        "https://www.data.gov.in/resource/registrars-companies-roc-wise-company-master-data"
    )

    identifiers = [{
        "id": cin,
        "scheme": "IN-MCA",
        "schemeName": "Corporate Identification Number (CIN) — Ministry of Corporate Affairs",
    }]

    addresses: list[dict[str, Any]] = []
    address = (bundle.get("address") or "").strip()
    if address:
        addresses.append(_addr("registered", address, "IN"))

    stmt = make_entity_statement(
        source_id="mca_india",
        local_id=cin,
        name=name,
        jurisdiction=("India", "IN"),
        identifiers=identifiers,
        founding_date=(bundle.get("registration_date") or None),
        addresses=addresses,
        source_url=source_url,
    )

    record_details = stmt.get("recordDetails") or {}

    # Entity type subtype from the register's class ("Public" / "Private" /
    # "One Person Company"); category + sub-category as details.
    company_class = (bundle.get("company_class") or "").strip()
    detail_bits = [
        b for b in (
            (bundle.get("category") or "").strip(),
            (bundle.get("sub_category") or "").strip(),
        ) if b
    ]
    if company_class or detail_bits:
        entity_type: dict[str, Any] = {"type": "registeredEntity"}
        if company_class:
            entity_type["subtype"] = company_class
        if detail_bits:
            entity_type["details"] = " — ".join(detail_bits)
        record_details["entityType"] = entity_type

    stmt["recordDetails"] = record_details

    # Register status → liveness annotation (Phase 151). MCA publishes no
    # date, so ``dissolutionDate`` is never set — it used to be the literal
    # "unknown", which the schema forbids.
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["mca_india"],
        liveness=_liveness.classify(
            bundle.get("status"),
            live=_MCA_LIVE_STATUSES,
            pending=_MCA_PENDING_STATUSES,
            terminal=_MCA_TERMINAL_STATUSES,
        ),
        raw=(bundle.get("status") or "").strip() or None,
    )

    yield stmt


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
    """CAC PSC rows for the NEITI solid-minerals subset — reuses the CAC
    interest mapping, with the (dated) NEITI filter evidence annotated on the
    subject so the extractives scoping is auditable."""
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

    groups: dict[str, dict[str, Any]] = {}
    for psc in cac_record.get("pscs") or []:
        owner = (psc.get("owner_name") or "").strip()
        if not owner:
            continue
        kind = psc.get("owner_kind") or "entity"
        g = groups.setdefault(owner, {"kind": kind, "psc": psc, "ilists": []})
        g["ilists"].append(_cac_interests(
            psc,
            record_kind=(
                "psc_natural_person" if kind == "person" else "psc_corporate"
            ),
        ))

    emitted: set[str] = set()
    for owner, g in groups.items():
        kind = g["kind"]
        psc = g["psc"]
        owner_rc = psc.get("owner_rc") or None
        juris = psc.get("owner_jurisdiction") or None

        if kind == "person":
            local_id = f"ng:person:{owner}"
            nationalities = []
            nat = psc.get("nationality") or ""
            co = _country_obj(nat) if nat else None
            if co:
                nationalities = [co]
            if local_id not in emitted:
                yield make_person_statement(
                    source_id="eiti_bo",
                    local_id=local_id,
                    full_name=owner,
                    nationalities=nationalities,
                    source_url=source_url,
                    statement_date=statement_date,
                )
                emitted.add(local_id)
            ip_id = _stable_id("eiti_bo", "person", local_id)
            ip_type = "person"
        else:
            entity_type = {
                "arrangement": "arrangement",
                "unknown": "unknownEntity",
            }.get(kind, "registeredEntity")
            local_id = f"ng:entity:{owner_rc or owner}"
            idents = []
            if owner_rc:
                idents = [{
                    "id": str(owner_rc),
                    "scheme": "NG-CAC",
                    "schemeName": "Nigeria Corporate Affairs Commission",
                }]
            if local_id not in emitted:
                yield make_entity_statement(
                    source_id="eiti_bo",
                    local_id=local_id,
                    name=owner,
                    jurisdiction=("Nigeria", "NG") if juris == "NG" else None,
                    identifiers=idents,
                    entity_type=entity_type,
                    source_url=source_url,
                    statement_date=statement_date,
                )
                emitted.add(local_id)
            ip_id = _stable_id("eiti_bo", "entity", local_id)
            ip_type = "entity"

        yield make_relationship_statement(
            source_id="eiti_bo",
            local_id=f"ng:{rc}:{owner}",
            subject_statement_id=subject_id,
            interested_party_statement_id=ip_id,
            interested_party_type=ip_type,
            interests=_cac_merge_interests(g["ilists"]),
            source_url=source_url,
            statement_date=statement_date,
        )
