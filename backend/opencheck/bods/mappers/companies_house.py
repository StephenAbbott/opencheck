"""UK Companies House (profile, officers, PSCs) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from ...identifiers import ch_identification_is_uk, normalise_ch_company_number
from .. import identity_verification as _idv, liveness as _liveness
from ..annotations import annotate, identifying, pointer, transformation
from ..ch_constants import describe_company_type, describe_officer_role
from ..psc_natures import describe_statement, describe_super_secure
from ..statements import (
    BODSBundle,
    SOURCE_NAMES,
    _addr,
    _birth_date_precision_note,
    _country_code,
    _parse_nature,
    _today,
    is_majority_stake,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
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

    # One ``_ChCompany`` per company mapped, keyed on the normalised number the
    # adapter keys ``related_companies`` on, so the chain roll-up below can
    # follow a corporate PSC to the company it names.
    companies: dict[str, _ChCompany] = {}
    root = _emit_company_statements(bundle, result, seen_sids)
    companies[root.number] = root

    for sub_bundle in (bundle.get("related_companies") or {}).values():
        sub = _emit_company_statements(sub_bundle, result, seen_sids)
        companies.setdefault(sub.number, sub)

    _rollup_ch_chains(root, companies, result, seen_sids)
    _annotate_verified_people(result)

    return result


def _annotate_verified_people(result: BODSBundle) -> None:
    """Phase 203: annotate each person whose identity Companies House verified.

    Runs after every company in the bundle is mapped, and reads the
    per-role annotations the relationships already carry — so a person is
    marked verified from exactly the roles the output shows, and a director
    whose verification statement is in place on a parent's board but not yet on
    the subject's is still marked, although the subject's copy of the person
    statement was written first (Phase 193's first-writer-wins).
    """
    roles: dict[str, list[_idv.Verification]] = {}
    for stmt in result.statements:
        if stmt.get("recordType") != "relationship":
            continue
        party = (stmt.get("recordDetails") or {}).get("interestedParty")
        if not isinstance(party, str):
            continue
        for annotation in stmt.get("annotations") or []:
            verification = _idv.role_verification_from_annotation(annotation)
            if verification:
                roles.setdefault(party, []).append(verification)
    if not roles:
        return
    for stmt in result.statements:
        if stmt.get("recordType") != "person":
            continue
        found = roles.get(stmt.get("statementId"))
        if not found:
            continue
        annotate(
            stmt,
            _idv.person_annotation(
                pointer("recordDetails"),
                found,
                url=(stmt.get("source") or {}).get("url"),
            ),
        )


@dataclass
class _ChPscHop:
    """One PSC relationship a company bundle produced, as the roll-up sees it."""

    ip_sid: str
    ip_type: str  # "entity" | "person"
    uk_number: str | None  # the related company a corporate PSC names, if UK
    natures: list[str]
    ceased: bool
    relationship: dict[str, Any]


@dataclass
class _ChCompany:
    number: str
    entity: dict[str, Any]
    url: str
    hops: list[_ChPscHop] = field(default_factory=list)


def _rollup_ch_chains(
    root: _ChCompany,
    companies: dict[str, _ChCompany],
    result: BODSBundle,
    seen_sids: set[str],
) -> None:
    """Fix 2 (Phase 183): the primary-plus-components structure for UK chains.

    Where the subject's corporate PSC is itself a UK company whose PSCs the
    adapter fetched (``related_companies``), and an *individual* is reached
    through that chain, the BODS modelling guidance (*Representing beneficial
    ownership*) wants one **primary** relationship from the person to the
    subject — ``directOrIndirect: "indirect"``, ``beneficialOwnershipOrControl:
    true``, ``isComponent: false`` — whose ``componentRecords`` list the
    ``recordId`` of every intermediary entity and every hop relationship, all
    of which are marked ``isComponent: true`` and published before it.

    This is only assembled where the register's own regime says the person is
    a PSC of the subject: under the Companies Act 2006, Sch 1A paras 18–19, an
    interest is held indirectly only through a chain of entities in each of
    which the holder has a **majority stake** (``is_majority_stake``). The
    first hop — the RLE's own interest in the subject — can be any PSC nature;
    it is *that* interest the person holds indirectly, so the primary carries
    the first hop's interest types. A chain with a sub-majority upper hop
    (a person holding 30 % of the holding company) yields no primary: the hops
    stay as the register filed them, ``isComponent`` untouched. Ceased hops,
    super-secure PSCs and corporate PSCs registered outside the UK (or beyond
    the adapter's depth) end a chain without a primary.

    The primary is OpenCheck's assembly, not a Companies House filing:
    ``source.type`` is ``thirdParty``, ``source.assertedBy`` names OpenCheck,
    and a ``transformation`` annotation says which component records it was
    derived from. The hops keep their register-sourced flags.
    """
    root_sid = root.entity["statementId"]
    primaries: dict[str, dict[str, Any]] = {}  # person sid -> primary statement

    def walk(
        company: _ChCompany,
        depth: int,
        visited: frozenset[str],
        first_hop: _ChPscHop | None,
        entities: tuple[dict[str, Any], ...],
        rels: tuple[dict[str, Any], ...],
    ) -> None:
        for hop in company.hops:
            if hop.ceased:
                continue
            if depth > 0 and not is_majority_stake(hop.natures):
                continue
            head = first_hop or hop
            if hop.ip_type == "person":
                if depth == 0:
                    continue  # a direct PSC of the subject: nothing to roll up
                _add_primary(head, hop, entities, rels + (hop.relationship,))
                continue
            if hop.ip_type != "entity" or not hop.uk_number:
                continue
            nxt = companies.get(hop.uk_number)
            if nxt is None or nxt.number in visited or nxt.number == root.number:
                continue
            walk(
                nxt,
                depth + 1,
                visited | {nxt.number},
                head,
                entities + (nxt.entity,),
                rels + (hop.relationship,),
            )

    def _add_primary(
        first_hop: _ChPscHop,
        person_hop: _ChPscHop,
        entities: tuple[dict[str, Any], ...],
        rels: tuple[dict[str, Any], ...],
    ) -> None:
        for stmt in entities + rels:
            stmt["recordDetails"]["isComponent"] = True
        components = [s["recordId"] for s in entities] + [r["recordId"] for r in rels]
        person_sid = person_hop.ip_sid
        existing = primaries.get(person_sid)
        if existing is not None:
            # A second chain to the same person: one primary, the union of
            # the component records, in first-seen order.
            have = existing["recordDetails"]["componentRecords"]
            have.extend(c for c in components if c not in have)
            return

        first_interests = first_hop.relationship["recordDetails"].get("interests") or []
        interests: list[dict[str, Any]] = []
        seen_types: set[str] = set()
        for interest in first_interests:
            itype = interest.get("type") or "unknownInterest"
            if itype in seen_types:
                continue
            seen_types.add(itype)
            interests.append(
                {
                    "type": itype,
                    "directOrIndirect": "indirect",
                    "beneficialOwnershipOrControl": True,
                    # Names one chain; a second chain to the same person only
                    # extends componentRecords, which is the complete list.
                    "details": (
                        "Held indirectly along the chain "
                        + " → ".join(e["recordDetails"].get("name", "?") for e in entities)
                        + "; every intermediary entity and hop is listed in "
                        "componentRecords. The share band is the intermediary's "
                        "and is not restated here."
                    ),
                }
            )
        if not interests:
            interests = [
                {
                    "type": "unknownInterest",
                    "directOrIndirect": "indirect",
                    "beneficialOwnershipOrControl": True,
                }
            ]

        dates = [r.get("statementDate") for r in rels if r.get("statementDate")]
        primary = make_relationship_statement(
            source_id="companies_house",
            local_id=f"{root.number}:{person_sid}:indirect",
            subject_statement_id=root_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=interests,
            source_url=root.url,
            statement_date=max(dates) if dates else None,
            component_records=components,
        )
        # OpenCheck's assembly, not a Companies House filing.
        primary["source"]["type"] = ["thirdParty"]
        primary["source"]["assertedBy"] = [
            {"name": "OpenCheck", "uri": "https://opencheck.world"}
        ]
        annotate(
            primary,
            transformation(
                pointer("recordDetails", "componentRecords"),
                (
                    "Assembled by OpenCheck from the Companies House PSC filings "
                    "listed in componentRecords: the person is a registered PSC "
                    "of the last intermediary, and holds — or controls a trust "
                    "or firm that holds — a majority stake (Companies Act 2006, "
                    "Sch 1A para 18) in every entity above the subject, so under "
                    "the PSC regime the intermediary's interest in the subject "
                    "is held indirectly by the person. No single filing states "
                    "this relationship."
                ),
                creation_date=_today(),
            ),
        )
        primaries[person_sid] = primary

    walk(root, 0, frozenset({root.number}), None, (), ())

    for primary in primaries.values():
        if primary["statementId"] not in seen_sids:
            result.statements.append(primary)
            seen_sids.add(primary["statementId"])


def _ch_officer_id(officer: dict[str, Any]) -> str | None:
    """Companies House's own officer id, from ``links.officer.appointments``.

    The path has the form ``/officers/{id}/appointments``, and that id is the
    register's **own grouping of one person's appointments** — its view of who
    is the same human, based on the name, date of birth and address it holds.
    It is the only source-provided key a natural person has here, and it is
    not an identity guarantee: Companies House itself sometimes holds two ids
    for one person. Everything downstream treats it as the register's grouping
    rather than as an assertion of identity.
    """
    links_path: str = (
        (officer.get("links") or {})
        .get("officer", {})
        .get("appointments", "")
    )
    parts = [p for p in links_path.split("/") if p]
    if "officers" in parts:
        idx = parts.index("officers")
        if idx + 1 < len(parts):
            return parts[idx + 1] or None
    return None


def _ch_officer_local_id(company_number: str, officer: dict[str, Any]) -> str:
    """A stable local_id for one officer *appointment* — a person at a company.

    This keys the relationship statement, which is company-scoped by nature:
    the same director on two boards holds two appointments. The person's own
    key is :func:`_ch_officer_person_local_id`, which is not company-scoped.

    Falls back to a SHA-256 digest of ``{company_number}|{name}|{appointed_on}``
    when the links block is absent, so ids stay stable either way.
    """
    officer_id = _ch_officer_id(officer)
    if officer_id:
        return f"{company_number}:director:{officer_id}"
    # Fallback: hash of stable fields
    name = officer.get("name") or ""
    appointed_on = officer.get("appointed_on") or ""
    digest = hashlib.sha256(
        f"{company_number}|{name}|{appointed_on}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{company_number}:director:{digest}"


def _ch_officer_person_local_id(company_number: str, officer: dict[str, Any]) -> str:
    """The key for the *person*: the register's officer id, with no company in it.

    Phase 193. Before this, a person was keyed on ``{company}:director:{id}``,
    so one director sitting on a subject's board and its parent's board became
    two person statements — and the FullCheck canvas drew two nodes for one
    human. Measured on Lloyds Bank PLC and Lloyds Banking Group plc: 13
    directors serve both boards, and Companies House gives 12 of the 13 the
    same officer id. Keying the person on that id alone collapses them without
    any name matching.

    It is deliberately the same key ``_map_companies_house_officer`` already
    uses (``officer:{id}``), so a person reached through a company lookup and
    the same person reached through an officer lookup are one statement.

    Without a links block there is nothing to group on, so the fallback stays
    company-scoped: two records of the same human stay two people rather than
    being merged on a name, which is the merge ``reconcile.ts`` refuses to
    make for the same reason.
    """
    officer_id = _ch_officer_id(officer)
    if officer_id:
        return f"officer:{officer_id}"
    return _ch_officer_local_id(company_number, officer)


def _ch_officer_grouping_note(officer_id: str | None) -> dict[str, Any] | None:
    """Publish the officer id the person was grouped on, as an annotation.

    Not as ``recordDetails.identifiers``: BODS reserves that array for
    identity documents and lib-cove-bods enforces it — a person identifier's
    scheme must read ``<ISO 3166-1 alpha-3>-{PASSPORT|TAXID|IDCARD}``. A
    register's internal key for a person is none of those, so putting it there
    produces a conformant-looking file that fails the standard's own checks.
    (``GB-COH-OFFICER``, which the officer-appointments path published until
    Phase 193, failed three of them; nothing validated that path, so it went
    unseen.)

    The ``identifying`` motivation is the construct that fits: it states what
    the grouping was made on, in words that stop short of asserting identity —
    which is exactly as far as the register itself goes. The id is also in the
    statement's ``source.url``, so a consumer can regroup or reject it.
    """
    if not officer_id:
        return None
    return identifying(
        pointer("recordDetails"),
        (
            f"Grouped on Companies House officer id {officer_id}: the register's "
            "own grouping of one person's appointments, published so this "
            "statement can be regrouped or rejected. Companies House derives it "
            "from the name, date of birth and address it holds, and it is not "
            "an assertion of identity — the register sometimes holds two ids "
            "for one person."
        ),
    )


def _ch_officer_url(officer_id: str | None, company_url: str) -> str:
    """Where a reader verifies the person: their own appointments page when the
    register gives one, the company otherwise."""
    if not officer_id:
        return company_url
    return (
        "https://find-and-update.company-information.service.gov.uk/officers/"
        f"{officer_id}/appointments"
    )


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

    * A ``personStatement`` (``knownPerson``) with name, DOB, nationality,
      service address and — where the register gives one — its own officer id
      as a published identifier. The person is keyed on that id alone, so one
      director on several boards is one statement with several relationships
      (see :func:`_ch_officer_person_local_id`).
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
        officer_id = _ch_officer_id(officer)
        person = make_person_statement(
            source_id="companies_house",
            local_id=_ch_officer_person_local_id(company_number, officer),
            full_name=name,
            person_type="knownPerson",
            nationalities=nationalities,
            birth_date=birth_date,
            addresses=addresses,
            source_url=_ch_officer_url(officer_id, company_url),
        )
        annotate(
            person,
            _birth_date_precision_note(birth_date),
            _ch_officer_grouping_note(officer_id),
        )
        person_sid = person["statementId"]
        # First writer wins, and the subject is always mapped first
        # (``map_companies_house`` emits the root bundle before any related
        # company), so where two boards file different details for one person
        # — a different service address, a nationality spelt differently — the
        # subject's own filing is the one published. A parent's copy fills in
        # only for a director the subject does not name.
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
        # Phase 203: the register's "Verified" label is per role, so the
        # verification statement is recorded on the appointment. The person is
        # annotated from these in ``_annotate_verified_people``.
        verification = _idv.read_verification(officer)
        if verification:
            annotate(
                rel,
                _idv.role_annotation(
                    pointer("recordDetails", "interestedParty"),
                    verification,
                    url=_ch_officer_url(officer_id, company_url),
                ),
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
) -> _ChCompany:
    """Emit entity + PSC + director statements for one company bundle into *result*.

    *seen_sids* is updated in place; statements whose ``statementId`` is
    already present are silently skipped so the same entity/relationship is
    never duplicated across the root + related-company passes. Returns the
    company and its PSC hops for ``_rollup_ch_chains``.
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
    else:
        # The related-company pass re-emits an entity the PSC pass already
        # produced; the roll-up must flag the statement that is in the bundle.
        entity = next(s for s in result.statements if s["statementId"] == entity_sid)
    company = _ChCompany(number=number, entity=entity, url=company_url)

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
            # pass emits for the same company (both use local_id = the
            # canonical eight-character number). Phase 177: the filed number
            # is normalised first — PSC filings drop leading zeros
            # (``2999029``), and the adapter now keys ``related_companies`` on
            # the normalised form, so the two sides must agree on it. The same
            # predicate and normaliser the adapter uses; a second copy of the
            # gate is how the old one survived.
            ident = psc.get("identification") or {}
            uk_number = (
                normalise_ch_company_number(ident.get("registration_number"))
                if ch_identification_is_uk(ident)
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

        # Phase 203: an individual PSC's identity verification statement, on
        # the notification it was supplied for. Corporate PSCs (relevant legal
        # entities) have no verification requirement yet and carry no block; a
        # ceased notification is no longer a role, so it earns no mark.
        if ip_type == "person" and "individual" in psc_kind and not ceased_on:
            verification = _idv.read_verification(psc)
            if verification:
                annotate(
                    rel,
                    _idv.role_annotation(
                        pointer("recordDetails", "interestedParty"),
                        verification,
                        url=f"{company_url}/persons-with-significant-control",
                    ),
                )

        rel_sid = rel["statementId"]
        if rel_sid not in seen_sids:
            result.statements.append(rel)
            seen_sids.add(rel_sid)
        else:
            rel = next(s for s in result.statements if s["statementId"] == rel_sid)
        company.hops.append(
            _ChPscHop(
                ip_sid=ip_sid,
                ip_type=ip_type,
                uk_number=uk_number if ip_type == "entity" else None,
                natures=list(natures),
                ceased=bool(ceased_on),
                relationship=rel,
            )
        )

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
    return company


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
        source_url=person_url,
    )
    annotate(
        person,
        _birth_date_precision_note(birth_date),
        _ch_officer_grouping_note(officer_id),
    )
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
        if not appointment.get("resigned_on"):
            verification = _idv.read_verification(appointment)
            if verification:
                annotate(
                    rel,
                    _idv.role_annotation(
                        pointer("recordDetails", "interestedParty"),
                        verification,
                        url=person_url,
                    ),
                )
        result.statements.append(rel)

    _annotate_verified_people(result)
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
    # Publish the canonical Companies House number (``02999029``) rather than
    # the filed spelling (``2999029``): GB-COH is defined as the eight-character
    # form, GLEIF's ``registeredAs`` carries it that way, and the reconciler
    # merges on it — a dropped leading zero would leave the same company as
    # two nodes. The filed text is still on the raw PSC record.
    reg_number = uk_number or identification.get("registration_number")
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
