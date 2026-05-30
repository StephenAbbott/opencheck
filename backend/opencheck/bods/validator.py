"""Lightweight BODS v0.4 shape validator.

This is a sanity check, not a full conformance test. Phase 2 will wire in
``libcovebods`` for the authoritative validator; in Phase 1 we just want to
catch obvious bugs (missing recordId, wrong recordType, malformed
recordDetails) before statements hit the UI or an export adapter.
"""

from __future__ import annotations

from typing import Any, Iterable


class ValidationError(Exception):
    """Raised when a BODS statement fails shape validation."""


_VALID_RECORD_TYPES = {"entity", "person", "relationship"}
_VALID_RECORD_STATUSES = {"new", "updated", "closed"}
_VALID_ENTITY_TYPES = {
    "registeredEntity",
    "legalEntity",
    "arrangement",
    "anonymousEntity",
    "unknownEntity",
    "state",
    "stateBody",
}
_VALID_PERSON_TYPES = {"knownPerson", "anonymousPerson", "unknownPerson"}
# Complete BODS v0.4 interestType codelist.
# Source: https://raw.githubusercontent.com/openownership/data-standard/main/schema/codelists/interestType.csv
_VALID_INTEREST_TYPES = {
    "shareholding",
    "votingRights",
    "appointmentOfBoard",
    "otherInfluenceOrControl",
    "seniorManagingOfficial",
    "settlor",
    "trustee",
    "protector",
    "beneficiaryOfLegalArrangement",
    "rightsToSurplusAssetsOnDissolution",
    "rightsToProfitOrIncome",
    "rightsGrantedByContract",
    "conditionalRightsGrantedByContract",
    "controlViaCompanyRulesOrArticles",
    "controlByLegalFramework",
    "boardMember",
    "boardChair",
    "unknownInterest",
    "unpublishedInterest",
    "enjoymentAndUseOfAssets",
    "rightToProfitOrIncomeFromAssets",
    "nominee",
    "nominator",
}


def _is_unspecified_record(party: Any) -> bool:
    """True if a relationship ``subject``/``interestedParty`` is an unspecified
    record rather than a statement reference.

    An unspecified record names a *reason* the party cannot be identified
    instead of pointing at an entity/person statement.  We accept both the
    BODS v0.4 ``reason`` key and the v0.3 ``unspecifiedReason`` key, since
    externally sourced datasets (notably GLEIF Level 2 Reporting Exceptions)
    are published with the v0.3 idiom under a v0.4 version stamp.
    """
    return isinstance(party, dict) and (
        "reason" in party or "unspecifiedReason" in party
    )


def validate_shape(statements: Iterable[dict[str, Any]]) -> list[str]:
    """Return a list of human-readable issues. Empty list means OK.

    Checks:
    * Required top-level fields present
    * recordType in the v0.4 enum
    * recordDetails shape matches recordType
    * Relationship statements reference existing statement IDs
    * Interest type codes are in the v0.4 codelist

    Relationship cross-reference resolution
    ---------------------------------------
    BODS v0.4 specifies that ``subject`` and ``interestedParty`` are bare
    strings (statementId references).  OpenCheck sets statementId == recordId
    so either value works for our own output.  The canonical bods-fixtures pack
    uses *recordId* as the reference key, so we accept both statementId and
    recordId to avoid false positives on valid external datasets.
    """
    statements = list(statements)
    # Accept both statementId and recordId as valid reference targets.
    known_ids: set[str | None] = set()
    for s in statements:
        if s.get("statementId"):
            known_ids.add(s["statementId"])
        if s.get("recordId"):
            known_ids.add(s["recordId"])
    issues: list[str] = []

    for i, s in enumerate(statements):
        prefix = f"statement #{i} ({s.get('statementId', '?')})"

        for required in ("statementId", "recordId", "recordType", "recordStatus", "recordDetails"):
            if required not in s:
                issues.append(f"{prefix}: missing {required}")

        rt = s.get("recordType")
        if rt not in _VALID_RECORD_TYPES:
            issues.append(f"{prefix}: recordType {rt!r} not in {_VALID_RECORD_TYPES}")
            continue

        if s.get("recordStatus") not in _VALID_RECORD_STATUSES:
            issues.append(
                f"{prefix}: recordStatus {s.get('recordStatus')!r} not in {_VALID_RECORD_STATUSES}"
            )

        rd = s.get("recordDetails") or {}

        if rt == "entity":
            et = (rd.get("entityType") or {}).get("type")
            if et not in _VALID_ENTITY_TYPES:
                issues.append(f"{prefix}: entityType.type {et!r} not in {_VALID_ENTITY_TYPES}")
            if not rd.get("name"):
                issues.append(f"{prefix}: entity missing name")

        elif rt == "person":
            pt = rd.get("personType")
            if pt not in _VALID_PERSON_TYPES:
                issues.append(f"{prefix}: personType {pt!r} not in {_VALID_PERSON_TYPES}")
            names = rd.get("names") or []
            if not names or not names[0].get("fullName"):
                issues.append(f"{prefix}: person missing names[0].fullName")

        elif rt == "relationship":
            # BODS v0.4: subject/interestedParty are bare strings.
            # Legacy wrapped format: {"describedByEntityStatement": "id"}.
            #
            # Either side may also be an *unspecified record* rather than a
            # reference, when the party cannot or need not be identified — e.g.
            # GLEIF Level 2 Reporting Exceptions (NO_LEI, NON_CONSOLIDATING,
            # NON_PUBLIC) or a "no beneficial owners" declaration.  These carry
            # an ``unspecifiedReason`` (BODS v0.3 idiom) or ``reason`` (v0.4
            # ``UnspecifiedRecord``) key and have no statement to reference, so
            # they must be skipped rather than treated as dangling references.
            raw_subj = rd.get("subject") or {}
            if not _is_unspecified_record(raw_subj):
                subject_sid = (
                    raw_subj if isinstance(raw_subj, str)
                    else raw_subj.get("describedByEntityStatement")
                )
                if subject_sid not in known_ids:
                    issues.append(f"{prefix}: subject references unknown statement {subject_sid!r}")

            raw_ip = rd.get("interestedParty") or {}
            if not _is_unspecified_record(raw_ip):
                ip_sid = (
                    raw_ip if isinstance(raw_ip, str)
                    else (raw_ip.get("describedByPersonStatement") or raw_ip.get("describedByEntityStatement"))
                )
                if ip_sid not in known_ids:
                    issues.append(f"{prefix}: interestedParty references unknown statement {ip_sid!r}")

            interests = rd.get("interests") or []
            for j, interest in enumerate(interests):
                itype = interest.get("type")
                if itype not in _VALID_INTEREST_TYPES:
                    issues.append(
                        f"{prefix}: interests[{j}].type {itype!r} not in codelist"
                    )

    return issues


def assert_valid(statements: Iterable[dict[str, Any]]) -> None:
    """Raise ``ValidationError`` if ``validate_shape`` finds any issues."""
    issues = validate_shape(statements)
    if issues:
        raise ValidationError("; ".join(issues))
