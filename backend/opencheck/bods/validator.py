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


def validate_shape(statements: Iterable[dict[str, Any]]) -> list[str]:
    """Return a list of human-readable issues. Empty list means OK.

    Checks:
    * Required top-level fields present
    * recordType in the v0.4 enum
    * recordDetails shape matches recordType
    * Relationship statements reference existing statement IDs
    * Interest type codes are in the v0.4 codelist
    """
    statements = list(statements)
    known_ids = {s.get("statementId") for s in statements if s.get("statementId")}
    known_record_ids = {s.get("recordId") for s in statements if s.get("recordId")}
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
            # BODS v0.4: subject and interestedParty are plain recordId strings,
            # not v0.3-style {"describedByEntityStatement": ...} wrapper objects.
            subject_rid = rd.get("subject")
            if not isinstance(subject_rid, str) or subject_rid not in known_record_ids:
                issues.append(f"{prefix}: subject references unknown recordId {subject_rid!r}")

            ip_rid = rd.get("interestedParty")
            if isinstance(ip_rid, dict):
                # Unspecified-party object (e.g. {"unspecified": {"reason": ...}}) — valid
                pass
            elif not isinstance(ip_rid, str) or ip_rid not in known_record_ids:
                issues.append(f"{prefix}: interestedParty references unknown recordId {ip_rid!r}")

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
