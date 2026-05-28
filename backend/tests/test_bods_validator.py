"""Phase 6 — Unit tests for opencheck.bods.validator.validate_shape.

Every error branch in validate_shape() has at least one test.  Positive
("no issues") cases are tested for each statement type as well as for the
three inter-statement reference patterns the validator now supports:
  * statementId references (OpenCheck default: statementId == recordId)
  * recordId references (canonical bods-fixtures pack)
  * Inline unidentified-BO interestedParty dict (anonymous-person pattern)

Tests are grouped into classes matching the validator's internal structure.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck.bods.validator import ValidationError, assert_valid, validate_shape

# ---------------------------------------------------------------------------
# Helpers — minimal valid statement skeletons
# ---------------------------------------------------------------------------

_ENTITY_ID = "entity-001"
_PERSON_ID = "person-001"
_REL_ID = "rel-001"


def _entity(
    *,
    statement_id: str = _ENTITY_ID,
    record_id: str | None = None,
    record_status: str = "new",
    entity_type: str = "registeredEntity",
    name: str = "Acme Ltd",
    **extras: Any,
) -> dict[str, Any]:
    s: dict[str, Any] = {
        "statementId": statement_id,
        "recordId": record_id or statement_id,
        "recordType": "entity",
        "recordStatus": record_status,
        "recordDetails": {
            "entityType": {"type": entity_type},
            "name": name,
        },
    }
    s.update(extras)
    return s


def _person(
    *,
    statement_id: str = _PERSON_ID,
    record_id: str | None = None,
    record_status: str = "new",
    person_type: str = "knownPerson",
    full_name: str = "Jane Smith",
    **extras: Any,
) -> dict[str, Any]:
    s: dict[str, Any] = {
        "statementId": statement_id,
        "recordId": record_id or statement_id,
        "recordType": "person",
        "recordStatus": record_status,
        "recordDetails": {
            "personType": person_type,
            "names": [{"type": "individual", "fullName": full_name}],
        },
    }
    s.update(extras)
    return s


def _relationship(
    *,
    statement_id: str = _REL_ID,
    record_id: str | None = None,
    record_status: str = "new",
    subject: Any = _ENTITY_ID,
    interested_party: Any = _PERSON_ID,
    interests: list[dict[str, Any]] | None = None,
    **extras: Any,
) -> dict[str, Any]:
    if interests is None:
        interests = [
            {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
            }
        ]
    s: dict[str, Any] = {
        "statementId": statement_id,
        "recordId": record_id or statement_id,
        "recordType": "relationship",
        "recordStatus": record_status,
        "recordDetails": {
            "subject": subject,
            "interestedParty": interested_party,
            "interests": interests,
        },
    }
    s.update(extras)
    return s


# ---------------------------------------------------------------------------
# Clean bundles — must produce zero issues
# ---------------------------------------------------------------------------


class TestCleanBundles:
    """Positive cases — validate_shape() must return an empty list."""

    def test_single_entity(self):
        assert validate_shape([_entity()]) == []

    def test_single_person(self):
        assert validate_shape([_person()]) == []

    def test_entity_person_relationship(self):
        bundle = [_entity(), _person(), _relationship()]
        assert validate_shape(bundle) == []

    def test_updated_status(self):
        assert validate_shape([_entity(record_status="updated")]) == []

    def test_closed_status(self):
        assert validate_shape([_entity(record_status="closed")]) == []

    def test_empty_bundle(self):
        assert validate_shape([]) == []

    def test_anonymous_entity_type(self):
        assert validate_shape([_entity(entity_type="anonymousEntity", name="Unknown")]) == []

    def test_anonymous_person_type(self):
        assert validate_shape([_person(person_type="anonymousPerson")]) == []

    def test_unknown_person_type(self):
        assert validate_shape([_person(person_type="unknownPerson")]) == []

    def test_all_interest_types_valid(self):
        """Every codelist value should produce no issues."""
        from opencheck.bods.validator import _VALID_INTEREST_TYPES

        for itype in sorted(_VALID_INTEREST_TYPES):
            bundle = [
                _entity(),
                _person(),
                _relationship(interests=[{"type": itype}]),
            ]
            issues = validate_shape(bundle)
            assert issues == [], f"Unexpected issue for interest type {itype!r}: {issues}"


# ---------------------------------------------------------------------------
# Missing required top-level fields
# ---------------------------------------------------------------------------


class TestMissingRequiredFields:
    """Each required field must be flagged individually when absent."""

    def _drop(self, statement: dict[str, Any], field: str) -> dict[str, Any]:
        s = dict(statement)
        s.pop(field)
        return s

    def test_missing_statement_id(self):
        issues = validate_shape([self._drop(_entity(), "statementId")])
        assert any("missing statementId" in i for i in issues)

    def test_missing_record_id(self):
        issues = validate_shape([self._drop(_entity(), "recordId")])
        assert any("missing recordId" in i for i in issues)

    def test_missing_record_type(self):
        issues = validate_shape([self._drop(_entity(), "recordType")])
        assert any("missing recordType" in i for i in issues)

    def test_missing_record_status(self):
        issues = validate_shape([self._drop(_entity(), "recordStatus")])
        assert any("missing recordStatus" in i for i in issues)

    def test_missing_record_details(self):
        issues = validate_shape([self._drop(_entity(), "recordDetails")])
        assert any("missing recordDetails" in i for i in issues)

    def test_multiple_missing_fields(self):
        """All missing fields should appear — not short-circuited after the first."""
        s = {
            "statementId": "x",
            "recordType": "entity",
            "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "X"},
        }
        issues = validate_shape([s])
        assert any("missing recordId" in i for i in issues)
        assert any("missing recordStatus" in i for i in issues)


# ---------------------------------------------------------------------------
# recordType validation
# ---------------------------------------------------------------------------


class TestRecordType:
    def test_invalid_record_type_halts_deeper_checks(self):
        """An invalid recordType triggers an issue and skips type-specific checks
        to avoid spurious follow-on errors."""
        s = {
            "statementId": "x",
            "recordId": "x",
            "recordType": "bogus",
            "recordStatus": "new",
            "recordDetails": {},
        }
        issues = validate_shape([s])
        assert any("recordType 'bogus' not in" in i for i in issues)

    def test_none_record_type(self):
        s = {
            "statementId": "x",
            "recordId": "x",
            "recordType": None,
            "recordStatus": "new",
            "recordDetails": {},
        }
        issues = validate_shape([s])
        assert any("recordType" in i for i in issues)


# ---------------------------------------------------------------------------
# recordStatus validation
# ---------------------------------------------------------------------------


class TestRecordStatus:
    def test_invalid_record_status(self):
        issues = validate_shape([_entity(record_status="deleted")])
        assert any("recordStatus 'deleted' not in" in i for i in issues)

    def test_none_record_status(self):
        issues = validate_shape([_entity(record_status=None)])
        assert any("recordStatus" in i for i in issues)


# ---------------------------------------------------------------------------
# Entity-specific validation
# ---------------------------------------------------------------------------


class TestEntityValidation:
    def test_invalid_entity_type(self):
        issues = validate_shape([_entity(entity_type="corporation")])
        assert any("entityType.type 'corporation' not in" in i for i in issues)

    def test_none_entity_type(self):
        issues = validate_shape([_entity(entity_type=None)])
        assert any("entityType.type" in i for i in issues)

    def test_missing_name(self):
        issues = validate_shape([_entity(name="")])
        assert any("entity missing name" in i for i in issues)

    def test_none_name(self):
        s = _entity()
        s["recordDetails"]["name"] = None
        issues = validate_shape([s])
        assert any("entity missing name" in i for i in issues)

    def test_missing_entity_type_block(self):
        """Completely absent entityType dict should produce an error."""
        s = _entity()
        del s["recordDetails"]["entityType"]
        issues = validate_shape([s])
        assert any("entityType.type" in i for i in issues)

    def test_all_valid_entity_types(self):
        from opencheck.bods.validator import _VALID_ENTITY_TYPES

        for et in sorted(_VALID_ENTITY_TYPES):
            issues = validate_shape([_entity(entity_type=et)])
            assert issues == [], f"Unexpected issue for entity type {et!r}: {issues}"


# ---------------------------------------------------------------------------
# Person-specific validation
# ---------------------------------------------------------------------------


class TestPersonValidation:
    def test_invalid_person_type(self):
        issues = validate_shape([_person(person_type="fictional")])
        assert any("personType 'fictional' not in" in i for i in issues)

    def test_none_person_type(self):
        issues = validate_shape([_person(person_type=None)])
        assert any("personType" in i for i in issues)

    def test_empty_names_list(self):
        s = _person()
        s["recordDetails"]["names"] = []
        issues = validate_shape([s])
        assert any("person missing names[0].fullName" in i for i in issues)

    def test_missing_full_name(self):
        s = _person()
        s["recordDetails"]["names"] = [{"type": "individual"}]
        issues = validate_shape([s])
        assert any("person missing names[0].fullName" in i for i in issues)

    def test_none_full_name(self):
        s = _person()
        s["recordDetails"]["names"] = [{"type": "individual", "fullName": None}]
        issues = validate_shape([s])
        assert any("person missing names[0].fullName" in i for i in issues)

    def test_absent_names_key(self):
        s = _person()
        del s["recordDetails"]["names"]
        issues = validate_shape([s])
        assert any("person missing names[0].fullName" in i for i in issues)


# ---------------------------------------------------------------------------
# Relationship-specific validation
# ---------------------------------------------------------------------------


class TestRelationshipValidation:
    def _bundle(self, **rel_kwargs: Any) -> list[dict[str, Any]]:
        return [_entity(), _person(), _relationship(**rel_kwargs)]

    # --- subject ----------------------------------------------------------------

    def test_dangling_subject(self):
        issues = validate_shape(self._bundle(subject="does-not-exist"))
        assert any("subject references unknown statement 'does-not-exist'" in i for i in issues)

    def test_valid_subject_by_statement_id(self):
        """Default bundle uses statementId as subject — must pass."""
        assert validate_shape(self._bundle()) == []

    def test_valid_subject_by_record_id(self):
        """Relationship may reference the entity's recordId instead of statementId."""
        entity = _entity(statement_id="sid-001", record_id="rid-001")
        person = _person(statement_id="sid-002", record_id="rid-002")
        rel = _relationship(
            statement_id="sid-003",
            subject="rid-001",        # recordId reference
            interested_party="sid-002",
        )
        assert validate_shape([entity, person, rel]) == []

    def test_subject_legacy_wrapped_format(self):
        """v0.3-style {'describedByEntityStatement': '<id>'} must be resolved."""
        bundle = [
            _entity(),
            _person(),
            _relationship(subject={"describedByEntityStatement": _ENTITY_ID}),
        ]
        assert validate_shape(bundle) == []

    def test_subject_legacy_dangling(self):
        """Wrapped format pointing to a non-existent ID must be flagged."""
        bundle = [
            _entity(),
            _person(),
            _relationship(subject={"describedByEntityStatement": "ghost"}),
        ]
        issues = validate_shape(bundle)
        assert any("subject references unknown statement 'ghost'" in i for i in issues)

    # --- interestedParty --------------------------------------------------------

    def test_dangling_interested_party(self):
        issues = validate_shape(self._bundle(interested_party="ghost"))
        assert any("interestedParty references unknown statement 'ghost'" in i for i in issues)

    def test_valid_interested_party_person(self):
        assert validate_shape(self._bundle()) == []

    def test_valid_interested_party_entity(self):
        """interestedParty can reference another entity (intermediate ownership)."""
        entity_a = _entity(statement_id="e-a", name="Alpha Ltd")
        entity_b = _entity(statement_id="e-b", name="Beta Ltd")
        rel = _relationship(
            statement_id="rel-ab",
            subject="e-a",
            interested_party="e-b",
        )
        assert validate_shape([entity_a, entity_b, rel]) == []

    def test_interested_party_legacy_person(self):
        """v0.3-style {'describedByPersonStatement': '<id>'} must be resolved."""
        bundle = [
            _entity(),
            _person(),
            _relationship(interested_party={"describedByPersonStatement": _PERSON_ID}),
        ]
        assert validate_shape(bundle) == []

    def test_interested_party_legacy_entity(self):
        """v0.3-style {'describedByEntityStatement': '<id>'} must be resolved."""
        entity_a = _entity(statement_id="e-a", name="Alpha Ltd")
        entity_b = _entity(statement_id="e-b", name="Beta Ltd")
        rel = _relationship(
            statement_id="rel-ab",
            subject="e-a",
            interested_party={"describedByEntityStatement": "e-b"},
        )
        assert validate_shape([entity_a, entity_b, rel]) == []

    def test_interested_party_unidentified_bo(self):
        """Inline {'reason': ..., 'description': ...} is the BODS v0.4 pattern for
        an unidentifiable beneficial owner.  It must NOT be treated as a dangling
        statement reference."""
        entity = _entity()
        rel = _relationship(
            subject=_ENTITY_ID,
            interested_party={
                "reason": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
                "description": "Subject was unable to confirm.",
            },
        )
        assert validate_shape([entity, rel]) == []

    # --- interests --------------------------------------------------------------

    def test_invalid_interest_type(self):
        issues = validate_shape(
            self._bundle(interests=[{"type": "flying-saucer-control"}])
        )
        assert any("interests[0].type 'flying-saucer-control' not in codelist" in i for i in issues)

    def test_multiple_interests_second_invalid(self):
        issues = validate_shape(
            self._bundle(
                interests=[
                    {"type": "shareholding"},
                    {"type": "bogus"},
                ]
            )
        )
        assert any("interests[1].type 'bogus' not in codelist" in i for i in issues)

    def test_none_interest_type(self):
        issues = validate_shape(self._bundle(interests=[{"type": None}]))
        assert any("interests[0].type None not in codelist" in i for i in issues)

    def test_empty_interests_list_is_ok(self):
        """An empty interests list is not an error at the shape level."""
        assert validate_shape(self._bundle(interests=[])) == []

    def test_no_interests_key_is_ok(self):
        """Missing interests key defaults to empty — no error."""
        rel = _relationship()
        del rel["recordDetails"]["interests"]
        assert validate_shape([_entity(), _person(), rel]) == []


# ---------------------------------------------------------------------------
# Multiple errors in one bundle
# ---------------------------------------------------------------------------


class TestMultipleErrors:
    def test_two_bad_statements_both_reported(self):
        """Issues from all statements are collected, not just the first."""
        bad_entity = _entity(entity_type="bad-type")
        bad_person = _person(person_type="bad-type")
        issues = validate_shape([bad_entity, bad_person])
        assert len(issues) >= 2

    def test_error_prefix_includes_statement_id(self):
        """Issue messages must identify the offending statement by ID."""
        issues = validate_shape([_entity(entity_type="bad")])
        assert any(_ENTITY_ID in i for i in issues)


# ---------------------------------------------------------------------------
# assert_valid()
# ---------------------------------------------------------------------------


class TestAssertValid:
    def test_clean_bundle_does_not_raise(self):
        assert_valid([_entity(), _person(), _relationship()])

    def test_invalid_bundle_raises_validation_error(self):
        with pytest.raises(ValidationError):
            assert_valid([_entity(entity_type="bad")])

    def test_error_message_contains_issue(self):
        with pytest.raises(ValidationError, match="entityType.type"):
            assert_valid([_entity(entity_type="bad")])
