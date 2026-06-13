"""Phase 6 — Conformance tests using the canonical bods-fixtures pack.

The ``pytest-bods-v04-fixtures`` plugin auto-parametrizes the ``bods_fixture``
parameter across all cases in the pack:
  - core/01-direct-ownership
  - edge-cases/10-circular-ownership    (entity→entity circular graph)
  - edge-cases/11-anonymous-person      (inline unidentified-BO interestedParty)

Every canonical fixture must pass OpenCheck's internal shape validator with
zero issues.  Additional per-fixture tests assert structural properties that
characterise each case.
"""

from __future__ import annotations

from typing import Any

import pytest

# The bods-fixtures pack + its pytest plugin (pytest-bods-v04-fixtures) are
# dev/test-only dependencies. Skip the whole module cleanly when absent
# rather than aborting collection with a hard ModuleNotFoundError.
bods_fixtures = pytest.importorskip(
    "bods_fixtures",
    reason="pytest-bods-v04-fixtures not installed — run `uv sync` / install the test extra",
)
load = bods_fixtures.load

from opencheck.bods.validator import validate_shape

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _by_type(statements: list[dict[str, Any]], record_type: str) -> list[dict[str, Any]]:
    return [s for s in statements if s.get("recordType") == record_type]


def _interests(rel: dict[str, Any]) -> list[dict[str, Any]]:
    return rel.get("recordDetails", {}).get("interests") or []


# ---------------------------------------------------------------------------
# Parametrized — all canonical fixtures must pass validate_shape()
# ---------------------------------------------------------------------------


class TestAllFixturesPassValidator:
    """The canonical fixture pack is the ground truth for BODS v0.4 validity.
    Any issue produced by validate_shape() for these fixtures indicates a bug
    in the validator, not in the fixture.
    """

    def test_validate_shape_clean(self, bods_fixture):
        issues = validate_shape(bods_fixture.statements)
        assert issues == [], (
            f"validate_shape() raised {len(issues)} issue(s) for "
            f"fixture {bods_fixture.name!r}:\n"
            + "\n".join(f"  {i}" for i in issues)
        )

    def test_all_statements_have_required_fields(self, bods_fixture):
        required = {"statementId", "recordId", "recordType", "recordStatus", "recordDetails"}
        for s in bods_fixture.statements:
            missing = required - set(s.keys())
            assert not missing, (
                f"Fixture {bods_fixture.name!r}: statement "
                f"{s.get('statementId', '?')} is missing {missing}"
            )

    def test_record_types_are_valid(self, bods_fixture):
        valid = {"entity", "person", "relationship"}
        for s in bods_fixture.statements:
            assert s["recordType"] in valid, (
                f"Fixture {bods_fixture.name!r}: unexpected recordType "
                f"{s['recordType']!r}"
            )

    def test_record_statuses_are_valid(self, bods_fixture):
        valid = {"new", "updated", "closed"}
        for s in bods_fixture.statements:
            assert s["recordStatus"] in valid, (
                f"Fixture {bods_fixture.name!r}: unexpected recordStatus "
                f"{s['recordStatus']!r}"
            )

    def test_fixture_has_at_least_one_statement(self, bods_fixture):
        assert len(bods_fixture.statements) > 0

    def test_statement_ids_are_unique(self, bods_fixture):
        sids = [s["statementId"] for s in bods_fixture.statements]
        assert len(sids) == len(set(sids)), (
            f"Fixture {bods_fixture.name!r}: duplicate statementId values"
        )


# ---------------------------------------------------------------------------
# core/01-direct-ownership — entity + person + shareholding relationship
# ---------------------------------------------------------------------------


class TestDirectOwnership:
    """core/01-direct-ownership: one entity, one known person, one relationship."""

    @pytest.fixture(scope="class")
    def fix(self):
        return load("core/01-direct-ownership")

    def test_statement_count(self, fix):
        assert len(fix.statements) == 3

    def test_has_one_entity(self, fix):
        assert len(_by_type(fix.statements, "entity")) == 1

    def test_has_one_person(self, fix):
        assert len(_by_type(fix.statements, "person")) == 1

    def test_has_one_relationship(self, fix):
        assert len(_by_type(fix.statements, "relationship")) == 1

    def test_entity_name_present(self, fix):
        entity = _by_type(fix.statements, "entity")[0]
        assert entity["recordDetails"].get("name")

    def test_person_full_name_present(self, fix):
        person = _by_type(fix.statements, "person")[0]
        names = person["recordDetails"].get("names") or []
        assert names and names[0].get("fullName")

    def test_person_is_known_person(self, fix):
        person = _by_type(fix.statements, "person")[0]
        assert person["recordDetails"]["personType"] == "knownPerson"

    def test_relationship_has_interests(self, fix):
        rel = _by_type(fix.statements, "relationship")[0]
        assert _interests(rel)

    def test_shareholding_interest_type(self, fix):
        rel = _by_type(fix.statements, "relationship")[0]
        types = {i.get("type") for i in _interests(rel)}
        assert "shareholding" in types

    def test_beneficial_ownership_true(self, fix):
        rel = _by_type(fix.statements, "relationship")[0]
        assert any(i.get("beneficialOwnershipOrControl") is True for i in _interests(rel))

    def test_direct_ownership(self, fix):
        rel = _by_type(fix.statements, "relationship")[0]
        assert all(
            i.get("directOrIndirect") == "direct"
            for i in _interests(rel)
            if "directOrIndirect" in i
        )

    def test_validate_shape_clean(self, fix):
        assert validate_shape(fix.statements) == []


# ---------------------------------------------------------------------------
# edge-cases/10-circular-ownership — two entities each owning the other
# ---------------------------------------------------------------------------


class TestCircularOwnership:
    """edge-cases/10-circular-ownership: entity A owns entity B and vice versa.

    This tests that the validator handles graphs with cycles correctly — there
    is no person statement; both relationship interestedParties reference
    entities.
    """

    @pytest.fixture(scope="class")
    def fix(self):
        return load("edge-cases/10-circular-ownership")

    def test_statement_count(self, fix):
        assert len(fix.statements) == 4

    def test_has_two_entities(self, fix):
        assert len(_by_type(fix.statements, "entity")) == 2

    def test_has_no_person(self, fix):
        assert len(_by_type(fix.statements, "person")) == 0

    def test_has_two_relationships(self, fix):
        assert len(_by_type(fix.statements, "relationship")) == 2

    def test_both_relationships_have_interests(self, fix):
        for rel in _by_type(fix.statements, "relationship"):
            assert _interests(rel), f"Relationship {rel['statementId']} has no interests"

    def test_entity_names_distinct(self, fix):
        names = {e["recordDetails"]["name"] for e in _by_type(fix.statements, "entity")}
        assert len(names) == 2

    def test_validate_shape_clean(self, fix):
        assert validate_shape(fix.statements) == []


# ---------------------------------------------------------------------------
# edge-cases/11-anonymous-person — unidentifiable beneficial owner
# ---------------------------------------------------------------------------


class TestAnonymousPerson:
    """edge-cases/11-anonymous-person: entity with an unidentified BO.

    The relationship's interestedParty is an inline dict with a 'reason' key,
    not a reference to another statement.  validate_shape() must accept this
    without flagging a dangling reference.
    """

    @pytest.fixture(scope="class")
    def fix(self):
        return load("edge-cases/11-anonymous-person")

    def test_statement_count(self, fix):
        assert len(fix.statements) == 2

    def test_has_one_entity(self, fix):
        assert len(_by_type(fix.statements, "entity")) == 1

    def test_has_no_person_statement(self, fix):
        """The anonymous BO is represented inline, not as a person statement."""
        assert len(_by_type(fix.statements, "person")) == 0

    def test_has_one_relationship(self, fix):
        assert len(_by_type(fix.statements, "relationship")) == 1

    def test_interested_party_has_reason_key(self, fix):
        rel = _by_type(fix.statements, "relationship")[0]
        ip = rel["recordDetails"].get("interestedParty")
        assert isinstance(ip, dict), "interestedParty should be a dict in this fixture"
        assert "reason" in ip, "interestedParty dict should have a 'reason' key"

    def test_interested_party_is_not_a_string_reference(self, fix):
        rel = _by_type(fix.statements, "relationship")[0]
        ip = rel["recordDetails"].get("interestedParty")
        assert not isinstance(ip, str)

    def test_validate_shape_clean(self, fix):
        """This is the key test: the unidentified-BO pattern must not produce
        a 'references unknown statement' error."""
        assert validate_shape(fix.statements) == []

    def test_fixture_name_helper(self, fix):
        assert fix.name == "edge-cases/11-anonymous-person"

    def test_by_record_type_helper(self, fix):
        assert len(fix.by_record_type("entity")) == 1
        assert len(fix.by_record_type("person")) == 0
        assert len(fix.by_record_type("relationship")) == 1
