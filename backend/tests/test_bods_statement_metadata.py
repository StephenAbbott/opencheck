"""Phase 7 — BODS statement metadata completeness audit.

Tests cover three layers:

1. _source_block() — every registered source_id must produce a human-readable
   description (not the raw source_id) and the correct source.type tag
   ("officialRegister" vs "thirdParty").

2. _publication_details_block() — required fields are always present.

3. make_entity_statement / make_person_statement / make_relationship_statement
   — each factory must produce complete top-level metadata on every call.

These are separate from the graph-connectivity and interest-type tests in
test_bods_graph_integrity.py — they focus exclusively on the metadata envelope
(statementDate, source, publicationDetails) that BODS v0.4 requires on every
statement regardless of type.
"""

from __future__ import annotations

import re
from datetime import date, timezone
from typing import Any

import pytest

from opencheck.bods.mapper import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)
from opencheck.bods.validator import validate_shape

# ---------------------------------------------------------------------------
# Source ID catalogues — mirrors what _source_block has registered.
# These lists are the test's contract: any new source added to the mapper must
# appear in the correct catalogue.
# ---------------------------------------------------------------------------

# Official national registers and government data sources.
_OFFICIAL_REGISTERS = {
    "acra_singapore",
    "ariregister",
    "bce_belgium",
    "bods_gleif",
    "bods_uk_psc",
    "bolagsverket",
    "brreg",
    "companies_house",
    "corporations_canada",
    "cro",
    "cvr_denmark",
    "firmenbuch",
    "inpi",
    "jar_lithuania",
    "krs_poland",
    "kvk",
    "opencorporates",
    "prh",
    "rpo_slovakia",
    "rpvs_slovakia",
    "sec_edgar",
    "ur_latvia",
    "ares",
    "zefix",
}

# Third-party aggregators and derived sources.
_THIRD_PARTY_SOURCES = {
    "brightquery",
    "climatetrace",
    "everypolitician",
    "gleif",
    "openaleph",
    "opensanctions",
    "opentender",
    "wikidata",
}

_ALL_SOURCE_IDS = _OFFICIAL_REGISTERS | _THIRD_PARTY_SOURCES

# ---------------------------------------------------------------------------
# Access to private helpers via the module — we test them directly because
# the metadata envelope they build appears on every statement.
# ---------------------------------------------------------------------------

import opencheck.bods.mapper as _mapper_module

_source_block = _mapper_module._source_block
_publication_details_block = _mapper_module._publication_details_block
_stable_id = _mapper_module._stable_id
_today = _mapper_module._today


# ---------------------------------------------------------------------------
# _source_block tests
# ---------------------------------------------------------------------------


class TestSourceBlock:
    """_source_block(source_id, source_url) must produce correct metadata for
    every known source, and never fall back to the raw source_id as the
    description."""

    @pytest.mark.parametrize("source_id", sorted(_ALL_SOURCE_IDS))
    def test_description_is_human_readable(self, source_id: str):
        """description must not be the raw source_id (fallback sentinel)."""
        block = _source_block(source_id, None)
        assert block["description"] != source_id, (
            f"source_id {source_id!r} has no entry in source_names — "
            f"description fell back to the raw ID"
        )

    @pytest.mark.parametrize("source_id", sorted(_ALL_SOURCE_IDS))
    def test_description_is_non_empty_string(self, source_id: str):
        block = _source_block(source_id, None)
        assert isinstance(block["description"], str)
        assert len(block["description"]) > 0

    @pytest.mark.parametrize("source_id", sorted(_OFFICIAL_REGISTERS))
    def test_official_registers_get_official_register_type(self, source_id: str):
        block = _source_block(source_id, None)
        assert block["type"] == ["officialRegister"], (
            f"source_id {source_id!r} should have type=['officialRegister'] "
            f"but got {block['type']!r}"
        )

    @pytest.mark.parametrize("source_id", sorted(_THIRD_PARTY_SOURCES))
    def test_third_party_sources_get_third_party_type(self, source_id: str):
        block = _source_block(source_id, None)
        assert block["type"] == ["thirdParty"], (
            f"source_id {source_id!r} should have type=['thirdParty'] "
            f"but got {block['type']!r}"
        )

    @pytest.mark.parametrize("source_id", sorted(_ALL_SOURCE_IDS))
    def test_retrieved_at_is_iso_utc(self, source_id: str):
        """retrievedAt must be a UTC ISO-8601 timestamp ending in Z."""
        block = _source_block(source_id, None)
        val = block.get("retrievedAt", "")
        assert isinstance(val, str)
        assert val.endswith("Z"), f"retrievedAt {val!r} does not end with 'Z'"
        # Must parse as an ISO timestamp.
        # Format: YYYY-MM-DDTHH:MM:SSZ
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", val), (
            f"retrievedAt {val!r} is not in YYYY-MM-DDTHH:MM:SSZ format"
        )

    def test_url_absent_when_not_provided(self):
        block = _source_block("gleif", None)
        assert "url" not in block

    def test_url_present_when_provided(self):
        block = _source_block("gleif", "https://search.gleif.org/#/record/test")
        assert block["url"] == "https://search.gleif.org/#/record/test"

    def test_unknown_source_id_falls_back_to_id_and_third_party(self):
        """Any source_id not in the registry must fall back gracefully —
        raw ID as description and thirdParty type — rather than raising."""
        block = _source_block("some_future_source", None)
        assert block["description"] == "some_future_source"
        assert block["type"] == ["thirdParty"]

    # Spot-check specific source descriptions that were previously missing ----

    def test_acra_singapore_description(self):
        block = _source_block("acra_singapore", None)
        assert "ACRA" in block["description"]
        assert "Singapore" in block["description"]

    def test_bce_belgium_description(self):
        block = _source_block("bce_belgium", None)
        assert "BCE" in block["description"] or "KBO" in block["description"]

    def test_jar_lithuania_description(self):
        block = _source_block("jar_lithuania", None)
        assert "JAR" in block["description"] or "Lithuania" in block["description"]

    def test_prh_finland_description(self):
        block = _source_block("prh", None)
        assert "PRH" in block["description"] or "Finland" in block["description"]

    def test_bods_gleif_description(self):
        block = _source_block("bods_gleif", None)
        assert "GLEIF" in block["description"]

    def test_bods_uk_psc_description(self):
        block = _source_block("bods_uk_psc", None)
        assert "Companies House" in block["description"] or "PSC" in block["description"]


# ---------------------------------------------------------------------------
# _publication_details_block tests
# ---------------------------------------------------------------------------


class TestPublicationDetailsBlock:
    """_publication_details_block must always produce the three required fields."""

    def test_required_fields_present(self):
        block = _publication_details_block()
        assert "bodsVersion" in block
        assert "publicationDate" in block
        assert "publisher" in block

    def test_bods_version_is_0_4(self):
        block = _publication_details_block()
        assert block["bodsVersion"] == "0.4"

    def test_publisher_is_opencheck(self):
        block = _publication_details_block()
        assert block["publisher"] == {"name": "OpenCheck"}

    def test_publication_date_defaults_to_today(self):
        block = _publication_details_block()
        assert block["publicationDate"] == date.today().isoformat()

    def test_publication_date_overridable(self):
        block = _publication_details_block("2023-06-15")
        assert block["publicationDate"] == "2023-06-15"

    def test_publication_date_format_yyyy_mm_dd(self):
        block = _publication_details_block()
        val = block["publicationDate"]
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", val), (
            f"publicationDate {val!r} is not in YYYY-MM-DD format"
        )


# ---------------------------------------------------------------------------
# _stable_id tests
# ---------------------------------------------------------------------------


class TestStableId:
    def test_deterministic(self):
        a = _stable_id("companies_house", "entity", "12345678")
        b = _stable_id("companies_house", "entity", "12345678")
        assert a == b

    def test_different_inputs_produce_different_ids(self):
        a = _stable_id("companies_house", "entity", "12345678")
        b = _stable_id("companies_house", "entity", "87654321")
        assert a != b

    def test_prefix(self):
        result = _stable_id("gleif", "entity", "test")
        assert result.startswith("opencheck-")

    def test_length(self):
        """ID should be 'opencheck-' (10 chars) + 24 hex chars = 34 total."""
        result = _stable_id("gleif", "entity", "test")
        assert len(result) == 34

    def test_hex_suffix(self):
        result = _stable_id("gleif", "entity", "test")
        suffix = result[len("opencheck-"):]
        assert re.match(r"^[0-9a-f]+$", suffix)


# ---------------------------------------------------------------------------
# make_entity_statement tests
# ---------------------------------------------------------------------------

_ENTITY_SOURCE = "companies_house"
_ENTITY_LOCAL = "12345678"


class TestMakeEntityStatement:
    """make_entity_statement must produce all required top-level BODS fields."""

    @pytest.fixture
    def stmt(self) -> dict[str, Any]:
        return make_entity_statement(
            source_id=_ENTITY_SOURCE,
            local_id=_ENTITY_LOCAL,
            name="Acme Ltd",
        )

    def test_statement_id_present(self, stmt):
        assert "statementId" in stmt
        assert stmt["statementId"].startswith("opencheck-")

    def test_record_id_equals_statement_id(self, stmt):
        assert stmt["recordId"] == stmt["statementId"]

    def test_record_type_entity(self, stmt):
        assert stmt["recordType"] == "entity"

    def test_record_status_new(self, stmt):
        assert stmt["recordStatus"] == "new"

    def test_statement_date_today(self, stmt):
        assert stmt["statementDate"] == date.today().isoformat()

    def test_source_present(self, stmt):
        assert "source" in stmt

    def test_source_description_human_readable(self, stmt):
        assert stmt["source"]["description"] != _ENTITY_SOURCE

    def test_source_type_official_register(self, stmt):
        assert stmt["source"]["type"] == ["officialRegister"]

    def test_publication_details_present(self, stmt):
        assert "publicationDetails" in stmt

    def test_publication_details_version(self, stmt):
        assert stmt["publicationDetails"]["bodsVersion"] == "0.4"

    def test_record_details_name(self, stmt):
        assert stmt["recordDetails"]["name"] == "Acme Ltd"

    def test_record_details_entity_type_default(self, stmt):
        assert stmt["recordDetails"]["entityType"]["type"] == "registeredEntity"

    def test_passes_validate_shape(self, stmt):
        assert validate_shape([stmt]) == []

    def test_jurisdiction_optional(self, stmt):
        assert "jurisdiction" not in stmt["recordDetails"]

    def test_jurisdiction_included_when_provided(self):
        stmt = make_entity_statement(
            source_id=_ENTITY_SOURCE,
            local_id=_ENTITY_LOCAL,
            name="Acme Ltd",
            jurisdiction=("United Kingdom", "GB"),
        )
        assert stmt["recordDetails"]["jurisdiction"] == {"name": "United Kingdom", "code": "GB"}

    def test_founding_date_included_when_provided(self):
        stmt = make_entity_statement(
            source_id=_ENTITY_SOURCE,
            local_id=_ENTITY_LOCAL,
            name="Acme Ltd",
            founding_date="2005-03-15",
        )
        assert stmt["recordDetails"]["foundingDate"] == "2005-03-15"

    def test_identifiers_included(self):
        stmt = make_entity_statement(
            source_id=_ENTITY_SOURCE,
            local_id=_ENTITY_LOCAL,
            name="Acme Ltd",
            identifiers=[{"id": "12345678", "scheme": "GB-COH"}],
        )
        assert stmt["recordDetails"]["identifiers"] == [{"id": "12345678", "scheme": "GB-COH"}]

    def test_source_url_in_source_block(self):
        url = "https://find-and-update.company-information.service.gov.uk/company/12345678"
        stmt = make_entity_statement(
            source_id=_ENTITY_SOURCE,
            local_id=_ENTITY_LOCAL,
            name="Acme Ltd",
            source_url=url,
        )
        assert stmt["source"]["url"] == url

    def test_deterministic_statement_id(self):
        a = make_entity_statement(source_id=_ENTITY_SOURCE, local_id=_ENTITY_LOCAL, name="Acme Ltd")
        b = make_entity_statement(source_id=_ENTITY_SOURCE, local_id=_ENTITY_LOCAL, name="Acme Ltd")
        assert a["statementId"] == b["statementId"]

    def test_custom_entity_type(self):
        stmt = make_entity_statement(
            source_id=_ENTITY_SOURCE,
            local_id=_ENTITY_LOCAL,
            name="UK Gov",
            entity_type="state",
        )
        assert stmt["recordDetails"]["entityType"]["type"] == "state"
        assert validate_shape([stmt]) == []

    @pytest.mark.parametrize("source_id", sorted(_ALL_SOURCE_IDS))
    def test_all_sources_produce_valid_statements(self, source_id: str):
        """Every registered source_id must produce a valid entity statement."""
        stmt = make_entity_statement(
            source_id=source_id,
            local_id="test-001",
            name=f"Test Entity ({source_id})",
        )
        assert stmt["source"]["description"] != source_id, (
            f"source_id {source_id!r} fell back to raw ID as description"
        )
        assert validate_shape([stmt]) == []


# ---------------------------------------------------------------------------
# make_person_statement tests
# ---------------------------------------------------------------------------

_PERSON_SOURCE = "companies_house"
_PERSON_LOCAL = "psc-abc123"


class TestMakePersonStatement:
    @pytest.fixture
    def stmt(self) -> dict[str, Any]:
        return make_person_statement(
            source_id=_PERSON_SOURCE,
            local_id=_PERSON_LOCAL,
            full_name="Jane Smith",
        )

    def test_record_type_person(self, stmt):
        assert stmt["recordType"] == "person"

    def test_record_status_new(self, stmt):
        assert stmt["recordStatus"] == "new"

    def test_statement_date_today(self, stmt):
        assert stmt["statementDate"] == date.today().isoformat()

    def test_source_present(self, stmt):
        assert "source" in stmt

    def test_source_description_human_readable(self, stmt):
        assert stmt["source"]["description"] != _PERSON_SOURCE

    def test_publication_details_present(self, stmt):
        assert "publicationDetails" in stmt

    def test_full_name_in_names(self, stmt):
        assert stmt["recordDetails"]["names"][0]["fullName"] == "Jane Smith"

    def test_person_type_default(self, stmt):
        assert stmt["recordDetails"]["personType"] == "knownPerson"

    def test_passes_validate_shape(self, stmt):
        assert validate_shape([stmt]) == []

    def test_anonymous_person_type(self):
        stmt = make_person_statement(
            source_id=_PERSON_SOURCE,
            local_id=_PERSON_LOCAL,
            full_name="Unknown",
            person_type="anonymousPerson",
        )
        assert stmt["recordDetails"]["personType"] == "anonymousPerson"
        assert validate_shape([stmt]) == []

    def test_nationalities_included(self):
        stmt = make_person_statement(
            source_id=_PERSON_SOURCE,
            local_id=_PERSON_LOCAL,
            full_name="Jane Smith",
            nationalities=[{"code": "GB", "name": "British"}],
        )
        assert stmt["recordDetails"]["nationalities"] == [{"code": "GB", "name": "British"}]

    def test_birth_date_included(self):
        stmt = make_person_statement(
            source_id=_PERSON_SOURCE,
            local_id=_PERSON_LOCAL,
            full_name="Jane Smith",
            birth_date="1975-06-20",
        )
        assert stmt["recordDetails"]["birthDate"] == "1975-06-20"

    def test_record_id_equals_statement_id(self, stmt):
        assert stmt["recordId"] == stmt["statementId"]


# ---------------------------------------------------------------------------
# make_relationship_statement tests
# ---------------------------------------------------------------------------

_REL_SOURCE = "companies_house"
_REL_ENTITY_ID = _stable_id("companies_house", "entity", "12345678")
_REL_PERSON_ID = _stable_id("companies_house", "person", "psc-abc123")


class TestMakeRelationshipStatement:
    @pytest.fixture
    def entity_stmt(self) -> dict[str, Any]:
        return make_entity_statement(
            source_id=_ENTITY_SOURCE, local_id=_ENTITY_LOCAL, name="Acme Ltd"
        )

    @pytest.fixture
    def person_stmt(self) -> dict[str, Any]:
        return make_person_statement(
            source_id=_PERSON_SOURCE, local_id=_PERSON_LOCAL, full_name="Jane Smith"
        )

    @pytest.fixture
    def rel_stmt(self, entity_stmt, person_stmt) -> dict[str, Any]:
        return make_relationship_statement(
            source_id=_REL_SOURCE,
            local_id="psc-abc123",
            subject_statement_id=entity_stmt["statementId"],
            interested_party_statement_id=person_stmt["statementId"],
            interests=[
                {
                    "type": "shareholding",
                    "directOrIndirect": "direct",
                    "beneficialOwnershipOrControl": True,
                }
            ],
        )

    def test_record_type_relationship(self, rel_stmt):
        assert rel_stmt["recordType"] == "relationship"

    def test_record_status_new(self, rel_stmt):
        assert rel_stmt["recordStatus"] == "new"

    def test_statement_date_today(self, rel_stmt):
        assert rel_stmt["statementDate"] == date.today().isoformat()

    def test_source_present(self, rel_stmt):
        assert "source" in rel_stmt

    def test_source_description_human_readable(self, rel_stmt):
        assert rel_stmt["source"]["description"] != _REL_SOURCE

    def test_publication_details_present(self, rel_stmt):
        assert "publicationDetails" in rel_stmt

    def test_subject_points_to_entity(self, entity_stmt, rel_stmt):
        assert rel_stmt["recordDetails"]["subject"] == entity_stmt["statementId"]

    def test_interested_party_points_to_person(self, person_stmt, rel_stmt):
        assert rel_stmt["recordDetails"]["interestedParty"] == person_stmt["statementId"]

    def test_interests_populated(self, rel_stmt):
        assert rel_stmt["recordDetails"]["interests"]

    def test_full_bundle_passes_validate_shape(self, entity_stmt, person_stmt, rel_stmt):
        assert validate_shape([entity_stmt, person_stmt, rel_stmt]) == []

    def test_statement_id_differs_from_record_id(self, rel_stmt):
        """Relationship statements use separate statementId and recordId
        (unlike entity/person which have statementId == recordId)."""
        assert rel_stmt["statementId"] != rel_stmt["recordId"]

    def test_source_url_propagates(self, entity_stmt, person_stmt):
        url = "https://find-and-update.company-information.service.gov.uk/company/12345678/persons-with-significant-control"
        rel = make_relationship_statement(
            source_id=_REL_SOURCE,
            local_id="psc-abc123",
            subject_statement_id=entity_stmt["statementId"],
            interested_party_statement_id=person_stmt["statementId"],
            source_url=url,
        )
        assert rel["source"]["url"] == url


# ---------------------------------------------------------------------------
# Cross-source metadata consistency
# ---------------------------------------------------------------------------


class TestCrossSourceMetadataConsistency:
    """Statements from the same source_id + local_id must produce the same
    statementId regardless of when they are generated (determinism under the
    factory functions)."""

    def test_entity_statement_id_is_stable_across_calls(self):
        calls = [
            make_entity_statement(source_id="kvk", local_id="90000001", name="Test BV")
            for _ in range(3)
        ]
        ids = {s["statementId"] for s in calls}
        assert len(ids) == 1, "statementId is not deterministic across calls"

    def test_person_statement_id_is_stable_across_calls(self):
        calls = [
            make_person_statement(source_id="kvk", local_id="person-001", full_name="Jan de Vries")
            for _ in range(3)
        ]
        ids = {s["statementId"] for s in calls}
        assert len(ids) == 1

    def test_different_sources_same_local_id_produce_different_statement_ids(self):
        """Two sources reporting on the same local_id must not clash in statementId."""
        e_ch = make_entity_statement(source_id="companies_house", local_id="12345678", name="Acme")
        e_oc = make_entity_statement(source_id="opencorporates", local_id="12345678", name="Acme")
        assert e_ch["statementId"] != e_oc["statementId"]
