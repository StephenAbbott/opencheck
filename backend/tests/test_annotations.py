"""Phase 103 — BODS annotations recording what the register said.

The mapper transformed a great deal and recorded none of it. A reader could see
`otherInfluenceOrControl` without the Companies House code it came from, or a
`birthDate` of "1975-08" without knowing whether the register published only a
month or OpenCheck truncated a full date.

Two findings shaped the scope, both of which contradict the original plan:

1. **BODS already models date imprecision where it actually occurs.**
   `birthDate` legally accepts `YYYY`, `YYYY-MM` or `YYYY-MM-DD`, precisely
   because registers like Companies House publish month and year only, on
   purpose, for privacy. Rounding it would fabricate a day the register
   withheld deliberately. The strict fields (`foundingDate`, `startDate` and
   friends) do require `YYYY-MM-DD`, and no partial value currently reaches
   them — verified across the stored bundles and pinned by a canary below.
   So the phase records precision rather than changing it.

2. **The real loss was vocabulary, not dates.** Companies House
   nature-of-control codes were deliberately not modelled as BODS interest
   types (nominee arrangements need an intermediary `arrangement` entity, not
   yet implemented), so the code identity survived only inside an English prose
   descriptor — which is why the NOMINEE risk signal depended on the word
   "nominee" appearing in a sentence.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from opencheck.bods.annotations import (
    MOTIVATIONS,
    annotate,
    commenting,
    date_rounding_annotation,
    pointer,
    resolve_pointer,
    round_partial_date,
    transformation,
    validate_all,
    validate_annotations,
)
from opencheck.bods.mapper import map_companies_house


class TestPointer:
    def test_builds_rfc6901(self):
        assert pointer("recordDetails", "interests", 0, "type") == (
            "/recordDetails/interests/0/type"
        )

    def test_escapes_tilde_and_slash(self):
        # Without escaping, a field name containing either would silently
        # address a different fragment.
        assert pointer("a/b") == "/a~1b"
        assert pointer("a~b") == "/a~0b"

    def test_round_trips_through_resolve(self):
        doc = {"recordDetails": {"interests": [{"type": "shareholding"}]}}
        target = pointer("recordDetails", "interests", 0, "type")
        assert resolve_pointer(doc, target) == "shareholding"

    def test_resolve_handles_escaped_segments(self):
        doc = {"a/b": {"c~d": 1}}
        assert resolve_pointer(doc, pointer("a/b", "c~d")) == 1

    def test_resolve_raises_on_a_dangling_pointer(self):
        with pytest.raises((KeyError, IndexError)):
            resolve_pointer({"a": []}, "/a/0")


class TestAnnotationBuilders:
    def test_transformation_shape(self):
        a = transformation("/x", "source said 'Direktor'", transformed_content="boardMember")
        assert a["motivation"] == "transformation"
        assert a["statementPointerTarget"] == "/x"
        assert a["transformedContent"] == "boardMember"
        assert a["createdBy"]["name"] == "OpenCheck"

    def test_commenting_claims_no_transformation(self):
        a = commenting("/x", "context")
        assert a["motivation"] == "commenting"
        assert "transformedContent" not in a

    def test_every_motivation_used_is_in_the_codelist(self):
        for a in (transformation("/x", "d"), commenting("/x", "d")):
            assert a["motivation"] in MOTIVATIONS

    def test_annotate_skips_none(self):
        stmt: dict = {}
        annotate(stmt, None)
        assert "annotations" not in stmt

    def test_annotate_appends(self):
        stmt: dict = {}
        annotate(stmt, commenting("/a", "one"), commenting("/b", "two"))
        assert len(stmt["annotations"]) == 2


class TestDatePrecision:
    """BODS sanctions rounding for the strict date fields, but the rounding is
    then invisible — so it must be recorded."""

    def test_year_rounds_to_first_of_year(self):
        assert round_partial_date("2022") == ("2022-01-01", "year")

    def test_month_rounds_to_first_of_month(self):
        assert round_partial_date("2022-03") == ("2022-03-01", "month")

    def test_full_date_is_untouched(self):
        assert round_partial_date("2022-03-15") == ("2022-03-15", None)

    def test_none_passes_through(self):
        assert round_partial_date(None) == (None, None)

    def test_rounding_annotation_names_what_was_invented(self):
        a = date_rounding_annotation("/recordDetails/foundingDate", "2022", "year")
        assert "2022" in a["description"]
        assert "month and day" in a["description"]
        assert a["motivation"] == "transformation"


class TestValidation:
    def test_clean_statement_has_no_problems(self):
        stmt = {"recordDetails": {"birthDate": "1975-08"}}
        annotate(stmt, commenting(pointer("recordDetails", "birthDate"), "x"))
        assert validate_annotations(stmt) == []

    def test_dangling_pointer_is_caught(self):
        """A pointer into nothing validates fine against the JSON schema and is
        useless to a consumer — so it is checked here instead."""
        stmt = {"recordDetails": {}}
        annotate(stmt, commenting("/recordDetails/interests/0/type", "x"))
        problems = validate_annotations(stmt)
        assert problems and "does not resolve" in problems[0]

    def test_bad_motivation_is_caught(self):
        stmt = {"a": 1}
        stmt["annotations"] = [
            {"statementPointerTarget": "/a", "motivation": "explaining"}
        ]
        problems = validate_annotations(stmt)
        assert problems and "not in the codelist" in problems[0]


def _ch_bundle(natures=None, dob=None):
    return {
        "company_number": "00102498",
        "profile": {
            "company_number": "00102498",
            "company_name": "BP P.L.C.",
            "date_of_creation": "1909-04-14",
        },
        "officers": {"items": []},
        "pscs": {
            "items": [
                {
                    "kind": "individual-person-with-significant-control",
                    "name": "Jane SMITH",
                    "name_elements": {"forename": "Jane", "surname": "Smith"},
                    "etag": "abc123",
                    "notified_on": "2016-04-06",
                    "date_of_birth": dob if dob is not None else {"year": 1975, "month": 8},
                    "natures_of_control": natures
                    or ["ownership-of-shares-50-to-75-percent"],
                }
            ]
        },
    }


class TestCompaniesHouseAnnotations:
    def test_nature_of_control_code_survives_machine_readably(self):
        """The code was previously recoverable only from English prose."""
        stmts = list(map_companies_house(_ch_bundle()))
        descriptions = [
            a["description"]
            for s in stmts
            for a in s.get("annotations", [])
        ]
        assert any(
            "ownership-of-shares-50-to-75-percent" in d for d in descriptions
        )

    def test_nominee_code_is_recorded_even_though_unmapped(self):
        """registered-owner-as-nominee-* is deliberately not modelled as a BODS
        nominee interest (that needs an arrangement entity). Recording the code
        does not pre-empt that work, and stops the fact depending on prose."""
        stmts = list(
            map_companies_house(
                _ch_bundle(natures=["registered-owner-as-nominee-person-england-wales"])
            )
        )
        descriptions = [
            a["description"] for s in stmts for a in s.get("annotations", [])
        ]
        assert any("registered-owner-as-nominee" in d for d in descriptions)

    def test_annotation_points_at_the_interest_it_describes(self):
        stmts = list(
            map_companies_house(
                _ch_bundle(
                    natures=[
                        "ownership-of-shares-50-to-75-percent",
                        "voting-rights-50-to-75-percent",
                    ]
                )
            )
        )
        rel = next(s for s in stmts if s["recordType"] == "relationship")
        for annotation in rel["annotations"]:
            resolved = resolve_pointer(rel, annotation["statementPointerTarget"])
            assert isinstance(resolved, str)

    def test_imprecise_birth_date_is_explained_not_rounded(self):
        stmts = list(map_companies_house(_ch_bundle()))
        person = next(s for s in stmts if s["recordType"] == "person")
        # Not rounded: the register withheld the day on purpose.
        assert person["recordDetails"]["birthDate"] == "1975-08"
        note = next(
            a for a in person["annotations"]
            if a["statementPointerTarget"] == "/recordDetails/birthDate"
        )
        assert note["motivation"] == "commenting"
        assert "month and year only" in note["description"]

    def test_year_only_birth_date_is_explained(self):
        stmts = list(map_companies_house(_ch_bundle(dob={"year": 1975})))
        person = next(s for s in stmts if s["recordType"] == "person")
        assert person["recordDetails"]["birthDate"] == "1975"
        assert any("year only" in a["description"] for a in person["annotations"])

    def test_no_note_when_the_date_is_absent(self):
        stmts = list(map_companies_house(_ch_bundle(dob={})))
        person = next(s for s in stmts if s["recordType"] == "person")
        assert not any(
            a["statementPointerTarget"] == "/recordDetails/birthDate"
            for a in person.get("annotations", [])
        )

    def test_every_pointer_in_a_real_bundle_resolves(self):
        stmts = list(map_companies_house(_ch_bundle()))
        assert validate_all(stmts) == []


class TestStrictDateFieldCanary:
    """foundingDate / dissolutionDate / startDate / endDate MUST be full dates.

    Nothing currently emits a partial value into one. If an adapter ever does,
    this fails — and the fix is round_partial_date plus a rounding annotation,
    not a silently truncated string.
    """

    def test_no_partial_dates_in_strict_fields(self):
        stmts = list(map_companies_house(_ch_bundle()))
        for stmt in stmts:
            rd = stmt.get("recordDetails") or {}
            for field in ("foundingDate", "dissolutionDate"):
                value = rd.get(field)
                if value:
                    assert len(str(value)) >= 10, f"{field} is partial: {value}"
            for interest in rd.get("interests") or []:
                for field in ("startDate", "endDate"):
                    value = interest.get(field)
                    if value:
                        assert len(str(value)) >= 10, f"{field} is partial: {value}"


class TestSchemaCompliance:
    def test_annotated_output_validates(self):
        libcove = pytest.importorskip("libcovebods")
        from libcovebods.data_reader import DataReader
        from libcovebods.jsonschemavalidate import JSONSchemaValidator
        from libcovebods.schema import SchemaBODS

        stmts = list(map_companies_house(_ch_bundle()))
        assert any(s.get("annotations") for s in stmts), "expected annotations"

        path = Path(tempfile.mkdtemp()) / "out.json"
        path.write_text(json.dumps(stmts))
        reader = DataReader(str(path))
        errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
        assert errors == [], [e.json()["message"] for e in errors]


class TestSizeCost:
    """Annotations are not free and the deployment is memory-bound.

    The Render box is 512MB and has OOMed before (Phase 88). The mitigation is
    to annotate only lossy or non-obvious transformations — this pins that the
    policy is being followed, on a deliberately annotation-dense fixture.
    """

    def test_growth_stays_bounded(self):
        stmts = list(
            map_companies_house(
                _ch_bundle(
                    natures=[
                        "ownership-of-shares-50-to-75-percent",
                        "voting-rights-50-to-75-percent",
                    ]
                )
            )
        )
        stripped = json.dumps(
            [{k: v for k, v in s.items() if k != "annotations"} for s in stmts]
        )
        full = json.dumps(stmts)
        growth = (len(full) - len(stripped)) / len(stripped)
        # Worst case on a fixture where nearly every statement is annotated.
        # Real bundles are dominated by Open Ownership statements, which are
        # passed through verbatim and carry no OpenCheck annotations at all.
        assert growth < 0.75, f"annotations grew the bundle by {growth:.0%}"

    def test_unannotated_statements_carry_no_empty_array(self):
        """An empty `annotations: []` on every statement would be pure weight."""
        stmts = list(map_companies_house(_ch_bundle()))
        for stmt in stmts:
            assert stmt.get("annotations", None) != []
