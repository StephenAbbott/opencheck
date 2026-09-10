"""Phase 203 — Companies House identity verification as a BODS annotation.

Fixtures reproduce the three ``identity_verification_details`` shapes measured
on the live register on 10 Sept 2026 (Shell PLC's officers, DMGT's PSCs), with
the people renamed. See ``opencheck/bods/identity_verification.py`` for why the
rule is per role and why nothing is published for an unverified person.
"""

from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path

import pytest

from opencheck.bods import map_companies_house, validate_shape
from opencheck.bods.annotations import validate_all
from opencheck.bods.identity_verification import (
    ANNOTATION_KEY,
    ROUTE_ACSP,
    ROUTE_COMPANIES_HOUSE,
    read_verification,
)

TODAY = date(2026, 9, 10)

# --- the three live shapes --------------------------------------------------

ACSP = {
    "anti_money_laundering_supervisory_bodies": [
        "Faculty Office of the Archbishop of Canterbury (FO)"
    ],
    "appointment_verification_end_on": "9999-12-31",
    "appointment_verification_start_on": "2026-01-07",
    "authorised_corporate_service_provider_name": "EXAMPLE LLP ACSP",
    "identity_verified_on": "2025-07-28",
    "preferred_name": "Alex Preferred-Example",
}
DIRECT = {
    "appointment_verification_end_on": "9999-12-31",
    "appointment_verification_start_on": "2026-01-08",
}
DUE_ONLY = {"appointment_verification_statement_due_on": "2026-10-15"}
PSC_WINDOW_ONLY = {
    "appointment_verification_statement_date": "2026-06-01",
    "appointment_verification_statement_due_on": "2026-06-14",
}


def _officer(name: str, officer_id: str, ivd: dict | None, **extra) -> dict:
    item = {
        "name": name,
        "officer_role": "director",
        "appointed_on": "2020-01-01",
        "date_of_birth": {"year": 1970, "month": 5},
        "links": {"officer": {"appointments": f"/officers/{officer_id}/appointments"}},
    }
    if ivd is not None:
        item["identity_verification_details"] = ivd
    item.update(extra)
    return item


def _psc(name: str, etag: str, ivd: dict | None, **extra) -> dict:
    item = {
        "name": name,
        "etag": etag,
        "kind": "individual-person-with-significant-control",
        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
        "notified_on": "2016-04-06",
        "date_of_birth": {"year": 1967, "month": 8},
    }
    if ivd is not None:
        item["identity_verification_details"] = ivd
    item.update(extra)
    return item


def _bundle(officers: list[dict], pscs: list[dict] | None = None, **extra) -> dict:
    bundle = {
        "source_id": "companies_house",
        "company_number": "04366849",
        "profile": {"company_name": "EXAMPLE PLC"},
        "officers": {"items": officers},
        "pscs": {"items": pscs or []},
        "related_companies": {},
    }
    bundle.update(extra)
    return bundle


def _people(stmts) -> dict[str, dict]:
    return {
        s["recordDetails"]["names"][0]["fullName"]: s
        for s in stmts
        if s["recordType"] == "person"
    }


def _iv(stmt: dict) -> list[dict]:
    return [a for a in stmt.get("annotations") or [] if ANNOTATION_KEY in a]


def _role_rels(stmts, person_sid: str) -> list[dict]:
    return [
        s
        for s in stmts
        if s["recordType"] == "relationship"
        and s["recordDetails"].get("interestedParty") == person_sid
    ]


# --- the reader -------------------------------------------------------------


class TestReadVerification:
    def test_acsp_shape(self):
        v = read_verification({"identity_verification_details": ACSP}, today=TODAY)
        assert v is not None
        assert v.route == ROUTE_ACSP
        assert v.verifier == "EXAMPLE LLP ACSP"
        assert v.identity_verified_on == "2025-07-28"
        assert v.statement_on == "2026-01-07"
        assert v.supervisors == ("Faculty Office of the Archbishop of Canterbury (FO)",)

    def test_direct_shape_names_no_verifier_and_invents_no_date(self):
        v = read_verification({"identity_verification_details": DIRECT}, today=TODAY)
        assert v is not None
        assert v.route == ROUTE_COMPANIES_HOUSE
        assert v.verifier is None
        assert v.identity_verified_on is None
        assert v.statement_on == "2026-01-08"

    @pytest.mark.parametrize(
        "details",
        [None, {}, DUE_ONLY, PSC_WINDOW_ONLY, "not-a-dict"],
        ids=["absent", "empty", "due-only", "psc-window-only", "malformed"],
    )
    def test_not_verified_shapes(self, details):
        item = {} if details is None else {"identity_verification_details": details}
        assert read_verification(item, today=TODAY) is None

    def test_identity_verified_without_a_statement_is_not_a_tick(self):
        """CH would not show Verified against a role with no statement."""
        details = {k: v for k, v in ACSP.items() if not k.startswith("appointment_")}
        assert read_verification({"identity_verification_details": details}, today=TODAY) is None

    def test_removed_statement_is_not_verified(self):
        details = dict(DIRECT, appointment_verification_end_on="2026-03-01")
        assert read_verification({"identity_verification_details": details}, today=TODAY) is None

    def test_statement_ending_today_is_not_verified(self):
        details = dict(DIRECT, appointment_verification_end_on="2026-09-10")
        assert read_verification({"identity_verification_details": details}, today=TODAY) is None

    def test_future_end_is_still_verified(self):
        details = dict(DIRECT, appointment_verification_end_on="2027-01-01")
        assert read_verification({"identity_verification_details": details}, today=TODAY)

    def test_missing_end_is_open(self):
        details = {"appointment_verification_start_on": "2026-01-08"}
        assert read_verification({"identity_verification_details": details}, today=TODAY)

    @pytest.mark.parametrize("bad", ["08/01/2026", "2026-13-01", 20260108])
    def test_unreadable_start_date_is_not_verified(self, bad):
        details = dict(DIRECT, appointment_verification_start_on=bad)
        assert read_verification({"identity_verification_details": details}, today=TODAY) is None

    def test_unreadable_end_date_is_not_evidence_of_an_open_statement(self):
        details = dict(DIRECT, appointment_verification_end_on="open")
        assert read_verification({"identity_verification_details": details}, today=TODAY) is None


# --- officers on a company bundle -------------------------------------------


class TestOfficers:
    def _stmts(self):
        return list(
            map_companies_house(
                _bundle(
                    [
                        _officer("VERIFIED, Alex", "ofc-acsp", ACSP),
                        _officer("DIRECT, Dana", "ofc-direct", DIRECT),
                        _officer("PENDING, Robin", "ofc-due", DUE_ONLY),
                        _officer("NOBLOCK, Sam", "ofc-none", None),
                    ]
                )
            )
        )

    def test_only_verified_people_are_annotated(self):
        people = _people(self._stmts())
        assert len(_iv(people["VERIFIED, Alex"])) == 1
        assert len(_iv(people["DIRECT, Dana"])) == 1
        assert _iv(people["PENDING, Robin"]) == []
        assert _iv(people["NOBLOCK, Sam"]) == []

    def test_acsp_person_annotation_whole_object(self):
        stmts = self._stmts()
        (ann,) = _iv(_people(stmts)["VERIFIED, Alex"])
        assert ann == {
            "statementPointerTarget": "/recordDetails",
            "motivation": "commenting",
            "description": (
                "Companies House records this person's identity as verified. "
                "The identity was verified on 28 July 2025 by EXAMPLE LLP ACSP, "
                "an authorised corporate service provider supervised for "
                "anti-money laundering by Faculty Office of the Archbishop of "
                "Canterbury (FO). The first identity verification statement for "
                "a role in this data was supplied on 7 January 2026."
            ),
            "createdBy": {"name": "OpenCheck", "uri": "https://opencheck.world"},
            "url": "https://find-and-update.company-information.service.gov.uk/officers/ofc-acsp/appointments",
            ANNOTATION_KEY: {
                "status": "verified",
                "recordedBy": {
                    "name": "Companies House",
                    "uri": "https://www.gov.uk/government/organisations/companies-house",
                },
                "route": ROUTE_ACSP,
                "verifiedBy": {
                    "name": "EXAMPLE LLP ACSP",
                    "antiMoneyLaunderingSupervisoryBodies": [
                        "Faculty Office of the Archbishop of Canterbury (FO)"
                    ],
                },
                "identityVerifiedOn": "2025-07-28",
                "firstVerificationStatementOn": "2026-01-07",
            },
        }

    def test_direct_person_annotation_whole_object(self):
        stmts = self._stmts()
        (ann,) = _iv(_people(stmts)["DIRECT, Dana"])
        assert ann[ANNOTATION_KEY] == {
            "status": "verified",
            "recordedBy": {
                "name": "Companies House",
                "uri": "https://www.gov.uk/government/organisations/companies-house",
            },
            "route": ROUTE_COMPANIES_HOUSE,
            "firstVerificationStatementOn": "2026-01-08",
        }
        assert "no authorised corporate service provider" in ann["description"]
        assert "does not publish the date of verification" in ann["description"]
        assert "8 January 2026" in ann["description"]

    def test_role_annotation_sits_on_the_appointment(self):
        stmts = self._stmts()
        person = _people(stmts)["VERIFIED, Alex"]
        (rel,) = _role_rels(stmts, person["statementId"])
        (ann,) = _iv(rel)
        assert ann["statementPointerTarget"] == "/recordDetails/interestedParty"
        assert ann["motivation"] == "commenting"
        assert ann[ANNOTATION_KEY]["verificationStatementSuppliedOn"] == "2026-01-07"
        assert ann[ANNOTATION_KEY]["identityVerifiedOn"] == "2025-07-28"
        assert "firstVerificationStatementOn" not in ann[ANNOTATION_KEY]
        assert ann["description"].startswith(
            "Companies House records an identity verification statement for "
            "this role, supplied on 7 January 2026."
        )

    def test_preferred_name_is_never_published(self):
        assert "Alex Preferred-Example" not in json.dumps(self._stmts())

    def test_pointers_resolve_and_bundle_validates(self):
        stmts = self._stmts()
        assert validate_all(stmts) == []
        assert validate_shape(stmts) == []

    def test_resigned_verified_officer_is_not_mapped_at_all(self):
        stmts = list(
            map_companies_house(
                _bundle([_officer("GONE, Sam", "ofc-gone", ACSP, resigned_on="2026-05-01")])
            )
        )
        assert "GONE, Sam" not in _people(stmts)
        assert not any(_iv(s) for s in stmts)


class TestPersonAcrossBoards:
    """One director, two boards (Phase 193): statement in place on the parent only."""

    def _stmts(self):
        parent = {
            "company_number": "00000001",
            "profile": {"company_name": "PARENT PLC"},
            "officers": {"items": [_officer("VERIFIED, Alex", "ofc-shared", ACSP)]},
            "pscs": {"items": []},
        }
        root = _bundle(
            [_officer("VERIFIED, Alex", "ofc-shared", DUE_ONLY)],
            related_companies={"00000001": parent},
        )
        return list(map_companies_house(root))

    def test_person_written_first_by_the_unverified_board_is_still_marked(self):
        stmts = self._stmts()
        people = [s for s in stmts if s["recordType"] == "person"]
        assert len(people) == 1  # grouped on the officer id
        assert len(_iv(people[0])) == 1

    def test_only_the_verified_appointment_carries_a_role_annotation(self):
        stmts = self._stmts()
        person = next(s for s in stmts if s["recordType"] == "person")
        rels = _role_rels(stmts, person["statementId"])
        assert len(rels) == 2
        assert sorted(len(_iv(r)) for r in rels) == [0, 1]

    def test_richest_record_wins_and_first_statement_is_earliest(self):
        parent = {
            "company_number": "00000001",
            "profile": {"company_name": "PARENT PLC"},
            "officers": {"items": [_officer("VERIFIED, Alex", "ofc-shared", ACSP)]},
            "pscs": {"items": []},
        }
        earlier_direct = dict(DIRECT, appointment_verification_start_on="2025-12-01")
        root = _bundle(
            [_officer("VERIFIED, Alex", "ofc-shared", earlier_direct)],
            related_companies={"00000001": parent},
        )
        stmts = list(map_companies_house(root))
        person = next(s for s in stmts if s["recordType"] == "person")
        (ann,) = _iv(person)
        assert ann[ANNOTATION_KEY]["route"] == ROUTE_ACSP
        assert ann[ANNOTATION_KEY]["identityVerifiedOn"] == "2025-07-28"
        assert ann[ANNOTATION_KEY]["firstVerificationStatementOn"] == "2025-12-01"


# --- PSCs -------------------------------------------------------------------


class TestPscs:
    def _stmts(self, pscs):
        return list(map_companies_house(_bundle([], pscs)))

    def test_verified_individual_psc(self):
        stmts = self._stmts([_psc("Mr Example Owner", "e1", ACSP)])
        person = _people(stmts)["Mr Example Owner"]
        (ann,) = _iv(person)
        assert ann[ANNOTATION_KEY]["route"] == ROUTE_ACSP
        (rel,) = _role_rels(stmts, person["statementId"])
        (role,) = _iv(rel)
        assert role["url"].endswith("/company/04366849/persons-with-significant-control")
        assert validate_all(stmts) == []

    def test_psc_in_its_statement_window_is_not_verified(self):
        stmts = self._stmts([_psc("Mrs Window", "e2", PSC_WINDOW_ONLY)])
        assert not any(_iv(s) for s in stmts)

    def test_ceased_psc_earns_no_mark(self):
        stmts = self._stmts([_psc("Mr Former", "e3", ACSP, ceased_on="2026-04-01")])
        assert not any(_iv(s) for s in stmts)

    def test_corporate_psc_never_carries_one(self):
        corporate = {
            "name": "Example Holdings Limited",
            "kind": "corporate-entity-person-with-significant-control",
            "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
            "identification": {"registration_number": "99999999", "country_registered": "England"},
            # A block the register does not publish for RLEs today — even if it
            # did, the entity is not a person and must not be marked.
            "identity_verification_details": ACSP,
        }
        stmts = self._stmts([corporate])
        assert not any(_iv(s) for s in stmts)


# --- the officer-appointments bundle ------------------------------------------


class TestOfficerAppointmentsBundle:
    def _bundle(self):
        return {
            "officer_id": "ofc-appointments",
            "appointments": {
                "name": "Example PERSON",
                "date_of_birth": {"year": 1942, "month": 6},
                "items": [
                    {
                        "appointed_to": {"company_name": "EXAMPLE PLC", "company_number": "00041424"},
                        "officer_role": "director",
                        "appointed_on": "2022-07-20",
                        "identity_verification_details": dict(
                            ACSP, appointment_verification_start_on="2026-06-25"
                        ),
                    },
                    {
                        "appointed_to": {"company_name": "OLD LTD", "company_number": "00000587"},
                        "officer_role": "director",
                        "appointed_on": "1990-01-01",
                        "resigned_on": "1992-06-01",
                        "identity_verification_details": DIRECT,
                    },
                ],
            },
        }

    def test_person_marked_from_the_current_appointment_only(self):
        stmts = list(map_companies_house(self._bundle()))
        person = next(s for s in stmts if s["recordType"] == "person")
        (ann,) = _iv(person)
        assert ann[ANNOTATION_KEY]["firstVerificationStatementOn"] == "2026-06-25"
        rels = [s for s in stmts if s["recordType"] == "relationship"]
        assert [len(_iv(r)) for r in rels] == [1, 0]
        assert validate_all(stmts) == []


# --- the standard's own validator --------------------------------------------


def test_annotated_output_is_schema_valid_bods():
    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    stmts = list(
        map_companies_house(
            _bundle(
                [
                    _officer("VERIFIED, Alex", "ofc-acsp", ACSP),
                    _officer("DIRECT, Dana", "ofc-direct", DIRECT),
                ],
                [_psc("Mr Example Owner", "e1", ACSP)],
            )
        )
    )
    assert sum(1 for s in stmts if _iv(s)) == 6  # 3 people + 3 roles

    path = Path(tempfile.mkdtemp()) / "out.json"
    path.write_text(json.dumps(stmts))
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]
