"""Phase 102 — beneficialOwnershipOrControl is asserted, never inferred.

BODS distinguishes three states: ``true``, ``false``, and absent ("not
stated"). OpenCheck was collapsing absent into true in three places, by
reasoning from the *shape* of the interest rather than from anything a source
had said:

* OpenCorporates network relationships — ``interest_type in ("shareholding",
  "votingRights")``, plus two hard-coded ``True``s on the percentage path
* New Zealand — ``kind == "person"``
* SEC EDGAR — a hard-coded ``True`` for every 13D/13G filer, while the
  ``typeOfReportingPerson`` code that would qualify it sat unused in the bundle

A shareholding is a *legal* holding. Whether it is also a *beneficial* one is a
separate fact only a register or a BO declaration regime can supply. The
reasoning was already written down for the FollowTheMoney path; it had simply
never been applied to the commercial-register mappers.

The direction of the error matters. Over-claiming beneficial ownership is a
reputational assertion about a named person, and it travels into every export
(RDF, FtM, Senzing, BigQuery), well beyond any caveat the UI can attach.
"""

from __future__ import annotations

import pytest

from opencheck.bods.mapper import (
    _BO_ASSERTING_SOURCES,
    _SEC_CUSTODIAL_REPORTER_CODES,
    _sec_beneficial_ownership,
    map_nz_companies,
    map_sec_edgar,
    set_beneficial_ownership,
    source_may_assert_beneficial_ownership,
)


class TestPolicyHelper:
    def test_explicit_true_is_emitted_for_any_source(self):
        interest = set_beneficial_ownership({}, "opencorporates", asserted=True)
        assert interest["beneficialOwnershipOrControl"] is True

    def test_explicit_false_is_emitted_for_any_source(self):
        """A register saying "not beneficially held" is information, not silence."""
        interest = set_beneficial_ownership({}, "opencorporates", asserted=False)
        assert interest["beneficialOwnershipOrControl"] is False

    def test_silence_from_a_non_bo_source_omits_the_key(self):
        interest = set_beneficial_ownership({}, "opencorporates")
        assert "beneficialOwnershipOrControl" not in interest

    def test_silence_from_a_bo_source_still_asserts(self):
        interest = set_beneficial_ownership({}, "companies_house")
        assert interest["beneficialOwnershipOrControl"] is True

    def test_unknown_source_is_treated_conservatively(self):
        assert source_may_assert_beneficial_ownership("some_new_adapter") is False

    def test_allow_list_contains_only_bo_regimes(self):
        # Guard against a future edit quietly adding a plain company register.
        for source_id in _BO_ASSERTING_SOURCES:
            assert source_may_assert_beneficial_ownership(source_id)
        for source_id in ("opencorporates", "nz_companies", "sec_edgar", "gleif",
                          "brreg", "ares", "krs_poland", "ted_eu"):
            assert not source_may_assert_beneficial_ownership(source_id), (
                f"{source_id} publishes registered holdings, not BO declarations"
            )


class TestSecReportingCapacity:
    @pytest.mark.parametrize("code", ["IA", "BD", "IC", "BK"])
    def test_custodial_codes_make_no_beneficial_claim(self, code):
        """An investment adviser voting client shares has the SEC's
        beneficial-ownership (voting/dispositive power) without benefiting."""
        assert _sec_beneficial_ownership({"type_code": code}) is None

    def test_natural_person_is_not_custodial(self):
        assert "IN" not in _SEC_CUSTODIAL_REPORTER_CODES
        assert _sec_beneficial_ownership({"type_code": "IN"}) is True

    def test_ordinary_filer_still_asserts(self):
        assert _sec_beneficial_ownership({"type_code": "CO"}) is True

    def test_missing_code_defaults_to_asserting(self):
        assert _sec_beneficial_ownership({}) is True

    def test_lowercase_code_is_handled(self):
        assert _sec_beneficial_ownership({"type_code": "ia"}) is None


def _sec_bundle(type_code: str = "CO", **extra) -> dict:
    reporter = {
        "name": "Example Capital LP",
        "reporter_cik": "0001234567",
        "is_individual": False,
        "percent_of_class": 6.5,
        "type_code": type_code,
        **extra,
    }
    return {
        "issuer_cik": "0000320193",
        "filings": [
            {
                "filed": "2025-02-14",
                "filing_url": "https://www.sec.gov/example",
                "issuer": {"name": "APPLE INC", "cik": "0000320193"},
                "reporter": reporter,
            }
        ],
    }


class TestSecEdgarMapping:
    def _interest(self, bundle):
        rels = [s for s in map_sec_edgar(bundle) if s["recordType"] == "relationship"]
        assert rels
        return rels[0]["recordDetails"]["interests"][0]

    def test_ordinary_filer_asserts_beneficial_ownership(self):
        assert self._interest(_sec_bundle("CO"))["beneficialOwnershipOrControl"] is True

    def test_investment_adviser_makes_no_claim(self):
        interest = self._interest(_sec_bundle("IA"))
        assert "beneficialOwnershipOrControl" not in interest

    def test_custodial_capacity_is_explained_not_just_dropped(self):
        """Silently omitting the flag would lose the reason. Say it."""
        details = self._interest(_sec_bundle("IA")).get("details", "")
        assert "IA" in details and "custodial" in details.lower()

    def test_voting_power_split_is_no_longer_discarded(self):
        interest = self._interest(
            _sec_bundle("CO", sole_voting_power=1000.0, shared_voting_power=500.0)
        )
        details = interest.get("details", "")
        assert "sole voting power" in details
        assert "shared voting power" in details

    def test_share_percentage_is_unaffected(self):
        assert self._interest(_sec_bundle("IA"))["share"] == {"exact": 6.5}


class TestNzCompanies:
    def _bundle(self):
        return {
            "nz_company_number": "1234567",
            "nzbn": "9429000000000",
            "company": {"entityName": "EXAMPLE LIMITED", "entityStatusCode": "50"},
            "roles": [],
            "shareholders": [
                {
                    "kind": "person",
                    "name": "Jane Smith",
                    "percent": 60.0,
                    "shares": 60,
                }
            ],
        }

    def test_being_a_person_is_not_a_bo_declaration(self):
        rels = [
            s for s in map_nz_companies(self._bundle())
            if s["recordType"] == "relationship"
        ]
        assert rels
        for rel in rels:
            for interest in rel["recordDetails"]["interests"]:
                assert "beneficialOwnershipOrControl" not in interest

    def test_the_shareholding_itself_survives(self):
        rels = [
            s for s in map_nz_companies(self._bundle())
            if s["recordType"] == "relationship"
        ]
        assert any(
            i.get("type") == "shareholding"
            for rel in rels
            for i in rel["recordDetails"]["interests"]
        )


class TestNoInferenceCanary:
    """The regression guard: no non-BO source may assert the flag.

    If a future mapper change reintroduces inference, this fails. It walks the
    demo corpus rather than a synthetic fixture so it covers whatever the
    mappers actually emit.
    """

    def test_demo_corpus_has_no_inferred_assertions(self):
        from opencheck.bods.mapper import (
            map_opencorporates,
            map_nz_companies as _nz,
        )

        cases = [
            (
                map_opencorporates,
                {
                    "jurisdiction_code": "gb",
                    "company_number": "00102498",
                    "company": {
                        "name": "EXAMPLE PLC",
                        "jurisdiction_code": "gb",
                        "company_number": "00102498",
                    },
                    "network": {
                        "relationships": [
                            {
                                "relationship_type": "subsidiary",
                                "percentage_min_share_ownership": 75.0,
                                "percentage_max_share_ownership": 100.0,
                                "subject": {
                                    "name": "SUB LTD",
                                    "jurisdiction_code": "gb",
                                    "company_number": "00000001",
                                },
                            }
                        ]
                    },
                },
            ),
            (_nz, TestNzCompanies()._bundle()),
        ]
        for mapper, bundle in cases:
            for stmt in mapper(bundle):
                if stmt["recordType"] != "relationship":
                    continue
                for interest in stmt["recordDetails"].get("interests", []):
                    assert "beneficialOwnershipOrControl" not in interest, (
                        f"{mapper.__name__} asserted beneficial ownership no "
                        f"register declared: {interest}"
                    )
