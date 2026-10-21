"""Board or council members the e-Business Register names on its
beneficial-owner list because no owner was identified (manner-of-control
codes J / C) are not beneficial owners (Stephen, 20 Oct 2026): their
interest is a ``seniorManagingOfficial`` with
``beneficialOwnershipOrControl: false``, and the finding does not count
them as beneficial owners. A company obliged to file with none in the open
data reads "none published", never "none filed" — an entry can be
restricted for a person at risk or a minor."""

from __future__ import annotations

import pytest

from opencheck.bods.bo_regimes import boc_policy
from opencheck.bods.mappers.estonia import (
    EE_BO_FALLBACK_CODES,
    _ee_bo_interest,
    ee_bo_is_fallback,
    ee_person_role_label,
)
from opencheck.findings import finding_ariregister


def _row(code: str) -> dict:
    return {"kirje_id": "1", "kontrolli_teostamise_viis": code, "algus_kpv": "2020-01-01"}


def test_the_fallback_codes_are_j_and_c() -> None:
    assert EE_BO_FALLBACK_CODES == frozenset({"J", "C"})
    assert boc_policy("ariregister", "kasusaaja_fallback") == "assert_false"
    assert boc_policy("ariregister", "kasusaaja_bo") == "assert_true"


@pytest.mark.parametrize("code", ["J", "C"])
def test_a_fallback_official_is_not_a_beneficial_owner(code: str) -> None:
    interest = _ee_bo_interest(_row(code))
    assert interest["type"] == "seniorManagingOfficial"
    assert interest["beneficialOwnershipOrControl"] is False
    assert ee_bo_is_fallback(_row(code))


@pytest.mark.parametrize("code", ["O", "K", "H", "X", "Y", "Z", "S", "F", "M"])
def test_every_other_code_is_still_a_beneficial_owner(code: str) -> None:
    assert _ee_bo_interest(_row(code))["beneficialOwnershipOrControl"] is True
    assert not ee_bo_is_fallback(_row(code))


def test_the_role_label_says_why_a_fallback_official_is_listed() -> None:
    assert ee_person_role_label("beneficial_owners", "J").startswith(
        "Named because no beneficial owner was identified"
    )
    assert ee_person_role_label("beneficial_owners", "O").startswith("Beneficial owner")


def _bundle(**kw) -> dict:
    base = {"status": "Registered", "registration_date": "2010-01-01", "legal_form": "Private limited company",
            "shareholders": [{}], "officers": [{}], "beneficial_owners": []}
    base.update(kw)
    return base


def test_the_finding_does_not_count_fallback_officials_as_owners() -> None:
    finding = finding_ariregister(_bundle(beneficial_owners=[_row("J"), _row("C")]))
    assert "no owner identified, 2 named" in finding
    assert "beneficial owner" not in finding.replace("no owner identified", "")


def test_the_finding_counts_only_the_owners_when_both_are_listed() -> None:
    finding = finding_ariregister(_bundle(beneficial_owners=[_row("O"), _row("J")]))
    assert "1 beneficial owner" in finding and "no owner identified" not in finding


def test_none_in_the_open_data_reads_published_not_filed() -> None:
    finding = finding_ariregister(_bundle(general={"bo_obliged": True}))
    assert "beneficial owners due, none published" in finding
    assert "none filed" not in finding
