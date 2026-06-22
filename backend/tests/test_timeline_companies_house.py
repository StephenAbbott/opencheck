"""Tests for the Companies House Time Machine emitter.

Filing-history shapes mirror real Companies House categories/types, anchored on
Wm Morrison Supermarkets (company 00358949) — the same worked example as the
GLEIF side, so the two emitters can be checked against one entity. No network.
"""

from __future__ import annotations

from opencheck.timeline import (
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
    classify_companies_house_filing,
)


def _filing(category: str, ftype: str = "", *, date="2021-11-02",
            action_date=None, txn="MzEx") -> dict:
    item: dict = {
        "category": category,
        "type": ftype,
        "date": date,
        "links": {"self": f"/company/00358949/filing-history/{txn}"},
    }
    if action_date:
        item["action_date"] = action_date
    return item


# ---------------------------------------------------------------------------
# PSC (relationship / ownership) events — Tier 1
# ---------------------------------------------------------------------------

def test_psc_notification_is_owner_added():
    ev = classify_companies_house_filing(
        _filing("persons-with-significant-control", "PSC02"),
        company_id="00358949",
    )
    assert ev.change_type is ChangeType.OWNER_ADDED
    assert ev.tier is Tier.OWNERSHIP_CONTROL
    assert ev.record_type is RecordType.RELATIONSHIP
    assert ev.source_id == "companies_house"
    assert ev.subject_id == "00358949"


def test_psc_cessation_is_owner_removed():
    ev = classify_companies_house_filing(
        _filing("persons-with-significant-control", "PSC07"),
    )
    assert ev.change_type is ChangeType.OWNER_REMOVED
    assert ev.tier is Tier.OWNERSHIP_CONTROL


def test_psc_statement_is_reporting_exception():
    ev = classify_companies_house_filing(
        _filing("persons-with-significant-control", "PSC08"),
    )
    assert ev.change_type is ChangeType.REPORTING_EXCEPTION_CHANGED
    assert ev.tier is Tier.OWNERSHIP_CONTROL


def test_unknown_psc_type_still_surfaces_as_control_change():
    ev = classify_companies_house_filing(
        _filing("persons-with-significant-control", "PSC99"),
    )
    assert ev.change_type is ChangeType.CONTROL_NATURE_CHANGED
    assert ev.tier is Tier.OWNERSHIP_CONTROL


# ---------------------------------------------------------------------------
# Entity identity / status events — Tier 2
# ---------------------------------------------------------------------------

def test_change_of_name_is_legal_name_change():
    ev = classify_companies_house_filing(_filing("change-of-name", "CONNOT"))
    assert ev.change_type is ChangeType.LEGAL_NAME_CHANGE
    assert ev.tier is Tier.IDENTITY_STATUS
    assert ev.record_type is RecordType.ENTITY


def test_reregistration_is_legal_form_change():
    # Morrisons re-registered PLC -> Ltd as part of the 2021 take-private.
    ev = classify_companies_house_filing(_filing("reregistration", "RM01"))
    assert ev.change_type is ChangeType.LEGAL_FORM_CHANGE
    assert ev.tier is Tier.IDENTITY_STATUS


def test_address_change_is_notable():
    ev = classify_companies_house_filing(_filing("address", "AD01"))
    assert ev.change_type is ChangeType.ADDRESS_CHANGE
    assert ev.tier is Tier.IDENTITY_STATUS


def test_dissolution_is_status_change():
    ev = classify_companies_house_filing(_filing("dissolution", "GAZ2"))
    assert ev.change_type is ChangeType.STATUS_CHANGED
    assert ev.tier is Tier.IDENTITY_STATUS


# ---------------------------------------------------------------------------
# Noise — kept, suppressed by default (Tier 3)
# ---------------------------------------------------------------------------

def test_confirmation_statement_is_noise():
    # The canonical CS01 "confirmed, no change" — the GLEIF NextRenewalDate twin.
    ev = classify_companies_house_filing(_filing("confirmation-statement", "CS01"))
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE
    assert not ev.is_notable


def test_officer_appointment_is_suppressed_by_default():
    ev = classify_companies_house_filing(_filing("officers", "AP01"))
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


def test_accounts_filing_is_noise():
    ev = classify_companies_house_filing(_filing("accounts", "AA"))
    assert ev.tier is Tier.ADMIN_NOISE


# ---------------------------------------------------------------------------
# Dates — CH is effective-basis, high confidence; prefers action_date
# ---------------------------------------------------------------------------

def test_dates_are_effective_high_confidence_and_prefer_action_date():
    ev = classify_companies_house_filing(
        _filing("persons-with-significant-control", "PSC07",
                date="2021-11-02", action_date="2021-10-29"),
    )
    assert ev.event_date == "2021-10-29"  # action_date wins over filing date
    assert ev.date_basis is DateBasis.EFFECTIVE
    assert ev.date_confidence is DateConfidence.HIGH
    assert ev.raw_payload_ref == "/company/00358949/filing-history/MzEx"  # self link
