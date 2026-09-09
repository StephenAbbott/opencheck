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


# ---------------------------------------------------------------------------
# Officer filings — Tier 4, a stream of their own (Phase 194)
#
# 568 of Lloyds Bank PLC's 2,404 filings are officer filings. They were
# arriving untyped and unnamed in the administrative stream; the register
# files a discrete form per event and names the person in
# ``description_values.officer_name``.
# ---------------------------------------------------------------------------


def _officer_filing(ftype: str, name: str | None = "Mr Kelly Brian Bennett") -> dict:
    item = _filing("officers", ftype)
    if name:
        item["description_values"] = {"officer_name": name}
    return item


def test_officer_appointment_is_typed_and_named():
    ev = classify_companies_house_filing(_officer_filing("AP01"))
    assert ev.change_type is ChangeType.OFFICER_APPOINTED
    assert ev.tier is Tier.BOARD_CHANGE
    assert ev.record_type is RecordType.RELATIONSHIP
    assert ev.counterparty == "Mr Kelly Brian Bennett"
    assert ev.raw_field == "officers/director"


def test_officer_appointment_still_does_not_render_by_default():
    """A board change is not a beneficial-ownership change. It has its own
    stream precisely so it cannot crowd out the rows that are."""
    assert not classify_companies_house_filing(_officer_filing("AP01")).is_notable


def test_officer_termination_and_details_change_are_distinct():
    assert (
        classify_companies_house_filing(_officer_filing("TM01")).change_type
        is ChangeType.OFFICER_RESIGNED
    )
    details = classify_companies_house_filing(_officer_filing("CH01"))
    assert details.change_type is ChangeType.OFFICER_DETAILS_CHANGED


def test_a_details_change_is_not_board_turnover():
    """Nobody joined or left: a director's own address changing is
    administrative. 322 of Lloyds Bank PLC's 568 officer filings are these,
    and in the board stream they would bury the 246 that are turnover."""
    ev = classify_companies_house_filing(_officer_filing("CH01"))
    assert ev.tier is Tier.ADMIN_NOISE


def test_secretary_filings_say_so():
    ev = classify_companies_house_filing(_officer_filing("AP03"))
    assert ev.change_type is ChangeType.OFFICER_APPOINTED
    assert ev.raw_field == "officers/secretary"


def test_pre_2009_forms_are_the_same_three_events():
    """288a/288b/288c are what make a deep board history possible: a company
    incorporated in 1865 filed nothing else for most of its life."""
    assert (
        classify_companies_house_filing(_officer_filing("288a")).change_type
        is ChangeType.OFFICER_APPOINTED
    )
    assert (
        classify_companies_house_filing(_officer_filing("288b")).change_type
        is ChangeType.OFFICER_RESIGNED
    )
    assert (
        classify_companies_house_filing(_officer_filing("288c")).change_type
        is ChangeType.OFFICER_DETAILS_CHANGED
    )


def test_replacement_filing_is_read_by_the_form_it_replaces():
    """Live shape from Lloyds Bank PLC: `RP01AP01`, a replacement filing of a
    director appointment."""
    ev = classify_companies_house_filing(_officer_filing("RP01AP01"))
    assert ev.change_type is ChangeType.OFFICER_APPOINTED


def test_a_bare_288_is_read_from_the_registers_own_words():
    """Pre-2009 the form is a bare `288` and only the description says what
    happened. These four strings are Lloyds Bank PLC's, verbatim, and cover
    239 of its 568 officer filings."""
    def legacy(description: str) -> dict:
        item = _filing("officers", "288")
        item["description_values"] = {"description": description}
        return item

    assert (
        classify_companies_house_filing(legacy("New director appointed")).change_type
        is ChangeType.OFFICER_APPOINTED
    )
    assert (
        classify_companies_house_filing(legacy("Director resigned")).change_type
        is ChangeType.OFFICER_RESIGNED
    )
    assert (
        classify_companies_house_filing(legacy("Director's particulars changed")).change_type
        is ChangeType.OFFICER_DETAILS_CHANGED
    )
    secretary = classify_companies_house_filing(legacy("New secretary appointed"))
    assert secretary.change_type is ChangeType.OFFICER_APPOINTED
    assert secretary.raw_field == "officers/secretary"


def test_a_termination_is_not_read_as_an_appointment():
    """"Appointment terminated director maurice blank" contains the word
    "appointment". The ending is tested before the beginning for this reason."""
    item = _filing("officers", "288b")
    item["description_values"] = {"description": "Appointment terminated director maurice blank"}
    assert (
        classify_companies_house_filing(item).change_type is ChangeType.OFFICER_RESIGNED
    )


def test_an_unknown_officer_form_stays_untyped_but_keeps_its_stream():
    """A board filing whose kind we cannot name is still a board filing.
    Guessing "details changed" would be inventing a fact."""
    ev = classify_companies_house_filing(_officer_filing("ZZ99"))
    assert ev.change_type is None
    # ... and it is not called turnover, because nothing said it was.
    assert ev.tier is Tier.ADMIN_NOISE
    assert ev.counterparty == "Mr Kelly Brian Bennett"


def test_an_officer_filing_with_no_name_names_nobody():
    ev = classify_companies_house_filing(_officer_filing("AP01", name=None))
    assert ev.change_type is ChangeType.OFFICER_APPOINTED
    assert ev.counterparty is None


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
