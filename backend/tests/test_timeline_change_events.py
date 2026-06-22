"""Tests for the Time Machine ChangeEvent model + GLEIF emitter.

Fixtures are real GLEIF field-modification shapes for WM MORRISON SUPERMARKETS
LIMITED (LEI 213800IN6LSRGTZSOS29) — the 2021 Clayton, Dubilier & Rice
take-private — the worked example in docs/time-machine.md. No network calls.
"""

from __future__ import annotations

from opencheck.timeline import (
    CHANGE_TYPES,
    ChangeType,
    DateBasis,
    RecordType,
    Tier,
    classify_gleif_modification,
    relationship_interest_dates,
)

_LEI_PREFIX = "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/"
_RR_PREFIX = "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"


def _lei_mod(field: str, mtype: str, old, new, date="2021-12-09T16:00:00Z") -> dict:
    return {
        "lei": "213800IN6LSRGTZSOS29",
        "recordType": "LEI",
        "modificationType": mtype,
        "field": _LEI_PREFIX + field,
        "date": date,
        "valueOld": old,
        "valueNew": new,
    }


def _rr_mod(field: str, mtype: str, old, new, date="2023-11-25T00:00:00Z") -> dict:
    return {
        "lei": "213800IN6LSRGTZSOS29",
        "recordType": "RR",
        "modificationType": mtype,
        "field": _RR_PREFIX + field,
        "date": date,
        "valueOld": old,
        "valueNew": new,
    }


# ---------------------------------------------------------------------------
# Codelist integrity
# ---------------------------------------------------------------------------

def test_every_change_type_has_a_spec():
    for ct in ChangeType:
        assert ct in CHANGE_TYPES
        spec = CHANGE_TYPES[ct]
        assert spec.change_type is ct
        assert spec.tier in (Tier.OWNERSHIP_CONTROL, Tier.IDENTITY_STATUS)
        assert spec.bods_record_status in ("new", "updated", "closed")


# ---------------------------------------------------------------------------
# Notable entity changes (Tier 2)
# ---------------------------------------------------------------------------

def test_legal_name_change_is_notable():
    ev = classify_gleif_modification(
        _lei_mod("lei:Entity/lei:LegalName", "UPDATE",
                 "WM MORRISON SUPERMARKETS P L C",
                 "WM MORRISON SUPERMARKETS LIMITED")
    )
    assert ev.change_type is ChangeType.LEGAL_NAME_CHANGE
    assert ev.tier is Tier.IDENTITY_STATUS
    assert ev.record_type is RecordType.ENTITY
    assert ev.is_notable
    assert ev.event_date == "2021-12-09"
    assert ev.date_basis is DateBasis.RECORDED
    # Raw is always preserved.
    assert ev.value_old == "WM MORRISON SUPERMARKETS P L C"
    assert ev.source_id == "gleif"


def test_real_legal_form_class_change_is_notable():
    # B6ES (PLC) -> H0PO (private limited): a genuine class change.
    ev = classify_gleif_modification(
        _lei_mod("lei:Entity/lei:LegalForm/lei:EntityLegalFormCode", "UPDATE",
                 "B6ES", "H0PO", date="2022-01-11T16:00:00Z")
    )
    assert ev.change_type is ChangeType.LEGAL_FORM_CHANGE
    assert ev.tier is Tier.IDENTITY_STATUS


def test_registration_retired_is_notable():
    ev = classify_gleif_modification(
        _lei_mod("lei:Registration/lei:RegistrationStatus", "UPDATE",
                 "ISSUED", "RETIRED")
    )
    assert ev.change_type is ChangeType.REGISTRATION_RETIRED
    assert ev.tier is Tier.IDENTITY_STATUS


def test_entity_status_change_is_notable():
    ev = classify_gleif_modification(
        _lei_mod("lei:Entity/lei:EntityStatus", "UPDATE", "ACTIVE", "INACTIVE")
    )
    assert ev.change_type is ChangeType.STATUS_CHANGED
    assert ev.tier is Tier.IDENTITY_STATUS


# ---------------------------------------------------------------------------
# Guard 1 — legal-form encoding backfills are NOT real changes (suppressed)
# ---------------------------------------------------------------------------

def test_legal_form_encoding_backfill_is_suppressed():
    # 8888 (placeholder) -> B6ES (PLC): still PLC, just better encoded.
    ev = classify_gleif_modification(
        _lei_mod("lei:Entity/lei:LegalForm/lei:EntityLegalFormCode", "UPDATE",
                 "8888", "B6ES", date="2018-03-03T16:00:00Z")
    )
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE
    assert not ev.is_notable
    # ...but the raw change is still carried.
    assert ev.value_old == "8888"
    assert ev.value_new == "B6ES"


# ---------------------------------------------------------------------------
# Noise (Tier 3) — kept but suppressed by default
# ---------------------------------------------------------------------------

def test_next_renewal_date_is_noise():
    ev = classify_gleif_modification(
        _lei_mod("lei:Registration/lei:NextRenewalDate", "UPDATE",
                 "2026-01-11T00:00:00Z", "2027-01-11T00:00:00Z")
    )
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


def test_lapsed_issued_cycle_is_noise_not_retirement():
    ev = classify_gleif_modification(
        _lei_mod("lei:Registration/lei:RegistrationStatus", "UPDATE",
                 "LAPSED", "ISSUED")
    )
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


def test_address_region_recode_is_noise():
    ev = classify_gleif_modification(
        _lei_mod("lei:Entity/lei:HeadquartersAddress/lei:Region", "UPDATE",
                 "GB-UKM", "GB-ENG")
    )
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


def test_initial_entity_fields_are_not_changes():
    ev = classify_gleif_modification(
        _lei_mod("lei:Entity/lei:LegalName", "INITIAL", None,
                 "WM MORRISON SUPERMARKETS P L C", date="2018-02-08T14:15:08Z")
    )
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


# ---------------------------------------------------------------------------
# Relationship (ownership) changes — Tier 1
# ---------------------------------------------------------------------------

def test_relationship_added_is_tier1_owner_added():
    ev = classify_gleif_modification(
        _rr_mod("rr:Relationship/rr:RelationshipType", "INITIAL", None,
                "IS_DIRECTLY_CONSOLIDATED_BY")
    )
    assert ev.change_type is ChangeType.OWNER_ADDED
    assert ev.tier is Tier.OWNERSHIP_CONTROL
    assert ev.record_type is RecordType.RELATIONSHIP


def test_relationship_inactive_is_owner_removed():
    ev = classify_gleif_modification(
        _rr_mod("rr:Relationship/rr:RelationshipStatus", "UPDATE",
                "ACTIVE", "INACTIVE")
    )
    assert ev.change_type is ChangeType.OWNER_REMOVED
    assert ev.tier is Tier.OWNERSHIP_CONTROL


def test_relationship_validation_reference_is_noise():
    # The ValidationReference is a Companies House filing URL — provenance, not
    # an ownership change.
    ev = classify_gleif_modification(
        _rr_mod("rr:Registration/rr:ValidationReference", "UPDATE",
                "https://...old", "https://...new")
    )
    assert ev.change_type is None
    assert ev.tier is Tier.ADMIN_NOISE


# ---------------------------------------------------------------------------
# Guard 2 — interest dates come from the period, not the modification date
# ---------------------------------------------------------------------------

def test_relationship_interest_dates_use_period_not_publish_date():
    # The Market Bidco parent was PUBLISHED on 2023-11-25, but the relationship
    # period shows consolidation began 2021-11-01. The interest start must be the
    # economic date, not the publish date.
    mods = [
        _rr_mod("rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/rr:StartDate",
                "INITIAL", None, "2021-11-01T00:00:00Z"),
        _rr_mod("rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/rr:EndDate",
                "INITIAL", None, "2022-10-30T00:00:00Z"),
        _rr_mod("rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/rr:StartDate",
                "INITIAL", None, "2023-11-24T00:00:00Z"),
    ]
    start, end = relationship_interest_dates(mods)
    assert start == "2021-11-01"
    assert end == "2022-10-30"
