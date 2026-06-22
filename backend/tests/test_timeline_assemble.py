"""Tests for the Time Machine assembler.

Builds a Morrisons-shaped run (LEI 213800IN6LSRGTZSOS29 / company 00358949)
mixing GLEIF LEI mods, GLEIF RR mods, and Companies House filings, and checks the
merge, period-date attachment, cross-source de-dup, and noise suppression.
"""

from __future__ import annotations

from opencheck.timeline import ChangeType, DateBasis, Tier, assemble_timeline

_LEI = "213800IN6LSRGTZSOS29"
_PARENT = "549300RKU7UEPSC42U63"
_CO = "00358949"
_LEI_PREFIX = "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/"
_RR_PREFIX = "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"


def _lei_mod(field, mtype, old, new, date):
    return {
        "lei": _LEI, "recordType": "LEI", "modificationType": mtype,
        "field": _LEI_PREFIX + field, "date": date, "valueOld": old, "valueNew": new,
    }


def _rr_mod(field, mtype, old, new, date="2023-11-25T00:00:00Z"):
    return {
        "lei": _LEI, "recordType": "RR", "modificationType": mtype,
        "field": _RR_PREFIX + field, "date": date, "valueOld": old, "valueNew": new,
        "context": {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY", "endNode": _PARENT},
    }


def _filing(category, ftype, date, action_date=None):
    item = {"category": category, "type": ftype, "date": date,
            "links": {"self": f"/company/{_CO}/filing-history/{ftype}"}}
    if action_date:
        item["action_date"] = action_date
    return item


def _build():
    gleif_lei_mods = [
        _lei_mod("lei:Entity/lei:LegalName", "UPDATE",
                 "WM MORRISON SUPERMARKETS P L C", "WM MORRISON SUPERMARKETS LIMITED",
                 "2021-12-09T16:00:00Z"),
        _lei_mod("lei:Entity/lei:LegalForm/lei:EntityLegalFormCode", "UPDATE",
                 "B6ES", "H0PO", "2022-01-11T16:00:00Z"),
        # noise:
        _lei_mod("lei:Registration/lei:NextRenewalDate", "UPDATE",
                 "2026-01-11T00:00:00Z", "2027-01-11T00:00:00Z", "2025-11-20T00:00:00Z"),
        _lei_mod("lei:Entity/lei:LegalForm/lei:EntityLegalFormCode", "UPDATE",
                 "8888", "B6ES", "2018-03-03T16:00:00Z"),  # encoding backfill (suppressed)
    ]
    gleif_rr_mods = [
        _rr_mod("rr:Relationship/rr:RelationshipType", "INITIAL", None,
                "IS_DIRECTLY_CONSOLIDATED_BY"),
        _rr_mod("rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/rr:StartDate",
                "INITIAL", None, "2021-11-01T00:00:00Z"),
        _rr_mod("rr:Registration/rr:NextRenewalDate", "UPDATE",
                "2026-01-11T00:00:00Z", "2027-01-11T00:00:00Z"),  # noise
    ]
    ch_filings = [
        _filing("change-of-name", "CONNOT", "2022-01-05", action_date="2021-12-01"),
        _filing("reregistration", "RM01", "2022-01-11"),
        _filing("persons-with-significant-control", "PSC02", "2021-11-15"),
        _filing("confirmation-statement", "CS01", "2022-03-01"),  # noise
    ]
    return assemble_timeline(
        lei=_LEI, company_number=_CO,
        gleif_lei_mods=gleif_lei_mods, gleif_rr_mods=gleif_rr_mods, ch_filings=ch_filings,
    )


def _entries_of(tl, change_type):
    return [e for e in tl.notable if e.change_type is change_type]


def test_raw_events_include_noise():
    tl = _build()
    # 4 LEI mods + 3 RR mods + 4 CH filings = 11 raw events, noise included.
    assert len(tl.events) == 11
    assert any(e.change_type is None for e in tl.events)  # noise present


def test_notable_excludes_noise():
    tl = _build()
    for entry in tl.notable:
        assert entry.tier in (Tier.OWNERSHIP_CONTROL, Tier.IDENTITY_STATUS)
    # NextRenewalDate, CS01, and the 8888->B6ES backfill never appear.
    kinds = {e.change_type for e in tl.notable}
    assert ChangeType.LEGAL_NAME_CHANGE in kinds
    assert ChangeType.LEGAL_FORM_CHANGE in kinds


def test_name_change_is_corroborated_across_sources():
    tl = _build()
    entries = _entries_of(tl, ChangeType.LEGAL_NAME_CHANGE)
    assert len(entries) == 1  # GLEIF + CH merged into one row
    entry = entries[0]
    assert entry.sources == ["companies_house", "gleif"]
    # Companies House (effective) wins the displayed date over GLEIF (recorded).
    assert entry.date_basis is DateBasis.EFFECTIVE
    assert entry.date == "2021-12-01"
    assert len(entry.corroborating) == 1


def test_legal_form_change_merged_and_form_backfill_suppressed():
    tl = _build()
    entries = _entries_of(tl, ChangeType.LEGAL_FORM_CHANGE)
    assert len(entries) == 1
    assert set(entries[0].sources) == {"companies_house", "gleif"}


def test_gleif_owner_added_carries_period_dates_and_parent():
    tl = _build()
    owners = [e for e in _entries_of(tl, ChangeType.OWNER_ADDED)
              if e.primary.source_id == "gleif"]
    assert len(owners) == 1
    entry = owners[0]
    # Economic start from the relationship period, NOT the 2023-11-25 publish date.
    assert entry.interest_start_date == "2021-11-01"
    assert entry.counterparty == _PARENT


def test_ownership_changes_not_merged_across_sources():
    tl = _build()
    # GLEIF corporate parent add and CH PSC add are distinct ownership entries.
    owner_added = _entries_of(tl, ChangeType.OWNER_ADDED)
    sources = {e.primary.source_id for e in owner_added}
    assert sources == {"gleif", "companies_house"}
    assert len(owner_added) == 2


def test_notable_is_sorted_oldest_first():
    tl = _build()
    dated = [e.date for e in tl.notable if e.date]
    assert dated == sorted(dated)
