"""Tests for the New Zealand Time Machine emitter (the third emitter).

NZ reconstructs change events from dated current-and-historic records, with real
effective dates. No network — fed a FullEntity + history dicts directly.
"""

from __future__ import annotations

from opencheck.timeline import (
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
    assemble_timeline,
    nz_change_events,
)
from opencheck.timeline.model import ChangeEvent

_DATA = {
    "company_number": "1166320",
    "nzbn": "9429000035170",
    "full": {
        "company-details": {
            "shareholding": {
                "numberOfShares": 100,
                "shareAllocation": [
                    {"allocation": 60, "shareholder": [{
                        "appointmentDate": "2010-05-01T00:00:00Z",
                        "individualShareholder": {"fullName": "Jane Smith"},
                    }]},
                ],
                "historicShareholder": [
                    {"appointmentDate": "2000-01-01T00:00:00Z",
                     "vacationDate": "2009-12-31T00:00:00Z",
                     "historicIndividualShareholder": {"fullName": "Old Holder"}},
                ],
            },
        },
        "roles": [
            {"roleType": "Director", "startDate": "2015-03-01T00:00:00Z",
             "endDate": "2020-06-30T00:00:00Z",
             "rolePerson": {"firstName": "John", "lastName": "Doe"}},
        ],
    },
    "name_history": [
        {"entityName": "OLD NAME LIMITED", "startDate": "1990-01-01", "endDate": "2005-06-01"},
        {"entityName": "FONTERRA CO-OPERATIVE GROUP LIMITED", "startDate": "2005-06-01"},
    ],
    "status_history": [
        {"entityStatusDescription": "Registered", "startDate": "1990-01-01"},
    ],
    "address_history": [
        {"address1": "1 Old St", "postCode": "1000", "addressType": "REGISTERED",
         "startDate": "1990-01-01", "endDate": "2010-01-01"},
        {"address1": "109 Fanshawe St", "postCode": "1010", "addressType": "REGISTERED",
         "startDate": "2010-01-01"},
    ],
}


def _by(events, change_type):
    return [e for e in events if e.change_type == change_type]


def test_emitter_reconstructs_dated_events():
    events = nz_change_events(_DATA)

    added = _by(events, ChangeType.OWNER_ADDED)
    removed = _by(events, ChangeType.OWNER_REMOVED)
    assert len(added) == 3      # Jane (current), Old Holder (historic), John (director)
    assert len(removed) == 2    # Old Holder vacated, John resigned

    # Effective dates, high confidence — NZ's distinguishing trait.
    assert all(e.date_basis is DateBasis.EFFECTIVE for e in events)
    assert all(e.date_confidence is DateConfidence.HIGH for e in events)

    jane = next(e for e in added if "Jane Smith" in (e.counterparty or ""))
    assert jane.record_type is RecordType.RELATIONSHIP
    assert jane.tier is Tier.OWNERSHIP_CONTROL
    assert jane.interest_start_date == "2010-05-01"
    assert "(60.0%)" in jane.counterparty  # current shareholder carries %
    assert jane.subject_id == "1166320"

    director = next(e for e in added if "director" in (e.counterparty or ""))
    assert "John Doe" in director.counterparty
    assert director.event_date == "2015-03-01"

    old = next(e for e in removed if "Old Holder" in (e.counterparty or ""))
    assert old.event_date == "2009-12-31"


def test_identity_history_transitions():
    events = nz_change_events(_DATA)
    names = _by(events, ChangeType.LEGAL_NAME_CHANGE)
    assert len(names) == 1  # earliest is the original, one change after
    assert names[0].value_old == "OLD NAME LIMITED"
    assert "FONTERRA" in names[0].value_new
    assert names[0].event_date == "2005-06-01"
    assert names[0].record_type is RecordType.ENTITY

    # Single status row = original only, no change.
    assert _by(events, ChangeType.STATUS_CHANGED) == []

    addrs = _by(events, ChangeType.ADDRESS_CHANGE)
    assert len(addrs) == 1
    assert "1 Old St" in addrs[0].value_old and "109 Fanshawe St" in addrs[0].value_new
    assert addrs[0].event_date == "2010-01-01"


def test_empty_data():
    assert nz_change_events({}) == []


def test_assembler_accepts_extra_events():
    ev = ChangeEvent(
        source_id="nz_companies", subject_id="1166320",
        record_type=RecordType.RELATIONSHIP, raw_change_type="record",
        change_type=ChangeType.OWNER_ADDED, tier=Tier.OWNERSHIP_CONTROL,
        counterparty="John Doe — director", event_date="2015-03-01",
        interest_start_date="2015-03-01",
        date_basis=DateBasis.EFFECTIVE, date_confidence=DateConfidence.HIGH,
    )
    tl = assemble_timeline(lei="X", extra_events=[ev])
    assert len(tl.notable) == 1
    entry = tl.notable[0]
    assert entry.change_type is ChangeType.OWNER_ADDED
    assert entry.sources == ["nz_companies"]
    assert entry.date == "2015-03-01"
