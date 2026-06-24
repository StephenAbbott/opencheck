"""Tests for the Denmark (CVR) Time Machine emitter.

CVR is bitemporal — the adapter already returns every virkning period in the
bundle's _raw_* lists, so cvr_change_events reconstructs effective-dated change
events with no extra API calls. Modelled on the real Novo Nordisk history
(Novo Industri A/S → Novo Nordisk A/S, 1989).
"""

from __future__ import annotations

from opencheck.timeline.cvr_denmark import cvr_change_events
from opencheck.timeline.model import ChangeType, DateBasis, DateConfidence, Tier


def _bundle() -> dict:
    return {
        "cvr_number": "24256790",
        "_raw_virksomhed": [
            {"status": "AKTIV", "virkningFra": "1989-09-14T00:00:00.000Z",
             "virkningTil": "2020-01-01T00:00:00.000Z"},
            {"status": "UNDER_KONKURS", "virkningFra": "2020-01-01T00:00:00.000Z",
             "virkningTil": None},
        ],
        "_raw_navn": [
            {"vaerdi": "Novo Industri A/S", "sekvens": 0,
             "virkningFra": "1974-01-01T00:00:00.000Z", "virkningTil": "1989-09-13T23:59:59.000Z"},
            {"vaerdi": "Novo Nordisk A/S", "sekvens": 0,
             "virkningFra": "1989-09-14T00:00:00.000Z", "virkningTil": None},
            # secondary name (sekvens 1) must be ignored — not the legal name
            {"vaerdi": "Novo Trading", "sekvens": 1,
             "virkningFra": "2000-01-01T00:00:00.000Z", "virkningTil": None},
        ],
        "_raw_form": [
            {"vaerdiTekst": "Anpartsselskab",
             "virkningFra": "1974-01-01T00:00:00.000Z", "virkningTil": "1989-09-13T23:59:59.000Z"},
            {"vaerdiTekst": "Aktieselskab",
             "virkningFra": "1989-09-14T00:00:00.000Z", "virkningTil": None},
        ],
        "_raw_adressering": [
            {"AdresseringAnvendelse": "beliggenhedsadresse", "CVRAdresse_vejnavn": "Old Road",
             "CVRAdresse_postnummer": "2880", "CVRAdresse_postdistrikt": "Bagsværd",
             "virkningFra": "1974-01-01T00:00:00.000Z", "virkningTil": "2005-01-01T00:00:00.000Z"},
            {"AdresseringAnvendelse": "beliggenhedsadresse", "CVRAdresse_vejnavn": "Novo Allé",
             "CVRAdresse_postnummer": "2880", "CVRAdresse_postdistrikt": "Bagsværd",
             "virkningFra": "2005-01-01T00:00:00.000Z", "virkningTil": None},
        ],
        "_raw_branche": [
            {"vaerdi": "212000", "sekvens": 0,
             "virkningFra": "1974-01-01T00:00:00.000Z", "virkningTil": "2007-01-01T00:00:00.000Z"},
            {"vaerdi": "211000", "sekvens": 0,
             "virkningFra": "2007-01-01T00:00:00.000Z", "virkningTil": None},
        ],
    }


def _by_type(events):
    out: dict = {}
    for e in events:
        out.setdefault(e.change_type, []).append(e)
    return out


def test_name_change_is_effective_dated_and_ignores_secondary_names():
    events = cvr_change_events(_bundle())
    names = _by_type(events).get(ChangeType.LEGAL_NAME_CHANGE, [])
    assert len(names) == 1                       # secondary (sekvens 1) ignored
    e = names[0]
    assert e.subject_id == "24256790"
    assert e.value_old == "Novo Industri A/S"
    assert e.value_new == "Novo Nordisk A/S"
    assert e.event_date == "1989-09-14"
    assert e.date_basis is DateBasis.EFFECTIVE
    assert e.date_confidence is DateConfidence.HIGH
    assert e.tier is Tier.IDENTITY_STATUS


def test_form_status_and_address_changes():
    by = _by_type(cvr_change_events(_bundle()))
    form = by[ChangeType.LEGAL_FORM_CHANGE][0]
    assert (form.value_old, form.value_new) == ("Anpartsselskab", "Aktieselskab")
    status = by[ChangeType.STATUS_CHANGED][0]
    assert (status.value_old, status.value_new) == ("active", "in bankruptcy")
    assert status.event_date == "2020-01-01"
    addr = by[ChangeType.ADDRESS_CHANGE][0]
    assert addr.event_date == "2005-01-01" and "Novo Allé" in addr.value_new


def test_industry_change_is_admin_noise():
    # branche recodes are kept raw-first but unmapped Tier-3 (hidden by default).
    branche = [e for e in cvr_change_events(_bundle()) if e.raw_change_type == "branche"]
    assert len(branche) == 1
    assert branche[0].change_type is None
    assert branche[0].tier is Tier.ADMIN_NOISE
    assert branche[0].is_notable is False


def test_noop_reregistration_is_skipped():
    # A duplicate name period with the same value must NOT create a change event.
    b = _bundle()
    b["_raw_navn"].append({
        "vaerdi": "Novo Nordisk A/S", "sekvens": 0,
        "virkningFra": "2010-01-01T00:00:00.000Z", "virkningTil": None,
    })
    names = [e for e in cvr_change_events(b) if e.change_type == ChangeType.LEGAL_NAME_CHANGE]
    assert len(names) == 1   # still just the 1989 rename


def test_empty_bundle_is_safe():
    assert cvr_change_events({}) == []
    assert cvr_change_events({"cvr_number": "1", "_raw_navn": []}) == []
