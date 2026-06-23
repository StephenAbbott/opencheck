"""Tests for the Estonian (e-Äriregister) Time Machine emitter.

The emitter parses the documented RIK SOAP responses (detailandmed_v2 +
tegelikudKasusaajad_v2) into dated change events. No network — fed XML strings
modelled on RIK's published sample responses.
"""

from __future__ import annotations

from opencheck.timeline import (
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
    ariregister_change_events,
    assemble_timeline,
)

# detailandmed_v2 response: name change, address change, legal-form change, a
# single (unchanged) status, a current board member, and an ended OSAN
# shareholder. Namespaced like the real response so the emitter's namespace
# stripping is exercised.
_DETAIL_XML = """<?xml version="1.0"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
 <SOAP-ENV:Body>
  <detailandmed_v2Response xmlns="http://arireg.x-road.eu/producer/">
   <keha>
    <ettevotjad>
     <item>
      <ariregistri_kood>10584597</ariregistri_kood>
      <nimi>NEW NAME OÜ</nimi>
      <yldandmed>
       <arinimed>
        <item><sisu>OLD NAME OÜ</sisu><algus_kpv>2010-01-01Z</algus_kpv></item>
        <item><sisu>NEW NAME OÜ</sisu><algus_kpv>2018-05-13Z</algus_kpv></item>
       </arinimed>
       <aadressid>
        <item><aadress_ads__ads_normaliseeritud_taisaadress>Old St 1, Tallinn</aadress_ads__ads_normaliseeritud_taisaadress><algus_kpv>2010-01-01Z</algus_kpv></item>
        <item><aadress_ads__ads_normaliseeritud_taisaadress>New St 2, Tallinn</aadress_ads__ads_normaliseeritud_taisaadress><algus_kpv>2017-12-13Z</algus_kpv><lopp_kpv/></item>
       </aadressid>
       <juhatuse_asukoha_aadressid/>
       <oiguslikud_vormid>
        <item><sisu>AS</sisu><sisu_tekstina>Aktsiaselts</sisu_tekstina><algus_kpv>2010-01-01Z</algus_kpv></item>
        <item><sisu>OU</sisu><sisu_tekstina>Osaühing</sisu_tekstina><algus_kpv>2014-03-03Z</algus_kpv></item>
       </oiguslikud_vormid>
       <staatused>
        <item><staatus>R</staatus><staatus_tekstina>Registered</staatus_tekstina><algus_kpv>2010-01-01Z</algus_kpv></item>
       </staatused>
      </yldandmed>
      <isikuandmed>
       <kaardile_kantud_isikud>
        <item><isiku_tyyp>F</isiku_tyyp><isiku_roll>JUHL</isiku_roll><isiku_roll_tekstina>Management board member</isiku_roll_tekstina><eesnimi>Markus</eesnimi><nimi_arinimi>Villig</nimi_arinimi><algus_kpv>2013-02-07Z</algus_kpv></item>
        <item><isiku_tyyp>F</isiku_tyyp><isiku_roll>OSAN</isiku_roll><isiku_roll_tekstina>Shareholder</isiku_roll_tekstina><eesnimi>Old</eesnimi><nimi_arinimi>Owner</nimi_arinimi><algus_kpv>2010-01-01Z</algus_kpv><lopp_kpv>2016-08-06Z</lopp_kpv></item>
       </kaardile_kantud_isikud>
       <kaardivalised_isikud/>
      </isikuandmed>
     </item>
    </ettevotjad>
    <leitud_ettevotjate_arv>1</leitud_ettevotjate_arv>
   </keha>
  </detailandmed_v2Response>
 </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"""

# tegelikudKasusaajad_v2 response: one current BO (start only) + one ended BO.
_BO_XML = """<?xml version="1.0"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
 <SOAP-ENV:Body>
  <tegelikudKasusaajad_v2Response xmlns="http://arireg.x-road.eu/producer/">
   <keha>
    <kasusaajad>
     <kasusaajate_arv_kokku>2</kasusaajate_arv_kokku>
     <kasusaaja>
      <eesnimi>Edith</eesnimi><nimi>Rik</nimi>
      <kontrolli_teostamise_viis>J</kontrolli_teostamise_viis>
      <kontrolli_teostamise_viis_tekstina>member of the highest management body</kontrolli_teostamise_viis_tekstina>
      <algus_kpv>2022-01-31Z</algus_kpv>
     </kasusaaja>
     <kasusaaja>
      <eesnimi>Past</eesnimi><nimi>Beneficiary</nimi>
      <kontrolli_teostamise_viis_tekstina>direct ownership</kontrolli_teostamise_viis_tekstina>
      <algus_kpv>2018-01-01Z</algus_kpv><lopp_kpv>2021-12-31Z</lopp_kpv>
     </kasusaaja>
    </kasusaajad>
   </keha>
  </tegelikudKasusaajad_v2Response>
 </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"""

_DATA = {"registry_code": "10584597", "detail_xml": _DETAIL_XML, "bo_xml": _BO_XML}


def _by(events, change_type):
    return [e for e in events if e.change_type == change_type]


def test_identity_history_transitions():
    events = ariregister_change_events(_DATA)

    names = _by(events, ChangeType.LEGAL_NAME_CHANGE)
    assert len(names) == 1  # earliest is the original; one change after
    assert names[0].value_old == "OLD NAME OÜ"
    assert names[0].value_new == "NEW NAME OÜ"
    assert names[0].event_date == "2018-05-13"
    assert names[0].record_type is RecordType.ENTITY
    assert names[0].tier is Tier.IDENTITY_STATUS

    addrs = _by(events, ChangeType.ADDRESS_CHANGE)
    assert len(addrs) == 1
    assert "Old St 1" in addrs[0].value_old and "New St 2" in addrs[0].value_new
    assert addrs[0].event_date == "2017-12-13"

    forms = _by(events, ChangeType.LEGAL_FORM_CHANGE)
    assert len(forms) == 1
    assert forms[0].value_old == "Aktsiaselts" and forms[0].value_new == "Osaühing"
    assert forms[0].event_date == "2014-03-03"

    # Single status row = original only, no change emitted.
    assert _by(events, ChangeType.STATUS_CHANGED) == []


def test_owner_and_bo_events_reconstructed():
    events = ariregister_change_events(_DATA)

    added = _by(events, ChangeType.OWNER_ADDED)
    removed = _by(events, ChangeType.OWNER_REMOVED)
    # ADDED: board Markus, OSAN Old Owner, BO Edith, BO Past = 4
    assert len(added) == 4
    # REMOVED: OSAN Old Owner (ended), BO Past (ended) = 2
    assert len(removed) == 2

    board = next(e for e in added if "Markus Villig" in (e.counterparty or ""))
    assert board.record_type is RecordType.RELATIONSHIP
    assert board.tier is Tier.OWNERSHIP_CONTROL
    assert "management board member" in board.counterparty
    assert board.event_date == "2013-02-07"
    assert board.subject_id == "10584597"

    owner_out = next(e for e in removed if "Old Owner" in (e.counterparty or ""))
    assert owner_out.event_date == "2016-08-06"
    assert owner_out.interest_start_date == "2010-01-01"
    assert owner_out.interest_end_date == "2016-08-06"

    bo = next(e for e in added if "Edith Rik" in (e.counterparty or ""))
    assert "beneficial owner" in bo.counterparty
    assert "highest management body" in bo.counterparty
    assert bo.event_date == "2022-01-31"

    bo_out = next(e for e in removed if "Past Beneficiary" in (e.counterparty or ""))
    assert bo_out.event_date == "2021-12-31"


def test_all_events_effective_high_confidence():
    events = ariregister_change_events(_DATA)
    assert events
    assert all(e.source_id == "ariregister" for e in events)
    assert all(e.date_basis is DateBasis.EFFECTIVE for e in events)
    assert all(e.date_confidence is DateConfidence.HIGH for e in events)


def test_bo_branch_is_isolated():
    """BO events come only from bo_xml — so the whole BO branch can be dropped
    after 10 July 2026 by simply not passing bo_xml."""
    detail_only = ariregister_change_events(
        {"registry_code": "10584597", "detail_xml": _DETAIL_XML, "bo_xml": None}
    )
    assert detail_only
    assert all("beneficial owner" not in (e.counterparty or "") for e in detail_only)

    bo_only = ariregister_change_events(
        {"registry_code": "10584597", "detail_xml": None, "bo_xml": _BO_XML}
    )
    assert len(bo_only) == 3  # Edith added; Past added + removed
    assert all(e.source_id == "ariregister" for e in bo_only)


def test_empty_and_malformed_data():
    assert ariregister_change_events({}) == []
    assert ariregister_change_events(
        {"registry_code": "1", "detail_xml": "not-xml", "bo_xml": None}
    ) == []


def test_assembler_integrates_estonia_events():
    events = ariregister_change_events(_DATA)
    tl = assemble_timeline(lei="X", extra_events=events)
    # Notable = the Tier-1/2 events, clustered: ownership entries are 1:1.
    assert tl.notable
    assert any(entry.sources == ["ariregister"] for entry in tl.notable)
