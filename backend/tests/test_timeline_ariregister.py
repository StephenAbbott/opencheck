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
    # ADDED: OSAN Old Owner, BO Past = 2 (the board member is an office, on
    # the Tier-4 board stream, since the Estonia–Latvia phase; Edith's BO
    # row is coded J — named because no owner was identified — and is the
    # board stream too, since the History-tiering follow-up)
    assert len(added) == 2
    # REMOVED: OSAN Old Owner (ended), BO Past (ended) = 2
    assert len(removed) == 2

    board = next(e for e in _by(events, ChangeType.OFFICER_APPOINTED) if "Markus Villig" in (e.counterparty or ""))
    assert board.record_type is RecordType.RELATIONSHIP
    assert board.tier is Tier.BOARD_CHANGE
    assert "management board member" in board.counterparty
    assert board.event_date == "2013-02-07"
    assert board.subject_id == "10584597"
    assert not any("Markus Villig" in (e.counterparty or "") for e in added)

    owner_out = next(e for e in removed if "Old Owner" in (e.counterparty or ""))
    assert owner_out.event_date == "2016-08-06"
    assert owner_out.interest_start_date == "2010-01-01"
    assert owner_out.interest_end_date == "2016-08-06"

    assert not any("Edith Rik" in (e.counterparty or "") for e in added)
    bo = next(e for e in _by(events, ChangeType.OFFICER_APPOINTED) if "Edith Rik" in (e.counterparty or ""))
    assert bo.tier is Tier.BOARD_CHANGE
    assert "named on the beneficial-owner list because no owner was identified" in bo.counterparty
    assert "highest management body" in bo.counterparty
    assert bo.event_date == "2022-01-31"

    past = next(e for e in added if "Past Beneficiary" in (e.counterparty or ""))
    assert past.counterparty == "Past Beneficiary — beneficial owner (direct ownership)"
    assert past.tier is Tier.OWNERSHIP_CONTROL

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
    assert all("beneficial" not in (e.counterparty or "") for e in detail_only)

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


# --------------------------------------------------------------------------- #
# Roles are tiered, re-registrations folded, account prefixes stripped
# (the Estonia–Latvia emitters phase, 3 Nov 2026)
# --------------------------------------------------------------------------- #

_ROLES_XML = """<?xml version="1.0"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
 <SOAP-ENV:Body>
  <detailandmed_v2Response xmlns="http://arireg.x-road.eu/producer/">
   <keha><ettevotjad><item>
    <ariregistri_kood>10421629</ariregistri_kood>
    <isikuandmed>
     <kaardile_kantud_isikud>
      <item><isiku_roll>JUHL</isiku_roll><isiku_roll_tekstina>Management board member</isiku_roll_tekstina><eesnimi>Gunnar</eesnimi><nimi_arinimi>Okk</nimi_arinimi><algus_kpv>1998-03-31Z</algus_kpv><lopp_kpv>2005-12-28Z</lopp_kpv></item>
      <item><isiku_roll>PROK</isiku_roll><isiku_roll_tekstina>Procurator</isiku_roll_tekstina><eesnimi>Ain</eesnimi><nimi_arinimi>Saarepuu</nimi_arinimi><algus_kpv>1998-03-31Z</algus_kpv><lopp_kpv>1998-06-25Z</lopp_kpv></item>
     </kaardile_kantud_isikud>
     <kaardivalised_isikud>
      <item><isiku_roll>N</isiku_roll><isiku_roll_tekstina>Member of the supervisory board</isiku_roll_tekstina><eesnimi>Meelis</eesnimi><nimi_arinimi>Atonen</nimi_arinimi><algus_kpv>2008-05-20Z</algus_kpv><lopp_kpv>2011-05-19Z</lopp_kpv></item>
      <item><isiku_roll>N</isiku_roll><isiku_roll_tekstina>Member of the supervisory board</isiku_roll_tekstina><eesnimi>Meelis</eesnimi><nimi_arinimi>Atonen</nimi_arinimi><algus_kpv>2011-05-19Z</algus_kpv><lopp_kpv>2014-05-20Z</lopp_kpv></item>
      <item><isiku_roll>S</isiku_roll><isiku_roll_tekstina>Stockholder</isiku_roll_tekstina><nimi_arinimi>MAJANDUS- JA KOMMUNIKATSIOONIMINISTEERIUM</nimi_arinimi><osaluse_suurus>6000000</osaluse_suurus><osaluse_valuuta>EEK</osaluse_valuuta><algus_kpv>2001-04-27Z</algus_kpv><lopp_kpv>2005-04-01Z</lopp_kpv></item>
      <item><isiku_roll>S</isiku_roll><isiku_roll_tekstina>Stockholder</isiku_roll_tekstina><nimi_arinimi>MAJANDUS- JA KOMMUNIKATSIOONIMINISTEERIUM</nimi_arinimi><osaluse_suurus>7500000</osaluse_suurus><osaluse_valuuta>EEK</osaluse_valuuta><algus_kpv>2005-04-01Z</algus_kpv><lopp_kpv>2008-04-01Z</lopp_kpv></item>
      <item><isiku_roll>S</isiku_roll><isiku_roll_tekstina>Stockholder</isiku_roll_tekstina><nimi_arinimi>Lihtkonto: Rahandusministeerium</nimi_arinimi><osaluse_suurus>621645750</osaluse_suurus><osaluse_valuuta>EUR</osaluse_valuuta><algus_kpv>2017-12-22Z</algus_kpv><lopp_kpv>2023-08-16Z</lopp_kpv></item>
      <item><isiku_roll>S</isiku_roll><isiku_roll_tekstina>Stockholder</isiku_roll_tekstina><nimi_arinimi>Omanikukonto: Rahandusministeerium</nimi_arinimi><osaluse_suurus>621645750</osaluse_suurus><osaluse_valuuta>EUR</osaluse_valuuta><algus_kpv>2023-08-16Z</algus_kpv></item>
      <item><isiku_roll>D</isiku_roll><isiku_roll_tekstina>Audit firm</isiku_roll_tekstina><nimi_arinimi>Aktsiaselts PricewaterhouseCoopers</nimi_arinimi><algus_kpv>2002-11-14Z</algus_kpv></item>
      <item><isiku_roll>ARP</isiku_roll><isiku_roll_tekstina>The registrar of the shares of the fund</isiku_roll_tekstina><nimi_arinimi>Nasdaq CSD SE</nimi_arinimi><algus_kpv>2017-09-28Z</algus_kpv></item>
     </kaardivalised_isikud>
    </isikuandmed>
   </item></ettevotjad></keha>
  </detailandmed_v2Response>
 </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""


def _roles_events():
    return ariregister_change_events({"registry_code": "10421629", "detail_xml": _ROLES_XML, "bo_xml": None})


def test_an_office_is_the_board_stream_not_an_owner():
    events = _roles_events()
    officers = [e for e in events if e.tier is Tier.BOARD_CHANGE]
    assert {e.change_type for e in officers} == {ChangeType.OFFICER_APPOINTED, ChangeType.OFFICER_RESIGNED}
    names = {e.counterparty for e in officers}
    assert "Gunnar Okk — management board member" in names
    assert "Ain Saarepuu — procurator" in names
    assert "Meelis Atonen — member of the supervisory board" in names
    # No office is a Tier-1 owner event any more.
    owners = [e for e in events if e.tier is Tier.OWNERSHIP_CONTROL]
    assert all("ministeerium" in (e.counterparty or "").casefold() for e in owners)


def test_auditors_and_the_registrar_are_administrative_rows():
    events = _roles_events()
    admin = [e for e in events if e.tier is Tier.ADMIN_NOISE and e.change_type is None]
    assert {e.counterparty for e in admin} >= {
        "Aktsiaselts PricewaterhouseCoopers — audit firm",
        "Nasdaq CSD SE — the registrar of the shares of the fund",
    }
    assert all(e.record_type is RecordType.RELATIONSHIP for e in admin)
    assert not any("PricewaterhouseCoopers" in (e.counterparty or "") for e in events if e.tier is not Tier.ADMIN_NOISE)


def test_a_holding_reregistered_at_a_new_size_is_one_band_change():
    events = _roles_events()
    ministry = [e for e in events if "KOMMUNIKATSIOONIMINISTEERIUM" in (e.counterparty or "")]
    kinds = sorted((e.event_date, e.change_type.value) for e in ministry)
    assert kinds == [
        ("2001-04-27", "OWNER_ADDED"),
        ("2005-04-01", "CONTROL_BAND_CHANGED"),
        ("2008-04-01", "OWNER_REMOVED"),
    ]
    band = next(e for e in ministry if e.change_type is ChangeType.CONTROL_BAND_CHANGED)
    assert band.tier is Tier.OWNERSHIP_CONTROL
    assert (band.value_old, band.value_new) == ("6,000,000 EEK", "7,500,000 EEK")
    assert band.raw_change_type == "re-registered"


def test_an_office_renewed_on_the_day_it_ended_is_not_a_resignation():
    events = _roles_events()
    atonen = sorted((e.event_date, e.change_type.value, e.tier.value) for e in events if "Atonen" in (e.counterparty or ""))
    assert atonen == [
        ("2008-05-20", "OFFICER_APPOINTED", 4),
        ("2011-05-19", "OFFICER_DETAILS_CHANGED", 3),
        ("2014-05-20", "OFFICER_RESIGNED", 4),
    ]


def test_a_nasdaq_account_prefix_is_not_a_change_of_owner():
    events = _roles_events()
    rahandus = [e for e in events if "Rahandusministeerium" in (e.counterparty or "")]
    assert all(e.counterparty == "Rahandusministeerium — stockholder" for e in rahandus)
    # The holding moved from a simple account to an owner account at the
    # same size: one administrative row, not a band change and never an
    # owner leaving.
    assert sorted((e.event_date, e.change_type.value if e.change_type else e.raw_change_type, e.tier.value) for e in rahandus) == [
        ("2017-12-22", "OWNER_ADDED", 1),
        ("2023-08-16", "re-registered", 3),
    ]
    assert not any(e.change_type is ChangeType.OWNER_REMOVED for e in rahandus)


# --------------------------------------------------------------------------- #
# The History-tiering follow-up (4 Nov 2026): no role silently demoted
# --------------------------------------------------------------------------- #

#: Every person-role code in the register's own vocabulary (the ``role``
#: kind of the daily index's ``vocab`` table, cut 25 Oct 2026: 47 codes),
#: with the stream the History tab puts it on. A code the register adds
#: later falls to ``admin`` until it is listed here and in the mapper.
_REGISTER_ROLE_KINDS = {
    # ownership or control — Tier 1
    "OSAN": "ownership", "O": "ownership", "S": "ownership",
    "A": "ownership", "B": "ownership",
    "FIE": "ownership", "ETTEV": "ownership", "KOAS": "ownership",
    "YHL": "ownership", "YHLLV": "ownership",
    # offices — Tier 4 (partners follow the mapper's boardMember: Stephen,
    # 4 Nov 2026)
    "JUHL": "officer", "JUHJ": "officer", "JUHA": "officer", "JUHE": "officer",
    "HNKL": "officer", "PROK": "officer", "LIKV": "officer", "LIKVJ": "officer",
    "PANKR": "officer", "AJUTPH": "officer", "AJPH": "officer", "JPNKR": "officer",
    "ERIH": "officer", "TOSAN": "officer", "UOSAN": "officer", "EUSOS": "officer",
    "EUSOS2": "officer", "ASES": "officer", "ESIS": "officer", "ESIS2": "officer",
    "SJESI": "officer", "VFILJ": "officer", "FV": "officer", "VALIT": "officer",
    "N": "officer", "E": "officer",
    # no interest in the company — Tier 3
    "KISIK": "admin", "ORP": "admin", "ARP": "admin", "MDKPI": "admin",
    "D": "admin", "J": "admin", "M": "admin", "P": "admin", "V": "admin",
    "R": "admin", "L": "admin",
}


def test_every_register_role_code_has_its_stream():
    from opencheck.timeline.ariregister import role_kind

    assert len(_REGISTER_ROLE_KINDS) == 47
    assert {code: role_kind(code) for code in _REGISTER_ROLE_KINDS} == _REGISTER_ROLE_KINDS


def test_ownership_roles_agree_with_the_mapper_and_the_history_reader():
    """A shareholder the ended-ownership reader counts, and a role the
    mapper types ``otherInfluenceOrControl``, is ownership on History too —
    ``O`` (the OÜ shareholder off the card) and ``YHLLV`` were not."""
    from opencheck.bods.mappers.estonia import _EE_OFFICER_ROLE_MAP
    from opencheck.sources.ariregister_history import SHAREHOLDER_ROLES
    from opencheck.timeline.ariregister import OWNERSHIP_ROLES

    assert SHAREHOLDER_ROLES <= OWNERSHIP_ROLES
    assert {c for c, (kind, _) in _EE_OFFICER_ROLE_MAP.items() if kind == "otherInfluenceOrControl"} <= OWNERSHIP_ROLES


_FALLBACK_BO_XML = """<?xml version="1.0"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
 <SOAP-ENV:Body>
  <tegelikudKasusaajad_v2Response xmlns="http://arireg.x-road.eu/producer/">
   <keha>
    <kasusaajad>
     <kasusaaja>
      <eesnimi>Janek</eesnimi><nimi>Stalmeister</nimi>
      <kontrolli_teostamise_viis>J</kontrolli_teostamise_viis>
      <kontrolli_teostamise_viis_tekstina>Senior managing official i.e. management board member or member of the supervisory board</kontrolli_teostamise_viis_tekstina>
      <algus_kpv>2018-03-01Z</algus_kpv><lopp_kpv>2021-06-30Z</lopp_kpv>
     </kasusaaja>
     <kasusaaja>
      <eesnimi>Board</eesnimi><nimi>Member</nimi>
      <kontrolli_teostamise_viis>C</kontrolli_teostamise_viis>
      <algus_kpv>2019-01-01Z</algus_kpv>
     </kasusaaja>
     <kasusaaja>
      <eesnimi>Janek</eesnimi><nimi>Stalmeister</nimi>
      <kontrolli_teostamise_viis>O</kontrolli_teostamise_viis>
      <kontrolli_teostamise_viis_tekstina>Direct ownership</kontrolli_teostamise_viis_tekstina>
      <algus_kpv>2021-06-30Z</algus_kpv>
     </kasusaaja>
    </kasusaajad>
   </keha>
  </tegelikudKasusaajad_v2Response>
 </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"""


def test_a_fallback_bo_designation_is_the_board_stream_worded_as_such():
    events = ariregister_change_events({"registry_code": "10421629", "detail_xml": None, "bo_xml": _FALLBACK_BO_XML})
    rows = sorted((e.event_date, e.change_type.value, e.tier.value, e.counterparty) for e in events)
    fallback = "named on the beneficial-owner list because no owner was identified"
    assert rows == [
        ("2018-03-01", "OFFICER_APPOINTED", 4,
         f"Janek Stalmeister — {fallback} (Senior managing official i.e. management board member or member of the supervisory board)"),
        ("2019-01-01", "OFFICER_APPOINTED", 4, f"Board Member — {fallback} (C)"),
        # The same person becoming an owner the day the designation ends is
        # an owner arriving and a designation ending — two streams, never a
        # re-registration of one row.
        ("2021-06-30", "OFFICER_RESIGNED", 4,
         f"Janek Stalmeister — {fallback} (Senior managing official i.e. management board member or member of the supervisory board)"),
        ("2021-06-30", "OWNER_ADDED", 1, "Janek Stalmeister — beneficial owner (Direct ownership)"),
    ]
