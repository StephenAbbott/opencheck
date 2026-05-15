"""Tests for the Austrian Firmenbuch → BODS v0.4 adapter and mapper.

Covers:
  - FN normalisation utility
  - SOAP response XML parsers (search + extract)
  - map_firmenbuch: entity, officer, and shareholder statement shapes
  - BODS validator compliance for all fixture combinations

Fixtures use the real namespace-prefixed DKZ (Datenkennzeichen) XML structure
returned by the Firmenbuch HVD SOAP API, with namespace prefixes stripped by
_strip_namespaces() before ElementTree parsing.
"""

from __future__ import annotations

import pytest

from opencheck.bods import map_firmenbuch, validate_shape
from opencheck.sources.firmenbuch import (
    AT_FB_RA_CODE,
    FirmenbuchAdapter,
    _parse_extract_response,
    _parse_fun_per_officers,
    _parse_search_response,
    _strip_namespaces,
    is_valid_fn,
    normalise_fn,
)

# ---------------------------------------------------------------------------
# FN normalisation
# ---------------------------------------------------------------------------


def test_normalise_fn_lowercase_suffix() -> None:
    assert normalise_fn("473888W") == "473888w"


def test_normalise_fn_strips_whitespace() -> None:
    assert normalise_fn("  366715m  ") == "366715m"


def test_normalise_fn_strips_fn_prefix() -> None:
    assert normalise_fn("FN 473888w") == "473888w"
    assert normalise_fn("fn 366715m") == "366715m"


def test_normalise_fn_already_normalised() -> None:
    assert normalise_fn("473888w") == "473888w"


def test_normalise_fn_strips_space_before_suffix() -> None:
    """API returns FNR with a space before the letter, e.g. '229831 m'."""
    assert normalise_fn("229831 m") == "229831m"
    assert normalise_fn("473888 W") == "473888w"


def test_is_valid_fn_typical() -> None:
    assert is_valid_fn("473888w")
    assert is_valid_fn("366715m")


def test_is_valid_fn_with_space_format() -> None:
    """Space-separated form from API ('229831 m') should be valid after normalisation."""
    assert is_valid_fn("229831 m")


def test_is_valid_fn_rejects_digits_only() -> None:
    assert not is_valid_fn("473888")


def test_is_valid_fn_rejects_empty() -> None:
    assert not is_valid_fn("")


def test_at_fb_ra_code() -> None:
    assert AT_FB_RA_CODE == "RA000017"


# ---------------------------------------------------------------------------
# SOAP search response parser
#
# The real search response uses ERGEBNIS elements (confirmed from the official
# HVD interface description v1.3, page 11).  After namespace stripping the
# parser looks for ERGEBNIS elements with FNR and NAME child elements.
# ---------------------------------------------------------------------------

_SEARCH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns13:SUCHEFIRMARESPONSE
        xmlns:ns13="ns://firmenbuch.justiz.gv.at/Abfrage/SucheFirmaResponse">
      <ns13:ERGEBNIS>
        <ns13:FNR>473888 w</ns13:FNR>
        <ns13:STATUS/>
        <ns13:NAME>Muster GmbH</ns13:NAME>
        <ns13:SITZ>Wien</ns13:SITZ>
        <ns13:RECHTSFORM>
          <ns13:CODE>GmbH</ns13:CODE>
          <ns13:TEXT>Gesellschaft mit beschränkter Haftung</ns13:TEXT>
        </ns13:RECHTSFORM>
      </ns13:ERGEBNIS>
      <ns13:ERGEBNIS>
        <ns13:FNR>366715 m</ns13:FNR>
        <ns13:STATUS/>
        <ns13:NAME>Beispiel KG</ns13:NAME>
        <ns13:SITZ>Graz</ns13:SITZ>
        <ns13:RECHTSFORM>
          <ns13:CODE>KG</ns13:CODE>
          <ns13:TEXT>Kommanditgesellschaft</ns13:TEXT>
        </ns13:RECHTSFORM>
      </ns13:ERGEBNIS>
    </ns13:SUCHEFIRMARESPONSE>
  </env:Body>
</env:Envelope>"""

_SEARCH_XML_EMPTY = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns13:SUCHEFIRMARESPONSE
        xmlns:ns13="ns://firmenbuch.justiz.gv.at/Abfrage/SucheFirmaResponse">
    </ns13:SUCHEFIRMARESPONSE>
  </env:Body>
</env:Envelope>"""

_SEARCH_XML_MALFORMED = "this is not XML"


def test_parse_search_response_finds_two_hits() -> None:
    hits = _parse_search_response(_SEARCH_XML)
    assert len(hits) == 2


def test_parse_search_response_first_hit_name() -> None:
    hits = _parse_search_response(_SEARCH_XML)
    assert hits[0]["name"] == "Muster GmbH"


def test_parse_search_response_fn_keys() -> None:
    """Parser now returns 'fn', not 'firma_id'."""
    hits = _parse_search_response(_SEARCH_XML)
    fns = [h["fn"] for h in hits]
    assert "473888w" in fns
    assert "366715m" in fns


def test_parse_search_response_empty_returns_empty_list() -> None:
    assert _parse_search_response(_SEARCH_XML_EMPTY) == []


def test_parse_search_response_malformed_returns_empty_list() -> None:
    assert _parse_search_response(_SEARCH_XML_MALFORMED) == []


# ---------------------------------------------------------------------------
# SOAP extract response parser
#
# Real API format: AUSZUG_V2_RESPONSE with namespace-prefixed DKZ elements.
#   FI_DKZ02  name (BEZEICHNUNG children)
#   FI_DKZ03  address
#   FI_DKZ06  registered capital (BETRAG child)
#   FI_DKZ07  GmbH shareholders
#   FI_DKZ08  Geschäftsführer (AUFRECHT=false → terminated, skip)
#   FI_DKZ09  Prokuristen
#   FI_DKZ12  Komplementäre (general partners, KG)
#   FI_DKZ13  Kommanditisten (limited partners, KG)
#
# Note: founding_date and UID are not available in the Auszug response.
# ---------------------------------------------------------------------------

# GmbH with 2 active officers, 1 terminated officer, 2 shareholders.
# Officers use Max+Anna; shareholders use Leopold+Brigitte (no overlap).
_EXTRACT_XML_GMBH = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns6:AUSZUG_V2_RESPONSE
        xmlns:ns6="ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugResponse"
        ns6:FNR="473888 w"
        ns6:STICHTAG="2026-01-01"
        ns6:UMFANG="Auszug">
      <ns6:FIRMA>
        <ns6:FI_DKZ02 ns6:AUFRECHT="true" ns6:VNR="001">
          <ns6:BEZEICHNUNG>Muster GmbH</ns6:BEZEICHNUNG>
        </ns6:FI_DKZ02>
        <ns6:FI_DKZ03 ns6:AUFRECHT="true">
          <ns6:STRASSE>Musterstraße</ns6:STRASSE>
          <ns6:HAUSNUMMER>1</ns6:HAUSNUMMER>
          <ns6:PLZ>1010</ns6:PLZ>
          <ns6:ORT>Wien</ns6:ORT>
        </ns6:FI_DKZ03>
        <ns6:FI_DKZ06>
          <ns6:BETRAG>35000</ns6:BETRAG>
        </ns6:FI_DKZ06>
        <ns6:FI_DKZ08 ns6:AUFRECHT="true">
          <ns6:VORNAME>Max</ns6:VORNAME>
          <ns6:NACHNAME>Mustermann</ns6:NACHNAME>
          <ns6:GEBURTSDATUM>1970-06-15</ns6:GEBURTSDATUM>
        </ns6:FI_DKZ08>
        <ns6:FI_DKZ09 ns6:AUFRECHT="true">
          <ns6:VORNAME>Anna</ns6:VORNAME>
          <ns6:NACHNAME>Musterfrau</ns6:NACHNAME>
        </ns6:FI_DKZ09>
        <ns6:FI_DKZ08 ns6:AUFRECHT="false">
          <ns6:VORNAME>Former</ns6:VORNAME>
          <ns6:NACHNAME>Director</ns6:NACHNAME>
        </ns6:FI_DKZ08>
        <ns6:FI_DKZ07 ns6:AUFRECHT="true">
          <ns6:VORNAME>Leopold</ns6:VORNAME>
          <ns6:NACHNAME>Gesellschafter</ns6:NACHNAME>
          <ns6:STAMMEINLAGE>17500</ns6:STAMMEINLAGE>
        </ns6:FI_DKZ07>
        <ns6:FI_DKZ07 ns6:AUFRECHT="true">
          <ns6:VORNAME>Brigitte</ns6:VORNAME>
          <ns6:NACHNAME>Gesellschafterin</ns6:NACHNAME>
          <ns6:STAMMEINLAGE>17500</ns6:STAMMEINLAGE>
        </ns6:FI_DKZ07>
      </ns6:FIRMA>
    </ns6:AUSZUG_V2_RESPONSE>
  </env:Body>
</env:Envelope>"""

# GmbH with a single corporate shareholder linking to another Firmenbuch entry.
_EXTRACT_XML_CORPORATE_SHAREHOLDER = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns6:AUSZUG_V2_RESPONSE
        xmlns:ns6="ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugResponse"
        ns6:FNR="100000 a"
        ns6:STICHTAG="2026-01-01"
        ns6:UMFANG="Auszug">
      <ns6:FIRMA>
        <ns6:FI_DKZ02 ns6:AUFRECHT="true" ns6:VNR="001">
          <ns6:BEZEICHNUNG>Holding GmbH</ns6:BEZEICHNUNG>
        </ns6:FI_DKZ02>
        <ns6:FI_DKZ06>
          <ns6:BETRAG>100000</ns6:BETRAG>
        </ns6:FI_DKZ06>
        <ns6:FI_DKZ08 ns6:AUFRECHT="true">
          <ns6:VORNAME>Hans</ns6:VORNAME>
          <ns6:NACHNAME>Investor</ns6:NACHNAME>
        </ns6:FI_DKZ08>
        <ns6:FI_DKZ07 ns6:AUFRECHT="true" ns6:FNR="200000 b">
          <ns6:BEZEICHNUNG>Muttergesellschaft AG</ns6:BEZEICHNUNG>
          <ns6:STAMMEINLAGE>100000</ns6:STAMMEINLAGE>
        </ns6:FI_DKZ07>
      </ns6:FIRMA>
    </ns6:AUSZUG_V2_RESPONSE>
  </env:Body>
</env:Envelope>"""

# KG with a general partner (Komplementär, FI_DKZ12 → officer) and a limited
# partner (Kommanditist, FI_DKZ13 → shareholder).
_EXTRACT_XML_KG = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns6:AUSZUG_V2_RESPONSE
        xmlns:ns6="ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugResponse"
        ns6:FNR="999999 z"
        ns6:STICHTAG="2026-01-01"
        ns6:UMFANG="Auszug">
      <ns6:FIRMA>
        <ns6:FI_DKZ02 ns6:AUFRECHT="true" ns6:VNR="001">
          <ns6:BEZEICHNUNG>Muster KG</ns6:BEZEICHNUNG>
        </ns6:FI_DKZ02>
        <ns6:FI_DKZ12 ns6:AUFRECHT="true">
          <ns6:VORNAME>Klaus</ns6:VORNAME>
          <ns6:NACHNAME>Komplementaer</ns6:NACHNAME>
        </ns6:FI_DKZ12>
        <ns6:FI_DKZ13 ns6:AUFRECHT="true">
          <ns6:VORNAME>Maria</ns6:VORNAME>
          <ns6:NACHNAME>Kommanditistin</ns6:NACHNAME>
          <ns6:EINLAGE>50000</ns6:EINLAGE>
        </ns6:FI_DKZ13>
      </ns6:FIRMA>
    </ns6:AUSZUG_V2_RESPONSE>
  </env:Body>
</env:Envelope>"""


def test_parse_extract_name() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["name"] == "Muster GmbH"


def test_parse_extract_fn() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["fn"] == "473888w"


def test_parse_extract_founding_date_not_in_auszug() -> None:
    """founding_date is not available in the Auszug response; parser returns None."""
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["founding_date"] is None


def test_parse_extract_stamm_kapital() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["stamm_kapital"] == 35000.0


def test_parse_extract_address() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert "Musterstraße" in ext["address"]
    assert "Wien" in ext["address"]


def test_parse_extract_officers_count() -> None:
    """Terminated officer (AUFRECHT=false) should be excluded."""
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert len(ext["officers"]) == 2


def test_parse_extract_officer_names() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    names = [o["full_name"] for o in ext["officers"]]
    assert "Max Mustermann" in names
    assert "Anna Musterfrau" in names


def test_parse_extract_officer_roles() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    roles = {o["role_code"] for o in ext["officers"]}
    assert "GF" in roles
    assert "PK" in roles


def test_parse_extract_shareholders_count() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert len(ext["shareholders"]) == 2


def test_parse_extract_shareholder_einlage() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    einlagen = [sh["einlage"] for sh in ext["shareholders"]]
    assert all(e == 17500.0 for e in einlagen)


# ---------------------------------------------------------------------------
# STELLE address format (Kurzinformation)
#
# The official HVD interface description v1.3 (page 6) shows that
# Kurzinformation responses use STELLE for the address rather than
# STRASSE + HAUSNUMMER.  The XSD confirms this is a xs:choice.
# ---------------------------------------------------------------------------

_EXTRACT_XML_STELLE_ADDRESS = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns6:AUSZUG_V2_RESPONSE
        xmlns:ns6="ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugResponse"
        ns6:FNR="229831 m"
        ns6:STICHTAG="2026-05-14"
        ns6:UMFANG="Kurzinformation">
      <ns6:FIRMA>
        <ns6:FI_DKZ02 ns6:AUFRECHT="true" ns6:VNR="001">
          <ns6:BEZEICHNUNG>Kärntner Handels GmbH</ns6:BEZEICHNUNG>
        </ns6:FI_DKZ02>
        <ns6:FI_DKZ03 ns6:AUFRECHT="true">
          <ns6:STELLE>Kärntner Straße 337</ns6:STELLE>
          <ns6:PLZ>1010</ns6:PLZ>
          <ns6:ORT>Wien</ns6:ORT>
        </ns6:FI_DKZ03>
      </ns6:FIRMA>
    </ns6:AUSZUG_V2_RESPONSE>
  </env:Body>
</env:Envelope>"""


def test_parse_extract_stelle_address_parsed() -> None:
    """STELLE free-text address (Kurzinformation format) should be returned."""
    ext = _parse_extract_response(_EXTRACT_XML_STELLE_ADDRESS)
    assert "Kärntner Straße 337" in ext["address"]
    assert "Wien" in ext["address"]


def test_parse_extract_stelle_address_no_strasse_prefix() -> None:
    """STELLE result should not contain a spurious 'None' or empty prefix."""
    ext = _parse_extract_response(_EXTRACT_XML_STELLE_ADDRESS)
    assert ext["address"].startswith("Kärntner")


# ---------------------------------------------------------------------------
# Bundle factory helpers
# ---------------------------------------------------------------------------

_GMBH_EXTRACT = _parse_extract_response(_EXTRACT_XML_GMBH)
_CORPORATE_SH_EXTRACT = _parse_extract_response(_EXTRACT_XML_CORPORATE_SHAREHOLDER)
_KG_EXTRACT = _parse_extract_response(_EXTRACT_XML_KG)


def _bundle(
    fn: str = "473888w",
    extract: dict | None = None,
    legal_name: str = "",
    is_stub: bool = False,
) -> dict:
    return {
        "source_id": "firmenbuch",
        "fn": fn,
        "extract": extract if extract is not None else _GMBH_EXTRACT,
        "legal_name": legal_name,
        "is_stub": is_stub,
    }


# ---------------------------------------------------------------------------
# map_firmenbuch — basic shape
# ---------------------------------------------------------------------------


def test_map_firmenbuch_stub_returns_empty() -> None:
    assert list(map_firmenbuch(_bundle(is_stub=True))) == []


def test_map_firmenbuch_no_extract_returns_empty() -> None:
    assert list(map_firmenbuch(_bundle(extract={}))) == []


def test_map_firmenbuch_no_fn_returns_empty() -> None:
    b = _bundle()
    b["fn"] = ""
    assert list(map_firmenbuch(b)) == []


def test_map_firmenbuch_statement_count_gmbh() -> None:
    """GmbH with 2 officers + 2 shareholders (no overlap) → 1 entity + 4 person + 4 rel = 9."""
    stmts = list(map_firmenbuch(_bundle()))
    assert len(stmts) == 9


def test_map_firmenbuch_yields_entity_first() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    assert stmts[0]["recordType"] == "entity"


def test_map_firmenbuch_entity_name() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    assert stmts[0]["recordDetails"]["name"] == "Muster GmbH"


def test_map_firmenbuch_entity_jurisdiction() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    jur = stmts[0]["recordDetails"]["incorporatedInJurisdiction"]
    assert jur["code"] == "AT"
    assert jur["name"] == "Austria"


def test_map_firmenbuch_identifier_at_fb() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "AT-FB" in schemes


def test_map_firmenbuch_identifier_fn_value() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    fn_id = next(i["id"] for i in stmts[0]["recordDetails"]["identifiers"] if i["scheme"] == "AT-FB")
    assert fn_id == "473888w"


def test_map_firmenbuch_address_present() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    addrs = stmts[0]["recordDetails"].get("addresses") or []
    assert len(addrs) > 0
    assert "Musterstraße" in addrs[0]["address"]


# ---------------------------------------------------------------------------
# map_firmenbuch — officer statements
# ---------------------------------------------------------------------------


def test_map_firmenbuch_officer_person_statements() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    persons = [s for s in stmts if s["recordType"] == "person"]
    assert len(persons) == 4  # 2 officers + 2 shareholders (different people)


def test_map_firmenbuch_officer_relationship_statements() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    assert len(rels) == 4


def test_map_firmenbuch_officer_interest_type_gf() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    # At least one relationship should be otherInfluenceOrControl (GF)
    types = {
        i["type"]
        for r in rels
        for i in r["recordDetails"]["interests"]
    }
    assert "otherInfluenceOrControl" in types


def test_map_firmenbuch_officer_not_beneficial_owner() -> None:
    """All Firmenbuch relationships should have beneficialOwnershipOrControl=False."""
    stmts = list(map_firmenbuch(_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    for rel in rels:
        for interest in rel["recordDetails"]["interests"]:
            assert interest["beneficialOwnershipOrControl"] is False


# ---------------------------------------------------------------------------
# map_firmenbuch — shareholder statements
# ---------------------------------------------------------------------------


def test_map_firmenbuch_shareholder_interest_type() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    types = {
        i["type"]
        for r in rels
        for i in r["recordDetails"]["interests"]
    }
    assert "shareholding" in types


def test_map_firmenbuch_shareholder_share_percentage() -> None:
    """50% stake: 17500 / 35000 = 50.0."""
    stmts = list(map_firmenbuch(_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    share_rels = [
        r for r in rels
        if any(i["type"] == "shareholding" for i in r["recordDetails"]["interests"])
    ]
    for rel in share_rels:
        interest = next(i for i in rel["recordDetails"]["interests"] if i["type"] == "shareholding")
        assert interest["share"]["exact"] == 50.0


# ---------------------------------------------------------------------------
# map_firmenbuch — corporate shareholder
# ---------------------------------------------------------------------------


def test_map_firmenbuch_corporate_shareholder_emits_entity_statement() -> None:
    stmts = list(map_firmenbuch(_bundle("100000a", extract=_CORPORATE_SH_EXTRACT)))
    entities = [s for s in stmts if s["recordType"] == "entity"]
    # 1 subject entity + 1 corporate shareholder entity
    assert len(entities) == 2


def test_map_firmenbuch_corporate_shareholder_fn_identifier() -> None:
    stmts = list(map_firmenbuch(_bundle("100000a", extract=_CORPORATE_SH_EXTRACT)))
    entities = [s for s in stmts if s["recordType"] == "entity"]
    # The shareholder entity should carry the AT-FB identifier for its FN
    shareholder_entity = next(
        (e for e in entities if e["recordDetails"]["name"] == "Muttergesellschaft AG"),
        None,
    )
    assert shareholder_entity is not None
    schemes = {i["scheme"] for i in shareholder_entity["recordDetails"]["identifiers"]}
    assert "AT-FB" in schemes


# ---------------------------------------------------------------------------
# map_firmenbuch — KG entity (partners)
# ---------------------------------------------------------------------------


def test_map_firmenbuch_kg_komplementaer_present() -> None:
    stmts = list(map_firmenbuch(_bundle("999999z", extract=_KG_EXTRACT)))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    details_texts = [
        i["details"]
        for r in rels
        for i in r["recordDetails"]["interests"]
    ]
    assert any("Komplementär" in d for d in details_texts)


def test_map_firmenbuch_kg_kommanditist_present() -> None:
    stmts = list(map_firmenbuch(_bundle("999999z", extract=_KG_EXTRACT)))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    details_texts = [
        i["details"]
        for r in rels
        for i in r["recordDetails"]["interests"]
    ]
    assert any("Kommanditist" in d for d in details_texts)


# ---------------------------------------------------------------------------
# BODS validator compliance
# ---------------------------------------------------------------------------


def test_map_firmenbuch_gmbh_passes_validator() -> None:
    issues = validate_shape(map_firmenbuch(_bundle()))
    assert issues == [], issues


def test_map_firmenbuch_corporate_shareholder_passes_validator() -> None:
    issues = validate_shape(
        map_firmenbuch(_bundle("100000a", extract=_CORPORATE_SH_EXTRACT))
    )
    assert issues == [], issues


def test_map_firmenbuch_kg_passes_validator() -> None:
    issues = validate_shape(
        map_firmenbuch(_bundle("999999z", extract=_KG_EXTRACT))
    )
    assert issues == [], issues


def test_map_firmenbuch_stub_passes_validator() -> None:
    issues = validate_shape(map_firmenbuch(_bundle(is_stub=True)))
    assert issues == [], issues


# ---------------------------------------------------------------------------
# Source block
# ---------------------------------------------------------------------------


def test_map_firmenbuch_source_url_contains_fn() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    source = stmts[0].get("source") or {}
    assert "473888w" in (source.get("url") or "")


def test_map_firmenbuch_source_type_official_register() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    source = stmts[0].get("source") or {}
    assert "officialRegister" in (source.get("type") or [])


# ---------------------------------------------------------------------------
# FUN/PER officer parsing (Kurzinformation structure)
#
# The free HVD tier (UMFANG=Kurzinformation) returns officer data in top-level
# FUN and PER elements — siblings of FIRMA inside AUSZUG_V2_RESPONSE — not in
# FI_DKZ08/09 inside FIRMA.  This was confirmed by the Firmenbuch team (May
# 2026) and verified against a live API call for company 160573m
# (Bundesrechenzentrum GmbH), which returned 2 GF (FKEN="GF") and 5 PR
# (FKEN="PR") officers.
#
# Key structural facts from the live response:
#   - FUN carries FKEN, FKENTEXT, PNR attributes; FU_DKZ10 child has AUFRECHT
#   - PER carries PNR; PE_DKZ02 child has VORNAME, NACHNAME, GEBURTSDATUM
#   - Terminated appointments have FU_DKZ10 AUFRECHT="false" → skipped
#   - Role code FKEN="GF" → Geschäftsführer (managing director)
#   - Role code FKEN="PR" → Prokurist/in (authorised signatory)
# ---------------------------------------------------------------------------

# Kurzinformation response with namespace prefixes (like the real API).
# Contains 2 active officers (one GF, one PR) and 1 terminated PR.
# Namespace-stripping is applied by _parse_extract_response before parsing.
_EXTRACT_XML_KURZINFO_FUN_PER = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns6:AUSZUG_V2_RESPONSE
        xmlns:ns6="ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugResponse"
        ns6:FNR="160573 m"
        ns6:STICHTAG="2026-05-14"
        ns6:UMFANG="Kurzinformation">
      <ns6:FIRMA>
        <ns6:FI_DKZ02 ns6:AUFRECHT="true" ns6:VNR="001">
          <ns6:BEZEICHNUNG>Bundesrechenzentrum GmbH</ns6:BEZEICHNUNG>
        </ns6:FI_DKZ02>
        <ns6:FI_DKZ03 ns6:AUFRECHT="true">
          <ns6:STELLE>Hintere Zollamtsstraße 4</ns6:STELLE>
          <ns6:PLZ>1030</ns6:PLZ>
          <ns6:ORT>Wien</ns6:ORT>
        </ns6:FI_DKZ03>
      </ns6:FIRMA>
      <ns6:FUN ns6:FKEN="GF" ns6:FKENTEXT="GESCHÄFTSFÜHRER/IN (handelsrechtlich)" ns6:PNR="1001">
        <ns6:FU_DKZ10 ns6:AUFRECHT="true" ns6:VNR="001"/>
      </ns6:FUN>
      <ns6:FUN ns6:FKEN="PR" ns6:FKENTEXT="PROKURIST/IN" ns6:PNR="1002">
        <ns6:FU_DKZ10 ns6:AUFRECHT="true" ns6:VNR="001"/>
      </ns6:FUN>
      <ns6:FUN ns6:FKEN="PR" ns6:FKENTEXT="PROKURIST/IN" ns6:PNR="1003">
        <ns6:FU_DKZ10 ns6:AUFRECHT="false" ns6:VNR="001"/>
      </ns6:FUN>
      <ns6:PER ns6:PNR="1001">
        <ns6:PE_DKZ02 ns6:VNR="001">
          <ns6:VORNAME>Christine</ns6:VORNAME>
          <ns6:NACHNAME>Sumper-Billinger</ns6:NACHNAME>
          <ns6:GEBURTSDATUM>1973-09-06</ns6:GEBURTSDATUM>
        </ns6:PE_DKZ02>
      </ns6:PER>
      <ns6:PER ns6:PNR="1002">
        <ns6:PE_DKZ02 ns6:VNR="001">
          <ns6:VORNAME>Günther</ns6:VORNAME>
          <ns6:NACHNAME>Lauer</ns6:NACHNAME>
          <ns6:GEBURTSDATUM>1968-03-15</ns6:GEBURTSDATUM>
        </ns6:PE_DKZ02>
      </ns6:PER>
      <ns6:PER ns6:PNR="1003">
        <ns6:PE_DKZ02 ns6:VNR="001">
          <ns6:VORNAME>Former</ns6:VORNAME>
          <ns6:NACHNAME>Prokurist</ns6:NACHNAME>
          <ns6:GEBURTSDATUM>1960-01-01</ns6:GEBURTSDATUM>
        </ns6:PE_DKZ02>
      </ns6:PER>
    </ns6:AUSZUG_V2_RESPONSE>
  </env:Body>
</env:Envelope>"""


# ---------------------------------------------------------------------------
# _parse_fun_per_officers — unit tests on the stripped XML element
# ---------------------------------------------------------------------------

import xml.etree.ElementTree as ET


def _fun_per_resp_el() -> ET.Element:
    """Return the parsed AUSZUG_V2_RESPONSE element from the FUN/PER fixture."""
    stripped = _strip_namespaces(_EXTRACT_XML_KURZINFO_FUN_PER)
    root = ET.fromstring(stripped)
    el = root.find(".//AUSZUG_V2_RESPONSE")
    assert el is not None
    return el


def test_parse_fun_per_officers_count() -> None:
    """Two active FUN entries → 2 officers (terminated PNR=1003 is skipped)."""
    officers = _parse_fun_per_officers(_fun_per_resp_el())
    assert len(officers) == 2


def test_parse_fun_per_officers_gf_name() -> None:
    officers = _parse_fun_per_officers(_fun_per_resp_el())
    gf = next(o for o in officers if o["role_code"] == "GF")
    assert gf["full_name"] == "Christine Sumper-Billinger"


def test_parse_fun_per_officers_gf_dob() -> None:
    officers = _parse_fun_per_officers(_fun_per_resp_el())
    gf = next(o for o in officers if o["role_code"] == "GF")
    assert gf["dob"] == "1973-09-06"


def test_parse_fun_per_officers_pr_name() -> None:
    officers = _parse_fun_per_officers(_fun_per_resp_el())
    pr = next(o for o in officers if o["role_code"] == "PR")
    assert pr["full_name"] == "Günther Lauer"


def test_parse_fun_per_officers_pr_role_name() -> None:
    officers = _parse_fun_per_officers(_fun_per_resp_el())
    pr = next(o for o in officers if o["role_code"] == "PR")
    assert pr["role_name"] == "PROKURIST/IN"


def test_parse_fun_per_officers_skips_terminated() -> None:
    """FU_DKZ10 AUFRECHT='false' → that person must not appear in results."""
    officers = _parse_fun_per_officers(_fun_per_resp_el())
    names = [o["full_name"] for o in officers]
    assert "Former Prokurist" not in names


# ---------------------------------------------------------------------------
# _parse_extract_response — FUN/PER path via the full pipeline
# ---------------------------------------------------------------------------


def test_parse_extract_kurzinfo_name() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)
    assert ext["name"] == "Bundesrechenzentrum GmbH"


def test_parse_extract_kurzinfo_fn() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)
    assert ext["fn"] == "160573m"


def test_parse_extract_kurzinfo_address() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)
    assert "Hintere Zollamtsstraße 4" in ext["address"]
    assert "Wien" in ext["address"]


def test_parse_extract_kurzinfo_officers_count() -> None:
    """FUN/PER path: 2 active, 1 terminated → 2 officers."""
    ext = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)
    assert len(ext["officers"]) == 2


def test_parse_extract_kurzinfo_officer_names() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)
    names = [o["full_name"] for o in ext["officers"]]
    assert "Christine Sumper-Billinger" in names
    assert "Günther Lauer" in names


def test_parse_extract_kurzinfo_no_shareholders() -> None:
    """Kurzinformation does not return shareholder data."""
    ext = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)
    assert ext["shareholders"] == []


# ---------------------------------------------------------------------------
# map_firmenbuch — FUN/PER path BODS output
# ---------------------------------------------------------------------------

_KURZINFO_FUN_PER_EXTRACT = _parse_extract_response(_EXTRACT_XML_KURZINFO_FUN_PER)


def _kurzinfo_bundle() -> dict:
    return {
        "source_id": "firmenbuch",
        "fn": "160573m",
        "extract": _KURZINFO_FUN_PER_EXTRACT,
        "legal_name": "",
        "is_stub": False,
    }


def test_map_firmenbuch_kurzinfo_statement_count() -> None:
    """1 entity + 2 person (officers) + 2 relationship → 5 statements."""
    stmts = list(map_firmenbuch(_kurzinfo_bundle()))
    assert len(stmts) == 5


def test_map_firmenbuch_kurzinfo_gf_interest_type() -> None:
    stmts = list(map_firmenbuch(_kurzinfo_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    all_types = {i["type"] for r in rels for i in r["recordDetails"]["interests"]}
    assert "otherInfluenceOrControl" in all_types


def test_map_firmenbuch_kurzinfo_pr_mapped_label() -> None:
    """FKEN='PR' must map to 'Prokurist (Authorised Signatory)', not raw FKENTEXT."""
    stmts = list(map_firmenbuch(_kurzinfo_bundle()))
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    all_details = [i["details"] for r in rels for i in r["recordDetails"]["interests"]]
    assert any("Prokurist" in d for d in all_details)
    # Must NOT fall back to the raw uppercased "PROKURIST/IN" FKENTEXT
    assert not any(d == "PROKURIST/IN" for d in all_details)


def test_map_firmenbuch_kurzinfo_passes_validator() -> None:
    issues = validate_shape(map_firmenbuch(_kurzinfo_bundle()))
    assert issues == [], issues
