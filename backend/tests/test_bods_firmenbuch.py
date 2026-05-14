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
    _parse_search_response,
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
# The real search response uses namespace-prefixed elements.  After namespace
# stripping the parser looks for TREFFER elements with FNR attribute and
# BEZEICHNUNG children.
# ---------------------------------------------------------------------------

_SEARCH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns14:SUCHEFIRMARESPONSE
        xmlns:ns14="ns://firmenbuch.justiz.gv.at/Abfrage/SucheFirmaResponse">
      <ns14:TREFFERLISTE>
        <ns14:TREFFER ns14:FNR="473888 w" ns14:AUFRECHT="true">
          <ns14:BEZEICHNUNG>Muster GmbH</ns14:BEZEICHNUNG>
          <ns14:RECHTSFORM>GmbH</ns14:RECHTSFORM>
        </ns14:TREFFER>
        <ns14:TREFFER ns14:FNR="366715 m" ns14:AUFRECHT="true">
          <ns14:BEZEICHNUNG>Beispiel KG</ns14:BEZEICHNUNG>
          <ns14:RECHTSFORM>KG</ns14:RECHTSFORM>
        </ns14:TREFFER>
      </ns14:TREFFERLISTE>
    </ns14:SUCHEFIRMARESPONSE>
  </env:Body>
</env:Envelope>"""

_SEARCH_XML_EMPTY = """<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header/>
  <env:Body>
    <ns14:SUCHEFIRMARESPONSE
        xmlns:ns14="ns://firmenbuch.justiz.gv.at/Abfrage/SucheFirmaResponse">
      <ns14:TREFFERLISTE/>
    </ns14:SUCHEFIRMARESPONSE>
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
