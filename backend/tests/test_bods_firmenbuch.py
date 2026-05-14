"""Tests for the Austrian Firmenbuch → BODS v0.4 adapter and mapper.

Covers:
  - FN normalisation utility
  - SOAP response XML parsers (search + extract)
  - map_firmenbuch: entity, officer, and shareholder statement shapes
  - BODS validator compliance for all fixture combinations
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


def test_is_valid_fn_typical() -> None:
    assert is_valid_fn("473888w")
    assert is_valid_fn("366715m")


def test_is_valid_fn_rejects_digits_only() -> None:
    assert not is_valid_fn("473888")


def test_is_valid_fn_rejects_empty() -> None:
    assert not is_valid_fn("")


def test_at_fb_ra_code() -> None:
    assert AT_FB_RA_CODE == "RA000017"


# ---------------------------------------------------------------------------
# SOAP search response parser
# ---------------------------------------------------------------------------

_SEARCH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope">
  <soapenv:Body>
    <SUCHERESPONSE>
      <FIRMA>
        <FIRMA_ID>473888w</FIRMA_ID>
        <FIRMENWORTLAUT>Muster GmbH</FIRMENWORTLAUT>
        <FN>473888w</FN>
        <STATUS>AKTIV</STATUS>
        <RECHTSFORM>GmbH</RECHTSFORM>
      </FIRMA>
      <FIRMA>
        <FIRMA_ID>366715m</FIRMA_ID>
        <FIRMENWORTLAUT>Beispiel KG</FIRMENWORTLAUT>
        <FN>366715m</FN>
        <STATUS>AKTIV</STATUS>
        <RECHTSFORM>KG</RECHTSFORM>
      </FIRMA>
    </SUCHERESPONSE>
  </soapenv:Body>
</soapenv:Envelope>"""

_SEARCH_XML_EMPTY = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope">
  <soapenv:Body><SUCHERESPONSE /></soapenv:Body>
</soapenv:Envelope>"""

_SEARCH_XML_MALFORMED = "this is not XML"


def test_parse_search_response_finds_two_hits() -> None:
    hits = _parse_search_response(_SEARCH_XML)
    assert len(hits) == 2


def test_parse_search_response_first_hit_name() -> None:
    hits = _parse_search_response(_SEARCH_XML)
    assert hits[0]["name"] == "Muster GmbH"


def test_parse_search_response_firma_ids() -> None:
    hits = _parse_search_response(_SEARCH_XML)
    ids = [h["firma_id"] for h in hits]
    assert "473888w" in ids
    assert "366715m" in ids


def test_parse_search_response_empty_returns_empty_list() -> None:
    assert _parse_search_response(_SEARCH_XML_EMPTY) == []


def test_parse_search_response_malformed_returns_empty_list() -> None:
    assert _parse_search_response(_SEARCH_XML_MALFORMED) == []


# ---------------------------------------------------------------------------
# SOAP extract response parser
# ---------------------------------------------------------------------------

_EXTRACT_XML_GMBH = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope">
  <soapenv:Body>
    <AUSZUGRESPONSE>
      <FIRMENWORTLAUT>Muster GmbH</FIRMENWORTLAUT>
      <FN>473888w</FN>
      <UID>ATU12345678</UID>
      <RECHTSFORM>GmbH</RECHTSFORM>
      <STATUS>AKTIV</STATUS>
      <GRUENDUNGSDATUM>15.03.1995</GRUENDUNGSDATUM>
      <STAMMKAPITAL>35000</STAMMKAPITAL>
      <GESCHAEFTSANSCHRIFT>
        <STRASSE>Musterstraße</STRASSE>
        <HAUSNUMMER>1</HAUSNUMMER>
        <PLZ>1010</PLZ>
        <ORT>Wien</ORT>
      </GESCHAEFTSANSCHRIFT>
      <FUN>
        <FUNKTION_CODE>GF</FUNKTION_CODE>
        <FUNKTION_TEXT>Geschäftsführer</FUNKTION_TEXT>
        <EINTRITTSDATUM>01.01.2010</EINTRITTSDATUM>
        <PERSON>
          <VORNAME>Max</VORNAME>
          <NACHNAME>Mustermann</NACHNAME>
          <GEBURTSDATUM>15.06.1970</GEBURTSDATUM>
        </PERSON>
      </FUN>
      <FUN>
        <FUNKTION_CODE>PK</FUNKTION_CODE>
        <FUNKTION_TEXT>Prokurist</FUNKTION_TEXT>
        <EINTRITTSDATUM>01.06.2015</EINTRITTSDATUM>
        <PERSON>
          <VORNAME>Anna</VORNAME>
          <NACHNAME>Musterfrau</NACHNAME>
        </PERSON>
      </FUN>
      <FUN>
        <FUNKTION_CODE>GF</FUNKTION_CODE>
        <FUNKTION_TEXT>Geschäftsführer</FUNKTION_TEXT>
        <EINTRITTSDATUM>01.01.2005</EINTRITTSDATUM>
        <LOESCHDATUM>31.12.2020</LOESCHDATUM>
        <PERSON>
          <VORNAME>Former</VORNAME>
          <NACHNAME>Director</NACHNAME>
        </PERSON>
      </FUN>
      <GESELLSCHAFTER>
        <PERSON>
          <VORNAME>Max</VORNAME>
          <NACHNAME>Mustermann</NACHNAME>
          <GEBURTSDATUM>15.06.1970</GEBURTSDATUM>
        </PERSON>
        <STAMMEINLAGE>17500</STAMMEINLAGE>
      </GESELLSCHAFTER>
      <GESELLSCHAFTER>
        <PERSON>
          <VORNAME>Anna</VORNAME>
          <NACHNAME>Musterfrau</NACHNAME>
        </PERSON>
        <STAMMEINLAGE>17500</STAMMEINLAGE>
      </GESELLSCHAFTER>
    </AUSZUGRESPONSE>
  </soapenv:Body>
</soapenv:Envelope>"""

_EXTRACT_XML_CORPORATE_SHAREHOLDER = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope">
  <soapenv:Body>
    <AUSZUGRESPONSE>
      <FIRMENWORTLAUT>Holding GmbH</FIRMENWORTLAUT>
      <FN>100000a</FN>
      <RECHTSFORM>GmbH</RECHTSFORM>
      <STATUS>AKTIV</STATUS>
      <STAMMKAPITAL>100000</STAMMKAPITAL>
      <FUN>
        <FUNKTION_CODE>GF</FUNKTION_CODE>
        <FUNKTION_TEXT>Geschäftsführer</FUNKTION_TEXT>
        <PERSON>
          <VORNAME>Hans</VORNAME>
          <NACHNAME>Investor</NACHNAME>
        </PERSON>
      </FUN>
      <GESELLSCHAFTER>
        <GESELLSCHAFT>
          <FIRMENWORTLAUT>Muttergesellschaft AG</FIRMENWORTLAUT>
        </GESELLSCHAFT>
        <FN>200000b</FN>
        <STAMMEINLAGE>100000</STAMMEINLAGE>
      </GESELLSCHAFTER>
    </AUSZUGRESPONSE>
  </soapenv:Body>
</soapenv:Envelope>"""

_EXTRACT_XML_KG = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope">
  <soapenv:Body>
    <AUSZUGRESPONSE>
      <FIRMENWORTLAUT>Muster KG</FIRMENWORTLAUT>
      <FN>999999z</FN>
      <RECHTSFORM>KG</RECHTSFORM>
      <STATUS>AKTIV</STATUS>
      <FUN>
        <FUNKTION_CODE>GF</FUNKTION_CODE>
        <FUNKTION_TEXT>Geschäftsführender Komplementär</FUNKTION_TEXT>
        <PERSON>
          <VORNAME>Klaus</VORNAME>
          <NACHNAME>Komplementaer</NACHNAME>
        </PERSON>
      </FUN>
      <KOMPLEMENTAER>
        <PERSON>
          <VORNAME>Klaus</VORNAME>
          <NACHNAME>Komplementaer</NACHNAME>
        </PERSON>
      </KOMPLEMENTAER>
      <KOMMANDITIST>
        <PERSON>
          <VORNAME>Maria</VORNAME>
          <NACHNAME>Kommanditistin</NACHNAME>
        </PERSON>
        <EINLAGE>50000</EINLAGE>
      </KOMMANDITIST>
    </AUSZUGRESPONSE>
  </soapenv:Body>
</soapenv:Envelope>"""


def test_parse_extract_name() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["name"] == "Muster GmbH"


def test_parse_extract_fn() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["fn"] == "473888w"


def test_parse_extract_uid() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["uid"] == "ATU12345678"


def test_parse_extract_founding_date_raw() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    # Parser returns raw; date normalisation happens in the mapper
    assert ext["founding_date"] == "15.03.1995"


def test_parse_extract_stamm_kapital() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert ext["stamm_kapital"] == 35000.0


def test_parse_extract_address() -> None:
    ext = _parse_extract_response(_EXTRACT_XML_GMBH)
    assert "Musterstraße" in ext["address"]
    assert "Wien" in ext["address"]


def test_parse_extract_officers_count() -> None:
    """Terminated officer (LOESCHDATUM set) should be excluded."""
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
    """GmbH with 2 officers + 2 shareholders → 1 entity + 4 person + 4 rel = 9."""
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


def test_map_firmenbuch_identifier_uid() -> None:
    stmts = list(map_firmenbuch(_bundle()))
    schemes = {i["scheme"] for i in stmts[0]["recordDetails"]["identifiers"]}
    assert "AT-UID" in schemes


def test_map_firmenbuch_founding_date_normalised() -> None:
    """15.03.1995 should be normalised to 1995-03-15."""
    stmts = list(map_firmenbuch(_bundle()))
    assert stmts[0]["recordDetails"]["foundingDate"] == "1995-03-15"


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
    assert len(persons) == 4  # 2 officers + 2 shareholders (no overlap in fixture)


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
    """Officers should have beneficialOwnershipOrControl=False."""
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
