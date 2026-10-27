"""A bare OpenAleph ``REG`` number gets its country from the record, never a guess.

Found while investigating the BODS quality sweep of 26 Oct 2026
(``declaration_subject_not_subject|openaleph`` on Regulatory DataCorp and
A.P. Møller - Mærsk): some OpenAleph records of the subject carried a
registration number under the bare scheme ``REG`` with no jurisdiction, so the
24 Oct ``REG-<country>`` → ``JUR:<country>:`` bridge could not join them to
the subject's referent group.

Read on 26 Oct 2026, neither record fills ``jurisdiction`` or ``country`` —
the properties the mapper already read:

* Regulatory DataCorp Limited's UK Find a Tender party record carries the
  country in its **structured** address (``addressEntity.country: gb``).
* Mærsk's overseas-entity PSC record (OE030334) carries it only in free text
  ("…1263 København K, Denmark") and in ``legalForm``.

Stephen's decisions (26 Oct 2026): the structured address may qualify the
scheme, free text may not; scheme only — the statement's jurisdiction is left
empty, so an address country never reaches the risk checks.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from opencheck.bods import map_openaleph
from opencheck.bods.annotations import resolve_pointer, validate_annotations
from opencheck.bods.mappers.ftm import ftm_registration_country
from opencheck.bods.statements import make_entity_statement
from opencheck.subject_profile import subject_statements

# GET /api/2/entities/{id}, 26 Oct 2026 — trimmed to the fields the mapper reads.
RDC_FTS: dict[str, Any] = {
    "id": "ocds-party-gb-fts-88512.55954de47feef47ec3ccc7275cb7d51ca5a9ad09",
    "schema": "LegalEntity",
    "caption": "REGULATORY DATACORP LIMITED",
    "collection": {"foreign_id": "gb_ocds_find_a_tender_service"},
    "properties": {
        "address": [
            "Citypoint 16th Floor, One Ropemaker Street, London, EC2Y 9AW, United Kingdom"
        ],
        "registrationNumber": ["08739364"],
        "name": ["REGULATORY DATACORP LIMITED"],
        "addressEntity": [
            {
                "caption": "Citypoint 16th Floor, One Ropemaker Street, London, EC2Y 9AW, United Kingdom",
                "schema": "Address",
                "id": "addr-gb-732f717ffd776715d359727a8820c1a4e455a620.432fe5088e2855d6b7da8892897a1b1560c53aad",
                "properties": {
                    "full": [
                        "Citypoint 16th Floor, One Ropemaker Street, London, EC2Y 9AW, United Kingdom"
                    ],
                    "postalCode": ["EC2Y 9AW"],
                    "country": ["gb"],
                    "city": ["London,"],
                    "region": ["UK"],
                    "street": ["Citypoint 16th Floor, One Ropemaker Street"],
                },
            }
        ],
    },
}

MAERSK_OE: dict[str, Any] = {
    "id": "gb-coh-psc-OE030334-qo7-tu8-wumrri-vfkzr27uusx8.c9880f29f92ea1eb71c7f8838d93a5d7b7f1b1a8",
    "schema": "Company",
    "caption": "A.P. Møller - Mærsk A/S",
    "collection": {"foreign_id": "gb_coh_psc"},
    "properties": {
        "address": ["Esplanaden 50, 1263 København K, Denmark"],
        "legalForm": ["Denmark", "Joint Stock Company"],
        "name": ["A.P. Møller - Mærsk A/S"],
        "registrationNumber": ["22756214"],
    },
}


def _entity(bundle: dict[str, Any]) -> dict[str, Any]:
    [stmt] = [s for s in map_openaleph(bundle) if s.get("recordType") == "entity"]
    return stmt


def _reg(stmt: dict[str, Any]) -> list[dict[str, str]]:
    return [
        i for i in stmt["recordDetails"].get("identifiers") or []
        if str(i.get("scheme", "")).startswith("REG")
    ]


# ---------------------------------------------------------------------------
# The two production records
# ---------------------------------------------------------------------------


def test_regulatory_datacorp_reads_gb_from_its_structured_address():
    stmt = _entity(RDC_FTS)
    assert _reg(stmt) == [
        {"id": "08739364", "scheme": "REG-GB", "schemeName": "Local registry identifier"}
    ]
    # Scheme only: no jurisdiction is asserted from an address.
    assert "jurisdiction" not in stmt["recordDetails"]


def test_the_address_reading_is_said_on_the_identifier():
    stmt = _entity(RDC_FTS)
    [note] = [a for a in stmt.get("annotations") or [] if a["motivation"] == "transformation"
              and "addressEntity" in a["description"]]
    assert resolve_pointer(stmt, note["statementPointerTarget"]) == "REG-GB"
    assert note["transformedContent"] == "REG"
    assert "08739364" in note["description"] and "no jurisdiction or country" in note["description"]
    assert validate_annotations(stmt) == []


def test_maersk_free_text_address_leaves_the_number_bare():
    stmt = _entity(MAERSK_OE)
    assert [i["scheme"] for i in _reg(stmt)] == ["REG"]
    assert not any("addressEntity" in a.get("description", "")
                   for a in stmt.get("annotations") or [])


def test_regulatory_datacorp_joins_the_subject_referent_group():
    """The point of the ticket: the Find a Tender record now joins GLEIF's
    record of the subject through ``JUR:GB:08739364``."""
    lei = "894500LD30X1VN839203"
    gleif = make_entity_statement(
        source_id="gleif",
        local_id=lei,
        name="REGULATORY DATACORP LIMITED",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[
            {"id": lei, "scheme": "XI-LEI", "schemeName": "Legal Entity Identifier"},
            {"id": "08739364", "scheme": "GB-COH", "schemeName": "Companies House"},
        ],
    )
    oa = _entity(RDC_FTS)
    members = {s["statementId"] for s in subject_statements(lei, [gleif, oa])}
    assert oa["statementId"] in members

    bare = _entity(MAERSK_OE)
    # Without the address reading the same record would not have joined.
    unqualified = copy.deepcopy(RDC_FTS)
    del unqualified["properties"]["addressEntity"]
    assert _entity(unqualified)["statementId"] not in {
        s["statementId"] for s in subject_statements(lei, [gleif, _entity(unqualified)])
    }
    assert bare  # mapped, just not joined by country


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "props, read_address, expected",
    [
        ({"jurisdiction": ["dk"]}, False, ("DK", "jurisdiction")),
        ({"jurisdiction": ["Denmark", "dk"]}, False, ("DK", "jurisdiction")),
        # Two different countries: bare, never the first one.
        ({"jurisdiction": ["gb", "ie"]}, False, (None, "")),
        # jurisdiction decides when filled; country is not consulted.
        ({"jurisdiction": ["gb", "ie"], "country": ["gb"]}, False, (None, "")),
        ({"country": ["ru"]}, False, ("RU", "country")),
        # Address only when asked (entities), and only structured.
        (RDC_FTS["properties"], False, (None, "")),
        (RDC_FTS["properties"], True, ("GB", "address")),
        (MAERSK_OE["properties"], True, (None, "")),
        # Every expanded address must name the same one country.
        ({"addressEntity": [
            {"properties": {"country": ["gb"]}},
            {"properties": {"country": ["ie"]}},
        ]}, True, (None, "")),
        ({"addressEntity": [
            {"properties": {"country": ["gb"]}},
            {"properties": {"city": ["Dublin"]}},
        ]}, True, (None, "")),
        # An unexpanded reference says nothing either way.
        ({"addressEntity": ["addr-gb-1234", {"properties": {"country": ["gb"]}}]}, True,
         ("GB", "address")),
        ({"addressEntity": ["addr-gb-1234"]}, True, (None, "")),
    ],
)
def test_registration_country_rule(props, read_address, expected):
    assert ftm_registration_country(props, read_address=read_address) == expected


def test_a_jurisdiction_record_carries_no_address_note():
    bundle = copy.deepcopy(RDC_FTS)
    bundle["properties"]["jurisdiction"] = ["gb"]
    stmt = _entity(bundle)
    assert [i["scheme"] for i in _reg(stmt)] == ["REG-GB"]
    assert not any("addressEntity" in a.get("description", "")
                   for a in stmt.get("annotations") or [])
