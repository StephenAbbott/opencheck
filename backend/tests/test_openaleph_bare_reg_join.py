"""A bare FtM ``REG`` joins the subject in a country the record itself lists.

The BODS quality sweep of 23 Nov 2026 failed ``declaration_subject_not_subject|
openaleph`` on Bank Saderat PLC (``2138008KTNTDICZU8L25``). OpenAleph's OFAC
SDN and US Trade CSL records of the bank file ``registrationNumber 01126618``
with ``country: [ir, gb]``, so ``ftm_registration_country`` leaves the number
bare (``REG``), and a bare ``REG`` gave no ``JUR:`` key: the two records joined
the subject's referent group only through OpenAleph's Companies House record,
and a run whose answer left that record out named two subjects.

PR 697 stamps such a record as its source's subject when the lookup accepted it
on a shared identifier. This is the join itself (the ticket's option 1): the
number gets the jurisdiction key for each country the record names — its
jurisdiction and its addresses' countries — and never for any other.
"""

from __future__ import annotations

import copy
from typing import Any

from opencheck.bods import map_openaleph
from opencheck.bods.declaration import stamp_merged
from opencheck.bods.statements import make_entity_statement
from opencheck.bods_quality import check_declaration_subjects
from opencheck.reconcile import _identifier_keys
from opencheck.subject_profile import subject_statements

LEI = "2138008KTNTDICZU8L25"  # Bank Saderat PLC

# GET /deepen?source=openaleph&hit_id=trade-csl-4642…, 24 Nov 2026 — trimmed to
# the fields the mapper reads.
TRADE_CSL: dict[str, Any] = {
    "id": "trade-csl-4642.8d500a16155f1f5f67b410265ff712ce24539894",
    "schema": "Organization",
    "caption": "BANK SADERAT PLC",
    "collection": {"foreign_id": "us_trade_csl"},
    "properties": {
        "name": ["BANK SADERAT PLC"],
        "country": ["ir", "gb"],
        "registrationNumber": ["01126618"],
        "addressEntity": [
            {
                "schema": "Address",
                "id": "addr-ir-1",
                "caption": "PO Box 15175/584, 6th Floor, Sadaf Bldg, 1137 Vali Asr Ave, Tehran, 15119-43885, ir",
                "properties": {
                    "full": ["PO Box 15175/584, 6th Floor, Sadaf Bldg, 1137 Vali Asr Ave, Tehran, 15119-43885, ir"],
                    "country": ["ir"],
                },
            },
            {
                "schema": "Address",
                "id": "addr-gb-1",
                "caption": "5 Lothbury, London, EC2R 7HD, gb",
                "properties": {
                    "full": ["5 Lothbury, London, EC2R 7HD, gb"],
                    "country": ["gb"],
                },
            },
        ],
    },
}


def _ofac_sdn() -> dict[str, Any]:
    rec = copy.deepcopy(TRADE_CSL)
    rec["id"] = "ofac-sdn-4642.21cfb54c8371bee7617c572321e65c468e814e56"
    rec["schema"] = "Company"
    rec["collection"] = {"foreign_id": "us_ofac_sdn"}
    return rec


def _entities(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return [s for s in map_openaleph(raw) if s.get("recordType") == "entity"]


def _subject_from_gleif_and_ch() -> list[dict[str, Any]]:
    gleif = make_entity_statement(
        source_id="gleif", local_id=LEI, name="BANK SADERAT PLC",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[{"scheme": "XI-LEI", "id": LEI}, {"scheme": "GB-COH", "id": "01126618"}],
    )
    ch = make_entity_statement(
        source_id="companies_house", local_id="01126618", name="BANK SADERAT PLC",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[{"scheme": "GB-COH", "id": "01126618"}],
    )
    return [gleif, ch]


def test_the_sanctions_list_records_carry_a_bare_reg_and_a_gb_jurisdiction_key():
    [stmt] = _entities(TRADE_CSL)
    reg = [i for i in stmt["recordDetails"]["identifiers"] if i["scheme"].startswith("REG")]
    assert [i["scheme"] for i in reg] == ["REG"]  # two countries: left bare
    keys = _identifier_keys(stmt)
    assert "JUR:GB:01126618" in keys
    assert "JUR:IR:01126618" in keys


def test_without_openalephs_companies_house_record_both_join_the_subject_and_name_one():
    """The sweep's run: OpenAleph answered with the two sanctions-list records
    and not ``oc-companies-gb-01126618``."""
    oa = _entities(TRADE_CSL) + _entities(_ofac_sdn())
    merged = _subject_from_gleif_and_ch() + oa
    group = {s["statementId"] for s in subject_statements(LEI, merged)}
    assert {s["statementId"] for s in oa} <= group
    out = stamp_merged(merged, LEI)
    named = {
        s["declarationSubject"] for s in out
        if (s.get("source") or {}).get("opencheckSourceId") == "openaleph"
    }
    assert len(named) == 1 and named <= {s["recordId"] for s in oa}
    assert check_declaration_subjects(out) == []


def test_a_country_the_record_does_not_list_gives_no_key():
    rec = copy.deepcopy(TRADE_CSL)
    rec["properties"]["country"] = ["ir", "de"]
    rec["properties"]["addressEntity"][1]["properties"]["country"] = ["de"]
    [stmt] = _entities(rec)
    keys = _identifier_keys(stmt)
    assert "JUR:GB:01126618" not in keys
    merged = _subject_from_gleif_and_ch() + [stmt]
    assert stmt["statementId"] not in {s["statementId"] for s in subject_statements(LEI, merged)}


def test_a_companies_house_bare_reg_is_not_read_from_its_address():
    """Companies House writes a bare ``REG`` for a corporate officer whose
    country it reads from a service address; that says where post goes, not
    which register holds the number."""
    officer = make_entity_statement(
        source_id="companies_house", local_id="officer-1", name="SOME HOLDINGS SA",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[{"scheme": "REG", "id": "01126618", "schemeName": "Company register"}],
    )
    assert not any(k.startswith("JUR:") for k in _identifier_keys(officer))
