"""Tests for the Zambia EITI data portal adapter, mapper and index builder.

Three layers:
* the build script's pure helpers (name normalisation, TPIN parsing, the
  one-table-per-year tax rule, the placeholder date, the blank gender share);
* fixture-driven adapter / mapper / hit-builder / finding tests;
* a regression test that runs the real committed index through the mapper.
"""

from __future__ import annotations

import pytest

from opencheck.bods import map_eiti_zambia, validate_shape
from opencheck.findings import MAX_FINDING_CHARS, finding_eiti_zambia
from opencheck.routers.lookup import _bh_eiti_zambia, _build_result_hit, _LookupCtx
from opencheck.sources import REGISTRY, eiti_zambia
from opencheck.sources.base import SearchKind
from scripts import build_eiti_zambia_index as build

_LEI = "2549008ZVFBSUO8W2L37"  # Kansanshi Mining PLC
_LEI_NAME_ONLY = "984500ECC4F4DF950B64"  # Blaze Metals — licences only, no TPIN
_LEI_MISS = "9999009999999999XX99"

_RECORD = {
    "lei": _LEI,
    "gleif_legal_name": "Kansanshi Mining PLC",
    "lei_registration_status": "ISSUED",
    "tpins": ["1001602517"],
    "names_as_filed": ["KANSANSHI MINING PLC", "Kansanshi"],
    "match": {"method": "tpin_via_name", "confidence": "medium", "aliases": []},
    "zra_tax": [
        {"year": "2024", "dataset": "zra-revenue-payments-2024", "payments": 156,
         "amounts_summed": True, "total_zmw": 6383073000.0,
         "by_tax_type": [{"tax_type": "Mineral Royalty", "zmw": 3011500000.0}]},
        {"year": "2022", "dataset": "revenue-payments-eiti-2022-2023", "payments": 663,
         "amounts_summed": True, "total_zmw": 13996727386.0,
         "by_tax_type": [{"tax_type": "Income Tax", "zmw": 7531649291.0}]},
    ],
    "eiti_reconciliation": [
        {"year": "2023", "total_zmw": 6294754362.6, "total_usd": 0.0, "lines": 27,
         "by_receiving_entity": [{"entity": "ZRA", "zmw": 6221127013.3, "usd": 0.0}]},
    ],
    "employment": [{"year": "2022", "dataset": "company-employment-data-in-2022",
                    "employees": 3146, "domestic": 3036, "expatriate": 110, "women_share": None}],
    "licences": [],
    "water_offences": [],
    "datasets_used": ["payment-report", "zra-revenue-payments-2024"],
}

_NAME_ONLY_RECORD = {
    "lei": _LEI_NAME_ONLY,
    "gleif_legal_name": "BLAZE METALS RESOURCES LIMITED",
    "lei_registration_status": "ISSUED",
    "tpins": [],
    "names_as_filed": [],
    "match": {"method": "name_only", "confidence": "medium", "aliases": []},
    "zra_tax": [],
    "eiti_reconciliation": [],
    "employment": [],
    "licences": [{"code": "36066-HQ-LEL", "type": "LEL", "status": "Active",
                  "commodities": "Au. Cu", "area": "40.7543 ha", "location": "Copperbelt. Lufwanyama",
                  "grant_date": "02/05/2024", "expiry_date": None,
                  "holder_as_filed": "Blaze Metals Resources Limited", "holder_share_pct": None}],
    "water_offences": [{"name_as_filed": "Blaze Metals Resources", "offence": "Illegal Water Abstraction from Kafue River",
                        "activity": "Mining", "permit": "Not permitted"}],
    "datasets_used": ["mining-and-non-mining-rights-2023-2025-q2", "mining-companies-offences"],
}

_META = {
    "built": "2026-10-07T13:00:00+00:00",
    "licence": "ZEITI Open Data Policy (2016) — no named licence; see policy",
    "licence_url": eiti_zambia.POLICY_URL,
    "gleif_zm_records": 69,
    "datasets": {"payment-report": {"name": "Payment Report"}},
}


@pytest.fixture(autouse=True)
def _inject_index(monkeypatch):
    monkeypatch.setattr(eiti_zambia, "_index", {_LEI: _RECORD, _LEI_NAME_ONLY: _NAME_ONLY_RECORD})
    monkeypatch.setattr(eiti_zambia, "_meta", dict(_META))
    yield
    eiti_zambia._reset_index_for_tests()


@pytest.fixture
def adapter():
    return eiti_zambia.EitiZambiaAdapter()


def _ctx(lei, name):
    return _LookupCtx(
        lei=lei, legal_name=name, jurisdiction="ZM", registered_as="",
        derived={}, ocid=None, spglobal=None, qid=None,
    )


# ---------------------------------------------------------------------------
# Build helpers
# ---------------------------------------------------------------------------


def test_norm_name_strips_share_forms_and_parenthesised_zambia():
    assert build.norm_name("Grovenor Resources Zambia Ltd (100%)") == "GROVENOR RESOURCES ZAMBIA"
    assert build.norm_name("GROVENOR RESOURCES ZAMBIA LIMITED") == "GROVENOR RESOURCES ZAMBIA"
    assert build.norm_name("Konkola Copper Mines Plc.") == build.norm_name("KONKOLA COPPER MINES PLC")
    assert build.norm_name("Acme (Z) Limited") == "ACME"
    # Bare ZAMBIA is kept: "X Zambia Ltd" and "X Ltd" are different companies.
    assert build.norm_name("Acme Zambia Ltd") != build.norm_name("Acme Ltd")


def test_holder_share_reads_the_cadastre_suffix():
    assert build.holder_share("BUNTINGWA RESOURCES LIMITED  (100%)") == 100.0
    assert build.holder_share("Blaze Metals Resources Limited") is None


def test_base_tpin_takes_the_ten_digits_before_a_slash():
    assert build.base_tpin("1001831030/62959") == "1001831030"
    assert build.base_tpin(" 1001602517 ") == "1001602517"
    assert build.base_tpin("100166382") is None  # nine digits: a typo, not a TPIN
    assert build.base_tpin("1123") is None
    assert build.base_tpin("10016023171") is None


def test_search_terms_are_verbatim_two_word_prefixes():
    assert build._search_terms(["ZCCM - IH Investments Holdings Plc"]) == ["ZCCM - IH"]
    assert build._search_terms(["ZAMBIA NATIONAL COMMERCIAL BANK PLC"]) == ["ZAMBIA NATIONAL"]


def test_zra_tax_takes_one_table_per_year_and_never_sums_the_blank_2024_table():
    rows = {
        "zra-revenue-payments-2024": [],
        "zra-tax-revenue-2024": [
            {"TPIN": "1", "Tax Type": "Pay As You Earn", "Date": "12/04/2024", "Amount paid (KMW)": ""},
            {"TPIN": "1", "Tax Type": "Pay As You Earn", "Date": "12/05/2024", "Amount paid (KMW)": 3.5},
        ],
        "revenue-payments-eiti-2022-2023": [
            {"Tax Type": "Income Tax", "Payment Date": "13/01/2022", "Amount": 100},
            # A 2023 row in the 2022-2023 table is left to the 2023 table.
            {"Tax Type": "Income Tax", "Payment Date": "13/01/2023", "Amount": 999},
        ],
        "zra-tax-payment-in-2023-detailed-service-values": [
            {"Tax Type": "Mineral Royalty", "Payment Date": "01/02/2023", "Amount (ZMW)": 50.5},
        ],
    }
    out = {e["year"]: e for e in build._zra_tax(rows)}
    assert out["2022"]["total_zmw"] == 100
    assert out["2023"]["total_zmw"] == 50.5
    assert out["2024"]["amounts_summed"] is False
    assert "total_zmw" not in out["2024"]
    assert out["2024"]["payments"] == 2


def test_zra_tax_prefers_the_clean_2024_table_and_scales_millions():
    rows = {
        "zra-revenue-payments-2024": [
            {"Tax Type": "Income Tax", "Compilation Date": "31/12/2024", "ZMW (Millions)": 1.5},
        ],
        "zra-tax-revenue-2024": [
            {"Tax Type": "Income Tax", "Date": "12/04/2024", "Amount paid (KMW)": 7},
        ],
    }
    (entry,) = build._zra_tax(rows)
    assert entry["dataset"] == "zra-revenue-payments-2024"
    assert entry["total_zmw"] == 1_500_000


def test_cadastre_placeholder_date_is_dropped():
    (lic,) = build._licences([{
        "Company Name": "Grovenor Resources Zambia Ltd (100%)", "Code": "40948-HQ-LEL",
        "Grant Date": "13/08/2025", "Expiry Date": "01/01/2023",
    }])
    assert lic["grant_date"] == "13/08/2025"
    assert lic["expiry_date"] is None
    assert lic["holder_share_pct"] == 100.0
    assert lic["holder_as_filed"] == "Grovenor Resources Zambia Ltd"


def test_employment_without_a_gender_breakdown_has_no_share():
    (row,) = build._employment({"company-employment-data-in-2022": [
        {"Year": "31/12/2022", "Male": 0, "Female": 0, "No of Employees": 3146,
         "Share of Women employed": 0},
    ]})
    assert row["women_share"] is None
    assert row["employees"] == 3146


def test_reconciliation_keeps_the_above_threshold_line_and_cleans_its_label():
    (year,) = build._reconciliation([
        {"Reporting Date": "31/12/2022", "Receipting Entity": "ZRA", "Paid in ZMW": 10,
         "Other Payments Made in USD": 0},
        {"Reporting Date": "31/12/2022", "Receipting Entity": "All", "Paid in ZMW": 5,
         "Other Payments Made in USD": 0, "Type of Payment": ">\ufffdZMW\ufffd20\ufffdmillion"},
    ])
    assert year["total_zmw"] == 15
    assert [e["entity"] for e in year["by_receiving_entity"]] == ["ZRA", "All"]


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


async def test_info(adapter):
    info = adapter.info
    assert info.id == "eiti_zambia"
    assert info.category == "esg"
    assert info.requires_api_key is False
    assert SearchKind.ENTITY in info.supports
    assert "open data policy" in info.license.lower()
    assert "non-commercial" not in info.license.lower()


async def test_search_is_empty(adapter):
    assert await adapter.search("Kansanshi", SearchKind.ENTITY) == []


async def test_fetch_by_lei_match_miss_and_stub(adapter):
    b = await adapter.fetch_by_lei(_LEI.lower())
    assert b is not None and b["source_id"] == "eiti_zambia" and b["is_stub"] is False
    assert b["identifiers"] == {"zm_tpin": "1001602517"}
    assert b["licence_url"] == eiti_zambia.POLICY_URL
    assert b["datasets"]["payment-report"]["url"].endswith("/datasets/payment-report")
    assert await adapter.fetch_by_lei(_LEI_MISS) is None
    assert (await adapter.fetch(_LEI_MISS))["is_stub"] is True


async def test_covers_lei(adapter):
    assert adapter.covers_lei(_LEI)
    assert not adapter.covers_lei(_LEI_MISS)


async def test_name_only_record_asserts_nothing(adapter):
    b = await adapter.fetch_by_lei(_LEI_NAME_ONLY)
    assert b["identifiers"] == {}


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------


async def test_mapper_emits_one_entity_with_the_tpin_and_an_lei_annotation(adapter):
    stmts = list(map_eiti_zambia(await adapter.fetch_by_lei(_LEI)))
    assert validate_shape(stmts) == []
    assert [s["recordType"] for s in stmts] == ["entity"]
    details = stmts[0]["recordDetails"]
    assert details["identifiers"] == [{
        "id": "1001602517", "scheme": "ZM-TPIN",
        "schemeName": "Taxpayer Identification Number — Zambia Revenue Authority",
    }]
    # No jurisdiction: a tax number is not a place of incorporation.
    assert "jurisdiction" not in details
    assert "ZRA tax receipts 2024: ZMW 6.4 billion" in details["entityType"]["details"]
    (ann,) = [a for a in stmts[0]["annotations"] if a["motivation"] == "identifying"]
    assert _LEI in ann["description"] and ann["url"].endswith(_LEI)
    assert "not an identifier the portal asserts" in ann["description"]
    assert stmts[0]["source"]["type"] == ["thirdParty"]


async def test_mapper_name_only_record(adapter):
    stmts = list(map_eiti_zambia(await adapter.fetch_by_lei(_LEI_NAME_ONLY)))
    assert validate_shape(stmts) == []
    details = stmts[0]["recordDetails"]
    assert details["identifiers"] == []
    assert "1 mining right in the cadastre" in details["entityType"]["details"]
    assert "mining-rights cadastre" in stmts[0]["annotations"][0]["description"]


def test_mapper_ignores_stubs():
    assert list(map_eiti_zambia({"is_stub": True})) == []
    assert list(map_eiti_zambia({})) == []


# ---------------------------------------------------------------------------
# Hit builder and finding
# ---------------------------------------------------------------------------


async def test_hit_builder_asserts_the_tpin_not_the_lei(adapter):
    b = await adapter.fetch_by_lei(_LEI)
    hit = _bh_eiti_zambia(b, _ctx(_LEI, "Kansanshi Mining PLC"))
    assert hit.identifiers == {"zm_tpin": "1001602517"}
    assert "lei" not in hit.identifiers
    assert hit.summary == "ZM-TPIN 1001602517 · ZRA tax 2022–2024"
    assert _build_result_hit("eiti_zambia", b, _ctx(_LEI, "x")) is not None
    assert _build_result_hit("eiti_zambia", None, _ctx(_LEI_MISS, "x")) is None


def test_finding_counts_payments_never_sums():
    assert finding_eiti_zambia(_RECORD) == (
        "156 tax payments recorded by the Zambia Revenue Authority in 2024, "
        "no mining rights matched in the cadastre."
    )


def test_finding_leads_with_a_warma_listing():
    finding = finding_eiti_zambia(_NAME_ONLY_RECORD)
    assert finding == (
        "Listed by WARMA: Illegal Water Abstraction from Kafue River, "
        "1 mining right in the cadastre."
    )
    assert len(finding) <= MAX_FINDING_CHARS


def test_finding_degrades():
    assert finding_eiti_zambia({"is_stub": True}) is None
    assert finding_eiti_zambia({}) is None


def test_registered_in_registry():
    assert REGISTRY.get("eiti_zambia") is not None


# ---------------------------------------------------------------------------
# Regression: the real committed index
# ---------------------------------------------------------------------------


async def test_committed_index_maps_and_validates():
    eiti_zambia._reset_index_for_tests()
    adapter = eiti_zambia.EitiZambiaAdapter()
    index, meta = eiti_zambia._load()
    assert len(index) >= 12
    # Lafarge Cement Zambia is deliberately out (see the build script).
    assert "529900WBXE4UFXCA9422" not in index
    assert meta["licence_url"] == eiti_zambia.POLICY_URL
    for lei, record in index.items():
        bundle = await adapter.fetch_by_lei(lei)
        stmts = list(map_eiti_zambia(bundle))
        assert validate_shape(stmts) == [], f"{lei} produced invalid BODS"
        hit = _bh_eiti_zambia(bundle, _ctx(lei, record["gleif_legal_name"]))
        assert "lei" not in hit.identifiers
        finding = hit.finding
        assert finding is None or len(finding) <= MAX_FINDING_CHARS
        # Never a sum across tables: one ZRA entry per year.
        years = [t["year"] for t in record["zra_tax"]]
        assert len(years) == len(set(years))
    eiti_zambia._reset_index_for_tests()


async def test_committed_kansanshi_is_joined_on_tpin_not_name():
    """The EITI payment report files Kansanshi as "Kansanshi": found by TPIN."""
    eiti_zambia._reset_index_for_tests()
    index, _ = eiti_zambia._load()
    k = index[_LEI]
    assert k["tpins"] == ["1001602517"]
    assert {e["year"] for e in k["eiti_reconciliation"]} == {"2021", "2022", "2023"}
    eiti_zambia._reset_index_for_tests()
