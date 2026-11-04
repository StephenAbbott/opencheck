"""The normalised disclosure status per source (``opencheck.disclosure``).

Stephen's decisions of 3 Nov 2026 are in the module docstring; each test
below pins one of them on statements the real mappers produce.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from opencheck import disclosure
from opencheck.bods.declaration import stamp_bundle
from opencheck.bods.mapper import map_companies_house, map_gleif
from opencheck.coverage import source_coverage

from .test_bods_mapper import _sample_bundle
from .test_lei_registration import afpc_client  # noqa: F401 — fixture

AS_OF = "2026-11-03"
PKG = Path(__file__).resolve().parents[1] / "opencheck"
FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "disclosureStatus.ts"


def _ch(payload: dict) -> list[dict]:
    return stamp_bundle(
        list(map_companies_house(payload)),
        source_id="companies_house",
        hit_id=payload["company_number"],
    )


def _gleif(**parents) -> list[dict]:
    bundle = {
        "lei": "213800LBDB8WB3QGVN21",
        "record": {
            "id": "213800LBDB8WB3QGVN21",
            "attributes": {
                "lei": "213800LBDB8WB3QGVN21",
                "entity": {
                    "legalName": {"name": "TEST GMBH"},
                    "jurisdiction": "DE",
                    "registeredAt": {"id": "RA000561", "other": None},
                    "legalAddress": {"addressLines": ["Musterstr. 1"], "city": "Berlin",
                                     "postalCode": "10115", "country": "DE"},
                },
            },
        },
        "direct_parent": None,
        "ultimate_parent": None,
        "direct_parent_exception": None,
        "ultimate_parent_exception": None,
        "direct_children": [],
    }
    bundle.update(parents)
    return stamp_bundle(list(map_gleif(bundle)), source_id="gleif", ref="213800LBDB8WB3QGVN21")


def _values(entries: list[dict]) -> list[tuple[str, str | None]]:
    return [(e["value"], e.get("covers")) for e in disclosure._merge(entries)]


def _psc_statements(*codes: str) -> dict:
    return {"items": [{"statement": c, "notified_on": "2017-01-01", "etag": f"s{i}"}
                      for i, c in enumerate(codes)]}


# --- reading one source -------------------------------------------------------


def test_companies_house_lists_every_value_it_supports() -> None:
    """Three decisions in one: the PSCs are declared, the unidentified owner is
    not provided, and both are listed — no precedence picks one."""
    payload = _sample_bundle()
    payload["psc_statements"] = _psc_statements("psc-exists-but-not-identified")
    entries = disclosure.entries_from_statements("companies_house", _ch(payload), AS_OF)
    merged = disclosure._merge(entries)
    assert _values(entries) == [
        ("declared", "beneficial_owners"),
        ("not_provided", "beneficial_owners"),
    ]
    assert merged[0]["count"] == 2  # Jane Smith and the corporate PSC
    assert merged[1]["reasons"] == ["subjectUnableToConfirmOrIdentifyBeneficialOwner"]


def test_no_registrable_person_is_none_exist() -> None:
    payload = {
        "company_number": "00088888",
        "profile": {"company_name": "Gap Co Ltd", "company_number": "00088888"},
        "officers": {"items": []},
        "pscs": {"items": []},
        "psc_statements": _psc_statements("no-individual-or-entity-with-signficant-control"),
    }
    entries = disclosure.entries_from_statements("companies_house", _ch(payload), AS_OF)
    assert _values(entries) == [("none_exist", "beneficial_owners")]


def test_contacted_no_response_is_not_provided() -> None:
    payload = {
        "company_number": "00088888",
        "profile": {"company_name": "Gap Co Ltd", "company_number": "00088888"},
        "officers": {"items": []},
        "pscs": {"items": []},
        "psc_statements": _psc_statements("psc-contacted-but-no-response"),
    }
    entries = disclosure.entries_from_statements("companies_house", _ch(payload), AS_OF)
    assert _values(entries) == [("not_provided", "beneficial_owners")]


@pytest.mark.parametrize(
    ("exception", "value"),
    [("NATURAL_PERSONS", "exempt"), ("NO_KNOWN_PERSON", "exempt"),
     ("NON_CONSOLIDATING", "exempt"), ("NON_PUBLIC", "not_provided")],
)
def test_gleif_exceptions_are_about_the_parent(exception: str, value: str) -> None:
    stmts = _gleif(direct_parent_exception={"type": exception, "reason": exception})
    entries = disclosure.entries_from_statements("gleif", stmts, AS_OF)
    assert _values(entries) == [(value, "parent")]
    assert disclosure._merge(entries)[0]["reasons"] == [exception]


def test_an_officer_is_never_an_owner() -> None:
    payload = _sample_bundle()
    payload["pscs"] = {"items": []}
    payload["officers"] = {"items": [{
        "name": "DOE, John", "officer_role": "director", "appointed_on": "2020-01-01",
        "links": {"officer": {"appointments": "/officers/abc/appointments"}},
    }]}
    entries = disclosure.entries_from_statements("companies_house", _ch(payload), AS_OF)
    assert all(e["value"] != "declared" for e in entries)


def test_a_source_that_is_not_an_ownership_source_reads_nothing() -> None:
    payload = _sample_bundle()
    assert disclosure.entries_from_statements("opensanctions", _ch(payload), AS_OF) == []


# --- one lookup ----------------------------------------------------------------


def _hit(source_id: str, *, found: bool = True) -> dict:
    return {"source_id": source_id, "hit_id": "x", "raw": {} if found else {"not_found": True},
            "is_stub": False}


def _lookup(bods: list[dict], hits: list[dict], *, errors=None, degraded=(),
            knowability=None, hints=None, countries=None) -> dict:
    cov = source_coverage(hits, errors or {}, [h["source_id"] for h in hits])
    return disclosure.for_lookup(
        bods=bods, coverage=cov, degraded=list(degraded), knowability=knowability,
        hints=hints, as_of=AS_OF, source_countries=countries or {},
    )


def test_a_record_with_no_owner_and_no_reason_is_none_filed() -> None:
    payload = {"company_number": "00077777",
               "profile": {"company_name": "Quiet Ltd", "company_number": "00077777"},
               "officers": {"items": []}, "pscs": {"items": []}}
    out = _lookup(_ch(payload), [_hit("companies_house")])
    assert out["sources"]["companies_house"]["values"] == ["none_filed"]


def test_unreachable_no_record_and_not_covered() -> None:
    out = _lookup(
        [],
        [_hit("companies_house"), _hit("nz_companies", found=False), _hit("opensanctions")],
        errors={"companies_house": "HTTP 503"},
        degraded=[{"source_id": "companies_house", "check": "source_fetch",
                   "reason": "upstream_error"}],
    )
    s = out["sources"]
    assert s["companies_house"]["values"] == ["unreachable"]
    assert s["nz_companies"]["values"] == ["no_record"]
    assert s["opensanctions"]["values"] == ["not_covered"]
    assert "opensanctions" in out["not_covered"]
    assert "gleif" in s  # the anchor is applicable on every LEI lookup
    # A value for every applicable source.
    assert set(s) >= {"companies_house", "nz_companies", "opensanctions", "gleif"}


def test_restricted_only_for_a_register_in_the_closed_country() -> None:
    knowability = {"code": "NL", "access": "legitimate_interest", "stated_absence": False}
    countries = {"kvk": "NL", "opensanctions": None, "brreg": "NO"}
    out = _lookup([], [_hit("kvk"), _hit("opensanctions"), _hit("brreg")],
                  knowability=knowability, countries=countries)
    s = out["sources"]
    assert s["kvk"]["entries"] == [{"value": "restricted", "covers": "beneficial_owners"}]
    assert s["opensanctions"]["values"] == ["not_covered"]
    assert s["brreg"]["values"] == ["not_covered"]


def test_a_public_register_is_never_restricted() -> None:
    knowability = {"code": "NL", "access": "public", "stated_absence": False}
    out = _lookup([], [_hit("kvk")], knowability=knowability, countries={"kvk": "NL"})
    assert out["sources"]["kvk"]["values"] == ["not_covered"]


def test_a_partial_read_adds_not_read_beside_what_was_read() -> None:
    payload = _sample_bundle()
    out = _lookup(_ch(payload), [_hit("companies_house")],
                  errors={"companies_house": "timeout"},
                  degraded=[{"source_id": "companies_house", "check": "source_read",
                             "reason": "timeout"}])
    assert out["sources"]["companies_house"]["values"] == ["declared", "unreachable"]


# --- Estonia: what the statements cannot say ------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"general": {"bo_obliged": False}}, ("exempt", "beneficial_owners")),
        ({"general": {"bo_obliged": True}}, ("none_filed", "beneficial_owners")),
        ({"bo_not_read": True}, ("unreachable", "beneficial_owners")),
        ({"beneficial_owners": [{"kontrolli_teostamise_viis": "J"}]},
         ("none_exist", "beneficial_owners")),
    ],
)
def test_estonia_hints(raw: dict, expected: tuple[str, str]) -> None:
    hints = disclosure.bundle_hints("ariregister", raw)
    assert [(e["value"], e["covers"]) for e in hints] == [expected]


def test_estonia_with_owners_needs_no_hint() -> None:
    raw = {"beneficial_owners": [{"kontrolli_teostamise_viis": "O"}],
           "general": {"bo_obliged": True}}
    assert disclosure.bundle_hints("ariregister", raw) == []


def test_a_hint_fills_the_gap_instead_of_none_filed() -> None:
    hints = {"ariregister": disclosure.bundle_hints("ariregister", {"general": {"bo_obliged": False}})}
    out = _lookup([], [_hit("ariregister")], hints=hints)
    vals = [(e["value"], e["covers"]) for e in out["sources"]["ariregister"]["entries"]]
    assert vals == [("exempt", "beneficial_owners"), ("none_filed", "shareholders")]


# --- BODS -----------------------------------------------------------------------


def test_bods_gets_one_annotation_on_the_subject_and_no_new_statement() -> None:
    stmts = _gleif(direct_parent_exception={"type": "NATURAL_PERSONS", "reason": "x"})
    payload = _lookup(stmts, [_hit("gleif")])
    subject = next(s for s in stmts if s["recordType"] == "entity"
                   and s["recordDetails"].get("name") == "TEST GMBH")
    out = disclosure.apply_to_bods(stmts, payload, [subject["statementId"]])
    assert len(out) == len(stmts)
    new = next(s for s in out if s["statementId"] == subject["statementId"])
    ann = new["annotations"][-1]
    assert ann["motivation"] == "commenting"
    assert ann["statementPointerTarget"] == "/recordDetails"
    assert "Parent: Exempt" in ann["description"]
    assert "annotations" not in subject or ann not in subject["annotations"]  # copied


# --- the rules around it ----------------------------------------------------------


def test_every_value_and_word_is_in_the_vocabulary() -> None:
    from opencheck.knowability import BANNED_VOCABULARY

    assert set(disclosure.VALUE_WORDS) == set(disclosure.VALUES)
    assert set(disclosure.COVERS_WORDS) == set(disclosure.COVERS)
    for words in [*disclosure.VALUE_WORDS.values(), *disclosure.COVERS_WORDS.values()]:
        for banned in BANNED_VOCABULARY:
            assert not re.search(rf"\b{re.escape(banned)}\b", words, re.I), (words, banned)


def test_every_reason_maps_to_a_value() -> None:
    assert set(disclosure.REASON_VALUES.values()) <= set(disclosure.VALUES)


def test_discloses_names_registered_sources_and_every_bo_register() -> None:
    from opencheck.bods.bo_regimes import REGIMES
    from opencheck.sources import REGISTRY

    assert set(disclosure.DISCLOSES) <= set(REGISTRY)
    for sid, covers in disclosure.DISCLOSES.items():
        assert covers and set(covers) <= set(disclosure.COVERS), sid
    for sid, regime in REGIMES.items():
        if regime.regime_kind == "bo_register" and sid in REGISTRY:
            assert "beneficial_owners" in disclosure.DISCLOSES.get(sid, ()), sid


def test_restricted_access_matches_bo_access() -> None:
    from opencheck import bo_access

    assert disclosure.RESTRICTED_ACCESS == bo_access._RESTRICTED_NOW


@pytest.mark.parametrize("module", ["risk.py", "verdict.py", "signalstats.py", "narrative"])
def test_the_status_raises_nothing(module: str) -> None:
    """Describes only (Stephen, 3 Nov 2026): no signal, verdict or narrative
    module may read it."""
    paths = [PKG / module] if module.endswith(".py") else sorted((PKG / module).rglob("*.py"))
    for path in paths:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [a.name for a in node.names]
            assert not any("disclosure" in n for n in names), path


def test_frontend_words_match() -> None:
    src = FRONTEND.read_text()
    for table, words in (("VALUE_WORDS", disclosure.VALUE_WORDS),
                         ("COVERS_WORDS", disclosure.COVERS_WORDS)):
        block = re.search(rf"export const {table}[^=]*=\s*\{{(.*?)\n\}}", src, re.S)
        assert block, table
        parsed = dict(re.findall(r'^\s*(\w+):\s*"([^"]*)",?\s*$', block.group(1), re.M))
        assert parsed == words, table


# --- through the pipeline ----------------------------------------------------------


def test_a_lookup_carries_a_value_for_every_applicable_source(afpc_client) -> None:  # noqa: F811
    from tests.test_lei_registration import AFPC

    body = afpc_client.get("/lookup", params={"lei": AFPC}).json()
    status = body["disclosure_status"]
    for sid in ["gleif", *body["sources_applicable"]]:
        assert status["sources"][sid]["values"], sid
    assert status["as_of"]
    # GLEIF is an ownership source (the parent) and says something about it.
    gleif = status["sources"]["gleif"]
    assert gleif["ownership_source"] is True
    assert {e["covers"] for e in gleif["entries"]} == {"parent"}


def test_the_mcp_lookup_carries_one_line_per_ownership_source(afpc_client) -> None:  # noqa: F811
    from opencheck.mcp import shaping
    from opencheck.routers.lookup import LookupResponse
    from tests.test_lei_registration import AFPC

    body = afpc_client.get("/lookup", params={"lei": AFPC}).json()
    shaped = shaping.shape_lookup(LookupResponse.model_validate(body))
    block = shaped["disclosure_status"]
    rows = {r["source"]: r for r in block["by_source"]}
    assert "gleif" in rows and rows["gleif"]["line"].startswith("Parent: ")
    assert not set(rows) & set(block["not_covered"])
    assert "not an exemption" in block["hint"]


def test_the_batch_row_and_csv_cell() -> None:
    payload = {
        "sources": {
            "companies_house": {"values": ["exempt"], "entries": [
                {"value": "exempt", "covers": "beneficial_owners"}]},
            "gleif": {"values": ["exempt"], "entries": [{"value": "exempt", "covers": "parent"}]},
            "opencorporates": {"values": ["no_record"], "entries": [{"value": "no_record"}]},
            "wikidata": {"values": ["not_covered"], "entries": [{"value": "not_covered"}]},
        },
    }
    tokens = disclosure.row_tokens(payload)
    assert tokens == {
        "companies_house": ["beneficial_owners=exempt"],
        "gleif": ["parent=exempt"],
        "opencorporates": ["no_record"],
    }
    assert disclosure.csv_cell(tokens) == (
        "companies_house/beneficial_owners=exempt gleif/parent=exempt opencorporates/no_record"
    )
    assert disclosure.csv_cell(None) == ""


def test_the_batch_row_carries_the_field_and_the_csv_its_column(afpc_client) -> None:  # noqa: F811
    from opencheck import batch as _batch
    from opencheck.mcp.shaping import shape_batch_row
    from opencheck.routers.lookup import LookupResponse
    from tests.test_lei_registration import AFPC

    body = afpc_client.get("/lookup", params={"lei": AFPC}).json()
    row = shape_batch_row(LookupResponse.model_validate(body))
    assert row["disclosure"]["gleif"][0].startswith("parent=")
    assert _batch.CSV_COLUMNS[-1] == "disclosure"
    text = _batch.rows_csv(_batch.BatchResult(rows=[row]), origin="https://opencheck.world")
    assert text.strip().split("\r\n")[1].endswith(disclosure.csv_cell(row["disclosure"]))


def test_the_export_tool_carries_the_block(afpc_client) -> None:  # noqa: F811
    from opencheck.mcp.server import _disclosure_status_block
    from opencheck.routers.lookup import LookupResponse
    from tests.test_lei_registration import AFPC

    body = afpc_client.get("/lookup", params={"lei": AFPC}).json()
    block = _disclosure_status_block(LookupResponse.model_validate(body))
    assert any(r["source"] == "gleif" for r in block["by_source"])


def test_an_older_payload_shapes_to_null() -> None:
    from types import SimpleNamespace

    from opencheck.mcp.shaping import _disclosure_status

    assert _disclosure_status(SimpleNamespace(disclosure_status=None)) is None


def test_the_reports_carry_the_section() -> None:
    from opencheck.reporting.html_report import _disclosure as html_section
    from opencheck.reporting.markdown_report import _disclosure as md_section

    report = {"disclosure_status": {
        "sources": {
            "gleif": {"values": ["exempt"], "entries": [
                {"value": "exempt", "covers": "parent", "details": ["controlled by natural persons"]}]},
            "ted_eu": {"values": ["not_covered"], "entries": [{"value": "not_covered", "covers": None}]},
        },
        "not_covered": ["ted_eu"],
        "as_of": AS_OF,
    }}
    html = html_section(report)
    md = "\n".join(md_section(report))
    for out in (html, md):
        assert disclosure.REPORT_HEADING in out
        assert "Parent: Exempt — controlled by natural persons" in out
        assert "Not ownership sources:" in out
    assert html_section({}) == "" and md_section({}) == []


def test_the_docs_page_maps_every_reason() -> None:
    doc = (PKG.parent.parent / "docs" / "disclosure-status.md").read_text()
    for reason in disclosure.REASON_VALUES:
        assert f"`{reason}`" in doc, reason
    for value in disclosure.VALUES:
        assert f"`{value}`" in doc, value
