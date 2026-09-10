"""Tests for the EITI Company Assessment adapter, mapper and index builder.

Three of these exist because of specific past failures rather than for
coverage, and are marked as such — widening any of them should be a deliberate
edit, not a side effect.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
from pathlib import Path

import pytest

from opencheck.bods import map_eiti_assessment
from opencheck.findings import finding_eiti_assessment
from opencheck.sources import REGISTRY
from opencheck.sources.eiti_assessment import (
    EitiAssessmentAdapter,
    _reset_index_for_tests,
    bo_disclosure_result,
    bo_disclosure_url,
    latest_year,
)
from opencheck.sources.schemas import validate_raw
from opencheck.sources.schemas.eiti_assessment import EitiAssessmentBundle

LEI = "21380068P1DRHMJ8KU70"

_RECORD = {
    "name": "Shell Plc",
    "hq_country": "GBR",
    "hq_city": "London",
    "sectors": ["Oil & Gas"],
    "company_type": "Public",
    "business_activity": None,
    "eiti_published": {
        "open_corporates_id": None,
        "legal_entity_id": None,
        "estma_id": None,
    },
    "match": {
        "method": "gleif_name_exact",
        "reviewed": True,
        "gleif_legal_name": "SHELL PLC",
    },
    "assessments": {
        "2023": {
            "exp_6": {
                "label": "Company disclose beneficial ownership",
                "result": "Expectation met",
                "response": "Yes",
                "bo_disclosure": "Yes",
                "bo_disclosure_url": "https://example.org/shell-2023.pdf",
            }
        },
        "2025": {
            "exp_2": {
                "label": "Company publish a list of controlled subsidiaries",
                "result": "Expectation met",
            },
            "exp_6": {
                "label": "Company disclose beneficial ownership",
                "result": "Expectation met",
                "response": "Yes",
                "bo_disclosure": "Yes",
            },
        },
    },
    "subsidiaries": [
        {"name": "A/S Norske Shell", "country": "NOR", "years": ["2021", "2024"]},
        {"name": "Shell Nigeria Gas Ltd", "country": "NGA", "years": ["2024"]},
        {"name": "Shell Trinidad Limited", "country": "TTO", "years": ["2024"]},
    ],
}


@pytest.fixture(autouse=True)
def _index(monkeypatch):
    """Point the adapter at an in-memory index for every test in this module."""
    import opencheck.sources.eiti_assessment as mod

    _reset_index_for_tests()
    monkeypatch.setattr(mod, "_index", {LEI: _RECORD})
    monkeypatch.setattr(mod, "_source_snapshot", "2026-09-04T18:55:30+00:00")
    yield
    _reset_index_for_tests()


# --------------------------------------------------------------------------
# Adapter
# --------------------------------------------------------------------------


def test_registered_in_registry():
    assert REGISTRY["eiti_assessment"].id == "eiti_assessment"
    assert REGISTRY["eiti_assessment"].info.category == "esg"


async def test_fetch_by_lei_returns_bundle():
    bundle = await EitiAssessmentAdapter().fetch_by_lei(LEI)
    assert bundle is not None
    assert bundle["lei"] == LEI
    assert bundle["name"] == "Shell Plc"
    assert len(bundle["subsidiaries"]) == 3
    validate_raw("eiti_assessment", EitiAssessmentBundle, bundle)


async def test_fetch_by_lei_absent_returns_none():
    assert await EitiAssessmentAdapter().fetch_by_lei("00000000000000000000") is None


def test_covers_lei_gates_dispatch():
    a = EitiAssessmentAdapter()
    assert a.covers_lei(LEI)
    assert a.covers_lei(LEI.lower())
    assert not a.covers_lei("00000000000000000000")


async def test_asserts_no_identifiers():
    """Corroboration rule: this source publishes nothing OpenCheck can assert.

    The LEI is OpenCheck-derived; EITI's ``legal_entity_id`` is populated for 3
    companies of 10,116; and ``eiti_id_company`` is a name-derived dedup key
    EITI regenerated wholesale in this release. An empty set is the correct
    answer — a future change that starts asserting one should have to delete
    this test and explain why.
    """
    bundle = await EitiAssessmentAdapter().fetch_by_lei(LEI)
    assert bundle["identifiers"] == {}


# --------------------------------------------------------------------------
# Reading helpers
# --------------------------------------------------------------------------


def test_latest_year_and_bo_result():
    assert latest_year(_RECORD) == "2025"
    assert bo_disclosure_result(_RECORD) == "Expectation met"


def test_bo_url_falls_back_to_an_earlier_year():
    """EITI populated the disclosure URLs for 2023 and not for 2025.

    Reading only the latest year would show no link for almost every company,
    which reads as "no disclosure published" rather than "EITI did not carry
    the link that year".
    """
    assert bo_disclosure_url(_RECORD) == "https://example.org/shell-2023.pdf"


# --------------------------------------------------------------------------
# Mapper
# --------------------------------------------------------------------------


def test_subsidiaries_never_enter_bods():
    """PINNED: the declared subsidiary names must not become BODS statements.

    EITI publishes them as free-text names with no identifier. Emitting them
    would assert that OpenCheck has identified those companies, which it has
    not. They are evidence on a card, not graph nodes.

    This test exists to make widening that a deliberate act. If a future change
    maps the subsidiaries, it must delete this test and say why in the commit.
    """
    stmts = list(map_eiti_assessment({"lei": LEI, "is_stub": False, **_RECORD}))

    assert len(stmts) == 1, "expected exactly one statement — the subject entity"
    assert stmts[0]["recordType"] == "entity"

    blob = json.dumps(stmts)
    for sub in _RECORD["subsidiaries"]:
        assert sub["name"] not in blob, (
            f"{sub['name']!r} reached the BODS output — declared subsidiaries "
            "must never enter the graph"
        )
    assert not [s for s in stmts if s["recordType"] == "relationship"]
    assert not [s for s in stmts if s["recordType"] == "person"]


def test_mapper_asserts_no_identifiers():
    stmts = list(map_eiti_assessment({"lei": LEI, "is_stub": False, **_RECORD}))
    assert stmts[0]["recordDetails"].get("identifiers", []) == []


def test_mapper_records_the_bo_assessment_as_an_annotation():
    stmts = list(map_eiti_assessment({"lei": LEI, "is_stub": False, **_RECORD}))
    notes = " ".join(a["description"] for a in stmts[0].get("annotations", []))
    assert "expectation 6" in notes
    assert "Expectation met" in notes
    # It must say what it is NOT, or a reader takes it for ownership data.
    assert "not a" in notes.lower()


def test_mapper_flags_a_differing_matched_name():
    """Chevron Corporation is anchored on Chevron U.S.A. Inc.'s LEI.

    Several supporting companies have no LEI of their own. The statement has to
    say which legal entity it is actually about, or it misstates both.
    """
    record = {**_RECORD, "match": {**_RECORD["match"], "gleif_legal_name": "Chevron U.S.A. Inc."}}
    stmts = list(map_eiti_assessment({"lei": LEI, "is_stub": False, **record}))
    notes = " ".join(a["description"] for a in stmts[0].get("annotations", []))
    assert "Chevron U.S.A. Inc." in notes


def test_mapper_publishes_the_lei_match_as_an_identifying_annotation():
    """The FullCheck graph floated this statement as a second, unconnected node
    beside the subject (PT Pertamina (Persero), 2026-09-10): no identifier and
    no jurisdiction left nothing to join it on. The link is published as an
    `identifying` annotation naming the GLEIF record — never as an identifier,
    because the LEI is OpenCheck's match, not EITI's assertion."""
    from opencheck.bods.annotations import validate_all

    stmts = list(map_eiti_assessment({"lei": LEI, "is_stub": False, **_RECORD}))
    [link] = [a for a in stmts[0]["annotations"] if a["motivation"] == "identifying"]
    assert link["url"] == f"https://search.gleif.org/#/record/{LEI}"
    assert link["statementPointerTarget"] == "/recordDetails"
    assert LEI in link["description"]
    assert "exact legal-name match in GLEIF, reviewed by hand" in link["description"]
    assert "OpenCheck's match rather than an identifier EITI asserts" in link["description"]
    assert stmts[0]["recordDetails"].get("identifiers", []) == []
    assert validate_all(stmts) == []


@pytest.mark.parametrize(
    ("method", "phrase"),
    [
        ("published_lei", "the LEI EITI publishes for it"),
        ("candidates_only", "chosen by hand from GLEIF name candidates"),
        ("", "a match made by OpenCheck"),
    ],
)
def test_lei_match_annotation_says_how_the_match_was_made(method, phrase):
    record = {**_RECORD, "match": {**_RECORD["match"], "method": method}}
    stmts = list(map_eiti_assessment({"lei": LEI, "is_stub": False, **record}))
    [link] = [a for a in stmts[0]["annotations"] if a["motivation"] == "identifying"]
    assert phrase in link["description"]
    # EITI did publish the LEI in that case, so the note must not say it didn't.
    said_not_asserted = "rather than an identifier EITI asserts" in link["description"]
    assert said_not_asserted is (method != "published_lei")


def test_mapper_ignores_stub():
    assert list(map_eiti_assessment({"is_stub": True})) == []


# --------------------------------------------------------------------------
# Findings template
# --------------------------------------------------------------------------


def test_finding_sentence():
    s = finding_eiti_assessment({"is_stub": False, **_RECORD})
    # The beneficial ownership clause leads, deliberately: clauses_to_sentence
    # drops TRAILING clauses to fit the 140-char cap, so putting it last lost
    # it on every company that also had a subsidiary list.
    assert s.startswith("Recorded by EITI in 2025 as disclosing its beneficial owners")
    assert "3 controlled subsidiaries" in s
    assert s.endswith(".")
    assert len(s) <= 140


def test_finding_states_absence_in_the_same_voice():
    """A company with no subsidiary list says so, rather than going quiet."""
    record = {**_RECORD, "subsidiaries": []}
    s = finding_eiti_assessment({"is_stub": False, **record})
    assert "no subsidiary list" in s


def test_finding_keeps_the_bo_clause_when_the_sentence_is_long():
    """REGRESSION: the 140-char cap silently ate the disclosure clause.

    The first draft ordered the clauses year-then-subsidiaries-then-disclosure,
    and clauses_to_sentence drops trailing clauses to fit — so the one fact the
    template exists to report was the first thing thrown away, on exactly the
    companies with the most to say. Any reordering must keep this passing.
    """
    record = {
        **_RECORD,
        "subsidiaries": [
            {"name": f"Sub {i}", "country": f"C{i % 40:02d}"} for i in range(144)
        ],
    }
    s = finding_eiti_assessment({"is_stub": False, **record})
    assert "beneficial owners" in s, s
    assert len(s) <= 140


@pytest.mark.parametrize("result", ["Not available", "Not applicable"])
def test_finding_never_reads_not_available_as_a_failure(result):
    """PINNED: "not available" is a fact about the assessment, not the company.

    27 of the 2025 cohort carry it. Rendering it as though the company failed
    to disclose would be a false accusation — the same class of error as the
    counter-sanctions chip reading as "sanctioned".
    """
    record = {
        **_RECORD,
        "assessments": {"2025": {"exp_6": {"result": result}}},
    }
    s = finding_eiti_assessment({"is_stub": False, **record}).lower()
    assert "not assessed by eiti" in s
    assert "not disclosing" not in s


def test_finding_reports_an_unrecognised_result_rather_than_guessing():
    record = {**_RECORD, "assessments": {"2025": {"exp_6": {"result": "Something new"}}}}
    s = finding_eiti_assessment({"is_stub": False, **record})
    assert "something new" in s.lower()


def test_finding_none_without_assessments():
    assert finding_eiti_assessment({"is_stub": False, **_RECORD, "assessments": {}}) is None


# --------------------------------------------------------------------------
# The index builder — exercised through its REAL entry point
# --------------------------------------------------------------------------


def _load_builder():
    """Import the builder script as a module, by path.

    Deliberately imports the *actual script* rather than reimplementing its
    logic in the test. A test that arranges the thing it is meant to prove
    proves nothing — the lesson from the ΓΕΜΗ call budget, which passed a
    covering test for 28 phases while being inert in production.
    """
    path = Path(__file__).resolve().parent.parent / "scripts" / "build_eiti_assessment_index.py"
    spec = importlib.util.spec_from_file_location("build_eiti_assessment_index", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_RAW = {
    "meta": {"harvested": "2026-09-04T18:55:30+00:00", "source": "test"},
    "companies": [],
    "assessments": [
        {
            "company_name": "Testco",
            "company_hq_country_iso3": "GBR",
            "assessment_year": 2025,
            "expectation_shorthand": "exp_6",
            "expectation_label": "Company disclose beneficial ownership",
            "assessment_result": "Expectation met",
            "response": "Yes",
            "bo_disclosure": "Yes",
            "bo_url": "Not available",
        }
    ],
    "subsidiaries": [
        {
            "eiti_supporting_company": '"Testco"',
            "subsidiary_name": '"Testco Ghana Ltd"',
            "eiti_implementing_country": '"GHA"',
            "eiti_report_year": '"2024"',
        }
    ],
}

_HEADER = (
    "decision\teiti_name\thq_country\tsubsidiaries\tmethod\tlei\t"
    "gleif_legal_name\tgleif_country\tflag\tnote\n"
)


def _write_inputs(tmp_path: Path, decision: str) -> tuple[Path, Path, Path]:
    raw = tmp_path / "raw.json.gz"
    with gzip.open(raw, "wt", encoding="utf-8") as f:
        json.dump(_RAW, f)
    review = tmp_path / "review.tsv"
    review.write_text(
        _HEADER + f"{decision}\tTestco\tGBR\t1\tgleif_name_exact\t{LEI}\tTESTCO PLC\tGB\t\t\n",
        encoding="utf-8",
    )
    return raw, review, tmp_path / "out.json.gz"


def test_builder_refuses_while_a_row_is_unreviewed(tmp_path):
    """PINNED: the review gate is the only thing standing between a bad name
    match and a card rendered against the wrong company. Review caught a match
    on TECK GmbH, an unrelated German company, that had passed as exact."""
    mod = _load_builder()
    raw, review, out = _write_inputs(tmp_path, "REVIEW")
    with pytest.raises(SystemExit):
        mod.build(raw, review, out)
    assert not out.exists(), "no index may be written while a row is unreviewed"


def test_builder_writes_an_index_once_reviewed(tmp_path):
    mod = _load_builder()
    raw, review, out = _write_inputs(tmp_path, "accept")
    mod.build(raw, review, out)
    with gzip.open(out, "rt", encoding="utf-8") as f:
        data = json.load(f)
    assert data["meta"]["parents_accepted"] == 1
    assert data["meta"]["review_gate"]
    rec = data["index"][LEI]
    assert rec["name"] == "Testco"
    assert rec["subsidiaries"][0]["name"] == "Testco Ghana Ltd"
    assert rec["subsidiaries"][0]["country"] == "GHA"
    # "Not available" is EITI's sentinel for "the company gave nothing", and
    # must not survive into the index as though it were a URL.
    assert "bo_url" not in rec["assessments"]["2025"]["exp_6"]


def test_builder_rejects_a_row_without_indexing_it(tmp_path):
    mod = _load_builder()
    raw, review, out = _write_inputs(tmp_path, "reject")
    mod.build(raw, review, out)
    with gzip.open(out, "rt", encoding="utf-8") as f:
        data = json.load(f)
    assert data["index"] == {}
    assert data["meta"]["unresolved_names"] == ["Testco"]


def test_builder_unjson_strips_the_raw_layer_quoting():
    """A LEI arrives from a ``raw_*`` table as 22 characters, not 20."""
    mod = _load_builder()
    assert mod._unjson('"549300071188HIDJEB11"') == "549300071188HIDJEB11"
    assert mod._clean('"Not available"') is None
    assert mod._clean('"GHA"') == "GHA"


def test_builder_canonicalises_eiti_s_two_spellings():
    """EITI spells the same company differently across its two sheets.

    "Anglo American" in the assessment sheet, "AngloAmerican" in the subsidiary
    sheet; the UUIDv5 deduplication does not collapse them. Without the
    spaceless join one company becomes two parents and the review file asks the
    same question twice.
    """
    mod = _load_builder()
    raw = {
        "companies": [],
        "assessments": [
            {"company_name": "Anglo American", "assessment_year": 2025,
             "expectation_shorthand": "exp_6", "assessment_result": "Expectation met"}
        ],
        "subsidiaries": [
            {"eiti_supporting_company": '"AngloAmerican"',
             "subsidiary_name": '"De Beers UK Limited"'}
        ],
    }
    parents = mod._parents(raw)
    assert len(parents) == 1
    (rec,) = parents.values()
    assert rec["name"] == "Anglo American"
    assert rec["subsidiary_count"] == 1
    assert "AngloAmerican" in rec["aliases"]
