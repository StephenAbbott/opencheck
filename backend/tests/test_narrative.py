"""Offline tests for the narrative package (no live LLM call).

Covers the evidence-packet builder, the citation validator, and conformance of
the golden fixtures. The Anthropic call in ``summarise`` is exercised only by the
opt-in eval harness, never here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from opencheck.narrative import (
    EvidencePacket,
    build_evidence_packet,
    validate_narrative,
)

GOLDEN_DIR = Path(__file__).parent / "golden_narrative"


# --- a realistic serialised lookup result -----------------------------------


def _report() -> dict:
    return {
        "lei": "2138000000000000A001",
        "legal_name": "Northwind Logistics Ltd",
        "jurisdiction": "GB",
        "derived_identifiers": {"company_number": "08123456"},
        "bods": [
            {
                "statementId": "ent-1",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Northwind Logistics Ltd",
                    "jurisdiction": {"name": "United Kingdom"},
                    "identifiers": [
                        {"scheme": "GB-COH", "id": "08123456"},
                        {"scheme": "XI-LEI", "id": "2138000000000000A001"},
                    ],
                },
                "source": {
                    "description": "UK Companies House",
                    "type": ["officialRegister"],
                    "url": "https://find-and-update.company-information.service.gov.uk/company/08123456",
                },
            },
            {
                "statementId": "per-1",
                "recordType": "person",
                "recordDetails": {"names": [{"fullName": "Jane Eleanor Smith"}]},
                "source": {"description": "UK Companies House", "type": ["officialRegister"]},
            },
            {
                "statementId": "rel-1",
                "recordType": "relationship",
                "recordDetails": {
                    "interestedParty": "per-1",
                    "subject": "ent-1",
                    "interests": [
                        {
                            "details": "ownership of shares — 75% or more",
                            "share": {"exclusiveMinimum": 75, "maximum": 100},
                            "startDate": "2016-04-06",
                        }
                    ],
                },
                "source": {
                    "description": "UK Companies House",
                    "type": ["officialRegister"],
                    "url": "https://example.org/psc",
                },
            },
        ],
        "risk_signals": [
            {
                "code": "NON_EU_JURISDICTION",
                "confidence": "high",
                "summary": "Controlling party resident outside the EU/EEA.",
                "source_id": "opensanctions",
            }
        ],
        "hits": [
            {"source_id": "companies_house", "is_stub": False},
            {"source_id": "gleif", "is_stub": False},
            {"source_id": "kvk", "is_stub": True},
        ],
        "errors": {},
        "license_notices": [],
    }


def test_build_packet_core_shape():
    packet = build_evidence_packet(_report())
    assert packet.subject_name == "Northwind Logistics Ltd"
    assert packet.lei == "2138000000000000A001"
    assert packet.subject_confidence == "identifier-confirmed"
    # A relationship fact + a subject registration fact at minimum.
    statements = [f.statement for f in packet.facts]
    assert any("Jane Eleanor Smith" in s and "Northwind" in s for s in statements)
    assert any("registered entity" in s for s in statements)
    # The PSC interest band + date make it into the fact text.
    assert any("2016-04-06" in s for s in statements)


def test_official_register_facts_are_high_confidence():
    packet = build_evidence_packet(_report())
    assert packet.facts, "expected at least one fact"
    assert all(f.confidence == "high" for f in packet.facts)


def test_risk_item_uses_registry_source_name():
    packet = build_evidence_packet(_report())
    assert len(packet.risks) == 1
    risk = packet.risks[0]
    assert risk.code == "NON_EU_JURISDICTION"
    assert risk.label == "Non-EU jurisdiction"
    assert risk.source_name == "OpenSanctions"


def test_sources_consulted_excludes_stub_hits():
    packet = build_evidence_packet(_report())
    ids = {s.source_id for s in packet.sources_consulted}
    assert "companies_house" in ids
    assert "gleif" in ids
    assert "kvk" not in ids  # stub hit dropped


def test_no_person_relationship_adds_gap():
    report = _report()
    # Drop the person + its relationship → no disclosed beneficial owner.
    report["bods"] = [s for s in report["bods"] if s["recordType"] == "entity"]
    packet = build_evidence_packet(report)
    assert any("No beneficial owner" in g.statement for g in packet.gaps)


def test_free_text_report_is_name_matched():
    report = {"query": "Acme Trading", "bods": [], "hits": [], "risk_signals": []}
    packet = build_evidence_packet(report)
    assert packet.lei is None
    assert packet.subject_confidence == "name-matched"


def test_errored_source_becomes_gap():
    report = _report()
    report["errors"] = {"opensanctions": "timeout"}
    packet = build_evidence_packet(report)
    assert any("OpenSanctions could not be queried" in g.statement for g in packet.gaps)


def test_no_risks_synthesises_citable_no_risk_fact():
    report = _report()
    report["risk_signals"] = []
    packet = build_evidence_packet(report)
    assert packet.risks == []
    assert any("no structural or jurisdictional risk" in f.statement.lower()
               for f in packet.facts)


def test_gaps_are_citable_with_stable_ids():
    report = _report()
    report["errors"] = {"opensanctions": "timeout"}
    packet = build_evidence_packet(report)
    assert packet.gap_ids()  # non-empty
    assert all(gid.startswith("g") for gid in packet.gap_ids())
    # evidence_ids unions facts, risks and gaps.
    assert packet.gap_ids() <= packet.evidence_ids()
    assert packet.fact_ids() <= packet.evidence_ids()


# --- validator ---------------------------------------------------------------


def _packet_with_facts(*ids: str) -> EvidencePacket:
    return EvidencePacket(
        subject_name="X",
        facts=[
            {"id": i, "statement": f"fact {i}", "source_name": "S"} for i in ids
        ],
    )


def test_validator_accepts_grounded_claim():
    packet = _packet_with_facts("f1", "f2")
    result = {
        "summary": "X is a thing.",
        "claims": [{"id": "c1", "text": "X is a thing.", "fact_ids": ["f1"], "confidence": "high"}],
        "overall_confidence": "high",
    }
    v = validate_narrative(packet, result)
    assert v.ok
    assert v.summary == "X is a thing."
    assert len(v.valid_claims) == 1


def test_validator_drops_unknown_citation_and_withholds_summary():
    packet = _packet_with_facts("f1")
    result = {
        "summary": "X owns the moon.",
        "claims": [
            {"id": "c1", "text": "X owns the moon.", "fact_ids": ["f99"], "confidence": "high"}
        ],
        "overall_confidence": "high",
    }
    v = validate_narrative(packet, result)
    assert not v.ok
    assert v.valid_claims == []
    assert v.summary == ""  # paragraph withheld on violation
    assert any("unknown" in i for i in v.issues)


def test_validator_drops_uncited_claim():
    packet = _packet_with_facts("f1")
    result = {
        "summary": "s",
        "claims": [{"id": "c1", "text": "uncited", "fact_ids": [], "confidence": "low"}],
        "overall_confidence": "low",
    }
    v = validate_narrative(packet, result)
    assert not v.ok
    assert v.dropped_claims and not v.valid_claims


def test_validator_accepts_gap_id_citation():
    packet = EvidencePacket(
        subject_name="X",
        facts=[{"id": "f1", "statement": "fact", "source_name": "S"}],
        gaps=["No beneficial owner was disclosed."],
    )
    assert packet.gap_ids() == {"g1"}
    result = {
        "summary": "s",
        "claims": [{"id": "c1", "text": "gap", "fact_ids": ["g1"], "confidence": "low"}],
        "overall_confidence": "low",
    }
    v = validate_narrative(packet, result)
    assert v.ok


def test_validator_accepts_risk_id_citation():
    packet = EvidencePacket(
        subject_name="X",
        facts=[{"id": "f1", "statement": "fact", "source_name": "S"}],
        risks=[{"id": "r1", "code": "PEP", "label": "PEP", "confidence": "medium",
                "rationale": "x", "source_name": "S"}],
    )
    result = {
        "summary": "s",
        "claims": [{"id": "c1", "text": "risk", "fact_ids": ["r1"], "confidence": "medium"}],
        "overall_confidence": "medium",
    }
    v = validate_narrative(packet, result)
    assert v.ok


# --- golden fixtures conform to the schema -----------------------------------


@pytest.mark.parametrize("path", sorted(GOLDEN_DIR.glob("*.json")), ids=lambda p: p.name)
def test_golden_packets_validate(path):
    packet = EvidencePacket.model_validate(json.loads(path.read_text()))
    assert packet.subject_name
    # Every risk's supporting fact ids (when given) must exist in the packet.
    known = packet.fact_ids()
    for r in packet.risks:
        for fid in r.fact_ids:
            assert fid in known, f"{path.name}: risk {r.id} cites missing {fid}"
