"""One SEC CIK, two spellings: the identifier-merge value (MCP testing, 7 Nov 2026).

Eli Lilly's MCP map said "No source names a current owner or controller" while
the same lookup's ``disclosure_status`` said SEC EDGAR declares three
shareholders. SEC EDGAR's own record of the issuer files ``US-SEC-CIK 59478``
and no LEI (it does not assert one); Wikirate's files the LEI and
``US-SEC-CIK 0000059478``. The merge keys kept the zeros, the two never formed
one referent group, and the 13D/13G holders never reached the subject.

The case table ``frontend/src/lib/identifierMatchCases.json`` is run here and
by ``reconcile.test.ts``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from opencheck import identifiers, integrated_ownership as io, owners
from opencheck.bods.statements import (
    make_entity_statement,
    make_relationship_statement,
)
from opencheck.consistency import referent_groups
from opencheck.subject_profile import subject_statements

_FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib"
_CASES = json.loads((_FRONTEND / "identifierMatchCases.json").read_text())["cases"]

LILLY_LEI = "FRDRIPF3EKNDJ2CQJL29"
AS_OF = "2026-11-07"


@pytest.mark.parametrize("case", _CASES, ids=lambda c: f"{c['scheme']}:{c['value']}")
def test_match_value_cases(case):
    assert identifiers.match_value(case["scheme"], case["value"]) == case["match"]


def test_the_frontend_lists_the_same_zero_padded_schemes():
    text = (_FRONTEND / "reconcile.ts").read_text()
    block = re.search(r"ZERO_PADDED_SCHEMES = new Set\(\[(.*?)\]\)", text, re.S).group(1)
    assert set(re.findall(r'"([^"]+)"', block)) == set(identifiers.ZERO_PADDED_SCHEMES)


def _lilly_bundle() -> list[dict]:
    """The production shape of 7 Nov 2026, reduced: GLEIF and Wikirate carry
    the LEI, SEC EDGAR's issuer record carries the unpadded CIK and the
    13D/13G holders hang off it."""
    gleif = make_entity_statement(
        source_id="gleif", local_id=LILLY_LEI, name="ELI LILLY AND COMPANY",
        jurisdiction=("United States", "US-IN"),
        identifiers=[{"scheme": "XI-LEI", "id": LILLY_LEI}],
    )
    wikirate = make_entity_statement(
        source_id="wikirate", local_id="eli-lilly", name="Eli Lilly & Co",
        identifiers=[
            {"scheme": "XI-LEI", "id": LILLY_LEI},
            {"scheme": "US-SEC-CIK", "id": "0000059478"},
        ],
    )
    issuer = make_entity_statement(
        source_id="sec_edgar", local_id="59478", name="ELI LILLY AND COMPANY",
        identifiers=[
            {"scheme": "US-SEC-CIK", "id": "59478"},
            {"scheme": "CUSIP", "id": "532457108"},
        ],
    )
    endowment = make_entity_statement(
        source_id="sec_edgar", local_id="filer:lilly-endowment", name="LILLY ENDOWMENT, INC.",
    )
    holding = make_relationship_statement(
        source_id="sec_edgar", local_id="59478:lilly-endowment",
        subject_statement_id=issuer["recordId"],
        interested_party_statement_id=endowment["recordId"],
        interested_party_type="entity",
        interests=[{
            "type": "shareholding", "directOrIndirect": "direct",
            "share": {"exact": 9.6}, "beneficialOwnershipOrControl": False,
        }],
    )
    return [gleif, wikirate, issuer, endowment, holding]


def test_the_sec_edgar_issuer_record_joins_the_subject():
    bods = _lilly_bundle()
    issuer = bods[2]
    groups = referent_groups(bods)
    assert any(issuer in g and bods[0] in g for g in groups)
    assert issuer["statementId"] in {s["statementId"] for s in subject_statements(LILLY_LEI, bods)}


def test_the_13g_holder_reaches_integrated_ownership_and_the_map():
    bods = _lilly_bundle()
    payload = io.for_lookup(LILLY_LEI, bods, AS_OF)
    names = {h["name"]: h for h in payload["holders"]}
    assert "LILLY ENDOWMENT, INC." in names
    assert names["LILLY ENDOWMENT, INC."]["figures"][0]["text"] == "9.6%"

    net = owners.Network(
        bods=bods, subject_ids=payload["subject_ids"], payload=payload,
        expanded=set(), refused={}, hops=0, capped=False, depth_reached=1,
    )
    answer, svg = owners.ownership_map(
        lei=LILLY_LEI, legal_name="ELI LILLY AND COMPANY", net=net, as_of=AS_OF,
    )
    assert [e["owner"] for e in answer["edges"]] == ["LILLY ENDOWMENT, INC."]
    assert answer["edges"][0]["source"].startswith("SEC EDGAR")
    assert svg and "LILLY ENDOWMENT" in svg


def test_a_companies_house_number_keeps_its_leading_zeros():
    a = make_entity_statement(source_id="companies_house", local_id="a", name="A LTD",
                              identifiers=[{"scheme": "GB-COH", "id": "00102498"}])
    b = make_entity_statement(source_id="opencorporates", local_id="b", name="B LTD",
                              identifiers=[{"scheme": "GB-COH", "id": "102498"}])
    assert referent_groups([a, b]) == []
