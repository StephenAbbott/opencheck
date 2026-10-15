"""Phase 342 — the sanctions 50% rule, computed per regime.

The ticket's cases first (aggregation, the exact 50%, propagation, a band,
a name-only match, two sources, a party on one regime's list only), then the
evidence the cross-check now carries (datasets, identifier match), the lookup
wiring, the report tables, the MCP answer and the label maps.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from opencheck import sanctions_ownership as so
from opencheck.risk import SANCTIONS_OWNERSHIP_TEST, SANCTIONS_OWNERSHIP_TEST_POSSIBLE

_BACKEND = Path(__file__).resolve().parents[1]
AS_OF = "2026-10-15"
SDN = ["us_ofac_sdn"]
ALL3 = ["us_ofac_sdn", "eu_fsf", "gb_hmt_sanctions"]


def _entity(sid: str, name: str, ids: list[tuple[str, str]] | None = None) -> dict[str, Any]:
    return {
        "statementId": sid, "recordType": "entity",
        "recordDetails": {
            "name": name,
            "identifiers": [{"scheme": s, "id": i} for s, i in ids or []],
        },
    }


def _own(sid: str, owner: str, owned: str, share: dict[str, Any] | None,
         source: str = "opensanctions", itype: str = "shareholding",
         end: str | None = None) -> dict[str, Any]:
    interest: dict[str, Any] = {"type": itype}
    if share is not None:
        interest["share"] = share
    if end:
        interest["endDate"] = end
    return {
        "statementId": sid, "recordType": "relationship",
        "source": {"opencheckSourceId": source, "description": source.title()},
        "recordDetails": {"subject": owned, "interestedParty": owner, "interests": [interest]},
    }


def _designated(sid: str, datasets: list[str], *, ident: bool = True,
                corroboration: list[str] | None = None, name: str = "") -> dict[str, Any]:
    ev: dict[str, Any] = {
        "subject_statement_id": sid, "matched_name": name or sid, "search_name": name or sid,
        "corroboration": corroboration or [], "name_match_only": False, "datasets": datasets,
    }
    if ident:
        ev["identifier_match"] = True
    return {"code": "RELATED_SANCTIONED", "source_id": "opensanctions", "hit_id": f"NK-{sid}",
            "confidence": "high", "evidence": ev, "kind": "risk"}


def _run(statements, signals, subject=("T",)):
    return so.build_signal(signals, statements, list(subject), subject="LEI", as_of=AS_OF)


def _regime(sig: dict[str, Any], key: str) -> dict[str, Any]:
    return next(r for r in sig["evidence"]["regimes"] if r["regime"] == key)


def _base(*extra: dict[str, Any]) -> list[dict[str, Any]]:
    return [_entity("T", "Target Co"), _entity("P", "Designated P"),
            _entity("Q", "Designated Q"), _entity("A", "Holding A"), *extra]


# --- the ticket's cases ---------------------------------------------------------


def test_ofac_adds_designated_holdings_and_the_uk_does_not():
    bods = _base(_own("r1", "P", "T", {"exact": 30}), _own("r2", "Q", "T", {"exact": 25}))
    sig = _run(bods, [_designated("P", ALL3), _designated("Q", ALL3)])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST and sig["kind"] == "risk"
    ofac, eu, uk = (_regime(sig, k) for k in ("OFAC", "EU", "UK"))
    assert ofac["result"] == eu["result"] == so.RESULT_MEETS
    assert ofac["sources"][0]["figure"]["text"] == "55%"
    assert uk["result"] == so.RESULT_NOT_MET
    assert "OFAC (US) and EU" in sig["summary"]
    assert "UK: not met" in sig["summary"]


def test_exactly_fifty_meets_ofac_and_the_eu_but_not_the_uk():
    bods = _base(_own("r1", "P", "T", {"exact": 50}))
    sig = _run(bods, [_designated("P", ALL3)])
    assert _regime(sig, "OFAC")["result"] == so.RESULT_MEETS
    assert _regime(sig, "EU")["result"] == so.RESULT_MEETS
    assert _regime(sig, "UK")["result"] == so.RESULT_NOT_MET


def test_more_than_fifty_meets_the_uk():
    bods = _base(_own("r1", "P", "T", {"exclusiveMinimum": 50, "maximum": 75}))
    sig = _run(bods, [_designated("P", ALL3)])
    assert {r["regime"]: r["result"] for r in sig["evidence"]["regimes"]} == {
        "OFAC": so.RESULT_MEETS, "EU": so.RESULT_MEETS, "UK": so.RESULT_MEETS,
    }
    assert _regime(sig, "UK")["sources"][0]["figure"]["text"] == "more than 50% up to 75%"


def test_a_caught_company_counts_in_full_not_multiplied():
    # SDN holds 60% of A; A holds 50% of the subject. Multiplied it would be
    # 30%; under the rule A is blocked and its 50% counts.
    bods = _base(_own("r1", "P", "A", {"exact": 60}), _own("r2", "A", "T", {"exact": 50}))
    sig = _run(bods, [_designated("P", SDN)])
    ofac = _regime(sig, "OFAC")
    assert ofac["result"] == so.RESULT_MEETS
    (holder,) = ofac["sources"][0]["holders"]
    assert holder["name"] == "Holding A" and holder["stake"] == "50%"
    assert holder["designated"] is False
    assert holder["held_by"] == [{"name": "Designated P", "stake": "60%"}]


def test_a_company_under_the_threshold_is_not_caught():
    bods = _base(_own("r1", "P", "A", {"exact": 40}), _own("r2", "A", "T", {"exact": 100}))
    assert _run(bods, [_designated("P", SDN)]) is None


def test_the_uk_reads_a_designated_persons_own_companies_together():
    # P holds 30% directly and 30% through A, which it owns 60% of: P's own
    # holding is 60%, more than 50%.
    bods = _base(
        _own("r1", "P", "T", {"exact": 30}), _own("r2", "P", "A", {"exact": 60}),
        _own("r3", "A", "T", {"exact": 30}),
    )
    sig = _run(bods, [_designated("P", ["gb_hmt_sanctions"])])
    assert _regime(sig, "UK")["result"] == so.RESULT_MEETS


def test_a_band_crossing_the_threshold_may_meet_and_is_context():
    bods = _base(_own("r1", "P", "T", {"exclusiveMinimum": 25, "maximum": 50}))
    sig = _run(bods, [_designated("P", ALL3)])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE and sig["kind"] == "context"
    assert _regime(sig, "OFAC")["result"] == so.RESULT_MAY_MEET
    assert _regime(sig, "EU")["result"] == so.RESULT_MAY_MEET
    # "up to 50%" can never be more than 50%.
    assert _regime(sig, "UK")["result"] == so.RESULT_NOT_MET


def test_an_unstated_share_may_meet():
    bods = _base(_own("r1", "P", "T", None))
    sig = _run(bods, [_designated("P", SDN)])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE
    assert _regime(sig, "OFAC")["sources"][0]["figure"]["text"] == "share not stated"


def test_a_name_only_match_is_never_counted():
    bods = _base(_own("r1", "P", "T", {"exact": 30}), _own("r2", "Q", "T", {"exact": 30}))
    sig = _run(bods, [_designated("P", SDN), _designated("Q", SDN, ident=False)])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE
    ofac = _regime(sig, "OFAC")
    assert ofac["result"] == so.RESULT_NOT_MET
    assert ofac["if_confirmed"] is True
    assert ofac["sources"][0]["name_only"] == [{"party": "Q", "name": "Designated Q"}]
    assert ofac["sources"][0]["figure"]["text"] == "30%"
    assert "if a name-only match were confirmed" in sig["summary"]


def test_a_name_only_match_alone_meets_nothing():
    bods = _base(_own("r1", "Q", "T", {"exact": 90}))
    sig = _run(bods, [_designated("Q", SDN, ident=False)])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST_POSSIBLE
    assert _regime(sig, "OFAC")["sources"][0]["holders"] == []


def test_a_corroborated_match_counts_at_medium_confidence():
    bods = _base(_own("r1", "P", "T", {"exact": 75}))
    sig = _run(bods, [_designated("P", SDN, ident=False, corroboration=["country"])])
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST and sig["confidence"] == "medium"
    assert _run(bods, [_designated("P", SDN)])["confidence"] == "high"


def test_two_sources_are_never_added():
    bods = _base(
        _own("r1", "P", "T", {"exact": 30}, source="opensanctions"),
        _own("r2", "P", "T", {"exact": 25}, source="companies_house"),
    )
    # Neither source alone reaches 50%, and each says the same holding.
    assert _run(bods, [_designated("P", SDN)]) is None
    bods = _base(
        _own("r1", "P", "T", {"exact": 30}, source="opensanctions"),
        _own("r2", "Q", "T", {"exact": 25}, source="companies_house"),
    )
    assert _run(bods, [_designated("P", SDN), _designated("Q", SDN)]) is None


def test_a_holding_stated_twice_by_one_source_is_not_added():
    bods = _base(_own("r1", "P", "T", {"exact": 30}), _own("r2", "P", "T", {"exact": 30}))
    assert _run(bods, [_designated("P", SDN)]) is None


def test_a_party_on_one_regimes_list_counts_for_that_regime_only():
    bods = _base(_own("r1", "P", "T", {"exact": 80}))
    sig = _run(bods, [_designated("P", ["eu_fsf"])])
    assert {r["regime"]: r["result"] for r in sig["evidence"]["regimes"]} == {
        "OFAC": so.RESULT_NONE, "EU": so.RESULT_MEETS, "UK": so.RESULT_NONE,
    }
    assert sig["evidence"]["designated"][0]["regimes"] == ["EU"]


def test_a_list_outside_the_three_regimes_is_not_read():
    bods = _base(_own("r1", "P", "T", {"exact": 80}))
    assert _run(bods, [_designated("P", ["ch_seco_sanctions", "us_ofac_cons"])]) is None


def test_gleif_meip_voting_rights_and_ended_holdings_are_not_ownership():
    for rel in (
        _own("r1", "P", "T", {"exact": 80}, source="gleif"),
        _own("r1", "P", "T", {"exact": 80}, source="meip"),
        _own("r1", "P", "T", {"exact": 80}, itype="votingRights"),
        _own("r1", "P", "T", {"exact": 80}, end="2020-01-01"),
        _own("r1", "P", "T", None, itype="otherInfluenceOrControl"),
    ):
        assert _run(_base(rel), [_designated("P", SDN)]) is None


def test_the_subjects_own_listing_is_not_an_owner():
    bods = _base(_own("r1", "P", "T", {"exact": 80}))
    assert _run(bods, [_designated("T", SDN)]) is None


def test_a_cycle_settles():
    bods = _base(
        _own("r1", "P", "A", {"exact": 60}), _own("r2", "A", "T", {"exact": 60}),
        _own("r3", "T", "A", {"exact": 10}),
    )
    assert _regime(_run(bods, [_designated("P", SDN)]), "OFAC")["result"] == so.RESULT_MEETS


def test_canonical_nodes_join_one_partys_statements():
    # The designation names the GLEIF statement; the shareholding is on the
    # OpenSanctions statement of the same company.
    bods = [_entity("T", "Target"), _entity("G", "P (GLEIF)"), _entity("O", "P (OS)"),
            _own("r1", "O", "T", {"exact": 70})]
    sig = so.build_signal([_designated("G", SDN)], bods, ["T"], subject="LEI",
                          as_of=AS_OF, canon={"O": "G"})
    assert sig["code"] == SANCTIONS_OWNERSHIP_TEST


def test_the_evidence_says_how_it_is_computed():
    bods = _base(_own("r1", "P", "T", {"exact": 60}))
    sig = _run(bods, [_designated("P", ALL3)])
    ev = sig["evidence"]
    assert ev["method"] == "sanctions_ownership_test" and ev["as_of"] == AS_OF
    assert "control tests are not computed" in ev["description"]
    assert "not computed" in ev["not_computed"]
    assert "T-Rank" in ev["credit"]
    assert ev["statement_id"] == "T"
    assert "not a determination that the company is sanctioned" in sig["summary"]
    assert ev["designated"] == [{"party": "P", "name": "Designated P",
                                 "regimes": ["OFAC", "EU", "UK"],
                                 "datasets": sorted(ALL3), "basis": "identifier"}]


def test_stake_words():
    assert so.stake_text(0.6, False, 0.6, False) == "60%"
    assert so.stake_text(0.25, True, 0.5, False) == "more than 25% up to 50%"
    assert so.stake_text(0.1, False, None, False) == "at least 10%"
    assert so.stake_text(0.0, False, None, False) == "share not stated"
    assert so.stake_text(0.0, False, 0.1, True) == "less than 10%"


# --- the cross-check evidence ---------------------------------------------------


def test_the_cross_check_carries_datasets_and_an_identifier_match():
    from opencheck import cross_check as cc
    from opencheck.sources import SearchKind, SourceHit

    ids = [("OPENSANCTIONS", "NK-abc123"), ("XI-LEI", "894500MDF6I8J93LO347")]
    bods = [_entity("O", "Bank X", ids)]
    (target,) = cc._collect_targets(bods)
    cc._pool_identifiers([target], bods)
    hit = SourceHit(source_id="opensanctions", hit_id="NK-zzz999", kind=SearchKind.ENTITY,
                    name="Bank X", summary="", identifiers={},
                    raw={"datasets": ["us_ofac_sdn", "eu_fsf"],
                         "properties": {"leiCode": ["894500MDF6I8J93LO347"]}})
    sig = cc._make_signal(code="RELATED_SANCTIONED", target=target, hit=hit, score=1.0,
                          summary_extra="sanctioned")
    assert sig.evidence["datasets"] == ["eu_fsf", "us_ofac_sdn"]
    assert sig.evidence["identifier_match"] is True
    assert so.match_basis(sig.evidence) == so.BASIS_IDENTIFIER

    other = SourceHit(source_id="opensanctions", hit_id="NK-other1", kind=SearchKind.ENTITY,
                      name="Bank X", summary="", identifiers={}, raw={"datasets": ["eu_fsf"]})
    sig = cc._make_signal(code="RELATED_SANCTIONED", target=target, hit=other, score=1.0,
                          summary_extra="sanctioned")
    assert "identifier_match" not in sig.evidence
    assert so.match_basis(sig.evidence) == so.BASIS_NAME_ONLY


def test_the_record_id_itself_is_an_identifier():
    from opencheck import cross_check as cc
    from opencheck.sources import SearchKind, SourceHit

    bods = [_entity("O", "Bank X", [("OPENSANCTIONS", "NK-abc123")])]
    (target,) = cc._collect_targets(bods)
    cc._pool_identifiers([target], bods)
    hit = SourceHit(source_id="opensanctions", hit_id="NK-abc123", kind=SearchKind.ENTITY,
                    name="Bank X", summary="", identifiers={}, raw={})
    sig = cc._make_signal(code="RELATED_SANCTIONED", target=target, hit=hit, score=1.0,
                          summary_extra="sanctioned")
    assert sig.evidence["identifier_match"] is True


def test_the_subjects_sanctioned_signal_carries_its_datasets():
    from opencheck.risk import _opensanctions_topic_signals_from_entity

    sigs = _opensanctions_topic_signals_from_entity(
        "NK-1", {"id": "NK-1", "datasets": ["us_ofac_sdn"], "properties": {"topics": ["sanction"]}}
    )
    (s,) = [s for s in sigs if s.code == "SANCTIONED"]
    assert s.evidence["datasets"] == ["us_ofac_sdn"]


# --- reports, MCP, labels -------------------------------------------------------


def _report_signal() -> dict[str, Any]:
    bods = _base(_own("r1", "P", "A", {"exact": 60}), _own("r2", "A", "T", {"exact": 50}))
    return _run(bods, [_designated("P", SDN)])


def test_report_rows_list_each_regime_side_by_side():
    rows = so.report_rows(_report_signal())
    assert rows[0][0] == "OFAC (US)" and rows[0][2] == "Meets the ownership test"
    assert "Holding A 50% (caught company)" in rows[0][3]
    assert [r[0] for r in rows] == ["OFAC (US)", "EU", "UK"]
    assert rows[1][2] == so.RESULT_WORDS[so.RESULT_NONE]
    footer = " ".join(so.report_footer(_report_signal()))
    assert "not computed" in footer and AS_OF in footer and "T-Rank" in footer


def test_the_html_and_markdown_reports_print_the_table():
    from opencheck.reporting.html_report import build_report_html
    from opencheck.reporting.markdown_report import build_report_markdown

    report = {"lei": "213800LH1BZH3DI6G760", "legal_name": "Target Co", "bods": [],
              "risk_signals": [_report_signal()]}
    html = build_report_html(report)
    assert '<h2 id="sown">Sanctions ownership test</h2>' in html
    assert "Meets the ownership test" in html
    md = build_report_markdown(report)
    assert "## Sanctions ownership test" in md
    assert "| OFAC (US) |" in md
    bare = {"lei": "213800LH1BZH3DI6G760", "legal_name": "Target Co", "bods": []}
    assert 'id="sown"' not in build_report_html(bare)
    assert "## Sanctions ownership test" not in build_report_markdown(bare)


def test_the_owners_answer_carries_the_test():
    from opencheck.owners import sanctions_ownership_block

    block = sanctions_ownership_block([_report_signal()])
    assert block is not None
    assert block["finding"] == SANCTIONS_OWNERSHIP_TEST
    ofac = block["regimes"][0]
    assert ofac["regime"] == "OFAC" and ofac["result"] == "meets"
    assert "not computed" in block["not_computed"]
    assert sanctions_ownership_block([]) is None


@pytest.mark.parametrize("code", [SANCTIONS_OWNERSHIP_TEST, SANCTIONS_OWNERSHIP_TEST_POSSIBLE])
def test_every_label_map_names_the_codes(code):
    from opencheck.narrative.packet import _RISK_LABELS
    from opencheck.og_image import SIGNAL_STYLE

    assert code in SIGNAL_STYLE and code in _RISK_LABELS


def test_the_verdict_names_the_test_and_the_context_variant_adds_nothing():
    from opencheck.verdict import build_verdict

    v = build_verdict([_report_signal()], [])
    assert "sanctions ownership test" in v
    possible = _run(_base(_own("r1", "P", "T", None)), [_designated("P", SDN)])
    assert "ownership test" not in build_verdict([possible], [])


def test_signal_rules_version_four_names_the_codes():
    from opencheck.watchlist import SIGNAL_RULES, SIGNAL_RULES_CHANGED

    assert SIGNAL_RULES == 4
    assert SIGNAL_RULES_CHANGED[4] == {SANCTIONS_OWNERSHIP_TEST, SANCTIONS_OWNERSHIP_TEST_POSSIBLE}


def test_the_module_never_reads_the_multiplied_figure():
    """The 50% rule counts a caught company's holding in full; the Phase 339
    integrated-ownership figure multiplies. The module borrows only the
    bundle-reading helpers from ``integrated_ownership``, never its figure."""
    tree = ast.parse((_BACKEND / "opencheck" / "sanctions_ownership.py").read_text("utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported |= {a.name for a in node.names}
    assert not imported & {"compute", "for_lookup", "integrated_ownership", "with_figures"}


# --- the frontend fixture -------------------------------------------------------

_FIXTURE = _BACKEND.parent / "frontend" / "src" / "lib" / "sanctionsOwnershipFixture.json"


def _fixture_now() -> dict[str, Any]:
    return {
        "meets": _report_signal(),
        "possible": _run(
            _base(_own("r1", "P", "T", {"exact": 30}), _own("r2", "Q", "T", {"exact": 30})),
            [_designated("P", SDN), _designated("Q", SDN, ident=False)],
        ),
    }


@pytest.mark.skipif(not _FIXTURE.is_file(), reason="frontend sources not present")
def test_the_frontend_fixture_is_what_the_backend_emits():
    """``sanctionsOwnership.test.ts`` reads these two signals; regenerate the
    file with this function's output when the evidence shape changes."""
    import json

    assert json.loads(_FIXTURE.read_text("utf-8")) == json.loads(json.dumps(_fixture_now()))


# --- the lookup wiring ----------------------------------------------------------


def test_the_lookup_pipeline_merges_the_signal(monkeypatch, tmp_path):
    """The offline-bundle lookup hands the pre-merge signals, the bundle and the
    subject's statements to ``for_lookup`` and merges what it returns."""
    import json

    from fastapi.testclient import TestClient

    from opencheck.app import app
    from opencheck.config import get_settings
    from opencheck.routers import lookup as L

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    seen: dict[str, Any] = {}

    def fake(signals, statements, subject_ids, *, subject, as_of):
        seen.update(subject=subject, as_of=as_of, subject_ids=set(subject_ids),
                    n=len(statements))
        return _report_signal()

    monkeypatch.setattr(L._sanctions_ownership, "for_lookup", fake)
    get_settings.cache_clear()
    lei = "213800LH1BZH3DI6G760"
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({
        "statementId": "e-subject", "recordType": "entity",
        "recordDetails": {"name": "Bundle Co", "identifiers": [{"id": lei, "scheme": "XI-LEI"}]},
    }) + "\n")
    try:
        body = TestClient(app).get("/lookup", params={"lei": lei}).json()
    finally:
        get_settings.cache_clear()
    assert seen["subject"] == lei and "e-subject" in seen["subject_ids"]
    codes = [s["code"] for s in body["risk_signals"]]
    assert codes.count(SANCTIONS_OWNERSHIP_TEST) == 1
    assert "sanctions ownership test" in body["verdict"]
