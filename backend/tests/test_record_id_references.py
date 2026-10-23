"""Relationships reference parties by recordId, and every consumer resolves them.

BODS v0.4: ``subject`` / ``interestedParty`` / ``componentRecords`` hold a
party's **recordId**. Every OpenCheck mapper has set ``statementId ==
recordId`` for entities and people, so a consumer keyed on ``statementId``
that reads a reference without resolving it still happens to work. Phase B of
the record-ID ticket makes ``statementId`` content-addressed, so the two will
differ on every statement — and the OECD's MEIP statements already differ
(Phase 210).

This test is the harness for that: it builds a realistic lookup bundle with
the factories, then *scrambles* it — every entity and person gets a different
``statementId`` while its ``recordId`` and every reference stay as they were,
exactly the shape phase B produces — and runs each consumer on both. The
results must agree once the scrambled run's ids are mapped back. A consumer
that compares a reference against a statementId set directly fails here, not
in production after phase B.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods.refs import resolver, statement_index
from opencheck.bods.statements import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)

LEI = "213800F4ETX85XLF5K47"  # A/S Norske Shell
PARENT_LEI = "724500KDSUZ5NW8YBM07"
HEAD_LEI = "21380068P1DRHMJ8KU70"
PREFIX = "ver1-"


@pytest.fixture(autouse=True)
def _scoped():
    with provenance.mapping_provenance(
        provenance.Provenance(liveness="live", retrieved_at=datetime.now(timezone.utc))
    ):
        yield


def _entity(source: str, local: str, name: str, code: str, idents: list[dict[str, str]],
            entity_type: str = "registeredEntity", founding: str | None = None) -> dict:
    return make_entity_statement(
        source_id=source, local_id=local, name=name,
        jurisdiction=(code, code), identifiers=idents, entity_type=entity_type,
        founding_date=founding,
    )


def bundle() -> list[dict[str, Any]]:
    """Two sources, a three-layer chain, a person, a state owner, a trust, a
    closed relationship and a two-hop indirect relationship with components."""
    subject_g = _entity("gleif", LEI, "A/S NORSKE SHELL", "NO",
                        [{"scheme": "XI-LEI", "id": LEI}, {"scheme": "NO-BRC", "id": "914807077"}])
    parent_g = _entity("gleif", PARENT_LEI, "SHELL PETROLEUM N.V.", "NL",
                       [{"scheme": "XI-LEI", "id": PARENT_LEI}])
    head_g = _entity("gleif", HEAD_LEI, "SHELL PLC", "GB", [{"scheme": "XI-LEI", "id": HEAD_LEI}])
    subject_b = _entity("brreg", "914807077", "A/S NORSKE SHELL", "NO",
                        [{"scheme": "NO-BRC", "id": "914807077"}], founding="1912-01-01")
    person = make_person_statement(
        source_id="brreg", local_id="p-1", full_name="Kari Nordmann", birth_date="1970-05",
        nationalities=[{"code": "NO", "name": "Norway"}],
    )
    state = _entity("brreg", "s-1", "NÆRINGS- OG FISKERIDEPARTEMENTET", "NO",
                    [{"scheme": "NO-BRC", "id": "912660680"}], entity_type="stateBody")
    trust = _entity("brreg", "t-1", "THE NORDMANN FAMILY TRUST", "KY", [], entity_type="arrangement")
    former = _entity("brreg", "f-1", "OLD HOLDINGS AS", "NO", [{"scheme": "NO-BRC", "id": "900000001"}])

    def rel(source: str, local: str, subj: dict, party: dict, interests: list[dict], **kw: Any) -> dict:
        return make_relationship_statement(
            source_id=source, local_id=local,
            subject_statement_id=subj["recordId"],
            interested_party_statement_id=party["recordId"],
            interested_party_type="person" if party["recordType"] == "person" else "entity",
            interests=interests, **kw,
        )

    r_direct = rel("gleif", f"{LEI}:direct-parent", subject_g, parent_g,
                   [{"type": "otherInfluenceOrControl", "directOrIndirect": "direct"}])
    r_ultimate = rel("gleif", f"{LEI}:ultimate-parent", subject_g, head_g,
                     [{"type": "otherInfluenceOrControl", "directOrIndirect": "indirect"}],
                     component_records=[r_direct["recordId"], parent_g["recordId"]])
    r_parent_head = rel("gleif", f"{PARENT_LEI}:direct-parent", parent_g, head_g,
                        [{"type": "otherInfluenceOrControl", "directOrIndirect": "direct"}])
    r_person = rel("brreg", "r-p", subject_b, person,
                   [{"type": "shareholding", "share": {"exact": 10}, "beneficialOwnershipOrControl": True}])
    r_state = rel("brreg", "r-s", subject_b, state,
                  [{"type": "shareholding", "share": {"exact": 67}}])
    r_trust = rel("brreg", "r-t", subject_b, trust,
                  [{"type": "shareholding", "share": {"minimum": 5, "maximum": 10}}])
    r_former = rel("brreg", "r-f", subject_b, former,
                   [{"type": "shareholding", "share": {"exact": 50}, "startDate": "2010-01-01",
                     "endDate": "2020-01-01"}])
    return [subject_g, parent_g, head_g, r_direct, r_ultimate, r_parent_head,
            subject_b, person, state, trust, former, r_person, r_state, r_trust, r_former]


def scramble(bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Phase B's shape: a new statementId on every entity and person, the
    recordId and every reference untouched."""
    out = []
    for s in copy.deepcopy(bods):
        if s["recordType"] in ("entity", "person"):
            s["statementId"] = PREFIX + s["statementId"]
        out.append(s)
    return out


def unscramble(value: Any) -> Any:
    """Map the scrambled run's ids back so two results can be compared."""
    if isinstance(value, str):
        return value.replace(PREFIX, "")
    if isinstance(value, dict):
        return {unscramble(k): unscramble(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [unscramble(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(unscramble(v) for v in value)
    return value


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, sort_keys=True))


def _same(plain: Any, scrambled: Any, what: str) -> None:
    # ``unscramble`` on both sides: it also turns sets into sorted lists.
    # Serialise first so an id inside a dataclass's ``str()`` is mapped too.
    assert unscramble(_json(unscramble(plain))) == unscramble(_json(unscramble(scrambled))), (
        f"{what} differs once ids are scrambled"
    )


# ---------------------------------------------------------------------------
# The shape itself
# ---------------------------------------------------------------------------


def test_the_factories_reference_parties_by_record_id() -> None:
    b = bundle()
    index = statement_index(b)
    for s in b:
        if s["recordType"] != "relationship":
            continue
        rd = s["recordDetails"]
        assert rd["subject"] in index and index[rd["subject"]]["recordId"] == rd["subject"]
        if isinstance(rd.get("interestedParty"), str):
            assert index[rd["interestedParty"]]["recordId"] == rd["interestedParty"]
        for ref in rd.get("componentRecords") or []:
            assert ref in {x["recordId"] for x in b}
        assert s["declarationSubject"] == index[rd["subject"]]["recordId"]


def test_the_scramble_is_the_phase_b_shape() -> None:
    b, sb = bundle(), scramble(bundle())
    for plain, scr in zip(b, sb):
        if plain["recordType"] == "relationship":
            assert scr == plain
        else:
            assert scr["statementId"] != scr["recordId"] == plain["recordId"]
    resolve = resolver(sb)
    assert resolve(sb[3]["recordDetails"]["subject"]) == PREFIX + b[0]["statementId"]


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


def test_risk_engine_reads_the_same_graph() -> None:
    from opencheck.risk import assess_structure

    plain = [s.to_dict() for s in assess_structure("brreg", {"entity_id": LEI}, bundle(), hit_id=LEI)]
    scr = [s.to_dict() for s in assess_structure("brreg", {"entity_id": LEI}, scramble(bundle()), hit_id=LEI)]
    assert plain, "the fixture should raise structural signals"
    _same(plain, scr, "risk signals")


def test_integrated_ownership_reads_the_same_figures() -> None:
    from opencheck.integrated_ownership import for_lookup

    _same(for_lookup(LEI, bundle(), "2026-10-23"), for_lookup(LEI, scramble(bundle()), "2026-10-23"),
          "integrated ownership")


def test_subject_identity_and_profile_agree() -> None:
    from opencheck.subject_identity import subject_identity
    from opencheck.subject_profile import build_subject_profile

    _same(subject_identity(LEI, bundle()).to_dict() if hasattr(subject_identity(LEI, bundle()), "to_dict")
          else vars(subject_identity(LEI, bundle())),
          subject_identity(LEI, scramble(bundle())).to_dict() if hasattr(subject_identity(LEI, bundle()), "to_dict")
          else vars(subject_identity(LEI, scramble(bundle()))), "subject identity")
    _same(build_subject_profile(LEI, bundle()), build_subject_profile(LEI, scramble(bundle())),
          "subject profile")


def test_consistency_compares_the_same_records() -> None:
    from opencheck.consistency import assess_consistency

    plain, scr = assess_consistency(bundle()), assess_consistency(scramble(bundle()))
    _same(plain.to_dict() if hasattr(plain, "to_dict") else vars(plain),
          scr.to_dict() if hasattr(scr, "to_dict") else vars(scr), "consistency")


def test_knowability_chain_walks_the_same_path() -> None:
    from opencheck.knowability import chain_for_lei

    _same(chain_for_lei(LEI, bundle()), chain_for_lei(LEI, scramble(bundle())), "knowability chain")


def test_possibly_same_pairs_agree() -> None:
    from opencheck.reconcile import possibly_same_entities

    _same([p.to_dict() for p in possibly_same_entities(bundle())],
          [p.to_dict() for p in possibly_same_entities(scramble(bundle()))], "possibly-same")


def test_graph_shape_counts_the_same() -> None:
    from opencheck.graph_shape import _graph_shape

    _same(_graph_shape(bundle(), []), _graph_shape(scramble(bundle()), []), "graph shape")


def test_related_party_targets_agree() -> None:
    from opencheck.cross_check import _collect_targets
    from opencheck.related_targets import select
    from opencheck.subject_identity import subject_identity

    def run(b: list[dict[str, Any]]) -> Any:
        ids = subject_identity(LEI, b).statement_ids
        sel = select(_collect_targets(b, exclude=ids), b, limit=25, subject_ids=ids)
        return sel.to_dict() if hasattr(sel, "to_dict") else vars(sel)

    _same(run(bundle()), run(scramble(bundle())), "related-party targets")


def test_exports_link_the_same_parties() -> None:
    from opencheck.bods.ftm import map_to_ftm
    from opencheck.bods.neo4j import to_cypher
    from opencheck.bods.senzing import map_to_senzing

    _same(map_to_ftm(bundle()), map_to_ftm(scramble(bundle())), "FtM")
    _same(map_to_senzing(bundle()), map_to_senzing(scramble(bundle())), "Senzing")
    _same(to_cypher(bundle()), to_cypher(scramble(bundle())), "Neo4j")


def test_rdf_links_on_record_id() -> None:
    from opencheck.bods.rdf import to_rdf

    plain = to_rdf(bundle(), fmt="nquads", anchor_lei=LEI, run_date="2026-10-23")
    scr = to_rdf(scramble(bundle()), fmt="nquads", anchor_lei=LEI, run_date="2026-10-23")
    import re

    def norm(text: str) -> list[str]:  # blank-node labels are fresh per run
        return sorted(re.sub(r"_:N[0-9a-f]+", "_:b", line) for line in text.splitlines())

    assert norm(plain) == norm(unscramble(scr))


def test_owners_tools_agree() -> None:
    from opencheck import owners
    from opencheck.integrated_ownership import for_lookup

    def run(b: list[dict[str, Any]]) -> dict[str, Any]:
        io = for_lookup(LEI, b, "2026-10-23")
        rule = owners.resolve_rule("NO", None, None)
        return {
            "canon": owners.party_canon(b),
            "calculated": owners.calculated_owners(io, rule),
            "declared": owners.declared_owners(b, LEI, owners.party_canon(b)),
        }

    _same(run(bundle()), run(scramble(bundle())), "owners")


def test_narrative_packet_names_the_same_parties() -> None:
    from opencheck.narrative import packet

    def run(b: list[dict[str, Any]]) -> Any:
        pk = packet.build_evidence_packet({"lei": LEI, "legal_name": "A/S NORSKE SHELL",
                                           "bods": b, "risk_signals": [], "hits": []})
        return pk.to_dict() if hasattr(pk, "to_dict") else vars(pk)

    _same(run(bundle()), run(scramble(bundle())), "narrative packet")


def _report(b: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "lei": LEI, "legal_name": "A/S NORSKE SHELL", "jurisdiction": "NO",
        "bods": b, "risk_signals": [], "hits": [], "degraded_sources": [],
        "derived_identifiers": {"lei": LEI}, "sources_applicable": ["gleif", "brreg"],
        "run_completed_at": "2026-10-23T10:00:00Z",
    }


def test_the_markdown_and_html_reports_draw_the_same_graph() -> None:
    from opencheck.reporting.html_report import build_report_html
    from opencheck.reporting.markdown_report import build_report_markdown

    md_plain = build_report_markdown(_report(bundle()))
    md_scr = build_report_markdown(_report(scramble(bundle())))
    assert "SHELL PLC" in md_plain and "Kari Nordmann" in md_plain
    assert md_plain == unscramble(md_scr)
    html_plain = build_report_html(_report(bundle()))
    html_scr = build_report_html(_report(scramble(bundle())))
    assert html_plain == unscramble(html_scr)


def test_the_validator_accepts_both_shapes() -> None:
    from opencheck.bods.validator import validate_shape

    assert validate_shape(bundle()) == unscramble(validate_shape(scramble(bundle()))) == []


def test_the_quality_sweep_finds_nothing_in_either_shape() -> None:
    from opencheck.bods_quality import check_statements

    plain = check_statements(bundle(), today="2026-10-23")
    scr = check_statements(scramble(bundle()), today="2026-10-23")
    # The scramble invents statementIds, which the content-addressed phase's
    # own check rightly calls stale; that check is not what this harness
    # measures.
    assert [f.check for f in plain] == [
        f.check for f in scr if f.check != "stale_statement_id"
    ]
