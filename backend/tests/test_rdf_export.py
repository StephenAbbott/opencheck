"""Unit tests for the BODS v0.4 → RDF exporter (`bods/rdf.py`).

Pure-function tests over hand-built BODS statements, mirroring the style of
``test_senzing.py``. Assertions parse the emitted TriG back with rdflib rather
than string-matching, so serialisation details can change freely.
"""

from __future__ import annotations

import pytest
from rdflib import Dataset, Literal, URIRef
from rdflib.namespace import RDF, XSD

from opencheck.bods import to_rdf
from opencheck.bods.rdf import BODS, CODES, OC, REC, STMT


def _entity(sid: str, **rd) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "entity",
            "recordStatus": "new", "statementDate": "2026-07-21",
            "recordDetails": {"entityType": {"type": "registeredEntity"}, **rd}}


def _person(sid: str, **rd) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "person",
            "recordDetails": {"personType": "knownPerson", **rd}}


def _rel(sid: str, subject: str, party, interests=()) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "relationship",
            "recordDetails": {"subject": subject, "interestedParty": party,
                              "interests": list(interests)}}


def _parse(trig: str) -> Dataset:
    ds = Dataset()
    ds.parse(data=trig, format="trig")
    return ds


def _graph(ds: Dataset, sid: str):
    return ds.graph(STMT[sid])


def test_statement_gets_its_own_named_graph():
    ds = _parse(to_rdf([_entity("ent-1", name="Acme Ltd")]))
    g = _graph(ds, "ent-1")
    assert (STMT["ent-1"], RDF.type, BODS.New) in g
    assert (STMT["ent-1"], BODS.recordDetails, REC["ent-1"]) in g
    assert (STMT["ent-1"], BODS.statementDate,
            Literal("2026-07-21", datatype=XSD.date)) in g
    # Nothing about ent-1 leaks into the default graph.
    assert len(list(ds.default_context)) == 0


def test_entity_core_mapping():
    ds = _parse(to_rdf([_entity(
        "ent-1",
        name="Acme Ltd",
        alternateNames=["Acme"],
        jurisdiction={"name": "United Kingdom", "code": "GB"},
        foundingDate="1990-01-01",
        identifiers=[{"id": "5493001KJTIIGC8Y1R12", "scheme": "XI-LEI",
                      "schemeName": "LEI"}],
    )]))
    g = _graph(ds, "ent-1")
    rec = REC["ent-1"]
    assert (rec, RDF.type, BODS.Entity) in g
    assert (rec, BODS.entityType, CODES.RegisteredEntity) in g
    name = g.value(rec, BODS.name)
    assert name is not None
    assert (name, RDF.type, BODS.Legal) in g
    assert (name, BODS.fullName, Literal("Acme Ltd")) in g
    [alt] = list(g.objects(rec, BODS.alternateName))
    assert (alt, RDF.type, BODS.Alternative) in g
    assert (alt, BODS.fullName, Literal("Acme")) in g
    ident = g.value(rec, BODS.identifier)
    assert ident is not None
    assert (ident, BODS.idString, Literal("5493001KJTIIGC8Y1R12")) in g
    assert (ident, BODS.scheme, Literal("XI-LEI")) in g
    jur = g.value(rec, BODS.jurisdiction)
    assert (jur, BODS.code, Literal("GB")) in g
    assert (rec, BODS.foundingDate, Literal("1990-01-01", datatype=XSD.date)) in g


def test_legal_form_label_uses_entity_type_details():
    """Decision 2026-07-21: legal form via entityType (legalEntity + details),
    not a private predicate."""
    ds = _parse(to_rdf([_entity("ent-1", name="Maersk A/S",
                                legalFormLabel="Aktieselskab")]))
    g = _graph(ds, "ent-1")
    assert (REC["ent-1"], BODS.entityType, CODES.LegalEntity) in g
    assert (REC["ent-1"], BODS.entityTypeDetails, Literal("Aktieselskab")) in g
    assert (REC["ent-1"], BODS.entityType, CODES.RegisteredEntity) not in g


def test_relationship_interest_mapping():
    ds = _parse(to_rdf([
        _entity("ent-1", name="Subject"),
        _person("per-1", names=[{"type": "legal", "fullName": "Ada Person"}]),
        _rel("rel-1", "ent-1", "per-1", interests=[{
            "type": "shareholding", "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": True,
            "share": {"exact": 51.0},
        }]),
    ]))
    g = _graph(ds, "rel-1")
    rec = REC["rel-1"]
    assert (rec, RDF.type, BODS.Relationship) in g
    assert (rec, BODS.subject, REC["ent-1"]) in g
    assert (rec, BODS.interestedParty, REC["per-1"]) in g
    interest = g.value(rec, BODS.interest)
    assert (interest, RDF.type, BODS.Shareholding) in g
    assert (interest, BODS.directOrIndirect, CODES.Direct) in g
    assert (interest, BODS.beneficialOwnershipOrControl,
            Literal(True)) in g
    assert (interest, BODS.shareExact,
            Literal(51.0, datatype=XSD.float)) in g


def test_unspecified_party_object_becomes_bods_unspecified():
    ds = _parse(to_rdf([_rel("rel-1", "ent-1",
                             {"reason": "interested-party-exempt-from-disclosure",
                              "description": "Controlled directly by natural persons."})]))
    g = _graph(ds, "rel-1")
    party = g.value(REC["rel-1"], BODS.interestedParty)
    assert party is not None
    assert (party, RDF.type, BODS.Unspecified) in g
    assert (REC["rel-1"], BODS.subject, REC["ent-1"]) in g


def test_oo_bulk_incorporated_in_jurisdiction_is_accepted():
    stmt = _entity("ent-1", name="Tammsaare OÜ")
    del stmt["recordDetails"]["entityType"]
    stmt["recordDetails"]["entityType"] = {"type": "registeredEntity"}
    stmt["recordDetails"]["incorporatedInJurisdiction"] = {"name": "Estonia", "code": "EE"}
    ds = _parse(to_rdf([stmt]))
    g = _graph(ds, "ent-1")
    jur = g.value(REC["ent-1"], BODS.jurisdiction)
    assert jur is not None
    assert (jur, BODS.code, Literal("EE")) in g


def test_person_mapping():
    ds = _parse(to_rdf([_person("per-1", names=[
        {"type": "legal", "fullName": "Ada Person", "familyName": "Person"}])]))
    g = _graph(ds, "per-1")
    rec = REC["per-1"]
    assert (rec, RDF.type, BODS.Person) in g
    assert (rec, BODS.personType, CODES.KnownPerson) in g
    name = g.value(rec, BODS.name)
    assert name is not None
    assert (name, BODS.fullName, Literal("Ada Person")) in g
    assert (name, BODS.familyName, Literal("Person")) in g


def test_source_block_and_licence():
    """A registered source's statement carries bods:source (typed) and the
    canonical licence URI as bods:license — GLEIF is CC0."""
    stmt = _entity("ent-1", name="Acme Ltd")
    stmt["source"] = {"type": ["thirdParty"], "description": "GLEIF",
                      "retrievedAt": "2026-07-21T10:00:00Z",
                      "url": "https://www.gleif.org/lei/X"}
    ds = _parse(to_rdf([stmt]))
    g = _graph(ds, "ent-1")
    src = g.value(STMT["ent-1"], BODS.source)
    assert src is not None
    assert (src, RDF.type, BODS.Source) in g
    assert (src, RDF.type, BODS.ThirdParty) in g
    assert (src, BODS.description, Literal("GLEIF")) in g
    assert (src, OC.sourceId, Literal("gleif")) in g
    lic = g.value(STMT["ent-1"], BODS.license)
    assert lic == Literal("https://creativecommons.org/publicdomain/zero/1.0/")


def test_unregistered_source_gets_no_licence():
    stmt = _entity("ent-1", name="Acme Ltd")
    stmt["source"] = {"type": ["thirdParty"], "description": "Not A Registry"}
    ds = _parse(to_rdf([stmt]))
    g = _graph(ds, "ent-1")
    assert g.value(STMT["ent-1"], BODS.license) is None
    assert g.value(g.value(STMT["ent-1"], BODS.source), OC.sourceId) is None


def test_risk_signal_becomes_annotation_in_analysis_graph():
    lei = "5493001KJTIIGC8Y1R12"
    anchor = _entity("ent-1", name="Acme Ltd", identifiers=[
        {"id": lei, "scheme": "XI-LEI", "schemeName": "LEI"}])
    anchor["source"] = {"type": ["thirdParty"], "description": "GLEIF"}
    trig = to_rdf(
        [anchor, _entity("ent-2", name="Parent")],
        anchor_lei=lei,
        run_date="2026-07-21",
        risk_signals=[{
            "code": "COMPLEX_OWNERSHIP_LAYERS", "confidence": "medium",
            "summary": "Ownership chain has 3 corporate layers.",
            "evidence": {"layers": 3, "longest_path": ["ent-2", "ent-1"]},
        }],
    )
    ds = _parse(trig)
    ag = ds.graph(URIRef(f"https://opencheck.world/analysis/{lei}/2026-07-21"))
    ann = ag.value(STMT["ent-1"], BODS.annotation)
    assert ann is not None, "annotation must target the anchor statement"
    assert (ann, RDF.type, BODS.Annotation) in ag
    assert (ann, BODS.motivation, CODES.Commenting) in ag
    assert (ann, BODS.statementPointerTarget, Literal("/")) in ag
    assert (ann, OC.signalCode, Literal("COMPLEX_OWNERSHIP_LAYERS")) in ag
    assert (ann, OC.confidence, Literal("medium")) in ag
    evidence = set(ag.objects(ann, OC.evidenceStatement))
    assert evidence == {STMT["ent-1"], STMT["ent-2"]}
    # The analytical layer must stay out of the statement graphs.
    assert _graph(ds, "ent-1").value(STMT["ent-1"], BODS.annotation) is None


def test_possibly_same_becomes_linking_annotation():
    trig = to_rdf(
        [_entity("ent-1", name="Acme Ltd"), _entity("ent-2", name="ACME LTD")],
        run_date="2026-07-21",
        possibly_same_entities=[{
            "a": "ent-1", "b": "ent-2", "reason": "same name + jurisdiction",
            "a_name": "Acme Ltd", "b_name": "ACME LTD", "b_source": "OpenAleph",
        }],
    )
    ds = _parse(trig)
    ag = ds.graph(URIRef("https://opencheck.world/analysis/bundle/2026-07-21"))
    ann = ag.value(STMT["ent-1"], BODS.annotation)
    assert ann is not None
    assert (ann, BODS.motivation, CODES.Linking) in ag
    assert (ann, OC.linkedStatement, STMT["ent-2"]) in ag


def test_no_analysis_graph_without_analytical_inputs():
    ds = _parse(to_rdf([_entity("ent-1", name="Acme Ltd")]))
    graphs = {str(g.identifier) for g in ds.graphs()}
    assert not any("analysis" in g for g in graphs)


def test_nquads_output_parses_and_matches_trig():
    bods = [_entity("ent-1", name="Acme Ltd")]
    trig_ds = _parse(to_rdf(bods))
    nq_ds = Dataset()
    nq_ds.parse(data=to_rdf(bods, fmt="nquads"), format="nquads")
    assert sum(len(g) for g in nq_ds.graphs()) == sum(len(g) for g in trig_ds.graphs())


def test_unknown_format_raises():
    with pytest.raises(ValueError):
        to_rdf([], fmt="turtle")


def test_only_private_terms_are_in_the_oc_namespace():
    """Every predicate/class outside RDF core must come from the published BODS
    vocabulary namespaces or the single private oc: namespace — no accidental
    third namespace can creep in."""
    stmt = _entity("ent-1", name="Acme Ltd", legalFormLabel="Aktieselskab",
                   jurisdiction={"name": "Denmark", "code": "DK"})
    stmt["source"] = {"type": ["thirdParty"], "description": "GLEIF"}
    trig = to_rdf([stmt], anchor_lei=None, run_date="2026-07-21",
                  risk_signals=[], possibly_same_entities=[{
                      "a": "ent-1", "b": "ent-1", "reason": "self",
                      "b_name": "x", "b_source": "y"}])
    ds = _parse(trig)
    # standard.openownership.org/codelists# is deliberately NOT allowed: it was
    # OpenCheck's codelist namespace until Phase 107, taken from the published
    # 0.4.0 Turtle's @prefix line, which Open Ownership confirm is an error in
    # the .ttl. Listing only the corrected namespace makes a revert fail here.
    allowed = ("https://vocab.openownership.org/terms#",
               "https://vocab.openownership.org/codelists#",
               "https://opencheck.world/",
               str(RDF), "http://www.w3.org/2000/01/rdf-schema#")
    for g in ds.graphs():
        for _s, p, o in g:
            for term in (p, o):
                if isinstance(term, URIRef) and not str(term).startswith("https://www.gleif.org"):
                    assert str(term).startswith(allowed), term


def test_malformed_identifier_uri_does_not_break_serialisation():
    """Real bulk data carries the odd bad URI (trailing/embedded spaces) —
    it must be cleaned or dropped, never crash the whole corpus export."""
    ds = _parse(to_rdf([_entity("ent-1", name="X", identifiers=[
        {"id": "1", "scheme": "S", "uri": "http://economie.fgov.be/fr/entreprises/BCE "},
        {"id": "2", "scheme": "S", "uri": "not a uri at all"},
    ])]))
    g = _graph(ds, "ent-1")
    uris = {str(u) for u in g.objects(None, BODS.uri)}
    assert uris == {"http://economie.fgov.be/fr/entreprises/BCE"}


# ----------------------------------------------------------------------
# Statement annotations (Phase 107) — the register's own words in RDF
# ----------------------------------------------------------------------


def _annotated_entity():
    from opencheck.bods import annotations as A

    stmt = _entity("ent-1", name="Acme Ltd")
    A.annotate(
        stmt,
        A.transformation(
            A.pointer("recordDetails", "interests", 0, "type"),
            "Companies House nature-of-control code: "
            "ownership-of-shares-75-to-100-percent",
            transformed_content="shareholding",
            creation_date="2026-08-15",
        ),
        A.commenting(
            A.pointer("recordDetails", "birthDate"),
            "The register published month and year only.",
            creation_date="2026-08-15",
        ),
    )
    return stmt


def test_statement_annotations_round_trip():
    """Phase 103 put the register's own vocabulary into the bundle; RDF
    dropped it on the floor until Phase 107."""
    ds = _parse(to_rdf([_annotated_entity()]))
    g = _graph(ds, "ent-1")

    nodes = list(g.objects(STMT["ent-1"], BODS.annotation))
    assert len(nodes) == 2, "both annotations must reach RDF"

    by_motivation = {g.value(n, BODS.motivation): n for n in nodes}
    assert set(by_motivation) == {CODES.Transformation, CODES.Commenting}

    t = by_motivation[CODES.Transformation]
    # The pointer is the whole point — a flattened or re-derived pointer
    # would throw away the RFC6901 escaping Phase 103 got right.
    assert g.value(t, BODS.statementPointerTarget) == Literal(
        "/recordDetails/interests/0/type"
    )
    assert "ownership-of-shares-75-to-100-percent" in str(
        g.value(t, BODS.description)
    )
    assert g.value(t, BODS.transformedContent) == Literal("shareholding")

    agent = g.value(t, BODS.createdBy)
    assert g.value(agent, BODS.agentName) == Literal("OpenCheck")
    assert g.value(agent, BODS.uri) == URIRef("https://opencheck.world")


def test_statement_annotations_live_in_the_statement_graph():
    """Not the analysis graph. A statement annotation is part of the
    statement; a risk signal is a conclusion about it. Keeping them in
    different graphs is what lets a consumer ask for one and not the other."""
    trig = to_rdf(
        [_annotated_entity()],
        anchor_lei=None,
        run_date="2026-08-15",
        risk_signals=[
            {"code": "NOMINEE", "summary": "nominee arrangement",
             "confidence": "high", "evidence": {"statement_id": "ent-1"}}
        ],
    )
    ds = _parse(trig)
    stmt_graph = _graph(ds, "ent-1")
    analysis = [
        g for g in ds.graphs()
        if str(g.identifier).startswith("https://opencheck.world/analysis/")
    ]
    assert analysis, "risk signals must still produce an analysis graph"

    # Two in the statement graph, one in the analysis graph — never mixed.
    assert len(list(stmt_graph.objects(STMT["ent-1"], BODS.annotation))) == 2
    assert len(list(analysis[0].objects(STMT["ent-1"], BODS.annotation))) == 1

    sigs = [
        o for g in analysis for o in g.objects(None, OC.signalCode)
    ]
    assert Literal("NOMINEE") in sigs
    assert not list(stmt_graph.objects(None, OC.signalCode)), (
        "analysis terms must not leak into the statement graph"
    )


def test_codelist_terms_use_the_vocab_namespace():
    """The rule is 'capitalise the code and prefix it with
    https://vocab.openownership.org/codelists#'
    (vocab.openownership.org/pages/3_quickref.html). The published 0.4.0
    Turtle declares standard.openownership.org instead — Open Ownership
    confirm that .ttl @prefix line is an error, so the prose rule wins."""
    assert str(CODES) == "https://vocab.openownership.org/codelists#"
    ds = _parse(to_rdf([_annotated_entity()]))
    g = _graph(ds, "ent-1")
    assert (REC["ent-1"], BODS.entityType, CODES.RegisteredEntity) in g
    motivations = set(g.objects(None, BODS.motivation))
    assert motivations == {CODES.Transformation, CODES.Commenting}
    for term in motivations:
        assert str(term).startswith("https://vocab.openownership.org/codelists#")


def test_unannotated_statements_emit_no_annotation_triples():
    """No empty scaffolding — an unannotated statement must be byte-identical
    to what Phase 81 produced, so the Estonia-scale corpus does not grow."""
    ds = _parse(to_rdf([_entity("ent-1", name="Acme Ltd")]))
    g = _graph(ds, "ent-1")
    assert not list(g.objects(STMT["ent-1"], BODS.annotation))
    assert (None, RDF.type, BODS.Annotation) not in g


def test_unknown_motivation_falls_back_rather_than_minting_a_term():
    """A motivation outside the codelist must not silently mint
    codes:Whatever — it degrades to commenting, which is the weakest claim."""
    stmt = _entity("ent-1", name="Acme Ltd")
    stmt["annotations"] = [{
        "statementPointerTarget": "/recordDetails/name",
        "motivation": "",
        "description": "no motivation supplied",
    }]
    g = _graph(_parse(to_rdf([stmt])), "ent-1")
    node = g.value(STMT["ent-1"], BODS.annotation)
    assert g.value(node, BODS.motivation) == CODES.Commenting


def test_annotation_size_cost_is_bounded():
    """Bulk corpora are the constraint — the published Estonia export is ~82 MB
    of NQuads, so per-statement overhead compounds.

    Deliberately measured as **triples per annotation, not a size ratio**. A
    ratio measures the fixture: the same annotation is +45% on a real
    Companies House statement and +107% on the sparse one in this file,
    because the denominator differs. Triples-per-annotation is a property of
    the emitter, which is the thing a regression would change. Nine is the
    full set — type, annotation link, motivation, description, pointer,
    creationDate, transformedContent, createdBy, agentName, agent uri — minus
    the optional ones this fixture omits.
    """
    from opencheck.bods import annotations as A

    plain = _entity("ent-1", name="Acme Ltd")
    annotated = _entity("ent-1", name="Acme Ltd")
    A.annotate(annotated, A.transformation(
        A.pointer("recordDetails", "name"),
        "register said 'ACME LTD.'",
        transformed_content="Acme Ltd",
        creation_date="2026-08-15",
    ))

    before = len(list(_graph(_parse(to_rdf([plain])), "ent-1")))
    after = len(list(_graph(_parse(to_rdf([annotated])), "ent-1")))
    per_annotation = after - before
    assert 0 < per_annotation <= 11, (
        f"{per_annotation} triples for one annotation — the emitter has grown "
        "fields, which multiplies across a bulk corpus"
    )
