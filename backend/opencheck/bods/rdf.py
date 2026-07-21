"""BODS v0.4 → RDF (TriG / NQuads), extending the published BODS RDF vocabulary.

Follows Open Ownership's documented conversion pattern — one named graph per
statement (vocab.openownership.org/pages/4_convertingdata.html) — using only
terms from ``bods-vocabulary-0.4.0.ttl`` for the BODS data itself, validated in
the 2026-07-21 spike (66 vocabulary terms used, 0 unknown).

What this projection carries that no other OpenCheck export format can:

* **Machine-readable licensing** — every statement gets ``bods:license`` with
  the canonical licence URI of its asserting source (the licence matrix
  travelling *with* the data, not a ``LICENCES.md`` a human has to read).
* **The analytical layer as a detachable overlay** — risk signals,
  POSSIBLY_SAME_AS links and degraded screens are emitted as
  ``bods:Annotation`` nodes (standard terms: ``bods:motivation``,
  ``bods:statementPointerTarget``, ``bods:createdBy``) in a *separate* named
  graph, so OpenCheck's analysis can ship independently of — and point into —
  anyone's BODS statements.

Only the analytical *values* need private terms, in the ``oc:`` namespace:
``oc:sourceId``, ``oc:signalCode``, ``oc:confidence``, ``oc:evidenceStatement``,
``oc:linkedStatement``. Everything else is the published vocabulary.

Deliberate mapping choices (see the spike ticket for the full rationale):

* ``legalFormLabel`` → ``bods:entityType codes:LegalEntity`` +
  ``bods:entityTypeDetails "<local label>"`` — the published term for a local
  legal-form name, so no private predicate is needed.
* Known 0.4.0 ttl domain/range glitches (``bods:source`` domain, ``bods:url``/
  ``bods:description`` domains, identifier ``bods:uri``, statement-level
  ``bods:declaration``) are used per the *documented intent* rather than the
  erroneous machine-readable domains; reported upstream on openownership/bodsld.

Pure and side-effect-free, like ``senzing.py`` / ``ftm.py`` / ``neo4j.py``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from urllib.parse import quote, urlsplit

from rdflib import BNode, Dataset, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD

BODS = Namespace("https://vocab.openownership.org/terms#")
CODES = Namespace("https://standard.openownership.org/codelists#")
STMT = Namespace("https://opencheck.world/statements/")
REC = Namespace("https://opencheck.world/records/")
OC = Namespace("https://opencheck.world/ns#")

_STATUS_CLASS = {"new": BODS.New, "updated": BODS.Updated, "closed": BODS.Closed}
_SOURCE_TYPE_CLASS = {
    "officialRegister": BODS.OfficialRegister,
    "thirdParty": BODS.ThirdParty,
    "primaryResearch": BODS.PrimaryResearch,
    "selfDeclaration": BODS.SelfDeclaration,
    "verified": BODS.Verified,
}
_ADDR_CLASS = {
    "registered": BODS.Registered,
    "business": BODS.Business,
    "service": BODS.Service,
    "residence": BODS.Residence,
}
_DIRECT_OR_INDIRECT = {
    "direct": CODES.Direct,
    "indirect": CODES.Indirect,
    "unknown": CODES.Unknown,
}
#: JSON interestType code → vocabulary class (interests are typed, not coded).
_INTEREST_CLASS = {
    "shareholding": BODS.Shareholding,
    "votingRights": BODS.VotingRights,
    "appointmentOfBoard": BODS.AppointmentOfBoard,
    "otherInfluenceOrControl": BODS.OtherInfluenceOrControl,
    "seniorManagingOfficial": BODS.SeniorManagingOfficial,
    "settlor": BODS.Settlor,
    "trustee": BODS.Trustee,
    "protector": BODS.Protector,
    "nominee": BODS.Nominee,
    "nominator": BODS.Nominator,
    "beneficiaryOfLegalArrangement": BODS.BeneficiaryOfLegalArrangement,
    "rightsToSurplusAssetsOnDissolution": BODS.RightsToSurplusAssetsOnDissolution,
    "rightsToProfitOrIncome": BODS.RightsToProfitOrIncome,
    "rightToProfitOrIncomeFromAssets": BODS.RightToProfitOrIncomeFromAssets,
    "controlByLegalFramework": BODS.ControlByLegalFramework,
    "controlViaCompanyRulesOrArticles": BODS.ControlViaCompanyRulesOrArticles,
    "conditionalRightsGrantedByContract": BODS.ConditionalRightsGrantedByContract,
    "rightsGrantedByContract": BODS.RightsGrantedByContract,
    "enjoymentAndUseOfAssets": BODS.EnjoymentAndUseOfAssets,
    "boardMember": BODS.BoardMember,
    "boardChair": BODS.BoardChair,
    "unknownInterest": BODS.UnknownInterest,
    "unpublishedInterest": BODS.UnpublishedInterest,
}


def _uri_or_none(value: Any) -> URIRef | None:
    """A cleaned URIRef, or None when the value can't serialise as a URI.

    Real-world BODS carries the odd malformed identifier URI (trailing spaces,
    embedded blanks — e.g. the Estonia bulk snapshot); those must not be able
    to break serialisation of the whole corpus."""
    if not isinstance(value, str):
        return None
    cleaned = quote(value.strip(), safe=":/?#[]@!$&'()*+,;=%~-._")
    parts = urlsplit(cleaned)
    if not parts.scheme or not (parts.netloc or parts.path):
        return None
    return URIRef(cleaned)


def _date_lit(value: Any) -> Literal:
    text = str(value)
    return Literal(text, datatype=XSD.dateTime if "T" in text else XSD.date)


def _code_term(value: str) -> URIRef:
    """camelCase codelist value → codes: class (registeredEntity → codes:RegisteredEntity)."""
    return CODES[value[0].upper() + value[1:]] if value else CODES.UnknownEntity


def _license_literal_for(stmt: dict[str, Any]) -> Literal | None:
    """The canonical licence URI (or identifier) of the statement's source."""
    from ..licensing import most_restrictive
    from .senzing import _source_ids_of

    source_ids = _source_ids_of(stmt)
    if not source_ids:
        return None
    lic = most_restrictive(source_ids)
    if lic is None:
        return None
    return Literal(lic.terms.url or lic.terms.license)


def _add_name(g, holder, name_obj: Any, cls=BODS.Legal, prop=BODS.name) -> None:
    node = BNode()
    g.add((holder, prop, node))
    g.add((node, RDF.type, cls))
    if isinstance(name_obj, str):
        g.add((node, BODS.fullName, Literal(name_obj)))
        return
    for key, prop in (
        ("fullName", BODS.fullName),
        ("givenName", BODS.givenName),
        ("familyName", BODS.familyName),
        ("patronymicName", BODS.patronymicName),
    ):
        if name_obj.get(key):
            g.add((node, prop, Literal(name_obj[key])))


def _add_jurisdiction(g, holder, prop, jur: dict[str, Any]) -> None:
    node = BNode()
    g.add((holder, prop, node))
    g.add((node, RDF.type, BODS.Jurisdiction))
    if jur.get("code"):
        g.add((node, BODS.code, Literal(jur["code"])))
    if jur.get("name"):
        # No name property on Jurisdiction in the published ttl; rdfs:label is
        # the least-invention way to keep the human-readable name.
        g.add((node, RDFS.label, Literal(jur["name"])))


def _add_source(g, stmt_uri, stmt: dict[str, Any]) -> None:
    src = stmt.get("source") or {}
    if not src:
        return
    from .senzing import _source_ids_of

    node = BNode()
    g.add((stmt_uri, BODS.source, node))
    g.add((node, RDF.type, BODS.Source))
    for t in src.get("type") or []:
        cls = _SOURCE_TYPE_CLASS.get(t)
        if cls is not None:
            g.add((node, RDF.type, cls))
    if src.get("description"):
        g.add((node, BODS.description, Literal(src["description"])))
    src_url = _uri_or_none(src.get("url"))
    if src_url is not None:
        g.add((node, BODS.url, src_url))
    if src.get("retrievedAt"):
        g.add((node, BODS.retrievedAt, Literal(src["retrievedAt"], datatype=XSD.dateTime)))
    for sid in sorted(_source_ids_of(stmt)):
        g.add((node, OC.sourceId, Literal(sid)))


def _add_entity(g, rec_uri, rd: dict[str, Any]) -> None:
    g.add((rec_uri, RDF.type, BODS.Entity))
    legal_form = rd.get("legalFormLabel")
    entity_type = (rd.get("entityType") or {}).get("type") or "registeredEntity"
    if legal_form:
        # Decision 2026-07-21: express the legal form through entityType —
        # type legalEntity, details = the local legal-form label.
        g.add((rec_uri, BODS.entityType, CODES.LegalEntity))
        g.add((rec_uri, BODS.entityTypeDetails, Literal(legal_form)))
    else:
        g.add((rec_uri, BODS.entityType, _code_term(entity_type)))
    if rd.get("name"):
        _add_name(g, rec_uri, rd["name"])
    for alt in rd.get("alternateNames") or []:
        _add_name(g, rec_uri, alt, cls=BODS.Alternative, prop=BODS.alternateName)
    for ident in rd.get("identifiers") or []:
        node = BNode()
        g.add((rec_uri, BODS.identifier, node))
        g.add((node, RDF.type, BODS.Identifier))
        if ident.get("id"):
            g.add((node, BODS.idString, Literal(ident["id"])))
        if ident.get("scheme"):
            g.add((node, BODS.scheme, Literal(ident["scheme"])))
        if ident.get("schemeName"):
            g.add((node, BODS.schemeName, Literal(ident["schemeName"])))
        ident_uri = _uri_or_none(ident.get("uri"))
        if ident_uri is not None:
            g.add((node, BODS.uri, ident_uri))
    # OpenCheck's mapper emits ``jurisdiction``; Open Ownership's bulk BODS
    # (e.g. gleif_version_0_4) emits ``incorporatedInJurisdiction`` — accept both.
    jurisdiction = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction")
    if jurisdiction:
        _add_jurisdiction(g, rec_uri, BODS.jurisdiction, jurisdiction)
    if rd.get("foundingDate"):
        g.add((rec_uri, BODS.foundingDate, _date_lit(rd["foundingDate"])))
    if rd.get("dissolutionDate"):
        g.add((rec_uri, BODS.dissolutionDate, _date_lit(rd["dissolutionDate"])))
    for addr in rd.get("addresses") or []:
        node = BNode()
        g.add((rec_uri, BODS.address, node))
        g.add((node, RDF.type, _ADDR_CLASS.get(addr.get("type") or "", BODS.Address)))
        if addr.get("address"):
            g.add((node, BODS.streetAddress, Literal(addr["address"])))
        if addr.get("postCode"):
            g.add((node, BODS.postCode, Literal(addr["postCode"])))
        country = addr.get("country")
        if isinstance(country, dict):
            _add_jurisdiction(g, node, BODS.country, country)
        elif isinstance(country, str) and country:
            _add_jurisdiction(g, node, BODS.country, {"code": country})


def _add_person(g, rec_uri, rd: dict[str, Any]) -> None:
    g.add((rec_uri, RDF.type, BODS.Person))
    g.add((rec_uri, BODS.personType, _code_term(rd.get("personType") or "knownPerson")))
    for name in rd.get("names") or []:
        legal = isinstance(name, str) or name.get("type") in (None, "legal")
        cls = BODS.Legal if legal else BODS.Alternative
        _add_name(g, rec_uri, name, cls=cls)
    for nat in rd.get("nationalities") or []:
        if isinstance(nat, dict):
            _add_jurisdiction(g, rec_uri, BODS.nationality, nat)
    if rd.get("birthDate"):
        g.add((rec_uri, BODS.birthDate, _date_lit(rd["birthDate"])))
    if rd.get("deathDate"):
        g.add((rec_uri, BODS.deathDate, _date_lit(rd["deathDate"])))
    for addr in rd.get("addresses") or []:
        node = BNode()
        g.add((rec_uri, BODS.address, node))
        g.add((node, RDF.type, _ADDR_CLASS.get(addr.get("type") or "", BODS.Address)))
        if addr.get("address"):
            g.add((node, BODS.streetAddress, Literal(addr["address"])))


def _add_relationship(g, rec_uri, rd: dict[str, Any]) -> None:
    g.add((rec_uri, RDF.type, BODS.Relationship))
    # Only plain statement-id references resolve to records; an "unspecified"
    # (unknown) party object has no record URI (same rule as neo4j.py).
    if isinstance(rd.get("subject"), str):
        g.add((rec_uri, BODS.subject, REC[rd["subject"]]))
    if isinstance(rd.get("interestedParty"), str):
        g.add((rec_uri, BODS.interestedParty, REC[rd["interestedParty"]]))
    elif isinstance(rd.get("interestedParty"), dict):
        # Reporting-exception form: an unspecified party {reason, description}.
        # bods:Unspecified is the published class; reason/description keep the
        # least-invention labels (no unspecified* properties exist in the ttl).
        party = rd["interestedParty"]
        node = BNode()
        g.add((rec_uri, BODS.interestedParty, node))
        g.add((node, RDF.type, BODS.Unspecified))
        if party.get("reason"):
            g.add((node, RDFS.label, Literal(party["reason"])))
        if party.get("description"):
            g.add((node, RDFS.comment, Literal(party["description"])))
    for interest in rd.get("interests") or []:
        node = BNode()
        g.add((rec_uri, BODS.interest, node))
        interest_cls = _INTEREST_CLASS.get(interest.get("type") or "", BODS.UnknownInterest)
        g.add((node, RDF.type, interest_cls))
        doi = _DIRECT_OR_INDIRECT.get(interest.get("directOrIndirect") or "")
        if doi is not None:
            g.add((node, BODS.directOrIndirect, doi))
        if "beneficialOwnershipOrControl" in interest:
            g.add((node, BODS.beneficialOwnershipOrControl,
                   Literal(bool(interest["beneficialOwnershipOrControl"]))))
        if interest.get("details"):
            g.add((node, BODS.details, Literal(interest["details"])))
        share = interest.get("share") or {}
        for key, prop in (
            ("exact", BODS.shareExact),
            ("minimum", BODS.shareMinimum),
            ("maximum", BODS.shareMaximum),
            ("exclusiveMinimum", BODS.shareExclusiveMinimum),
            ("exclusiveMaximum", BODS.shareExclusiveMaximum),
        ):
            if share.get(key) is not None:
                g.add((node, prop, Literal(float(share[key]), datatype=XSD.float)))
        if interest.get("startDate"):
            g.add((node, BODS.startDate, _date_lit(interest["startDate"])))
        if interest.get("endDate"):
            g.add((node, BODS.endDate, _date_lit(interest["endDate"])))


def _build_dataset(bods_statements: Iterable[dict[str, Any]]) -> tuple[Dataset, set[str]]:
    ds = Dataset()
    for prefix, ns in (("bods", BODS), ("codes", CODES), ("stmt", STMT), ("rec", REC), ("oc", OC)):
        ds.bind(prefix, ns)

    statement_ids: set[str] = set()
    for stmt in bods_statements or []:
        sid = stmt.get("statementId")
        if not sid:
            continue
        statement_ids.add(sid)
        stmt_uri = STMT[sid]
        rec_id = stmt.get("recordId") or sid
        rec_uri = REC[rec_id]
        g = ds.graph(stmt_uri)  # one named graph per statement

        status_cls = _STATUS_CLASS.get(stmt.get("recordStatus") or "new", BODS.Statement)
        g.add((stmt_uri, RDF.type, status_cls))
        g.add((stmt_uri, BODS.statementIdString, Literal(sid)))
        g.add((stmt_uri, BODS.recordIdString, Literal(rec_id)))
        if stmt.get("statementDate"):
            g.add((stmt_uri, BODS.statementDate, _date_lit(stmt["statementDate"])))
        pub = stmt.get("publicationDetails") or {}
        if pub.get("bodsVersion"):
            g.add((stmt_uri, BODS.bodsVersion, Literal(pub["bodsVersion"])))
        if pub.get("publicationDate"):
            g.add((stmt_uri, BODS.publicationDate, _date_lit(pub["publicationDate"])))
        publisher = (pub.get("publisher") or {}).get("name")
        if publisher:
            agent = BNode()
            g.add((stmt_uri, BODS.publisher, agent))
            g.add((agent, RDF.type, BODS.Agent))
            g.add((agent, BODS.agentName, Literal(publisher)))
        if isinstance(stmt.get("declarationSubject"), str):
            g.add((stmt_uri, BODS.declaration, Literal(stmt["declarationSubject"])))
        lic = _license_literal_for(stmt)
        if lic is not None:
            g.add((stmt_uri, BODS.license, lic))
        _add_source(g, stmt_uri, stmt)

        g.add((stmt_uri, BODS.recordDetails, rec_uri))
        rd = stmt.get("recordDetails") or {}
        record_type = stmt.get("recordType")
        if record_type == "entity":
            _add_entity(g, rec_uri, rd)
        elif record_type == "person":
            _add_person(g, rec_uri, rd)
        elif record_type == "relationship":
            _add_relationship(g, rec_uri, rd)
    return ds, statement_ids


def _anchor_statement(
    bods_statements: list[dict[str, Any]], anchor_lei: str | None
) -> URIRef | None:
    """The GLEIF-anchored entity statement for the subject LEI, if present."""
    if not anchor_lei:
        return None
    fallback: URIRef | None = None
    for stmt in bods_statements or []:
        if stmt.get("recordType") != "entity" or not stmt.get("statementId"):
            continue
        for ident in (stmt.get("recordDetails") or {}).get("identifiers") or []:
            if ident.get("scheme") == "XI-LEI" and ident.get("id") == anchor_lei:
                if ((stmt.get("source") or {}).get("description") or "") == "GLEIF":
                    return STMT[stmt["statementId"]]
                fallback = fallback or STMT[stmt["statementId"]]
    return fallback


def _add_annotations(
    ds: Dataset,
    statement_ids: set[str],
    *,
    anchor: URIRef | None,
    graph_uri: URIRef,
    run_date: str,
    risk_signals: list[dict[str, Any]],
    possibly_same_entities: list[dict[str, Any]],
    degraded_sources: list[dict[str, Any]],
) -> None:
    ag = ds.graph(graph_uri)

    def _annotation(target: URIRef, motivation: URIRef, description: str) -> BNode:
        node = BNode()
        ag.add((target, BODS.annotation, node))
        ag.add((node, RDF.type, BODS.Annotation))
        ag.add((node, BODS.motivation, motivation))
        ag.add((node, BODS.description, Literal(description)))
        ag.add((node, BODS.statementPointerTarget, Literal("/")))  # whole statement (RFC 6901)
        ag.add((node, BODS.creationDate, Literal(run_date, datatype=XSD.date)))
        agent = BNode()
        ag.add((node, BODS.createdBy, agent))
        ag.add((agent, RDF.type, BODS.Agent))
        ag.add((agent, BODS.agentName, Literal("OpenCheck risk engine")))
        return node

    for signal in risk_signals:
        evidence = signal.get("evidence") or {}
        target: URIRef | None = None
        for key in ("statement_id", "subject_statement_id"):
            if evidence.get(key) in statement_ids:
                target = STMT[evidence[key]]
        if target is None:
            path = [STMT[x] for x in evidence.get("longest_path") or [] if x in statement_ids]
            target = anchor if anchor in path else (path[0] if path else anchor)
        if target is None:
            continue
        node = _annotation(
            target, CODES.Commenting,
            f"{signal.get('code', '')}: {signal.get('summary', '')}".strip(": "),
        )
        if signal.get("code"):
            ag.add((node, OC.signalCode, Literal(signal["code"])))
        if signal.get("confidence"):
            ag.add((node, OC.confidence, Literal(signal["confidence"])))
        for path_sid in evidence.get("longest_path") or []:
            if path_sid in statement_ids:
                ag.add((node, OC.evidenceStatement, STMT[path_sid]))

    for pair in possibly_same_entities:
        if pair.get("a") not in statement_ids:
            continue
        node = _annotation(
            STMT[pair["a"]], CODES.Linking,
            f"POSSIBLY_SAME_AS {pair.get('b_name', '')} ({pair.get('b_source', '')}): "
            f"{pair.get('reason', '')}",
        )
        if pair.get("b") in statement_ids:
            ag.add((node, OC.linkedStatement, STMT[pair["b"]]))

    for degraded in degraded_sources:
        if anchor is None:
            break
        detail = degraded.get("detail") or degraded.get("reason") or ""
        node = _annotation(
            anchor, CODES.Commenting,
            f"DEGRADED_SOURCE {degraded.get('source_id', '')}: "
            f"{degraded.get('check', '')} did not fully run ({detail})".strip(),
        )
        if degraded.get("source_id"):
            ag.add((node, OC.sourceId, Literal(degraded["source_id"])))


def to_rdf(
    bods_statements: list[dict[str, Any]],
    *,
    fmt: str = "trig",
    anchor_lei: str | None = None,
    run_date: str | None = None,
    risk_signals: list[dict[str, Any]] | None = None,
    possibly_same_entities: list[dict[str, Any]] | None = None,
    degraded_sources: list[dict[str, Any]] | None = None,
) -> str:
    """Render a BODS bundle as RDF — ``fmt`` is ``"trig"`` or ``"nquads"``.

    With any of ``risk_signals`` / ``possibly_same_entities`` /
    ``degraded_sources`` supplied, OpenCheck's analytical layer is emitted as
    ``bods:Annotation`` nodes in a separate named graph
    (``https://opencheck.world/analysis/{anchor}/{run_date}``) targeting the
    statement ids the risk engine already stamps on its evidence.
    """
    if fmt not in ("trig", "nquads"):
        raise ValueError(f"Unknown RDF format {fmt!r} (expected 'trig' or 'nquads')")

    ds, statement_ids = _build_dataset(bods_statements)

    date = run_date or next(
        (s["statementDate"] for s in bods_statements or [] if s.get("statementDate")), ""
    )
    if risk_signals or possibly_same_entities or degraded_sources:
        anchor = _anchor_statement(bods_statements, anchor_lei)
        graph_uri = URIRef(
            f"https://opencheck.world/analysis/{anchor_lei or 'bundle'}/{date or 'undated'}"
        )
        _add_annotations(
            ds, statement_ids,
            anchor=anchor,
            graph_uri=graph_uri,
            run_date=date or "",
            risk_signals=risk_signals or [],
            possibly_same_entities=possibly_same_entities or [],
            degraded_sources=degraded_sources or [],
        )

    return ds.serialize(format=fmt)
