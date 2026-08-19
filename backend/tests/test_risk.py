"""Tests for the deterministic risk-signal rules."""

from __future__ import annotations

from opencheck.risk import (
    COUNTER_SANCTIONED,
    DEBARMENT,
    GLEIF_REPORTING_EXCEPTION,
    OFFSHORE_LEAKS,
    OPAQUE_OWNERSHIP,
    PEP,
    SANCTIONED,
    SANCTIONS_CONTROLLED,
    SANCTIONS_LINKED,
    assess_bundle,
    assess_hit,
    assess_hits,
    classify_sanction_topics,
)
from opencheck.sources import SearchKind, SourceHit


def _hit(source_id: str, hit_id: str, *, kind=SearchKind.ENTITY, is_stub=False, **raw) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=kind,
        name=f"{source_id} {hit_id}",
        summary="",
        identifiers={},
        raw=raw,
        is_stub=is_stub,
    )


# ---------------------------------------------------------------------
# Search-time signals (assess_hit / assess_hits)
# ---------------------------------------------------------------------


def test_pep_signal_from_opensanctions_topic() -> None:
    hit = _hit(
        "opensanctions",
        "NK-putin",
        kind=SearchKind.PERSON,
        topics=["role.pep", "role.head-of-state"],
    )
    signals = assess_hit(hit)
    assert len(signals) == 1
    assert signals[0].code == PEP
    assert signals[0].confidence == "high"
    assert signals[0].evidence["topics"] == ["role.pep"]


def test_sanctioned_signal_from_opensanctions_topic() -> None:
    hit = _hit(
        "opensanctions",
        "NK-bp",
        topics=["sanction"],
    )
    signals = assess_hit(hit)
    assert len(signals) == 1
    assert signals[0].code == SANCTIONED
    assert signals[0].confidence == "high"


def test_counter_sanction_is_not_reported_as_sanctioned() -> None:
    """A Russian MFA retaliation listing is a direct listing of the record,
    but not a designation by an authority the reader owes anything to.
    Until Phase 105 it collapsed into SANCTIONED, so a Wall Street Journal
    journalist on Russia's counter-list rendered identically to an OFAC
    designation."""
    hit = _hit("opensanctions", "NK-counter", topics=["sanction.counter"])
    signals = assess_hit(hit)
    assert [s.code for s in signals] == [COUNTER_SANCTIONED]
    assert signals[0].confidence == "high"
    assert signals[0].evidence["topics"] == ["sanction.counter"]
    summary = signals[0].summary
    assert "counter-sanctions regime" in summary
    # The copy must not leave a reader thinking this is a mainstream listing.
    assert "not a designation by an EU, UK, US, UN" in summary


def test_counter_and_real_sanction_both_report() -> None:
    """Both are listings of the record itself and neither substitutes for
    the other — a record on OFAC *and* on a counter-list carries two
    distinct facts, and SANCTIONED must lead."""
    hit = _hit(
        "opensanctions", "NK-both", topics=["sanction", "sanction.counter"]
    )
    assert [s.code for s in assess_hit(hit)] == [SANCTIONED, COUNTER_SANCTIONED]


def test_sanction_linked_is_not_sanctioned() -> None:
    """`sanction.linked` means connected to — not the subject of — sanctions.
    It must surface as SANCTIONS_LINKED (medium), never SANCTIONED. This is
    the Vale S.A. false-positive guard. Vale's record also carries
    `debarment`, which surfaces as its own DEBARMENT signal."""
    hit = _hit(
        "opensanctions",
        "NK-vale",
        topics=["corp.public", "sanction.linked", "debarment"],
    )
    signals = assess_hit(hit)
    codes = {s.code for s in signals}
    assert SANCTIONED not in codes
    assert codes == {SANCTIONS_LINKED, DEBARMENT}
    linked = next(s for s in signals if s.code == SANCTIONS_LINKED)
    assert linked.confidence == "medium"
    assert "not itself sanctioned" in linked.summary


def test_sanction_control_is_its_own_signal_not_mere_adjacency() -> None:
    """A subsidiary of a designated party is not "standing next to" one.

    OpenSanctions declares ``sanction.linked`` a superset of
    ``sanction.control``, so the real-world payload carries both topics. Only
    the stronger signal should surface — the weaker chip is the same fact
    stated less precisely.
    """
    hit = _hit(
        "opensanctions",
        "NK-subsidiary",
        topics=["corp.public", "sanction.control", "sanction.linked"],
    )
    signals = assess_hit(hit)
    codes = {s.code for s in signals}
    assert codes == {SANCTIONS_CONTROLLED}
    assert SANCTIONS_LINKED not in codes
    assert SANCTIONED not in codes

    controlled = signals[0]
    # The ownership assertion is deterministic — OpenSanctions walked the
    # chain. What is uncertain is the legal threshold, and that belongs in the
    # copy, not in the confidence dot.
    assert controlled.confidence == "high"
    assert controlled.evidence["topics"] == ["sanction.control"]
    assert "not itself designated" in controlled.summary
    assert "50 Percent Rule" in controlled.summary


def test_direct_listing_and_control_both_reported() -> None:
    """An entity can be designated in its own right *and* sit inside another
    designated party's ownership chain. Both are real, separate facts, so both
    are reported (Stephen's call, 2026-08-13) — unlike the linked/control
    superset, where one is merely a vaguer statement of the other."""
    hit = _hit(
        "opensanctions",
        "NK-both",
        topics=["sanction", "sanction.control", "sanction.linked"],
    )
    codes = {s.code for s in assess_hit(hit)}
    assert codes == {SANCTIONED, SANCTIONS_CONTROLLED}
    assert SANCTIONS_LINKED not in codes


def test_plain_adjacency_is_not_upgraded_to_control() -> None:
    """The guard in the other direction: a director or family member of a
    designated party carries ``sanction.linked`` alone and must stay in the
    softer bucket."""
    hit = _hit("opensanctions", "NK-director", topics=["sanction.linked"])
    signals = assess_hit(hit)
    assert [s.code for s in signals] == [SANCTIONS_LINKED]
    assert signals[0].confidence == "medium"


def test_classify_sanction_topics_splits_the_family() -> None:
    """The classifier separates the four meanings; the call sites rank them.

    ``sanction.linked`` is declared upstream as a superset of
    ``sanction.control``, so the real-world shape for a subsidiary of a
    sanctioned party is *both* topics on one record.
    """
    both = classify_sanction_topics(["corp.public", "sanction.control", "sanction.linked"])
    assert both.control == ("sanction.control",)
    assert both.linked == ("sanction.linked",)
    assert both.direct == ()
    assert both.unknown == ()

    direct = classify_sanction_topics(["sanction", "sanction.counter"])
    assert direct.direct == ("sanction",)
    assert direct.counter == ("sanction.counter",)
    assert not direct.control and not direct.linked

    assert not classify_sanction_topics(["role.pep", "debarment"])


def test_classify_sanction_topics_flags_unknown_subtopics(caplog) -> None:
    """A new upstream ``sanction.*`` subtopic still degrades to "linked", but
    it must never do so *silently* — that is exactly how ``sanction.control``
    spent months being reported as plain adjacency."""
    with caplog.at_level("WARNING"):
        result = classify_sanction_topics(["sanction.somethingnew"])
    assert result.unknown == ("sanction.somethingnew",)
    assert result.linked == ()
    assert "sanction.somethingnew" in caplog.text


def test_unknown_sanction_subtopic_still_reports_as_linked() -> None:
    """End-to-end conservative default: an unrecognised subtopic must never
    escalate to SANCTIONED."""
    hit = _hit("opensanctions", "NK-future", topics=["sanction.somethingnew"])
    codes = {s.code for s in assess_hit(hit)}
    assert codes == {SANCTIONS_LINKED}


def test_debarment_signal_from_opensanctions_topic() -> None:
    """The `debarment` topic → a DEBARMENT signal (excluded from public
    contracts), independent of any sanctions status."""
    hit = _hit("opensanctions", "NK-debar", topics=["debarment"])
    signals = assess_hit(hit)
    assert [s.code for s in signals] == [DEBARMENT]
    assert signals[0].confidence == "high"
    assert "public contracts" in signals[0].summary
    assert SANCTIONED not in {s.code for s in signals}


def test_pep_and_sanctions_linked_can_co_occur() -> None:
    hit = _hit(
        "opensanctions",
        "NK-double",
        kind=SearchKind.PERSON,
        topics=["role.pep", "sanction.linked"],
    )
    codes = {s.code for s in assess_hit(hit)}
    assert codes == {PEP, SANCTIONS_LINKED}


def test_topics_can_live_under_properties() -> None:
    """OpenSanctions sometimes nests topics inside ``properties``."""
    hit = _hit(
        "opensanctions",
        "NK-nested",
        properties={"topics": ["sanction"]},
    )
    signals = assess_hit(hit)
    assert [s.code for s in signals] == [SANCTIONED]


def test_everypolitician_hit_is_pep_by_construction() -> None:
    hit = _hit(
        "everypolitician",
        "Q7747-pep",
        kind=SearchKind.PERSON,
    )
    signals = assess_hit(hit)
    assert len(signals) == 1
    assert signals[0].code == PEP
    assert signals[0].evidence == {"dataset": "peps"}


def test_everypolitician_entity_kind_is_not_signalled() -> None:
    """Entity searches against EveryPolitician shouldn't fire PEP."""
    hit = _hit(
        "everypolitician",
        "X",
        kind=SearchKind.ENTITY,
    )
    assert assess_hit(hit) == []


def test_stub_hits_never_signal() -> None:
    hit = _hit(
        "opensanctions",
        "NK-stub-0001",
        kind=SearchKind.PERSON,
        is_stub=True,
        topics=["role.pep", "sanction"],
    )
    assert assess_hit(hit) == []


def test_assess_hits_dedupes_within_source() -> None:
    """If two records on the same hit fire the same code, dedupe."""
    a = _hit("opensanctions", "NK-1", topics=["role.pep"])
    b = _hit("opensanctions", "NK-1", topics=["role.pep"])  # dup
    c = _hit("opensanctions", "NK-2", topics=["role.pep"])
    signals = assess_hits([a, b, c])
    assert len(signals) == 2
    assert {s.hit_id for s in signals} == {"NK-1", "NK-2"}


# ---------------------------------------------------------------------
# Deepen-time signals (assess_bundle)
# ---------------------------------------------------------------------


def test_assess_bundle_opensanctions_pep() -> None:
    raw = {
        "source_id": "opensanctions",
        "entity_id": "NK-putin",
        "entity": {
            "id": "NK-putin",
            "topics": ["role.pep"],
            "schema": "Person",
        },
    }
    signals = assess_bundle("opensanctions", raw)
    assert [s.code for s in signals] == [PEP]
    assert signals[0].source_id == "opensanctions"
    assert signals[0].hit_id == "NK-putin"


def test_assess_bundle_everypolitician_always_pep() -> None:
    raw = {
        "source_id": "everypolitician",
        "entity_id": "Q7747-pep",
        "entity": {"id": "Q7747-pep", "schema": "Person", "topics": []},
    }
    signals = assess_bundle("everypolitician", raw)
    assert [s.code for s in signals] == [PEP]


def test_assess_bundle_everypolitician_with_sanction() -> None:
    raw = {
        "source_id": "everypolitician",
        "entity_id": "Q7747-pep",
        "entity": {
            "id": "Q7747-pep",
            "schema": "Person",
            "topics": ["sanction"],
        },
    }
    codes = {s.code for s in assess_bundle("everypolitician", raw)}
    assert codes == {PEP, SANCTIONED}


def test_offshore_leaks_signal_from_panama_papers_collection() -> None:
    raw = {
        "source_id": "openaleph",
        "entity_id": "aleph-123",
        "entity": {"id": "aleph-123", "schema": "Company"},
        "collection": {
            "foreign_id": "panama_papers",
            "label": "Panama Papers",
        },
    }
    signals = assess_bundle("openaleph", raw)
    assert [s.code for s in signals] == [OFFSHORE_LEAKS]
    assert signals[0].confidence == "medium"
    assert "panama" in signals[0].evidence["match"]["foreign_id"]


def test_offshore_leaks_signal_from_label_when_foreign_id_missing() -> None:
    raw = {
        "source_id": "openaleph",
        "entity_id": "aleph-456",
        "entity": {"id": "aleph-456"},
        "collection": {"label": "ICIJ Offshore Leaks"},
    }
    signals = assess_bundle("openaleph", raw)
    assert [s.code for s in signals] == [OFFSHORE_LEAKS]
    assert signals[0].evidence["match"]["label"] == "icij offshore leaks"


def test_no_offshore_leaks_signal_for_unrelated_collection() -> None:
    raw = {
        "source_id": "openaleph",
        "entity_id": "aleph-789",
        "entity": {"id": "aleph-789"},
        "collection": {"foreign_id": "us_companies", "label": "US Companies"},
    }
    assert assess_bundle("openaleph", raw) == []


def test_wikidata_pep_when_position_is_currently_held() -> None:
    raw = {
        "qid": "Q7747",
        "is_person": True,
        "is_entity": False,
        "positions": [
            {"qid": "Q11696", "label": "President of Russia", "start": "2012-05-07", "end": None},
            {"qid": "Q899", "label": "Prime Minister of Russia", "start": "2008-05-08", "end": "2012-05-07"},
        ],
    }
    signals = assess_bundle("wikidata", raw)
    assert [s.code for s in signals] == [PEP]
    assert signals[0].confidence == "medium"
    assert "President of Russia" in signals[0].evidence["positions"]


def test_wikidata_no_pep_when_all_positions_have_ended() -> None:
    raw = {
        "qid": "Q1",
        "is_person": True,
        "positions": [
            {"label": "Foo", "start": "2000", "end": "2005"},
        ],
    }
    assert assess_bundle("wikidata", raw) == []


def test_wikidata_no_pep_for_non_person() -> None:
    raw = {
        "qid": "Q42",
        "is_person": False,
        "is_entity": True,
        "positions": [],
    }
    assert assess_bundle("wikidata", raw) == []


def test_opaque_ownership_does_not_fire_on_unknown_person() -> None:
    """unknownPerson means unknown-to-this-source, not deliberately withheld.

    The GLEIF/OO reporting-exception bridges use unknownPerson for the benign
    NATURAL_PERSONS / NO_KNOWN_PERSON reasons — firing on the type produced
    the Eli Lilly false positive ("unknown person in ownership chain").
    """
    raw = {"source_id": "companies_house", "hit_id": "00000000"}
    bods = [
        {"statementType": "entityStatement", "entityType": "registeredEntity"},
        {"statementType": "personStatement", "personType": "unknownPerson"},
    ]
    assert assess_bundle("companies_house", raw, bods) == []


def test_opaque_ownership_anonymous_person_super_secure() -> None:
    """A CH super-secure PSC (anonymousPerson) is genuine, asserted opacity."""
    raw = {"source_id": "companies_house", "hit_id": "00000000"}
    bods = [
        {"statementType": "entityStatement", "entityType": "registeredEntity"},
        {
            "statementId": "anon-1",
            "statementType": "personStatement",
            "personType": "anonymousPerson",
        },
    ]
    signals = assess_bundle("companies_house", raw, bods, hit_id="00000000")
    assert [s.code for s in signals] == [OPAQUE_OWNERSHIP]
    assert signals[0].confidence == "high"
    assert signals[0].hit_id == "00000000"
    assert "withheld" in signals[0].summary
    assert signals[0].evidence["matches"] == [{"statement_id": "anon-1"}]


def test_opaque_ownership_anonymous_entity_in_bods() -> None:
    raw = {"source_id": "openaleph", "entity_id": "aleph-anon"}
    bods = [
        {"statementType": "entityStatement", "entityType": "anonymousEntity"},
    ]
    signals = assess_bundle("openaleph", raw, bods)
    assert [s.code for s in signals] == [OPAQUE_OWNERSHIP]


def test_opaque_ownership_unidentified_psc_statement() -> None:
    """CH 'PSC exists but not identified' → unspecified interestedParty reason."""
    raw = {"source_id": "companies_house", "hit_id": "00000000"}
    bods = [
        {"statementType": "entityStatement", "entityType": "registeredEntity"},
        {
            "statementId": "rel-1",
            "recordType": "relationship",
            "recordDetails": {
                "subject": "e-1",
                "interestedParty": {
                    "reason": "subjectUnableToConfirmOrIdentifyBeneficialOwner",
                    "description": (
                        "The company knows or has reasonable cause to believe"
                        " that there is a registrable person in relation to the"
                        " company but it has not identified the registrable person"
                    ),
                },
                "interests": [],
            },
        },
    ]
    signals = assess_bundle("companies_house", raw, bods, hit_id="00000000")
    assert [s.code for s in signals] == [OPAQUE_OWNERSHIP]
    assert signals[0].evidence["matches"] == [{"statement_id": "rel-1"}]


def test_opaque_ownership_not_fired_for_no_beneficial_owners() -> None:
    """'Nobody meets the threshold' is a clean declaration, not opacity."""
    raw = {"source_id": "companies_house", "hit_id": "00000000"}
    bods = [
        {"statementType": "entityStatement", "entityType": "registeredEntity"},
        {
            "recordType": "relationship",
            "recordDetails": {
                "subject": "e-1",
                "interestedParty": {
                    "reason": "noBeneficialOwners",
                    "description": "No individual or entity with significant control",
                },
                "interests": [],
            },
        },
    ]
    assert assess_bundle("companies_house", raw, bods) == []


def _gleif_raw(reason: str, *, kinds: tuple[str, ...] = ("direct",)) -> dict:
    raw: dict = {
        "source_id": "gleif",
        "lei": "LEI00000000000000099",
        "record": {"attributes": {"lei": "LEI00000000000000099"}},
        "direct_parent": None,
        "ultimate_parent": None,
        "direct_parent_exception": None,
        "ultimate_parent_exception": None,
    }
    for kind in kinds:
        raw[f"{kind}_parent_exception"] = {
            "attributes": {
                "category": f"{kind.upper()}_ACCOUNTING_CONSOLIDATION_PARENT",
                "reason": reason,
            }
        }
    return raw


def test_gleif_natural_persons_exception_is_context_not_opaque() -> None:
    """Eli Lilly regression: a NATURAL_PERSONS exception (both categories) is a
    permitted GLEIF reporting exception, not opaque ownership — and no wording
    may claim an 'unknown person', since GLEIF Level 2 only ever names entities.
    """
    raw = _gleif_raw("NATURAL_PERSONS", kinds=("direct", "ultimate"))
    bods = [{"statementType": "entityStatement", "entityType": "registeredEntity"}]
    signals = assess_bundle("gleif", raw, bods, hit_id="LEI00000000000000099")
    codes = [s.code for s in signals]
    assert OPAQUE_OWNERSHIP not in codes
    assert codes == [GLEIF_REPORTING_EXCEPTION]
    sig = signals[0]
    assert sig.kind == "context"
    assert sig.confidence == "high"
    assert sig.hit_id == "LEI00000000000000099"
    assert "unknown person" not in sig.summary.lower()
    assert "natural person" in sig.summary.lower()
    assert "permitted" in sig.summary.lower()
    # Both categories are reported in the evidence.
    assert [e["relationship"] for e in sig.evidence["exceptions"]] == [
        "direct",
        "ultimate",
    ]
    # Bridge statement ids ride in matches[] so the graph can badge the node.
    assert all(m["statement_id"] for m in sig.evidence["matches"])


def test_gleif_benign_exception_reasons_are_context() -> None:
    for reason in ("NO_KNOWN_PERSON", "NON_CONSOLIDATING", "NO_LEI"):
        raw = _gleif_raw(reason)
        signals = assess_bundle(
            "gleif", raw, [{"statementType": "entityStatement"}], hit_id="X"
        )
        codes = [s.code for s in signals]
        assert codes == [GLEIF_REPORTING_EXCEPTION], reason
        assert signals[0].kind == "context"


def test_gleif_no_lei_exception_wording_says_parent_exists() -> None:
    raw = _gleif_raw("NO_LEI")
    signals = assess_bundle(
        "gleif", raw, [{"statementType": "entityStatement"}], hit_id="X"
    )
    assert "a parent exists" in signals[0].summary.lower()


def test_gleif_non_public_exception_fires_opaque_ownership() -> None:
    raw = _gleif_raw("NON_PUBLIC")
    signals = assess_bundle(
        "gleif", raw, [{"statementType": "entityStatement"}], hit_id="X"
    )
    codes = [s.code for s in signals]
    assert codes == [OPAQUE_OWNERSHIP]
    sig = signals[0]
    assert sig.kind == "risk"
    assert sig.confidence == "high"
    assert "NON_PUBLIC" in sig.summary
    assert sig.evidence["exceptions"][0]["reason"] == "NON_PUBLIC"


def test_gleif_deprecated_refusal_reasons_fire_opaque_ownership() -> None:
    for reason in (
        "BINDING_LEGAL_COMMITMENTS",
        "LEGAL_OBSTACLES",
        "DISCLOSURE_DETRIMENTAL",
        "DETRIMENT_NOT_EXCLUDED",
        "CONSENT_NOT_OBTAINED",
    ):
        raw = _gleif_raw(reason)
        signals = assess_bundle(
            "gleif", raw, [{"statementType": "entityStatement"}], hit_id="X"
        )
        assert [s.code for s in signals] == [OPAQUE_OWNERSHIP], reason


def test_gleif_mixed_exceptions_fire_both_signals() -> None:
    raw = _gleif_raw("NATURAL_PERSONS")
    raw["ultimate_parent_exception"] = {
        "attributes": {
            "category": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
            "reason": "NON_PUBLIC",
        }
    }
    signals = assess_bundle(
        "gleif", raw, [{"statementType": "entityStatement"}], hit_id="X"
    )
    codes = {s.code for s in signals}
    assert codes == {OPAQUE_OWNERSHIP, GLEIF_REPORTING_EXCEPTION}


def test_gleif_unrecognised_exception_reason_is_context() -> None:
    """A future ROC reason code must degrade to context, never to a risk chip."""
    raw = _gleif_raw("SOME_FUTURE_REASON")
    signals = assess_bundle(
        "gleif", raw, [{"statementType": "entityStatement"}], hit_id="X"
    )
    assert [s.code for s in signals] == [GLEIF_REPORTING_EXCEPTION]


def test_gleif_bridge_statements_do_not_double_fire_generic_scan() -> None:
    """The mapped GLEIF bundle contains the bridging statements — the gleif
    branch must classify from the raw exception records only, so an
    anonymousEntity bridge doesn't ALSO fire via the generic statement scan."""
    from opencheck.bods.mapper import map_gleif

    raw = _gleif_raw("NON_PUBLIC")
    bods = list(map_gleif(raw))
    assert any(
        (s.get("recordDetails") or {}).get("entityType", {}).get("type")
        == "anonymousEntity"
        for s in bods
    )
    signals = assess_bundle("gleif", raw, bods, hit_id="X")
    assert [s.code for s in signals] == [OPAQUE_OWNERSHIP]


def test_bods_gleif_placeholders_stay_silent() -> None:
    """The OO bulk dataset flattens away the exception reason — a placeholder
    there cannot be classified, so no signal fires rather than a wrong one."""
    raw = {"source_id": "bods_gleif", "hit_id": "XI-LEI-X"}
    bods = [
        {"statementType": "personStatement", "personType": "unknownPerson"},
        {"statementType": "entityStatement", "entityType": "anonymousEntity"},
    ]
    assert assess_bundle("bods_gleif", raw, bods) == []


def test_no_signals_for_stub_bundle() -> None:
    raw = {"source_id": "opensanctions", "hit_id": "NK-stub", "is_stub": True}
    assert assess_bundle("opensanctions", raw) == []


def test_dict_serialisation() -> None:
    raw = {
        "source_id": "opensanctions",
        "entity_id": "NK-x",
        "entity": {"id": "NK-x", "topics": ["sanction"]},
    }
    payload = assess_bundle("opensanctions", raw)[0].to_dict()
    assert payload["code"] == SANCTIONED
    assert payload["confidence"] == "high"
    assert payload["source_id"] == "opensanctions"
    assert payload["hit_id"] == "NK-x"
    assert payload["evidence"]["topics"] == ["sanction"]
    # statement_id is now included so the frontend can highlight the BODS graph node
    assert "statement_id" in payload["evidence"]
