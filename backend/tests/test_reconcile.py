"""Tests for the cross-source reconciler (Phase 3)."""

from __future__ import annotations

from opencheck.reconcile import reconcile
from opencheck.sources import SearchKind, SourceHit


def _hit(source_id: str, hit_id: str, *, kind=SearchKind.ENTITY, **identifiers) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=kind,
        name=f"{source_id} hit {hit_id}",
        summary="",
        identifiers=identifiers,
        is_stub=False,
    )


# ---------------------------------------------------------------------
# Strong bridges
# ---------------------------------------------------------------------


def test_reconcile_links_two_sources_sharing_lei() -> None:
    hits = [
        _hit("gleif", "213800LBDB8WB3QGVN21", lei="213800LBDB8WB3QGVN21"),
        _hit(
            "opensanctions",
            "NK-bp",
            lei="213800LBDB8WB3QGVN21",
            opensanctions_id="NK-bp",
        ),
    ]
    links = reconcile(hits)
    assert len(links) == 1
    assert links[0].key == "wikidata_qid" or links[0].key == "lei"
    # Specifically, LEI takes the bridge here:
    assert links[0].key == "lei"
    assert links[0].confidence == "strong"
    assert {h.source_id for h in links[0].hits} == {"gleif", "opensanctions"}


def test_reconcile_links_three_sources_sharing_qid() -> None:
    """Wikidata Q-ID is the primary bridge for persons."""
    hits = [
        _hit("wikidata", "Q7747", kind=SearchKind.PERSON, wikidata_qid="Q7747"),
        _hit(
            "opensanctions",
            "NK-putin",
            kind=SearchKind.PERSON,
            wikidata_qid="Q7747",
            opensanctions_id="NK-putin",
        ),
        _hit(
            "everypolitician",
            "Q7747-pep",
            kind=SearchKind.PERSON,
            wikidata_qid="Q7747",
            opensanctions_id="Q7747-pep",
        ),
    ]
    links = reconcile(hits)
    qid_links = [l for l in links if l.key == "wikidata_qid"]
    assert len(qid_links) == 1
    assert {h.source_id for h in qid_links[0].hits} == {
        "wikidata",
        "opensanctions",
        "everypolitician",
    }


def test_reconcile_skips_stub_qids() -> None:
    """Two adapters in stub mode shouldn't both 'agree' they describe Q0."""
    hits = [
        _hit("wikidata", "Q0", wikidata_qid="Q0"),
        _hit("everypolitician", "poli-stub", wikidata_qid="Q0"),
    ]
    assert reconcile(hits) == []


def test_reconcile_returns_no_links_when_no_match() -> None:
    hits = [
        _hit("gleif", "AAAA", lei="AAAA000000000000AAAA"),
        _hit("opensanctions", "NK-bp", opensanctions_id="NK-bp"),
    ]
    assert reconcile(hits) == []


def test_reconcile_links_gb_coh_across_companies_house_and_gleif() -> None:
    """A live UK company often appears in both CH and GLEIF — bridge by company number."""
    hits = [
        _hit("companies_house", "00102498", gb_coh="00102498"),
        _hit(
            "gleif",
            "213800LBDB8WB3QGVN21",
            lei="213800LBDB8WB3QGVN21",
            gb_coh="00102498",
        ),
    ]
    links = reconcile(hits)
    coh_links = [l for l in links if l.key == "gb_coh"]
    assert len(coh_links) == 1
    assert {h.source_id for h in coh_links[0].hits} == {"companies_house", "gleif"}


# ---------------------------------------------------------------------
# Weak bridges
# ---------------------------------------------------------------------


def test_reconcile_emits_possible_link_on_normalised_name_match() -> None:
    """Two persons with no shared id but same name should be 'possibly-same-as'."""
    hits = [
        _hit(
            "opensanctions",
            "NK-putin",
            kind=SearchKind.PERSON,
            opensanctions_id="NK-putin",
        ),
        _hit(
            "openaleph",
            "aleph-putin",
            kind=SearchKind.PERSON,
        ),
    ]
    # Match on normalised name — adjust display name to be identical.
    hits[0] = SourceHit(
        source_id="opensanctions",
        hit_id="NK-putin",
        kind=SearchKind.PERSON,
        name="Vladimir Putin",
        summary="",
        identifiers={"opensanctions_id": "NK-putin"},
        is_stub=False,
    )
    hits[1] = SourceHit(
        source_id="openaleph",
        hit_id="aleph-putin",
        kind=SearchKind.PERSON,
        name="Vladimir  PUTIN!",  # trailing punct, double space, casing
        summary="",
        identifiers={},
        is_stub=False,
    )
    links = reconcile(hits)
    assert len(links) == 1
    assert links[0].key == "name"
    assert links[0].confidence == "possible"


def test_reconcile_ignores_name_match_when_strong_link_already_exists() -> None:
    """If two hits already match on Q-ID, don't double-count via name."""
    hits = [
        SourceHit(
            source_id="wikidata",
            hit_id="Q7747",
            kind=SearchKind.PERSON,
            name="Vladimir Putin",
            summary="",
            identifiers={"wikidata_qid": "Q7747"},
            is_stub=False,
        ),
        SourceHit(
            source_id="opensanctions",
            hit_id="NK-putin",
            kind=SearchKind.PERSON,
            name="Vladimir Putin",
            summary="",
            identifiers={"wikidata_qid": "Q7747", "opensanctions_id": "NK-putin"},
            is_stub=False,
        ),
    ]
    links = reconcile(hits)
    assert len(links) == 1
    assert links[0].key == "wikidata_qid"


def test_reconcile_wikidata_qid_surfaced_with_single_source() -> None:
    """wikidata_qid is shown even when only Wikidata carries it.

    GLEIF does not publish an official Wikidata mapping, so the QID is no
    longer stamped onto the GLEIF hit.  The reconciler must therefore surface
    the QID with just one source rather than silently dropping it.
    """
    hits = [
        _hit("gleif", "213800ABC", lei="213800ABCDEF0000LEI0"),
        _hit("wikidata", "Q12345", wikidata_qid="Q12345", lei="213800ABCDEF0000LEI0"),
    ]
    links = reconcile(hits)
    qid_links = [l for l in links if l.key == "wikidata_qid"]
    assert len(qid_links) == 1
    assert qid_links[0].confidence == "strong"
    assert {h.source_id for h in qid_links[0].hits} == {"wikidata"}


def test_reconcile_gleif_not_in_wikidata_qid_confirmers() -> None:
    """GLEIF must not appear as a confirmer of wikidata_qid.

    The QID is derived from Wikidata's own SPARQL endpoint.  If it were
    echoed onto the GLEIF SourceHit, the reconciler would incorrectly show
    GLEIF as an independent corroborator.
    """
    hits = [
        # GLEIF hit with NO wikidata_qid — correct post-fix behaviour.
        _hit("gleif", "213800ABC", lei="213800ABCDEF0000LEI0"),
        _hit("wikidata", "Q12345", wikidata_qid="Q12345", lei="213800ABCDEF0000LEI0"),
    ]
    links = reconcile(hits)
    qid_links = [l for l in links if l.key == "wikidata_qid"]
    confirmer_ids = {h.source_id for h in qid_links[0].hits}
    assert "gleif" not in confirmer_ids
    assert "wikidata" in confirmer_ids


def test_reconcile_dict_serialisation() -> None:
    hits = [
        _hit("gleif", "L", lei="L0000000000000000000"),
        _hit("opensanctions", "X", lei="L0000000000000000000"),
    ]
    payload = reconcile(hits)[0].to_dict()
    assert payload["confidence"] == "strong"
    assert payload["key"] == "lei"
    assert {h["source_id"] for h in payload["hits"]} == {"gleif", "opensanctions"}
