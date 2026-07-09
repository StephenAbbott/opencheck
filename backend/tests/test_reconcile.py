"""Tests for the cross-source reconciler (Phase 3)."""

from __future__ import annotations

from opencheck.reconcile import possibly_same_entities, reconcile
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


# ---------------------------------------------------------------------
# POSSIBLY_SAME_AS — name-only entity candidates (Splink-spike outcome)
# ---------------------------------------------------------------------


def _ent(sid: str, name: str, jur: str, date: str | None = None, *, jur_key: str = "jurisdiction") -> dict:
    rd: dict = {"name": name, jur_key: {"code": jur}, "identifiers": []}
    if date:
        rd["foundingDate"] = date
    return {"statementId": sid, "recordType": "entity", "recordDetails": rd}


def test_possibly_same_flags_name_plus_jurisdiction() -> None:
    pairs = possibly_same_entities([
        _ent("a", "Acme Ltd", "GB", "1990-01-01"),
        _ent("b", "ACME LTD.", "GB", "1990-06-02"),
    ])
    assert len(pairs) == 1
    assert {pairs[0].a, pairs[0].b} == {"a", "b"}
    assert pairs[0].reason == "same name + jurisdiction"


def test_possibly_same_respects_jurisdiction() -> None:
    pairs = possibly_same_entities([
        _ent("a", "Acme Ltd", "GB"),
        _ent("b", "Acme Ltd", "US"),
    ])
    assert pairs == []


def test_possibly_same_founding_date_tiebreaker() -> None:
    pairs = possibly_same_entities([
        _ent("a", "Acme Ltd", "GB", "1990-01-01"),
        _ent("b", "Acme Ltd", "GB", "2005-01-01"),
    ])
    assert pairs == []  # different incorporation year -> different entity


def test_possibly_same_missing_date_is_compatible() -> None:
    pairs = possibly_same_entities([
        _ent("a", "Acme Ltd", "GB", "1990"),
        _ent("b", "Acme Ltd", "GB"),
    ])
    assert len(pairs) == 1


def test_possibly_same_does_not_flag_alpha_vs_beta() -> None:
    pairs = possibly_same_entities([
        _ent("a", "BP Exploration (Alpha) Limited", "GB", "1990"),
        _ent("b", "BP Exploration (Beta) Limited", "GB", "1990"),
    ])
    assert pairs == []


def test_possibly_same_skips_identifier_linked_pairs() -> None:
    # Share an LEI -> known same (identifier-linked), not a name-only candidate.
    lei = "529900T8BM49AURSDO55"
    a = {"statementId": "a", "recordType": "entity", "recordDetails": {
        "name": "Acme Ltd", "jurisdiction": {"code": "GB"},
        "identifiers": [{"scheme": "XI-LEI", "id": lei}]}}
    b = {"statementId": "b", "recordType": "entity", "recordDetails": {
        "name": "ACME LTD.", "jurisdiction": {"code": "GB"},
        "identifiers": [{"scheme": "XI-LEI", "id": lei}]}}
    assert possibly_same_entities([a, b]) == []


def test_possibly_same_reads_incorporated_in_jurisdiction() -> None:
    # GLEIF uses incorporatedInJurisdiction; OpenSanctions uses jurisdiction.
    pairs = possibly_same_entities([
        _ent("a", "Globex Holdings", "FR", "2001", jur_key="incorporatedInJurisdiction"),
        _ent("b", "GLOBEX HOLDINGS", "FR", "2001", jur_key="jurisdiction"),
    ])
    assert len(pairs) == 1


def test_possibly_same_carries_names_for_display() -> None:
    # The QuickCheck report renders pairs without the BODS bundle, so each pair
    # must carry display names + jurisdiction.
    pairs = possibly_same_entities([
        _ent("a", "Acme Ltd", "GB", "1990-01-01"),
        _ent("b", "ACME LTD.", "GB", "1990-06-02"),
    ])
    d = pairs[0].to_dict()
    assert {d["a_name"], d["b_name"]} == {"Acme Ltd", "ACME LTD."}
    assert d["jurisdiction"] == "GB"
    assert set(d) == {"a", "b", "reason", "a_name", "b_name", "jurisdiction", "a_source", "b_source"}


def test_possibly_same_carries_per_record_sources() -> None:
    """Each record in a pair carries the source that asserted it — the key
    context for the human reviewing a name-only match (issue #25 follow-up)."""
    a = _ent("a", "Acme Ltd", "GB")
    a["source"] = {"description": "GLEIF"}
    b = _ent("b", "ACME LTD.", "GB")
    b["source"] = {"description": "OpenCorporates"}
    pairs = possibly_same_entities([a, b])
    assert len(pairs) == 1
    d = pairs[0].to_dict()
    # Pair ordering is by sorted statementId ("a" < "b").
    assert d["a_source"] == "GLEIF"
    assert d["b_source"] == "OpenCorporates"


def test_possibly_same_sources_default_empty_when_absent() -> None:
    pairs = possibly_same_entities([
        _ent("a", "Acme Ltd", "GB"),
        _ent("b", "ACME LTD.", "GB"),
    ])
    assert pairs[0].to_dict()["a_source"] == ""
    assert pairs[0].to_dict()["b_source"] == ""
