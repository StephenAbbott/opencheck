"""The declaration-subject phase — ``declarationSubject`` names the network's subject.

BODS v0.4: every statement about a declared network carries the subject's
``recordId`` as its ``declarationSubject``. Until this phase every OpenCheck
entity and person statement carried its own, and a relationship its immediate
subject's — the Ørsted lookup of 22 Oct 2026 showed a Wikidata party with
``statementId == recordId == declarationSubject``. These tests hold the two
passes (``bods.declaration``) against the shapes that produced that.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from opencheck import signalstats
from opencheck import provenance
from opencheck.bods.declaration import (
    OUTCOMES,
    find_subject,
    stamp_bundle,
    stamp_merged,
)
from opencheck.bods.refs import resolver
from opencheck.bods.statements import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)

LEI = "W9NG6WMZIYEU8VEDOG48"  # Ørsted A/S


@pytest.fixture(autouse=True)
def _scoped():
    with provenance.mapping_provenance(provenance.Provenance(liveness="live", retrieved_at=datetime.now(timezone.utc))):
        yield


def _entity(source_id: str, local_id: str, name: str, identifiers=()) -> dict:
    return make_entity_statement(
        source_id=source_id, local_id=local_id, name=name,
        jurisdiction=("Denmark", "DK"), identifiers=list(identifiers),
    )


def _wikidata_bundle() -> list[dict]:
    """Ørsted as Wikidata describes it: the company, a board member, a
    majority owner, a subsidiary, and the three relationships."""
    subject = _entity("wikidata", "Q1234", "Ørsted A/S", [
        {"scheme": "XI-LEI", "id": LEI}, {"scheme": "WIKIDATA", "id": "Q1234"},
    ])
    person = make_person_statement(source_id="wikidata", local_id="Q77", full_name="A Board Member")
    owner = _entity("wikidata", "Q55", "Kingdom of Denmark")
    child = _entity("wikidata", "Q99", "Ørsted Wind Power A/S")
    rels = [
        make_relationship_statement(
            source_id="wikidata", local_id="r-board",
            subject_statement_id=subject["statementId"],
            interested_party_statement_id=person["statementId"],
            interests=[{"type": "boardMember"}],
        ),
        make_relationship_statement(
            source_id="wikidata", local_id="r-owner",
            subject_statement_id=subject["statementId"],
            interested_party_statement_id=owner["statementId"],
            interested_party_type="entity",
            interests=[{"type": "shareholding", "share": {"exact": 50.1}}],
        ),
        # The subsidiary's edge: its immediate subject is the child, but the
        # network's subject is still Ørsted.
        make_relationship_statement(
            source_id="wikidata", local_id="r-child",
            subject_statement_id=child["statementId"],
            interested_party_statement_id=subject["statementId"],
            interested_party_type="entity",
            interests=[{"type": "shareholding", "share": {"exact": 100}}],
        ),
    ]
    return [subject, person, owner, child, *rels]


# ---------------------------------------------------------------------------
# The factory default is what the ticket found
# ---------------------------------------------------------------------------


def test_the_factories_still_default_every_party_to_itself() -> None:
    """The shape the Ørsted lookup showed, kept as the fallback when a
    bundle's subject cannot be found — never an invented id."""
    bundle = _wikidata_bundle()
    for s in bundle:
        if s["recordType"] in ("entity", "person"):
            assert s["declarationSubject"] == s["recordId"]
    subject = bundle[0]
    assert bundle[-1]["declarationSubject"] != subject["recordId"], (
        "the child's edge names the child, the shape stamp_bundle corrects"
    )


# ---------------------------------------------------------------------------
# Finding the subject of one bundle
# ---------------------------------------------------------------------------


def test_find_subject_by_the_lookup_reference() -> None:
    bundle = _wikidata_bundle()
    stmt, how = find_subject(bundle, ref=LEI)
    assert stmt is bundle[0] and how == "ref"


def test_find_subject_by_a_register_reference() -> None:
    subject = _entity("companies_house", "00102498", "ASDA STORES LIMITED",
                      [{"scheme": "GB-COH", "id": "00102498"}])
    owner = _entity("companies_house", "x", "ASDA GROUP LIMITED",
                    [{"scheme": "GB-COH", "id": "01396513"}])
    stmt, how = find_subject([owner, subject], ref="GB-COH:00102498")
    assert stmt is subject and how == "ref"


def test_find_subject_by_the_hit_id_when_there_is_no_reference() -> None:
    """The Data drawer's /deepen knows the adapter's hit id and nothing else."""
    bundle = _wikidata_bundle()
    stmt, how = find_subject(bundle, hit_id="Q1234")
    assert stmt is bundle[0] and how == "hit_id"
    # An OpenCorporates-style ``jurisdiction/number`` hit id names the
    # statement carrying the number.
    subject = _entity("opencorporates", "gb/00102498", "ASDA STORES LIMITED",
                      [{"scheme": "GB-COH", "id": "00102498"}])
    other = _entity("opencorporates", "gb/1", "OTHER", [{"scheme": "GB-COH", "id": "1"}])
    assert find_subject([other, subject], hit_id="gb/00102498")[0] is subject


def test_find_subject_falls_back_to_the_only_entity() -> None:
    only = _entity("prh", "123", "OY AB")
    person = make_person_statement(source_id="prh", local_id="p", full_name="Someone")
    assert find_subject([person, only], hit_id="not-an-identifier") == (only, "single")


def test_find_subject_invents_nothing() -> None:
    a = _entity("prh", "1", "A OY")
    b = _entity("prh", "2", "B OY")
    assert find_subject([a, b], hit_id="3") == (None, "unresolved")
    assert find_subject([], ref=LEI) == (None, "unresolved")
    assert {"ref", "hit_id", "single", "unresolved"} == OUTCOMES


# ---------------------------------------------------------------------------
# Stamping one bundle
# ---------------------------------------------------------------------------


def test_stamp_bundle_names_the_subject_on_every_statement() -> None:
    bundle = _wikidata_bundle()
    before = [dict(s) for s in bundle]
    out = stamp_bundle(bundle, source_id="wikidata", ref=LEI)
    subject_rid = bundle[0]["recordId"]
    assert [s["declarationSubject"] for s in out] == [subject_rid] * len(out)
    # The subject's own statement is unchanged (it already named itself),
    # as are the two relationships whose immediate subject it is; the
    # others are copies — the originals are never mutated.
    assert out[0] is bundle[0]
    assert bundle == before
    changed = [o for o, b in zip(out, bundle) if o is not b]
    assert len(changed) == 4  # the person, the owner, the child, the child's edge
    # Every other field is as it was.
    for o, b in zip(out, bundle):
        assert {k: v for k, v in o.items() if k != "declarationSubject"} == {
            k: v for k, v in b.items() if k != "declarationSubject"
        }


def test_stamp_bundle_is_idempotent() -> None:
    once = stamp_bundle(_wikidata_bundle(), source_id="wikidata", ref=LEI)
    twice = stamp_bundle(once, source_id="wikidata", ref=LEI)
    assert twice == once and all(a is b for a, b in zip(twice, once))


def test_stamp_bundle_leaves_an_unresolved_bundle_as_it_is() -> None:
    a = _entity("prh", "1", "A OY")
    b = _entity("prh", "2", "B OY")
    assert stamp_bundle([a, b], source_id="prh", hit_id="3") == [a, b]


def test_another_publishers_statements_are_never_stamped() -> None:
    """The OECD's MEIP statements and Open Ownership's stored bundles carry
    their publisher's declarationSubject, which is theirs to set."""
    meip = {
        "statementId": "3f6e3c2a-7b1e-5c9a-9d0b-2e4f8a1c6d70",
        "recordId": "meip-entity-100959",
        "recordType": "entity",
        "declarationSubject": "meip-entity-100913",
        "recordDetails": {"name": "A/S Norske Shell", "identifiers": [{"scheme": "XI-LEI", "id": LEI}]},
        "publicationDetails": {"publisher": {"name": "OECD-UNSD"}},
        "source": {"description": "OECD-UNSD MEIP", "opencheckSourceId": "meip"},
    }
    assert stamp_bundle([meip], source_id="meip", ref=LEI) == [meip]
    oo = {
        "statementId": "e-oo", "recordId": "e-oo", "recordType": "entity",
        "declarationSubject": "e-oo",
        "recordDetails": {"name": "X", "identifiers": [{"scheme": "XI-LEI", "id": LEI}]},
        "publicationDetails": {"publisher": {"name": "Open Ownership"}},
        "source": {"description": "GLEIF (via Open Ownership)"},
    }
    party = {**oo, "statementId": "e-oo-2", "recordId": "e-oo-2", "declarationSubject": "e-oo-2",
             "recordDetails": {"name": "Y"}}
    out = stamp_bundle([oo, party], source_id="bods_gleif", ref=LEI)
    assert out[1]["declarationSubject"] == "e-oo-2"


def test_a_chain_names_the_subject_at_every_layer() -> None:
    """In a chain deeper than one layer the upper relationship's immediate
    subject is the company in the middle; the network's subject is still
    the company looked up."""
    subject = _entity("companies_house", "1", "SUB LTD", [{"scheme": "GB-COH", "id": "00000001"}])
    middle = _entity("companies_house", "2", "MID LTD", [{"scheme": "GB-COH", "id": "00000002"}])
    top = _entity("companies_house", "3", "TOP LTD", [{"scheme": "GB-COH", "id": "00000003"}])
    lower = make_relationship_statement(
        source_id="companies_house", local_id="r1",
        subject_statement_id=subject["statementId"],
        interested_party_statement_id=middle["statementId"], interested_party_type="entity",
        interests=[{"type": "shareholding"}],
    )
    upper = make_relationship_statement(
        source_id="companies_house", local_id="r2",
        subject_statement_id=middle["statementId"],
        interested_party_statement_id=top["statementId"], interested_party_type="entity",
        interests=[{"type": "shareholding"}],
    )
    assert upper["declarationSubject"] == middle["recordId"]
    out = stamp_bundle([subject, middle, top, lower, upper],
                       source_id="companies_house", hit_id="00000001")
    assert {s["declarationSubject"] for s in out} == {subject["recordId"]}
    # The edges themselves are untouched: the resolver still walks the chain.
    resolve = resolver(out)
    assert resolve(out[4]["recordDetails"]["subject"]) == middle["statementId"]


# ---------------------------------------------------------------------------
# The merged pass — per source, by decision
# ---------------------------------------------------------------------------


def test_stamp_merged_stamps_each_source_to_its_own_subject_statement() -> None:
    """A register bundle carries no LEI, so alone its subject is found only
    by the hit id; in the merged bundle GLEIF ties the LEI to the company
    number and the referent group names both subject statements."""
    gleif_subject = _entity("gleif", LEI, "ASDA STORES LIMITED", [
        {"scheme": "XI-LEI", "id": LEI}, {"scheme": "GB-COH", "id": "00102498"},
    ])
    gleif_parent = _entity("gleif", "P", "ASDA GROUP LIMITED", [{"scheme": "XI-LEI", "id": "5493001KJTIIGC8Y1R12"}])
    ch_subject = _entity("companies_house", "00102498", "ASDA STORES LIMITED",
                         [{"scheme": "GB-COH", "id": "00102498"}])
    ch_psc = make_person_statement(source_id="companies_house", local_id="psc-1", full_name="A Person")
    ch_rel = make_relationship_statement(
        source_id="companies_house", local_id="r",
        subject_statement_id=ch_subject["statementId"],
        interested_party_statement_id=ch_psc["statementId"],
        interests=[{"type": "shareholding"}],
    )
    merged = [gleif_subject, gleif_parent, ch_subject, ch_psc, ch_rel]
    out = stamp_merged(merged, LEI)
    by_sid = {s["statementId"]: s for s in out}
    assert by_sid[gleif_parent["statementId"]]["declarationSubject"] == gleif_subject["recordId"]
    assert by_sid[ch_psc["statementId"]]["declarationSubject"] == ch_subject["recordId"]
    assert by_sid[ch_rel["statementId"]]["declarationSubject"] == ch_subject["recordId"]
    # Per source: never the anchor's record on another source's statement.
    assert by_sid[ch_psc["statementId"]]["declarationSubject"] != gleif_subject["recordId"]
    # The originals are untouched.
    assert ch_psc["declarationSubject"] == ch_psc["recordId"]


def test_stamp_merged_leaves_a_source_with_no_subject_statement_alone() -> None:
    gleif_subject = _entity("gleif", LEI, "X", [{"scheme": "XI-LEI", "id": LEI}])
    stray = _entity("wikidata", "Q1", "Y")  # no identifier joins it to the subject
    out = stamp_merged([gleif_subject, stray], LEI)
    assert out[1] is stray


# ---------------------------------------------------------------------------
# /signalstats — the miss is loud, and only closed vocabularies are counted
# ---------------------------------------------------------------------------


def test_outcomes_are_counted_by_source_and_closed_vocabulary() -> None:
    signalstats.reset()
    stamp_bundle(_wikidata_bundle(), source_id="wikidata", ref=LEI)
    a = _entity("prh", "1", "A OY")
    b = _entity("prh", "2", "B OY")
    stamp_bundle([a, b], source_id="prh", hit_id="Ørsted A/S")
    signalstats.record_declaration_subject("prh", "a LEI or a name")
    stats = signalstats.stats()["declaration_subject"]
    assert stats == {"wikidata|ref": 1, "prh|unresolved": 1}
    signalstats.reset()


def test_another_publishers_bundle_counts_nothing() -> None:
    signalstats.reset()
    oo = {
        "statementId": "e-oo", "recordId": "e-oo", "recordType": "entity",
        "declarationSubject": "e-oo", "recordDetails": {"name": "X"},
        "publicationDetails": {"publisher": {"name": "Open Ownership"}},
    }
    stamp_bundle([oo], source_id="companies_house", hit_id="1")
    assert signalstats.stats()["declaration_subject"] == {}
    signalstats.reset()


# ---------------------------------------------------------------------------
# Through the deepen path — the one place a source bundle is mapped
# ---------------------------------------------------------------------------


async def test_safe_deepen_stamps_a_register_bundle_by_the_hit_id(monkeypatch, tmp_path) -> None:
    """A register bundle carries no LEI: alone, its subject is the statement
    carrying the company number the lookup dispatched on."""
    from opencheck.config import get_settings
    from opencheck.routers import lookup as L
    from opencheck.sources import REGISTRY

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    number = "00102498"
    bundle = [
        _entity("companies_house", number, "ASDA STORES LIMITED",
                [{"scheme": "GB-COH", "id": number}]),
        make_person_statement(source_id="companies_house", local_id="psc-1", full_name="A Person"),
    ]
    bundle.append(make_relationship_statement(
        source_id="companies_house", local_id="r",
        subject_statement_id=bundle[0]["statementId"],
        interested_party_statement_id=bundle[1]["statementId"],
        interests=[{"type": "shareholding"}],
    ))

    async def _fetch(hit_id, **kwargs):
        return {"source_id": "companies_house", "company_number": hit_id}

    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", _fetch)
    monkeypatch.setattr(L, "_mapper_for", lambda sid: (lambda raw: iter(bundle)))
    signalstats.reset()
    try:
        deep = await L._safe_deepen("companies_house", number)
        assert deep is not None
        rid = bundle[0]["recordId"]
        assert [s["declarationSubject"] for s in deep["bods"]] == [rid, rid, rid]
        assert signalstats.stats()["declaration_subject"] == {"companies_house|hit_id": 1}
        # With the lookup's own reference the register number still names it.
        signalstats.reset()
        deep = await L._safe_deepen("companies_house", number, subject=f"GB-COH:{number}")
        assert deep is not None
        assert {s["declarationSubject"] for s in deep["bods"]} == {rid}
        assert signalstats.stats()["declaration_subject"] == {"companies_house|ref": 1}
    finally:
        signalstats.reset()
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The weekly sweep says when production drifts
# ---------------------------------------------------------------------------


def test_the_sweep_passes_a_stamped_bundle_and_fails_the_factory_default() -> None:
    from opencheck.bods_quality import check_declaration_subjects

    bundle = _wikidata_bundle()
    assert check_declaration_subjects(stamp_bundle(bundle, source_id="wikidata", ref=LEI)) == []
    found = check_declaration_subjects(bundle)
    assert found and {f.check for f in found} == {"declaration_subject_not_subject"}
    assert all(f.severity == "fail" and f.source_id == "wikidata" for f in found)
    assert any("different declarationSubject values" in f.message for f in found)
    # A value naming nothing in the bundle, and a subject that does not name itself.
    stray = [dict(s, declarationSubject="opencheck-000000000000000000000000") for s in bundle]
    assert any("no entity's or person's recordId" in f.message for f in check_declaration_subjects(stray))
    # Another publisher's statements are theirs to declare.
    oo = {
        "statementId": "e-oo", "recordId": "e-oo", "recordType": "entity",
        "declarationSubject": "elsewhere", "recordDetails": {"name": "X"},
        "publicationDetails": {"publisher": {"name": "Open Ownership"}},
    }
    assert check_declaration_subjects([oo]) == []
