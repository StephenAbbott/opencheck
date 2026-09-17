"""Phase 220 — risk signals and ended relationships.

Phase 219 made an ended relationship *look* ended on every diagram; the risk
engine still read every relationship as current. Stephen's decisions
(17 Sept 2026):

* **Structural signals keep ended relationships** — layers, the AMLA
  composite, state control, the non-EU upstream walk, textual nominee and
  opaque ownership — and say "including ended relationships" in the summary
  and the evidence whenever an ended link is part of what they counted.
* **Related-party screens keep former parties** and call them "former".

"Ended" is ``bods.lifecycle``'s rule: a closed record, or every interest with
an ``endDate`` on or before today. Both shapes are tested.
"""

from __future__ import annotations

import pytest

from opencheck.config import get_settings
from opencheck.cross_check import (
    NameMatch,
    _collect_targets,
    _make_signal,
    match_summary,
    related_party_label,
)
from opencheck.findings import finding_everypolitician
from opencheck.icij_check import _collect_targets as icij_collect_targets
from opencheck.icij_check import _signal_from_match
from opencheck.openaleph_check import _screening_entry, _signals_from_percolate
from opencheck.risk import (
    COMPLEX_CORPORATE_STRUCTURE,
    COMPLEX_OWNERSHIP_LAYERS,
    INCLUDING_ENDED,
    NOMINEE,
    NON_EU_JURISDICTION,
    OPAQUE_OWNERSHIP,
    STATE_CONTROLLED,
    _ended_relationship_ids,
    _state_controlled_signals,
    _subject_entity_id,
    assess_amla,
    assess_bundle,
    former_party_ids,
)
from opencheck.sources import SearchKind, SourceHit

PAST = "2024-10-04"
FUTURE = "2099-01-01"


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------
# Bundle builders — the two "ended" shapes BODS allows
# ---------------------------------------------------------------------


def _entity(sid, *, name="Acme", entity_type="registeredEntity", jurisdiction=None):
    rd = {"entityType": {"type": entity_type}, "name": name}
    if jurisdiction:
        rd["jurisdiction"] = {"code": jurisdiction, "name": jurisdiction}
    return {"statementId": sid, "recordType": "entity", "recordDetails": rd}


def _person(sid, *, name="Seyed Ziya Imany", person_type="knownPerson"):
    return {
        "statementId": sid,
        "recordType": "person",
        "recordDetails": {
            "personType": person_type,
            "names": [{"type": "individual", "fullName": name}],
        },
    }


def _rel(sid, subject, ip, *, closed=False, end_date=None, interests=None, ip_value=None):
    if interests is None:
        interest = {"type": "shareholding", "directOrIndirect": "direct"}
        if end_date:
            interest["endDate"] = end_date
        interests = [interest]
    stmt = {
        "statementId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": ip if ip_value is None else ip_value,
            "interests": interests,
        },
    }
    if closed:
        stmt["recordStatus"] = "closed"
    return stmt


# ---------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------


def test_both_ended_shapes_are_read_and_a_future_end_date_is_not() -> None:
    bods = [
        _rel("closed", "S", "A", closed=True),
        _rel("dated", "S", "B", end_date=PAST),
        _rel("scheduled", "S", "C", end_date=FUTURE),
        _rel("current", "S", "D"),
        _rel(
            "half",
            "S",
            "E",
            interests=[{"type": "shareholding", "endDate": PAST}, {"type": "votingRights"}],
        ),
    ]
    assert _ended_relationship_ids(bods) == {"closed", "dated"}


def test_a_ceased_psc_is_former_and_the_company_is_not() -> None:
    """BANK SADERAT PLC's shape: one closed PSC record. The person is a former
    party; the company, which is never an interested party, is not — even
    though every relationship it has has ended."""
    bods = [
        _entity("S", name="BANK SADERAT PLC"),
        _person("P"),
        _rel("R1", "S", "P", closed=True),
    ]
    assert former_party_ids(bods) == {"P"}


def test_a_party_with_any_current_link_is_not_former() -> None:
    bods = [
        _entity("S"),
        _person("P"),
        _rel("psc", "S", "P", end_date=PAST),       # ceased as PSC …
        _rel("director", "S", "P"),                  # … still a director
    ]
    assert former_party_ids(bods) == set()


def test_a_former_owner_with_current_owners_of_its_own_stays_current() -> None:
    """Conservative by design: the rule never invents a former party."""
    bods = [
        _entity("S"),
        _entity("P", name="Old Parent"),
        _entity("G", name="Grandparent"),
        _rel("R1", "S", "P", closed=True),
        _rel("R2", "P", "G"),
    ]
    assert former_party_ids(bods) == set()


# ---------------------------------------------------------------------
# Structural signals — kept, and said
# ---------------------------------------------------------------------


def _chain(*, ended_link: str | None = None):
    """S <- H1 <- H2 (three layers). ``ended_link`` closes R1 or R2."""
    return [
        _entity("S", name="Subject Ltd"),
        _entity("H1", name="Holding 1", jurisdiction="JE"),
        _entity("H2", name="Holding 2", jurisdiction="KY"),
        _rel("R1", "S", "H1", closed=ended_link == "R1"),
        _rel("R2", "H1", "H2", end_date=PAST if ended_link == "R2" else None),
    ]


def _layers(bods):
    return next(
        s for s in assess_amla("companies_house", {"entity_id": "S"}, bods)
        if s.code == COMPLEX_OWNERSHIP_LAYERS
    )


def test_layers_on_a_current_chain_are_unchanged() -> None:
    sig = _layers(_chain())
    assert sig.summary == (
        "Ownership chain above the subject has 3 corporate layers (AMLA threshold: ≥3)."
    )
    assert "includes_ended_relationships" not in sig.evidence
    assert "ended_relationship_statement_ids" not in sig.evidence


@pytest.mark.parametrize("link", ["R1", "R2"])
def test_layers_through_an_ended_link_still_count_and_say_so(link) -> None:
    sig = _layers(_chain(ended_link=link))
    assert sig.evidence["layers"] == 3
    assert f"3 corporate layers, {INCLUDING_ENDED} (AMLA" in sig.summary
    assert sig.evidence["includes_ended_relationships"] is True
    assert sig.evidence["ended_relationship_statement_ids"] == [link]


def test_layers_prefer_an_equally_long_current_path() -> None:
    """Two routes to the same depth; only one runs through an ended link. The
    signal reports the current one and is not qualified."""
    bods = [
        _entity("S"),
        _entity("A"),
        _entity("B"),
        _entity("C"),
        _rel("R1", "S", "A", closed=True),
        _rel("R2", "A", "C"),
        _rel("R3", "S", "B"),
        _rel("R4", "B", "C"),
    ]
    sig = _layers(bods)
    assert sig.evidence["longest_path"] == ["S", "B", "C"]
    assert "includes_ended_relationships" not in sig.evidence
    assert INCLUDING_ENDED not in sig.summary


def test_a_current_record_beside_an_ended_one_makes_the_link_current() -> None:
    bods = _chain() + [_rel("R1-old", "S", "H1", closed=True)]
    assert "includes_ended_relationships" not in _layers(bods).evidence


def test_composite_carries_the_ended_qualifier() -> None:
    bods = _chain(ended_link="R2") + [
        _person("N", name="Nominee Director"),
        _rel(
            "RN",
            "S",
            "N",
            interests=[{"type": "appointmentOfBoard", "details": "nominee director"}],
        ),
    ]
    sigs = assess_amla("companies_house", {"entity_id": "S"}, bods)
    composite = next(s for s in sigs if s.code == COMPLEX_CORPORATE_STRUCTURE)
    assert f"3 layers of ownership ({INCLUDING_ENDED}) combined with" in composite.summary
    assert composite.evidence["ended_relationship_statement_ids"] == ["R2"]


def test_state_control_through_an_ended_holding_still_fires_and_says_so() -> None:
    bods = [
        _entity("S", name="Subject Co"),
        _entity("M", entity_type="stateBody", name="Ministry of Energy"),
        _rel("R1", "S", "M", end_date=PAST),
    ]
    [sig] = _state_controlled_signals("wikidata", "Q1", bods)
    assert sig.code == STATE_CONTROLLED
    assert sig.summary.startswith(
        f"A controlling owner is a state or state body ({INCLUDING_ENDED}) — "
    )
    assert sig.evidence["ended_relationship_statement_ids"] == ["R1"]
    assert sig.evidence["statement_id"] == "M"


def test_state_control_with_a_current_holding_is_unqualified_and_anchors_on_it() -> None:
    bods = [
        _entity("S", name="Subject Co"),
        _entity("OLD", name="Old Subsidiary"),
        _entity("M", entity_type="state", name="Republic"),
        _rel("R0", "OLD", "M", closed=True),   # ended, listed first
        _rel("R1", "S", "M"),                  # current
    ]
    [sig] = _state_controlled_signals("wikidata", "Q1", bods)
    assert INCLUDING_ENDED not in sig.summary
    assert "includes_ended_relationships" not in sig.evidence
    assert sig.evidence["subject_statement_id"] == "S"


def test_non_eu_reached_only_through_an_ended_parent_says_so() -> None:
    bods = [
        _entity("S", name="Subject GmbH", jurisdiction="DE"),
        _entity("P", name="Former Parent", jurisdiction="KY"),
        _rel("R1", "S", "P", closed=True),
    ]
    sig = next(
        s for s in assess_amla("gleif", {"entity_id": "S"}, bods)
        if s.code == NON_EU_JURISDICTION
    )
    assert sig.summary.startswith(
        f"Ownership chain ({INCLUDING_ENDED}) reaches jurisdictions outside the EU/EEA: KY."
    )
    assert sig.evidence["ended_relationship_statement_ids"] == ["R1"]


def test_non_eu_through_a_current_parent_is_unchanged() -> None:
    bods = [
        _entity("S", name="Subject GmbH", jurisdiction="DE"),
        _entity("P", name="Parent", jurisdiction="KY"),
        _entity("Q", name="Former Parent", jurisdiction="FR"),
        _rel("R1", "S", "P"),
        _rel("R2", "S", "Q", closed=True),   # ended, but EU — not the claim
    ]
    sig = next(
        s for s in assess_amla("gleif", {"entity_id": "S"}, bods)
        if s.code == NON_EU_JURISDICTION
    )
    assert sig.summary.startswith("Ownership chain reaches jurisdictions outside")
    assert "includes_ended_relationships" not in sig.evidence


def test_subject_inference_ignores_an_ended_link_to_a_former_subsidiary() -> None:
    """A former subsidiary is a second sink in the full graph; the current
    graph alone has one, and that is the subject."""
    bods = [
        _entity("SUB", name="Former Subsidiary"),
        _entity("S", name="Subject"),
        _entity("P", name="Parent"),
        _rel("R0", "SUB", "S", closed=True),
        _rel("R1", "S", "P"),
    ]
    # R0 and R1 alone give one sink, SUB, in the full graph; a second ended
    # link makes the full graph ambiguous, so the current graph decides.
    bods.append(_entity("X", name="Other"))
    bods.append(_rel("R2", "X", "P", closed=True))
    assert _subject_entity_id("", bods) == "S"


def test_textual_nominee_on_an_ended_relationship_says_so() -> None:
    bods = [
        _entity("S"),
        _person("N", name="Jane Smith"),
        _rel(
            "RN",
            "S",
            "N",
            closed=True,
            interests=[{"type": "appointmentOfBoard", "details": "nominee director"}],
        ),
    ]
    sig = next(
        s for s in assess_amla("opencorporates", {"entity_id": "S"}, bods)
        if s.code == NOMINEE
    )
    assert f"(1 statement(s), {INCLUDING_ENDED})" in sig.summary
    assert sig.evidence["ended_relationship_statement_ids"] == ["RN"]


def test_opaque_disclosure_on_an_ended_relationship_says_so() -> None:
    bods = [
        _entity("S"),
        _rel(
            "RU",
            "S",
            None,
            end_date=None,
            closed=True,
            ip_value={"reason": "subjectUnableToConfirmOrIdentifyBeneficialOwner"},
        ),
    ]
    [sig] = [
        s for s in assess_bundle("companies_house", {"entity_id": "S"}, bods, hit_id="S")
        if s.code == OPAQUE_OWNERSHIP
    ]
    assert sig.summary.startswith(
        "The register discloses that ownership information is withheld or "
        f"could not be obtained ({INCLUDING_ENDED}): "
    )
    assert sig.evidence["ended_relationship_statement_ids"] == ["RU"]


def test_a_ceased_super_secure_psc_still_counts_as_opaque_and_says_so() -> None:
    bods = [
        _entity("S"),
        _person("A", person_type="anonymousPerson"),
        _rel("RA", "S", "A", end_date=PAST),
    ]
    [sig] = [
        s for s in assess_bundle("companies_house", {"entity_id": "S"}, bods, hit_id="S")
        if s.code == OPAQUE_OWNERSHIP
    ]
    assert INCLUDING_ENDED in sig.summary
    assert sig.evidence["ended_relationship_statement_ids"] == ["RA"]


def test_a_current_super_secure_psc_is_unchanged() -> None:
    bods = [
        _entity("S"),
        _person("A", person_type="anonymousPerson"),
        _rel("RA", "S", "A"),
    ]
    [sig] = [
        s for s in assess_bundle("companies_house", {"entity_id": "S"}, bods, hit_id="S")
        if s.code == OPAQUE_OWNERSHIP
    ]
    assert INCLUDING_ENDED not in sig.summary
    assert "includes_ended_relationships" not in sig.evidence


# ---------------------------------------------------------------------
# Related-party screens — kept, and called former
# ---------------------------------------------------------------------


def _saderat_bundle():
    return [
        _entity("S", name="BANK SADERAT PLC"),
        _person("P", name="Seyed Ziya Imany"),
        _person("D", name="Current Director"),
        _rel("R1", "S", "P", closed=True),
        _rel("R2", "S", "D"),
    ]


def test_screen_targets_keep_former_parties_and_mark_them() -> None:
    for collect in (_collect_targets, icij_collect_targets):
        by_id = {t["statement_id"]: t for t in collect(_saderat_bundle())}
        assert set(by_id) == {"S", "P", "D"}  # nobody dropped
        assert by_id["P"]["former"] is True
        assert by_id["D"]["former"] is False
        assert by_id["S"]["former"] is False


def test_related_party_label_wording() -> None:
    assert related_party_label({"kind": "person"}) == "Related party"
    assert related_party_label({"kind": "entity"}) == "Related entity"
    assert related_party_label({"kind": "person", "former": True}) == "Former related party"
    assert related_party_label({"kind": "entity", "former": True}) == "Former related entity"


def test_match_summary_says_former_in_both_sentence_shapes() -> None:
    target = {"kind": "person", "name": "Seyed Ziya Imany", "former": True}
    assert match_summary(
        target=target, source_id="opensanctions", summary_extra="sanctioned", corroboration=()
    ).startswith("Possible name match only: former related party 'Seyed Ziya Imany' shares")
    assert match_summary(
        target=target,
        source_id="opensanctions",
        summary_extra="sanctioned",
        corroboration=("birth_year",),
    ).startswith("Former related party 'Seyed Ziya Imany' matches a record on OpenSanctions")


def test_opensanctions_signal_carries_former_in_evidence() -> None:
    hit = SourceHit(
        source_id="opensanctions",
        hit_id="os-1",
        kind=SearchKind.PERSON,
        name="Seyed Ziya Imany",
        summary="",
        identifiers={},
        raw={"properties": {"topics": ["sanction"]}},
        is_stub=False,
    )
    former = {"kind": "person", "statement_id": "P", "name": "Seyed Ziya Imany", "former": True}
    current = {**former, "former": False}
    sig = _make_signal(code="RELATED_SANCTIONED", target=former, hit=hit, score=1.0,
                       summary_extra="sanctioned")
    assert sig.evidence["former"] is True
    assert "former related party" in sig.summary
    sig = _make_signal(code="RELATED_SANCTIONED", target=current, hit=hit, score=1.0,
                       summary_extra="sanctioned")
    assert "former" not in sig.evidence
    assert "former" not in sig.summary.lower()


def test_icij_signal_says_former() -> None:
    sig = _signal_from_match(
        {
            "id": "1",
            "name": "SEYED ZIYA IMANY",
            "score": 100,
            "match": True,
            "types": [{"id": ".../oldb/officer", "name": "Officer"}],
            "description": "Officer node extracted from the Panama Papers data.",
        },
        {"kind": "person", "statement_id": "P", "name": "Seyed Ziya Imany", "former": True},
        min_score=70,
    )
    assert sig is not None
    assert sig.summary.startswith("Former related party 'Seyed Ziya Imany' matches a record")
    assert sig.evidence["former"] is True


def test_openaleph_signal_and_screening_entry_carry_former() -> None:
    item = {"id": "oa-1", "schema": "Person", "properties": {"topics": ["sanction"]}}
    target = {"kind": "entity", "statement_id": "E", "name": "Old Parent Ltd", "former": True}
    sigs = _signals_from_percolate(item, target, "Old Parent Ltd", "Old Parent Ltd", 1.0)
    assert sigs, "a sanction topic must produce a signal"
    assert all(s.evidence["former"] is True for s in sigs)
    assert all(s.summary.startswith("Former related entity") for s in sigs)
    assert _screening_entry(item, target, "Old Parent Ltd", "Old Parent Ltd", 1.0)["former"] is True


def test_everypolitician_finding_names_a_former_party() -> None:
    assert finding_everypolitician("Member of Parliament", "Seyed Ziya Imany", former=True).startswith(
        "Possible name match only for Seyed Ziya Imany, a former party named in this company's records"
    )
    assert "former" not in finding_everypolitician("Member of Parliament", "Jane Smith")
    assert NameMatch(
        source_id="everypolitician", hit=None, target_name="x", subject_statement_id="P"
    ).former is False
