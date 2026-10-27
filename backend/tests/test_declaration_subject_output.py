"""The declarationSubject fixes found by the BODS quality sweep of 26 Oct 2026.

Three failures, one test group each:

* ``/deepen`` on an LEI-keyed source whose mapper asserts no LEI
  (``cac_nigeria``, ``eiti_bo``, ``eiti_soe``) could not find the subject:
  the hit id is the LEI, and the bundle holds the company and its owners, so
  none of ``ref`` / ``hit_id`` / ``single`` named one and every party kept
  itself. The relationships already named the company — ``find_subject``
  now reads them.
* The merged second pass (``stamp_merged``) ran inside the pipeline only;
  the ``/lookup`` response is folded from ``deepen_result`` payloads, so it,
  the exports and saved reports carried the first pass, and OpenAleph's
  several records of one subject each named themselves (ASDA, Maersk,
  Regulatory DataCorp). The mapping now rides a ``declaration_subjects``
  event the fold applies.
* ``/deepen`` turned an upstream timeout into the app's bare 500
  ("Internal server error: ") — Corning on ``ny_dos``. It is a 502 that names
  the cause.
"""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest
from fastapi.testclient import TestClient

from opencheck import provenance
from opencheck.bods.declaration import (
    apply_network_subjects,
    find_subject,
    network_subjects,
    stamp_bundle,
    stamp_merged,
)
from opencheck.bods.statements import (
    make_entity_statement,
    make_relationship_statement,
)
from opencheck.bods_quality import check_declaration_subjects

DANGOTE_LEI = "029200697A0R1BH0A835"
RDC_LEI = "894500LD30X1VN839203"


@pytest.fixture(autouse=True)
def _scoped():
    with provenance.mapping_provenance(
        provenance.Provenance(liveness="live", retrieved_at=datetime.now(timezone.utc))
    ):
        yield


def _entity(source_id, local_id, name, identifiers=(), entity_type="registeredEntity",
            jurisdiction=("Nigeria", "NG")):
    return make_entity_statement(
        source_id=source_id, local_id=local_id, name=name,
        jurisdiction=jurisdiction, identifiers=list(identifiers), entity_type=entity_type,
    )


def _owned(source_id, subject, owners):
    rels = [
        make_relationship_statement(
            source_id=source_id, local_id=f"r-{i}",
            subject_statement_id=subject["recordId"],
            interested_party_statement_id=o["recordId"], interested_party_type="entity",
            interests=[{"type": "shareholding"}],
        )
        for i, o in enumerate(owners)
    ]
    return [subject, *owners, *rels]


def _dangote(source_id: str) -> list[dict]:
    """Dangote Cement PLC as ``cac_nigeria`` / ``eiti_bo`` map it: the company
    by its RC number (no LEI — CAC publishes none) and three owners."""
    subject = _entity(source_id, "208767", "DANGOTE CEMENT PLC",
                      [{"scheme": "NG-CAC", "id": "208767"}])
    owners = [
        _entity(source_id, "o1", "Stanbic IBTC Nominees Nigeria Limited"),
        _entity(source_id, "o2", "Dangote Industries Limited"),
        _entity(source_id, "o3", "Various shareholders (unspecified)", entity_type="unknownEntity"),
    ]
    return _owned(source_id, subject, owners)


# ---------------------------------------------------------------------------
# find_subject reads the relationships
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source_id", ["cac_nigeria", "eiti_bo"])
def test_an_lei_keyed_bundle_without_the_lei_is_resolved_by_its_relationships(source_id) -> None:
    bundle = _dangote(source_id)
    assert find_subject(bundle, hit_id=DANGOTE_LEI) == (bundle[0], "relationships")
    out = stamp_bundle(bundle, source_id=source_id, hit_id=DANGOTE_LEI)
    assert {s["declarationSubject"] for s in out} == {bundle[0]["recordId"]}
    assert check_declaration_subjects(out) == []


def test_eiti_soe_company_and_its_state_owner() -> None:
    subject = _entity("eiti_soe", "equinor-energy", "Equinor Energy AS", jurisdiction=("Norway", "NO"))
    state = _entity("eiti_soe", "no-gov", "Government of Norway", entity_type="state",
                    jurisdiction=("Norway", "NO"))
    bundle = _owned("eiti_soe", subject, [state])
    assert find_subject(bundle, hit_id="98450073EGD581D89F03") == (subject, "relationships")


def test_a_subsidiary_bundle_is_not_read_as_one_subject() -> None:
    """One parent over several children: the relationships name several
    subjects, and the rule says nothing rather than pick a child."""
    parent = _entity("gleif", "p", "Parent")
    kids = [_entity("gleif", f"c{i}", f"Child {i}") for i in range(2)]
    rels = [
        make_relationship_statement(
            source_id="gleif", local_id=f"r{i}",
            subject_statement_id=k["recordId"],
            interested_party_statement_id=parent["recordId"], interested_party_type="entity",
            interests=[{"type": "shareholding"}],
        )
        for i, k in enumerate(kids)
    ]
    assert find_subject([parent, *kids, *rels], hit_id="nothing") == (None, "unresolved")


def test_a_chain_whose_only_subject_is_also_an_owner_is_left_alone() -> None:
    """A → B → C with only the A–B edge present names B as subject, but if B
    is also an interested party somewhere the rule refuses."""
    a, b, c = (_entity("x", n, n) for n in ("a", "b", "c"))
    r1 = make_relationship_statement(
        source_id="x", local_id="r1", subject_statement_id=b["recordId"],
        interested_party_statement_id=a["recordId"], interested_party_type="entity",
        interests=[{"type": "shareholding"}],
    )
    r2 = make_relationship_statement(
        source_id="x", local_id="r2", subject_statement_id=b["recordId"],
        interested_party_statement_id=b["recordId"], interested_party_type="entity",
        interests=[{"type": "shareholding"}],
    )
    assert find_subject([a, b, c, r1, r2], hit_id="nothing") == (None, "unresolved")
    # No relationships at all: nothing to read.
    assert find_subject([a, b], hit_id="nothing") == (None, "unresolved")


# ---------------------------------------------------------------------------
# The merged pass reaches the response
# ---------------------------------------------------------------------------


def _rdc_merged() -> tuple[list[dict], list[dict], list[dict]]:
    """Regulatory DataCorp on 26 Oct 2026: GLEIF's record, and two OpenAleph
    records of the company — one with ``REG-GB 08739364`` (joins the subject),
    one an OCDS procurement party with a bare ``REG`` and no jurisdiction
    (does not join). Each OpenAleph record is its own deepened bundle."""
    gleif = _entity("gleif", RDC_LEI, "Regulatory DataCorp Limited",
                    [{"scheme": "XI-LEI", "id": RDC_LEI}, {"scheme": "GB-COH", "id": "08739364"}],
                    jurisdiction=("United Kingdom", "GB"))
    oa_reg = _entity("openaleph", "oc-companies-gb-08739364", "REGULATORY DATACORP LIMITED",
                     [{"scheme": "OPENALEPH", "id": "oc-companies-gb-08739364"},
                      {"scheme": "REG-GB", "id": "08739364"}],
                     jurisdiction=("United Kingdom", "GB"))
    oa_ocds = make_entity_statement(
        source_id="openaleph", local_id="ocds-party-gb-fts-88512",
        name="REGULATORY DATACORP LIMITED",
        identifiers=[{"scheme": "OPENALEPH", "id": "ocds-party-gb-fts-88512"},
                     {"scheme": "REG", "id": "08739364"}],
    )
    return [gleif], [oa_reg], [oa_ocds]


def test_the_merged_pass_folds_into_the_response() -> None:
    from opencheck.routers.lookup import fold_lookup_events

    gleif, oa1, oa2 = _rdc_merged()
    # What production returned: the first pass only, two OpenAleph subjects.
    first_pass = [*gleif, *oa1, *oa2]
    assert any("different declarationSubject" in f.message
               for f in check_declaration_subjects(first_pass))

    subjects = network_subjects(first_pass, RDC_LEI)
    assert subjects == {"gleif": gleif[0]["recordId"], "openaleph": oa1[0]["recordId"]}
    resp = fold_lookup_events(
        RDC_LEI,
        [
            ("gleif_done", {"lei": RDC_LEI, "legal_name": "Regulatory DataCorp Limited",
                            "jurisdiction": "GB", "derived_identifiers": {}}),
            ("deepen_result", {"source_id": "gleif", "bods": gleif}),
            ("deepen_result", {"source_id": "openaleph", "bods": oa1}),
            ("deepen_result", {"source_id": "openaleph", "bods": oa2}),
            ("declaration_subjects", {"subjects": subjects}),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert check_declaration_subjects(resp.bods) == []
    declared = {s["declarationSubject"] for s in resp.bods
                if s["source"]["opencheckSourceId"] == "openaleph"}
    assert declared == {oa1[0]["recordId"]}
    # The fold agrees with the pipeline's own merged pass.
    assert resp.bods == stamp_merged(first_pass, RDC_LEI)


def test_a_run_recorded_before_the_event_folds_as_it_did() -> None:
    from opencheck.routers.lookup import fold_lookup_events

    gleif, oa1, oa2 = _rdc_merged()
    resp = fold_lookup_events(
        RDC_LEI,
        [
            ("deepen_result", {"bods": [*gleif, *oa1, *oa2]}),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert resp.bods == [*gleif, *oa1, *oa2]


def test_apply_network_subjects_ignores_junk_and_copies_only_changes() -> None:
    gleif, oa1, oa2 = _rdc_merged()
    bundle = [*gleif, *oa1, *oa2]
    assert apply_network_subjects(bundle, {}) is bundle
    assert apply_network_subjects(bundle, {"": "x", "openaleph": None}) is bundle  # type: ignore[dict-item]
    out = apply_network_subjects(bundle, {"openaleph": oa1[0]["recordId"]})
    assert out[0] is bundle[0] and out[1] is bundle[1] and out[2] is not bundle[2]
    assert bundle[2]["declarationSubject"] == bundle[2]["recordId"]  # never mutated


# ---------------------------------------------------------------------------
# /deepen says what failed
# ---------------------------------------------------------------------------


def test_deepen_answers_502_with_the_cause_when_the_upstream_times_out(monkeypatch, tmp_path) -> None:
    from opencheck.app import app
    from opencheck.config import get_settings
    from opencheck.sources import REGISTRY

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    async def _timeout(hit_id, **kwargs):
        raise httpx.ReadTimeout("")

    monkeypatch.setattr(REGISTRY["ny_dos"], "fetch", _timeout)
    try:
        r = TestClient(app).get("/deepen", params={"source": "ny_dos", "hit_id": "49779"})
    finally:
        get_settings.cache_clear()
    assert r.status_code == 502
    detail = r.json()["detail"]
    assert detail.startswith("ny_dos did not answer: ReadTimeout")
    assert "Internal server error" not in detail
