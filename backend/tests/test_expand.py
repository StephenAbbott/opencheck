"""Tests for progressive-discovery expansion (/expand + /expand-layer).

Expansion re-anchors a standard lookup on a node's LEI and collapses every
representation of that entity onto the existing graph node (`anchor`) so the new
owners layer stitches on by statementId — including a national-register
representation that keys on the company number rather than the LEI (the
cross-source duplicate the first spike pass left floating). The underlying lookup
is monkeypatched so the tests are deterministic and offline.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods.mapper import _stable_id
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


_LEI_A = "213800LH1BZH3DI6G760"
_LEI_B = "5493001KJTIIGC8Y1R12"


def _layer(lei: str) -> list[dict]:
    """A minimal owners layer: the looked-up entity (GLEIF subject) + one owner
    person + the ownership relationship between them. Owner/rel ids are derived
    from the LEI so two layers don't falsely de-dupe."""
    subj = _stable_id("gleif", "entity", lei)
    tag = lei[-4:]
    return [
        {"statementId": subj, "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo",
                           "identifiers": [{"id": lei, "scheme": "XI-LEI", "schemeName": "LEI"}]}},
        {"statementId": f"owner-{tag}", "recordType": "person",
         "recordDetails": {"personType": "knownPerson",
                           "names": [{"type": "legal", "fullName": f"Owner {tag}"}]}},
        {"statementId": f"rel-{tag}", "recordType": "relationship",
         "recordDetails": {"subject": subj, "interestedParty": f"owner-{tag}",
                           "interests": [{"type": "shareholding"}]}},
    ]


def _patch_lookup(monkeypatch, builder=_layer, signals_builder=None):
    async def _fake_lookup(*, lei, deepen_top=3):
        sigs = signals_builder(lei) if signals_builder else []
        return SimpleNamespace(
            lei=lei, bods=builder(lei), bods_issues=[], risk_signals=sigs
        )

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake_lookup)


def test_expand_remaps_subject_onto_anchor(client, monkeypatch):
    _patch_lookup(monkeypatch)
    r = client.get("/expand", params={"lei": _LEI_A, "anchor": "ANCHOR-A"})
    assert r.status_code == 200
    bods = r.json()["bods"]
    subj = _stable_id("gleif", "entity", _LEI_A)
    ids = [s["statementId"] for s in bods]

    assert subj not in ids and all(subj not in str(s) for s in bods)
    rel = next(s for s in bods if s["recordType"] == "relationship")
    assert rel["recordDetails"]["subject"] == "ANCHOR-A"
    assert any(s["recordType"] == "person" for s in bods)  # owner came through


def test_expand_collapses_cross_source_duplicate(client, monkeypatch):
    """The fix: a national-register entity statement keyed on the company number
    (no LEI) collapses onto the anchor too, so its directors stitch on rather than
    floating as a duplicate node."""
    def _cross_source(lei: str) -> list[dict]:
        subj = _stable_id("gleif", "entity", lei)
        nat = "nat-entity-xyz"
        return [
            # GLEIF subject ties the LEI to the company number.
            {"statementId": subj, "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo",
                               "identifiers": [{"id": lei, "scheme": "XI-LEI"},
                                               {"id": "12345678", "scheme": "GB-COH"}]}},
            # National register: SAME company number, NO LEI → must still collapse.
            {"statementId": nat, "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo Ltd",
                               "identifiers": [{"id": "12345678", "scheme": "GB-COH"}]}},
            {"statementId": "dir-1", "recordType": "person",
             "recordDetails": {"personType": "knownPerson",
                               "names": [{"type": "legal", "fullName": "Jane Roe"}]}},
            {"statementId": "rel-nat", "recordType": "relationship",
             "recordDetails": {"subject": nat, "interestedParty": "dir-1",
                               "interests": [{"type": "seniorManagingOfficial"}]}},
        ]

    _patch_lookup(monkeypatch, _cross_source)
    r = client.get("/expand", params={"lei": _LEI_A, "anchor": "ANCHOR-A"})
    bods = r.json()["bods"]

    # Neither the GLEIF nor the national-register id survives — both collapsed.
    assert all("nat-entity-xyz" not in str(s) for s in bods)
    assert all(_stable_id("gleif", "entity", _LEI_A) not in str(s) for s in bods)
    # The national register's director now points at the anchor, not a duplicate.
    rel = next(s for s in bods if s["statementId"] == "rel-nat")
    assert rel["recordDetails"]["subject"] == "ANCHOR-A"


def test_expand_layer_batches_and_dedupes(client, monkeypatch):
    _patch_lookup(monkeypatch)
    r = client.post("/expand-layer", json={
        "items": [
            {"lei": _LEI_A, "anchor": "ANCHOR-A"},
            {"lei": _LEI_B, "anchor": "ANCHOR-B"},
        ]
    })
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 2
    assert data["expanded"] == ["ANCHOR-A", "ANCHOR-B"]
    assert data["truncated"] is False

    rels = [s for s in data["bods"] if s["recordType"] == "relationship"]
    subjects = {s["recordDetails"]["subject"] for s in rels}
    assert subjects == {"ANCHOR-A", "ANCHOR-B"}  # each owner stitched to its own anchor


def test_expand_layer_subsidiaries_direction(client, monkeypatch):
    """direction='subsidiaries' digs DOWN: fetch the node's GLEIF children and
    stitch them under the anchor (reusing the subsidiaries service)."""
    def _children_bundle(lei: str) -> dict:
        subj = _stable_id("gleif", "entity", lei)
        return {"bods": [
            {"statementId": subj, "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Parent",
                               "identifiers": [{"id": lei, "scheme": "XI-LEI"}]}},
            {"statementId": "child-1", "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Child Co"}},
            # In subsidiary BODS the child is the SUBJECT, parent the interestedParty.
            {"statementId": "rel-sub", "recordType": "relationship",
             "recordDetails": {"subject": "child-1", "interestedParty": subj,
                               "interests": [{"type": "otherInfluenceOrControl"}]}},
        ]}

    async def _fake_subs(lei, *, include_bods=False):
        assert include_bods is True
        return _children_bundle(lei.strip().upper())

    monkeypatch.setattr("opencheck.subsidiaries.assemble_subsidiaries", _fake_subs)

    r = client.post("/expand-layer", json={
        "items": [{"lei": _LEI_A, "anchor": "LEAF-A"}],
        "direction": "subsidiaries",
    })
    assert r.status_code == 200
    bods = r.json()["bods"]
    # The parent (the leaf) is collapsed onto the anchor; its child now hangs off it.
    rel = next(s for s in bods if s["statementId"] == "rel-sub")
    assert rel["recordDetails"]["interestedParty"] == "LEAF-A"
    assert any(s["statementId"] == "child-1" for s in bods)


def test_expand_layer_returns_remapped_risk_signals(client, monkeypatch):
    """Phase 2: each hop's sub-lookup already screens the expanded entity, so
    /expand-layer returns those risk signals with their statement-id evidence
    remapped onto the anchor (so they land on the right node in the network)."""
    def _sig(lei: str) -> list[dict]:
        subj = _stable_id("gleif", "entity", lei)
        return [{
            "code": "SANCTIONED", "confidence": "high", "source_id": "opensanctions",
            "hit_id": "X", "evidence": {"statement_id": subj},
        }]

    _patch_lookup(monkeypatch, signals_builder=_sig)
    r = client.post("/expand-layer", json={"items": [{"lei": _LEI_A, "anchor": "ANCHOR-A"}]})
    assert r.status_code == 200
    sigs = r.json()["risk_signals"]
    assert len(sigs) == 1
    assert sigs[0]["code"] == "SANCTIONED"
    # the expanded entity collapsed onto the anchor → the evidence id follows it.
    assert sigs[0]["evidence"]["statement_id"] == "ANCHOR-A"


def test_expand_rejects_bad_deepen(client):
    r = client.get("/expand", params={"lei": _LEI_A, "anchor": "A", "deepen_top": 99})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Phase 182 — the frontier keyed on register-scoped identifiers, with cheap hops
# ---------------------------------------------------------------------------

from opencheck import register_hops  # noqa: E402

_CH_SUBJECT = "02999029"  # Babcock Defence Systems Limited, filed as "2999029"
_CH_PARENT = "01915771"


def _ch_bundle(number: str) -> list[dict]:
    """What the Companies House mapper emits for a company whose only PSC is
    another UK company: the subject keyed on its number, the corporate PSC keyed
    on its own, and the ownership relationship between them."""
    subj = _stable_id("companies_house", "entity", number)
    parent = _stable_id("companies_house", "entity", _CH_PARENT)

    def _entity(sid: str, name: str, ident: str, scheme_name: str) -> dict:
        return {"statementId": sid, "recordType": "entity",
                "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": name,
                                  "identifiers": [{"id": ident, "scheme": "GB-COH",
                                                   "schemeName": scheme_name}]}}

    return [
        _entity(subj, "BABCOCK DEFENCE SYSTEMS LIMITED", number, "Companies House"),
        _entity(parent, "BABCOCK SOUTHERN HOLDINGS LIMITED", _CH_PARENT, "UK Companies House"),
        {"statementId": "rel-ch", "recordType": "relationship",
         "recordDetails": {"subject": subj, "interestedParty": parent,
                           "interests": [{"type": "shareholding"}]}},
    ]


def _patch_register_hop(monkeypatch, *, fetched: list[str], screened: list[list[dict]]):
    """Stand in for the Companies House fetch, its mapper and the name screen —
    the three things a register hop spends — recording what was asked."""
    async def _fake_fetch(adapter, hit_id, **kwargs):
        assert adapter.id == "companies_house"
        fetched.append(hit_id)
        return {"source_id": "companies_house", "company_number": hit_id}, None

    def _fake_mapper(source_id):
        assert source_id == "companies_house"
        return lambda raw: _ch_bundle(raw["company_number"])

    async def _fake_screen(bods, *, degraded=None, **kwargs):
        screened.append(bods)
        from opencheck.risk import RiskSignal
        subj = next(s["statementId"] for s in bods if s["recordType"] == "entity")
        return [RiskSignal(
            code="RELATED_SANCTIONED", confidence="medium", source_id="opensanctions",
            hit_id="os-1", summary="name match",
            evidence={"subject_statement_id": subj},
        )]

    async def _no_lookup(**kwargs):
        raise AssertionError("a register hop must not run the full lookup")

    monkeypatch.setattr("opencheck.routers.lookup._fetch_with_provenance", _fake_fetch)
    monkeypatch.setattr("opencheck.routers.lookup._mapper_for", _fake_mapper)
    monkeypatch.setattr("opencheck.routers.lookup.assess_cross_source_names", _fake_screen)
    monkeypatch.setattr("opencheck.routers.lookup.assess_bundle", lambda *a, **k: [])
    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _no_lookup)


def test_hop_schemes_are_derived_from_what_the_adapters_declare():
    hops = register_hops.hop_schemes()
    assert hops["GB-COH"].source_id == "companies_house"
    # Any scheme the mapper knows an RA code for, whose adapter declares a
    # deriver on that code, is a hop — per-scheme, not UK-only.
    assert hops["NL-KVK"].source_id == "kvk"
    assert hops["FR-INSEE"].source_id == "inpi"
    assert register_hops.hop_for("gb-coh") is hops["GB-COH"]
    # The mappers' fallback scheme for an unnamed register — "REG-GB" on a PSC
    # filed as registered in "England And Wales", and on OpenAleph's UK
    # records — is the same register.
    assert register_hops.hop_for("REG-GB") is hops["GB-COH"]
    assert register_hops.hop_for("REG-NL") is hops["NL-KVK"]
    assert register_hops.hop_for("XI-LEI") is None and register_hops.hop_for(None) is None
    # The Companies House normaliser is Phase 177's: the filed spelling
    # becomes the canonical eight characters, and a bare word is refused.
    assert hops["GB-COH"].normalise("2999029") == _CH_SUBJECT
    with pytest.raises(ValueError):
        hops["GB-COH"].normalise("Uk")


def test_expand_schemes_endpoint_lists_the_hops(client):
    r = client.get("/expand-schemes")
    assert r.status_code == 200
    schemes = r.json()["schemes"]
    assert schemes["GB-COH"] == {"source_id": "companies_house", "name": "UK Companies House"}


def test_a_register_hop_files_its_walk_under_the_hop_origin(client, monkeypatch):
    """Phase 184: while the register is fetched, ``signalstats.walk_origin``
    reads "hop", so the adapter's walk counters separate FullCheck hops from
    subject lookups; afterwards it is back to the default."""
    from opencheck import signalstats

    seen: list[str] = []
    fetched: list[str] = []
    _patch_register_hop(monkeypatch, fetched=fetched, screened=[])
    real_fetch = __import__("opencheck.routers.lookup", fromlist=["x"])._fetch_with_provenance

    async def _observing_fetch(adapter, hit_id, **kwargs):
        seen.append(signalstats.walk_origin.get())
        return await real_fetch(adapter, hit_id, **kwargs)

    monkeypatch.setattr("opencheck.routers.lookup._fetch_with_provenance", _observing_fetch)
    r = client.post("/expand-layer", json={
        "items": [{"scheme": "GB-COH", "id": "2999029", "anchor": "ANCHOR-CH"}],
    })
    assert r.status_code == 200
    assert seen == ["hop"]
    assert signalstats.walk_origin.get() == "lookup"


def test_a_register_hop_dispatches_only_the_owning_register(client, monkeypatch):
    fetched: list[str] = []
    screened: list[list[dict]] = []
    _patch_register_hop(monkeypatch, fetched=fetched, screened=screened)

    r = client.post("/expand-layer", json={
        "items": [{"scheme": "GB-COH", "id": "2999029", "anchor": "ANCHOR-CH"}],
    })
    assert r.status_code == 200
    data = r.json()
    # One register call, on the canonical number — not the filed spelling.
    assert fetched == [_CH_SUBJECT]
    assert data["hops"] == {"lei": 0, "register": 1, "skipped": 0}
    assert data["expanded"] == ["ANCHOR-CH"]

    bods = data["bods"]
    subj = _stable_id("companies_house", "entity", _CH_SUBJECT)
    # The register's own statement for the node collapsed onto the anchor; its
    # parent hangs off the anchor rather than a floating duplicate.
    assert all(subj not in str(s) for s in bods)
    rel = next(s for s in bods if s["statementId"] == "rel-ch")
    assert rel["recordDetails"]["subject"] == "ANCHOR-CH"
    parent = _stable_id("companies_house", "entity", _CH_PARENT)
    assert rel["recordDetails"]["interestedParty"] == parent
    assert any(s["statementId"] == parent for s in bods)

    # The new node was screened, and the signal followed it onto the anchor.
    assert len(screened) == 1
    sigs = data["risk_signals"]
    assert [s["code"] for s in sigs] == ["RELATED_SANCTIONED"]
    assert sigs[0]["evidence"]["subject_statement_id"] == "ANCHOR-CH"


def test_a_layer_mixes_lei_and_register_hops(client, monkeypatch):
    fetched: list[str] = []
    _patch_register_hop(monkeypatch, fetched=fetched, screened=[])
    _patch_lookup(monkeypatch)  # re-enables the full lookup for the LEI item

    r = client.post("/expand-layer", json={
        "items": [
            {"lei": _LEI_A, "anchor": "ANCHOR-A"},
            {"scheme": "GB-COH", "id": _CH_SUBJECT, "anchor": "ANCHOR-CH"},
            # A node carrying both is expanded on its LEI: no register call.
            {"lei": _LEI_B, "scheme": "GB-COH", "id": "00000001", "anchor": "ANCHOR-B"},
        ],
    })
    assert r.status_code == 200
    data = r.json()
    assert data["hops"] == {"lei": 2, "register": 1, "skipped": 0}
    assert fetched == [_CH_SUBJECT]
    rels = [s for s in data["bods"] if s["recordType"] == "relationship"]
    assert {s["recordDetails"]["subject"] for s in rels} == {"ANCHOR-A", "ANCHOR-B", "ANCHOR-CH"}


@pytest.mark.parametrize(
    ("item", "direction"),
    [
        ({"scheme": "XI-LEI", "id": "5493001KJTIIGC8Y1R12", "anchor": "X"}, "owners"),
        ({"scheme": "REG-JE", "id": "12345", "anchor": "X"}, "owners"),  # Jersey: no register
        ({"scheme": "GB-COH", "id": _CH_SUBJECT, "anchor": "X"}, "subsidiaries"),  # needs an LEI
    ],
)
def test_a_register_item_without_a_hop_is_skipped_not_guessed(client, monkeypatch, item, direction):
    fetched: list[str] = []
    _patch_register_hop(monkeypatch, fetched=fetched, screened=[])
    r = client.post("/expand-layer", json={"items": [item], "direction": direction})
    assert r.status_code == 200
    data = r.json()
    assert data["hops"] == {"lei": 0, "register": 0, "skipped": 1}
    assert data["bods"] == [] and data["risk_signals"] == []
    assert fetched == []
    # Still reported as expanded: the frontier must not offer it again.
    assert data["expanded"] == ["X"]


def test_a_value_that_is_not_a_number_of_that_register_yields_nothing(client, monkeypatch):
    fetched: list[str] = []
    _patch_register_hop(monkeypatch, fetched=fetched, screened=[])
    r = client.post("/expand-layer", json={
        "items": [{"scheme": "GB-COH", "id": "Uk", "anchor": "X"}],
    })
    assert r.status_code == 200
    assert r.json()["bods"] == [] and fetched == []


def test_an_item_needs_an_lei_or_a_scheme_and_an_id(client):
    r = client.post("/expand-layer", json={"items": [{"anchor": "X"}]})
    assert r.status_code == 422
    r = client.post("/expand-layer", json={"items": [{"scheme": "GB-COH", "anchor": "X"}]})
    assert r.status_code == 422


def test_anchor_replacements_seed_the_gleif_subject_only_for_an_lei():
    from opencheck.routers.lookup import _anchor_replacements

    bods = _ch_bundle(_CH_SUBJECT)
    repl = _anchor_replacements(bods, _CH_SUBJECT, "ANCHOR")
    assert repl == {_stable_id("companies_house", "entity", _CH_SUBJECT): "ANCHOR"}
    # The parent shares no identifier value with the subject: untouched.
    assert _stable_id("companies_house", "entity", _CH_PARENT) not in repl
