"""Phase 235 — the subject identity set, and one statement per statementId.

Two findings from the Opus 5.5 project check (DQ-2, DQ-11):

* The related-party screens screened every entity statement in the merged
  bundle, including other sources' statements *of the looked-up company*.
  Rosneft (``253400JT3MQWNDKMJE44``): OpenSanctions' own Rosneft record, which
  carries Rosneft's LEI, produced RELATED_SANCTIONED / RELATED_DEBARMENT /
  RELATED_EXPORT_CONTROLLED / RELATED_EXPORT_RISK, and GEM's "Rosneft PJSC"
  more through OpenAleph. CLP HOLDINGS LIMITED read "Related entity 'CLP
  HOLDINGS LIMITED'" in the Panama Papers — twice.
* ``/export`` repeated statementIds: 13 on Rosneft (the OpenSanctions mapper,
  one copy per FtM edge naming a party), one on Swire Pacific and one on DBS
  (GLEIF emitting the parent once as direct and once as ultimate).

The bundles below are those shapes, trimmed to the statements that matter.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck.app import app
from opencheck.bods import unique_statements, validate_shape
from opencheck.bods.mapper import map_ftm, map_gleif
from opencheck.config import get_settings
from opencheck.cross_check import _collect_targets, assess_cross_source_names
from opencheck.icij_check import _RECONCILE_URL, assess_icij_names
from opencheck.openaleph_check import assess_openaleph_names
from opencheck.routers.lookup import fold_lookup_events
from opencheck.sources import REGISTRY, SearchKind, SourceHit
from opencheck.subject_identity import subject_identity

ROSNEFT = "253400JT3MQWNDKMJE44"
CLP = "213800TWMS92BSR6XO37"


def _entity(
    sid: str,
    name: str,
    *,
    source: str,
    identifiers: list[tuple[str, str]] = (),
    jurisdiction: str | None = None,
) -> dict[str, Any]:
    rd: dict[str, Any] = {
        "isComponent": False,
        "entityType": {"type": "registeredEntity"},
        "name": name,
        "identifiers": [{"scheme": s, "id": v} for s, v in identifiers],
    }
    if jurisdiction:
        rd["jurisdiction"] = {"code": jurisdiction, "name": jurisdiction}
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "entity",
        "recordStatus": "new",
        "recordDetails": rd,
        "source": {"type": ["thirdParty"], "description": source},
    }


def _person(sid: str, name: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "person",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "personType": "knownPerson",
            "names": [{"type": "legal", "fullName": name}],
        },
        "source": {"type": ["thirdParty"], "description": "Wikidata"},
    }


def _rel(sid: str, subject: str, party: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "relationship",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": party,
            "interests": [{"type": "shareholding", "directOrIndirect": "direct"}],
        },
    }


def _rosneft_bundle() -> list[dict[str, Any]]:
    """GLEIF's Rosneft, OpenSanctions' Rosneft (LEI + OGRN), GEM's "Rosneft
    PJSC" (LEI), an OpenSanctions record carrying only Rosneft's OGRN, a real
    subsidiary, a real owner and a person."""
    return [
        _entity(
            "gleif-rosneft",
            'публичное акционерное общество "Нефтяная компания "Роснефть"',
            source="GLEIF",
            identifiers=[("XI-LEI", ROSNEFT), ("RU-OGRN", "1027700043502")],
            jurisdiction="RU",
        ),
        _entity(
            "os-rosneft",
            "PUBLIC JOINT-STOCK COMPANY ROSNEFT OIL COMPANY",
            source="OpenSanctions",
            identifiers=[
                ("OPENSANCTIONS", "NK-nq68oAyhLJDTxPnYfQq8Vg"),
                ("XI-LEI", ROSNEFT),
                ("RU-OGRN", "1027700043502"),
            ],
        ),
        _entity(
            "gem-rosneft",
            "Rosneft PJSC",
            source="Global Energy Monitor / Climate TRACE",
            identifiers=[("GEM-ENTITY", "E100000000874"), ("XI-LEI", ROSNEFT)],
        ),
        _entity(
            "os-rosneft-ogrn-only",
            "Rosneft Oil Company",
            source="OpenSanctions",
            identifiers=[("OPENSANCTIONS", "NK-other"), ("RU-OGRN", "1027700043502")],
        ),
        _entity(
            "os-neft-aktiv",
            "Neft-Aktiv LLC",
            source="OpenSanctions",
            identifiers=[("OPENSANCTIONS", "NK-WuLeLicyX6X9VYjevbn5Zv"), ("RU-OGRN", "1077746098495")],
        ),
        _entity(
            "wd-rosneftegaz",
            "Rosneftegaz",
            source="Wikidata",
            identifiers=[("WIKIDATA", "Q4397843")],
        ),
        _rel("r1", "os-neft-aktiv", "os-rosneft"),
        _rel("r2", "gem-rosneft", "wd-rosneftegaz"),
        _person("wd-sechin", "Igor Sechin"),
    ]


SUBJECT_IDS = {"gleif-rosneft", "os-rosneft", "gem-rosneft", "os-rosneft-ogrn-only"}


# ---------------------------------------------------------------------
# The identity set
# ---------------------------------------------------------------------


def test_identity_set_holds_every_source_statement_of_the_subject() -> None:
    identity = subject_identity(ROSNEFT, _rosneft_bundle())
    assert set(identity.statement_ids) == SUBJECT_IDS
    # GLEIF's statement anchors a subject-level finding.
    assert identity.anchor_statement_id() == "gleif-rosneft"


def test_a_registry_number_alone_joins_the_set_but_a_name_never_does() -> None:
    bundle = _rosneft_bundle() + [
        _entity("namesake", "PUBLIC JOINT-STOCK COMPANY ROSNEFT OIL COMPANY", source="OpenAleph"),
    ]
    identity = subject_identity(ROSNEFT, bundle)
    assert "os-rosneft-ogrn-only" in identity  # shares the OGRN
    assert "namesake" not in identity  # same name, no identifier


def test_no_lei_or_no_carrier_means_an_empty_set() -> None:
    assert not subject_identity(None, _rosneft_bundle())
    assert not subject_identity("", _rosneft_bundle())
    assert not subject_identity("5493000000000000XX00", _rosneft_bundle())


def test_a_company_cannot_own_itself() -> None:
    """A statement joined to the subject by a relationship is a counterparty,
    whatever identifier it shares: the end that does not itself carry the LEI
    leaves the set and stays a screened related party."""
    subject_lei = "213800ABCDEFGHIJKL12"
    bundle = [
        _entity(
            "gleif-subject",
            "Pension Trustee Ltd",
            source="GLEIF",
            identifiers=[("XI-LEI", subject_lei), ("GB-COH", "01234567")],
        ),
        # The parent carries the subject's company number (filed against the
        # wrong company) but not its LEI.
        _entity(
            "parent",
            "Parent Group PLC",
            source="OpenCorporates",
            identifiers=[("GB-COH", "01234567")],
        ),
        _rel("parent-rel", "gleif-subject", "parent"),
    ]
    identity = subject_identity(subject_lei, bundle)
    assert set(identity.statement_ids) == {"gleif-subject"}


# ---------------------------------------------------------------------
# The three screens
# ---------------------------------------------------------------------


@pytest.fixture()
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    monkeypatch.setenv("OPENALEPH_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _RecordingAdapter:
    def __init__(self, hits_for: dict[str, list[SourceHit]] | None = None) -> None:
        self.queries: list[str] = []
        self._hits_for = hits_for or {}

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        self.queries.append(query)
        return list(self._hits_for.get(query, []))


def _os_hit(name: str, topics: list[str]) -> SourceHit:
    return SourceHit(
        source_id="opensanctions",
        hit_id=f"NK-{name[:6]}",
        kind=SearchKind.ENTITY,
        name=name,
        summary="",
        identifiers={},
        raw={"id": "x", "schema": "Company", "properties": {"name": [name], "topics": topics}, "topics": topics},
        is_stub=False,
    )


def test_collect_targets_leaves_out_the_identity_set() -> None:
    bundle = _rosneft_bundle()
    identity = subject_identity(ROSNEFT, bundle)
    names = {t["name"] for t in _collect_targets(bundle, exclude=identity.statement_ids)}
    assert names == {"Neft-Aktiv LLC", "Rosneftegaz", "Igor Sechin"}


async def test_rosneft_is_not_screened_as_its_own_related_party(live, monkeypatch) -> None:
    os_adapter = _RecordingAdapter(
        {
            "PUBLIC JOINT-STOCK COMPANY ROSNEFT OIL COMPANY": [
                _os_hit("PUBLIC JOINT-STOCK COMPANY ROSNEFT OIL COMPANY", ["sanction", "debarment"])
            ],
            "Neft-Aktiv LLC": [_os_hit("Neft-Aktiv LLC", ["sanction"])],
        }
    )
    monkeypatch.setitem(REGISTRY, "opensanctions", os_adapter)
    monkeypatch.setitem(REGISTRY, "everypolitician", _RecordingAdapter())

    signals = await assess_cross_source_names(_rosneft_bundle(), subject_lei=ROSNEFT)

    screened = set(os_adapter.queries)
    assert not screened & {
        "PUBLIC JOINT-STOCK COMPANY ROSNEFT OIL COMPANY",
        "Rosneft PJSC",
        "Rosneft Oil Company",
    }
    assert all(s.evidence["subject_statement_id"] not in SUBJECT_IDS for s in signals)
    # A real related party still fires.
    assert {s.evidence["subject_statement_id"] for s in signals} == {"os-neft-aktiv"}


async def test_without_a_subject_lei_the_screen_is_unchanged(live, monkeypatch) -> None:
    """The register hop in /expand-layer screens the hop node on purpose — its
    anchor is not the looked-up company — and passes no LEI."""
    os_adapter = _RecordingAdapter()
    monkeypatch.setitem(REGISTRY, "opensanctions", os_adapter)
    monkeypatch.setitem(REGISTRY, "everypolitician", _RecordingAdapter())
    await assess_cross_source_names(_rosneft_bundle())
    assert "PUBLIC JOINT-STOCK COMPANY ROSNEFT OIL COMPANY" in os_adapter.queries


async def test_openaleph_does_not_percolate_the_subject(live, monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake(text, *, schema=None, topics=(), limit=25):
        calls.append({"text": text, "schema": schema})
        return []

    monkeypatch.setattr(REGISTRY["openaleph"], "percolate_text", fake)
    await assess_openaleph_names(_rosneft_bundle(), subject_lei=ROSNEFT)
    entity_text = next(c["text"] for c in calls if c["schema"] != "Person")
    assert set(entity_text.split("\n")) == {"Neft-Aktiv LLC", "Rosneftegaz"}


# ---------------------------------------------------------------------
# ICIJ — the subject is screened once, as the subject
# ---------------------------------------------------------------------


def _clp_bundle() -> list[dict[str, Any]]:
    return [
        _entity(
            "gleif-clp",
            "CLP HOLDINGS LIMITED",
            source="GLEIF",
            identifiers=[("XI-LEI", CLP), ("", "134207")],
            jurisdiction="JE",
        ),
        _entity(
            "oa-clp",
            "CLP HOLDINGS LIMITED",
            source="OpenAleph",
            identifiers=[("XI-LEI", CLP), ("REG-JE", "134207")],
        ),
        _entity("owner", "Offshore Vehicle Ltd", source="GLEIF"),
        _rel("r", "gleif-clp", "owner"),
    ]


def _icij_callback(matches: dict[str, dict[str, Any]]):
    """Answer each reconcile query whose text is a key of ``matches``."""

    def _cb(request: httpx.Request) -> httpx.Response:
        queries = json.loads(parse_qs(request.content.decode())["queries"][0])
        out: dict[str, Any] = {}
        for key, q in queries.items():
            m = matches.get(q["query"])
            out[key] = {"result": [m] if m and key.endswith("intermediary") else []}
        return httpx.Response(200, json=out)

    return _cb


async def test_icij_screens_the_subject_once_and_words_it_as_the_subject(
    live, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_callback(
        _icij_callback(
            {
                "CLP HOLDINGS LIMITED": {
                    "id": "23000105",
                    "name": "CLP HOLDINGS LIMITED",
                    "score": 100,
                    "match": True,
                    "types": [{"id": "x", "name": "Intermediary"}],
                    "description": "Intermediary node extracted from the Panama Papers data.",
                },
                "Offshore Vehicle Ltd": {
                    "id": "42",
                    "name": "OFFSHORE VEHICLE LTD",
                    "score": 100,
                    "match": True,
                    "types": [{"id": "x", "name": "Intermediary"}],
                    "description": "Intermediary node extracted from the Panama Papers data.",
                },
            }
        ),
        url=_RECONCILE_URL,
        method="POST",
        is_reusable=True,
    )

    signals = await assess_icij_names(_clp_bundle(), subject_lei=CLP)

    subject = [s for s in signals if s.evidence.get("subject")]
    related = [s for s in signals if not s.evidence.get("subject")]
    # One signal for the company's own name, not one per source statement.
    assert len(subject) == 1
    s = subject[0]
    assert s.evidence["statement_id"] == "gleif-clp"
    assert "subject_statement_id" not in s.evidence
    assert s.summary.startswith("The looked-up company's name 'CLP HOLDINGS LIMITED'")
    assert "Related" not in s.summary
    # A real related party keeps its related-party wording.
    assert [r.evidence["subject_statement_id"] for r in related] == ["owner"]
    assert related[0].summary.startswith("Related entity 'Offshore Vehicle Ltd'")


# ---------------------------------------------------------------------
# One statement per statementId
# ---------------------------------------------------------------------


def test_unique_statements_keeps_the_first_of_each_id_in_order() -> None:
    a, b = _entity("a", "A", source="GLEIF"), _entity("b", "B", source="GLEIF")
    changed = dict(a, recordStatus="updated")
    assert unique_statements([a, b, dict(a), changed]) == [a, b]


def test_validate_shape_reports_a_duplicate_statement_id() -> None:
    a = _entity("a", "A", source="GLEIF")
    issues = validate_shape([a, dict(a)])
    assert any("duplicate statementId" in i for i in issues)
    assert not any("duplicate" in i for i in validate_shape([a]))


def _gleif_record(lei: str, name: str) -> dict[str, Any]:
    return {
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "jurisdiction": "HK",
                "legalAddress": {"country": "HK"},
            },
            "registration": {"status": "ISSUED", "lastUpdateDate": "2026-09-01T00:00:00Z"},
        },
    }


def test_gleif_parent_that_is_both_direct_and_ultimate_is_one_statement() -> None:
    """Swire Pacific: John Swire & Sons is the direct and the ultimate parent."""
    parent = _gleif_record("213800FTN3BUK6LABB14", "JOHN SWIRE & SONS LIMITED")
    bundle = map_gleif(
        {
            "lei": "549300KZOZHII0DGF611",
            "record": _gleif_record("549300KZOZHII0DGF611", "SWIRE PACIFIC LIMITED"),
            "direct_parent": parent,
            "ultimate_parent": parent,
        }
    )
    stmts = list(bundle)
    ids = [s["statementId"] for s in stmts]
    assert len(ids) == len(set(ids))
    parents = [s for s in stmts if (s["recordDetails"].get("name") == "JOHN SWIRE & SONS LIMITED")]
    assert len(parents) == 1
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    # Both relationships stay, and both point at the one party statement.
    assert {r["recordDetails"]["interests"][0]["directOrIndirect"] for r in rels} == {"direct", "indirect"}
    assert {r["recordDetails"]["interestedParty"] for r in rels} == {parents[0]["statementId"]}
    assert not any("duplicate" in i for i in validate_shape(stmts))


def test_gleif_exception_bridges_for_direct_and_ultimate_stay_two() -> None:
    """GLEIF does not say a direct and an ultimate exception are one party
    (Stephen, 23 Sept 2026): two unknown parties, two distinct ids."""
    exc = {"attributes": {"reason": "NON_CONSOLIDATING", "category": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT"}}
    bundle = map_gleif(
        {
            "lei": "213800TWMS92BSR6XO37",
            "record": _gleif_record("213800TWMS92BSR6XO37", "CLP HOLDINGS LIMITED"),
            "direct_parent_exception": exc,
            "ultimate_parent_exception": exc,
        }
    )
    unknown = [
        s for s in bundle
        if s["recordType"] == "entity" and s["recordDetails"]["entityType"]["type"] == "unknownEntity"
    ]
    assert len(unknown) == 2
    assert len({s["statementId"] for s in unknown}) == 2


def test_ftm_party_named_by_two_edges_is_one_statement() -> None:
    """Rosneft's OpenSanctions record names a subsidiary once per edge."""
    sub = {"id": "co-sub", "schema": "Company", "properties": {"name": ["RN Vankor OOO"]}}
    edges = [
        {
            "id": f"own-{i}",
            "schema": "Ownership",
            "properties": {"percentage": [pct], "owner": ["NK-rosneft"], "asset": [sub]},
        }
        for i, pct in ((1, "50"), (2, "49"))
    ]
    payload = {
        "id": "NK-rosneft",
        "schema": "Company",
        "properties": {"name": ["ROSNEFT"], "ownershipOwner": edges},
    }
    stmts = list(map_ftm(payload, source_id="opensanctions"))
    ids = [s["statementId"] for s in stmts]
    assert len(ids) == len(set(ids))
    assert sum(1 for s in stmts if s["recordDetails"].get("name") == "RN Vankor OOO") == 1
    assert sum(1 for s in stmts if s["recordType"] == "relationship") == 2


def test_the_fold_publishes_each_statement_once() -> None:
    """Two deepened results of one source can map the same party; a saved
    report stored before Phase 235 replays through the same fold."""
    party = _entity("opencheck-x", "RN-Aktiv", source="OpenSanctions")
    events = [
        ("deepen_result", {"source_id": "opensanctions", "bods": [party]}),
        ("deepen_result", {"source_id": "opensanctions", "bods": [dict(party)]}),
    ]
    resp = fold_lookup_events(ROSNEFT, events)
    assert [s["statementId"] for s in resp.bods] == ["opencheck-x"]


def test_export_network_publishes_each_statement_once() -> None:
    party = _entity("opencheck-x", "RN-Aktiv", source="OpenSanctions")
    client = TestClient(app)
    r = client.post(
        "/export-network",
        json={"bods": [party, dict(party)], "format": "json", "slug": "t"},
    )
    assert r.status_code == 200, r.text
    assert [s["statementId"] for s in r.json()] == ["opencheck-x"]
