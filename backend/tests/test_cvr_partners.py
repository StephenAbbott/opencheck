"""CVR fully liable participants (BackgroundCheck Phase F, 21 Oct 2026).

Datafordeler's open CVR data names a partnership's fully liable participants
(I/S, K/S, P/S partners) through ``CVR_FuldtAnsvarligDeltagerRelation`` and
nothing else about people. The fake server below answers in the shapes
recorded live on 21 Oct 2026 (MADEGÅRDEN I/S, CVR 20652047; Willis Towers
Watson Consultancy Services I/S, CVR 31796407) — names and participant
numbers replaced. What it pins:

* every list is paged (Datafordeler answers 100 rows a page; Willis has 132
  relation rows and the adapter used to read 100);
* an ``in`` list holds at most 100 ids (DAF-GQL-0016);
* a person is keyed on CVR's participant number, never a CPR number, with no
  birth date; ``AndreDeltagerePerson`` is a person too;
* a partner whose every period ended is a closed relationship;
* a failed participant read is a degradation, and the bundle is not cached.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck import degradation, provenance
from opencheck.bods.mapper import map_cvr_denmark
from opencheck.bods.mappers.denmark import CVR_PARTICIPANT_SCHEME
from opencheck.sources import cvr_denmark as cvr
from opencheck.sources.cvr_denmark import CvrDenmarkAdapter, build_participants

_FIRM_ID = "4001037784"


def _root(query: str) -> str:
    return query.split("{", 2)[1].split("(")[0].strip()


class FakeDatafordeler:
    """Answers the adapter's queries the way Datafordeler does.

    ``page`` rows a page (100 live), refuses an ``in`` list over 100.
    """

    def __init__(self, relations: list[dict], units: list[dict], names: list[dict], *, page: int = 100):
        self.relations = relations
        self.units = {u["id"]: u for u in units}
        self.names = names
        self.page = page
        self.calls: list[str] = []
        self.fail_root: str | None = None

    def _paged(self, root: str, rows: list[dict], variables: dict) -> dict:
        size = min(variables.get("first") or 100, self.page)
        start = int(variables.get("after") or 0)
        chunk = rows[start : start + size]
        more = start + size < len(rows)
        return {
            "data": {
                root: {
                    "nodes": chunk,
                    "pageInfo": {"hasNextPage": more, "endCursor": str(start + size) if more else None},
                }
            }
        }

    async def post(self, url: str, **kwargs: Any) -> MagicMock:
        body = kwargs["json"]
        root = _root(body["query"])
        variables = body.get("variables") or {}
        self.calls.append(root)
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if root == self.fail_root:
            resp.json.return_value = {"errors": [{"message": "boom"}], "data": {root: None}}
            return resp
        ids = variables.get("ids")
        if ids is not None and len(ids) > 100:
            resp.json.return_value = {
                "errors": [{"message": "The number of elements in the supplied 'in' list is not allowed to exceed 100", "extensions": {"code": "DAF-GQL-0016"}}],
                "data": {root: None},
            }
            return resp
        if root == "CVR_Virksomhed":
            payload = {"data": {root: {"nodes": [{"CVRNummer": 20652047, "id": _FIRM_ID, "status": "aktiv", "virksomhedStartdato": "1998-03-01", "virksomhedOphoersdato": None, "virkningFra": "1998-03-01", "virkningTil": None}]}}}
        elif root == "CVR_FuldtAnsvarligDeltagerRelation":
            payload = self._paged(root, self.relations, variables)
        elif root == "CVR_CVREnhed":
            payload = self._paged(root, [self.units[i] for i in ids if i in self.units], variables)
        elif root == "CVR_Navn" and ids is not None:
            payload = self._paged(root, [n for n in self.names if n["CVREnhedsId"] in ids], variables)
        elif root == "CVR_Navn":
            payload = self._paged(root, [{"vaerdi": "EXAMPLE PARTNERS I/S", "sekvens": 0, "virkningFra": "1998-03-01", "virkningTil": None}], variables)
        else:
            payload = self._paged(root, [], variables)
        resp.json.return_value = payload
        return resp


def _person(pid: str, etype: str = "CVRPerson") -> dict:
    return {"id": pid, "enhedsType": etype, "forretningsnoegletype": "CVREnhedsId", "forretningsnoegle": pid}


def _name(pid: str, name: str, to: str | None = None) -> dict:
    return {"CVREnhedsId": pid, "vaerdi": name, "sekvens": 0, "virkningFra": "1998-03-01", "virkningTil": to}


@pytest.fixture
def live(monkeypatch, tmp_path):
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CVR_DENMARK_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def _fetch(fake: FakeDatafordeler, **kw) -> dict:
    client = AsyncMock()
    client.post = fake.post
    client.aclose = AsyncMock()
    with patch("opencheck.sources.cvr_denmark.build_client", return_value=client):
        return await CvrDenmarkAdapter().fetch("20652047", **kw)


def _map(bundle: dict) -> list[dict]:
    with provenance.recording():
        return list(map_cvr_denmark(bundle))


# ---------------------------------------------------------------------------
# The two-partner I/S (MADEGÅRDEN's shape)
# ---------------------------------------------------------------------------


async def test_two_current_partners_become_two_people(live) -> None:
    fake = FakeDatafordeler(
        relations=[
            {"deltagendeEnhedsId": "4000000001", "virkningFra": "1998-03-01", "virkningTil": None},
            {"deltagendeEnhedsId": "4000000002", "virkningFra": "1998-03-01", "virkningTil": None},
        ],
        units=[_person("4000000001"), _person("4000000002")],
        names=[_name("4000000001", "Anders Example"), _name("4000000002", "Bente Example")],
    )
    bundle = await _fetch(fake)
    assert bundle["participants_read"] is True
    assert bundle["fully_liable_participant_ids"] == ["4000000001", "4000000002"]
    stmts = _map(bundle)
    people = [s for s in stmts if s["recordType"] == "person"]
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    assert sorted(p["recordDetails"]["names"][0]["fullName"] for p in people) == ["Anders Example", "Bente Example"]
    assert len(rels) == 2 and all(r.get("recordStatus") != "closed" for r in rels)
    for p in people:
        rd = p["recordDetails"]
        assert "birthDate" not in rd
        # Not an identity document: the number rides in an identifying
        # annotation (Phase 255), where BackgroundCheck reads it.
        assert not rd.get("identifiers")
        notes = [a["description"] for a in p.get("annotations", []) if a.get("motivation") == "identifying"]
        assert any(f"scheme {CVR_PARTICIPANT_SCHEME}" in n for n in notes)
    interest = rels[0]["recordDetails"]["interests"][0]
    assert interest["type"] == "otherInfluenceOrControl"
    assert interest["startDate"] == "1998-03-01"
    assert "beneficialOwnershipOrControl" not in interest
    assert "share" not in interest
    # Two extra requests for every participant, not two per participant.
    assert fake.calls.count("CVR_CVREnhed") == 1
    assert fake.calls.count("CVR_Navn") == 2  # the firm's own names + the partners'


async def test_no_partners_means_no_extra_requests(live) -> None:
    fake = FakeDatafordeler(relations=[], units=[], names=[])
    bundle = await _fetch(fake)
    assert bundle["participants"] == []
    assert "CVR_CVREnhed" not in fake.calls
    assert [s["recordType"] for s in _map(bundle)] == ["entity"]


# ---------------------------------------------------------------------------
# The large firm (Willis's shape): paging, the in-list cap, ended partners
# ---------------------------------------------------------------------------


def _big_firm(n: int = 132, current: int = 2) -> FakeDatafordeler:
    relations, units, names = [], [], []
    for i in range(n):
        pid = f"40000{i:05d}"
        ended = i >= current
        relations.append({"deltagendeEnhedsId": pid, "virkningFra": "2008-10-01", "virkningTil": "2023-09-01" if ended else None})
        units.append(_person(pid))
        names.append(_name(pid, f"Partner {i}"))
    return FakeDatafordeler(relations, units, names)


async def test_every_relation_row_is_read_past_the_first_page(live) -> None:
    fake = _big_firm()
    bundle = await _fetch(fake)
    assert len(bundle["participants"]) == 132
    assert fake.calls.count("CVR_FuldtAnsvarligDeltagerRelation") == 2  # 100 + 32
    assert fake.calls.count("CVR_CVREnhed") == 2  # ids in chunks of 100
    assert bundle["fully_liable_participant_ids"] == ["4000000000", "4000000001"]


async def test_ended_partners_are_closed_relationships(live) -> None:
    bundle = await _fetch(_big_firm())
    rels = [s for s in _map(bundle) if s["recordType"] == "relationship"]
    closed = [r for r in rels if r.get("recordStatus") == "closed"]
    assert len(rels) == 132 and len(closed) == 130
    assert all(r["recordDetails"]["interests"][0]["endDate"] == "2023-09-01" for r in closed)


async def test_a_firm_whose_partners_all_left_has_no_current_partner(live) -> None:
    # ``_current`` falls back to every row when none is current; the partner
    # list must not.
    bundle = await _fetch(_big_firm(n=3, current=0))
    assert bundle["fully_liable_participant_ids"] == []
    assert all(not p["current"] for p in bundle["participants"])


# ---------------------------------------------------------------------------
# Kinds of participant
# ---------------------------------------------------------------------------


def test_participant_kinds() -> None:
    rel = [
        {"deltagendeEnhedsId": "1", "virkningFra": "2010-01-01", "virkningTil": None},
        {"deltagendeEnhedsId": "2", "virkningFra": "2010-01-01", "virkningTil": None},
        {"deltagendeEnhedsId": "3", "virkningFra": "2010-01-01", "virkningTil": None},
        {"deltagendeEnhedsId": "4", "virkningFra": "2010-01-01", "virkningTil": None},
    ]
    units = [
        _person("1"),
        _person("2", "AndreDeltagerePerson"),
        {"id": "3", "enhedsType": "Virksomhed", "forretningsnoegletype": "CVRNummer", "forretningsnoegle": "11563445"},
    ]
    parts = {p["participant_id"]: p for p in build_participants(rel, units, [_name("3", "EXAMPLE HOLDING A/S")])}
    assert parts["1"]["kind"] == "person"
    assert parts["2"]["kind"] == "person"
    assert parts["3"]["kind"] == "entity" and parts["3"]["cvr_number"] == "11563445"
    assert parts["4"]["kind"] == "unknown"


def test_a_partner_company_is_keyed_on_its_cvr_number() -> None:
    bundle = {
        "cvr_number": "20652047", "name": "EXAMPLE PARTNERS I/S", "status": "active",
        "source_url": "https://datacvr.virk.dk/enhed/virksomhed/20652047",
        "participants": [{
            "participant_id": "3", "kind": "entity", "cvr_number": "11563445",
            "name": "EXAMPLE HOLDING A/S",
            "periods": [{"from": "2010-01-01", "to": None}], "current": True,
        }],
    }
    stmts = _map(bundle)
    entity = [s for s in stmts if s["recordType"] == "entity"][1]
    assert entity["recordDetails"]["identifiers"][0] == {
        "id": "11563445", "scheme": "DK-CVR",
        "schemeName": "Det Centrale Virksomhedsregister — Danish Central Business Register",
    }
    # The same statement id the company's own CVR record gets.
    own = _map({"cvr_number": "11563445", "name": "EXAMPLE HOLDING A/S", "status": "active",
                "source_url": "https://datacvr.virk.dk/enhed/virksomhed/11563445"})[0]
    assert entity["statementId"] == own["statementId"]


def test_an_unnamed_person_is_an_unknown_person() -> None:
    bundle = {
        "cvr_number": "20652047", "name": "EXAMPLE PARTNERS I/S", "status": "active",
        "source_url": "https://datacvr.virk.dk/enhed/virksomhed/20652047",
        "participants": [{"participant_id": "9", "kind": "person", "cvr_number": None, "name": None,
                          "periods": [{"from": "2010-01-01", "to": None}], "current": True}],
    }
    person = [s for s in _map(bundle) if s["recordType"] == "person"][0]
    assert person["recordDetails"]["personType"] == "unknownPerson"


# ---------------------------------------------------------------------------
# Failure: said, and never cached
# ---------------------------------------------------------------------------


async def test_a_failed_participant_read_is_a_degradation_and_is_not_cached(live) -> None:
    from opencheck.cache import Cache

    fake = FakeDatafordeler(
        relations=[{"deltagendeEnhedsId": "4000000001", "virkningFra": "1998-03-01", "virkningTil": None}],
        units=[_person("4000000001")],
        names=[_name("4000000001", "Anders Example")],
    )
    fake.fail_root = "CVR_CVREnhed"
    cache = MagicMock(spec=Cache)
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    degradation.begin()
    bundle = await _fetch(fake, cache=cache)
    recorded = degradation.collect()
    assert bundle["participants_read"] is False
    assert bundle["participants"] == []
    cache.set.assert_not_called()
    assert [(d.source_id, d.check, d.detail) for d in recorded] == [
        ("cvr_denmark", degradation.CHECK_SOURCE_READ, cvr.PARTICIPANTS_NOT_READ)
    ]
    # The fixed sentence names no one.
    assert "Example" not in json.dumps([d.detail for d in recorded])


async def test_a_complete_bundle_is_cached_under_v2(live) -> None:
    from opencheck.cache import Cache

    fake = FakeDatafordeler(relations=[], units=[], names=[])
    cache = MagicMock(spec=Cache)
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    await _fetch(fake, cache=cache)
    assert cache.set.call_args.args[0] == "cvr_denmark:v2:20652047"
