"""Tests for the GLEIF subsidiary-network reveal (lazy ``/subsidiaries``).

Covers the BODS mapping (a ``both`` child → two relationship statements, kept
distinct), the assemble summary (counts, render-mode threshold, direct-first
ordering, gating), the Phase 146 honest-degradation behaviour (GLEIF refusing
is not an entity without children; a degraded result is never cached; the
Golden Copy snapshot stands in for the direct relation) and endpoint LEI
validation. No network.
"""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest
from fastapi import HTTPException

from opencheck import subsidiaries as subs
from opencheck.bods import map_gleif_subsidiaries
from opencheck.config import get_settings
from opencheck.gleif_throttle import GleifRateLimitedError
from opencheck.routers.subsidiaries import SubsidiariesResponse
from opencheck.routers.subsidiaries import subsidiaries as subsidiaries_endpoint

_SUBJECT = "549300NCQQ9E4O5JX172"  # Fonterra Co-operative Group


def _l1(lei: str, name: str, *, jur: str = "NZ", status: str = "ACTIVE") -> dict:
    """A minimal GLEIF Level-1 record (data object)."""
    return {
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "jurisdiction": jur,
                "status": status,
            },
        },
    }


def _children(*specs: tuple[str, str, list[str]]) -> list[dict]:
    return [{"record": _l1(lei, name), "relations": rels} for lei, name, rels in specs]


# ---------------------------------------------------------------------------
# BODS mapping
# ---------------------------------------------------------------------------


def test_both_child_emits_two_distinct_relationship_statements():
    children = _children(("254900AAAAAAAAAAAA01", "Both Child Ltd", ["direct", "ultimate"]))
    stmts = map_gleif_subsidiaries(_SUBJECT, {"entity": {"legalName": {"name": "Subject"}}}, children)

    rels = [s for s in stmts if s["recordType"] == "relationship"]
    ents = [s for s in stmts if s["recordType"] == "entity"]
    # subject + one child entity, two relationships (direct + indirect).
    assert len(ents) == 2
    assert len(rels) == 2

    dirs = sorted(r["recordDetails"]["interests"][0]["directOrIndirect"] for r in rels)
    assert dirs == ["direct", "indirect"]
    # Both statements stay distinct (different statementId) but share the pair.
    assert len({r["statementId"] for r in rels}) == 2
    details = {r["recordDetails"]["interests"][0]["details"] for r in rels}
    assert any("direct-child" in d for d in details)
    assert any("ultimate-child" in d for d in details)


def test_direct_and_ultimate_only_children_emit_single_statements():
    children = _children(
        ("254900AAAAAAAAAAAA02", "Direct Only Ltd", ["direct"]),
        ("254900AAAAAAAAAAAA03", "Ultimate Only Ltd", ["ultimate"]),
    )
    stmts = map_gleif_subsidiaries(_SUBJECT, {}, children)
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    assert len(rels) == 2
    by_dir = {r["recordDetails"]["interests"][0]["directOrIndirect"] for r in rels}
    assert by_dir == {"direct", "indirect"}


def test_mapper_returns_empty_without_subject_lei():
    assert map_gleif_subsidiaries("", {}, _children(("X", "Y", ["direct"]))) == []


# ---------------------------------------------------------------------------
# assemble_subsidiaries — gating + summary shaping
# ---------------------------------------------------------------------------


async def test_assemble_unavailable_when_live_disabled(monkeypatch):
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    res = await subs.assemble_subsidiaries(_SUBJECT)
    get_settings.cache_clear()
    assert res["available"] is False
    assert res["reason"] == "live mode disabled"


def _fake_build(direct, ultimate, children):
    async def _inner(lei: str):
        return {
            "lei": lei,
            "subject_attrs": {"entity": {"legalName": {"name": "Subject"}}},
            "direct_total": direct,
            "ultimate_total": ultimate,
            "children": children,
        }

    return _inner


async def test_assemble_small_network_is_graph_mode(monkeypatch):
    children = _children(
        ("254900AAAAAAAAAAAA10", "Alpha", ["direct", "ultimate"]),
        ("254900AAAAAAAAAAAA11", "Bravo", ["direct"]),
        ("254900AAAAAAAAAAAA12", "Charlie", ["ultimate"]),
    )
    monkeypatch.setattr(subs, "_build", _fake_build(2, 2, children))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    get_settings.cache_clear()
    res = await subs.assemble_subsidiaries(_SUBJECT, include_bods=True)
    get_settings.cache_clear()

    assert res["available"] is True
    assert res["render_mode"] == "graph"
    assert res["distinct_fetched"] == 3
    assert res["indirect_only"] == 1  # Charlie
    # node_estimate = max(direct_total, ultimate_total, distinct) = 3
    assert res["node_estimate"] == 3
    assert res["bods"] is not None
    # one "both" child → 2 rels; one direct + one ultimate → 1 each = 4 rels.
    rels = [s for s in res["bods"] if s["recordType"] == "relationship"]
    assert len(rels) == 4


async def test_assemble_large_network_degrades_to_table(monkeypatch):
    # Counts exceed the graph threshold even though only a few rows were fetched.
    children = _children(("254900AAAAAAAAAAAA20", "Only One", ["direct"]))
    monkeypatch.setattr(subs, "_build", _fake_build(400, 350, children))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    get_settings.cache_clear()
    res = await subs.assemble_subsidiaries(_SUBJECT)
    get_settings.cache_clear()

    assert res["render_mode"] == "table"
    assert res["node_estimate"] == 400
    assert res["truncated"] is True  # 1 fetched << 400 estimated
    assert res["bods"] is None  # not requested


async def test_endpoint_rejects_bad_lei():
    with pytest.raises(HTTPException) as exc:
        await subsidiaries_endpoint(request=None, response=None, lei="not-a-lei", format="summary")
    assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# Phase 146 — GLEIF refusing must not read as "this entity has no children"
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    """Answers the subject record and the two children endpoints by URL.

    ``children`` maps ``"direct"``/``"ultimate"`` to a response *or* an
    exception to raise — the three shapes GLEIF saturation actually produces
    (a 429 handed back by the Phase 143 transport, the throttle refusing to
    send, a network error).
    """

    def __init__(self, *, subject=None, children=None) -> None:
        self._subject = subject if subject is not None else _FakeResponse(200, {})
        self._children = children or {}
        self.calls: list[str] = []

    async def get(self, url: str, **kwargs):
        self.calls.append(url)
        for kind in ("direct", "ultimate"):
            if url.endswith(f"/{kind}-children"):
                outcome = self._children.get(kind)
                if isinstance(outcome, BaseException):
                    raise outcome
                if outcome is None:
                    return _FakeResponse(404)
                return outcome
        if isinstance(self._subject, BaseException):
            raise self._subject
        return self._subject


class _FakeCM:
    def __init__(self, client) -> None:
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *exc):
        return False


def _page(records: list[dict], total: int) -> _FakeResponse:
    return _FakeResponse(
        200,
        {"data": records, "meta": {"pagination": {"total": total, "lastPage": 1}}},
    )


def _gleif_429() -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "https://api.gleif.org/api/v1/lei-records/X/direct-children")
    resp = httpx.Response(429, request=req)
    return httpx.HTTPStatusError("Client error '429'", request=req, response=resp)


@pytest.fixture
def _live(monkeypatch, tmp_path):
    """Live mode on, with an isolated (empty) cache root."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.mark.parametrize(
    "failure",
    [
        _FakeResponse(429),
        GleifRateLimitedError("budget exhausted"),
        httpx.ConnectTimeout("timed out"),
    ],
    ids=["429-handed-back", "throttle-refused-to-send", "network"],
)
async def test_gleif_refusal_is_declared_not_reported_as_no_children(_live, failure):
    """The 2026-08-29 failure: Shell's 105 direct children rendered as none.

    Every refusal shape must produce a 200 that SAYS the network could not be
    checked — never an empty list that reads as a childless entity.
    """
    client = _FakeClient(children={"direct": failure, "ultimate": failure})
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        with patch.object(subs, "_snapshot_children", lambda lei: None):
            res = await subs.assemble_subsidiaries(_SUBJECT)

    assert res["children_available"] is False
    assert res["direct_available"] is False and res["ultimate_available"] is False
    assert res["degraded_detail"] and "not a finding" in res["degraded_detail"]
    assert res["children"] == []


async def test_404_children_is_a_real_answer(_live):
    """GLEIF answering "no children of this kind" is not degradation."""
    client = _FakeClient(children={"direct": None, "ultimate": None})
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        res = await subs.assemble_subsidiaries(_SUBJECT)

    assert res["children_available"] is True
    assert res["degraded_detail"] is None
    assert res["available"] is False  # genuinely childless


async def test_partial_refusal_names_the_relation_that_failed(_live):
    """One relation answered, one refused → rows on screen, declared partial."""
    child = _l1("254900AAAAAAAAAAAA30", "Direct Only Ltd")
    client = _FakeClient(
        children={"direct": _page([child], 1), "ultimate": _FakeResponse(429)}
    )
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        res = await subs.assemble_subsidiaries(_SUBJECT)

    assert res["children_available"] is True
    assert res["direct_available"] is True and res["ultimate_available"] is False
    assert "ultimate" in res["degraded_detail"]
    assert res["distinct_fetched"] == 1


async def test_degraded_result_is_never_cached(_live):
    """The worst half of the bug: a degraded empty network cached with no age
    bound was served as truth long after GLEIF recovered."""
    client = _FakeClient(children={"direct": _FakeResponse(429), "ultimate": _FakeResponse(429)})
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        with patch.object(subs, "_snapshot_children", lambda lei: None):
            await subs.assemble_subsidiaries(_SUBJECT)
    assert subs._cache.get_payload(f"{subs._CACHE_NS}/{_SUBJECT}") is None

    # A complete answer, by contrast, is cached — and then served.
    good = _FakeClient(children={"direct": _page([_l1("254900AAAAAAAAAAAA31", "Alpha")], 1)})
    with patch.object(subs, "build_client", lambda: _FakeCM(good)):
        await subs.assemble_subsidiaries(_SUBJECT)
    cached = subs._cache.get_payload(f"{subs._CACHE_NS}/{_SUBJECT}")
    assert cached is not None and cached[0]["complete"] is True


async def test_cache_entries_without_the_completeness_marker_are_refetched(_live):
    """Entries written before Phase 146 may be exactly the poisoned empties
    the saturation wave wrote, so they are not trusted."""
    key = f"{subs._CACHE_NS}/{_SUBJECT}"
    subs._cache.put(key, {
        "lei": _SUBJECT, "subject_attrs": {}, "direct_total": 0,
        "ultimate_total": 0, "children": [],
    })
    client = _FakeClient(children={"direct": _page([_l1("254900AAAAAAAAAAAA32", "Bravo")], 1)})
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        res = await subs.assemble_subsidiaries(_SUBJECT)
    assert res["distinct_fetched"] == 1  # refetched, not served from the stale empty
    assert client.calls


async def test_snapshot_stands_in_for_the_direct_relation(_live):
    """Golden Copy fallback, exactly as the anchor's _snapshot_bundle does —
    badged as snapshot, and never claiming the ultimate relation it lacks."""
    snap_child = _l1("254900AAAAAAAAAAAA40", "Snapshot Child Ltd")
    client = _FakeClient(children={"direct": _FakeResponse(429), "ultimate": _FakeResponse(429)})
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        with patch.object(subs, "_snapshot_children", lambda lei: ([snap_child], 7, "2026-08-01")):
            res = await subs.assemble_subsidiaries(_SUBJECT, include_bods=True)

    assert res["children_available"] is True
    assert res["direct_available"] is True
    assert res["ultimate_available"] is False
    assert res["snapshot_fallback"] is True and res["snapshot_date"] == "2026-08-01"
    assert "snapshot" in res["degraded_detail"] and "2026-08-01" in res["degraded_detail"]
    assert res["direct_total"] == 7
    assert res["children"][0]["lei"] == "254900AAAAAAAAAAAA40"
    # The rows are real, so they are exported — as snapshot-dated statements.
    assert res["bods"] is not None
    subject = [s for s in res["bods"] if s["recordType"] == "entity"][0]
    assert subject["source"]["retrievedAt"].startswith("2026-08-01")


async def test_snapshot_children_reads_the_entity_store(_live, monkeypatch):
    """The helper itself: store rows in, GLEIF-shaped records out."""
    from opencheck import entity_pages

    row = entity_pages.EntityRow(
        lei="254900AAAAAAAAAAAA50", name="Store Child Ltd", slug="store-child-ltd",
        entity_status="ACTIVE", registration_status="ISSUED", jurisdiction="NZ",
        legal_form=None, city="Auckland", region=None, country="NZ",
        first_registered=None, last_updated=None, successor_lei=None,
        direct_parent_lei=_SUBJECT, ultimate_parent_lei=None,
    )

    class _Store:
        def children(self, lei, limit=20):
            return ([row], 1)

        def meta(self):
            return {"source_publish_date": "2026-08-01T00:00:00Z"}

    monkeypatch.setattr(entity_pages, "get_store", lambda: _Store())
    out = subs._snapshot_children(_SUBJECT)
    assert out is not None
    records, total, publish = out
    assert total == 1 and publish == "2026-08-01"
    assert records[0]["attributes"]["entity"]["legalName"]["name"] == "Store Child Ltd"
    # registeredAs/registeredAt are NOT in the store — never invented.
    assert "registeredAs" not in records[0]["attributes"]["entity"]


async def test_endpoint_reports_degradation_as_200(_live):
    """Router-level: the response carries the flags, and it is still a 200."""
    client = _FakeClient(children={"direct": _FakeResponse(429), "ultimate": _FakeResponse(429)})
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        with patch.object(subs, "_snapshot_children", lambda lei: None):
            resp = await subsidiaries_endpoint(
                request=None, response=None, lei=_SUBJECT, format="summary"
            )
    # The route returns the assembled dict; FastAPI serialises it through the
    # response model, so validate it the same way the app would.
    model = SubsidiariesResponse(**resp)
    assert model.children_available is False
    assert model.degraded_detail
    assert model.children == []
