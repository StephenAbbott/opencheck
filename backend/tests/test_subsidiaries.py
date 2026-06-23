"""Tests for the GLEIF subsidiary-network reveal (lazy ``/subsidiaries``).

Covers the BODS mapping (a ``both`` child → two relationship statements, kept
distinct), the assemble summary (counts, render-mode threshold, direct-first
ordering, gating) and endpoint LEI validation. No network.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from opencheck import subsidiaries as subs
from opencheck.bods import map_gleif_subsidiaries
from opencheck.config import get_settings
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
        await subsidiaries_endpoint(lei="not-a-lei", format="summary")
    assert exc.value.status_code == 400
