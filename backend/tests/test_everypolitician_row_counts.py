"""EveryPolitician rows get a statement count, so a one-statement row has no
Diagram button (Phase 300).

The rows arrive from the related-party name screen, after the dispatch-stage
``bods_counts`` event, so until now they had no count and every row offered a
diagram that opened onto a single person statement and vanished. The count is
read, not assumed — a politician whose record carries an ownership edge maps to
more than one statement and keeps the diagram.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from opencheck.routers import lookup
from opencheck.sources import REGISTRY


def _person(entity_id: str, **props: list[Any]) -> dict[str, Any]:
    return {
        "id": entity_id,
        "schema": "Person",
        "properties": {"name": [f"Politician {entity_id}"], "nationality": ["no"], **props},
    }


class _OpenSanctionsStub:
    """Stands in for the OpenSanctions adapter's cached ``fetch``."""

    def __init__(self, records: dict[str, Any]) -> None:
        self.records = records
        self.calls: list[str] = []

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        self.calls.append(hit_id)
        rec = self.records[hit_id]
        if isinstance(rec, BaseException):
            raise rec
        return rec


def _run(monkeypatch: pytest.MonkeyPatch, records: dict[str, Any], ids: list[str]):
    stub = _OpenSanctionsStub(records)
    monkeypatch.setitem(REGISTRY, "opensanctions", stub)
    return asyncio.run(lookup._everypolitician_row_counts(ids)), stub


def test_a_plain_politician_counts_one_person_statement(monkeypatch) -> None:
    (counts, breakdown), stub = _run(
        monkeypatch,
        {"NK-a": {"entity_id": "NK-a", "entity": _person("NK-a")}},
        ["NK-a"],
    )
    assert counts == {"everypolitician:NK-a": 1}
    assert breakdown["everypolitician:NK-a"] == {
        "entities": 0, "persons": 1, "relationships": 0,
    }
    assert stub.calls == ["NK-a"]


def test_a_politician_who_owns_a_company_keeps_the_diagram(monkeypatch) -> None:
    owned = {
        "id": "own-1",
        "schema": "Ownership",
        "properties": {
            "owner": ["NK-b"],
            "asset": [{
                "id": "co-1",
                "schema": "Company",
                "properties": {"name": ["Fjord Holding AS"], "jurisdiction": ["no"]},
            }],
            "percentage": ["60"],
        },
    }
    (counts, breakdown), _ = _run(
        monkeypatch,
        {"NK-b": {"entity_id": "NK-b", "entity": _person("NK-b", ownershipOwner=[owned])}},
        ["NK-b"],
    )
    assert counts["everypolitician:NK-b"] > 1
    assert breakdown["everypolitician:NK-b"]["relationships"] >= 1


def test_an_unreadable_record_gets_no_count(monkeypatch) -> None:
    """No count leaves the row as it was: the button shown until opened."""
    (counts, breakdown), _ = _run(
        monkeypatch,
        {
            "NK-ok": {"entity_id": "NK-ok", "entity": _person("NK-ok")},
            "NK-err": RuntimeError("upstream down"),
            "NK-stub": {"source_id": "opensanctions", "hit_id": "NK-stub", "is_stub": True},
        },
        ["NK-ok", "NK-err", "NK-stub"],
    )
    assert set(counts) == {"everypolitician:NK-ok"}
    assert set(breakdown) == {"everypolitician:NK-ok"}


def test_no_rows_reads_nothing(monkeypatch) -> None:
    (counts, breakdown), stub = _run(monkeypatch, {}, [])
    assert counts == {} and breakdown == {}
    assert stub.calls == []
