"""Guard the OpenTender blob projection (issue #30).

``opencheck.opentender_projection.project_tender`` slims a raw DIGIWHIST tender
down to the fields OpenCheck actually consumes, so the ~5 GB ``opentender.db``
can be rebuilt small. These tests pin the projection against the *real*
consumers by behaviour-equality: every field ``_tender_hit``, ``_walk_bodies`` /
``_bridge_identifier`` and ``map_opentender`` read must survive projection, so
running each consumer on a projected record yields output equal to running it on
the full record. Idempotency and an end-to-end slim of a synthetic fixture DB
(built via the extractor's own schema path) are covered too.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck.bods import map_opentender
from opencheck.opentender_projection import project_tender
from opencheck.sources.opentender import (
    OpenTenderAdapter,
    _bridge_identifier,
    _walk_bodies,
)

# The build/slim scripts aren't importable package members — load them by path,
# exactly as the runtime does.
_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extract_opentender = _load("extract_opentender")
slim_opentender = _load("slim_opentender")


# ---------------------------------------------------------------------------
# Realistic synthetic tenders covering every consumed field (plus lots of noise
# that projection must drop).
# ---------------------------------------------------------------------------

def _full_tender_awarded() -> dict[str, Any]:
    """An awarded tender exercising buyers, onBehalfOf, lots→bids→bidders/subs,
    a winning bid (→ relationship statement), identifiers of every bridged type,
    addresses, prices, and ot scores — wrapped in noise fields."""
    return {
        "persistentId": "UK-tender-0001",
        "id": "src-0001",
        "title": "Supply of widgets",
        "titleEnglish": "Supply of widgets (EN)",
        "country": "UK",
        "procedureType": "OPEN",
        "awardDecisionDate": "2024-06-01",
        "isAwarded": True,
        "ot": {"integrity": 0.83, "transparency": 0.61, "corruptionRisk": 0.2},
        "publications": [
            {
                "humanReadableURL": "https://opentender.eu/uk/tender/UK-tender-0001",
                "publicationDate": "2024-05-01",
                "source": "TED",
            },
            {"humanReadableURL": "https://example.test/notice/2"},
        ],
        # --- NOISE (must be dropped) ---
        "description": "x" * 4000,
        "cpvs": [{"code": "30190000", "isMain": True}],
        "corrections": [{"foo": "bar"}],
        "documents": [{"url": "https://example.test/doc.pdf"}],
        "buyers": [
            {
                "name": "Ministry of Widgets",
                "bodyIds": [
                    {"type": "VAT", "scope": "GB", "id": "GB123456789", "extra": "n"},
                    {"type": "ORGANIZATION_ID", "scope": "UNKNOWN", "id": "12345"},
                ],
                "address": {
                    "street": "1 Whitehall",
                    "city": "London",
                    "postcode": "SW1A 2AA",
                    "country": "GB",
                    "rawAddress": "noise",
                },
                "contactName": "noise",
                "email": "noise@example.test",
            }
        ],
        "onBehalfOf": [
            {
                "name": "Central Procurement",
                "bodyIds": [{"type": "TRADE_REGISTER", "scope": "GB", "id": "REG-99"}],
                "junk": True,
            }
        ],
        "lots": [
            {
                "awardDecisionDate": "2024-06-15",
                "title": "Lot 1 noise",
                "bids": [
                    {
                        "isWinning": True,
                        "price": {
                            "netAmount": 100000,
                            "currency": "GBP",
                            "amountWithVat": 120000,
                        },
                        "bidders": [
                            {
                                "name": "WidgetCo Ltd",
                                "bodyIds": [
                                    {"type": "ORGANIZATION_ID", "scope": "GB", "id": "9876"}
                                ],
                                "address": {"city": "Leeds", "country": "GB"},
                                "noise": 1,
                            }
                        ],
                        "subcontractors": [
                            {
                                "name": "SubbyCo",
                                "bodyIds": [
                                    {"type": "VAT", "scope": "GB", "id": "GB999888777"}
                                ],
                            }
                        ],
                    },
                    {
                        "isWinning": False,
                        "price": {"netAmount": 110000, "currency": "GBP"},
                        "bidders": [
                            {
                                "name": "Runner Up Ltd",
                                "bodyIds": [
                                    {
                                        "type": "ETALON_ID",
                                        "scope": "GLOBAL",
                                        "id": "5493001KJTIIGC8Y1R12",
                                    }
                                ],
                            }
                        ],
                    },
                ],
                "noiseKey": ["a", "b"],
            }
        ],
    }


def _full_tender_minimal() -> dict[str, Any]:
    """A sparse tender: only a couple of consumed fields present, lots of gaps."""
    return {
        "id": "src-0002",
        "title": "Cleaning services",
        "country": "DE",
        "ot": {"integrity": 0.5},
        "buyers": [{"name": "Stadt Berlin"}],
        "description": "y" * 2000,
    }


ALL_TENDERS = [_full_tender_awarded(), _full_tender_minimal()]


# ---------------------------------------------------------------------------
# Consumer helpers — reduce each consumer to the semantics that matter.
# ---------------------------------------------------------------------------

def _strip_volatile(obj: Any) -> Any:
    """Recursively drop wall-clock fields (``retrievedAt``) so behaviour-equality
    isn't defeated by two calls landing in different seconds."""
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k != "retrievedAt"}
    if isinstance(obj, list):
        return [_strip_volatile(v) for v in obj]
    return obj


def _hit_semantics(item: dict[str, Any]) -> tuple[Any, ...]:
    hit = OpenTenderAdapter._tender_hit(item)
    # ``raw`` is a passthrough of the input (not a "consumed field"); everything
    # the adapter derives from the tender lives in the other attributes.
    return (
        hit.source_id,
        hit.hit_id,
        hit.kind,
        hit.name,
        hit.summary,
        dict(hit.identifiers),
        hit.is_stub,
    )


def _walk_semantics(item: dict[str, Any]) -> tuple[list[Any], list[Any]]:
    bodies = list(_walk_bodies(item))
    names = [b.get("name") for b in bodies]
    bridges = [
        _bridge_identifier(ident)
        for b in bodies
        for ident in (b.get("bodyIds") or [])
    ]
    return names, bridges


def _bundle_statements(item: dict[str, Any]) -> list[dict[str, Any]]:
    bundle = {
        "tender_id": item.get("persistentId") or item.get("id"),
        "tender": item,
    }
    return _strip_volatile(map_opentender(bundle).statements)


# ---------------------------------------------------------------------------
# Behaviour-equality: every consumed field survives projection.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("full", ALL_TENDERS, ids=["awarded", "minimal"])
def test_tender_hit_survives_projection(full: dict[str, Any]) -> None:
    projected = project_tender(full)
    assert _hit_semantics(projected) == _hit_semantics(full)


@pytest.mark.parametrize("full", ALL_TENDERS, ids=["awarded", "minimal"])
def test_walk_bodies_and_bridges_survive_projection(full: dict[str, Any]) -> None:
    projected = project_tender(full)
    assert _walk_semantics(projected) == _walk_semantics(full)


@pytest.mark.parametrize("full", ALL_TENDERS, ids=["awarded", "minimal"])
def test_map_opentender_survives_projection(full: dict[str, Any]) -> None:
    projected = project_tender(full)
    full_stmts = _bundle_statements(full)
    proj_stmts = _bundle_statements(projected)
    # Non-trivial: the awarded tender must actually produce statements, else the
    # equality is vacuous.
    if full.get("lots"):
        assert full_stmts, "expected the awarded tender to yield BODS statements"
    assert proj_stmts == full_stmts


def test_projection_actually_drops_noise() -> None:
    full = _full_tender_awarded()
    projected = project_tender(full)
    # Sanity: the projection is doing real work, not returning the input.
    assert "description" not in projected
    assert "cpvs" not in projected
    assert "documents" not in projected
    assert "amountWithVat" not in projected["lots"][0]["bids"][0]["price"]
    assert "email" not in projected["buyers"][0]
    assert "rawAddress" not in projected["buyers"][0]["address"]
    assert len(json.dumps(projected)) < len(json.dumps(full))
    # ot is kept whole (integrity + transparency both preserved).
    assert projected["ot"] == full["ot"]


# ---------------------------------------------------------------------------
# Idempotency: projecting a projected record is a no-op.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("full", ALL_TENDERS, ids=["awarded", "minimal"])
def test_projection_is_idempotent(full: dict[str, Any]) -> None:
    once = project_tender(full)
    twice = project_tender(once)
    assert twice == once


def test_projection_does_not_mutate_input() -> None:
    full = _full_tender_awarded()
    snapshot = copy.deepcopy(full)
    project_tender(full)
    assert full == snapshot


# ---------------------------------------------------------------------------
# End-to-end: slim a synthetic fixture DB built via the extractor's own schema.
# ---------------------------------------------------------------------------

def _build_full_fixture_db(path: Path, tenders: list[dict[str, Any]]) -> None:
    """Build a fixture DB with FULL-FAT blobs via the extractor's schema path."""
    conn = sqlite3.connect(str(path))
    conn.executescript(extract_opentender._DDL)
    cur = conn.cursor()
    for tender in tenders:
        assert extract_opentender._insert_tender(cur, tender, slim=False)
    conn.commit()
    conn.execute("ANALYZE")
    conn.commit()
    conn.close()
    extract_opentender.finalise_db(path)


def _data_bytes(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM tenders"
        ).fetchone()[0]
    finally:
        conn.close()


def _integrity_ok(path: Path) -> bool:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_slim_db_end_to_end(tmp_path: Path) -> None:
    db = tmp_path / "opentender.db"
    # Repeat the tenders so the data column is a meaningful fraction of the file.
    tenders = []
    for i in range(40):
        t = _full_tender_awarded()
        t["persistentId"] = f"UK-tender-{i:04d}"
        tenders.append(t)
    _build_full_fixture_db(db, tenders)

    before_data = _data_bytes(db)
    before_body_ids = sqlite3.connect(str(db)).execute(
        "SELECT COUNT(*) FROM body_ids"
    ).fetchone()[0]

    slim_opentender.slim_db(db)

    # 1. The data column shrank.
    after_data = _data_bytes(db)
    assert after_data < before_data

    # 2. The DB still passes integrity_check.
    assert _integrity_ok(db)

    # 3. Sidecar tables are untouched (slim only rewrites tenders.data).
    after_body_ids = sqlite3.connect(str(db)).execute(
        "SELECT COUNT(*) FROM body_ids"
    ).fetchone()[0]
    assert after_body_ids == before_body_ids

    # 4. Consumed-field queries still work: fetch a slimmed blob exactly as the
    #    adapter does and run the real consumers — output matches the full record.
    full = _full_tender_awarded()
    full["persistentId"] = "UK-tender-0000"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT data FROM tenders WHERE persistent_id = ?", ("UK-tender-0000",)
    ).fetchone()
    conn.close()
    stored = json.loads(row["data"])
    assert _hit_semantics(stored) == _hit_semantics(full)
    assert _bundle_statements(stored) == _bundle_statements(full)

    # 5. Idempotent end-to-end: re-running leaves every tender blob byte-for-byte
    #    unchanged (projecting a projected record is a no-op). The file SHA is not
    #    asserted equal — VACUUM INTO is not guaranteed byte-deterministic across
    #    runs (SQLite freelist/rowid state) — but the payload it stores is.
    def _all_blobs() -> dict[str, str]:
        conn = sqlite3.connect(str(db))
        try:
            return dict(conn.execute("SELECT persistent_id, data FROM tenders"))
        finally:
            conn.close()

    blobs_before = _all_blobs()
    slim_opentender.slim_db(db)
    assert _all_blobs() == blobs_before
    assert _integrity_ok(db)
