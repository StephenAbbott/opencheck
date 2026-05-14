"""Tests for the OpenTender (DIGIWHIST) adapter + BODS mapper."""

from __future__ import annotations

import json
import sqlite3
import textwrap
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import map_opentender, validate_shape
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.opentender import (
    OpenTenderAdapter,
    _bridge_identifier,
    _slug,
)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENTENDER_DB_FILE", raising=False)
    monkeypatch.delenv("OPENTENDER_S3_URL", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _seed(tmp_path: Path, key: str, payload: dict) -> None:
    target = tmp_path / "cache" / "demos" / f"{key}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"_cached_at": 0, "payload": payload}))


# ------------------------------------------------------------------
# SQLite fixture helpers
# ------------------------------------------------------------------

_DDL = textwrap.dedent("""
    CREATE TABLE tenders (
        persistent_id TEXT PRIMARY KEY,
        source_id TEXT,
        country TEXT NOT NULL,
        title TEXT,
        is_awarded INTEGER DEFAULT 0,
        award_date TEXT,
        integrity_score REAL,
        transparency_score REAL,
        data TEXT NOT NULL
    );
    CREATE VIRTUAL TABLE body_names_fts
    USING fts5(persistent_id UNINDEXED, name, role,
               tokenize = "unicode61 remove_diacritics 1");
    CREATE TABLE body_ids (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        persistent_id TEXT NOT NULL,
        id_type TEXT, id_scope TEXT, id_value TEXT
    );
    CREATE INDEX idx_body_ids_lookup ON body_ids (id_type, id_value);
""")


def _make_db(tmp_path: Path, tenders: list[dict]) -> Path:
    """Write a minimal opentender.db with the supplied tender records."""
    db_path = tmp_path / "opentender.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_DDL)
    cur = conn.cursor()
    for tender in tenders:
        pid = tender.get("persistentId") or tender.get("id") or ""
        country = tender.get("country", "GB")
        cur.execute(
            "INSERT OR REPLACE INTO tenders "
            "(persistent_id, source_id, country, title, is_awarded, award_date, "
            "integrity_score, transparency_score, data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pid,
                tender.get("id"),
                country,
                tender.get("title"),
                1 if tender.get("isAwarded") else 0,
                tender.get("awardDecisionDate"),
                (tender.get("ot") or {}).get("integrity"),
                (tender.get("ot") or {}).get("transparency"),
                json.dumps(tender),
            ),
        )
        # FTS entries for buyers + bidders.
        for body in tender.get("buyers") or []:
            name = (body.get("name") or "").strip()
            if name:
                cur.execute(
                    "INSERT INTO body_names_fts (persistent_id, name, role) VALUES (?,?,?)",
                    (pid, name, "buyer"),
                )
        for lot in tender.get("lots") or []:
            for bid in lot.get("bids") or []:
                for body in bid.get("bidders") or []:
                    name = (body.get("name") or "").strip()
                    if name:
                        cur.execute(
                            "INSERT INTO body_names_fts (persistent_id, name, role) VALUES (?,?,?)",
                            (pid, name, "bidder"),
                        )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------
# Adapter — no DB (fixture / stub mode)
# ---------------------------------------------------------------------


def test_adapter_is_registered() -> None:
    assert "opentender" in REGISTRY
    info = REGISTRY["opentender"].info
    assert info.license == "CC-BY-NC-SA-4.0"
    assert SearchKind.ENTITY in info.supports
    # Without OPENTENDER_DB_FILE the adapter reports live_available=False.
    assert info.live_available is False


def test_adapter_live_available_when_db_configured(
    monkeypatch, tmp_path: Path
) -> None:
    """live_available becomes True once OPENTENDER_DB_FILE points at an existing DB."""
    db_path = _make_db(tmp_path, [])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()
    info = REGISTRY["opentender"].info
    assert info.live_available is True


async def test_search_rejects_person_kind() -> None:
    adapter = OpenTenderAdapter()
    assert await adapter.search("acme", SearchKind.PERSON) == []


async def test_search_returns_stub_when_no_fixture(tmp_path: Path) -> None:
    adapter = OpenTenderAdapter()
    hits = await adapter.search("nothing-here", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


async def test_search_serves_demo_fixture(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        f"opentender/search/{_slug('Acme')}",
        {
            "tenders": [
                {
                    "id": "OT-XX-1",
                    "title": "Demo tender",
                    "country": "DE",
                    "buyers": [
                        {
                            "name": "Demo Authority",
                            "bodyIds": [
                                {"id": "DE111111111", "type": "VAT", "scope": "EU"}
                            ],
                        }
                    ],
                }
            ]
        },
    )
    adapter = OpenTenderAdapter()
    hits = await adapter.search("Acme", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is False
    assert hits[0].name == "Demo tender"
    # VAT identifier was bridged through to a strong-bridge key.
    assert hits[0].identifiers["vat"] == "DE111111111"
    assert hits[0].identifiers["opentender_id"] == "OT-XX-1"


async def test_fetch_returns_stub_when_no_fixture() -> None:
    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("OT-missing")
    assert bundle["is_stub"] is True


async def test_fetch_serves_demo_fixture(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        f"opentender/tender/{_slug('OT-XX-1')}",
        {"id": "OT-XX-1", "title": "Demo tender"},
    )
    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("OT-XX-1")
    assert bundle["tender_id"] == "OT-XX-1"
    assert bundle["tender"]["title"] == "Demo tender"


# ---------------------------------------------------------------------
# Adapter — SQLite live mode
# ---------------------------------------------------------------------

_UK_TENDER = {
    "id": "cf-source-001",
    "persistentId": "UK_abc123def456",
    "title": "Road maintenance framework",
    "country": "UK",
    "isAwarded": True,
    "awardDecisionDate": "2024-06-01",
    "ot": {"integrity": 0.82, "transparency": 0.74},
    "buyers": [
        {
            "name": "Highways England",
            "bodyIds": [],
        }
    ],
    "lots": [
        {
            "lotId": "L1",
            "bids": [
                {
                    "isWinning": True,
                    "bidders": [
                        {
                            "name": "Balfour Beatty Ltd",
                            "bodyIds": [
                                {
                                    "id": "395826",
                                    "type": "ORGANIZATION_ID",
                                    "scope": "UNKNOWN",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ],
}


async def test_db_search_finds_buyer_by_name(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("Highways England", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].hit_id == "UK_abc123def456"
    assert hits[0].name == "Road maintenance framework"


async def test_db_search_finds_bidder_by_name(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("Balfour Beatty", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].hit_id == "UK_abc123def456"


async def test_db_fetch_returns_tender(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("UK_abc123def456")
    assert bundle["tender_id"] == "UK_abc123def456"
    assert bundle["tender"]["title"] == "Road maintenance framework"


async def test_db_fetch_stub_when_not_found(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("nonexistent")
    assert bundle["is_stub"] is True


async def test_db_search_no_results_returns_empty(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("zzznomatch9999", SearchKind.ENTITY)
    assert hits == []


# ---------------------------------------------------------------------
# _bridge_identifier
# ---------------------------------------------------------------------


def test_bridge_vat() -> None:
    key, val = _bridge_identifier({"type": "VAT", "scope": "EU", "id": "DE123456789"})
    assert key == "vat"
    assert val == "DE123456789"


def test_bridge_organization_id_gb_scope() -> None:
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "GB", "id": "06426844"})
    assert key == "gb_coh"
    assert val == "06426844"


def test_bridge_organization_id_unknown_scope() -> None:
    """DIGIWHIST UK data publishes ORGANIZATION_ID with scope UNKNOWN — should bridge to gb_coh."""
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "UNKNOWN", "id": "395826"})
    assert key == "gb_coh"
    assert val == "00395826"  # zero-padded to 8 digits


def test_bridge_organization_id_unknown_scope_already_8_digits() -> None:
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "UNKNOWN", "id": "06426844"})
    assert key == "gb_coh"
    assert val == "06426844"


def test_bridge_organization_id_non_gb_scope_not_bridged() -> None:
    """ORGANIZATION_ID with a non-UK scope should not be bridged to gb_coh."""
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "DE", "id": "12345678"})
    assert key is None


def test_bridge_lei_detected_by_shape() -> None:
    lei = "2138003EK6PNMJUVGA51"
    key, val = _bridge_identifier({"type": "ETALON_ID", "scope": "GLOBAL", "id": lei})
    assert key == "lei"
    assert val == lei.upper()


def test_bridge_header_ico() -> None:
    key, val = _bridge_identifier({"type": "HEADER_ICO", "scope": "CZ", "id": "12345678"})
    assert key == "registration_number"
    assert val == "12345678"


def test_bridge_unknown_type_returns_none() -> None:
    key, val = _bridge_identifier({"type": "SOME_UNKNOWN_TYPE", "scope": "", "id": "abc"})
    assert key is None
    assert val is None


# ---------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------


def _sample_tender() -> dict:
    return {
        "id": "OT-DE-2024-1",
        "title": "Crude oil framework",
        "country": "DE",
        "isAwarded": True,
        "awardDecisionDate": "2024-03-15",
        "buyers": [
            {
                "name": "Bundesamt für Energie",
                "address": {"city": "Berlin", "country": "DE"},
                "bodyIds": [
                    {"id": "DE324523002", "type": "VAT", "scope": "EU"}
                ],
            }
        ],
        "lots": [
            {
                "lotId": "L1",
                "awardDecisionDate": "2024-03-15",
                "bids": [
                    {
                        "isWinning": True,
                        "price": {"netAmount": 12500000, "currency": "EUR"},
                        "bidders": [
                            {
                                "name": "Acme Trading GmbH",
                                "address": {"country": "DE"},
                                "bodyIds": [
                                    {"id": "DE123456789", "type": "VAT", "scope": "EU"}
                                ],
                            }
                        ],
                    },
                    {
                        "isWinning": False,
                        "bidders": [
                            {
                                "name": "Loser Trading Ltd",
                                "bodyIds": [
                                    {"id": "12345678", "type": "ORGANIZATION_ID", "scope": "GB"}
                                ],
                            }
                        ],
                    },
                ],
            }
        ],
    }


def test_map_opentender_emits_buyer_and_bidder_entity_statements() -> None:
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": _sample_tender()})
    statements = list(bundle)

    entities = [s for s in statements if s["recordType"] == "entity"]
    names = sorted(s["recordDetails"]["name"] for s in entities)
    assert "Bundesamt für Energie" in names
    assert "Acme Trading GmbH" in names
    # Losing bidder is also surfaced (so reconciler can bridge them).
    assert "Loser Trading Ltd" in names


def test_map_opentender_emits_award_relationship_only_for_winning_bid() -> None:
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": _sample_tender()})
    statements = list(bundle)

    rels = [s for s in statements if s["recordType"] == "relationship"]
    # One winner × one buyer = one relationship; losing bidder gets none.
    assert len(rels) == 1
    interest = rels[0]["recordDetails"]["interests"][0]
    assert interest["type"] == "otherInfluenceOrControl"
    assert interest["beneficialOwnershipOrControl"] is False
    assert "12500000" in interest["details"]
    assert interest["startDate"] == "2024-03-15"


def test_map_opentender_bridges_gb_organization_id_to_gb_coh() -> None:
    """A GB-scoped ORGANIZATION_ID lands as the GB-ORG scheme identifier."""
    bundle = map_opentender({"tender_id": "OT-GB-1", "tender": {
        "id": "OT-GB-1",
        "buyers": [{"name": "Crown Commercial Service", "bodyIds": [
            {"id": "06426844", "type": "ORGANIZATION_ID", "scope": "GB"},
        ]}],
        "lots": [],
    }})
    entities = [s for s in bundle if s["recordType"] == "entity"]
    schemes = {
        i["scheme"]: i["id"]
        for s in entities
        for i in s["recordDetails"]["identifiers"]
    }
    assert schemes.get("ORG") is None  # Promoted to GB-ORG instead.
    assert schemes.get("GB-ORG") == "06426844"


def test_map_opentender_bridges_unknown_scope_organization_id_to_gb_org() -> None:
    """ORGANIZATION_ID with scope UNKNOWN (UK DIGIWHIST pattern) → GB-ORG, zero-padded."""
    bundle = map_opentender({"tender_id": "OT-UK-1", "tender": {
        "id": "OT-UK-1",
        "buyers": [{"name": "Highways England", "bodyIds": [
            {"id": "395826", "type": "ORGANIZATION_ID", "scope": "UNKNOWN"},
        ]}],
        "lots": [],
    }})
    entities = [s for s in bundle if s["recordType"] == "entity"]
    schemes = {
        i["scheme"]: i["id"]
        for s in entities
        for i in s["recordDetails"]["identifiers"]
    }
    assert schemes.get("GB-ORG") == "00395826"


def test_map_opentender_uk_country_maps_to_gb_jurisdiction() -> None:
    """DIGIWHIST country code 'UK' must map to ISO 'GB' in BODS jurisdiction."""
    bundle = map_opentender({"tender_id": "OT-UK-2", "tender": {
        "id": "OT-UK-2",
        "country": "UK",
        "buyers": [{"name": "NHS England", "address": {"country": "UK"}, "bodyIds": []}],
        "lots": [],
    }})
    entities = [s for s in bundle if s["recordType"] == "entity"]
    assert entities, "Expected at least one entity statement"
    jurisdiction = entities[0]["recordDetails"].get("jurisdiction")
    if jurisdiction:
        assert jurisdiction.get("code") == "GB", (
            f"Expected jurisdiction code 'GB', got {jurisdiction.get('code')!r}"
        )


def test_map_opentender_output_passes_bods_validation() -> None:
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": _sample_tender()})
    issues = validate_shape(list(bundle))
    assert issues == []


def test_map_opentender_handles_empty_bundle() -> None:
    assert list(map_opentender({})) == []


# ---------------------------------------------------------------------
# Endpoint integration
# ---------------------------------------------------------------------


def test_deepen_opentender_flags_nc_sa_license(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        f"opentender/tender/{_slug('OT-DE-2024-1')}",
        _sample_tender(),
    )
    client = TestClient(app)
    r = client.get(
        "/deepen", params={"source": "opentender", "hit_id": "OT-DE-2024-1"}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["license"] == "CC-BY-NC-SA-4.0"
    assert body["license_notice"] is not None
    assert "CC-BY-NC-SA-4.0" in body["license_notice"]
    # BODS shape made it through end-to-end.
    assert body["bods"], "no BODS statements emitted"
    assert any(s["recordType"] == "relationship" for s in body["bods"])
