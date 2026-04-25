"""Tests for the OpenTender (DIGIWHIST) adapter + BODS mapper."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import map_opentender, validate_shape
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.opentender import OpenTenderAdapter, _slug


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _seed(tmp_path: Path, key: str, payload: dict) -> None:
    target = tmp_path / "cache" / "demos" / f"{key}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"_cached_at": 0, "payload": payload}))


# ---------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------


def test_adapter_is_registered() -> None:
    assert "opentender" in REGISTRY
    info = REGISTRY["opentender"].info
    assert info.license == "CC-BY-NC-SA-4.0"
    assert SearchKind.ENTITY in info.supports
    # Live mode is not yet wired — should report False even if
    # OPENCHECK_ALLOW_LIVE is true.
    assert info.live_available is False


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
    """A GB-scoped ORGANIZATION_ID lands as the GB-COH bridge identifier."""
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
