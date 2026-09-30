"""Phase 267 — a statement names its source by id, and a miss is loud.

Until Phase 267 the licence lookups (RDF ``bods:license``, Senzing
``DATA_LICENSE`` / ``ATTRIBUTION``, a FullCheck network's ``LICENSES.md``)
recovered the adapter id by matching ``source.description`` back against the
registry, and returned an empty set on a miss — silently. Production showed
the miss: Shell's export on 30 Sept 2026 carried an OECD-UNSD MEIP statement
whose OECD-written description matched nothing, and its Senzing record
shipped without a licence.
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from opencheck import signalstats
from opencheck.app import app
from opencheck.bods import source_ids
from opencheck.bods.mapper import map_meip
from opencheck.bods.rdf import _license_literal_for
from opencheck.bods.senzing import map_to_senzing
from opencheck.bods.source_ids import (
    SOURCE_ID_KEY,
    contributing_source_ids,
    source_id_of,
    source_ids_of,
    unresolved,
)
from opencheck.bods.statements import SOURCE_NAMES, _source_block, make_entity_statement
from opencheck.routers.export import _network_source_ids
from opencheck.sources import REGISTRY

#: The description the OECD writes on every MEIP statement (production,
#: 30 Sept 2026).
OECD_DESCRIPTION = "OECD-UNSD Multinational Enterprise Information Platform (MEIP), 'Group Register' sheet."


@pytest.fixture(autouse=True)
def _clean():
    signalstats.reset()
    source_ids._reset_warnings()
    yield
    signalstats.reset()
    source_ids._reset_warnings()


def _entity(source_id: str, name: str = "ACME LTD") -> dict:
    return make_entity_statement(
        local_id=f"{source_id}-e",
        name=name,
        source_id=source_id,
    )


def _meip_statement() -> dict:
    return {
        "statementId": "3f53626b0bda4308dc42578036405d89d48054b18d6d32f8cf9311631b0e0a35",
        "statementDate": "2024-12-31",
        "recordId": "meip-entity-4",
        "recordType": "entity",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": "BETA SA",
        },
        "source": {"type": ["thirdParty"], "description": OECD_DESCRIPTION},
    }


# ---------------------------------------------------------------------------
# The round trip — the check that would have caught the whole class
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sid", sorted(REGISTRY))
def test_every_registered_source_survives_the_round_trip(sid: str) -> None:
    block = _source_block(sid, None)
    assert block[SOURCE_ID_KEY] == sid
    assert source_id_of({"source": block}) == sid
    # And through the description alone, as a statement stamped before
    # Phase 267 carries it.
    legacy = {k: v for k, v in block.items() if k != SOURCE_ID_KEY}
    assert source_id_of({"source": legacy}) == sid


def test_every_registered_source_has_a_distinct_display_name() -> None:
    """Two sources sharing a description would make the fallback ambiguous."""
    names = [SOURCE_NAMES.get(sid, sid) for sid in REGISTRY]
    assert len(names) == len(set(names))


def test_the_id_survives_a_renamed_display_name(monkeypatch) -> None:
    """The failure the ticket names: edit a display name, and every statement
    already stamped with the old one must still resolve."""
    stamped = _entity("companies_house")
    monkeypatch.setitem(SOURCE_NAMES, "companies_house", "Companies House (UK)")
    source_ids.description_index.cache_clear()
    try:
        assert stamped["source"]["description"] == "UK Companies House"
        assert source_ids_of(stamped) == {"companies_house"}
    finally:
        monkeypatch.undo()
        source_ids.description_index.cache_clear()


def test_the_stamped_id_wins_over_the_description() -> None:
    stmt = {"source": {"description": "GLEIF", SOURCE_ID_KEY: "opensanctions"}}
    assert source_id_of(stmt) == "opensanctions"


def test_an_unregistered_stamped_id_falls_back_to_the_description() -> None:
    """``bods_gleif`` is deliberately unregistered, and the licence tables read
    the REGISTRY — so an id they cannot price is not accepted as an answer."""
    stmt = {"source": {"description": "GLEIF", SOURCE_ID_KEY: "bods_gleif"}}
    assert source_id_of(stmt) == "gleif"


# ---------------------------------------------------------------------------
# MEIP: the miss that was live in production
# ---------------------------------------------------------------------------


def test_map_meip_stamps_the_oecd_statements() -> None:
    raw = {"bods_statements": [_meip_statement()]}
    (out,) = list(map_meip(raw))
    assert out["source"][SOURCE_ID_KEY] == "meip"
    assert out["source"]["description"] == OECD_DESCRIPTION
    assert source_id_of(out) == "meip"
    assert SOURCE_ID_KEY not in raw["bods_statements"][0]["source"]


def test_an_oecd_statement_saved_before_phase_267_still_resolves() -> None:
    assert source_id_of(_meip_statement()) == "meip"


def test_the_meip_senzing_record_carries_a_licence() -> None:
    (out,) = list(map_meip({"bods_statements": [_meip_statement()]}))
    (record,) = map_to_senzing([out])
    assert record["DATA_LICENSE"] == "OECD-Terms"
    assert "ATTRIBUTION" in record


def test_the_meip_statement_gets_a_licence_triple() -> None:
    (out,) = list(map_meip({"bods_statements": [_meip_statement()]}))
    assert _license_literal_for(out) is not None


def test_the_network_licence_list_names_meip() -> None:
    (out,) = list(map_meip({"bods_statements": [_meip_statement()]}))
    bods = [_entity("gleif", "SHELL PLC"), out]
    assert _network_source_ids(bods) == ["gleif", "meip"]
    assert contributing_source_ids(bods) == ["gleif", "meip"]


def test_the_network_export_licences_md_lists_the_oecd() -> None:
    (out,) = list(map_meip({"bods_statements": [_meip_statement()]}))
    client = TestClient(app)
    r = client.post(
        "/export-network",
        json={"bods": [_entity("gleif", "SHELL PLC"), out], "format": "zip", "slug": "net"},
    )
    assert r.status_code == 200
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        (name,) = [n for n in zf.namelist() if n.endswith("LICENSES.md")]
        md = zf.read(name).decode("utf-8")
    assert "`meip`" in md and "OECD-Terms" in md


# ---------------------------------------------------------------------------
# A miss is loud
# ---------------------------------------------------------------------------


def test_an_unrecognised_source_is_logged_once(caplog) -> None:
    stmt = {"source": {"description": "Some Register Nobody Mapped"}}
    with caplog.at_level(logging.WARNING, logger="opencheck.bods.source_ids"):
        assert source_ids_of(stmt) == set()
        assert source_ids_of(stmt) == set()
    warnings = [r for r in caplog.records if "Some Register Nobody Mapped" in r.getMessage()]
    assert len(warnings) == 1


def test_unresolved_counts_by_reason() -> None:
    bods = [
        _entity("gleif"),
        {"statementId": "x"},  # no source block at all
        {"source": {"description": "Some Register Nobody Mapped"}},
    ]
    assert unresolved(bods) == {"no_source": 1, "unrecognised": 1}


def test_the_export_routes_count_unresolved_statements_on_signalstats() -> None:
    client = TestClient(app)
    bods = [_entity("gleif"), {"statementId": "y", "source": {"description": "Some Register Nobody Mapped"}}]
    r = client.post("/export-network", json={"bods": bods, "format": "json", "slug": "net"})
    assert r.status_code == 200
    stats = client.get("/signalstats").json()
    assert stats["unresolved_sources_total"] == 1
    assert stats["unresolved_sources"] == {"export_network|unrecognised": 1}


def test_a_fully_attributed_export_counts_nothing() -> None:
    client = TestClient(app)
    r = client.post("/export-network", json={"bods": [_entity("gleif")], "format": "json"})
    assert r.status_code == 200
    stats = client.get("/signalstats").json()
    assert stats["unresolved_sources_total"] == 0
    assert stats["unresolved_sources"] == {}


def test_the_counter_takes_only_its_closed_vocabularies() -> None:
    signalstats.record_unresolved_sources("export", {"unrecognised": 2, "Some Register": 5})
    signalstats.record_unresolved_sources("a secret LEI 9999000000000000ZZ99", {"no_source": 1})
    assert signalstats.stats()["unresolved_sources"] == {
        "export|unrecognised": 2,
        "other|no_source": 1,
    }


# ---------------------------------------------------------------------------
# The extension field is valid BODS
# ---------------------------------------------------------------------------


def test_a_stamped_statement_still_validates_under_libcovebods() -> None:
    pytest.importorskip("libcovebods")
    from tests.test_bods_libcovebods import validate_bods_statements

    report = validate_bods_statements([_entity("companies_house")])
    assert report["json_errors"] == [], report["json_errors"]
    assert report["additional_errors"] == [], report["additional_errors"]
