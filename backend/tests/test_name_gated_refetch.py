"""A register's name gate holds through the deepen and count passes.

Found by the post-deploy tester on 12 Nov 2026 (Notion: "Pennsylvania: a
refused filing number still puts that business's officers into the lookup").
EVERETT CASH MUTUAL INSURANCE CO. files ``114497`` under RA000632; the
Department of State's ``114497`` carries another name, so ``pa_dos``'s
dispatch answered with the name-gate note card. The deepen pass then fetched
the same number **without** the legal name, the gate never ran, and the
lookup carried 31 ``pa_dos`` statements — fifteen people who reached the
related-party screens (RELATED_PEP, RELATED_DEBARMENT) and the verdict.

Every register that gates on a name (``lookup_pass_legal_name``) had the same
shape, so the fix is in the pipeline: every re-fetch of a dispatched hit
passes the name the dispatch passed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers import lookup as L
from opencheck.sources import REGISTRY

_FIX = Path(__file__).parent / "fixtures" / "pa_dos"
_LEI = "254900I1V8BWQCAENQ27"
_NUMBER = "3039985"  # COMCAST CORPORATION on the Department's list


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _cache_the_department_rows() -> None:
    """The Department's answer for the number, as a cached read (no network)."""
    rows = json.loads((_FIX / "filing_0003039985.json").read_text(encoding="utf-8"))
    REGISTRY["pa_dos"]._cache.put(
        f"pa_dos/filing/{_NUMBER}", {"rows": rows, "data_cut": "2026-11-03"}
    )


def _gleif_answers(monkeypatch, name: str) -> None:
    """GLEIF's live record, as the adapter returns it, filing the number
    under RA000632 — so the pipeline dispatches ``pa_dos`` with the name."""
    bundle = {
        "source_id": "gleif",
        "lei": _LEI,
        "record": {
            "type": "lei-records",
            "id": _LEI,
            "attributes": {
                "lei": _LEI,
                "entity": {
                    "legalName": {"name": name, "language": "en"},
                    "jurisdiction": "US-PA",
                    "registeredAs": _NUMBER,
                    "registeredAt": {"id": "RA000632"},
                    "status": "ACTIVE",
                    "legalAddress": {"country": "US", "region": "US-PA"},
                },
                "registration": {"status": "ISSUED"},
            },
        },
    }

    async def fetch(lei: str, **_: Any) -> dict[str, Any]:
        return dict(bundle)

    monkeypatch.setattr(REGISTRY["gleif"], "fetch", fetch)


def _pa_statements(body: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        s for s in body["bods"]
        if (s.get("source") or {}).get("opencheckSourceId") == "pa_dos"
    ]


def test_a_refused_filing_number_maps_nothing_into_the_lookup(monkeypatch) -> None:
    _cache_the_department_rows()
    _gleif_answers(monkeypatch, "EVERETT CASH MUTUAL INSURANCE CO.")

    body = TestClient(app).get("/lookup", params={"lei": _LEI}).json()

    (hit,) = [h for h in body["hits"] if h["source_id"] == "pa_dos"]
    assert hit["raw"]["name_mismatch"] is True
    assert _pa_statements(body) == []
    # No Comcast officer reaches a screen or a signal.
    for signal in body["risk_signals"]:
        assert signal.get("source_id") != "pa_dos"
    assert "COMCAST" not in json.dumps(body["bods"]).upper()


def test_an_agreeing_name_still_maps_the_record(monkeypatch) -> None:
    # The control: the same number with the register's own name is mapped.
    _cache_the_department_rows()
    _gleif_answers(monkeypatch, "COMCAST CORPORATION")

    body = TestClient(app).get("/lookup", params={"lei": _LEI}).json()

    statements = _pa_statements(body)
    assert any(s["recordType"] == "person" for s in statements)
    (company,) = [
        s for s in statements
        if s["recordType"] == "entity"
        and s["recordDetails"]["name"] == "COMCAST CORPORATION"
    ]
    assert company["recordDetails"]["jurisdiction"]["code"] == "US-PA"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_id",
    sorted(sid for sid, a in REGISTRY.items() if a.lookup_pass_legal_name),
)
async def test_every_refetch_hands_a_name_gated_register_the_name(
    source_id: str, monkeypatch
) -> None:
    adapter = REGISTRY[source_id]
    seen: list[dict[str, Any]] = []

    async def fake_fetch(hit_id: str, **kwargs: Any) -> dict[str, Any]:
        seen.append(kwargs)
        return {"is_stub": True}

    monkeypatch.setattr(adapter, "fetch", fake_fetch)
    monkeypatch.setattr(L, "_bods_data_override", lambda *a, **k: None)
    await L._safe_deepen(source_id, "1", subject=_LEI, legal_name="Some Name Ltd")
    await L._count_only(source_id, "1", legal_name="Some Name Ltd")
    assert [kw.get("legal_name") for kw in seen] == ["Some Name Ltd"] * 2


@pytest.mark.asyncio
async def test_a_source_that_takes_no_name_is_given_none(monkeypatch) -> None:
    adapter = REGISTRY["opensanctions"]
    assert not adapter.lookup_pass_legal_name
    seen: list[dict[str, Any]] = []

    async def fake_fetch(hit_id: str, **kwargs: Any) -> dict[str, Any]:
        seen.append(kwargs)
        return {"is_stub": True}

    monkeypatch.setattr(adapter, "fetch", fake_fetch)
    await L._safe_deepen("opensanctions", "x", legal_name="Anything")
    await L._count_only("opensanctions", "x", legal_name="Anything")
    assert seen == [{}, {}]


# ---------------------------------------------------------------------------
# The register-anchored lookup does not infer a country for a foreign
# registration (the US-PA jurisdiction ticket, 12 Nov 2026: SHEETZ AVIATION,
# INC., a Foreign Business Corporation, answered top-level "US").
# ---------------------------------------------------------------------------


def test_only_the_us_state_registers_record_foreign_entities() -> None:
    from opencheck.register_hops import hop_schemes

    foreign = {s for s in hop_schemes() if L._registers_foreign_entities(s)}
    assert foreign == {"US-CT", "US-DC", "US-FL", "US-NY", "US-PA"}


def test_a_foreign_registration_gets_no_country_from_the_scheme() -> None:
    foreign = [{"recordType": "entity", "recordDetails": {"name": "SHEETZ AVIATION, INC."}}]
    assert L._subject_name_and_jurisdiction(
        foreign, {}, "US", infer_country=False
    ) == ("SHEETZ AVIATION, INC.", "")
    domestic = [{"recordType": "entity", "recordDetails": {
        "name": "COMCAST CORPORATION",
        "jurisdiction": {"name": "Pennsylvania, United States", "code": "US-PA"},
    }}]
    assert L._subject_name_and_jurisdiction(
        domestic, {}, "US", infer_country=False
    ) == ("COMCAST CORPORATION", "US-PA")
    # No statement at all: the scheme's country still stands in, as before.
    assert L._subject_name_and_jurisdiction([], {}, "US", infer_country=False)[1] == "US"
    # A national register keeps the fallback whatever its statement says.
    assert L._subject_name_and_jurisdiction(foreign, {}, "GB")[1] == "GB"
