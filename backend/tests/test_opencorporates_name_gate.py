"""OpenCorporates refuses a company number that names another company.

Found by the post-deploy tester on 16 Nov 2026 (Notion: "OpenCorporates shows
another company's record on American Foreign Policy Council's report, with the
LEI attached"). The lookup reaches OpenCorporates through GLEIF's
LEI-to-OpenCorporates mapping, which links American Foreign Policy Council
(``549300W96W2VKSMVDF81``, a LAPSED LEI) to ``us_dc/L21249``. OpenCorporates'
``us_dc/L21249`` is CONNIE-19 STREET LLC — the collision ``dlcp_dc`` already
name-gates. With no gate here, the report carried CONNIE-19's status, founding
date and officers against AFPC, the hit asserted AFPC's LEI, and
``RECORD_CONSISTENCY`` counted the wrong company as agreement.

Also pinned: the card's summary when OpenCorporates sends
``current_status: null`` (Equinor ASA read "OC no/923609016 · None", Notion
"OpenCorporates card shows "None" when the company has no status").
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from opencheck.routers import lookup as L
from opencheck.routers.lookup import _build_result_hit, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources.opencorporates import (
    _API_BASE,
    COVERAGE_NAME_MISMATCH,
    OpenCorporatesAdapter,
    names_agree,
    publishes_lei,
)

AFPC_LEI = "549300W96W2VKSMVDF81"
AFPC = "American Foreign Policy Council"
OCID = "us_dc/L21249"
COMPANY_URL = f"{_API_BASE}/companies/us_dc/L21249"
NETWORK_URL = f"{COMPANY_URL}/network"

CONNIE = {
    "name": "CONNIE-19 STREET LLC",
    "company_number": "L21249",
    "jurisdiction_code": "us_dc",
    "incorporation_date": "2004-11-24",
    "company_type": "Domestic Limited Liability Company",
    "current_status": "Active",
    "officers": [
        {"officer": {"name": "A PERSON", "position": "manager"}},
        {"officer": {"name": "ANOTHER PERSON", "position": "manager"}},
    ],
}


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCORPORATES_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The name rule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("legal_name", "company", "agrees"),
    [
        (AFPC, CONNIE, False),
        ("Equinor ASA", {"name": "EQUINOR ASA"}, True),
        ("Moody's Corporation", {"name": "MOODY'S CORPORATION"}, True),
        ("UNILEVER PLC", {"name": "UNILEVER PLC"}, True),
        # A renamed company agrees on a name OpenCorporates kept.
        (
            "Statoil ASA",
            {"name": "EQUINOR ASA", "previous_names": [{"company_name": "STATOIL ASA"}]},
            True,
        ),
        (
            "Acme Trading Ltd",
            {"name": "ZEBRA HOLDINGS LTD", "alternative_names": [{"company_name": "ACME TRADING"}]},
            True,
        ),
        # Nothing to compare: never refused.
        (None, CONNIE, True),
        (AFPC, {}, True),
        # Scripts that cannot be compared letter by letter: never refused.
        ("トヨタ自動車株式会社", {"name": "TOYOTA MOTOR CORPORATION"}, True),
        ("Toyota Motor Corporation", {"name": "トヨタ自動車株式会社"}, True),
    ],
)
def test_names_agree(legal_name: str | None, company: dict[str, Any], agrees: bool) -> None:
    assert names_agree(legal_name, company) is agrees


def test_the_lei_is_asserted_only_where_opencorporates_publishes_it() -> None:
    company = {"identifiers": [{"identifier": {"uid": AFPC_LEI, "identifier_system_code": "lei"}}]}
    assert publishes_lei(company, AFPC_LEI)
    assert not publishes_lei(CONNIE, AFPC_LEI)
    assert not publishes_lei(company, None)


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


@respx.mock
async def test_a_mismatched_number_is_refused_and_nothing_else_is_asked() -> None:
    respx.get(COMPANY_URL).mock(
        return_value=httpx.Response(200, json={"results": {"company": CONNIE}})
    )
    network = respx.get(NETWORK_URL).mock(return_value=httpx.Response(200, json={}))

    bundle = await OpenCorporatesAdapter().fetch(OCID, legal_name=AFPC)

    assert bundle["name_mismatch"] is True
    assert bundle["not_found"] is True
    assert bundle["is_stub"] is False
    assert bundle["registered_name"] == "CONNIE-19 STREET LLC"
    assert bundle["coverage_note"] == COVERAGE_NAME_MISMATCH
    assert "company" not in bundle and "officers" not in bundle
    assert network.call_count == 0


@respx.mock
async def test_without_a_name_the_record_is_returned_as_before() -> None:
    respx.get(COMPANY_URL).mock(
        return_value=httpx.Response(200, json={"results": {"company": CONNIE}})
    )
    respx.get(NETWORK_URL).mock(return_value=httpx.Response(404))
    bundle = await OpenCorporatesAdapter().fetch(OCID)
    assert bundle["company"]["name"] == "CONNIE-19 STREET LLC"
    assert not bundle.get("name_mismatch")


# ---------------------------------------------------------------------------
# The pipeline: a note card, no identifiers, nothing mapped
# ---------------------------------------------------------------------------


def _ctx(lei: str = AFPC_LEI, name: str = AFPC, ocid: str = OCID) -> _LookupCtx:
    ctx = _LookupCtx(lei=lei, legal_name=name)
    ctx.ocid = ocid
    return ctx


def test_the_mismatch_is_a_note_card_with_no_identifier() -> None:
    bundle = {
        "source_id": "opencorporates", "hit_id": OCID, "ocid": OCID,
        "is_stub": False, "not_found": True, "name_mismatch": True,
        "registered_name": "CONNIE-19 STREET LLC",
        "coverage_note": COVERAGE_NAME_MISMATCH,
    }
    hit = _build_result_hit("opencorporates", bundle, _ctx())
    assert hit is not None
    assert hit.name == AFPC
    assert hit.identifiers == {}
    assert hit.raw["not_found"] is True and hit.raw["name_mismatch"] is True
    assert "None" not in hit.summary
    assert "CONNIE-19 STREET LLC" in (hit.finding or "")
    assert "differently named company" in (hit.finding or "")


async def test_the_deepen_pass_maps_nothing_for_a_refused_number(monkeypatch) -> None:
    adapter = REGISTRY["opencorporates"]
    seen: list[dict[str, Any]] = []

    async def fake_get(path: str, *, cache_key: str) -> dict[str, Any]:
        return {"results": {"company": CONNIE}}

    real_fetch = adapter.fetch

    async def spy(hit_id: str, **kwargs: Any) -> dict[str, Any]:
        seen.append(kwargs)
        return await real_fetch(hit_id, **kwargs)

    monkeypatch.setattr(adapter, "_get", fake_get)
    monkeypatch.setattr(adapter, "fetch", spy)
    monkeypatch.setattr(L, "_bods_data_override", lambda *a, **k: None)

    out = await L._safe_deepen("opencorporates", OCID, subject=AFPC_LEI, legal_name=AFPC)
    assert seen == [{"legal_name": AFPC}]
    assert out is not None and out["bods"] == []


def test_a_status_is_shown_and_a_missing_one_is_not() -> None:
    ctx = _ctx(lei="OW6OFBNCKXC4US5C7523", name="Equinor ASA", ocid="no/923609016")
    company = {"name": "EQUINOR ASA", "current_status": None}
    hit = _build_result_hit("opencorporates", {"company": company, "ocid": ctx.ocid}, ctx)
    assert hit is not None
    assert hit.summary == "OC no/923609016"
    assert "lei" not in hit.identifiers
    assert hit.identifiers["ocid"] == "no/923609016"

    company = {"name": "UNILEVER PLC", "current_status": "Active"}
    ctx = _ctx(lei="549300MKFYEKVRWML317", name="UNILEVER PLC", ocid="gb/00041424")
    hit = _build_result_hit("opencorporates", {"company": company, "ocid": ctx.ocid}, ctx)
    assert hit is not None and hit.summary == "OC gb/00041424 · Active"
