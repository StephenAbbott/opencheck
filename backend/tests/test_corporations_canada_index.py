"""The Canada index ticket (9 Nov 2026) — Corporations Canada read from a daily
index of the Federal Corporations open data first, then the ISED API.

Every index here is written by the **real builder**
(``scripts/build_corporations_canada_index.py``) from CSVs in the exact
published shape (a BOM, the eighteen-column English header, ISO dates) and
read back through the **real adapter** — the JAR / Cyprus rule. The API is
mocked with respx: the gateway, the keyless legacy endpoint and directors.
"""

from __future__ import annotations

import importlib.util
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods.mapper import map_corporations_canada
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_corporations_canada
from opencheck.routers.lookup import _build_derived, _build_result_hit, _LookupCtx
from opencheck.sources import corporations_canada as cc
from opencheck.sources.base import SearchKind
from opencheck.sources.corporations_canada import (
    CA_CORP_RA_CODE,
    COVERAGE_BN_UNRESOLVED,
    CorporationsCanadaAdapter,
    is_bn_filing,
    normalise_corp_id,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_corporations_canada_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_corporations_canada_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-11-08"
HEADER = ",".join(builder.HEADER)
GATEWAY = cc._API_BASE
LEGACY = cc.LEGACY_API_BASE

# Rows in the published shape, from the 8 Nov 2026 files.
ACTIVE_CBCA = [
    "8660115,835752437,MINDANGLER CAPITAL INC.,,Canada Business Corporations Act,Active,,2013-10-10,2026,2023-10-10,515 Legget Drive,Suite 800,Ottawa,ON,CA,K2K 3G4,1,12",
    "821080,100094283,AIRMEC CLIMATISATION LTEE,,Canada Business Corporations Act,Active,Active - Dissolution Pending (non-compliance),1979-02-26,2000,1999-11-03,7961  RUE VAUBAN,,ANJOU,QC,CA,H1J2V1,1,5",
    "358185,100117555,FCA Canada Inc.,FCA Canada Inc. (Société),Canada Business Corporations Act,Active,,1979-12-31,2026,2026-05-01,1 Riverside Drive West,,Windsor,ON,CA,N9A 5K3,1,15",
    # Shares a BN with an amalgamated predecessor: the active one wins.
    "4111117,100004092,NEWCO HOLDINGS INC.,,Canada Business Corporations Act,Active,,2002-03-01,2026,2026-03-01,1 Main St,,Toronto,ON,CA,M5V 1A1,1,10",
]
ACTIVE_OTHER = [
    "456926,118965557,The Huntsman Marine Science Centre,,Canada Not-for-profit Corporations Act,Active,,2013-01-29,2026,2025-10-29,1 LOWER CAMPUS ROAD,,ST. ANDREWS,NB,CA,E5B 2L7,5,12",
]
INACTIVE_CBCA = [
    "7286139,837253251,7286139 CANADA LTD.,,Canada Business Corporations Act,Inactive,Inactive - Amalgamated,2009-11-26,2025,2025-11-26,\"45, Queen Street\",,Sherbrooke,QC,CA,J1M 1J2,1,10",
    "8593094,846556835,Stellar Point Productions Inc.,,Canada Business Corporations Act,Dissolved,Dissolved by the corporation,2013-07-29,,,5 Carlow Court,,Whitby,ON,CA,L1N 9T7,1,10",
    "3001001,100004092,OLDCO HOLDINGS INC.,,Canada Business Corporations Act,Inactive,Inactive - Amalgamated,1995-01-01,2001,2001-01-01,1 Main St,,Toronto,ON,CA,M5V 1A1,1,10",
    # Two inactive corporations sharing a BN: no single answer.
    "3002002,100003888,ALPHA LTD.,,Canada Business Corporations Act,Inactive,Inactive - Amalgamated,1990-01-01,,,,,Halifax,NS,CA,,1,10",
    "3003003,100003888,BETA LTD.,,Canada Business Corporations Act,Inactive,Inactive - Discontinued,1991-01-01,,,,,Halifax,NS,CA,,1,10",
]
INACTIVE_OTHER = [
    "1821,,THE BLUE RIVER AND DISTRICT CHAMBER OF COMMERCE,,Boards of Trade Act - Part I,Dissolved,Dissolved – Part II of CCA,1929-10-01,,,,,BLUE RIVER,BC,CA,,11,11",
]

CORP_8660115: dict[str, Any] = {
    "corporationId": "8660115",
    "act": "Canada Business Corporations Act",
    "status": "Active",
    "corporationNames": [{"CorporationName": {
        "name": "MINDANGLER CAPITAL INC.", "nameType": "Primary", "current": True,
        "effectiveDate": "2013-10-10"}}],
    "adresses": [{"address": {
        "addressLine": ["515 Legget Drive", "Suite 800"], "city": "Ottawa",
        "postalCode": "K2K 3G4", "provinceCode": "ON", "countryCode": "CA",
        "typeCode": "2", "current": True}}],
    "businessNumbers": {"businessNumber": "835752437"},
    "activities": [{"activity": {"activity": "Incorporation", "date": "2013-10-10"}}],
}
CORP_8593094: dict[str, Any] = {
    "corporationId": "8593094",
    "act": "Canada Business Corporations Act",
    "status": "Dissolved",
    "corporationNames": [{"CorporationName": {
        "name": "Stellar Point Productions Inc.", "nameType": "Primary", "current": True,
        "effectiveDate": "2013-07-29"}}],
    "adresses": [],
    "businessNumbers": {"businessNumber": "846556835"},
    "activities": [
        {"activity": {"activity": "Dissolution", "date": "2014-05-27", "section": "9"}},
        {"activity": {"activity": "Incorporation", "date": "2013-07-29"}},
    ],
}
NOT_FOUND = ["could not find corporation 999", "Corporation 999 est inconnu."]
DIRECTORS = {"_embedded": {"directors": [
    {"firstName": "ALICE", "lastName": "SMITH"},
    {"firstName": "BOB", "lastName": "JONES"},
]}}


def _write(path: Path, rows: list[str]) -> Path:
    path.write_text("﻿" + "\n".join([HEADER, *rows]) + "\n", encoding="utf-8")
    return path


def _sources(tmp_path: Path, **override: list[str]) -> dict[str, Path]:
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    rows = {
        "active_cbca": ACTIVE_CBCA, "active_other": ACTIVE_OTHER,
        "inactive_cbca": INACTIVE_CBCA, "inactive_other": INACTIVE_OTHER, **override,
    }
    return {short: _write(src / name, rows[short]) for short, name in cc.CSV_FILES.items()}


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("CORPORATIONS_CANADA_DB_FILE", None)
    else:
        os.environ["CORPORATIONS_CANADA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    cc.reset_connection()


@pytest.fixture()
def live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Live calls on, a fresh response cache, the gateway key set."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "root"))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CORPORATIONS_CANADA_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture()
def index(tmp_path: Path, live):
    out = tmp_path / "data" / "corporations_canada.sqlite"
    out.parent.mkdir()
    stats = builder.build(_sources(tmp_path), out, CUT, min_rows={})
    _point_at(out)
    yield out, stats
    _point_at(None)


@pytest.fixture()
def no_index(live):
    _point_at(None)
    yield
    _point_at(None)


async def _fetch(hit_id: str, legal_name: str = "") -> dict[str, Any]:
    return await CorporationsCanadaAdapter().fetch(hit_id, legal_name=legal_name)


def _record_route(mock: respx.MockRouter, base: str, corp_id: str, body: Any, status: int = 200):
    return mock.get(f"{base}/v1/corporations/{corp_id}.json" if base == GATEWAY
                    else f"{base}/corporations/{corp_id}.json").mock(
        return_value=httpx.Response(status, json=body)
    )


def _directors_route(mock: respx.MockRouter, corp_id: str, body: Any = None, status: int = 200):
    return mock.get(f"{GATEWAY}/v2/corporations/{corp_id}/directors.json").mock(
        return_value=httpx.Response(status, json=body if body is not None else DIRECTORS)
    )


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("728613-9", "7286139"),
        ("035818-5", "358185"),  # GLEIF's leading zero; the register never writes one
        ("0105333", "105333"),
        ("17099207", "17099207"),
        (" 1007 ", "1007"),
        ("123456789RC0001", "BN123456789"),
        ("123456789 rc 0001", "BN123456789"),
        ("", ""),
    ],
)
def test_normalise_corp_id(raw: str, expected: str) -> None:
    assert normalise_corp_id(raw) == expected


def test_a_business_number_filing_is_told_apart() -> None:
    assert is_bn_filing("100117555RC0001") is True
    assert is_bn_filing("358185") is False
    assert is_bn_filing("035818-5") is False


def test_the_deriver_gives_the_registers_spelling() -> None:
    ctx = _LookupCtx(lei="549300DJBIZ5T0OC3F21", legal_name="FCA Canada Inc.",
                     jurisdiction="CA", registered_as="035818-5")
    _build_derived(ctx, CA_CORP_RA_CODE)
    assert ctx.derived["ca_corp_id"] == "358185"


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_reads_the_published_shape_and_records_its_cut(index) -> None:
    _out, stats = index
    assert stats["corporations"] == 11
    assert (stats["active_cbca"], stats["active_other"]) == (4, 1)
    assert (stats["inactive_cbca"], stats["inactive_other"]) == (5, 1)
    assert stats["malformed"] == 0 and stats["repeated"] == 0
    meta = cc.read_meta()
    assert meta["cut"] == CUT and meta["schema_version"] == cc.INDEX_SCHEMA_VERSION
    assert provenance.parse_moment(meta["built_at"]) is not None
    row = cc.index_record("7286139")
    assert row is not None
    assert row["street"] == "45, Queen Street" and row["detail"] == "Inactive - Amalgamated"
    assert row["directors_min"] == 1 and row["directors_max"] == 10
    assert cc.index_record("1821")["bn"] is None


def test_the_builder_refuses_another_header(tmp_path: Path) -> None:
    paths = _sources(tmp_path)
    paths["active_cbca"].write_text("﻿Corporation number,Name\n1,X\n", encoding="utf-8")
    with pytest.raises(builder.RefusedInput, match="header"):
        builder.build(paths, tmp_path / "x.sqlite", CUT, min_rows={})
    assert not (tmp_path / "x.sqlite").exists()


def test_the_builder_refuses_a_short_file_and_a_bad_cut(tmp_path: Path) -> None:
    paths = _sources(tmp_path)
    with pytest.raises(builder.RefusedInput, match="not the register"):
        builder.build(paths, tmp_path / "x.sqlite", CUT)  # the real floors
    with pytest.raises(builder.RefusedInput, match="YYYY-MM-DD"):
        builder.build(paths, tmp_path / "x.sqlite", "08/11/2026", min_rows={})


def test_a_repeated_number_is_kept_once_and_counted(tmp_path: Path) -> None:
    paths = _sources(tmp_path, active_other=[*ACTIVE_OTHER, ACTIVE_CBCA[0]])
    stats = builder.build(paths, tmp_path / "x.sqlite", CUT, min_rows={})
    assert stats["repeated"] == 1 and stats["corporations"] == 11


def test_files_from_two_days_are_not_one_cut() -> None:
    day = datetime(2026, 11, 8, 11, 6, tzinfo=UTC)
    assert builder.cut_of({"a": day, "b": day.replace(minute=7)}) == "2026-11-08"
    with pytest.raises(builder.RefusedInput, match="not one cut"):
        builder.cut_of({"a": day, "b": day.replace(day=10)})


# ---------------------------------------------------------------------------
# The adapter: index first, then the API
# ---------------------------------------------------------------------------


@respx.mock
async def test_an_index_hit_reads_the_api_for_activities_and_directors(index) -> None:
    gateway = _record_route(respx.mock, GATEWAY, "8660115", [CORP_8660115, None])
    _directors_route(respx.mock, "8660115")
    degradation.begin()
    with provenance.recording() as rec:
        bundle = await _fetch("866011-5", "Mindangler Capital")
    assert degradation.collect() == []
    assert gateway.called
    assert bundle["tier"] == "index" and bundle["cut"] == CUT
    assert bundle["index"]["name"] == "MINDANGLER CAPITAL INC."
    assert bundle["corporation"]["corporationId"] == "8660115"
    assert bundle["record_via"] == "gateway"
    assert bundle["directors_read"] is True and len(bundle["directors"]) == 2
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == CUT


@respx.mock
async def test_without_a_key_the_record_comes_from_the_keyless_endpoint(
    index, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CORPORATIONS_CANADA_API_KEY", raising=False)
    get_settings.cache_clear()
    gateway = _record_route(respx.mock, GATEWAY, "8660115", [CORP_8660115, None])
    legacy = _record_route(respx.mock, LEGACY, "8660115", [CORP_8660115, None])
    degradation.begin()
    bundle = await _fetch("8660115")
    assert degradation.collect() == [], "a missing key is not a failure"
    assert legacy.called and not gateway.called
    assert bundle["record_via"] == "legacy"
    assert bundle["directors_read"] is False and bundle["directors"] == []


@respx.mock
async def test_a_failing_gateway_falls_back_to_the_keyless_endpoint(index) -> None:
    _record_route(respx.mock, GATEWAY, "8593094", {"error": "x"}, status=503)
    legacy = _record_route(respx.mock, LEGACY, "8593094", [CORP_8593094, None])
    _directors_route(respx.mock, "8593094", status=404)
    degradation.begin()
    bundle = await _fetch("8593094")
    assert degradation.collect() == []
    assert legacy.called and bundle["record_via"] == "legacy"
    assert bundle["directors_read"] is True and bundle["directors"] == []  # 404: none on file


@respx.mock
async def test_an_index_hit_survives_an_api_outage_as_a_partial_read(index) -> None:
    _record_route(respx.mock, GATEWAY, "8660115", {}, status=500)
    _record_route(respx.mock, LEGACY, "8660115", {}, status=502)
    _directors_route(respx.mock, "8660115", status=500)
    degradation.begin()
    bundle = await _fetch("8660115")
    collected = degradation.collect()
    assert bundle["is_stub"] is False and bundle["tier"] == "index"
    assert bundle["corporation"] is None and bundle["directors_read"] is False
    assert {d.check for d in collected} == {degradation.CHECK_SOURCE_READ}
    assert all(d.source_id == "corporations_canada" for d in collected)


@respx.mock
async def test_an_outage_with_nothing_in_hand_is_a_failure_not_no_record(index) -> None:
    _record_route(respx.mock, GATEWAY, "9999999", {}, status=500)
    _record_route(respx.mock, LEGACY, "9999999", {}, status=500)
    degradation.begin()
    bundle = await _fetch("9999999")
    collected = degradation.collect()
    assert bundle["is_stub"] is True
    assert [d.check for d in collected] == [degradation.CHECK_SOURCE_FETCH]
    assert degradation.unanswered_fetch.__name__  # the pipeline reads it as "did not answer"


@respx.mock
async def test_a_number_on_neither_is_a_note_card(index) -> None:
    _record_route(respx.mock, GATEWAY, "9999999", NOT_FOUND)
    degradation.begin()
    bundle = await _fetch("999999-9", "GHOST LTD")
    assert degradation.collect() == []
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["coverage_note"] == cc.COVERAGE_NOT_FOUND
    assert list(map_corporations_canada(bundle)) == []


@respx.mock
async def test_a_corporation_newer_than_the_cut_is_read_from_the_api(index) -> None:
    newer = {**CORP_8660115, "corporationId": "17000001"}
    _record_route(respx.mock, GATEWAY, "17000001", [newer, None])
    _directors_route(respx.mock, "17000001")
    bundle = await _fetch("17000001")
    assert bundle["tier"] == "api" and bundle["index"] is None and bundle["cut"] is None


@respx.mock
async def test_the_record_is_reread_after_a_day_and_directors_after_a_week(
    index, monkeypatch: pytest.MonkeyPatch
) -> None:
    from opencheck.cache import Cache

    asked: list[tuple[str, float | None]] = []
    real = Cache.get_payload

    def spy(self, key, max_age_days=None):
        asked.append((key, max_age_days))
        return real(self, key, max_age_days=max_age_days)

    monkeypatch.setattr(Cache, "get_payload", spy)
    route = _record_route(respx.mock, GATEWAY, "8660115", [CORP_8660115, None])
    _directors_route(respx.mock, "8660115")
    adapter = CorporationsCanadaAdapter()
    await adapter.fetch("8660115")
    await adapter.fetch("8660115")
    assert route.call_count == 1, "the second read within the day is the cached record"
    assert ("corporations_canada/v1/8660115", cc.RECORD_MAX_AGE_DAYS) in asked
    assert ("corporations_canada/v2/directors/8660115", cc.DIRECTORS_MAX_AGE_DAYS) in asked
    assert cc.RECORD_MAX_AGE_DAYS == 1.0 and cc.DIRECTORS_MAX_AGE_DAYS == 7.0


@respx.mock
async def test_a_business_number_resolves_to_the_one_active_corporation(index) -> None:
    newco = {**CORP_8660115, "corporationId": "4111117"}
    _record_route(respx.mock, GATEWAY, "4111117", [newco, None])
    _directors_route(respx.mock, "4111117")
    bundle = await _fetch("100004092RC0001")
    assert bundle["corp_id"] == "4111117"
    assert bundle["resolved_from_bn"] == "100004092"


async def test_an_ambiguous_business_number_is_not_guessed(index) -> None:
    degradation.begin()
    bundle = await _fetch("100003888RC0001")
    assert degradation.collect() == []
    assert bundle["not_found"] is True and bundle["coverage_note"] == COVERAGE_BN_UNRESOLVED
    assert "Business Number" in finding_corporations_canada(bundle)


async def test_without_an_index_a_business_number_reads_nothing(no_index) -> None:
    assert (await _fetch("100004092RC0001"))["is_stub"] is True


@respx.mock
async def test_without_an_index_the_api_answers_alone(no_index) -> None:
    _record_route(respx.mock, GATEWAY, "8660115", [CORP_8660115, None])
    _directors_route(respx.mock, "8660115")
    bundle = await _fetch("8660115")
    assert bundle["tier"] == "api" and bundle["corporation"]["corporationId"] == "8660115"


async def test_offline_with_no_index_nothing_is_read(no_index, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    assert (await _fetch("8660115"))["is_stub"] is True


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


async def test_name_search_reads_both_name_forms_active_first(index) -> None:
    adapter = CorporationsCanadaAdapter()
    hits = await adapter.search("holdings", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == ["4111117", "3001001"]
    assert hits[0].identifiers == {"ca_corp_id": "4111117"}
    assert hits[1].summary.endswith("inactive - amalgamated")
    assert all(h.liveness == "snapshot" and not h.is_stub for h in hits)
    french = await adapter.search("Société", SearchKind.ENTITY)
    assert [h.hit_id for h in french] == ["358185"]
    assert await adapter.search("Alice Smith", SearchKind.PERSON) == []


async def test_without_an_index_there_is_no_name_search(no_index) -> None:
    assert await CorporationsCanadaAdapter().search("holdings", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# Mapper, finding and hit
# ---------------------------------------------------------------------------


def _entity(statements: list[dict]) -> dict:
    return next(s for s in statements if s["recordType"] == "entity")


def _index_bundle(corp_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "source_id": "corporations_canada", "corp_id": corp_id, "corporation": None,
        "directors": [], "directors_read": False, "index": cc.index_record(corp_id),
        "tier": "index", "cut": CUT, "legal_name": "", "is_stub": False, **extra,
    }


def test_an_index_only_record_maps_the_open_data(index) -> None:
    with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
        statements = list(map_corporations_canada(_index_bundle("358185")))
    entity = _entity(statements)
    rd = entity["recordDetails"]
    assert rd["name"] == "FCA Canada Inc."
    assert "FCA Canada Inc. (Société)" in rd.get("alternateNames", [])
    assert rd["foundingDate"] == "1979-12-31"
    assert rd["entityType"]["details"] == "Governed by the Canada Business Corporations Act"
    schemes = {i["scheme"]: i["id"] for i in rd["identifiers"]}
    assert schemes == {"CA-CC": "358185", "CA-BN": "100117555"}
    assert rd["addresses"][0]["address"].startswith("1 Riverside Drive West, Windsor, ON")


def test_dissolution_pending_is_pending_and_said_in_the_registers_words(index) -> None:
    bundle = _index_bundle("821080")
    with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
        entity = _entity(list(map_corporations_canada(bundle)))
    notes = " ".join(a.get("description", "") for a in entity.get("annotations", []))
    assert "Dissolution Pending (non-compliance)" in notes
    assert "dissolutionDate" not in entity["recordDetails"]
    sentence = finding_corporations_canada(bundle)
    assert sentence.startswith("Dissolution Pending (non-compliance)")


def test_a_dissolution_is_dated_by_the_api(index) -> None:
    bundle = _index_bundle("8593094", corporation=CORP_8593094)
    with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
        entity = _entity(list(map_corporations_canada(bundle)))
    assert entity["recordDetails"]["dissolutionDate"] == "2014-05-27"
    assert finding_corporations_canada(bundle).startswith(
        "Dissolved by the corporation on 27 May 2014"
    )


def test_an_amalgamated_corporation_is_said_without_the_inactive_prefix(index) -> None:
    # 7286139's record (6 Nov 2026) has no amalgamation activity, so no date.
    sentence = finding_corporations_canada(_index_bundle("7286139"))
    assert sentence.startswith("Amalgamated, under the Canada Business Corporations Act")
    assert "Inactive" not in sentence


def test_the_finding_counts_directors_only_when_they_were_read(index) -> None:
    unread = _index_bundle("8660115")
    assert "director" not in finding_corporations_canada(unread)
    read = _index_bundle("8660115", directors=[], directors_read=True)
    sentence = finding_corporations_canada(read)
    assert sentence.startswith("Active since 10 October 2013")
    assert "0 directors on file" in sentence and "last annual return 2026" in sentence
    assert len(sentence) <= MAX_FINDING_CHARS


@respx.mock
async def test_a_resolved_business_number_asserts_the_corporation_number(index) -> None:
    newco = {**CORP_8660115, "corporationId": "4111117",
             "corporationNames": [{"CorporationName": {"name": "NEWCO HOLDINGS INC.", "current": True}}]}
    _record_route(respx.mock, GATEWAY, "4111117", [newco, None])
    _directors_route(respx.mock, "4111117")
    bundle = await _fetch("100004092RC0001")
    ctx = _LookupCtx(lei="5493000000000000TEST", legal_name="Newco",
                     jurisdiction="CA", registered_as="100004092RC0001")
    _build_derived(ctx, CA_CORP_RA_CODE)
    hit = _build_result_hit("corporations_canada", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {"ca_corp_id": "4111117"}
    assert hit.hit_id == "4111117" and hit.name == "NEWCO HOLDINGS INC."


@respx.mock
async def test_the_note_card_asserts_no_identifier(index) -> None:
    _record_route(respx.mock, GATEWAY, "9999999", NOT_FOUND)
    bundle = await _fetch("9999999", "GHOST LTD")
    ctx = _LookupCtx(lei="5493000000000000TEST", legal_name="GHOST LTD",
                     jurisdiction="CA", registered_as="999999-9")
    _build_derived(ctx, CA_CORP_RA_CODE)
    hit = _build_result_hit("corporations_canada", bundle, ctx)
    assert hit is not None and hit.identifiers == {}
    assert hit.finding and hit.finding.startswith("Not on Corporations Canada's federal register")
