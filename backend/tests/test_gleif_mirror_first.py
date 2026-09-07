"""Phase 179 — the GLEIF mirror as the first source, behind a flag.

With ``OPENCHECK_GLEIF_MIRROR_FIRST`` on and a Phase 178 mirror holding the
LEI, the anchor (record, both parents or the exceptions filed in their place,
the children page) and the subsidiary network are served from the mirror and
GLEIF is not called. A miss goes live exactly as before. With the flag off
nothing changes — the tests here pin both, plus the never-invent rule on a
mirror-served bundle, the honest provenance (``snapshot``, dated to the
mirror's watermark, detail "mirror" not "rate-limited"), the degradation path
still being reached when live also fails, the optional live-confirm counters,
and the aggregate ``/mirror`` endpoint.

The fixture file is built by the real builder from the Phase 178 fixture rows
(``test_gleif_mirror_store``), compressed, so every read here goes through the
dictionary-inflate path a production file needs.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import entity_pages as ep
from opencheck import mirrorstats, provenance
from opencheck import subsidiaries as subs
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.gleif_throttle import GleifRateLimitedError, get_throttle, reset_throttle_for_tests
from opencheck.sources.gleif import GleifAdapter
from tests.test_gleif_mirror_store import (
    EASY,
    LEI2_HEADER,
    LEI2_ROWS,
    MAPPER_READS,
    PARENT,
    REPEX_HEADER,
    REPEX_ROWS,
    RETIRED_CHILD,
    RR_HEADER,
    RR_ROWS,
    TOP,
    _v1_schema,
    _write_csv,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from build_entity_pages_db import (  # noqa: E402
    compress_detail_column,
    ensure_schema,
    load_lei2,
    load_repex,
    load_rr,
    write_meta,
)

_API = "https://api.gleif.org/api/v1"
_MISSING = "MISSING000000000XX79"
_WATERMARK = "2026-09-07 16:00:00"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mirror_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("mirror-first")
    out = tmp / "entity_pages.sqlite"
    conn = sqlite3.connect(out)
    ensure_schema(conn)
    assert load_lei2(conn, _write_csv(tmp / "lei2.csv", LEI2_HEADER, LEI2_ROWS)) == 5
    assert load_rr(conn, _write_csv(tmp / "rr.csv", RR_HEADER, RR_ROWS)) == 6
    assert load_repex(conn, _write_csv(tmp / "repex.csv", REPEX_HEADER, REPEX_ROWS)) == 3
    assert compress_detail_column(conn) == 5
    write_meta(
        conn,
        source_publish_date=_WATERMARK,
        source_publish_datetime=_WATERMARK,
        schema_version=ep.SCHEMA_VERSION,
        record_count="5",
        relationship_count="6",
        exception_count="3",
    )
    conn.close()
    return out


@pytest.fixture
def v1_db(tmp_path: Path) -> Path:
    """A Phase 88 file holding the same subject, page columns only."""
    db = tmp_path / "v1.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(_v1_schema())
    conn.execute(
        "INSERT INTO entities (lei, name, slug, entity_status, registration_status, "
        "jurisdiction, city, country, direct_parent_lei) "
        "VALUES (?, 'EASY POWER (V1)', 'easy-power', 'ACTIVE', 'ISSUED', 'GR', "
        "'ATHENS', 'GR', ?)",
        (EASY, PARENT),
    )
    conn.commit()
    conn.close()
    return db


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_FILE", raising=False)
    monkeypatch.delenv("OPENCHECK_GLEIF_MIRROR_FIRST", raising=False)
    monkeypatch.delenv("OPENCHECK_GLEIF_LIVE_CONFIRM", raising=False)
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    reset_throttle_for_tests()
    mirrorstats.reset_for_tests()
    yield
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    reset_throttle_for_tests()
    mirrorstats.reset_for_tests()


def _configure(monkeypatch: pytest.MonkeyPatch, db: Path, *, mirror_first: bool,
               live_confirm: bool = False) -> None:
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_GLEIF_MIRROR_FIRST", "true" if mirror_first else "false")
    monkeypatch.setenv("OPENCHECK_GLEIF_LIVE_CONFIRM", "true" if live_confirm else "false")
    get_settings.cache_clear()
    ep.reset_store_for_tests()


def _mock_live_anchor(httpx_mock: HTTPXMock, lei: str, name: str) -> None:
    """A complete live answer for ``lei``: record, no parents, no exceptions,
    no children — six requests, the Phase 143 shape."""
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}",
        json={"data": {"id": lei, "attributes": {"lei": lei, "entity": {
            "legalName": {"name": name}}}}},
    )
    for path in (
        "direct-parent", "direct-parent-reporting-exception",
        "ultimate-parent", "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(url=f"{_API}/lei-records/{lei}/{path}", status_code=404)
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}/direct-children?page[size]=100&page[number]=1",
        json={"data": [], "meta": {"pagination": {"total": 0}}},
    )


def _mock_live_record(httpx_mock: HTTPXMock, lei: str, *, name: str = "LIVE NAME",
                      crossrefs: bool = True, **extra: object) -> None:
    """The Level 1 record alone — what the cross-reference top-up fetches —
    carrying the mapping-file ids the Golden Copy lacks."""
    attrs: dict = {"lei": lei, "entity": {"legalName": {"name": name}}}
    if crossrefs:
        attrs.update({"ocid": "gr/007132601000", "spglobal": ["12345"], "bic": ["EASYGRA1"],
                      "qcc": None, "mic": []})
    attrs.update(extra)
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{lei}", json={"data": {"id": lei, "attributes": attrs}}
    )


def _counts() -> dict[str, int]:
    return mirrorstats.stats()["counts"]


# ---------------------------------------------------------------------------
# The flag
# ---------------------------------------------------------------------------


def test_both_flags_are_off_by_default() -> None:
    settings = get_settings()
    assert settings.gleif_mirror_first is False
    assert settings.gleif_live_confirm is False


async def test_flag_off_keeps_the_live_order(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Today's behaviour byte-for-byte: a mirror that holds the LEI is not
    consulted first when the flag is off — the anchor comes live."""
    _configure(monkeypatch, mirror_db, mirror_first=False)
    _mock_live_anchor(httpx_mock, EASY, "EASY POWER (LIVE)")
    bundle = await GleifAdapter().fetch(EASY)
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == "EASY POWER (LIVE)"
    assert "snapshot_source" not in bundle
    assert len(httpx_mock.get_requests()) == 6
    assert sum(_counts().values()) == 0


# ---------------------------------------------------------------------------
# Mirror first: the anchor
# ---------------------------------------------------------------------------


async def test_mirror_serves_the_anchor_with_one_call_for_the_cross_references(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The record, parents, exceptions and children come from the mirror; the
    one live call is the Level 1 record, for the mapping-file ids (ocid,
    spglobal, bic, mic, qcc) the Golden Copy does not carry — without them
    the export lost two to three identifiers per entity statement and the
    OpenCorporates dispatch was skipped."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    _mock_live_record(httpx_mock, EASY, name="EASY POWER (LIVE)")

    with provenance.recording() as recorder:
        bundle = await GleifAdapter().fetch(EASY)
    resolved = recorder.resolve()

    assert [str(r.url) for r in httpx_mock.get_requests()] == [f"{_API}/lei-records/{EASY}"]
    attrs = bundle["record"]["attributes"]
    assert attrs["ocid"] == "gr/007132601000" and attrs["spglobal"] == ["12345"]
    assert attrs["bic"] == ["EASYGRA1"]
    assert "qcc" not in attrs and "mic" not in attrs  # empty live values are not copied
    assert bundle["crossrefs_available"] is True
    # The record itself is still the mirror's, not the live one.
    assert attrs["entity"]["legalName"]["name"].startswith("EASY POWER")
    assert attrs["entity"]["legalName"]["name"] != "EASY POWER (LIVE)"
    # The full Level 1 record — what a v1 snapshot could never carry.
    entity = bundle["record"]["attributes"]["entity"]
    assert entity["registeredAs"] == "007132601000"
    assert entity["registeredAt"]["id"] == "RA000685"
    assert entity["legalAddress"]["language"] == "en-IE"  # as the file has it
    # Both parents, by name, from the RR table — the LAPSED ultimate included.
    assert bundle["direct_parent"]["attributes"]["lei"] == PARENT
    assert bundle["direct_parent"]["attributes"]["entity"]["legalName"]["name"] == (
        "Mirror Parent Holdings Ltd"
    )
    assert bundle["ultimate_parent"]["attributes"]["lei"] == TOP
    assert bundle["direct_parent_exception"] is None
    assert bundle["direct_children_total"] == 0
    # Chosen, not forced — and the badge says how old the mirror is.
    assert bundle["snapshot_source"] == "mirror"
    assert bundle["snapshot_fallback"] is False
    assert resolved.liveness == "snapshot"
    assert resolved.retrieved_at_iso() == "2026-09-07T16:00:00Z"
    assert resolved.detail == "GLEIF Golden Copy mirror"
    counts = _counts()
    assert counts["anchor.mirror"] == 1 and counts["anchor.miss_live"] == 0
    # Record + two parents + the children page: four live calls not spent —
    # against the one the top-up did spend.
    assert counts["anchor.live_calls_saved"] == 4 and counts["anchor.topup_live"] == 1


async def test_top_up_uses_the_cached_record_and_survives_a_refusal(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Second lookup within the record TTL: the top-up costs nothing. And when
    GLEIF refuses, the anchor is still served — without the cross-reference
    ids, and saying so."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    adapter = GleifAdapter()
    _mock_live_record(httpx_mock, EASY)
    await adapter.fetch(EASY)
    again = await adapter.fetch(EASY)
    assert len(httpx_mock.get_requests()) == 1
    assert again["record"]["attributes"]["ocid"] == "gr/007132601000"
    assert _counts()["anchor.topup_cached"] == 1

    httpx_mock.add_response(url=f"{_API}/lei-records/{TOP}", status_code=429)
    bundle = await adapter.fetch(TOP)
    assert bundle["snapshot_source"] == "mirror"
    assert bundle["crossrefs_available"] is False
    assert "ocid" not in bundle["record"]["attributes"]
    assert _counts()["anchor.topup_failed"] == 1


async def test_mirror_serves_the_exceptions_filed_in_place_of_a_parent(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TOP has no parents and two exceptions; live would probe the exception
    endpoint for each kind — six calls in all — and the mirror answers all
    of it, so the Phase 114 chip classifier sees the same ``reason``."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    _mock_live_record(httpx_mock, TOP, crossrefs=False)
    bundle = await GleifAdapter().fetch(TOP)
    assert len(httpx_mock.get_requests()) == 1  # the top-up only
    assert bundle["direct_parent"] is None and bundle["ultimate_parent"] is None
    assert bundle["direct_parent_exception"]["attributes"]["reason"] == "NON_CONSOLIDATING"
    assert bundle["ultimate_parent_exception"]["attributes"]["reason"] == "NATURAL_PERSONS"
    assert bundle["ultimate_parent_exception"]["attributes"]["reference"] == (
        "Section 399 Companies Act 2006"
    )
    # Children from both indexes: PARENT is a direct child of TOP.
    assert bundle["direct_children_total"] == 1
    assert bundle["direct_children"][0]["attributes"]["lei"] == PARENT
    assert _counts()["anchor.live_calls_saved"] == 6


async def test_mirror_never_invents_a_field(
    mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row the file carried without a registration authority yields a record
    without ``registeredAs`` — the registry bridge skips, it is not guessed."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")  # no top-up: the mirror alone
    get_settings.cache_clear()
    bundle = await GleifAdapter().fetch(RETIRED_CHILD)
    assert bundle["crossrefs_available"] is False
    assert _counts()["anchor.topup_skipped"] == 1
    entity = bundle["record"]["attributes"]["entity"]
    assert "registeredAs" not in entity and "registeredAt" not in entity
    assert "headquartersAddress" not in entity
    assert "ocid" not in bundle["record"]["attributes"]
    # And what it does hold is exactly the file's value.
    assert entity["legalName"] == {"name": "Mirror Retired Link Ltd"}
    assert bundle["direct_parent"] is None  # RETIRED record: not standing
    assert bundle["direct_parent_exception"]["attributes"]["reason"] == "NO_LEI"


async def test_mirror_miss_goes_live_and_the_cache_keeps_it(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An LEI the mirror lacks takes the live path unchanged; the adapter's
    own cache then serves the repeat, so the next lookup is a hit without a
    second write path into the mirror (Phase 180's deltas are that path)."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    _mock_live_anchor(httpx_mock, _MISSING, "NEWLY ISSUED LTD")
    adapter = GleifAdapter()

    bundle = await adapter.fetch(_MISSING)
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == "NEWLY ISSUED LTD"
    assert "snapshot_source" not in bundle
    assert len(httpx_mock.get_requests()) == 6

    again = await adapter.fetch(_MISSING)
    assert again["record"] == bundle["record"]
    assert len(httpx_mock.get_requests()) == 6  # served from the adapter cache
    counts = _counts()
    assert counts["anchor.miss_live"] == 2 and counts["anchor.mirror"] == 0


async def test_mirror_miss_with_live_refused_still_reaches_the_degradation_path(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    httpx_mock.add_response(status_code=429, is_reusable=True)
    with pytest.raises(GleifRateLimitedError):
        await GleifAdapter().fetch(_MISSING)


async def test_a_v1_file_is_never_served_first(
    httpx_mock: HTTPXMock, v1_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The flag on with a Phase 88 file: its rows cannot carry ``registeredAs``
    or an exception, so the live order runs and the miss is counted as
    ``no_mirror`` — the /mirror number that says the file needs rebuilding."""
    _configure(monkeypatch, v1_db, mirror_first=True)
    assert ep.get_store() is not None and ep.get_store().is_mirror is False
    _mock_live_anchor(httpx_mock, EASY, "EASY POWER (LIVE)")
    bundle = await GleifAdapter().fetch(EASY)
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == "EASY POWER (LIVE)"
    assert "snapshot_source" not in bundle
    assert _counts()["anchor.no_mirror"] == 1


async def test_fetch_entity_from_the_mirror(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Phase 162 dispatch-drift check reads ``registeredAs`` from the
    mirror — the one call per anchor it used to spend is gone too."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    entity = await GleifAdapter().fetch_entity(EASY)
    assert entity["registeredAs"] == "007132601000"
    assert httpx_mock.get_requests() == []
    assert _counts()["entity.mirror"] == 1


@pytest.mark.httpx_mock(assert_all_requests_were_expected=False)  # climatetrace warm-up
async def test_the_lookup_pipeline_anchor_comes_from_the_mirror(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end through ``/lookup``: the GLEIF hit is badged snapshot, dated
    to the watermark, and not one request reached api.gleif.org."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")  # keep the other 40 sources quiet
    get_settings.cache_clear()
    response = TestClient(app).get("/lookup", params={"lei": TOP})
    assert response.status_code == 200
    gleif_hits = [h for h in response.json()["hits"] if h["source_id"] == "gleif"]
    assert len(gleif_hits) == 1
    assert gleif_hits[0]["liveness"] == "snapshot"
    assert gleif_hits[0]["retrieved_at"] == "2026-09-07T16:00:00Z"
    body = response.json()
    assert body["source_liveness"]["gleif"]["detail"] == "GLEIF Golden Copy mirror"
    assert not any("api.gleif.org" in str(r.url) for r in httpx_mock.get_requests())


# ---------------------------------------------------------------------------
# Live-confirm (measurement only, riding on the top-up's record)
# ---------------------------------------------------------------------------


async def test_live_confirm_counts_agreement_and_names_the_differing_field(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True, live_confirm=True)
    adapter = GleifAdapter()
    store = ep.get_store()
    assert store is not None
    live_same = ep.gleif_record_from_row(store.get(EASY))
    httpx_mock.add_response(url=f"{_API}/lei-records/{EASY}", json={"data": live_same})
    bundle = await adapter.fetch(EASY)
    assert bundle["snapshot_source"] == "mirror"
    assert _counts()["confirm.same"] == 1

    live_changed = json.loads(json.dumps(live_same))
    live_changed["attributes"]["entity"]["legalName"]["name"] = "EASY POWER (RENAMED)"
    httpx_mock.add_response(url=f"{_API}/lei-records/{TOP}", json={"data": live_changed})
    bundle = await adapter.fetch(TOP)  # a different LEI, so the record is not cached
    # Served from the mirror regardless: the comparison is a count, not a choice.
    assert bundle["record"]["attributes"]["entity"]["legalName"]["name"] == "Mirror Top plc"
    stats = mirrorstats.stats()
    assert stats["counts"]["confirm.differs"] == 1
    assert "attributes/entity/legalName/name" in stats["differing_paths"]
    assert stats["confirm_differ_rate"] == 0.5


async def test_top_up_never_spends_the_budget_a_lookup_needs(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True, live_confirm=True)
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "3")
    get_settings.cache_clear()
    reset_throttle_for_tests()
    throttle = get_throttle()
    for _ in range(2):  # two of three slots taken: the last third is reserved
        await throttle.acquire()
    bundle = await GleifAdapter().fetch(EASY)
    assert httpx_mock.get_requests() == []
    assert bundle["snapshot_source"] == "mirror" and bundle["crossrefs_available"] is False
    assert _counts()["anchor.topup_skipped"] == 1


async def test_top_up_is_bounded_by_the_snapshot_wait(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A GLEIF that does not answer within OPENCHECK_GLEIF_SNAPSHOT_AFTER_S
    cannot hold the mirror-served anchor hostage."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    monkeypatch.setenv("OPENCHECK_GLEIF_SNAPSHOT_AFTER_S", "0.2")
    get_settings.cache_clear()

    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(2)
        return httpx.Response(200, json={"data": {}})

    httpx_mock.add_callback(slow, url=f"{_API}/lei-records/{EASY}")
    start = time.monotonic()
    bundle = await GleifAdapter().fetch(EASY)
    assert time.monotonic() - start < 1.5
    assert bundle["snapshot_source"] == "mirror" and bundle["crossrefs_available"] is False
    assert _counts()["anchor.topup_failed"] == 1


# ---------------------------------------------------------------------------
# Mirror first: the subsidiary network
# ---------------------------------------------------------------------------


async def test_subsidiary_network_from_the_mirror(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    result = await subs.assemble_subsidiaries(TOP, include_bods=True)
    assert httpx_mock.get_requests() == []
    assert result["direct_total"] == 1 and result["ultimate_total"] == 2
    by_lei = {c["lei"]: c["relation"] for c in result["children"]}
    assert by_lei == {PARENT: "both", EASY: "ultimate"}
    assert result["direct_available"] and result["ultimate_available"]
    assert result["children_available"] is True
    # Chosen, not forced: badged with the date, no degradation sentence.
    assert result["snapshot_source"] == "mirror"
    assert result["snapshot_fallback"] is True
    assert result["snapshot_date"] == "2026-09-07"
    assert result["degraded_detail"] is None
    # The exported statements carry the snapshot provenance, not "live now".
    sources = [s.get("source") for s in result["bods"] if s.get("source")]
    assert sources and all(src.get("retrievedAt") == "2026-09-07T00:00:00Z" for src in sources)
    assert _counts()["subsidiaries.mirror"] == 1
    # Not written to the response cache: the mirror is the cache.
    assert subs._cache.get_payload(f"{subs._CACHE_NS}/{TOP}") is None


async def test_subsidiary_network_miss_goes_live(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    httpx_mock.add_response(url=f"{_API}/lei-records/{_MISSING}", status_code=404)
    for kind in ("direct", "ultimate"):
        httpx_mock.add_response(
            url=f"{_API}/lei-records/{_MISSING}/{kind}-children?page[size]=100&page[number]=1",
            status_code=404,
        )
    result = await subs.assemble_subsidiaries(_MISSING)
    assert result["snapshot_source"] is None
    assert result["direct_total"] == 0 and result["children"] == []
    assert _counts()["subsidiaries.miss_live"] == 1


async def test_subsidiary_network_flag_off_stays_live(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=False)
    httpx_mock.add_response(url=f"{_API}/lei-records/{TOP}", status_code=404)
    for kind in ("direct", "ultimate"):
        httpx_mock.add_response(
            url=f"{_API}/lei-records/{TOP}/{kind}-children?page[size]=100&page[number]=1",
            status_code=404,
        )
    result = await subs.assemble_subsidiaries(TOP)
    assert result["snapshot_source"] is None
    assert len(httpx_mock.get_requests()) == 3


# ---------------------------------------------------------------------------
# /mirror and the pinned path list
# ---------------------------------------------------------------------------


async def test_mirror_endpoint_is_aggregate_only(
    mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    await GleifAdapter().fetch(EASY)
    response = TestClient(app).get("/mirror")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    assert body["flags"] == {"mirror_first": True, "live_confirm": False}
    assert body["store"]["is_mirror"] is True
    assert body["store"]["schema_version"] == "2"
    assert body["store"]["watermark"] == _WATERMARK
    assert body["store"]["record_count"] == 5
    assert body["counts"]["anchor.mirror"] == 1
    assert body["anchor_mirror_hit_rate"] == 1.0
    text = json.dumps(body)
    for lei in (EASY, PARENT, TOP, RETIRED_CHILD):
        assert lei not in text
    assert "EASY POWER" not in text


def test_mirror_endpoint_without_a_store() -> None:
    body = TestClient(app).get("/mirror").json()
    assert body["store"] is None
    assert body["anchor_mirror_hit_rate"] is None


def test_unknown_counter_keys_are_dropped() -> None:
    mirrorstats.record("anchor.mirror")
    mirrorstats.record("anything/2138001EXFNP9E7AYB46")
    mirrorstats.record_differing_paths(["attributes/entity/legalName/name", "not/a/path"])
    stats = mirrorstats.stats()
    assert set(stats["counts"]) == set(mirrorstats._KEYS)
    assert stats["differing_paths"] == {"attributes/entity/legalName/name": 1}


def test_mapper_read_paths_are_the_parity_tests_list() -> None:
    """The live-confirm compares exactly the paths the parity test pins."""
    assert ep.MAPPER_READ_PATHS == MAPPER_READS


# ---------------------------------------------------------------------------
# Boot: the page-cache warm
# ---------------------------------------------------------------------------


def test_prewarm_reads_the_whole_file_once(mirror_db: Path) -> None:
    note = ep.prewarm_page_cache(mirror_db)
    assert note.startswith(f"page cache warmed: {mirror_db.stat().st_size} bytes")


def test_prewarm_is_bounded_and_never_raises(tmp_path: Path) -> None:
    assert ep.prewarm_page_cache(tmp_path / "missing.sqlite").startswith("page-cache warm failed")
    big = tmp_path / "big.sqlite"
    big.write_bytes(b"\0" * (3 * ep._PREWARM_CHUNK))
    note = ep.prewarm_page_cache(big, max_seconds=0.0)  # stops after the first chunk
    assert note.startswith(f"page cache warmed: {ep._PREWARM_CHUNK} bytes")


def test_boot_warm_up_with_a_local_file_warms_it(
    mirror_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_URL", "https://example.test/unused.gz")
    get_settings.cache_clear()
    note = ep.warm_entity_pages_db()["entity_pages"]
    assert note.startswith(f"present: {mirror_db}; page cache warmed")
