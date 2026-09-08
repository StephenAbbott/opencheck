"""Phase 186 — the UK PSC graph: a local store built from Companies House's
daily PSC snapshot, walked instead of fetched.

The fixture snapshot below is the register's own shape — one zip member of
JSON lines ``{"company_number", "data": {…the API PSC item…}}`` — carrying
the chains the ticket measured live: Timpson Ltd up through Sandymere,
Timpson Group and Timpson Holdings to three trust-controlling individuals;
a ceased record; a corporate PSC registered in Jersey; one whose number is
filed without its leading zeros and in lower case; a cross-holding cycle;
and a record of a kind the store does not know.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import entity_pages as ep
from opencheck import psc_graph
from opencheck.app import app
from opencheck.config import get_settings

_SEQ = iter(range(1, 10_000))


def _rec(company: str, kind: str, name: str, **data) -> dict:
    item = {"kind": kind, "name": name, "etag": f"e-{company}-{name}"[:40], **data}
    item.setdefault(
        "links",
        {"self": f"/company/{company}/persons-with-significant-control/x/id{next(_SEQ):04d}"},
    )
    item.setdefault("notified_on", "2016-04-06")
    return {"company_number": company, "data": item}


CORP = "corporate-entity-person-with-significant-control"
INDIV = "individual-person-with-significant-control"
LEGAL = "legal-person-person-with-significant-control"
MAJ = [
    "ownership-of-shares-75-to-100-percent",
    "voting-rights-75-to-100-percent",
    "right-to-appoint-and-remove-directors",
]
TRUST = [n + "-as-trust" for n in MAJ]


def _uk(number: str) -> dict:
    return {
        "country_registered": "England",
        "place_registered": "Companies House",
        "registration_number": number,
    }


RECORDS = [
    # Timpson Ltd ← Sandymere ← Timpson Group ← Timpson Holdings ← 3 individuals (as trust)
    _rec("00675216", CORP, "Sandymere Ltd", natures_of_control=MAJ, identification=_uk("00323208")),
    _rec(
        "00323208", CORP, "Timpson Group Plc", natures_of_control=MAJ, identification=_uk("2339274")
    ),  # dropped zero
    _rec(
        "02339274",
        CORP,
        "Timpson Holdings Ltd",
        natures_of_control=MAJ,
        identification=_uk("02588889"),
    ),
    _rec(
        "02588889",
        INDIV,
        "Mr Charles William Holroyd",
        natures_of_control=TRUST,
        date_of_birth={"month": 5, "year": 1960},
        nationality="British",
        country_of_residence="England",
    ),
    _rec(
        "02588889",
        INDIV,
        "Mr Matthew Samuel Davies",
        natures_of_control=TRUST,
        date_of_birth={"year": 1972},
    ),
    _rec(
        "02588889",
        INDIV,
        "Sir William John Anthony Timpson",
        natures_of_control=TRUST,
        date_of_birth={"month": 3, "year": 1943},
    ),
    # A ceased individual on Timpson Holdings — the build drops it.
    _rec(
        "02588889",
        INDIV,
        "Mr William James Timpson",
        natures_of_control=TRUST,
        ceased_on="2024-07-05",
    ),
    # A subject whose corporate PSC is registered in Jersey: not followed.
    _rec(
        "00000001",
        CORP,
        "Offshore Parent Limited",
        natures_of_control=MAJ,
        identification={"country_registered": "Jersey", "registration_number": "12345"},
    ),
    # A legal-person PSC filed lower-case with a Scottish prefix.
    _rec(
        "00000002",
        LEGAL,
        "The Scottish Partnership",
        natures_of_control=["significant-influence-or-control"],
        identification={"country_registered": "Scotland", "registration_number": "sc 12345"},
    ),
    _rec("SC012345", INDIV, "Ms Highland Owner", natures_of_control=MAJ),
    # A ↔ B cross-holding: each is the other's corporate PSC.
    _rec("00000010", CORP, "B Limited", natures_of_control=MAJ, identification=_uk("00000011")),
    _rec("00000011", CORP, "A Limited", natures_of_control=MAJ, identification=_uk("00000010")),
    # A record of a kind the store does not hold (a PSC statement).
    {
        "company_number": "00000003",
        "data": {
            "kind": "persons-with-significant-control-statement",
            "statement": "no-individual-or-entity-with-signficant-control",
            "links": {"self": "/company/00000003/persons-with-significant-control-statements/abc"},
        },
    },
    # A super-secure person.
    _rec(
        "00000004",
        "super-secure-person-with-significant-control",
        "",
        description="super-secure-persons-with-significant-control",
    ),
]


def _snapshot_zip(path: Path, records: list[dict] = RECORDS) -> Path:
    body = "\n".join(json.dumps(r) for r in records) + "\n"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("persons-with-significant-control-snapshot-2026-09-08.txt", body)
    return path


@pytest.fixture
def snapshot(tmp_path: Path) -> Path:
    return _snapshot_zip(tmp_path / "persons-with-significant-control-snapshot-2026-09-08.zip")


@pytest.fixture
def store_path(tmp_path: Path, snapshot: Path) -> Path:
    out = tmp_path / "psc_graph.sqlite"
    psc_graph.build_psc_graph(out, source_url=f"file://{snapshot}", snapshot_date="2026-09-08")
    return out


@pytest.fixture
def store(store_path: Path) -> psc_graph.PscGraphStore:
    s = psc_graph.PscGraphStore(store_path)
    yield s
    s.close()


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("OPENCHECK_PSC_GRAPH_DB_FILE", raising=False)
    monkeypatch.delenv("OPENCHECK_PSC_GRAPH_DB_URL", raising=False)
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()
    yield
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()


# ---------------------------------------------------------------------------
# Reading the snapshot
# ---------------------------------------------------------------------------


def test_snapshot_url_shapes():
    assert psc_graph.snapshot_url("2026-09-08").endswith(
        "persons-with-significant-control-snapshot-2026-09-08.zip"
    )
    assert psc_graph.snapshot_date_from_url(psc_graph.snapshot_url("2026-09-08")) == "2026-09-08"
    assert psc_graph.snapshot_date_from_url("file:///tmp/x.zip") is None


def test_latest_snapshot_url_walks_back_to_the_newest_published(httpx_mock: HTTPXMock):
    """The register publishes no snapshot on weekends and bank holidays."""
    from datetime import UTC, datetime

    today = datetime(2026, 9, 7, 8, 0, tzinfo=UTC)  # a Monday, before the day's publish
    httpx_mock.add_response(
        method="HEAD", url=psc_graph.snapshot_url("2026-09-07"), status_code=404
    )
    httpx_mock.add_response(
        method="HEAD", url=psc_graph.snapshot_url("2026-09-06"), status_code=404
    )
    httpx_mock.add_response(
        method="HEAD", url=psc_graph.snapshot_url("2026-09-05"), status_code=200
    )
    assert psc_graph.latest_snapshot_url(today=today) == psc_graph.snapshot_url("2026-09-05")


def test_latest_snapshot_url_gives_up_after_the_lookback(httpx_mock: HTTPXMock):
    from datetime import UTC, datetime

    for _ in range(3):
        httpx_mock.add_response(method="HEAD", status_code=404)
    with pytest.raises(RuntimeError, match="no PSC snapshot"):
        psc_graph.latest_snapshot_url(today=datetime(2026, 9, 7, tzinfo=UTC), lookback_days=2)


def test_iter_snapshot_records_inflates_the_member_on_the_fly(snapshot: Path):
    with open(snapshot, "rb") as fh:
        records = list(psc_graph.iter_snapshot_records(fh))
    assert len(records) == len(RECORDS)
    assert records[0]["company_number"] == "00675216"
    assert records[0]["data"]["name"] == "Sandymere Ltd"


def test_iter_snapshot_records_refuses_a_stored_member(tmp_path: Path):
    path = tmp_path / "stored.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("x.txt", "{}\n")
    with open(path, "rb") as fh, pytest.raises(ValueError, match="deflate"):
        list(psc_graph.iter_snapshot_records(fh))


# ---------------------------------------------------------------------------
# The build
# ---------------------------------------------------------------------------


def test_build_keeps_active_known_kinds_only(store_path: Path):
    conn = sqlite3.connect(store_path)
    rows = conn.execute(
        "SELECT company_number, kind, name, reg_number, ceased_on FROM psc "
        "ORDER BY company_number, name"
    ).fetchall()
    names = [r[2] for r in rows]
    assert "Mr William James Timpson" not in names  # ceased
    assert all(r[4] is None for r in rows)
    kinds = {r[1] for r in rows}
    assert kinds <= {"i", "c", "l", "s"}
    # The PSC statement (unknown kind) and nothing else was dropped besides the ceased record.
    assert len(rows) == len(RECORDS) - 2


def test_build_normalises_the_filed_uk_number_and_leaves_others_null(store_path: Path):
    conn = sqlite3.connect(store_path)
    reg = dict(conn.execute("SELECT name, reg_number FROM psc WHERE kind IN ('c','l')").fetchall())
    assert reg["Timpson Group Plc"] == "02339274"  # filed as 2339274
    assert reg["The Scottish Partnership"] == "SC012345"  # filed as "sc 12345"
    assert reg["Offshore Parent Limited"] is None  # Jersey
    assert reg["Sandymere Ltd"] == "00323208"


def test_build_packs_natures_as_bytes_and_records_the_codes(store_path: Path):
    conn = sqlite3.connect(store_path)
    codes = dict(conn.execute("SELECT code_byte, code FROM nature_codes").fetchall())
    packed = conn.execute("SELECT natures FROM psc WHERE name = 'Sandymere Ltd'").fetchone()[0]
    assert [codes[b] for b in packed] == MAJ
    assert len(codes) == len(set(MAJ) | set(TRUST) | {"significant-influence-or-control"})


def test_build_writes_meta_counts_and_dates(store_path: Path):
    meta = dict(sqlite3.connect(store_path).execute("SELECT key, value FROM meta"))
    assert meta["schema_version"] == psc_graph.SCHEMA_VERSION
    assert meta[psc_graph.META_SNAPSHOT_DATE] == "2026-09-08"
    assert meta[psc_graph.META_ROW_COUNT] == str(len(RECORDS) - 2)
    assert meta[psc_graph.META_EDGE_COUNT] == "6"  # Sandymere, Group, Holdings, Scottish, A, B
    # 00675216, 00323208, 02339274, 02588889, 00000001, 00000002, SC012345,
    # 00000010, 00000011, 00000004
    assert int(meta[psc_graph.META_COMPANY_COUNT]) == 10
    assert meta[psc_graph.META_BUILT_AT].startswith("2026")
    assert meta[psc_graph.META_SOURCE_URL].startswith("file://")
    assert psc_graph.META_STREAM_TIMEPOINT not in meta


def test_build_renames_into_place_only_at_the_end(tmp_path: Path, snapshot: Path):
    out = tmp_path / "psc_graph.sqlite"
    psc_graph.build_psc_graph(out, source_url=f"file://{snapshot}")
    assert out.exists() and not out.with_suffix(".sqlite.build").exists()
    # meta.snapshot_date falls back to the URL's date when not given.
    meta = dict(sqlite3.connect(out).execute("SELECT key, value FROM meta"))
    assert meta["snapshot_date"] == "2026-09-08"


def test_build_dob_precision_is_the_register_s(store_path: Path):
    conn = sqlite3.connect(store_path)
    dob = dict(conn.execute("SELECT name, dob FROM psc WHERE kind = 'i'").fetchall())
    assert dob["Mr Charles William Holroyd"] == "1960-05"
    assert dob["Mr Matthew Samuel Davies"] == "1972"
    assert dob["Ms Highland Owner"] is None


# ---------------------------------------------------------------------------
# The store and the walk
# ---------------------------------------------------------------------------


def test_store_reads_a_company_s_active_pscs(store: psc_graph.PscGraphStore):
    rows = store.pscs("02588889")
    assert [r.name for r in rows] == [
        "Mr Charles William Holroyd",
        "Mr Matthew Samuel Davies",
        "Sir William John Anthony Timpson",
    ]
    assert rows[0].kind == INDIV
    assert rows[0].natures == tuple(TRUST)
    assert rows[0].dob == "1960-05" and rows[0].nationality == "British"
    assert not rows[0].followable
    assert store.pscs("2588889") == rows  # a filed spelling is normalised
    assert store.pscs("not a number") == []
    assert store.pscs("99999999") == []


def test_store_meta_and_schema(store: psc_graph.PscGraphStore):
    assert store.schema_version == "1"
    assert store.meta()[psc_graph.META_SNAPSHOT_DATE] == "2026-09-08"


def test_walk_follows_the_timpson_chain(store: psc_graph.PscGraphStore):
    walk = store.walk("00675216")
    assert list(walk.companies) == ["00675216", "00323208", "02339274", "02588889"]
    assert walk.depth_reached == 3
    assert walk.reads == 4
    assert walk.unfollowed == []
    assert [r.name for r in walk.companies["02588889"]] == [
        "Mr Charles William Holroyd",
        "Mr Matthew Samuel Davies",
        "Sir William John Anthony Timpson",
    ]


def test_walk_honours_the_depth_cap_and_says_so(store: psc_graph.PscGraphStore):
    walk = store.walk("00675216", max_depth=1)
    assert list(walk.companies) == ["00675216", "00323208"]
    assert walk.depth_reached == 1
    assert walk.unfollowed == [
        {
            "subject_company_number": "00323208",
            "name": "Timpson Group Plc",
            "registration_number": "02339274",
            "reason": "max_depth_reached",
        }
    ]


def test_walk_honours_the_fan_out_cap(store: psc_graph.PscGraphStore):
    walk = store.walk("00675216", max_related=1)
    assert list(walk.companies) == ["00675216", "00323208"]
    assert walk.unfollowed[0]["reason"] == "related_companies_cap_reached"


def test_walk_does_not_follow_a_non_uk_corporate_psc(store: psc_graph.PscGraphStore):
    walk = store.walk("00000001")
    assert list(walk.companies) == ["00000001"]
    assert walk.unfollowed == [
        {
            "subject_company_number": "00000001",
            "name": "Offshore Parent Limited",
            "reason": "not_uk_registered_or_not_a_company_number",
        }
    ]


def test_walk_follows_a_legal_person_filed_lower_case(store: psc_graph.PscGraphStore):
    walk = store.walk("00000002")
    assert list(walk.companies) == ["00000002", "SC012345"]
    assert walk.companies["SC012345"][0].name == "Ms Highland Owner"


def test_walk_terminates_on_a_cycle(store: psc_graph.PscGraphStore):
    walk = store.walk("00000010")
    assert list(walk.companies) == ["00000010", "00000011"]
    assert walk.unfollowed == []


def test_walk_of_an_unknown_or_bad_number_is_empty(store: psc_graph.PscGraphStore):
    assert store.walk("99999999").companies == {}
    assert store.walk("Uk").companies == {} and store.walk("Uk").subject == "Uk"


# ---------------------------------------------------------------------------
# The process-wide store, the boot rule and the periodic asset check
# ---------------------------------------------------------------------------


def _gz(path: Path) -> bytes:
    return gzip.compress(path.read_bytes())


def test_get_store_needs_a_configured_existing_file(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    assert psc_graph.get_store() is None
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(tmp_path / "missing.sqlite"))
    get_settings.cache_clear()
    assert psc_graph.get_store() is None
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    get_settings.cache_clear()
    s = psc_graph.get_store()
    assert s is not None and s is psc_graph.get_store()  # opened once
    psc_graph.reload_store()
    assert psc_graph.get_store() is not s


def test_boot_does_nothing_without_a_file_path(httpx_mock: HTTPXMock):
    note = psc_graph.warm_psc_graph_db()
    assert "not configured" in note["psc_graph"]
    assert psc_graph.state()["last_outcome"] == "skipped_not_configured"
    assert httpx_mock.get_requests() == []


def test_boot_downloads_when_the_file_is_absent(
    store_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
):
    target = tmp_path / "disk" / "psc_graph.sqlite"
    url = "https://example.test/psc_graph.sqlite.gz"
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(target))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", url)
    get_settings.cache_clear()
    httpx_mock.add_response(
        url=url, content=_gz(store_path), headers={"Last-Modified": "Tue, 08 Sep 2026 10:55:00 GMT"}
    )
    note = psc_graph.warm_psc_graph_db()
    assert note["psc_graph"].startswith(f"downloaded: {target}")
    assert target.exists() and not target.with_suffix(".download").exists()
    assert ep.read_meta(target)[ep.ASSET_STAMP_KEY] == "Tue, 08 Sep 2026 10:55:00 GMT"
    assert psc_graph.get_store() is not None
    assert psc_graph.get_store().walk("00675216").depth_reached == 3
    st = psc_graph.state()
    assert st["last_outcome"] == "downloaded" and st["replaced"] == 1 and st["runs"] == 1


def test_boot_keeps_a_stamped_file_whose_asset_is_unchanged(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
):
    url = "https://example.test/psc_graph.sqlite.gz"
    ep.record_asset_stamp(store_path, "Tue, 08 Sep 2026 10:55:00 GMT")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", url)
    get_settings.cache_clear()
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Tue, 08 Sep 2026 10:55:00 GMT"}
    )
    note = psc_graph.warm_psc_graph_db()
    assert note["psc_graph"].startswith("already present")
    assert [r.method for r in httpx_mock.get_requests()] == ["HEAD"]
    assert psc_graph.state()["last_outcome"] == "kept"


def test_boot_replaces_the_file_when_the_asset_differs(
    store_path: Path,
    tmp_path: Path,
    snapshot: Path,
    monkeypatch: pytest.MonkeyPatch,
    httpx_mock: HTTPXMock,
):
    """Yesterday's file on disk, today's asset published: the file is
    replaced and the open store reopened on the new inode."""
    url = "https://example.test/psc_graph.sqlite.gz"
    ep.record_asset_stamp(store_path, "Mon, 07 Sep 2026 10:55:00 GMT")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", url)
    get_settings.cache_clear()
    old = psc_graph.get_store()
    assert old is not None
    newer = tmp_path / "newer.sqlite"
    psc_graph.build_psc_graph(newer, source_url=f"file://{snapshot}", snapshot_date="2026-09-09")
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Tue, 08 Sep 2026 10:55:00 GMT"}
    )
    httpx_mock.add_response(
        method="GET",
        url=url,
        content=_gz(newer),
        headers={"Last-Modified": "Tue, 08 Sep 2026 10:55:00 GMT"},
    )
    note = psc_graph.warm_psc_graph_db()
    assert note["psc_graph"].startswith("replaced")
    new = psc_graph.get_store()
    assert new is not old
    assert new.meta()[psc_graph.META_SNAPSHOT_DATE] == "2026-09-09"
    assert ep.read_meta(store_path)[ep.ASSET_STAMP_KEY] == "Tue, 08 Sep 2026 10:55:00 GMT"
    assert psc_graph.state()["last_outcome"] == "replaced"


def test_boot_keeps_the_file_when_the_asset_cannot_be_checked(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
):
    url = "https://example.test/psc_graph.sqlite.gz"
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", url)
    get_settings.cache_clear()
    httpx_mock.add_response(method="HEAD", url=url, status_code=503)
    note = psc_graph.warm_psc_graph_db()
    assert note["psc_graph"].startswith("already present")
    assert psc_graph.get_store() is not None


def test_boot_counts_a_failed_download_and_keeps_serving(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
):
    url = "https://example.test/psc_graph.sqlite.gz"
    ep.record_asset_stamp(store_path, "Mon, 07 Sep 2026 10:55:00 GMT")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", url)
    get_settings.cache_clear()
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Tue, 08 Sep 2026 10:55:00 GMT"}
    )
    httpx_mock.add_response(method="GET", url=url, status_code=500)
    note = psc_graph.warm_psc_graph_db()
    assert note["psc_graph"].startswith("failed")
    st = psc_graph.state()
    assert st["last_outcome"] == "failed" and st["failures"] == 1 and "500" in st["last_error"]
    assert psc_graph.get_store() is not None  # the old file still serves
    assert psc_graph.get_store().walk("00675216").depth_reached == 3


def test_boot_with_a_file_and_no_url_uses_what_is_there(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
):
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", "")
    get_settings.cache_clear()
    assert psc_graph.warm_psc_graph_db()["psc_graph"].startswith("present")
    assert httpx_mock.get_requests() == []


def test_refresh_once_runs_the_boot_rule_and_returns_the_outcome(
    store_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
):
    url = "https://example.test/psc_graph.sqlite.gz"
    ep.record_asset_stamp(store_path, "Tue, 08 Sep 2026 10:55:00 GMT")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_URL", url)
    get_settings.cache_clear()
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Tue, 08 Sep 2026 10:55:00 GMT"}
    )
    assert psc_graph.refresh_once() == "kept"
    assert psc_graph.state()["runs"] == 1


async def test_refresh_loop_sleeps_first_then_checks(monkeypatch: pytest.MonkeyPatch):
    import asyncio

    calls: list[str] = []
    monkeypatch.setattr(psc_graph, "refresh_once", lambda: calls.append("run") or "kept")
    task = asyncio.create_task(psc_graph.refresh_loop(0.01))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls and psc_graph.state()["enabled"] is True


def test_lifespan_starts_the_loop_only_when_a_file_is_configured(
    store_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from opencheck import app as app_module

    started: list[float] = []

    async def _fake_loop(interval: float) -> None:
        started.append(interval)
        import asyncio

        await asyncio.sleep(3600)

    monkeypatch.setattr(psc_graph, "refresh_loop", _fake_loop)
    monkeypatch.setattr(app_module, "_warm_caches_background", _noop_warm)
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_REFRESH_INTERVAL_S", "123")
    get_settings.cache_clear()
    with TestClient(app):
        pass
    assert started == []  # no file configured
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    get_settings.cache_clear()
    with TestClient(app):
        pass
    assert started == [123.0]


async def _noop_warm() -> None:
    return None


# ---------------------------------------------------------------------------
# GET /pscgraph
# ---------------------------------------------------------------------------


def test_pscgraph_endpoint_from_cold():
    r = TestClient(app).get("/pscgraph")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-store"
    body = r.json()
    assert body["configured"] is False and body["available"] is False
    assert body["store"] is None
    assert body["flags"] == {"ch_graph_first": False}
    assert body["refresh"]["runs"] == 0


def test_pscgraph_endpoint_describes_the_file_not_its_contents(
    store_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    get_settings.cache_clear()
    body = TestClient(app).get("/pscgraph").json()
    assert body["configured"] and body["available"]
    assert body["store"]["snapshot_date"] == "2026-09-08"
    assert body["store"]["row_count"] == len(RECORDS) - 2
    assert body["store"]["edge_count"] == 6
    assert body["store"]["stream_timepoint"] is None
    text = json.dumps(body)
    for name in ("Timpson", "Holroyd", "Sandymere", "00675216", "SC012345"):
        assert name not in text
