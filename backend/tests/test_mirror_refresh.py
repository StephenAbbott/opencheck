"""Phase 180 — the in-process delta refresh and the persistent-disk boot rules.

The mirror is rebuilt in full monthly; between rebuilds ``mirror_refresh``
applies GLEIF's Golden Copy deltas in-process. Pinned here: delta selection by
gap; a delta applied through the same loaders the full build uses, compressed
at insert with the file's own dictionary; the read-only store seeing the
change without reopening; idempotence (a delta re-applied changes nothing);
the watermark advancing only on success and staying put on any failure; the
release asset re-downloaded when the gap exceeds the widest delta; a v1 file
left alone; the ``/mirror`` payload carrying the task's state; and the boot
rules for a file on a persistent disk (absent → download; the asset is not
the one the file came from → replace; otherwise keep).

Phase 181 changed "not the one the file came from" from a plain ``built_at``
comparison to the asset's ``Last-Modified`` stamped into the file: the
workflow uploads minutes after the build finishes, so the asset was always
"newer" and every deploy re-downloaded it. The boot tests carry the
production numbers of 2026-09-07.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import entity_pages as ep
from opencheck import mirror_refresh as mr
from opencheck import mirrorstats
from opencheck.app import app
from opencheck.config import get_settings
from tests.test_gleif_mirror_store import (
    EASY,
    LEI2_HEADER,
    LEI2_ROWS,
    PARENT,
    REPEX_HEADER,
    REPEX_ROWS,
    RR_HEADER,
    RR_ROWS,
    TOP,
    _lei2_row,
    _repex_row,
    _rr_row,
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

WATERMARK = "2026-09-07 16:00:00"
NEW_LEI = "2138000000000000N180"


def _build_mirror(path: Path, tmp: Path) -> Path:
    conn = sqlite3.connect(path)
    ensure_schema(conn)
    assert load_lei2(conn, _write_csv(tmp / "lei2.csv", LEI2_HEADER, LEI2_ROWS)) == 5
    assert load_rr(conn, _write_csv(tmp / "rr.csv", RR_HEADER, RR_ROWS)) == 6
    assert load_repex(conn, _write_csv(tmp / "repex.csv", REPEX_HEADER, REPEX_ROWS)) == 3
    assert compress_detail_column(conn) == 5
    write_meta(
        conn,
        built_at="2026-09-07T17:00:00+00:00",
        source_publish_date=WATERMARK,
        source_publish_datetime=WATERMARK,
        schema_version=ep.SCHEMA_VERSION,
        record_count="5",
    )
    conn.close()
    return path


def _delta_publish(tmp: Path, publish_date: str, *, delta: str = "any") -> dict:
    """A publish-API entry whose ``delta`` files are local CSVs: one new LEI,
    EASY renamed, EASY's direct parent record retired, TOP's direct exception
    withdrawn."""
    lei2 = _write_csv(tmp / f"lei2-{delta}.csv", LEI2_HEADER, [
        _lei2_row(**{
            "LEI": NEW_LEI, "Entity.LegalName": "Mirror Newcomer AS",
            "Entity.LegalAddress.City": "OSLO", "Entity.LegalAddress.Country": "NO",
            "Entity.LegalJurisdiction": "NO", "Entity.EntityStatus": "ACTIVE",
            "Entity.RegistrationAuthority.RegistrationAuthorityID": "RA000472",
            "Entity.RegistrationAuthority.RegistrationAuthorityEntityID": "999888777",
            "Registration.InitialRegistrationDate": publish_date.replace(" ", "T") + "Z",
            "Registration.LastUpdateDate": publish_date.replace(" ", "T") + "Z",
            "Registration.RegistrationStatus": "ISSUED",
        }),
        _lei2_row(**{
            "LEI": EASY, "Entity.LegalName": "EASY POWER (RENAMED)",
            "Entity.LegalAddress.City": "ATHENS", "Entity.LegalAddress.Country": "GR",
            "Entity.LegalJurisdiction": "GR", "Entity.EntityStatus": "ACTIVE",
            "Entity.RegistrationAuthority.RegistrationAuthorityID": "RA000685",
            "Entity.RegistrationAuthority.RegistrationAuthorityEntityID": "007132601000",
            "Registration.LastUpdateDate": publish_date.replace(" ", "T") + "Z",
            "Registration.RegistrationStatus": "ISSUED",
        }),
    ])
    rr = _write_csv(tmp / f"rr-{delta}.csv", RR_HEADER, [
        _rr_row(EASY, PARENT, "IS_DIRECTLY_CONSOLIDATED_BY", reg="RETIRED"),
    ])
    repex = _write_csv(tmp / f"repex-{delta}.csv", REPEX_HEADER, [
        _repex_row(TOP, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NON_CONSOLIDATING",
                   deleted="2026-09-08T01:00:00Z"),
    ])
    # As the real publish entry does, every window is listed; here they all
    # point at the same files, so the test can check which one was chosen.
    files = {
        kind: {"delta_files": {w: {"csv": {"url": f"file://{p}"}} for w, _ in mr.DELTA_WINDOWS}}
        for kind, p in (("lei2", lei2), ("rr", rr), ("repex", repex))
    }
    return {"publish_date": publish_date, **files}


@pytest.fixture
def mirror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db = _build_mirror(tmp_path / "entity_pages.sqlite", tmp_path)
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_URL", raising=False)
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "0")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    mr.reset_for_tests()
    mirrorstats.reset_for_tests()
    yield db
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    mr.reset_for_tests()
    mirrorstats.reset_for_tests()


# ---------------------------------------------------------------------------
# Delta selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("hours", "expected"),
    [(0.5, "IntraDay"), (8, "IntraDay"), (9, "LastDay"), (24, "LastDay"), (25, "LastWeek"),
     (24 * 7, "LastWeek"), (24 * 8, "LastMonth"), (24 * 31, "LastMonth"), (24 * 32, None)],
)
def test_the_smallest_delta_covering_the_gap_is_chosen(hours: float, expected: str | None) -> None:
    assert mr.choose_delta(timedelta(hours=hours)) == expected


def test_publish_datetime_parses_gleifs_format() -> None:
    assert mr.parse_publish_datetime("2026-09-07 16:00:00") == datetime(
        2026, 9, 7, 16, tzinfo=UTC
    )
    assert mr.parse_publish_datetime("2026-09-07") == datetime(2026, 9, 7, tzinfo=UTC)
    assert mr.parse_publish_datetime("") is None and mr.parse_publish_datetime("soon") is None


# ---------------------------------------------------------------------------
# Applying a delta
# ---------------------------------------------------------------------------


def test_a_delta_lands_through_the_builders_loaders_and_the_reader_sees_it(
    mirror: Path, tmp_path: Path
) -> None:
    store = ep.get_store()
    assert store is not None and store.get(NEW_LEI) is None
    assert store.get(EASY).direct_parent_lei == PARENT
    assert set(store.exceptions(TOP)) == {"direct", "ultimate"}

    publish = _delta_publish(tmp_path, "2026-09-08 02:00:00")
    assert mr.refresh_once(publish=publish) == "applied"

    # The same store object — no reopen — reads the upserts.
    row = store.get(NEW_LEI)
    assert row is not None and row.name == "Mirror Newcomer AS"
    assert row.detail == {
        "legalAddress": {"city": "OSLO", "country": "NO"},
        "registeredAt": {"id": "RA000472", "other": None},
        "registeredAs": "999888777",
    }
    assert store.get(EASY).name == "EASY POWER (RENAMED)"
    assert store.get(EASY).direct_parent_lei is None  # RETIRED → column cleared
    assert set(store.exceptions(TOP)) == {"ultimate"}  # DeletedAt honoured
    # Written compressed at insert with the file's dictionary — nothing left as text.
    conn = sqlite3.connect(mirror)
    assert conn.execute(
        "SELECT COUNT(*) FROM entities WHERE typeof(detail_json) = 'text'"
    ).fetchone()[0] == 0
    meta = dict(conn.execute("SELECT key, value FROM meta"))
    conn.close()
    # The watermark advanced, the full build's date did not.
    assert meta["source_publish_datetime"] == "2026-09-08 02:00:00"
    assert meta["mode"] == "delta:LastDay"  # a 10 h gap: past IntraDay's window
    assert meta["built_at"] == "2026-09-07T17:00:00+00:00"
    assert meta["record_count"] == "6"
    assert store.watermark() == datetime(2026, 9, 8, 2, tzinfo=UTC)

    state = mr.state()
    assert state["last_outcome"] == "applied" and state["last_delta"] == "LastDay"
    assert state["rows_applied"] == {
        "entities": 2, "relationships": 1, "exceptions": 1, "compressed": 0,
    }
    assert state["last_gap_hours"] == 10.0 and state["applied"] == 1


def test_re_applying_a_delta_changes_nothing(mirror: Path, tmp_path: Path) -> None:
    publish = _delta_publish(tmp_path, "2026-09-08 02:00:00")
    assert mr.refresh_once(publish=publish) == "applied"
    before = sqlite3.connect(mirror).execute(
        "SELECT lei, name, direct_parent_lei, detail_json FROM entities ORDER BY lei"
    ).fetchall()
    # Same publish again: nothing to do.
    assert mr.refresh_once(publish=publish) == "up_to_date"
    # Forced through apply_delta a second time: an idempotent upsert.
    mr.apply_delta(mirror, publish, "LastDay")
    after = sqlite3.connect(mirror).execute(
        "SELECT lei, name, direct_parent_lei, detail_json FROM entities ORDER BY lei"
    ).fetchall()
    assert after == before


def test_a_wider_gap_picks_a_wider_delta(mirror: Path, tmp_path: Path) -> None:
    assert mr.refresh_once(publish=_delta_publish(tmp_path, "2026-09-07 20:00:00")) == "applied"
    assert mr.state()["last_delta"] == "IntraDay"  # 4 h
    assert mr.refresh_once(publish=_delta_publish(tmp_path, "2026-09-10 02:00:00")) == "applied"
    assert mr.state()["last_delta"] == "LastWeek"  # 2.25 days


def test_the_watermark_stays_put_when_a_delta_fails(mirror: Path, tmp_path: Path) -> None:
    publish = _delta_publish(tmp_path, "2026-09-08 02:00:00")
    # A CSV whose columns moved: the loader refuses loudly, nothing commits.
    broken = _write_csv(tmp_path / "broken.csv", ["Something", "Else"], [["a", "b"]])
    publish["lei2"]["delta_files"]["LastDay"]["csv"]["url"] = f"file://{broken}"
    assert mr.refresh_once(publish=publish) == "failed"
    store = ep.get_store()
    assert store is not None
    assert store.watermark() == datetime(2026, 9, 7, 16, tzinfo=UTC)
    assert store.get(NEW_LEI) is None
    state = mr.state()
    assert state["failures"] == 1 and "LastDay" in state["last_error"]
    assert state["outcomes"]["failed"] == 1


def test_a_publish_api_that_does_not_answer_is_a_counted_failure(
    mirror: Path, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(status_code=503)
    assert mr.refresh_once() == "failed"
    assert mr.state()["last_error"].startswith("publish API")
    assert ep.get_store().watermark() == datetime(2026, 9, 7, 16, tzinfo=UTC)


def test_up_to_date_and_older_publishes_change_nothing(mirror: Path) -> None:
    assert mr.refresh_once(publish={"publish_date": WATERMARK}) == "up_to_date"
    assert mr.refresh_once(publish={"publish_date": "2026-09-01 02:00:00"}) == "up_to_date"
    assert mr.state()["runs"] == 2 and mr.state()["applied"] == 0


def test_a_v1_file_is_left_alone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "v1.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(_v1_schema())
    conn.commit()
    conn.close()
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "0")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    mr.reset_for_tests()
    try:
        assert mr.refresh_once(publish={"publish_date": "2026-09-08 02:00:00"}) == (
            "skipped_not_mirror"
        )
        assert db.stat().st_size == sqlite3.connect(db).execute(
            "PRAGMA page_count"
        ).fetchone()[0] * sqlite3.connect(db).execute("PRAGMA page_size").fetchone()[0]
    finally:
        get_settings.cache_clear()
        ep.reset_store_for_tests()
        mr.reset_for_tests()


def test_no_store_configured_is_a_skip() -> None:
    mr.reset_for_tests()
    assert mr.refresh_once(publish={"publish_date": "2026-09-08 02:00:00"}) == "skipped_no_store"


# ---------------------------------------------------------------------------
# Beyond the widest delta: the release asset
# ---------------------------------------------------------------------------


def test_a_gap_beyond_a_month_reloads_the_release_asset(
    mirror: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
) -> None:
    # The asset: a fresh mirror whose watermark is the new publish.
    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    fresh = _build_mirror(fresh_dir / "fresh.sqlite", fresh_dir)
    conn = sqlite3.connect(fresh)
    write_meta(conn, source_publish_datetime="2026-10-15 02:00:00",
               source_publish_date="2026-10-15 02:00:00")
    conn.close()
    archive = gzip.compress(fresh.read_bytes())
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_URL", "https://example.test/entity_pages.sqlite.gz")
    get_settings.cache_clear()
    httpx_mock.add_response(url="https://example.test/entity_pages.sqlite.gz", content=archive)

    old_store = ep.get_store()
    assert old_store is not None
    assert mr.refresh_once(publish={"publish_date": "2026-10-15 02:00:00"}) == "asset_replaced"
    new_store = ep.get_store()
    assert new_store is not None and new_store is not old_store
    assert new_store.watermark() == datetime(2026, 10, 15, 2, tzinfo=UTC)
    assert mr.state()["last_delta"] == "asset"
    leftovers = [p.name for p in mirror.parent.iterdir() if p.suffix in (".sqlite", ".download")]
    assert leftovers == ["entity_pages.sqlite"]


def test_a_gap_beyond_a_month_with_no_asset_url_is_a_skip(mirror: Path) -> None:
    assert mr.refresh_once(publish={"publish_date": "2026-10-15 02:00:00"}) == (
        "skipped_gap_too_large"
    )
    assert ep.get_store().watermark() == datetime(2026, 9, 7, 16, tzinfo=UTC)


# ---------------------------------------------------------------------------
# /mirror carries the task's state
# ---------------------------------------------------------------------------


def test_mirror_endpoint_reports_the_refresh(mirror: Path, tmp_path: Path) -> None:
    mr.refresh_once(publish=_delta_publish(tmp_path, "2026-09-08 02:00:00"))
    body = TestClient(app).get("/mirror").json()
    refresh = body["refresh"]
    assert refresh["last_outcome"] == "applied" and refresh["last_delta"] == "LastDay"
    assert refresh["outcomes"]["applied"] == 1
    assert body["store"]["watermark"] == "2026-09-08 02:00:00"
    text = json.dumps(body)
    assert NEW_LEI not in text and EASY not in text and "Newcomer" not in text


# ---------------------------------------------------------------------------
# Boot on a persistent disk
# ---------------------------------------------------------------------------


def _gz_asset(path: Path) -> bytes:
    return gzip.compress(path.read_bytes())


def test_boot_downloads_to_the_configured_file_when_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src = _build_mirror(src_dir / "src.sqlite", src_dir)
    target = tmp_path / "disk" / "entity_pages.sqlite"
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(target))
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_URL", "https://example.test/entity_pages.sqlite.gz")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    httpx_mock.add_response(
        url="https://example.test/entity_pages.sqlite.gz",
        content=_gz_asset(src),
        headers={"Last-Modified": "Mon, 07 Sep 2026 19:55:14 GMT"},
    )
    try:
        note = ep.warm_entity_pages_db()["entity_pages"]
        assert note.startswith(f"downloaded: {target}")
        assert ep.get_store() is not None and ep.get_store().is_mirror
        # Phase 181: the file remembers which asset it came from.
        assert ep.read_meta(target)[ep.ASSET_STAMP_KEY] == "Mon, 07 Sep 2026 19:55:14 GMT"
        assert ep.read_meta(src).get(ep.ASSET_STAMP_KEY) is None
    finally:
        get_settings.cache_clear()
        ep.reset_store_for_tests()


def test_boot_keeps_a_file_whose_asset_was_uploaded_minutes_after_its_build(
    mirror: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
) -> None:
    """The Phase 180 defect, with the production numbers: the workflow
    uploads the asset *after* the build finishes, so the asset is always a
    couple of minutes newer than ``built_at`` and a plain ``>`` re-downloaded
    879 MB on every deploy. The file is kept — and stamped, so the next boot
    compares exactly."""
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_URL", "https://example.test/entity_pages.sqlite.gz")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    url = "https://example.test/entity_pages.sqlite.gz"
    conn = sqlite3.connect(mirror)
    write_meta(conn, built_at="2026-09-07T19:53:15+00:00")
    conn.close()
    assert ep.read_meta(mirror).get(ep.ASSET_STAMP_KEY) is None

    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Mon, 07 Sep 2026 19:55:14 GMT"}
    )
    assert ep.warm_entity_pages_db()["entity_pages"].startswith("already present")
    assert ep.read_meta(mirror)[ep.ASSET_STAMP_KEY] == "Mon, 07 Sep 2026 19:55:14 GMT"
    # Nothing was fetched: HEAD only.
    assert [r.method for r in httpx_mock.get_requests()] == ["HEAD"]


def test_boot_replaces_the_file_only_when_the_asset_differs_from_its_stamp(
    mirror: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, httpx_mock: HTTPXMock
) -> None:
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_URL", "https://example.test/entity_pages.sqlite.gz")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    url = "https://example.test/entity_pages.sqlite.gz"
    ep.record_asset_stamp(mirror, "Mon, 07 Sep 2026 19:55:14 GMT")

    # The same asset, however the header is spelled: kept, no GET.
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Mon, 07 Sep 2026 19:55:14 GMT"}
    )
    assert ep.warm_entity_pages_db()["entity_pages"].startswith("already present")
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Mon, 07 Sep 2026 20:55:14 +0100"}
    )
    assert ep.warm_entity_pages_db()["entity_pages"].startswith("already present")
    assert all(r.method == "HEAD" for r in httpx_mock.get_requests())

    # A different asset (the monthly rebuild): replaced, the store reopened on
    # the new file, and the new file stamped from the GET's own header.
    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    fresh = _build_mirror(fresh_dir / "fresh.sqlite", fresh_dir)
    conn = sqlite3.connect(fresh)
    write_meta(conn, built_at="2026-10-01T06:45:00+00:00",
               source_publish_datetime="2026-10-01 02:00:00")
    conn.close()
    old_store = ep.get_store()
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Thu, 01 Oct 2026 06:50:00 GMT"}
    )
    httpx_mock.add_response(
        method="GET", url=url, content=_gz_asset(fresh),
        headers={"Last-Modified": "Thu, 01 Oct 2026 06:50:00 GMT"},
    )
    assert ep.warm_entity_pages_db()["entity_pages"].startswith("downloaded")
    assert ep.get_store() is not old_store
    assert ep.get_store().watermark() == datetime(2026, 10, 1, 2, tzinfo=UTC)
    assert ep.read_meta(mirror)[ep.ASSET_STAMP_KEY] == "Thu, 01 Oct 2026 06:50:00 GMT"

    # The next boot sees the asset it has: kept.
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Thu, 01 Oct 2026 06:50:00 GMT"}
    )
    assert ep.warm_entity_pages_db()["entity_pages"].startswith("already present")

    # An asset that cannot be checked: the file on disk is kept, stamp intact.
    httpx_mock.add_response(method="HEAD", url=url, status_code=503)
    assert ep.warm_entity_pages_db()["entity_pages"].startswith("already present")
    assert ep.read_meta(mirror)[ep.ASSET_STAMP_KEY] == "Thu, 01 Oct 2026 06:50:00 GMT"


def test_an_unstamped_file_is_replaced_by_an_asset_well_after_its_build(
    mirror: Path, httpx_mock: HTTPXMock
) -> None:
    """The fallback rule for a file downloaded before Phase 181 or built
    locally: ``built_at`` plus an hour's slack."""
    url = "https://example.test/entity_pages.sqlite.gz"
    conn = sqlite3.connect(mirror)
    write_meta(conn, built_at="2026-09-07T19:53:15+00:00")
    conn.close()
    for header, expected in (
        ("Mon, 07 Sep 2026 19:55:14 GMT", "keep"),  # uploaded two minutes after the build
        ("Mon, 07 Sep 2026 20:53:15 GMT", "keep"),  # exactly the slack: still the same build
        ("Mon, 07 Sep 2026 20:53:16 GMT", "replace"),
        ("Thu, 01 Oct 2026 06:50:00 GMT", "replace"),  # the monthly rebuild
        ("Tue, 01 Sep 2026 11:59:51 GMT", "keep"),  # older than the file
    ):
        httpx_mock.add_response(method="HEAD", url=url, headers={"Last-Modified": header})
        assert ep.asset_check(url, mirror) == (expected, header), header


def test_the_asset_check_keeps_a_file_it_cannot_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    url = "https://example.test/x.gz"
    db = tmp_path / "nobuild.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(ep.SCHEMA)
    conn.commit()
    conn.close()
    httpx_mock.add_response(
        method="HEAD", url=url, headers={"Last-Modified": "Thu, 01 Oct 2026 06:50:00 GMT"}
    )
    assert ep.asset_check(url, db) == ("keep", "Thu, 01 Oct 2026 06:50:00 GMT")
    # No Last-Modified at all: nothing to compare, keep.
    httpx_mock.add_response(method="HEAD", url=url)
    assert ep.asset_check(url, db) == ("keep", None)
    # Not even a file: keep, and no exception.
    httpx_mock.add_response(method="HEAD", url=url)
    assert ep.asset_check(url, tmp_path / "missing.sqlite") == ("keep", None)


def test_a_local_only_file_is_used_as_found(mirror: Path) -> None:
    note = ep.warm_entity_pages_db()["entity_pages"]
    assert note.startswith(f"present: {mirror}; page cache warmed")


def test_the_lifespan_starts_the_refresh_loop_when_enabled(
    mirror: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "3600")
    get_settings.cache_clear()
    with TestClient(app):
        assert mr.state()["enabled"] is True
    assert mr.state()["enabled"] is False


def test_download_honours_file_urls(tmp_path: Path) -> None:
    """``mb._download`` copies a ``file://`` URL — what the tests and an
    operator replaying a delta they already hold rely on."""
    from opencheck import mirror_build as mb

    src = tmp_path / "delta.csv"
    src.write_text("LEI\n")
    out = tmp_path / "out"
    out.mkdir()
    dest = mb._download(f"file://{src}", out, "x")
    assert dest == out / "delta.csv" and dest.read_text() == "LEI\n"
