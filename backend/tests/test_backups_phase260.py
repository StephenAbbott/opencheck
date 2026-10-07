"""Phase 260 — encrypted off-host backups of the watchlist and saved-report
files, against an in-memory GitHub (httpx.MockTransport) that behaves like
the releases API: tags, assets, uploads on the uploads host, the 302 to the
object store on download."""

from __future__ import annotations

import io
import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from opencheck import backups as bk
from opencheck import saved_reports as sr
from opencheck import watchlist as wl
from opencheck.config import get_settings

REPO = "someone/opencheck-backups"
PASS = "correct horse battery staple"


class FakeGitHub:
    def __init__(self, *, private: bool = True, exists: bool = True) -> None:
        self.private = private
        self.exists = exists
        self.release: dict[str, Any] | None = None
        self.assets: dict[int, dict[str, Any]] = {}
        self.blobs: dict[int, bytes] = {}
        self.next_id = 100
        self.auth_on_blob: list[str | None] = []

    def handler(self, req: httpx.Request) -> httpx.Response:
        url = req.url
        path = url.path
        if url.host == "objects.example" and path.startswith("/blob/"):
            self.auth_on_blob.append(req.headers.get("authorization"))
            return httpx.Response(200, content=self.blobs[int(path.rsplit("/", 1)[1])])
        if path == f"/repos/{REPO}":
            if not self.exists:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json={"full_name": REPO, "private": self.private})
        if path == f"/repos/{REPO}/releases/tags/sqlite-backups":
            return httpx.Response(200, json=self.release) if self.release else httpx.Response(404, json={})
        if path == f"/repos/{REPO}/releases" and req.method == "POST":
            self.release = {"id": 7, "tag_name": json.loads(req.content)["tag_name"]}
            return httpx.Response(201, json=self.release)
        m = re.fullmatch(rf"/repos/{REPO}/releases/7/assets", path)
        if m and url.host == "uploads.github.com" and req.method == "POST":
            aid = self.next_id
            self.next_id += 1
            body = req.read()
            self.blobs[aid] = body
            self.assets[aid] = {
                "id": aid, "name": url.params["name"], "label": url.params.get("label"), "size": len(body),
            }
            return httpx.Response(201, json=self.assets[aid])
        if m and req.method == "GET":
            page = int(url.params.get("page", "1"))
            per = int(url.params.get("per_page", "30"))
            items = sorted(self.assets.values(), key=lambda a: a["id"])
            return httpx.Response(200, json=items[(page - 1) * per : page * per])
        m = re.fullmatch(rf"/repos/{REPO}/releases/assets/(\d+)", path)
        if m:
            aid = int(m[1])
            if req.method == "DELETE":
                self.assets.pop(aid, None)
                return httpx.Response(204)
            return httpx.Response(302, headers={"Location": f"https://objects.example/blob/{aid}"})
        return httpx.Response(404, json={"message": f"unrouted {req.method} {url}"})

    def client(self) -> httpx.Client:
        return httpx.Client(
            transport=httpx.MockTransport(self.handler),
            follow_redirects=True,
            headers={"Authorization": "Bearer tok"},
        )


def _store(gh: FakeGitHub) -> bk.GitHubReleaseStore:
    return bk.GitHubReleaseStore(REPO, "tok", "sqlite-backups", client=gh.client())


@pytest.fixture
def files(tmp_path: Path) -> dict[str, Path]:
    w = tmp_path / "disk" / "watchlist.sqlite"
    store = wl.WatchlistStore(w, wl.Caps(10, 10))
    th = wl.token_hash(store.create_list())
    store.set_meta("opensanctions_version", "20260928125339-kkd")
    s = tmp_path / "disk" / "saved_reports.sqlite"
    srs = sr.SavedReportsStore(s, retention_days=90, max_total=10)
    srs.put_disposition("L" * 20, "r", '{"a": 1}', "2026-09-28T00:00:00Z")
    return {"watchlist": w, "saved_reports": s, "th": th}  # type: ignore[dict-item]


@pytest.fixture
def configured(files: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    monkeypatch.setenv("OPENCHECK_WATCHLIST_DB_FILE", str(files["watchlist"]))
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_DB_FILE", str(files["saved_reports"]))
    monkeypatch.setenv("OPENCHECK_BACKUP_GITHUB_REPO", REPO)
    monkeypatch.setenv("OPENCHECK_BACKUP_GITHUB_TOKEN", "ghp_example_token_value")
    monkeypatch.setenv("OPENCHECK_BACKUP_PASSPHRASE", PASS)
    get_settings.cache_clear()
    bk.reset_for_tests()
    yield files
    get_settings.cache_clear()
    bk.reset_for_tests()


# ---- the format -----------------------------------------------------------------


def test_encryption_round_trips_across_chunk_boundaries() -> None:
    for size in (0, 1, bk.CHUNK - 1, bk.CHUNK, bk.CHUNK + 1, 3 * bk.CHUNK + 17):
        data = bytes(range(256)) * (size // 256) + b"x" * (size % 256)
        enc = io.BytesIO()
        bk.encrypt_stream(io.BytesIO(data), enc, PASS)
        out = io.BytesIO()
        bk.decrypt_stream(io.BytesIO(enc.getvalue()), out, PASS)
        assert out.getvalue() == data, size


def _encrypted(data: bytes) -> bytes:
    enc = io.BytesIO()
    bk.encrypt_stream(io.BytesIO(data), enc, PASS)
    return enc.getvalue()


def test_a_wrong_passphrase_a_flipped_bit_or_a_truncation_does_not_restore() -> None:
    import os

    blob = _encrypted(os.urandom(3 * bk.CHUNK))  # random: gzip cannot shrink it below 3 chunks
    header = len(bk.MAGIC) + 16 + 7
    with pytest.raises(bk.BackupError, match="did not decrypt"):
        bk.decrypt_stream(io.BytesIO(blob), io.BytesIO(), "wrong")
    flipped = bytearray(blob)
    flipped[-5] ^= 1
    with pytest.raises(bk.BackupError):
        bk.decrypt_stream(io.BytesIO(bytes(flipped)), io.BytesIO(), PASS)
    # Cut exactly at a chunk boundary: every remaining chunk is intact, and
    # only the last-chunk flag tells the reader the file is short.
    cut = blob[: header + bk.CHUNK + 16]
    with pytest.raises(bk.BackupError):
        bk.decrypt_stream(io.BytesIO(cut), io.BytesIO(), PASS)
    with pytest.raises(bk.BackupError, match="bad header"):
        bk.decrypt_stream(io.BytesIO(b"SQLite format 3\x00"), io.BytesIO(), PASS)


def test_the_plaintext_is_not_in_the_file() -> None:
    blob = _encrypted(b"STEPHEN-WATCHES-THIS-LEI " * 1000)
    assert b"STEPHEN" not in blob and not blob[len(bk.MAGIC):].startswith(b"\x1f\x8b")


# ---- snapshot and restore ------------------------------------------------------------


def test_a_backup_of_a_live_wal_file_restores_with_its_rows(configured: dict[str, Path], tmp_path: Path) -> None:
    src = configured["watchlist"]
    # A write sitting in the WAL, not yet checkpointed into the main file.
    live = wl.WatchlistStore(src, wl.Caps(10, 10))
    live.set_meta("in_the_wal", "yes")
    work = tmp_path / "work"
    work.mkdir()
    enc, version = bk.build_backup(src, work, PASS)
    assert version == len(wl.MIGRATIONS)  # 2 since Phase 303
    out = tmp_path / "restored" / "watchlist.sqlite"
    with enc.open("rb") as fh:
        assert bk.restore(fh, out, PASS) == len(wl.MIGRATIONS)
    restored = wl.WatchlistStore(out, wl.Caps(10, 10))
    assert restored.get_meta("in_the_wal") == "yes"
    assert restored.get_meta("opensanctions_version") == "20260928125339-kkd"
    assert restored.list_exists(configured["th"])  # type: ignore[arg-type]
    with enc.open("rb") as fh, pytest.raises(bk.BackupError, match="exists"):
        bk.restore(fh, out, PASS)


def test_a_corrupt_file_is_never_uploaded(tmp_path: Path) -> None:
    bad = tmp_path / "watchlist.sqlite"
    bad.write_bytes(b"SQLite format 3\x00" + b"\x00" * 4000)
    with pytest.raises((bk.BackupError, sqlite3.DatabaseError)):
        bk.build_backup(bad, tmp_path, PASS)


# ---- the task -------------------------------------------------------------------------


def test_run_once_backs_up_both_files_and_they_restore(configured: dict[str, Path], tmp_path: Path) -> None:
    gh = FakeGitHub()
    results = bk.run_once(client=gh.client())
    assert [r["action"] for r in results] == ["uploaded", "uploaded"]
    names = sorted(a["name"] for a in gh.assets.values())
    assert names[0].startswith("saved_reports-") and names[1].startswith("watchlist-")
    labels = {a["name"].split("-")[0]: a["label"] for a in gh.assets.values()}
    assert labels["saved_reports"].endswith("schema v1")
    assert labels["watchlist"].endswith(f"schema v{len(wl.MIGRATIONS)}")  # 2 since Phase 303
    state = bk.state()
    assert state["files"]["watchlist"]["last_ok_at"] and state["files"]["saved_reports"]["schema_version"] == 1

    store = _store(gh)
    [latest] = store.backups("saved_reports")
    buf = io.BytesIO()
    store.download(latest["id"], buf)
    assert gh.auth_on_blob == [None]  # the token never goes to the object store
    out = tmp_path / "back" / "saved_reports.sqlite"
    bk.restore(io.BytesIO(buf.getvalue()), out, PASS)
    assert sr.SavedReportsStore(out, retention_days=90, max_total=10).get_disposition("L" * 20, "r") == '{"a": 1}'


def test_a_backup_is_due_by_the_release_not_by_the_clock(configured: dict[str, Path]) -> None:
    gh = FakeGitHub()
    t0 = datetime(2026, 9, 28, 3, 0, tzinfo=UTC)
    store = _store(gh)
    src = configured["watchlist"]
    kw = {"keep": 14, "interval_s": 86400.0}
    assert bk.backup_file(store, src, PASS, now=t0, **kw)["action"] == "uploaded"
    # A restart an hour later (new process, fresh state) asks the release.
    bk.reset_for_tests()
    later = bk.backup_file(_store(gh), src, PASS, now=t0 + timedelta(hours=1), **kw)
    assert later["action"] == "not_due"
    assert bk.backup_file(_store(gh), src, PASS, now=t0 + timedelta(days=1), **kw)["action"] == "uploaded"
    assert len(gh.assets) == 2


def test_only_the_newest_are_kept(configured: dict[str, Path]) -> None:
    gh = FakeGitHub()
    t0 = datetime(2026, 9, 1, tzinfo=UTC)
    for day in range(5):
        bk.backup_file(_store(gh), configured["watchlist"], PASS, keep=3, interval_s=1, now=t0 + timedelta(days=day))
    kept = sorted(a["name"] for a in gh.assets.values())
    assert kept == [bk.asset_name("watchlist", t0 + timedelta(days=d)) for d in (2, 3, 4)]


def test_a_public_repository_is_refused(configured: dict[str, Path]) -> None:
    gh = FakeGitHub(private=False)
    results = bk.run_once(client=gh.client())
    assert {r["action"] for r in results} == {"failed"}
    assert "not private" in results[0]["error"]
    assert gh.assets == {} and gh.release is None
    assert "not private" in bk.state()["files"]["watchlist"]["last_error"]


def test_a_missing_repository_says_what_to_do(configured: dict[str, Path]) -> None:
    results = bk.run_once(client=FakeGitHub(exists=False).client())
    assert "create it as a private repository" in results[0]["error"]


def test_nothing_runs_unless_all_three_settings_are_set(configured: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENCHECK_BACKUP_PASSPHRASE")
    get_settings.cache_clear()
    gh = FakeGitHub()
    assert bk.run_once(client=gh.client()) == []
    assert gh.release is None


def test_errors_never_carry_the_token_or_passphrase(configured: dict[str, Path]) -> None:
    def boom(req: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "Bad credentials"})

    client = httpx.Client(transport=httpx.MockTransport(boom), headers={"Authorization": "Bearer ghp_example_token_value"})
    results = bk.run_once(client=client)
    text = json.dumps(results) + json.dumps(bk.state())
    assert "401" in text
    assert "ghp_example_token_value" not in text and PASS not in text


def test_watchstats_reports_backups_without_the_repository(configured: dict[str, Path]) -> None:
    from fastapi.testclient import TestClient

    from opencheck.app import app

    bk.run_once(client=FakeGitHub().client())
    body = TestClient(app).get("/watchstats").json()
    assert body["backups"]["files"]["watchlist"]["schema_version"] == len(wl.MIGRATIONS)
    assert REPO not in json.dumps(body) and "tok" not in json.dumps(body["backups"])


def test_the_restore_script_restores_a_file(configured: dict[str, Path], tmp_path: Path, capsys) -> None:  # noqa: ANN001
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "restore_backup", Path(__file__).resolve().parents[1] / "scripts" / "restore_backup.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    work = tmp_path / "w"
    work.mkdir()
    enc, _ = bk.build_backup(configured["watchlist"], work, PASS)
    out = tmp_path / "r.sqlite"
    assert mod.main(["restore-file", str(enc), "--out", str(out)]) == 0
    assert "integrity ok" in capsys.readouterr().out
    assert wl.WatchlistStore(out, wl.Caps(1, 1)).get_meta("opensanctions_version") == "20260928125339-kkd"
