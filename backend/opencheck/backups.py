"""Off-host backups of OpenCheck's own SQLite files (Phase 260).

``watchlist.sqlite`` and ``saved_reports.sqlite`` are the two files OpenCheck
writes and promises to keep — a saved report is sold as a durable record whose
SHA-256 a reader can re-check — and both live on one Render disk. This module
copies each, once a day, to an asset of one release on a **private** GitHub
repository. Things that will be re-derived otherwise:

- **The copy is SQLite's online backup API** (``Connection.backup``), not a
  file copy: it is consistent while the watcher, the mirror-refresh hook and
  request handlers write, WAL included. The copy is ``PRAGMA
  integrity_check``-ed before it leaves the machine.
- **Encrypted, then uploaded, and only to a private repo.** Saved reports are
  unlisted capabilities and a watchlist says what someone watches, so the
  public ``StephenAbbott/opencheck`` releases the other artefacts use are out.
  The task asks GitHub whether the repository is private before every upload
  and refuses when it is not. The file is also encrypted — AES-256-GCM, key
  from ``OPENCHECK_BACKUP_PASSPHRASE`` by scrypt — so a leaked token or a
  repository made public later does not leak the contents.
- **Streamed, in 1 MiB chunks** (``_ChunkWriter``): a 100 MB saved-reports
  file must not be read into memory on an instance with a history of OOM
  kills. Each chunk is sealed on its own with a nonce made of a random
  prefix, the chunk counter and a last-chunk flag, so a truncated, reordered
  or spliced file fails to decrypt rather than restoring short.
- **"Due" is read off the release, not a clock in memory**: the newest asset
  for a file says when it was last backed up, so a deploy (which restarts the
  process several times a day) never resets the schedule and never skips it.
- **Restore** is ``backend/scripts/restore_backup.py`` — download or read a
  file, decrypt, decompress, integrity-check, and write a new file; it never
  overwrites without ``--force``.

Format: ``MAGIC (6) | salt (16) | nonce prefix (7) | chunk…``, each chunk
``ciphertext + 16-byte tag`` of up to ``CHUNK`` plaintext bytes, the
plaintext being the gzipped SQLite file.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import re
import secrets
import shutil
import sqlite3
import tempfile
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from .secret_scrub import describe_exception

log = logging.getLogger("opencheck.backups")

MAGIC = b"OCBK1\n"
CHUNK = 1 << 20
_TAG = 16
_SALT = 16
_PREFIX = 7
#: scrypt cost: 32 MiB and ~0.1 s per backup — once a day, not per request.
_SCRYPT = {"n": 1 << 15, "r": 8, "p": 1}
#: How often the task wakes to ask whether a backup is due.
CHECK_INTERVAL_S = 3600.0
#: The first check waits this long after boot — boot is busy enough.
FIRST_CHECK_DELAY_S = 600.0

GITHUB_API = "https://api.github.com"
GITHUB_UPLOADS = "https://uploads.github.com"
_ASSET_RE = re.compile(r"^(?P<stem>[a-z0-9_]+)-(?P<stamp>\d{8}T\d{6}Z)\.sqlite\.gz\.enc$")


class BackupError(RuntimeError):
    """A backup or restore that must not go ahead, in words fit for a log."""


# ---------------------------------------------------------------------------
# Encryption
# ---------------------------------------------------------------------------


def _key(passphrase: str, salt: bytes) -> bytes:
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

    if not passphrase:
        raise BackupError("No backup passphrase is set.")
    return Scrypt(salt=salt, length=32, **_SCRYPT).derive(passphrase.encode("utf-8"))


def _nonce(prefix: bytes, counter: int, last: bool) -> bytes:
    return prefix + counter.to_bytes(4, "big") + (b"\x01" if last else b"\x00")


class _ChunkWriter:
    """A file-like sink: bytes written to it are sealed chunk by chunk into
    ``out``. ``close()`` seals the final chunk (possibly empty) with the
    last-chunk flag, which is what lets a reader detect truncation."""

    def __init__(self, out: IO[bytes], passphrase: str):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        salt = secrets.token_bytes(_SALT)
        self._prefix = secrets.token_bytes(_PREFIX)
        self._aead = AESGCM(_key(passphrase, salt))
        self._out = out
        self._buf = bytearray()
        self._counter = 0
        out.write(MAGIC + salt + self._prefix)

    def write(self, data: bytes) -> int:
        self._buf += data
        while len(self._buf) > CHUNK:
            self._seal(bytes(self._buf[:CHUNK]), last=False)
            del self._buf[:CHUNK]
        return len(data)

    def flush(self) -> None:  # gzip calls it; sealing waits for a full chunk
        pass

    def _seal(self, chunk: bytes, *, last: bool) -> None:
        if self._counter >= 1 << 32:
            raise BackupError("Backup too large for the chunk counter.")
        nonce = _nonce(self._prefix, self._counter, last)
        self._out.write(self._aead.encrypt(nonce, chunk, MAGIC))
        self._counter += 1

    def close(self) -> None:
        self._seal(bytes(self._buf), last=True)
        self._buf.clear()


def encrypt_stream(src: IO[bytes], out: IO[bytes], passphrase: str) -> None:
    """Gzip ``src`` and seal it into ``out``."""
    sink = _ChunkWriter(out, passphrase)
    with gzip.GzipFile(fileobj=sink, mode="wb", mtime=0) as gz:  # type: ignore[arg-type]
        shutil.copyfileobj(src, gz, CHUNK)
    sink.close()


def _decrypted_chunks(src: IO[bytes], passphrase: str) -> Iterator[bytes]:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    head = src.read(len(MAGIC) + _SALT + _PREFIX)
    if len(head) < len(MAGIC) + _SALT + _PREFIX or not head.startswith(MAGIC):
        raise BackupError("Not an OpenCheck backup (bad header).")
    salt = head[len(MAGIC) : len(MAGIC) + _SALT]
    prefix = head[len(MAGIC) + _SALT :]
    aead = AESGCM(_key(passphrase, salt))
    counter = 0
    block = src.read(CHUNK + _TAG)
    while True:
        nxt = src.read(CHUNK + _TAG)
        last = not nxt
        try:
            yield aead.decrypt(_nonce(prefix, counter, last), block, MAGIC)
        except InvalidTag as exc:
            raise BackupError(
                "The backup did not decrypt: wrong passphrase, or the file is damaged or truncated."
            ) from exc
        if last:
            return
        counter += 1
        block = nxt


class _ChunkReader:
    """Read side of :func:`_decrypted_chunks`, as a file object for gzip."""

    def __init__(self, src: IO[bytes], passphrase: str):
        self._chunks = _decrypted_chunks(src, passphrase)
        self._buf = b""

    def read(self, n: int = -1) -> bytes:
        while n < 0 or len(self._buf) < n:
            try:
                self._buf += next(self._chunks)
            except StopIteration:
                break
        if n < 0:
            out, self._buf = self._buf, b""
        else:
            out, self._buf = self._buf[:n], self._buf[n:]
        return out


def decrypt_stream(src: IO[bytes], out: IO[bytes], passphrase: str) -> None:
    """Undo :func:`encrypt_stream` into ``out`` (the SQLite file's bytes)."""
    with gzip.GzipFile(fileobj=_ChunkReader(src, passphrase), mode="rb") as gz:  # type: ignore[arg-type]
        shutil.copyfileobj(gz, out, CHUNK)


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


def check_sqlite(path: Path) -> int:
    """``PRAGMA integrity_check`` must say ``ok``. Returns ``user_version``."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = [r[0] for r in conn.execute("PRAGMA integrity_check").fetchall()]
        if rows != ["ok"]:
            raise BackupError(f"{path.name} failed its integrity check: {'; '.join(rows[:3])}")
        return int(conn.execute("PRAGMA user_version").fetchone()[0])
    finally:
        conn.close()


def snapshot_sqlite(src: Path, dest: Path) -> int:
    """Consistent copy of a live SQLite file with the online backup API,
    integrity-checked. Returns the copy's ``user_version``."""
    if not src.is_file():
        raise BackupError(f"{src.name} does not exist.")
    source = sqlite3.connect(f"file:{src}?mode=ro", uri=True, timeout=30.0)
    target = sqlite3.connect(dest)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()
    # The copy is a standalone file, not in WAL mode, so it restores alone.
    conn = sqlite3.connect(dest)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
    finally:
        conn.close()
    return check_sqlite(dest)


def build_backup(src: Path, workdir: Path, passphrase: str) -> tuple[Path, int]:
    """Snapshot, encrypt and prove the encrypted file restores. Returns the
    encrypted file and the snapshot's schema version."""
    snap = workdir / f"{src.stem}.snapshot.sqlite"
    version = snapshot_sqlite(src, snap)
    enc = workdir / f"{src.stem}.sqlite.gz.enc"
    with snap.open("rb") as fh, enc.open("wb") as out:
        encrypt_stream(fh, out, passphrase)
    # Round-trip before upload: a backup that does not restore is not one.
    check = workdir / f"{src.stem}.verify.sqlite"
    with enc.open("rb") as fh, check.open("wb") as out:
        decrypt_stream(fh, out, passphrase)
    check_sqlite(check)
    check.unlink()
    snap.unlink()
    return enc, version


def restore(src: IO[bytes], dest: Path, passphrase: str, *, force: bool = False) -> int:
    """Decrypt a backup into ``dest`` and integrity-check it. Writes beside
    ``dest`` and renames, so a failed restore leaves nothing half-written.
    Returns the restored file's ``user_version``."""
    dest = Path(dest)
    if dest.exists() and not force:
        raise BackupError(f"{dest} exists; pass force to replace it.")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".restoring")
    try:
        with tmp.open("wb") as out:
            decrypt_stream(src, out, passphrase)
        version = check_sqlite(tmp)
        for suffix in ("-wal", "-shm"):
            stale = dest.with_name(dest.name + suffix)
            if stale.exists():
                stale.unlink()
        os.replace(tmp, dest)
        return version
    finally:
        if tmp.exists():
            tmp.unlink()


# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------


def asset_name(stem: str, when: datetime) -> str:
    return f"{stem}-{when.strftime('%Y%m%dT%H%M%SZ')}.sqlite.gz.enc"


def parse_asset_name(name: str) -> tuple[str, datetime] | None:
    m = _ASSET_RE.match(name or "")
    if not m:
        return None
    return m["stem"], datetime.strptime(m["stamp"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


class GitHubReleaseStore:
    """The one release that holds every backup, on a private repository."""

    def __init__(self, repo: str, token: str, tag: str, client: Any | None = None):
        import httpx

        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo or ""):
            raise BackupError("OPENCHECK_BACKUP_GITHUB_REPO must be owner/name.")
        self.repo = repo
        self.tag = tag
        self._own = client is None
        self.client = client or httpx.Client(
            timeout=httpx.Timeout(60.0, read=300.0),
            follow_redirects=True,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "OpenCheck backups",
            },
        )
        self._release: dict[str, Any] | None = None

    def close(self) -> None:
        if self._own:
            self.client.close()

    def check_private(self) -> None:
        r = self.client.get(f"{GITHUB_API}/repos/{self.repo}")
        if r.status_code == 404:
            raise BackupError(
                f"{self.repo} was not found — create it as a private repository, and check "
                "the token has access to it."
            )
        r.raise_for_status()
        if r.json().get("private") is not True:
            raise BackupError(f"{self.repo} is not private; refusing to upload backups to it.")

    def release(self, *, create: bool = True) -> dict[str, Any] | None:
        if self._release is not None:
            return self._release
        r = self.client.get(f"{GITHUB_API}/repos/{self.repo}/releases/tags/{self.tag}")
        if r.status_code == 404:
            if not create:
                return None
            r = self.client.post(
                f"{GITHUB_API}/repos/{self.repo}/releases",
                json={
                    "tag_name": self.tag,
                    "name": "OpenCheck SQLite backups",
                    "body": (
                        "Encrypted daily backups of watchlist.sqlite and saved_reports.sqlite, "
                        "written by the OpenCheck API (opencheck/backups.py). Restore with "
                        "backend/scripts/restore_backup.py."
                    ),
                    "prerelease": True,
                },
            )
            if r.status_code == 422:
                raise BackupError(
                    f"GitHub would not create the {self.tag} release on {self.repo} — a release "
                    "needs a commit to tag, so give the repository one (a README will do)."
                )
        r.raise_for_status()
        self._release = r.json()
        return self._release

    def assets(self) -> list[dict[str, Any]]:
        rel = self.release(create=False)
        if rel is None:
            return []
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            r = self.client.get(
                f"{GITHUB_API}/repos/{self.repo}/releases/{rel['id']}/assets",
                params={"per_page": 100, "page": page},
            )
            r.raise_for_status()
            batch = r.json()
            out.extend(batch)
            if len(batch) < 100:
                return out
            page += 1

    def backups(self, stem: str) -> list[dict[str, Any]]:
        """This file's backups, oldest first, each with a parsed ``when``."""
        found = []
        for a in self.assets():
            parsed = parse_asset_name(a.get("name", ""))
            if parsed and parsed[0] == stem:
                found.append({**a, "when": parsed[1]})
        return sorted(found, key=lambda a: a["when"])

    def upload(self, path: Path, name: str, label: str | None = None) -> dict[str, Any]:
        rel = self.release()
        assert rel is not None
        size = path.stat().st_size
        params = {"name": name}
        if label:
            params["label"] = label
        with path.open("rb") as fh:
            r = self.client.post(
                f"{GITHUB_UPLOADS}/repos/{self.repo}/releases/{rel['id']}/assets",
                params=params,
                content=fh,
                headers={"Content-Type": "application/octet-stream", "Content-Length": str(size)},
            )
        r.raise_for_status()
        return r.json()

    def delete(self, asset_id: int) -> None:
        r = self.client.delete(f"{GITHUB_API}/repos/{self.repo}/releases/assets/{asset_id}")
        if r.status_code != 404:
            r.raise_for_status()

    def download(self, asset_id: int, out: IO[bytes]) -> None:
        """httpx drops the Authorization header on the redirect to GitHub's
        object store, which is what that host expects."""
        with self.client.stream(
            "GET",
            f"{GITHUB_API}/repos/{self.repo}/releases/assets/{asset_id}",
            headers={"Accept": "application/octet-stream"},
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes(CHUNK):
                out.write(chunk)


# ---------------------------------------------------------------------------
# The task
# ---------------------------------------------------------------------------


@dataclass
class FileState:
    last_ok_at: str | None = None
    last_asset: str | None = None
    last_size: int | None = None
    schema_version: int | None = None
    last_error: str | None = None
    last_error_at: str | None = None


@dataclass
class BackupState:
    """What ``/watchstats`` reports under ``backups``. No repo, token, asset
    URL or content — names of OpenCheck's own files and dates only."""

    enabled: bool = False
    last_check_at: str | None = None
    files: dict[str, FileState] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "last_check_at": self.last_check_at,
            "files": {k: dict(v.__dict__) for k, v in self.files.items()},
        }


_state = BackupState()
_state_lock = threading.Lock()


def state() -> dict[str, Any]:
    with _state_lock:
        return _state.to_dict()


def reset_for_tests() -> None:
    global _state
    with _state_lock:
        _state = BackupState()


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def configured(settings: Any | None = None) -> bool:
    from .config import get_settings

    s = settings or get_settings()
    return bool(s.backup_github_repo and s.backup_github_token and s.backup_passphrase_secret)


def targets(settings: Any | None = None) -> list[Path]:
    """The files to back up: those configured that exist on disk."""
    from .config import get_settings

    s = settings or get_settings()
    out = []
    for raw in (s.watchlist_db_file, s.saved_reports_db_file):
        if raw and Path(raw).is_file():
            out.append(Path(raw))
    return out


def backup_file(
    store: GitHubReleaseStore,
    src: Path,
    passphrase: str,
    *,
    keep: int,
    interval_s: float,
    now: datetime | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Back ``src`` up if its newest backup is older than ``interval_s``.
    Returns what happened. Raises on failure (the caller records it)."""
    now = now or _now()
    stem = src.stem
    existing = store.backups(stem)
    if existing and not force:
        age = (now - existing[-1]["when"]).total_seconds()
        if age < interval_s:
            return {"file": stem, "action": "not_due", "age_s": int(age)}
    store.check_private()
    workdir = Path(tempfile.mkdtemp(prefix="opencheck-backup-"))
    try:
        enc, version = build_backup(src, workdir, passphrase)
        name = asset_name(stem, now)
        size = enc.stat().st_size
        store.upload(enc, name, label=f"{stem} schema v{version}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    deleted = 0
    if keep > 0:
        current = store.backups(stem)
        for old in current[: max(0, len(current) - keep)]:
            store.delete(old["id"])
            deleted += 1
    with _state_lock:
        fs = _state.files.setdefault(stem, FileState())
        fs.last_ok_at, fs.last_asset, fs.last_size, fs.schema_version = _iso(now), name, size, version
        fs.last_error = fs.last_error_at = None
    log.info("backups: %s → %s (%d bytes, schema v%d); pruned %d", src.name, name, size, version, deleted)
    return {"file": stem, "action": "uploaded", "asset": name, "size": size, "schema_version": version, "pruned": deleted}


def run_once(*, force: bool = False, client: Any | None = None) -> list[dict[str, Any]]:
    """One pass over every target. Never raises: a failure is logged and
    recorded per file, and the other file still gets its turn."""
    from .config import get_settings

    s = get_settings()
    if not configured(s) or s.backup_interval_s <= 0:
        return []
    with _state_lock:
        _state.enabled = True
        _state.last_check_at = _iso(_now())
    results: list[dict[str, Any]] = []
    try:
        store = GitHubReleaseStore(
            s.backup_github_repo or "", s.backup_github_token or "", s.backup_release_tag, client=client
        )
    except BackupError as exc:
        log.warning("backups: %s", exc)
        return [{"action": "failed", "error": str(exc)}]
    try:
        for src in targets(s):
            try:
                results.append(
                    backup_file(
                        store, src, s.backup_passphrase_secret or "",
                        keep=s.backup_keep, interval_s=s.backup_interval_s, force=force,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — recorded, and the next file still runs
                msg = str(exc) if isinstance(exc, BackupError) else describe_exception(exc)
                log.warning("backups: %s failed: %s", src.name, msg)
                with _state_lock:
                    fs = _state.files.setdefault(src.stem, FileState())
                    fs.last_error, fs.last_error_at = msg, _iso(_now())
                results.append({"file": src.stem, "action": "failed", "error": msg})
    finally:
        store.close()
    return results


async def backup_loop(check_interval_s: float = CHECK_INTERVAL_S, first_delay_s: float = FIRST_CHECK_DELAY_S) -> None:
    """Ask hourly whether a backup is due; the work runs on a thread."""
    with _state_lock:
        _state.enabled = True
    await asyncio.sleep(first_delay_s)
    while True:
        try:
            await asyncio.to_thread(run_once)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover — run_once does not raise
            log.exception("backups: unexpected error")
        await asyncio.sleep(check_interval_s)
