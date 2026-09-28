"""GLEIF's ISIN-to-LEI file as a local table behind ``/securities`` (Phase 258).

GLEIF publishes the complete ISIN-to-LEI mapping every day as a keyless zip
(``mapping.gleif.org/api/v2/isin-lei/latest``, ~32 MB, ~9.2 million rows). On
28 Sept 2026 it held 9,150,433 rows across only 98,677 LEIs — 2.9% of the LEIs
GLEIF has issued — so for ~97% of the companies OpenCheck is asked about the
honest answer to "how many securities are mapped to this LEI?" is *none*, and a
local table can give it without a GLEIF call. Before this, every QuickCheck
render spent one of the 50 GLEIF calls a minute the whole process shares on
the ``/isins`` endpoint, and as a *discretionary* call (Phase 234) it was the
first refused when the budget ran low.

The table is built on the server from GLEIF's own file — never hand-edited, no
release asset: at boot when absent, then whenever GLEIF has published a newer
file (checked every ``OPENCHECK_ISIN_INDEX_REFRESH_INTERVAL_S``). A build
stages the rows in a scratch table, reads them back in ``(lei, isin)`` order,
and writes:

* ``counts(lei, total)`` — the count on its own, so the count for an issuer
  with 636,388 ISINs is one indexed read, never a decompression;
* ``chunks(lei, chunk, isins)`` — the list in ISIN order, ``CHUNK`` ISINs per
  zlib blob, so a page of 20 is a slice of at most two chunks.

**ISIN order, not GLEIF's** (Stephen, 28 Sept 2026). The live API's pages come
in no stable order (Shell's page 1 opens ``US82266MXH68…``); a sorted list
makes page 2 today page 2 tomorrow, and a saved report's page reads the same.

**The file's date is the answer's date.** ``lookup`` returns the file's
``uploadedAt``; ``/securities`` reports it as ``isin_list_as_of`` and the panel
says "From GLEIF's ISIN-to-LEI file of <date>". A table older than
``OPENCHECK_ISIN_INDEX_MAX_AGE_DAYS`` is not used at all — the Phase 253 live
path (cached a day) answers instead — so a refresh that keeps failing degrades
to what shipped before, never to a silently old count.

Never raises into a request: a missing, half-built or wrong-schema file reads
as "no table".
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import re
import sqlite3
import tempfile
import threading
import time
import zipfile
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import get_settings

log = logging.getLogger(__name__)

MAPPING_LATEST_URL = "https://mapping.gleif.org/api/v2/isin-lei/latest"
SCHEMA_VERSION = "1"
#: ISINs per compressed chunk. A page of 20 touches at most two chunks; an
#: issuer with 636k ISINs is ~1,270 chunks, none read for its count.
CHUNK = 500
ISIN_LEN = 12

_LEI_RE = re.compile(r"^[A-Z0-9]{20}$")
_ISIN_RE = re.compile(r"^[A-Z0-9]{12}$")
_BATCH = 50_000

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
CREATE TABLE counts (lei TEXT PRIMARY KEY, total INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE chunks (
    lei TEXT NOT NULL,
    chunk INTEGER NOT NULL,
    isins BLOB NOT NULL,
    PRIMARY KEY (lei, chunk)
) WITHOUT ROWID;
"""


class IndexBuildError(Exception):
    """The file could not be turned into a table (wrong header, no rows)."""


@dataclass(frozen=True)
class IsinPage:
    total: int
    isins: list[str]
    #: GLEIF's ``uploadedAt`` for the file, ISO 8601 UTC.
    as_of: str
    file_name: str


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def index_path() -> Path:
    configured = get_settings().isin_index_db_file
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "opencheck" / "isin_lei.sqlite"


def sync_enabled() -> bool:
    settings = get_settings()
    return bool(settings.allow_live and settings.isin_index_sync)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _open_rows(source: Path) -> Iterator[list[str]]:
    """Rows of the CSV, whether ``source`` is GLEIF's zip or a bare CSV."""
    if zipfile.is_zipfile(source):
        with zipfile.ZipFile(source) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not members:
                raise IndexBuildError("the zip holds no CSV")
            with zf.open(members[0]) as raw:
                yield from csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        return
    with open(source, encoding="utf-8-sig", newline="") as fh:
        yield from csv.reader(fh)


def _pack(isins: list[str]) -> bytes:
    return zlib.compress("".join(isins).encode("ascii"), 6)


def _unpack(blob: bytes) -> list[str]:
    text = zlib.decompress(blob).decode("ascii")
    return [text[i : i + ISIN_LEN] for i in range(0, len(text), ISIN_LEN)]


def build_index(
    source: Path,
    out: Path,
    *,
    file_name: str,
    uploaded_at: str,
    source_url: str = "",
) -> dict[str, str]:
    """Build the table from GLEIF's zip (or its CSV) into ``out``. Blocking.

    Written to ``<out>.building`` and swapped in with ``os.replace``, so a
    reader never sees a half-built file and a failed build leaves the old
    table in place.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    started = time.monotonic()
    conn = sqlite3.connect(tmp)
    try:
        conn.executescript(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE;"
        )
        conn.executescript(_SCHEMA)
        conn.execute("CREATE TABLE staging (lei TEXT, isin TEXT)")
        rows = _open_rows(source)
        header = [h.strip().upper() for h in next(rows, [])]
        if header[:2] != ["LEI", "ISIN"]:
            raise IndexBuildError(f"unexpected header {header[:4]!r}")
        batch: list[tuple[str, str]] = []
        skipped = 0
        staged = 0
        for row in rows:
            if len(row) < 2:
                skipped += 1
                continue
            lei, isin = row[0].strip().upper(), row[1].strip().upper()
            if not _LEI_RE.match(lei) or not _ISIN_RE.match(isin):
                skipped += 1
                continue
            batch.append((lei, isin))
            if len(batch) >= _BATCH:
                conn.executemany("INSERT INTO staging VALUES (?, ?)", batch)
                staged += len(batch)
                batch.clear()
        if batch:
            conn.executemany("INSERT INTO staging VALUES (?, ?)", batch)
            staged += len(batch)
        if staged == 0:
            raise IndexBuildError("the file holds no rows")

        leis = isins = 0
        current: str | None = None
        bucket: list[str] = []
        total = 0
        chunk_no = 0
        last_isin: str | None = None
        counts: list[tuple[str, int]] = []
        chunks: list[tuple[str, int, bytes]] = []

        def flush_chunk() -> None:
            nonlocal chunk_no
            if bucket:
                chunks.append((current, chunk_no, _pack(bucket)))  # type: ignore[arg-type]
                chunk_no += 1
                bucket.clear()

        def flush_lei() -> None:
            nonlocal leis
            if current is not None:
                flush_chunk()
                counts.append((current, total))
                leis += 1

        for lei, isin in conn.execute("SELECT lei, isin FROM staging ORDER BY lei, isin"):
            if lei != current:
                flush_lei()
                current, total, chunk_no, last_isin = lei, 0, 0, None
            if isin == last_isin:  # a duplicated row in the file
                continue
            last_isin = isin
            bucket.append(isin)
            total += 1
            isins += 1
            if len(bucket) >= CHUNK:
                flush_chunk()
            if len(chunks) >= 2_000:
                conn.executemany("INSERT INTO chunks VALUES (?, ?, ?)", chunks)
                chunks.clear()
            if len(counts) >= _BATCH:
                conn.executemany("INSERT INTO counts VALUES (?, ?)", counts)
                counts.clear()
        flush_lei()
        conn.executemany("INSERT INTO chunks VALUES (?, ?, ?)", chunks)
        conn.executemany("INSERT INTO counts VALUES (?, ?)", counts)
        conn.execute("DROP TABLE staging")
        meta = {
            "schema_version": SCHEMA_VERSION,
            "file_name": file_name,
            "uploaded_at": uploaded_at,
            "source_url": source_url,
            "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "leis": str(leis),
            "isins": str(isins),
            "skipped_rows": str(skipped),
            "build_seconds": f"{time.monotonic() - started:.1f}",
        }
        conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
        conn.commit()
        conn.execute("VACUUM")
    except BaseException:
        conn.close()
        try:
            tmp.unlink()
        except OSError:  # pragma: no cover - best effort
            pass
        raise
    conn.close()
    os.replace(tmp, out)
    log.info(
        "isin_index: built %s — %s LEIs, %s ISINs in %ss",
        file_name, meta["leis"], meta["isins"], meta["build_seconds"],
    )
    return meta


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

_SHARED: dict[str, sqlite3.Connection | None] = {}


def read_meta(path: Path | None = None) -> dict[str, str]:
    target = path or index_path()
    if not target.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        try:
            return dict(conn.execute("SELECT key, value FROM meta").fetchall())
        finally:
            conn.close()
    except sqlite3.Error:
        return {}


def _shared_conn() -> sqlite3.Connection | None:
    conn = _SHARED.get("conn")
    if conn is not None:
        return conn
    path = index_path()
    if not path.exists():
        return None
    meta = read_meta(path)
    if meta.get("schema_version") != SCHEMA_VERSION:
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    except sqlite3.Error:
        return None
    _SHARED["conn"] = conn
    _SHARED["meta"] = meta  # type: ignore[assignment]
    return conn


def reset_connection() -> None:
    """Forget the cached connection (after a rebuild, and in tests).

    Deliberately not closed: a reader in another thread may be mid-query, and
    the old connection keeps reading the file it opened after ``os.replace``.
    """
    _SHARED.pop("conn", None)
    _SHARED.pop("meta", None)


def _meta() -> dict[str, str]:
    return dict(_SHARED.get("meta") or {})  # type: ignore[arg-type]


def age_days(meta: dict[str, str]) -> float | None:
    """How old the file is, from GLEIF's ``uploadedAt``."""
    raw = meta.get("uploaded_at") or ""
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - when).total_seconds() / 86_400


def usable() -> bool:
    """A current-schema table no older than the configured maximum."""
    if _shared_conn() is None:
        return False
    age = age_days(_meta())
    return age is not None and age <= get_settings().isin_index_max_age_days


def lookup(lei: str, page: int = 1, page_size: int = 20) -> IsinPage | None:
    """One page of an LEI's ISINs from the table, or ``None`` when there is no
    usable table (absent, wrong schema, too old, unreadable).

    An LEI the table does not hold is an answer — ``total`` 0 — because the
    file is GLEIF's complete mapping, not a sample.
    """
    if not usable():
        return None
    conn = _shared_conn()
    meta = _meta()
    if conn is None:
        return None
    lei = lei.strip().upper()
    page = max(1, int(page))
    page_size = max(1, int(page_size))
    try:
        row = conn.execute("SELECT total FROM counts WHERE lei = ?", (lei,)).fetchone()
        total = int(row[0]) if row else 0
        start = (page - 1) * page_size
        end = min(start + page_size, total)
        isins: list[str] = []
        if start < end:
            first, last = start // CHUNK, (end - 1) // CHUNK
            blobs = conn.execute(
                "SELECT chunk, isins FROM chunks WHERE lei = ? AND chunk BETWEEN ? AND ? "
                "ORDER BY chunk",
                (lei, first, last),
            ).fetchall()
            flat: list[str] = []
            for _, blob in blobs:
                flat.extend(_unpack(blob))
            offset = start - first * CHUNK
            isins = flat[offset : offset + (end - start)]
    except (sqlite3.Error, zlib.error, UnicodeDecodeError) as exc:
        log.warning("isin_index: read failed for %s: %s", lei, exc)
        return None
    return IsinPage(
        total=total,
        isins=isins,
        as_of=meta.get("uploaded_at") or "",
        file_name=meta.get("file_name") or "",
    )


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

_BUILD_LOCK = threading.Lock()
_STATE: dict[str, Any] = {"last_outcome": None, "last_checked_at": None}


def latest_file() -> dict[str, str]:
    """``{file_name, uploaded_at, url}`` of GLEIF's newest ISIN-to-LEI file."""
    import httpx

    resp = httpx.get(MAPPING_LATEST_URL, timeout=60.0, follow_redirects=True)
    resp.raise_for_status()
    attrs = ((resp.json() or {}).get("data") or {}).get("attributes") or {}
    url = str(attrs.get("downloadLink") or "")
    name = str(attrs.get("fileName") or "")
    uploaded = str(attrs.get("uploadedAt") or "")
    if not (url and name and uploaded):
        raise IndexBuildError("the mapping API named no file")
    return {"file_name": name, "uploaded_at": uploaded, "url": url}


def _download(url: str, target: Path) -> int:
    import httpx

    target.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with httpx.stream("GET", url, timeout=300.0, follow_redirects=True) as response:
        response.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in response.iter_bytes():
                size += len(chunk)
                fh.write(chunk)
    return size


def _retrying(fn, attempts: int = 3, pause_s: float = 5.0):
    """``fn()``, retried on a network error — a reset connection is the
    commonest failure seen against mapping.gleif.org."""
    import httpx

    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            if attempt == attempts or (
                isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500
            ):
                raise
            log.info("isin_index: %s, retrying (%d/%d)", exc, attempt, attempts)
            time.sleep(pause_s * attempt)
    raise AssertionError("unreachable")  # pragma: no cover


def _outcome(kind: str, meta: dict[str, str], *, error: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "isin_index": kind,
        "path": str(index_path()),
        "file_name": meta.get("file_name") or None,
        "uploaded_at": meta.get("uploaded_at") or None,
        "leis": int(meta["leis"]) if meta.get("leis") else None,
        "isins": int(meta["isins"]) if meta.get("isins") else None,
    }
    if error:
        result["error"] = error
    _STATE["last_outcome"] = result
    return result


def sync_index(*, force: bool = False) -> dict[str, Any]:
    """Make sure the table holds GLEIF's newest file. Blocking; never raises.

    Keeps the table when GLEIF names the same file; otherwise downloads the
    new one and rebuilds. A failure keeps whatever table there was.
    """
    path = index_path()
    with _BUILD_LOCK:
        _STATE["last_checked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        meta = read_meta(path)
        current = meta.get("schema_version") == SCHEMA_VERSION
        if not sync_enabled():
            return _outcome("kept" if current else "not_configured", meta)
        archive = path.with_name(path.name + ".zip")
        try:
            latest = _retrying(latest_file)
            if not force and current and latest["file_name"] == meta.get("file_name"):
                return _outcome("kept", meta)
            _retrying(lambda: _download(latest["url"], archive))
            new_meta = build_index(
                archive,
                path,
                file_name=latest["file_name"],
                uploaded_at=latest["uploaded_at"],
                source_url=latest["url"],
            )
            reset_connection()
            return _outcome("built", new_meta)
        except Exception as exc:  # noqa: BLE001 - reported, never raised
            log.warning("isin_index: sync failed: %s", exc)
            return _outcome("failed", meta, error=f"{type(exc).__name__}: {exc}"[:300])
        finally:
            if archive.exists():
                try:
                    archive.unlink()
                except OSError:  # pragma: no cover - best effort
                    pass


def warm_index() -> dict[str, Any]:
    """Startup hook: build the table when absent, refresh it when GLEIF has
    published a newer file. Synchronous; run in a thread."""
    if not sync_enabled() and not index_path().exists():
        return _outcome("not_configured", {})
    return sync_index()


#: After a failed sync the loop tries again sooner than the full interval,
#: so a transient failure at boot does not leave the server without a table
#: for six hours.
RETRY_AFTER_FAILURE_S = 900.0


def _next_wait(interval_s: float) -> float:
    last = (_STATE.get("last_outcome") or {}).get("isin_index")
    return min(interval_s, RETRY_AFTER_FAILURE_S) if last == "failed" else interval_s


async def refresh_loop(interval_s: float) -> None:
    """Re-check GLEIF for a newer file every ``interval_s`` (sooner after a
    failure). The first check waits: boot's ``warm_index`` has just run."""
    while True:
        await asyncio.sleep(_next_wait(interval_s))
        try:
            await asyncio.to_thread(sync_index)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never kill the loop
            log.warning("isin_index: refresh failed: %s", exc)


def status() -> dict[str, Any]:
    """What the table is, for ``/signalstats``: the file, its age, whether it
    is being used, and what the last sync did. No LEI or ISIN appears."""
    meta = read_meta()
    age = age_days(meta) if meta else None
    return {
        "configured_path": str(index_path()),
        "present": bool(meta),
        "usable": usable(),
        "file_name": meta.get("file_name") or None,
        "uploaded_at": meta.get("uploaded_at") or None,
        "built_at": meta.get("built_at") or None,
        "age_days": round(age, 2) if age is not None else None,
        "max_age_days": get_settings().isin_index_max_age_days,
        "leis": int(meta["leis"]) if meta.get("leis") else None,
        "isins": int(meta["isins"]) if meta.get("isins") else None,
        "build_seconds": float(meta["build_seconds"]) if meta.get("build_seconds") else None,
        "last_checked_at": _STATE.get("last_checked_at"),
        "last_outcome": (_STATE.get("last_outcome") or {}).get("isin_index"),
        "last_error": (_STATE.get("last_outcome") or {}).get("error"),
    }


def reset_for_tests() -> None:
    reset_connection()
    _STATE["last_outcome"] = None
    _STATE["last_checked_at"] = None


__all__ = [
    "CHUNK",
    "IsinPage",
    "IndexBuildError",
    "build_index",
    "index_path",
    "lookup",
    "read_meta",
    "refresh_loop",
    "reset_connection",
    "status",
    "sync_index",
    "usable",
    "warm_index",
]
