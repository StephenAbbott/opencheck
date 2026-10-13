#!/usr/bin/env python3
"""Build the Lithuanian Register of Legal Entities (JAR) index (Phase 325).

Registrų centras publishes the whole register as daily CSV files under
CC BY 4.0 at https://www.registrucentras.lt/atviri-duomenys-ir-statistika/jar-pirminiai-duomenys-raw-data,
but serves them — like its search page — behind a Cloudflare JavaScript
challenge that no server can pass. Datacentre addresses (Render, GitHub
Actions, the cloud) are challenged; a residential connection is not. The
national portal that republishes them (data.gov.lt / get.data.gov.lt) is
geo-fenced to Lithuania. So the index is built **off-datacentre** and
shipped in as the ``jar-lithuania-index`` release asset, which production
downloads at boot (``jar_lithuania.warm_index``) — the ONRC / ChileCompra
arrangement.

    python3 scripts/build_jar_lithuania_index.py --out /tmp/jar_lithuania.sqlite --gzip

``scripts/refresh_jar_lithuania_index.sh`` runs this and replaces the release
asset. Standard library only, on purpose: it must run with a bare
``python3`` on whatever machine can reach the files.

What it reads, and what it refuses
----------------------------------

Two files, the whole register:

* ``JAR_IREGISTRUOTI.csv`` — every registered entity (≈235,000 rows), with
  its legal status and the date that status took effect.
* ``JAR_ISREGISTRUOTI.csv`` — every deregistered one (≈315,000), with its
  deregistration date. Kept because a lapsed or retired LEI often names a
  company that is gone, and "deregistered on <date>" is the answer.

Pipe-delimited UTF-8 with doubled-quote escaping; one cut date
(``formavimo_data``) on every row. The build refuses, rather than writes a
plausible-looking index, when:

* a download is not ``text/plain`` or its first line is not the expected
  header — a wrong file name returns **HTTP 200 with the site's HTML shell**,
  so the status code proves nothing;
* parsed records ≠ physical lines − 1 — a stray quote would merge rows
  silently (the ONRC ``QUOTE_NONE`` lesson, Phase 211);
* a file is implausibly short (truncated transfer, or the files swapped);
* the two files, or rows within a file, carry different cut dates.

Names the register withholds
----------------------------

The open data replaces the name of every sole proprietorship, general and
limited partnership (and their branches) with the legal form's name —
``Individuali įmonė`` in the name column for all 13,078 registered sole
proprietorships on 12 Oct 2026 — because those names usually carry the
owner's personal name. The index stores **no name** for such a row (NULL),
so nothing downstream mistakes the form for a name; the adapter falls back to
the GLEIF legal name and says so. The rule is the data's own (name equal to
the form name), not a list of forms, so a form the register starts naming
is picked up without a code change.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import logging
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Iterator

log = logging.getLogger("build_jar_lithuania_index")

#: Bump when the table layout changes; the adapter refuses any other value
#: (``jar_lithuania.INDEX_SCHEMA_VERSION``, pinned equal by a test).
SCHEMA_VERSION = "1"

BASE_URL = "https://www.registrucentras.lt/aduomenys/?byla="
PAGE_URL = (
    "https://www.registrucentras.lt/atviri-duomenys-ir-statistika/"
    "jar-pirminiai-duomenys-raw-data"
)
LICENSE = "CC-BY-4.0"

REGISTERED_FILE = "JAR_IREGISTRUOTI.csv"
DEREGISTERED_FILE = "JAR_ISREGISTRUOTI.csv"

REGISTERED_HEADER = [
    "ja_kodas", "ja_pavadinimas", "adresas", "ja_reg_data", "form_kodas",
    "form_pavadinimas", "stat_kodas", "stat_pavadinimas", "stat_data_nuo",
    "formavimo_data",
]
DEREGISTERED_HEADER = [
    "ja_kodas", "ja_pavadinimas", "adresas", "ja_reg_data", "form_kodas",
    "form_pavadinimas", "isreg_data", "formavimo_data",
]

#: Floors well under the 12 Oct 2026 counts (235,248 and 315,580): a file
#: shorter than this was cut short, or is not the file it claims to be.
#: ``--min-rows 0`` lowers them for fixtures.
MIN_ROWS = {REGISTERED_FILE: 150_000, DEREGISTERED_FILE: 200_000}

USER_AGENT = "OpenCheck JAR index builder (+https://opencheck.world)"

_CODE_RE = re.compile(r"^\d{9}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE form (code TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE status (code TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE entity (
    code TEXT PRIMARY KEY,          -- the 9-digit legal entity code
    name TEXT,                      -- NULL where the register withholds it
    address TEXT,
    registered_on TEXT,             -- ja_reg_data
    form_code TEXT,
    status_code TEXT,               -- registered entities only
    status_since TEXT,              -- stat_data_nuo
    deregistered_on TEXT            -- isreg_data; NULL while registered
) WITHOUT ROWID;
"""


class BuildRefused(RuntimeError):
    """The input is not what the index may be built from."""


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download(name: str, dest: Path, *, attempts: int = 3) -> Path:
    """Fetch one open-data file to ``dest``, checking it is the CSV.

    A wrong file name — or the Cloudflare challenge — answers with HTML, and
    the challenge answers 403. Either is refused here, with the reason.
    """
    url = BASE_URL + name
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=300) as resp:
                ctype = resp.headers.get("Content-Type", "")
                if not ctype.lower().startswith("text/plain"):
                    raise BuildRefused(
                        f"{name}: expected text/plain, got {ctype!r} — the site "
                        "answered with a page, not the file"
                    )
                tmp = dest.with_suffix(dest.suffix + ".part")
                with open(tmp, "wb") as fh:
                    shutil.copyfileobj(resp, fh, length=1 << 20)
                tmp.replace(dest)
                log.info("downloaded %s: %d bytes", name, dest.stat().st_size)
                return dest
        except BuildRefused:
            raise
        except Exception as exc:  # noqa: BLE001 — retried, then reported
            last = exc
            log.warning("download %s failed (attempt %d/%d): %s", name, attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(10 * attempt)
    raise BuildRefused(
        f"{name}: could not download ({last}). From a datacentre address this "
        "is the Cloudflare challenge (HTTP 403); run the builder where the "
        "site answers."
    )


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_rows(path: Path, header: list[str], *, min_rows: int) -> Iterator[dict[str, str]]:
    """Yield the file's records as dicts, after checking its shape.

    Two passes over a ~40 MB file is cheap, and the first one is the check
    that matters: records must equal physical lines less the header.
    """
    if not path.is_file():
        raise BuildRefused(f"{path}: no such file")
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    physical = raw.count(b"\n") + (0 if raw.endswith(b"\n") or not raw else 1)
    text = raw.decode("utf-8")
    # ``newline=""`` and csv's own line handling: ``str.splitlines`` would
    # also break on U+2028 and friends, splitting a record mid-field.
    reader = csv.reader(io.StringIO(text, newline=""), delimiter="|", quotechar='"')
    first = next(reader, None)
    if first != header:
        raise BuildRefused(f"{path.name}: header is {first!r}, expected {header!r}")
    rows = list(reader)
    if len(rows) != physical - 1:
        raise BuildRefused(
            f"{path.name}: {len(rows)} records from {physical - 1} lines — a "
            "quote is merging or splitting rows"
        )
    if len(rows) < min_rows:
        raise BuildRefused(f"{path.name}: {len(rows)} records, fewer than {min_rows}")
    width = len(header)
    for number, row in enumerate(rows, start=2):
        if len(row) != width:
            raise BuildRefused(f"{path.name}: line {number} has {len(row)} fields, not {width}")
        yield dict(zip(header, row))


def _date(value: str) -> str | None:
    value = (value or "").strip()
    return value if _DATE_RE.match(value) else None


def _name(row: dict[str, str]) -> str | None:
    """The entity's name, or None where the register put the form name there."""
    name = " ".join((row.get("ja_pavadinimas") or "").split())
    form = " ".join((row.get("form_pavadinimas") or "").split())
    if not name or name == form:
        return None
    return name


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _stamp(moment: dt.datetime) -> str:
    """``meta.built_at``: when OpenCheck read the register, to the second, in
    UTC — it becomes ``source.retrievedAt`` (Phase 318: a bare day reads as
    midnight, which is what a cut published as a download looks like)."""
    return moment.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def build_index(
    registered: Path,
    deregistered: Path,
    out_path: Path,
    *,
    min_rows: dict[str, int] | None = None,
    started: dt.datetime | None = None,
) -> dict[str, str]:
    """Build the SQLite index at ``out_path`` from the two CSV files.

    Written to a temporary file beside ``out_path`` and renamed at the end,
    so a failed build never leaves a half-written index where one was.
    Returns the ``meta`` table.
    """
    floors = MIN_ROWS if min_rows is None else min_rows
    started = started or dt.datetime.now(dt.timezone.utc)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()

    conn = sqlite3.connect(tmp)
    conn.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;" + _SCHEMA)
    forms: dict[str, str] = {}
    statuses: dict[str, str] = {}
    cuts: set[str] = set()
    counts = {"registered": 0, "deregistered": 0, "name_withheld": 0, "in_both_files": 0}
    seen: set[str] = set()

    def _check_code(code: str, source: str) -> str:
        code = code.strip()
        if not _CODE_RE.match(code):
            raise BuildRefused(f"{source}: entity code {code!r} is not nine digits")
        return code

    for row in read_rows(registered, REGISTERED_HEADER, min_rows=floors.get(REGISTERED_FILE, 0)):
        code = _check_code(row["ja_kodas"], REGISTERED_FILE)
        if code in seen:
            raise BuildRefused(f"{REGISTERED_FILE}: code {code} appears twice")
        seen.add(code)
        cuts.add(row["formavimo_data"].strip())
        forms.setdefault(row["form_kodas"].strip(), " ".join(row["form_pavadinimas"].split()))
        statuses.setdefault(row["stat_kodas"].strip(), " ".join(row["stat_pavadinimas"].split()))
        name = _name(row)
        counts["name_withheld"] += name is None
        conn.execute(
            "INSERT INTO entity VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                code, name, " ".join(row["adresas"].split()) or None,
                _date(row["ja_reg_data"]), row["form_kodas"].strip() or None,
                row["stat_kodas"].strip() or None, _date(row["stat_data_nuo"]),
            ),
        )
        counts["registered"] += 1

    dereg_seen: set[str] = set()
    for row in read_rows(deregistered, DEREGISTERED_HEADER, min_rows=floors.get(DEREGISTERED_FILE, 0)):
        code = _check_code(row["ja_kodas"], DEREGISTERED_FILE)
        if code in dereg_seen:
            raise BuildRefused(f"{DEREGISTERED_FILE}: code {code} appears twice")
        dereg_seen.add(code)
        cuts.add(row["formavimo_data"].strip())
        forms.setdefault(row["form_kodas"].strip(), " ".join(row["form_pavadinimas"].split()))
        if code in seen:
            # None on 12 Oct 2026. Were one to appear, the registered row is
            # the register's current word on the entity.
            counts["in_both_files"] += 1
            continue
        name = _name(row)
        counts["name_withheld"] += name is None
        conn.execute(
            "INSERT INTO entity VALUES (?, ?, ?, ?, ?, NULL, NULL, ?)",
            (
                code, name, " ".join(row["adresas"].split()) or None,
                _date(row["ja_reg_data"]), row["form_kodas"].strip() or None,
                _date(row["isreg_data"]),
            ),
        )
        counts["deregistered"] += 1

    cuts.discard("")
    if len(cuts) != 1 or not _DATE_RE.match(next(iter(cuts))):
        conn.close()
        tmp.unlink()
        raise BuildRefused(f"expected one cut date across both files, found {sorted(cuts)}")
    cut = next(iter(cuts))

    conn.executemany("INSERT INTO form VALUES (?, ?)", sorted(forms.items()))
    conn.executemany("INSERT INTO status VALUES (?, ?)", sorted(statuses.items()))
    meta = {
        "schema_version": SCHEMA_VERSION,
        "cut": cut,
        "built_at": _stamp(started),
        "registered": str(counts["registered"]),
        "deregistered": str(counts["deregistered"]),
        "name_withheld": str(counts["name_withheld"]),
        "in_both_files": str(counts["in_both_files"]),
        "source_page": PAGE_URL,
        "source_files": f"{REGISTERED_FILE},{DEREGISTERED_FILE}",
        "license": LICENSE,
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out_path)
    log.info(
        "built %s: %s registered, %s deregistered, %s names withheld, cut %s",
        out_path, meta["registered"], meta["deregistered"], meta["name_withheld"], cut,
    )
    return meta


def gzip_file(path: Path) -> Path:
    out = path.with_name(path.name + ".gz")
    with open(path, "rb") as src, gzip.open(out, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst, length=1 << 20)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", required=True, type=Path, help="the SQLite file to write")
    parser.add_argument(
        "--from-dir", type=Path,
        help="build from CSVs already in this directory instead of downloading",
    )
    parser.add_argument(
        "--keep-dir", type=Path,
        help="download into this directory and keep the CSVs (default: a temp dir)",
    )
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    parser.add_argument(
        "--min-rows", type=int, default=None,
        help="override both row floors (fixtures only)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    started = dt.datetime.now(dt.timezone.utc)
    floors = None if args.min_rows is None else {
        REGISTERED_FILE: args.min_rows, DEREGISTERED_FILE: args.min_rows,
    }
    scratch: tempfile.TemporaryDirectory | None = None
    try:
        if args.from_dir:
            src = args.from_dir
        else:
            if args.keep_dir:
                src = args.keep_dir
                src.mkdir(parents=True, exist_ok=True)
            else:
                scratch = tempfile.TemporaryDirectory(prefix="jar-lt-")
                src = Path(scratch.name)
            for name in (REGISTERED_FILE, DEREGISTERED_FILE):
                download(name, src / name)
        meta = build_index(
            src / REGISTERED_FILE, src / DEREGISTERED_FILE, args.out,
            min_rows=floors, started=started,
        )
    except BuildRefused as exc:
        log.error("refused: %s", exc)
        return 2
    finally:
        if scratch is not None:
            scratch.cleanup()
    if args.gzip:
        gz = gzip_file(args.out)
        log.info("gzipped: %s (%d bytes)", gz, gz.stat().st_size)
    print(" ".join(f"{k}={v}" for k, v in meta.items() if k not in {"source_page"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
