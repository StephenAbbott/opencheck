"""Build the NTA Corporate Number (Japan) SQLite index from the nationwide file.

The National Tax Agency publishes every corporate number it has assigned as
one monthly nationwide file (全件データ) on the Corporate Number Publication
Site, keyless, under the Public Data License 1.0. The download is a form
post, not a link: the page carries a session token and one file number per
format, and the server answers the post with a zip holding the CSV and its
detached OpenPGP signature. ``.github/workflows/refresh-nta-japan-index.yml``
runs this monthly and ships the result as the ``nta-japan-index`` release
asset (the site answers a datacentre address — 268 MB in ten seconds from the
cloud on 16 Nov 2026).

Usage
-----
    python3 scripts/build_nta_japan_index.py --download --out nta_japan.sqlite --gzip
    python3 scripts/build_nta_japan_index.py --zip 00_zenkoku_all_20261030.zip --out nta_japan.sqlite
    python3 scripts/build_nta_japan_index.py --csv 00_zenkoku_all_20261030.csv --cut 2026-10-30 \\
        --out nta_japan.sqlite

``--verify`` checks the CSV's signature with ``gpg`` against the key committed
beside this script (``nta_japan_signing_key.asc``, fingerprint
``NTA_KEY_FINGERPRINT``, as printed on the site's 公開鍵 page) and refuses an
unsigned or wrongly signed file. The workflow always passes it.

What the file holds (30 Oct 2026, measured)
-------------------------------------------
* No header; UTF-8; thirty columns per row (``COLUMNS`` in the adapter).
  5,850,621 rows, one per number, no repeats, every check digit valid.
* Kinds: 301 株式会社 2,959,503 · 302 有限会社 1,675,010 · 399 other
  registered 587,828 · 305 合同会社 467,367 · 304 93,157 · 401 foreign
  20,241 · 499 other 19,953 · 303 19,296 · 201 local public bodies 7,418 ·
  101 national organs 848.
* 790,701 closed records; 62,477 name a successor; 15,430 carry an English
  name; 20,249 an address abroad; 40,296 ``hihyoji`` = 1.

The index keeps the register's codes; the adapter holds the glosses and the
status classes, so a reading can change without a rebuild. Name search is a
trigram FTS5 table over the names of open records (``search_text`` of the
registered and the English name). About 1.2 GB on disk, under half that
gzipped.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import http.cookiejar
import io
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Iterator
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.nta_japan import (  # noqa: E402
    COLUMNS,
    DOWNLOAD_URL,
    INDEX_SCHEMA_VERSION,
    row_from_csv,
    search_text,
    status_class,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_nta_japan_index")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"
FORM_URL = DOWNLOAD_URL + "index.html"
TOKEN_FIELD = "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token"

KEY_FILE = Path(__file__).with_name("nta_japan_signing_key.asc")
#: The NTA's signing key, as the Corporate Number Publication Site prints it.
NTA_KEY_FINGERPRINT = "F4109E821F0DE67C4BA1947C4BCD87A24D615230"

#: Below this the file is not the register (5,850,621 on 30 Oct 2026).
MIN_ROWS = 5_000_000
#: Rows that are not thirty columns with a valid number. None on 30 Oct 2026.
MAX_MALFORMED = 10

FIELDS = (
    "number", "name", "kind", "prefecture", "city", "street", "post_code",
    "address_outside", "close_date", "close_cause", "successor", "process",
    "change_date", "change_cause", "update_date", "assignment_date", "en_name",
    "en_prefecture", "en_city", "en_address_outside", "hihyoji",
)

SCHEMA = f"""
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  number INTEGER PRIMARY KEY,
  {", ".join(f"{f} TEXT" for f in FIELDS[1:-1])},
  hihyoji INTEGER NOT NULL DEFAULT 0
);
CREATE VIRTUAL TABLE name_fts USING fts5(name, en, content='', tokenize='trigram');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
_INSERT = f"INSERT INTO entity VALUES ({','.join('?' * len(FIELDS))})"


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


def cut_from_name(name: str) -> str | None:
    """``00_zenkoku_all_20261030.zip`` → ``2026-10-30``."""
    m = re.search(r"zenkoku_all_(\d{4})(\d{2})(\d{2})", name or "")
    return f"{m[1]}-{m[2]}-{m[3]}" if m else None


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def form_fields(page_html: str) -> tuple[str, str]:
    """The session token and the nationwide Unicode CSV's file number."""
    token = re.search(rf'name="{re.escape(TOKEN_FIELD)}"\s+value="([^"]+)"', page_html)
    section = page_html.find('id="csv-unicode"')
    file_no = re.search(r"doDownload\((\d+)\)", page_html[section:]) if section >= 0 else None
    if not token or not file_no:
        raise RefusedInput("the download page carries no token or no Unicode CSV file number")
    return token[1], file_no[1]


def download(work: Path) -> Path:
    """Post the form for the nationwide Unicode CSV; return the zip's path."""
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = [("User-Agent", USER_AGENT)]
    with opener.open(FORM_URL, timeout=60) as resp:
        token, file_no = form_fields(resp.read().decode("utf-8", "replace"))
    body = urllib.parse.urlencode(
        {TOKEN_FIELD: token, "event": "download", "selDlFileNo": file_no}
    ).encode()
    log.info("downloading file %s", file_no)
    with opener.open(urllib.request.Request(FORM_URL, data=body), timeout=600) as resp:
        disposition = resp.headers.get("Content-Disposition") or ""
        name = urllib.parse.unquote(disposition.rsplit("'", 1)[-1]) if "'" in disposition else ""
        if not cut_from_name(name):
            raise RefusedInput(f"the answer is not the nationwide file: {disposition!r}")
        dest = work / name
        with open(dest, "wb") as fh:
            shutil.copyfileobj(resp, fh, 1 << 20)
    return dest


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


def verify(stream: Iterator[bytes], signature: bytes, key_file: Path = KEY_FILE) -> None:
    """Refuse unless ``signature`` is a good signature of the streamed bytes
    by the NTA key (``VALIDSIG`` with the pinned fingerprint)."""
    with tempfile.TemporaryDirectory() as home:
        env = {**os.environ, "GNUPGHOME": home}
        subprocess.run(["gpg", "--batch", "-q", "--import", str(key_file)], env=env, check=True,
                       capture_output=True)
        sig = Path(home) / "data.asc"
        sig.write_bytes(signature)
        proc = subprocess.Popen(
            ["gpg", "--batch", "--status-fd", "1", "--verify", str(sig), "-"],
            env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        assert proc.stdin is not None
        for chunk in stream:
            proc.stdin.write(chunk)
        proc.stdin.close()
        status = proc.stdout.read().decode() if proc.stdout else ""
        proc.wait()
    if proc.returncode != 0 or f"VALIDSIG {NTA_KEY_FINGERPRINT}" not in status:
        raise RefusedInput("the file's signature is not the National Tax Agency's")


def _zip_members(path: Path) -> tuple[str, str | None]:
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
    csvs = [n for n in names if n.lower().endswith(".csv")]
    if len(csvs) != 1:
        raise RefusedInput(f"{path.name}: expected one CSV, found {names!r}")
    sig = csvs[0] + ".asc"
    return csvs[0], sig if sig in names else None


def _chunks(fh, size: int = 1 << 20) -> Iterator[bytes]:
    while chunk := fh.read(size):
        yield chunk


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def build(text: io.TextIOBase, out: Path, cut: str, *, min_rows: int | None = None,
          source_file: str = "") -> dict[str, int]:
    """Build the index from the CSV's text; ``min_rows`` overrides ``MIN_ROWS``
    (tests pass 0)."""
    floor = MIN_ROWS if min_rows is None else min_rows
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cut):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    con.executescript(SCHEMA)
    stats = {"rows": 0, "malformed": 0, "closed": 0, "searchable": 0,
             "english_names": 0, "successors": 0}

    def _refuse(message: str) -> RefusedInput:
        con.close()
        tmp.unlink(missing_ok=True)
        return RefusedInput(message)

    batch: list[tuple] = []
    fts: list[tuple] = []

    def _flush() -> None:
        try:
            con.executemany(_INSERT, batch)
        except sqlite3.IntegrityError as exc:
            raise _refuse(f"a corporate number repeats ({exc})") from None
        con.executemany("INSERT INTO name_fts(rowid, name, en) VALUES (?,?,?)", fts)
        batch.clear()
        fts.clear()

    for fields in csv.reader(text):
        if not fields:
            continue
        record = row_from_csv(fields) if len(fields) == len(COLUMNS) else None
        if record is None:
            stats["malformed"] += 1
            if stats["malformed"] > MAX_MALFORMED:
                raise _refuse(f"more than {MAX_MALFORMED} malformed rows — the format has changed")
            continue
        stats["rows"] += 1
        record["number"] = int(record["number"])
        batch.append(tuple(record[f] for f in FIELDS))
        if record["en_name"]:
            stats["english_names"] += 1
        if record["successor"]:
            stats["successors"] += 1
        if status_class(record) == "terminal":
            stats["closed"] += 1
        else:
            stats["searchable"] += 1
            fts.append((record["number"], search_text(record["name"]),
                        search_text(record["en_name"]) or None))
        if len(batch) >= 50_000:
            _flush()
            if stats["rows"] % 1_000_000 == 0:
                log.info("%s rows, %.0fs", f"{stats['rows']:,}", time.time() - t0)
    _flush()
    if stats["rows"] < floor:
        raise _refuse(f"only {stats['rows']:,} rows — not the register")

    con.commit()
    con.execute("INSERT INTO name_fts(name_fts) VALUES ('optimize')")
    con.executemany(
        "INSERT OR REPLACE INTO meta VALUES (?,?)",
        [
            ("schema_version", INDEX_SCHEMA_VERSION),
            ("source", "National Tax Agency Corporate Number nationwide file (全件データ, CSV Unicode)"),
            ("source_file", source_file),
            ("cut", cut),
            ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            ("licence", "PDL-1.0 — 国税庁法人番号公表サイト（国税庁）"),
            *[(f"count_{k}", str(v)) for k, v in stats.items()],
        ],
    )
    con.commit()
    con.execute("VACUUM")
    con.close()
    os.replace(tmp, out)
    stats["seconds"] = int(time.time() - t0)
    stats["megabytes"] = int(os.path.getsize(out) / 1e6)
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch the current nationwide file")
    ap.add_argument("--work", type=Path, help="where --download puts the zip (default: beside --out)")
    ap.add_argument("--zip", type=Path, help="a nationwide zip already downloaded")
    ap.add_argument("--csv", type=Path, help="the nationwide CSV, unzipped (needs --cut)")
    ap.add_argument("--cut", help="the file's date, YYYY-MM-DD (default: from the file name)")
    ap.add_argument("--verify", action="store_true", help="refuse a file the NTA key did not sign")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    try:
        if args.download:
            work = args.work or args.out.parent
            work.mkdir(parents=True, exist_ok=True)
            args.zip = download(work)
        if args.zip:
            member, sig_member = _zip_members(args.zip)
            cut = args.cut or cut_from_name(member) or cut_from_name(args.zip.name)
            with zipfile.ZipFile(args.zip) as zf:
                if args.verify:
                    if sig_member is None:
                        raise RefusedInput("the zip carries no signature")
                    with zf.open(member) as fh:
                        verify(_chunks(fh), zf.read(sig_member))
                    log.info("signature verified")
                with zf.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                    stats = build(text, args.out, cut or "", source_file=member)
        elif args.csv:
            cut = args.cut or cut_from_name(args.csv.name)
            if args.verify:
                sig = args.csv.with_name(args.csv.name + ".asc")
                if not sig.exists():
                    raise RefusedInput(f"no {sig.name} beside the CSV")
                with open(args.csv, "rb") as fh:
                    verify(_chunks(fh), sig.read_bytes())
                log.info("signature verified")
            with open(args.csv, encoding="utf-8-sig", newline="") as text:
                stats = build(text, args.out, cut or "", source_file=args.csv.name)
        else:
            ap.error("--download, --zip or --csv")
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], cut)
    for key, value in stats.items():
        log.info("  %-16s %s", key, f"{value:,}")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
