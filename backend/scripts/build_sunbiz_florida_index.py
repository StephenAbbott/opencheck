"""Build the Florida Division of Corporations (Sunbiz) SQLite index.

The Division publishes free bulk data on an SFTP server whose credentials are
printed on its data-downloads page (``sftp.floridados.gov``, user ``Public``):
a quarterly **corporate data** file (``Quarterly/Cor/cordata.zip``, all records
on file) and a quarterly **corporate events** file (``Quarterly/Cor/corevent.zip``),
generated in January, April, July and October. This script downloads both (or
reads given copies), keeps the records the lookup needs and writes the
``sunbiz_florida.sqlite`` the backend downloads at boot from the
``sunbiz-florida-index`` release asset (``SUNBIZ_FLORIDA_DB_URL``).

Usage
-----
    python3 scripts/build_sunbiz_florida_index.py --download --work /tmp/fl \\
        --out sunbiz_florida.sqlite --gzip
    python3 scripts/build_sunbiz_florida_index.py --cordata cordata.zip \\
        --corevent corevent.zip --lei-docs lei_docs.txt --out sunbiz_florida.sqlite

``--download`` needs ``paramiko``; reading the October 2026 files needs
``zipfile-deflate64`` (both zips but ``corevent`` are Deflate64, which the
standard library cannot read). Neither is a runtime dependency of the backend:
the workflow runs ``uv run --with paramiko --with zipfile-deflate64``. Without
``--lei-docs`` the script asks GLEIF's API for every ``registeredAs`` filed
under RA000603.

What the files hold (October 2026 quarterly, measured 29–30 Oct 2026)
---------------------------------------------------------------------
* ``cordata.zip`` — 1.79 GB, ten members ``cordata0.txt`` … ``cordata9.txt``
  (by the document number's last digit), 18.7 GB unzipped, **12,987,601
  records of 1,440 characters** (fixed width, latin-1, CRLF): 3,818,699
  status ``A``, 9,168,902 ``I``. The official record layout is at
  https://dos.fl.gov/sunbiz/other-services/data-downloads/corporate-data-file/
  — name, status, filing type, principal and mailing address, file date
  (``MMDDYYYY``), FEI number, more-than-six-officers flag, last transaction
  date, state/country of incorporation, three annual reports, registered
  agent, and six 128-character officer blocks. Daily files trim trailing
  blanks; rows are padded to 1,440 before slicing.
* ``corevent.zip`` — 195 MB, one member ``corevt.txt`` of 15,083,889 events of
  662 characters (layout in ``cor/Events/COREVENTREADME.txt``): document
  number, sequence, event code and description, effective and filed dates,
  three notes, a conversion/merger document number, and the corporation's
  name. A name-change event carries the name **after** the change.

What the index keeps (Stephen, 30 Oct 2026 — D1, D4)
----------------------------------------------------
* Every **active** record, every record whose document number a GLEIF LEI
  files under RA000603, and every record whose FEI an LEI filed instead
  (``99-9999999``) — about 3.8 million records, ~1.2 GB, ~350 MB gzipped.
* Officers and the registered agent by **name, type, city and state** — never
  a street address or ZIP.
* Events of the kinds the adapter reads (``KEPT_EVENTS``: dissolutions,
  revocations, withdrawals, reinstatements, name changes, mergers and
  conversions) for the kept records only.

Parsed in full on 30 Oct 2026 (the October 2026 files, a dry run without the
SQLite write): 12,987,601 records with none malformed, 3,821,336 kept (12,807
of them named by an LEI), 6,252,452 officers; 15,083,087 events, 802 without an
event code (trademark rows, skipped).

The builder refuses input it does not recognise and writes nothing then: a
member missing, a row of another length, too few records, a cut it cannot
date, or an events file far from the corporate file's date.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.request
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.sunbiz_florida import (  # noqa: E402
    FL_RA_CODE,
    INDEX_SCHEMA_VERSION,
    KEPT_EVENTS,
    clean_fei,
    normalise_doc,
    person_name,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_sunbiz_florida_index")

try:  # registers Deflate64 (zip method 9) with the standard zipfile module
    import zipfile_deflate64  # noqa: F401

    HAVE_DEFLATE64 = True
except ImportError:  # pragma: no cover - depends on the environment
    HAVE_DEFLATE64 = False

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"

SFTP_HOST = "sftp.floridados.gov"
#: Printed on https://dos.fl.gov/sunbiz/other-services/data-downloads/ for
#: anyone to use; overridable in case the Division changes them.
SFTP_USER = os.environ.get("SUNBIZ_SFTP_USER", "Public")
SFTP_PASSWORD = os.environ.get("SUNBIZ_SFTP_PASSWORD", "PubAccess1845!")
REMOTE_DIR = "/Public/doc/Quarterly/Cor"
#: The server refuses unbounded prefetch ("insufficient resources"); 48
#: outstanding requests read ~2.1 MB/s from a home connection (29 Oct 2026).
SFTP_CONCURRENCY = 48

RECORD_LEN = 1440
EVENT_LEN = 662
CORDATA_MEMBER = re.compile(r"^cordata(\d)\.txt$", re.I)
EVENT_MEMBER = "corevt.txt"

#: Below these the file is not the register (a cut file, a placeholder).
MIN_ROWS = {"records": 10_000_000, "active": 3_000_000, "events": 10_000_000}
#: Rows of the wrong shape above which the format has changed.
MAX_MALFORMED = 5_000
#: How far apart the corporate and events files' dates may be.
MAX_CUT_GAP_DAYS = 31

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  id INTEGER PRIMARY KEY,
  doc TEXT NOT NULL UNIQUE,
  name TEXT, status TEXT, filing_type TEXT,
  file_date TEXT, last_tx TEXT, fei TEXT, state_country TEXT,
  p_addr1 TEXT, p_addr2 TEXT, p_city TEXT, p_state TEXT, p_zip TEXT, p_country TEXT,
  m_addr1 TEXT, m_addr2 TEXT, m_city TEXT, m_state TEXT, m_zip TEXT, m_country TEXT,
  agent_name TEXT, agent_type TEXT, agent_city TEXT, agent_state TEXT,
  more_than_six TEXT, reports TEXT, lei_named INTEGER
);
CREATE TABLE officer (
  doc TEXT, seq INTEGER, title TEXT, type TEXT,
  surname TEXT, given TEXT, middle TEXT, name TEXT, city TEXT, state TEXT
);
CREATE TABLE event (
  doc TEXT, seq INTEGER, code TEXT, description TEXT,
  effective TEXT, filed TEXT, other_doc TEXT, name TEXT
);
CREATE VIRTUAL TABLE entity_fts USING fts5(name, content='', tokenize='unicode61');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
INDEXES = """
CREATE INDEX ix_officer ON officer(doc);
CREATE INDEX ix_event ON event(doc);
CREATE INDEX ix_entity_fei ON entity(fei) WHERE fei IS NOT NULL;
"""


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


# ---------------------------------------------------------------------------
# Field readers
# ---------------------------------------------------------------------------


def iso_day(raw: str) -> str | None:
    """``MMDDYYYY`` → ``YYYY-MM-DD``; blank or impossible → None."""
    text = (raw or "").strip()
    if not re.fullmatch(r"\d{8}", text):
        return None
    mm, dd, yyyy = text[0:2], text[2:4], text[4:8]
    try:
        date(int(yyyy), int(mm), int(dd))
    except ValueError:
        return None
    return f"{yyyy}-{mm}-{dd}"


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _party_name(field: str, kind: str) -> tuple[str | None, str | None, str | None, str]:
    """A 42-character name field: a person is surname 20 / first 14 /
    middle 8; a corporation is one string."""
    if kind == "P":
        surname, given, middle = _clean(field[0:20]), _clean(field[20:34]), _clean(field[34:42])
        return surname or None, given or None, middle or None, person_name(surname, given, middle)
    return None, None, None, _clean(field)


def parse_record(line: str) -> dict | None:
    """One corporate-data record, or None when it is not one."""
    rec = line.rstrip("\r\n")
    if len(rec) > RECORD_LEN:
        return None
    rec = rec.ljust(RECORD_LEN)
    doc = _clean(rec[0:12]).upper()
    status = rec[204]
    if not doc or status not in ("A", "I"):
        return None
    reports = []
    for y0, d0 in ((505, 510), (518, 523), (531, 536)):
        year = rec[y0:y0 + 4].strip()
        if re.fullmatch(r"\d{4}", year):
            entry = f"{year}:{iso_day(rec[d0:d0 + 8]) or ''}"
            # The register often repeats its latest report in the first two
            # slots (every sampled record, 30 Oct 2026); keep it once.
            if entry not in reports:
                reports.append(entry)
    agent_type = rec[586].strip() or None
    _, _, _, agent_name = _party_name(rec[544:586], agent_type or "")
    officers = []
    for i in range(6):
        block = rec[668 + i * 128: 668 + (i + 1) * 128]
        kind = block[4].strip() or None
        surname, given, middle, name = _party_name(block[5:47], kind or "")
        if not name:
            continue
        officers.append({
            "seq": i,
            "title": _clean(block[0:4]) or None,
            "type": kind,
            "surname": surname,
            "given": given,
            "middle": middle,
            "name": name,
            # Street (47:89) and ZIP (119:128) are not kept (D4).
            "city": _clean(block[89:117]) or None,
            "state": _clean(block[117:119]) or None,
        })
    return {
        "doc": doc,
        "name": _clean(rec[12:204]),
        "status": status,
        "filing_type": _clean(rec[205:220]) or None,
        "p": [_clean(rec[a:b]) or None for a, b in
              ((220, 262), (262, 304), (304, 332), (332, 334), (334, 344), (344, 346))],
        "m": [_clean(rec[a:b]) or None for a, b in
              ((346, 388), (388, 430), (430, 458), (458, 460), (460, 470), (470, 472))],
        "file_date": iso_day(rec[472:480]),
        "fei": clean_fei(rec[480:494]),
        "more_than_six": rec[494].strip() or None,
        "last_tx": iso_day(rec[495:503]),
        "state_country": _clean(rec[503:505]) or None,
        "reports": ";".join(reports) or None,
        "agent_name": agent_name or None,
        "agent_type": agent_type,
        # The agent's street (587:629) and ZIP (659:668) are not kept (D4).
        "agent_city": _clean(rec[629:657]) or None,
        "agent_state": _clean(rec[657:659]) or None,
        "officers": officers,
    }


def parse_event(line: str) -> dict | None:
    rec = line.rstrip("\r\n")
    if len(rec) > EVENT_LEN or len(rec) < 37:
        return None
    rec = rec.ljust(EVENT_LEN)
    doc = _clean(rec[0:12]).upper()
    code = _clean(rec[17:37])
    if not doc:
        return None
    # A row with no event code (802 trademark-cancellation rows in October
    # 2026) is read and skipped, not counted as a format change.
    seq = rec[12:17].strip()
    return {
        "doc": doc,
        "seq": int(seq) if seq.isdigit() else 0,
        "code": code,
        "description": _clean(rec[37:77]) or None,
        "effective": iso_day(rec[77:85]),
        "filed": iso_day(rec[85:93]),
        "other_doc": _clean(rec[198:210]).upper() or None,
        "name": _clean(rec[210:402]) or None,
    }


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


def _open_zip(path: Path) -> zipfile.ZipFile:
    zf = zipfile.ZipFile(path)
    if any(i.compress_type == 9 for i in zf.infolist()) and not HAVE_DEFLATE64:
        raise RefusedInput(
            f"{path.name} is Deflate64-compressed: install zipfile-deflate64 "
            "(uv run --with zipfile-deflate64 …)"
        )
    return zf


def _member_day(info: zipfile.ZipInfo) -> date:
    y, m, d = info.date_time[:3]
    return date(y, m, d)


#: The values GLEIF's ``filter[registration.status]`` accepts — any other
#: value is an HTTP 400 ("Invalid enum value", checked 31 Oct 2026; the first
#: workflow run failed on TRANSFERRED). ``gleif_registered_as`` also checks
#: that the per-status totals add up to the unfiltered total, so a status
#: GLEIF adds later cannot drop records unnoticed.
GLEIF_REGISTRATION_STATUSES = (
    "ISSUED", "LAPSED", "RETIRED", "MERGED", "DUPLICATE", "ANNULLED",
    "PENDING_TRANSFER", "PENDING_ARCHIVAL",
)


def gleif_registered_as(fetch=None) -> tuple[set[str], set[str]]:
    """Every ``registeredAs`` GLEIF files under RA000603, as document numbers
    and as FEIs. Split by registration status — the API stops paginating at
    10,000 rows — and by entity status when one status passes it.
    ``fetch`` (URL → JSON) is injectable for tests."""
    base = (
        "https://api.gleif.org/api/v1/lei-records?page%5Bsize%5D=200"
        f"&filter%5Bentity.registeredAt%5D={FL_RA_CODE}"
    )

    def _fetch(url: str) -> dict:
        for attempt in range(8):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/vnd.api+json"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return json.load(resp)
            except Exception as exc:  # noqa: BLE001
                log.warning("GLEIF %s (attempt %d): %s", url, attempt + 1, exc)
                time.sleep(3 + attempt * 3)
        raise RefusedInput(f"GLEIF did not answer: {url}")

    if fetch is None:
        fetch = _fetch
    values: set[str] = set()

    def walk(filters: str) -> int:
        first = fetch(base + filters + "&page%5Bnumber%5D=1")
        total = first["meta"]["pagination"]["total"]
        if total > 10_000:
            return -total
        last = first["meta"]["pagination"]["lastPage"]
        pages = [first] + [fetch(base + filters + f"&page%5Bnumber%5D={p}") for p in range(2, last + 1)]
        for page in pages:
            for rec in page.get("data") or []:
                ra = (rec["attributes"]["entity"].get("registeredAs") or "").strip()
                if ra:
                    values.add(ra)
        return total

    expected = fetch(base + "&page%5Bnumber%5D=1")["meta"]["pagination"]["total"]
    seen = 0
    for status in GLEIF_REGISTRATION_STATUSES:
        got = walk(f"&filter%5Bregistration.status%5D={status}")
        if got < 0:
            got = 0
            for entity_status in ("ACTIVE", "INACTIVE", "NULL"):
                sub = walk(f"&filter%5Bregistration.status%5D={status}&filter%5Bentity.status%5D={entity_status}")
                if sub < 0:
                    raise RefusedInput(f"GLEIF {status}/{entity_status} holds more than 10,000 records")
                got += sub
        seen += got
    if seen != expected:
        raise RefusedInput(
            f"GLEIF holds {expected:,} records under {FL_RA_CODE} but the "
            f"registration statuses add up to {seen:,} — a status is missing "
            "from GLEIF_REGISTRATION_STATUSES"
        )
    docs, feis = set(), set()
    for value in values:
        try:
            key = normalise_doc(value)
        except ValueError:
            continue
        if key.startswith("FEI:"):
            feis.add(key[4:])
        else:
            docs.add(key)
    log.info("GLEIF: %s registeredAs values under %s → %s documents, %s FEIs",
             f"{len(values):,}", FL_RA_CODE, f"{len(docs):,}", len(feis))
    return docs, feis


def read_lei_docs(path: Path) -> tuple[set[str], set[str]]:
    docs, feis = set(), set()
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            key = normalise_doc(line)
        except ValueError:
            continue
        (feis if key.startswith("FEI:") else docs).add(key[4:] if key.startswith("FEI:") else key)
    return docs, feis


def download(work: Path) -> tuple[Path, Path]:
    """Fetch ``cordata.zip`` and ``corevent.zip`` over SFTP, resuming a
    partial file; refuse a file whose size is not the server's."""
    import paramiko  # noqa: PLC0415 — a builder-only dependency

    work.mkdir(parents=True, exist_ok=True)
    transport = paramiko.Transport((SFTP_HOST, 22))
    transport.default_window_size = 2**27
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    paths = []
    try:
        for name in ("cordata.zip", "corevent.zip"):
            remote = f"{REMOTE_DIR}/{name}"
            local = work / name
            size = sftp.stat(remote).st_size
            have = local.stat().st_size if local.exists() else 0
            if have > size:
                local.unlink()
                have = 0
            if have < size:
                log.info("downloading %s (%s of %s bytes held)", remote, f"{have:,}", f"{size:,}")
                t0 = time.time()
                with sftp.open(remote, "rb") as src, open(local, "ab") as dst:
                    src.seek(have)
                    src.prefetch(file_size=size, max_concurrent_requests=SFTP_CONCURRENCY)
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        dst.write(chunk)
                log.info("  %s MB in %.0fs", f"{(size - have) / 1e6:,.0f}", time.time() - t0)
            if local.stat().st_size != size:
                raise RefusedInput(f"{name}: {local.stat().st_size} bytes, the server says {size}")
            paths.append(local)
    finally:
        sftp.close()
        transport.close()
    return paths[0], paths[1]


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def build(
    cordata: Path,
    corevent: Path,
    out: Path,
    *,
    lei_docs: set[str],
    lei_feis: set[str],
    min_rows: dict[str, int] | None = None,
    expected_members: int = 10,
) -> dict[str, int | str]:
    """Build the index; ``min_rows`` overrides ``MIN_ROWS`` (tests pass ``{}``)."""
    floors = MIN_ROWS if min_rows is None else min_rows
    t0 = time.time()
    zc = _open_zip(cordata)
    members = sorted(
        (i for i in zc.infolist() if CORDATA_MEMBER.match(i.filename)), key=lambda i: i.filename
    )
    if len(members) != expected_members:
        raise RefusedInput(
            f"{cordata.name}: {len(members)} cordata members, expected {expected_members}"
        )
    cut_day = max(_member_day(i) for i in members)
    ze = _open_zip(corevent)
    try:
        event_info = ze.getinfo(EVENT_MEMBER)
    except KeyError as exc:
        raise RefusedInput(f"{corevent.name}: no {EVENT_MEMBER}") from exc
    events_day = _member_day(event_info)
    if abs((cut_day - events_day).days) > MAX_CUT_GAP_DAYS:
        raise RefusedInput(
            f"the corporate file is dated {cut_day} and the events file {events_day} — not one quarter"
        )

    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    con.executescript(SCHEMA)
    stats: dict[str, int] = {
        "records": 0, "active": 0, "kept": 0, "kept_inactive_lei": 0,
        "malformed": 0, "officers": 0, "events": 0, "events_kept": 0,
        "events_malformed": 0, "lei_docs_found": 0,
    }
    kept: set[str] = set()
    ent_batch: list[tuple] = []
    off_batch: list[tuple] = []

    def flush() -> None:
        con.executemany(
            "INSERT OR REPLACE INTO entity (doc, name, status, filing_type, file_date, last_tx, fei,"
            " state_country, p_addr1, p_addr2, p_city, p_state, p_zip, p_country, m_addr1, m_addr2,"
            " m_city, m_state, m_zip, m_country, agent_name, agent_type, agent_city, agent_state,"
            " more_than_six, reports, lei_named) VALUES (" + ",".join("?" * 27) + ")",
            ent_batch,
        )
        con.executemany("INSERT INTO officer VALUES (?,?,?,?,?,?,?,?,?,?)", off_batch)
        ent_batch.clear()
        off_batch.clear()

    for info in members:
        with zc.open(info) as fh:
            for raw in fh:
                rec = parse_record(raw.decode("latin-1"))
                if rec is None:
                    stats["malformed"] += 1
                    continue
                stats["records"] += 1
                active = rec["status"] == "A"
                stats["active"] += active
                named = rec["doc"] in lei_docs or (rec["fei"] is not None and rec["fei"] in lei_feis)
                stats["lei_docs_found"] += rec["doc"] in lei_docs
                if not (active or named):
                    continue
                stats["kept"] += 1
                stats["kept_inactive_lei"] += not active
                kept.add(rec["doc"])
                ent_batch.append((
                    rec["doc"], rec["name"], rec["status"], rec["filing_type"], rec["file_date"],
                    rec["last_tx"], rec["fei"], rec["state_country"], *rec["p"], *rec["m"],
                    rec["agent_name"], rec["agent_type"], rec["agent_city"], rec["agent_state"],
                    rec["more_than_six"], rec["reports"], 1 if named else 0,
                ))
                for o in rec["officers"]:
                    stats["officers"] += 1
                    off_batch.append((
                        rec["doc"], o["seq"], o["title"], o["type"], o["surname"], o["given"],
                        o["middle"], o["name"], o["city"], o["state"],
                    ))
                if len(ent_batch) >= 50_000:
                    flush()
        log.info("%s: %s records so far (%s kept), %.0fs", info.filename,
                 f"{stats['records']:,}", f"{stats['kept']:,}", time.time() - t0)
    flush()

    ev_batch: list[tuple] = []
    with ze.open(EVENT_MEMBER) as fh:
        for raw in fh:
            ev = parse_event(raw.decode("latin-1"))
            if ev is None:
                stats["events_malformed"] += 1
                continue
            stats["events"] += 1
            if ev["code"] not in KEPT_EVENTS or ev["doc"] not in kept:
                continue
            stats["events_kept"] += 1
            ev_batch.append((ev["doc"], ev["seq"], ev["code"], ev["description"], ev["effective"],
                             ev["filed"], ev["other_doc"], ev["name"]))
            if len(ev_batch) >= 50_000:
                con.executemany("INSERT INTO event VALUES (?,?,?,?,?,?,?,?)", ev_batch)
                ev_batch.clear()
    con.executemany("INSERT INTO event VALUES (?,?,?,?,?,?,?,?)", ev_batch)
    log.info("events: %s read, %s kept, %.0fs", f"{stats['events']:,}", f"{stats['events_kept']:,}",
             time.time() - t0)

    refusal = None
    for key in ("records", "active", "events"):
        if stats[key] < floors.get(key, 0):
            refusal = f"only {stats[key]:,} {key} — not the register"
    for key in ("malformed", "events_malformed"):
        if stats[key] > MAX_MALFORMED:
            refusal = f"{stats[key]:,} {key} rows — the format has changed"
    if refusal:
        con.close()
        tmp.unlink()
        raise RefusedInput(refusal)

    con.commit()
    con.execute("INSERT INTO entity_fts (rowid, name) SELECT id, name FROM entity")
    con.executescript(INDEXES)
    built_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.executemany(
        "INSERT OR REPLACE INTO meta VALUES (?,?)",
        [
            ("schema_version", INDEX_SCHEMA_VERSION),
            ("source", "Florida Division of Corporations quarterly corporate data and events"),
            ("source_files", f"{cordata.name}, {corevent.name}"),
            ("cut", cut_day.isoformat()),
            ("events_cut", events_day.isoformat()),
            ("built_at", built_at),
            ("licence", "FL-Public-Records — Florida Department of State, Division of Corporations"),
            ("lei_docs", str(len(lei_docs))),
            ("lei_feis", str(len(lei_feis))),
            *[(f"count_{k}", str(v)) for k, v in stats.items()],
        ],
    )
    con.commit()
    con.execute("VACUUM")
    con.close()
    os.replace(tmp, out)
    result: dict[str, int | str] = dict(stats)
    result["cut"] = cut_day.isoformat()
    result["seconds"] = int(time.time() - t0)
    result["megabytes"] = int(os.path.getsize(out) / 1e6)
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch the quarterly files over SFTP")
    ap.add_argument("--work", type=Path, help="where --download puts the files (default: beside --out)")
    ap.add_argument("--cordata", type=Path)
    ap.add_argument("--corevent", type=Path)
    ap.add_argument("--lei-docs", type=Path, help="registeredAs values, one per line (default: ask GLEIF)")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    try:
        if args.download:
            cordata, corevent = download(args.work or args.out.parent)
        else:
            if not (args.cordata and args.corevent):
                ap.error("--cordata and --corevent, or --download")
            cordata, corevent = args.cordata, args.corevent
        lei_docs, lei_feis = read_lei_docs(args.lei_docs) if args.lei_docs else gleif_registered_as()
        # SQLite cannot write on some mounted folders (the MEIP note): build
        # beside --out only when it is a plain local path.
        stats = build(cordata, corevent, args.out, lei_docs=lei_docs, lei_feis=lei_feis)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], stats["cut"])
    for key, value in stats.items():
        log.info("  %-22s %s", key, value)
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
