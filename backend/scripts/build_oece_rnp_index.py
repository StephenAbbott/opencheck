"""Build the Peruvian RNP supplier index from OECE's open-data file.

*Personas declaradas en la conformación jurídica de proveedores en el
Registro Nacional de Proveedores* — one pipe-delimited, Latin-1 CSV
(``conformacion_juridica.csv``, ~227 MB, ~1.5M rows) attached to OECE's
anonymous-readable Confluence page and replaced on the first days of each
month. Every supplier's latest approved declaration of its partners and
shareholders, legal representatives and governing-body members.

Usage
-----
    python3 scripts/build_oece_rnp_index.py --download --out oece_rnp.sqlite --gzip
    python3 scripts/build_oece_rnp_index.py --csv conformacion_juridica.csv --out oece_rnp.sqlite

What is kept, and what is never kept
------------------------------------
* ``supplier``: one row per supplier RUC with its legal form.
* ``party``: one row per declared entry — role, person/company/class
  (``oece_rnp.classify_party``), name, the entry's start in the RNP, and a
  company member's RUC.
* **No person's document number** (DNI, carnet de extranjería, passport)
  and nothing derived from one (Ley 29733; an 8-digit DNI is enumerable). A
  natural person's ``10…`` RUC is dropped too.

The builder refuses a changed header, a short file, two cuts in one file or
too many malformed rows, and writes nothing when it refuses.
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
import tempfile
import time
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.oece_rnp import (  # noqa: E402
    INDEX_SCHEMA_VERSION,
    ROLES,
    classify_party,
    ruc_check_ok,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_oece_rnp_index")

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"
CONFLUENCE = "https://osce-gob-pe.atlassian.net/wiki"
PAGE_ID = "106889267"
ATTACHMENT_TITLE = "conformacion_juridica.csv"

HEADER = [
    "FECHA_CORTE", "TIPO_DOCUMENTO", "NUMERO_DOCUMENTO",
    "NOMBRE_RAZONODENOMINACIONSOCIAL", "RUC", "TIPO_CONF_JURIDICA",
    "FECHA_INICIO_VIGENCIA", "ID_FORMA_SOCIETARIA", "DE_FORMA_SOCIETARIA",
]
#: 1,493,478 rows on the 2 Nov 2026 cut.
MIN_ROWS = 1_000_000
MAX_MALFORMED = 5_000


class RefusedInput(Exception):
    """The input is not the file this builder reads."""


def _get_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def current_source() -> dict[str, Any]:
    """The attachment's id, version and download URL, read off the page."""
    url = f"{CONFLUENCE}/rest/api/content/{PAGE_ID}/child/attachment?filename={ATTACHMENT_TITLE}&expand=version"
    results = _get_json(url).get("results") or []
    matches = [r for r in results if r.get("title") == ATTACHMENT_TITLE]
    if len(matches) != 1:
        raise RefusedInput(f"expected one {ATTACHMENT_TITLE} on page {PAGE_ID}, found {len(matches)}")
    att = matches[0]
    return {
        "id": att["id"],
        "version": (att.get("version") or {}).get("number"),
        "when": (att.get("version") or {}).get("when"),
        "url": f"{CONFLUENCE}/rest/api/content/{PAGE_ID}/child/attachment/{att['id']}/download",
    }


def _download(url: str, dest: Path) -> None:
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=900) as resp, open(dest, "wb") as fh:
                shutil.copyfileobj(resp, fh, 1 << 20)
            with open(dest, "rb") as fh:
                if not fh.read(64).startswith(b"FECHA_CORTE|"):
                    raise RefusedInput(f"{url} did not answer with the RNP file")
            return
        except OSError as exc:
            if attempt == 3:
                raise
            wait = 10 * 2**attempt
            log.warning("download failed (%s); retrying in %ss", type(exc).__name__, wait)
            time.sleep(wait)


def _iso(day: str) -> str | None:
    day = day.strip()
    if not re.fullmatch(r"\d{8}", day):
        return None
    try:
        return datetime.strptime(day, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def build(
    csv_path: Path, out: Path, *, source: dict[str, Any] | None = None, min_rows: int = MIN_ROWS
) -> dict[str, Any]:
    started = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(tmp)
    conn.executescript(
        """
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE supplier (ruc INTEGER PRIMARY KEY, form_id TEXT, form TEXT) WITHOUT ROWID;
        CREATE TABLE party (
            ruc INTEGER NOT NULL, seq INTEGER NOT NULL, role TEXT NOT NULL,
            kind TEXT NOT NULL, name TEXT NOT NULL, party_ruc INTEGER, since TEXT,
            PRIMARY KEY (ruc, seq)
        ) WITHOUT ROWID;
        """
    )
    cuts: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    roles: Counter[str] = Counter()
    malformed = 0
    rows = 0
    seq: Counter[int] = Counter()
    suppliers: dict[int, tuple[str, str]] = {}
    batch: list[tuple[Any, ...]] = []
    with open(csv_path, encoding="latin-1", newline="") as fh:
        header = fh.readline().rstrip("\r\n").split("|")
        if header != HEADER:
            raise RefusedInput(f"unexpected header: {header}")
        for line in fh:
            fields = line.rstrip("\r\n").split("|")
            if len(fields) != len(HEADER):
                malformed += 1
                continue
            cut, _doc_type, document, name, ruc, role, since, form_id, form = (f.strip() for f in fields)
            if not ruc_check_ok(ruc) or role not in ROLES or not name:
                malformed += 1
                continue
            rows += 1
            cuts[cut] += 1
            kind, party_ruc = classify_party(name, document)
            kinds[kind] += 1
            code = ROLES[role][0]
            roles[code] += 1
            key = int(ruc)
            seq[key] += 1
            suppliers.setdefault(key, (form_id, form))
            batch.append(
                (key, seq[key], code, kind, " ".join(name.split()),
                 int(party_ruc) if party_ruc else None, _iso(since))
            )
            if len(batch) >= 50_000:
                conn.executemany("INSERT INTO party VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
                batch.clear()
    if batch:
        conn.executemany("INSERT INTO party VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
    if rows < min_rows:
        conn.close()
        tmp.unlink()
        raise RefusedInput(f"only {rows} rows (want ≥ {min_rows})")
    if malformed > MAX_MALFORMED:
        conn.close()
        tmp.unlink()
        raise RefusedInput(f"{malformed} malformed rows (allow ≤ {MAX_MALFORMED})")
    if len(cuts) != 1:
        conn.close()
        tmp.unlink()
        raise RefusedInput(f"more than one cut in the file: {dict(cuts)}")
    cut = _iso(next(iter(cuts)))
    if not cut:
        conn.close()
        tmp.unlink()
        raise RefusedInput(f"unreadable cut: {next(iter(cuts))!r}")
    conn.executemany(
        "INSERT INTO supplier VALUES (?, ?, ?)",
        ((k, v[0] or None, v[1] or None) for k, v in suppliers.items()),
    )
    conn.execute("CREATE INDEX party_member ON party (party_ruc) WHERE party_ruc IS NOT NULL")
    built_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "cut": cut,
        "built_at": built_at,
        "rows": str(rows),
        "suppliers": str(len(suppliers)),
        "malformed": str(malformed),
        "kinds": json.dumps(dict(kinds), sort_keys=True),
        "roles": json.dumps(dict(roles), sort_keys=True),
    }
    if source:
        meta.update({f"source_{k}": str(v) for k, v in source.items() if v is not None})
    conn.executemany("INSERT INTO meta VALUES (?, ?)", meta.items())
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out)
    return {
        **{k: meta[k] for k in ("cut", "rows", "suppliers", "malformed", "kinds", "roles")},
        "megabytes": int(os.path.getsize(out) / 1e6),
        "seconds": int(time.time() - started),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch the file from OECE's Confluence page")
    ap.add_argument("--csv", type=Path, help="a local conformacion_juridica.csv")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    ap.add_argument("--sources-out", type=Path, help="write the attachment read, as JSON")
    ap.add_argument(
        "--skip-if-unchanged", type=Path,
        help="a previous --sources-out file: exit 3 without building when the attachment is unchanged",
    )
    args = ap.parse_args(argv)

    source: dict[str, Any] | None = None
    workdir: tempfile.TemporaryDirectory[str] | None = None
    try:
        if args.download:
            source = current_source()
            if args.skip_if_unchanged and args.skip_if_unchanged.exists():
                previous = json.loads(args.skip_if_unchanged.read_text(encoding="utf-8"))
                if all(previous.get(k) == source[k] for k in ("id", "version")):
                    log.info("attachment unchanged since the last build: %s", source)
                    return 3
            workdir = tempfile.TemporaryDirectory(prefix="oece-rnp-")
            csv_path = Path(workdir.name) / ATTACHMENT_TITLE
            log.info("downloading %s (version %s)", source["url"], source["version"])
            _download(source["url"], csv_path)
        else:
            if not args.csv:
                ap.error("--csv, or --download")
            csv_path = args.csv
        stats = build(csv_path, args.out, source=source)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    finally:
        if workdir is not None:
            workdir.cleanup()
    log.info("wrote %s (%s MB, %ss)", args.out, stats["megabytes"], stats["seconds"])
    for key, value in stats.items():
        log.info("  %-12s %s", key, value)
    if args.sources_out and source:
        args.sources_out.write_text(json.dumps(source, indent=2, sort_keys=True), encoding="utf-8")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
