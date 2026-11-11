"""Build the Répertoire national des associations (RNA) index.

Packs the Ministry of the Interior's monthly **waldec** file — one CSV per
département inside ``rna_waldec_<YYYYMM01>.zip`` (~410 MB zipped, ~1.5 GB of
CSV, ~2.3M associations) — into ``rna.sqlite``, one row per ``W`` number,
keeping only :data:`opencheck.sources.rna_index.COLUMNS`. The ``adrg_*``
management address (the declarant's, for most associations), the director's
title and the prefecture's free text are **never written** (an allowlist, not a
denylist).

The file is found through data.gouv.fr's API (dataset
``repertoire-national-des-associations``): the newest ``rna_waldec_*.zip``
first, then the month before when that one does not download.
``media.interieur.gouv.fr`` sits behind a CDN that answers an intermittent
HTTP 503 with an HTML page (the October 2026 file, from the cloud, on
11 Nov 2026), so every download is retried, must start with ``PK``, and every
member's header row must equal :data:`WALDEC_HEADER`. Nothing is written when
the build refuses.

Usage
-----
    uv run python scripts/build_rna_index.py --out rna.sqlite \\
        [--zip local.zip] [--sirene-parquet StockUniteLegale.parquet] [--gzip]

Standard library plus duckdb (a backend dependency), which reads the two
columns of INSEE's ``StockUniteLegale`` parquet the filter needs. ``.github/workflows/refresh-rna-index.yml`` runs it
monthly and replaces the ``rna-index`` release asset; the backend downloads it
at boot (``RNA_DB_URL``).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.rna_index import (  # noqa: E402
    COLUMNS,
    INDEX_SCHEMA_VERSION,
    SIRENE_COLUMNS,
    WALDEC_HEADER,
    iso_day,
    normalise_rna,
)

log = logging.getLogger("build_rna_index")

DATASET_API = "https://www.data.gouv.fr/api/1/datasets/repertoire-national-des-associations/"
SIRENE_DATASET_API = (
    "https://www.data.gouv.fr/api/1/datasets/"
    "base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/"
)
#: The CDN refuses urllib's default agent.
USER_AGENT = "Mozilla/5.0 (compatible; OpenCheck RNA index builder; +https://opencheck.world)"
_WALDEC_NAME = re.compile(r"rna_waldec_(\d{4})(\d{2})(\d{2})\.zip$")

#: A rowid table with a unique index on ``id``, not ``WITHOUT ROWID``: the
#: purpose text makes rows long, and a clustered index on long rows spills to
#: overflow pages (687 MB against 539 MB for the 1 Sept 2026 file).
#: A build reading fewer waldec rows is refused (2,306,094 on 1 Sept 2026).
MIN_ROWS = 2_000_000
#: …or keeping fewer associations named by Sirene (~1.1M).
MIN_KEPT = 800_000
#: …or a Sirene file naming fewer associations.
MIN_SIRENE_ASSOCIATIONS = 900_000
#: Malformed rows (a bad W number, a short row) tolerated before refusing.
MAX_MALFORMED = 2_000
#: The département files a full extract carries at least.
MIN_MEMBERS = 95

#: Columns stored as ISO days (NULL for the file's ``0001-01-01``).
_DATE_COLUMNS = frozenset({"date_creat", "date_decla", "date_publi", "date_disso"})

SCHEMA = f"""
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE association ({", ".join(f"{c} TEXT" for c in COLUMNS + SIRENE_COLUMNS)});
CREATE UNIQUE INDEX association_id ON association(id);
"""


class BuildRefused(Exception):
    """The input is not a waldec extract OpenCheck can trust."""


def _get(url: str, *, timeout: float = 600.0, attempts: int = 4) -> urllib.request.addinfourl:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            return urllib.request.urlopen(req, timeout=timeout)  # noqa: S310 — fixed https hosts
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
            wait = 15 * (2**attempt)
            log.warning("GET %s failed (%s); retrying in %ss", url, type(exc).__name__, wait)
            time.sleep(wait)
    raise BuildRefused(f"{url} did not answer after {attempts} attempts: {type(last).__name__}")


def waldec_candidates() -> list[tuple[str, str]]:
    """``(cut, url)`` for every waldec zip the dataset lists, newest first."""
    with _get(DATASET_API, timeout=60) as resp:
        body = json.load(resp)
    found: dict[str, str] = {}
    for resource in body.get("resources") or []:
        url = str(resource.get("url") or "")
        match = _WALDEC_NAME.search(url)
        if match:
            found[f"{match[1]}-{match[2]}-{match[3]}"] = url
    return sorted(found.items(), reverse=True)


def download(url: str, dest: Path, *, attempts: int = 4) -> None:
    """Download to ``dest``; the body must be a zip (``PK``), not the CDN's
    HTML error page."""
    for attempt in range(attempts):
        try:
            with _get(url) as resp, open(dest, "wb") as out:
                shutil.copyfileobj(resp, out, length=1 << 20)
            with open(dest, "rb") as fh:
                if fh.read(2) == b"PK":
                    return
            log.warning("%s answered something that is not a zip; retrying", url)
        except BuildRefused:
            raise
        except OSError as exc:
            log.warning("%s download failed (%s)", url, type(exc).__name__)
        time.sleep(30 * (2**attempt))
    raise BuildRefused(f"{url} never answered with a zip")


def sirene_parquet_url() -> str:
    """The newest ``StockUniteLegale`` parquet the Sirene dataset lists."""
    with _get(SIRENE_DATASET_API, timeout=60) as resp:
        body = json.load(resp)
    for resource in body.get("resources") or []:
        url = str(resource.get("url") or "")
        title = str(resource.get("title") or "")
        if "StockUniteLegale " in title + " " and "Historique" not in title and url.endswith(".parquet"):
            return url
    raise BuildRefused("the Sirene dataset lists no StockUniteLegale parquet")


def sirene_associations(parquet: Path) -> dict[str, str]:
    """RNA number → SIREN, for every Sirene unit that names one and is fully
    public (``statutDiffusionUniteLegale`` ``O``). Reads two columns only.
    A W number two units name keeps the lower SIREN (deterministic)."""
    import duckdb  # a backend dependency; the only non-stdlib import here

    rows = duckdb.sql(
        "SELECT identifiantAssociationUniteLegale AS rna, siren FROM read_parquet(?) "
        "WHERE identifiantAssociationUniteLegale IS NOT NULL "
        "AND statutDiffusionUniteLegale = 'O'",
        params=[str(parquet)],
    ).fetchall()
    found: dict[str, str] = {}
    for rna, siren in rows:
        try:
            key = normalise_rna(rna)
        except ValueError:
            continue
        siren = str(siren or "").zfill(9)
        if key not in found or siren < found[key]:
            found[key] = siren
    if len(found) < MIN_SIRENE_ASSOCIATIONS:
        raise BuildRefused(f"Sirene names {len(found)} associations, expected at least {MIN_SIRENE_ASSOCIATIONS}")
    return found


def cut_of(name: str) -> str | None:
    match = _WALDEC_NAME.search(name)
    return f"{match[1]}-{match[2]}-{match[3]}" if match else None


#: Windows-1252 punctuation the file carries as C1 control characters (an
#: apostrophe arrives as U+0092): read back as the character it was typed as.
_C1 = re.compile("[\x80-\x9f]")


def _cp1252(match: re.Match[str]) -> str:
    return bytes([ord(match.group())]).decode("cp1252", errors="ignore")


def _clean(column: str, value: str | None) -> str | None:
    text = " ".join(_C1.sub(_cp1252, str(value or "")).split())
    if not text or text == "_":
        return None
    if column in _DATE_COLUMNS:
        return iso_day(text)
    return text


def build(
    zip_path: Path,
    out: Path,
    *,
    cut: str,
    source_url: str,
    sirene: dict[str, str] | None,
    sirene_source: str = "",
) -> dict[str, str]:
    """Pack ``zip_path`` into ``out``, keeping the associations ``sirene``
    names (every one when it is None — a local experiment, never a release).
    Raises :class:`BuildRefused` (and writes nothing) on a member with a wrong
    header, too few rows or too many malformed ones."""
    with zipfile.ZipFile(zip_path) as archive:
        members = sorted(n for n in archive.namelist() if n.lower().endswith(".csv"))
        if len(members) < MIN_MEMBERS:
            raise BuildRefused(f"{len(members)} CSV members, expected at least {MIN_MEMBERS}")
        tmp = Path(tempfile.mkdtemp(prefix="rna-")) / "rna.sqlite"
        conn = sqlite3.connect(tmp)
        conn.executescript(SCHEMA)
        insert = (
            f"INSERT INTO association ({', '.join(COLUMNS + SIRENE_COLUMNS)}) "
            f"VALUES ({', '.join('?' * len(COLUMNS + SIRENE_COLUMNS))}) "
            "ON CONFLICT(id) DO UPDATE SET "
            + ", ".join(f"{c} = excluded.{c}" for c in COLUMNS + SIRENE_COLUMNS if c != "id")
            + " WHERE coalesce(excluded.maj_time, '') >= coalesce(association.maj_time, '')"
        )
        rows = malformed = 0
        positions: Counter[str] = Counter()
        for member in members:
            with archive.open(member) as raw:
                reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""), delimiter=";")
                header = tuple(next(reader, ()))
                if header != WALDEC_HEADER:
                    raise BuildRefused(f"{member}: unexpected header {header[:6]}…")
                index = {name: i for i, name in enumerate(header)}
                batch: list[tuple[str | None, ...]] = []
                for record in reader:
                    if len(record) != len(header):
                        malformed += 1
                        continue
                    try:
                        rna = normalise_rna(record[index["id"]])
                    except ValueError:
                        malformed += 1
                        continue
                    rows += 1
                    siren = sirene.get(rna) if sirene is not None else None
                    if sirene is not None and siren is None:
                        continue
                    values = [rna if c == "id" else _clean(c, record[index[c]]) for c in COLUMNS]
                    batch.append((*values, siren))
                    positions[values[COLUMNS.index("position")] or ""] += 1
                    if len(batch) >= 20_000:
                        conn.executemany(insert, batch)
                        batch.clear()
                conn.executemany(insert, batch)
            if malformed > MAX_MALFORMED:
                raise BuildRefused(f"{malformed} malformed rows (limit {MAX_MALFORMED})")
        if rows < MIN_ROWS:
            raise BuildRefused(f"{rows} rows, expected at least {MIN_ROWS}")
        stored = conn.execute("SELECT COUNT(*) FROM association").fetchone()[0]
        if sirene is not None and stored < MIN_KEPT:
            raise BuildRefused(f"{stored} associations named by Sirene, expected at least {MIN_KEPT}")
        meta = {
            "schema_version": INDEX_SCHEMA_VERSION,
            "built_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "cut": cut,
            "source_url": source_url,
            "rows_read": str(rows),
            "associations": str(stored),
            "filtered_by_sirene": "yes" if sirene is not None else "no",
            "sirene_source": sirene_source,
            "sirene_named": str(len(sirene)) if sirene is not None else "",
            "malformed": str(malformed),
            "positions": json.dumps(dict(sorted(positions.items()))),
        }
        conn.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
        conn.commit()
        conn.execute("VACUUM")
        conn.close()
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(tmp), out)
    return meta


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--zip", type=Path, help="a local rna_waldec_YYYYMMDD.zip (skips the download)")
    parser.add_argument("--cut", help="the extract day, when --zip's name does not carry it")
    parser.add_argument("--sirene-parquet", type=Path,
                        help="a local StockUniteLegale parquet (else the newest is downloaded)")
    parser.add_argument("--all", action="store_true",
                        help="keep every association, not only those Sirene names (experiments only)")
    parser.add_argument("--gzip", action="store_true")
    args = parser.parse_args(argv)

    try:
        sirene: dict[str, str] | None = None
        sirene_source = ""
        if not args.all:
            if args.sirene_parquet:
                sirene_source = args.sirene_parquet.name
                sirene = sirene_associations(args.sirene_parquet)
            else:
                url = sirene_parquet_url()
                sirene_source = url
                with tempfile.TemporaryDirectory(prefix="sirene-dl-") as work:
                    dest = Path(work) / "StockUniteLegale.parquet"
                    with _get(url) as resp, open(dest, "wb") as out:
                        shutil.copyfileobj(resp, out, length=1 << 20)
                    sirene = sirene_associations(dest)
            log.info("Sirene names %d associations", len(sirene))
        if args.zip:
            cut = args.cut or cut_of(args.zip.name)
            if not cut:
                parser.error("--cut is required when the zip's name carries no date")
            meta = build(args.zip, args.out, cut=cut, source_url=args.zip.name,
                         sirene=sirene, sirene_source=sirene_source)
        else:
            candidates = waldec_candidates()[:2]
            if not candidates:
                raise BuildRefused("the dataset lists no rna_waldec_*.zip")
            meta = None
            for cut, url in candidates:
                with tempfile.TemporaryDirectory(prefix="rna-dl-") as work:
                    dest = Path(work) / Path(url).name
                    try:
                        download(url, dest)
                    except BuildRefused as exc:
                        log.warning("%s; trying the month before", exc)
                        continue
                    meta = build(dest, args.out, cut=cut, source_url=url,
                                 sirene=sirene, sirene_source=sirene_source)
                    break
            if meta is None:
                raise BuildRefused("neither of the two newest waldec files downloaded")
    except BuildRefused as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("built %s: %s", args.out, json.dumps(meta, sort_keys=True))
    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
        log.info("gzipped: %s (%d bytes)", f"{args.out}.gz", os.path.getsize(f"{args.out}.gz"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
