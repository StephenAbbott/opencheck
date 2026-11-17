#!/usr/bin/env python3
"""Build one edition of the monthly BODS series: every LEI in GLEIF's Golden
Copy as BODS v0.4, published as what changed since the last edition.

The first layer of the layered monthly diff (the "Demonstrating change over
time using OpenCheck BODS data" ticket; Stephen's decisions of 17 Nov 2026 on
the ledger ticket): **GLEIF only**, **changes only**, one **GitHub release a
month**, dated the **first of the month** and built from the Golden Copy
mirror regenerated that day.

No API calls. Every LEI in the mirror (``entity_pages.sqlite``, the
``entity-pages-latest`` release asset) is mapped with OpenCheck's own GLEIF
mapper from the bundle a lookup is served from — the Estonia dataset's code
path (``sweep_estonia.gleif_network``): the LEI's own entity statement, its
direct and ultimate parent edges, and its reporting exceptions as filed. Each
edge is published once, from the child; a parent's entity statement comes
from the parent's own bundle. The publication ledger
(``opencheck/bods/publication_ledger.py``) then decides what this edition
says: ``new`` for a record it has not published, ``updated`` for one whose
``recordDetails`` changed (with a ``correcting`` annotation when OpenCheck's
reading version of GLEIF changed in between), ``closed`` for one the Golden
Copy no longer carries, and nothing for a record that is unchanged.

Stages (``--stage all`` runs them in order; ``map`` is resumable per shard)::

    map     → <work>/parties-<k>.jsonl.gz, relationships-<k>.jsonl.gz
    ledger  → <out>/opencheck-bods-gleif-<edition>-NN.jsonl.gz (parties
              before relationships), manifest.json, README.md; the ledger
              file is updated in place

Usage (from ``backend/``)::

    uv run python scripts/build_bods_series.py \\
        --entity-pages-db ../data/entity_pages.sqlite \\
        --ledger ../data/bods-series/ledger.sqlite \\
        --out-dir ../data/bods-series/out [--workers 4]

``--limit N`` maps the first N LEIs only (a trial): nothing is closed, and
the manifest says the edition is partial. Licence of the output: CC0 1.0
(GLEIF's data is CC0; OpenCheck adds no restriction).
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import multiprocessing as mp
import os
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))  # the opencheck package
sys.path.insert(0, str(_HERE))  # sweep_estonia

SOURCE_ID = "gleif"
DATASET_PREFIX = "opencheck-bods-gleif"
#: Statements per output part. ~100 bytes a statement gzipped, so a part
#: stays far under GitHub's 2 GB asset limit.
PART_STATEMENTS = 4_000_000
#: A delta whose relationships name more missing parties than this is refused.
MAX_DANGLING = 100
#: The full Golden Copy has passed 3.3M records (the mirror build's own gate).
MIN_FULL_LEIS = 3_300_000


def configure(entity_pages_db: Path) -> None:
    """Point the package at the mirror and turn live calls off."""
    os.environ["OPENCHECK_ENTITY_PAGES_DB_FILE"] = str(entity_pages_db)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "false"
    from opencheck import entity_pages
    from opencheck.config import get_settings

    get_settings.cache_clear()
    entity_pages.reset_store_for_tests()


def mirror_meta(db: Path) -> dict[str, str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return {
            str(k): str(v)
            for k, v in conn.execute("SELECT key, value FROM meta WHERE key != 'detail_zdict'")
        }
    finally:
        conn.close()


def publish_day(meta: dict[str, str]) -> str:
    raw = meta.get("source_publish_datetime") or meta.get("source_publish_date") or ""
    day = raw[:10]
    dt.date.fromisoformat(day)
    return day


def edition_for(meta: dict[str, str], requested: str | None) -> str:
    """The edition's date: the first of the month of the Golden Copy publish
    the mirror was built from. A mirror not built from a publish of the
    first is refused unless the edition is named — a mid-month trial."""
    day = publish_day(meta)
    if requested:
        dt.date.fromisoformat(requested)
        return requested
    if not day.endswith("-01"):
        raise SystemExit(
            f"the mirror was built from the Golden Copy of {day}, not of the first of a "
            "month; wait for the monthly rebuild or name the edition with --edition"
        )
    return day


def all_leis(db: Path, limit: int | None) -> list[str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        sql = "SELECT lei FROM entities ORDER BY lei"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [r[0] for r in conn.execute(sql)]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stage: map
# ---------------------------------------------------------------------------


def statements_for(lei: str) -> list[dict[str, Any]]:
    """What one LEI publishes of itself: its entity statement, its parent
    edges and its reporting exceptions. A parent's entity statement is left
    to the parent's own bundle — unless the mirror holds no record of it,
    when the copy on this edge is all there is."""
    from opencheck import entity_pages
    from sweep_estonia import _lei_of, gleif_network

    statements, _subject = gleif_network(lei, set())
    store = entity_pages.get_store()
    out: list[dict[str, Any]] = []
    for stmt in statements:
        if stmt.get("recordType") == "entity":
            other = _lei_of(stmt)
            if other and other != lei and store is not None and store.get(other) is not None:
                continue
        out.append(stmt)
    return out


_LEIS: list[str] = []


def _map_shard(job: tuple[int, int, str, str]) -> dict[str, int]:
    shard, shards, db, work = job
    configure(Path(db))
    done = Path(work) / f"shard-{shard}.done"
    if done.exists():
        return dict(json.loads(done.read_text()))
    counts: Counter[str] = Counter()
    parties = gzip.open(Path(work) / f"parties-{shard}.jsonl.gz", "wt", encoding="utf-8", compresslevel=1)
    relationships = gzip.open(
        Path(work) / f"relationships-{shard}.jsonl.gz", "wt", encoding="utf-8", compresslevel=1
    )
    with parties, relationships:
        for lei in _LEIS[shard::shards]:
            statements = statements_for(lei)
            if not statements:
                counts["not_mapped"] += 1
                continue
            counts["leis"] += 1
            for stmt in statements:
                kind = str(stmt.get("recordType"))
                counts[kind] += 1
                fh = relationships if kind == "relationship" else parties
                fh.write(json.dumps(stmt, ensure_ascii=False, separators=(",", ":")) + "\n")
    done.write_text(json.dumps(counts))
    return dict(counts)


def stage_map(args: argparse.Namespace) -> dict[str, int]:
    global _LEIS
    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    _LEIS = all_leis(Path(args.entity_pages_db), args.limit)
    shards = max(1, int(args.workers))
    jobs = [(k, shards, str(args.entity_pages_db), str(work)) for k in range(shards)]
    started = time.time()
    total: Counter[str] = Counter()
    if shards == 1:
        total.update(_map_shard(jobs[0]))
    else:
        # fork: the workers inherit the LEI list instead of pickling it.
        with mp.get_context("fork").Pool(shards) as pool:
            for counts in pool.imap_unordered(_map_shard, jobs):
                total.update(counts)
    total["selected"] = len(_LEIS)
    (work / "map.json").write_text(json.dumps({"shards": shards, "counts": total}))
    print(f"map: {dict(total)} in {time.time() - started:,.0f}s", flush=True)
    return dict(total)


# ---------------------------------------------------------------------------
# Stage: ledger
# ---------------------------------------------------------------------------


def _read(work: Path, kind: str, shards: int) -> Iterator[dict[str, Any]]:
    for k in range(shards):
        with gzip.open(work / f"{kind}-{k}.jsonl.gz", "rt", encoding="utf-8") as fh:
            for line in fh:
                yield json.loads(line)


class PartWriter:
    """The edition's statements as numbered gzip parts, each with its size
    and SHA-256 for the manifest."""

    def __init__(self, out_dir: Path, edition: str, per_part: int) -> None:
        self.out_dir = out_dir
        self.edition = edition
        self.per_part = per_part
        self.parts: list[dict[str, Any]] = []
        self._fh: Any = None
        self._path: Path | None = None
        self._n = 0

    def _open(self) -> None:
        name = f"{DATASET_PREFIX}-{self.edition}-{len(self.parts) + 1:02d}.jsonl.gz"
        self._path = self.out_dir / name
        self._fh = gzip.open(self._path, "wt", encoding="utf-8", compresslevel=6)
        self._n = 0

    def write(self, stmt: dict[str, Any]) -> None:
        if self._fh is None:
            self._open()
        self._fh.write(json.dumps(stmt, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._n += 1
        if self._n >= self.per_part:
            self.close()

    def close(self) -> None:
        if self._fh is None or self._path is None:
            return
        self._fh.close()
        digest = hashlib.sha256()
        with open(self._path, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                digest.update(block)
        self.parts.append(
            {
                "file": self._path.name,
                "statements": self._n,
                "bytes": self._path.stat().st_size,
                "sha256": digest.hexdigest(),
            }
        )
        self._fh = None
        self._path = None


def _party_refs(stmt: dict[str, Any]) -> list[str]:
    details = stmt.get("recordDetails") or {}
    return [v for v in (details.get("subject"), details.get("interestedParty")) if isinstance(v, str)]


def stage_ledger(args: argparse.Namespace) -> dict[str, Any]:
    from opencheck.bods.publication_ledger import PublicationLedger
    from opencheck.bods.reading_versions import reading_version

    work = Path(args.work_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    mapped = json.loads((work / "map.json").read_text())
    shards = int(mapped["shards"])
    meta = mirror_meta(Path(args.entity_pages_db))
    edition = edition_for(meta, args.edition)
    partial = bool(args.limit) or mapped["counts"].get("selected", 0) < MIN_FULL_LEIS
    reading = reading_version(SOURCE_ID)

    Path(args.ledger).parent.mkdir(parents=True, exist_ok=True)
    ledger = PublicationLedger(args.ledger)
    previous = ledger.last_edition()
    if previous is None:
        # Interleaved, so the dictionary's 32 KB holds every record type.
        kinds = [_read(work, kind, 1) for kind in ("parties", "relationships")]
        sample: list[dict[str, Any]] = []
        for pair in zip(*kinds, strict=False):
            sample.extend(pair)
            if len(sample) >= 400:
                break
        ledger.train(sample)

    writer = PartWriter(out_dir, edition, int(args.part_statements))
    started = time.time()
    dangling: list[str] = []
    n_dangling = 0
    with ledger.edition(edition) as ed:
        for stmt in ed.apply(SOURCE_ID, _read(work, "parties", shards), reading=reading):
            writer.write(stmt)
        print(f"ledger: parties applied in {time.time() - started:,.0f}s", flush=True)
        for stmt in ed.apply(SOURCE_ID, _read(work, "relationships", shards), reading=reading):
            for ref in _party_refs(stmt):
                if not ledger.is_live(ref):
                    n_dangling += 1
                    if len(dangling) < 20:
                        dangling.append(f"{stmt['statementId']} → {ref}")
            writer.write(stmt)
        print(f"ledger: relationships applied in {time.time() - started:,.0f}s", flush=True)
        missing, live = ed.missing(SOURCE_ID)
        if partial:
            print(f"ledger: partial edition — {missing:,} unseen records left open", flush=True)
        else:
            for stmt in ed.close_missing(SOURCE_ID):
                writer.write(stmt)
        writer.close()
        if n_dangling > MAX_DANGLING and not partial:
            raise SystemExit(
                f"{n_dangling:,} relationship references name no live record "
                f"(first: {dangling[:5]}); the edition was not recorded"
            )
        counts = dict(ed.counts)

    manifest = {
        "dataset": DATASET_PREFIX,
        "edition": edition,
        "previous_edition": previous,
        "partial": partial,
        "bods_version": "0.4",
        "licence": "CC0-1.0",
        "sources": {
            SOURCE_ID: {
                "golden_copy_publish": meta.get("source_publish_datetime")
                or meta.get("source_publish_date"),
                "mirror_built_at": meta.get("built_at"),
                "reading_version": reading,
                "leis_in_mirror": mapped["counts"].get("selected"),
                "mapped": {k: v for k, v in mapped["counts"].items() if k != "selected"},
            }
        },
        "statements": {k.split("|", 1)[1]: v for k, v in sorted(counts.items())},
        "statements_published": sum(p["statements"] for p in writer.parts),
        "live_records": ledger.live_counts(),
        "dangling_references": {"count": n_dangling, "first": dangling},
        "parts": writer.parts,
        "built_at": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    (out_dir / "README.md").write_text(render_readme(manifest))
    ledger.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    ledger.close()
    print(f"ledger: {manifest['statements']} in {time.time() - started:,.0f}s", flush=True)
    return manifest


def render_readme(manifest: dict[str, Any]) -> str:
    """The release notes: what the edition is and how to read it."""
    s = manifest["statements"]
    gleif = manifest["sources"][SOURCE_ID]
    first = manifest["previous_edition"] is None
    lines = [
        f"# OpenCheck BODS series — GLEIF, edition of {manifest['edition']}",
        "",
        "Every Legal Entity Identifier in GLEIF's Golden Copy as Beneficial Ownership Data "
        "Standard (BODS) v0.4 statements: each LEI's entity record, its direct and ultimate "
        "accounting-consolidation parents, and the reporting exceptions filed in their place.",
        "",
        (
            "This is the first edition: every record is published `new`."
            if first
            else f"This edition holds only what changed since {manifest['previous_edition']}. "
            "A record that is unchanged has no statement here."
        ),
        "",
        f"- Golden Copy publish: {gleif['golden_copy_publish']} (UTC)",
        f"- LEIs read: {gleif['leis_in_mirror']:,}",
        f"- Statements published: {manifest['statements_published']:,}",
        f"- New records: {s.get('new', 0):,}; returned after closing: {s.get('returned', 0):,}",
        f"- Updated: {s.get('updated', 0):,}; updated with a correction note: {s.get('corrected', 0):,}",
        f"- Closed: {s.get('closed', 0):,}",
        f"- Unchanged (not published): {s.get('unchanged', 0):,}",
        "",
    ]
    if manifest["partial"]:
        lines += ["**Partial edition** — a trial over part of the Golden Copy. Nothing was closed.", ""]
    lines += [
        "## How to read it",
        "",
        "- A record keeps one `recordId` across editions. Its first statement is "
        "`recordStatus: new`, a later change `updated`, its last `closed`.",
        "- `publicationDetails.publicationDate` is the edition; `statementDate` is GLEIF's "
        "own date for the record (for a `closed` statement, the edition).",
        "- An `updated` statement with a `correcting` annotation was published after "
        "OpenCheck changed how it reads GLEIF: what differs may be OpenCheck's reading "
        "and not GLEIF's record.",
        "- A `closed` statement means the Golden Copy no longer carries the record — a "
        "parent relationship that ended, a reporting exception withdrawn or replaced. "
        "GLEIF gives no end date, so none is written.",
        "- A record that returns after closing has a new `recordId`.",
        "- GLEIF Level 2 is accounting consolidation, not shareholding: no statement "
        "here states a percentage.",
        "- Parts are numbered; entity and person statements come before relationships. "
        "`manifest.json` lists each part's size and SHA-256.",
        "",
        "Source: GLEIF Golden Copy (CC0 1.0). Mapped by OpenCheck, https://opencheck.world. "
        "Licence of this dataset: CC0 1.0.",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--stage", choices=("map", "ledger", "all"), default="all")
    parser.add_argument("--entity-pages-db", required=True, type=Path)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--edition", default=None, help="YYYY-MM-DD; default: the mirror's first-of-month publish")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 0))
    parser.add_argument("--limit", type=int, default=None, help="map only the first N LEIs (a trial)")
    parser.add_argument("--part-statements", type=int, default=PART_STATEMENTS)
    args = parser.parse_args(argv)
    if args.work_dir is None:
        args.work_dir = Path(args.out_dir) / "work"
    configure(args.entity_pages_db)
    # Fail before an hour of mapping, not after it.
    edition_for(mirror_meta(args.entity_pages_db), args.edition)
    if args.stage in ("map", "all"):
        stage_map(args)
    if args.stage in ("ledger", "all"):
        stage_ledger(args)


if __name__ == "__main__":
    main()
