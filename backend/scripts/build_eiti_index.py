"""Build the committed EITI organisation-identifier index.

The EITI API (https://eiti.org/api) exposes ``/api/v2.0/organisation`` with
90% of companies carrying a national registry/tax ``identification`` — but
the documented ``identification`` filter is not implemented server-side
(verified 2026-07-07: nonsense values return the unfiltered set). Until
EITI fixes that, OpenCheck matches locally: this script crawls the full
organisation table (~1,650 pages of 50) and condenses it into
``opencheck/data/eiti_organisations.json.gz``, keyed::

    {"meta": {...},
     "index": {"GB": {"01285743": [{"id": "122855", "year": "2018",
                                     "label": "Equinor UK Ltd"}], ...}, ...}}

The lookup adapter matches ``(jurisdiction, registeredAs)`` from the GLEIF
anchor against this index, then fetches live per-company payment data via
the (working) ``/api/v2.0/revenue?organisation={id}`` filter.

Usage:
    python scripts/build_eiti_index.py            # full crawl (~3-5 min)
    python scripts/build_eiti_index.py --max-pages 40   # smoke run

Re-run when EITI refreshes summary data (roughly quarterly) and commit the
regenerated artifact.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import gzip
import json
import urllib.request
from collections import defaultdict
from pathlib import Path

API = "https://eiti.org/api/v2.0/organisation"
PAGE_SIZE = 50

# Template noise observed in the identification field.
_JUNK_MARKERS = (
    "if available",
    "link to the registry",
    "not applicable",
    "n/a",
)


def _clean_identification(raw: str | None) -> str | None:
    ident = (raw or "").strip()
    if not ident:
        return None
    low = ident.lower()
    if any(marker in low for marker in _JUNK_MARKERS):
        return None
    # Multi-value cells (seen in NL data) are ambiguous — skip rather than
    # guess; the company can still be matched in another reporting year.
    if "\n" in ident:
        return None
    return ident


def _fetch_page(page: int) -> list[dict]:
    url = f"{API}?page={page}&limit={PAGE_SIZE}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.load(resp)
    return payload.get("data") or []


def build(max_pages: int | None, workers: int) -> dict:
    # Page 0 also carries the total count.
    req = urllib.request.Request(
        f"{API}?page=0&limit={PAGE_SIZE}", headers={"Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        first = json.load(resp)
    total = int(first.get("count") or 0)
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    if max_pages is not None:
        pages = min(pages, max_pages)
    print(f"{total} organisation rows → {pages} pages (workers={workers})")

    index: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    rows_seen = companies = kept = 0

    def _ingest(rows: list[dict]) -> None:
        nonlocal rows_seen, companies, kept
        for row in rows:
            rows_seen += 1
            if row.get("type") != "company":
                continue
            companies += 1
            ident = _clean_identification(row.get("identification"))
            iso2 = (row.get("summary_data.iso2") or "").strip().upper()
            if not ident or not iso2:
                continue
            entry = {
                "id": row.get("id"),
                "year": row.get("summary_data.year"),
                "label": (row.get("label") or "").strip(),
            }
            bucket = index[iso2][ident]
            if entry not in bucket:
                bucket.append(entry)
                kept += 1

    _ingest(first.get("data") or [])
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for rows in pool.map(_fetch_page, range(1, pages)):
            _ingest(rows)

    identifications = sum(len(v) for v in index.values())
    print(
        f"scanned {rows_seen} rows / {companies} company rows → "
        f"{identifications} (country, identification) keys, {kept} org entries "
        f"across {len(index)} countries"
    )
    return {
        "meta": {
            "source": "EITI API v2.0 (https://eiti.org/api) — EITI International Secretariat, eiti.org",
            "generated": dt.date.today().isoformat(),
            "total_rows": rows_seen,
            "countries": len(index),
            "identifications": identifications,
        },
        "index": {cc: dict(idents) for cc, idents in index.items()},
    }


def build_from_dir(pages_dir: Path) -> dict:
    """Build the index from pre-downloaded page JSON files (offline rebuild).

    Expects files matching ``page_*.json``, each the raw API response for one
    page. Useful for resumable crawls and for environments with short
    execution windows.
    """
    index: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    rows_seen = companies = kept = 0
    files = sorted(pages_dir.glob("page_*.json"))
    for path in files:
        try:
            rows = json.loads(path.read_text()).get("data") or []
        except Exception:
            continue
        for row in rows:
            rows_seen += 1
            if row.get("type") != "company":
                continue
            companies += 1
            ident = _clean_identification(row.get("identification"))
            iso2 = (row.get("summary_data.iso2") or "").strip().upper()
            if not ident or not iso2:
                continue
            entry = {
                "id": row.get("id"),
                "year": row.get("summary_data.year"),
                "label": (row.get("label") or "").strip(),
            }
            bucket = index[iso2][ident]
            if entry not in bucket:
                bucket.append(entry)
                kept += 1
    identifications = sum(len(v) for v in index.values())
    print(
        f"{len(files)} page files: {rows_seen} rows / {companies} company rows → "
        f"{identifications} (country, identification) keys across {len(index)} countries"
    )
    return {
        "meta": {
            "source": "EITI API v2.0 (https://eiti.org/api) — EITI International Secretariat, eiti.org",
            "generated": dt.date.today().isoformat(),
            "total_rows": rows_seen,
            "countries": len(index),
            "identifications": identifications,
        },
        "index": {cc: dict(idents) for cc, idents in index.items()},
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-pages", type=int, default=None, help="Cap pages (smoke runs)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument(
        "--from-dir", type=Path, default=None,
        help="Build from pre-downloaded page_*.json files instead of crawling",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "opencheck" / "data" / "eiti_organisations.json.gz",
    )
    args = ap.parse_args()

    data = build_from_dir(args.from_dir) if args.from_dir else build(args.max_pages, args.workers)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.out, "wt", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"wrote {args.out} ({args.out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
