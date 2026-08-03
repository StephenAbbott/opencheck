"""Submit changed/new entity-page URLs to IndexNow (Phase 91 — SEO Phase D).

IndexNow (https://www.indexnow.org/) lets a site push changed URLs to
participating search engines (Bing, Seznam, Naver, Yandex, …) instead of
waiting for a recrawl. Google does not consume IndexNow — Google discovery
stays sitemap/lastmod-driven via Search Console.

What gets submitted: the GLEIF Golden Copy **LastMonth delta** (every LEI
issued or revised in the preceding 31 days) mapped to canonical
``https://opencheck.world/entity/{LEI}-{slug}`` URLs — the same slugs the
DB builder writes, because both import :func:`slugify_name`. Typically
~100–300k URLs/month, sent in the protocol's 10,000-URL batches.

Auth: the shared key must be provable on the host — the backend serves it
at ``/indexnow/{key}.txt`` (see routers/entity_pages.py) from the
``OPENCHECK_INDEXNOW_KEY`` env var; this script reads ``INDEXNOW_KEY``
(same value; the GitHub Actions secret). Without a key the script exits 0
with a notice, so the refresh workflow degrades gracefully.

Usage:
    INDEXNOW_KEY=... python3 scripts/submit_indexnow.py --delta LastMonth
    python3 scripts/submit_indexnow.py --lei2-file delta.csv.zip --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from collections.abc import Iterable, Iterator
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_entity_pages_db import (  # noqa: E402
    COL_LEI,
    COL_NAME,
    COL_TRANSLIT,
    _download,
    _latest_publish,
    _open_csv,
)

from opencheck.entity_pages import slugify_name  # noqa: E402

INDEXNOW_API = "https://api.indexnow.org/indexnow"
BATCH = 10_000  # the protocol's per-request URL limit
HOST = "opencheck.world"


def urls_from_delta(rows: Iterable[dict[str, str]], frontend: str) -> Iterator[str]:
    """Canonical entity URLs for the delta's rows — same slugs as the DB.

    Deduplicates (a LEI can appear more than once in a delta) and skips rows
    without an LEI. Retired/lapsed records still map to valid pages.
    """
    seen: set[str] = set()
    for row in rows:
        lei = (row.get(COL_LEI) or "").strip().upper()
        if not lei or lei in seen:
            continue
        seen.add(lei)
        name = (row.get(COL_NAME) or "").strip() or lei
        slug = slugify_name(name, (row.get(COL_TRANSLIT) or "").strip() or None)
        token = f"{lei}-{slug}" if slug else lei
        yield f"{frontend}/entity/{token}"


def batched(urls: Iterable[str], size: int = BATCH) -> Iterator[list[str]]:
    batch: list[str] = []
    for url in urls:
        batch.append(url)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def payload_for(batch: list[str], key: str, host: str = HOST) -> dict:
    return {
        "host": host,
        "key": key,
        "keyLocation": f"https://{host}/indexnow/{key}.txt",
        "urlList": batch,
    }


def submit(batches: Iterable[list[str]], key: str, *, dry_run: bool = False) -> int:
    import httpx

    sent = 0
    for i, batch in enumerate(batches, start=1):
        body = payload_for(batch, key)
        if dry_run:
            print(f"[dry-run] batch {i}: {len(batch)} URLs "
                  f"(first: {batch[0]}, last: {batch[-1]})")
        else:
            resp = httpx.post(INDEXNOW_API, json=body, timeout=60.0)
            # 200/202 = accepted. 4xx on a batch is a config problem worth
            # failing loudly on (bad key, bad key location).
            if resp.status_code not in (200, 202):
                raise SystemExit(
                    f"IndexNow batch {i} rejected: HTTP {resp.status_code} {resp.text[:200]}"
                )
            print(f"batch {i}: {len(batch)} URLs -> HTTP {resp.status_code}")
            time.sleep(1)  # be polite between large batches
        sent += len(batch)
    return sent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument(
        "--delta",
        default="LastMonth",
        choices=["LastMonth", "LastWeek", "LastDay", "IntraDay"],
        help="Which GLEIF delta window to submit (default LastMonth).",
    )
    ap.add_argument("--lei2-file", type=Path, help="Local delta CSV (.csv/.zip); skips download.")
    ap.add_argument("--frontend", default=f"https://{HOST}")
    ap.add_argument("--dry-run", action="store_true", help="Print batches, send nothing.")
    args = ap.parse_args()

    key = os.environ.get("INDEXNOW_KEY", "").strip()
    if not key and not args.dry_run:
        print("INDEXNOW_KEY not set — skipping IndexNow submission (not an error).")
        return

    with tempfile.TemporaryDirectory(prefix="indexnow-") as tmp:
        if args.lei2_file is not None:
            path = args.lei2_file
        else:
            publish = _latest_publish()
            entry = publish["lei2"]["delta_files"][args.delta]["csv"]
            print(f"delta {args.delta}: {entry['record_count']:,} records")
            path = _download(entry["url"], Path(tmp), f"lei2 {args.delta}")

        urls = urls_from_delta(_open_csv(path), args.frontend.rstrip("/"))
        sent = submit(batched(urls), key or "dry-run-key", dry_run=args.dry_run)

    print(json.dumps({"submitted": sent, "delta": args.delta, "dry_run": args.dry_run}))


if __name__ == "__main__":
    main()
