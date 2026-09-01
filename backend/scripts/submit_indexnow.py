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
with a notice, so the refresh workflow degrades gracefully. With one, it
fetches that key file itself before submitting anything — the engines'
verification failure is an opaque 403, so we'd rather find out here, by
name, than ship 300k URLs into a rejection.

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


def key_location_for(key: str, host: str = HOST) -> str:
    return f"https://{host}/indexnow/{key}.txt"


def redacted_key_location(key: str, host: str = HOST) -> str:
    """``key_location_for`` with the key stubbed out — safe to print.

    Actions masks the secret anyway, but this script also runs by hand, and a
    key that leaks into a terminal or a pasted log has to be rotated on both
    Render and GitHub.
    """
    return f"https://{host}/indexnow/{key[:4]}….txt"


def payload_for(batch: list[str], key: str, host: str = HOST) -> dict:
    return {
        "host": host,
        "key": key,
        "keyLocation": key_location_for(key, host),
        "urlList": batch,
    }


def check_key_location(key: str, host: str = HOST) -> str | None:
    """Fetch the key file the engines will fetch. ``None`` when it checks out,
    otherwise a human-readable reason it won't.

    IndexNow answers a submission it can't verify with a bare ``403`` and no
    body worth reading, which looks identical whether the backend is down, the
    route is missing, or the two halves of the shared key have drifted apart.
    Doing the engine's fetch ourselves first turns that into a message naming
    the actual problem — and avoids shipping ~300k URLs that will be rejected.
    """
    import httpx

    url = key_location_for(key, host)
    shown = redacted_key_location(key, host)
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
    except httpx.HTTPError as exc:
        return f"could not fetch {shown}: {exc}"
    if resp.status_code != 200:
        return (
            f"{shown} returned HTTP {resp.status_code}, expected 200. The backend "
            "serves this from OPENCHECK_INDEXNOW_KEY; it must be set to the same "
            "value as this job's INDEXNOW_KEY secret, and the deploy carrying "
            "the /indexnow route must be live."
        )
    if resp.text.strip() != key:
        return (
            f"{shown} is reachable but served a different value than the key we "
            "would submit — OPENCHECK_INDEXNOW_KEY and INDEXNOW_KEY have drifted "
            "apart."
        )
    return None


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
                    f"IndexNow batch {i} rejected: HTTP {resp.status_code} "
                    f"{resp.text[:200]}\n"
                    f"  keyLocation was {redacted_key_location(key)} — a 403 "
                    "here usually means the engines could not verify it."
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

    if key and not args.dry_run:
        problem = check_key_location(key)
        if problem:
            print(f"::warning::IndexNow key check failed: {problem}")
            print("Skipping IndexNow submission — no URLs were sent.")
            return
        print(f"key file verified at {redacted_key_location(key)}")

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
