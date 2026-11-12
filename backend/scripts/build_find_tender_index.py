#!/usr/bin/env python3
"""Build the Find a Tender supplier index from the OCP Data Registry.

Run weekly by ``.github/workflows/refresh-find-tender-index.yml``, which
uploads the gzipped result as the ``find-tender-index`` release asset that
production downloads at boot. Also the way to build one locally:

    # download the registry's compiled releases (~227 MB) and build
    python3 scripts/build_find_tender_index.py --out find_tender_uk.sqlite

    # or build from a copy already on disk
    python3 scripts/build_find_tender_index.py --out find_tender_uk.sqlite \\
        --input ~/full.jsonl.gz --as-of 2026-11-07

Then point ``FIND_TENDER_DB_FILE`` at the output (and set
``FIND_TENDER_DB_URL=`` empty to keep boot from replacing it).
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import shutil
import sys
import tempfile
from email.utils import parsedate_to_datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.find_tender_uk import REGISTRY_URL, build_index  # noqa: E402

USER_AGENT = "OpenCheck index builder (+https://opencheck.world)"


def _download(url: str, target: Path) -> str:
    """Fetch ``url`` to ``target``; return the registry's Last-Modified as an
    ISO moment (the day OCP collected the feed — the source's cut)."""
    import httpx

    headers = {"User-Agent": USER_AGENT}
    with httpx.stream("GET", url, timeout=900.0, follow_redirects=True, headers=headers) as resp:
        resp.raise_for_status()
        last_modified = resp.headers.get("last-modified") or ""
        tmp = target.with_suffix(".part")
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
        tmp.replace(target)
    if not last_modified:
        return ""
    try:
        return parsedate_to_datetime(last_modified).isoformat()
    except (TypeError, ValueError):
        return ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument("--input", type=Path, help="a local full.jsonl.gz (skips the download)")
    parser.add_argument("--as-of", default="", help="the registry's collection date for --input")
    parser.add_argument("--url", default=REGISTRY_URL, help="registry download URL")
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with tempfile.TemporaryDirectory() as scratch:
        if args.input:
            source, as_of = args.input, args.as_of
        else:
            source = Path(scratch) / "full.jsonl.gz"
            as_of = _download(args.url, source)
            print(f"Downloaded {source.stat().st_size:,} bytes; registry Last-Modified {as_of}")
        meta = build_index(source, args.out, source_as_of=as_of or None, source_url=args.url)

    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
    print(json.dumps(meta, indent=2))
    print(f"{args.out}: {args.out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
