#!/usr/bin/env python3
"""Build the ChileCompra supplier index from the monthly open-data files.

Run monthly by ``.github/workflows/refresh-chilecompra-index.yml``, which
uploads the gzipped result as the ``chilecompra-index`` release asset that
production downloads at boot. Also the way to build one locally:

    # the newest twelve complete months, straight from ChileCompra
    python3 scripts/build_chilecompra_index.py --out chilecompra.sqlite

    # keep the zips between runs (about 1.4 GB for twelve months)
    python3 scripts/build_chilecompra_index.py --out chilecompra.sqlite \\
        --cache-dir ~/chilecompra-files

Then point ``CHILECOMPRA_DB_FILE`` at the output (and set
``CHILECOMPRA_DB_URL=`` empty to keep boot from replacing it).
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.chilecompra import (  # noqa: E402
    MonthInput,
    build_index,
    latest_complete_month,
    month_urls,
    window_months,
)


def _download(url: str, target: Path) -> dict[str, object]:
    """Fetch ``url`` to ``target`` unless an identical copy is already there."""
    import httpx

    head = httpx.head(url, timeout=60.0, follow_redirects=True)
    head.raise_for_status()
    size = int(head.headers.get("content-length") or 0)
    last_modified = head.headers.get("last-modified") or ""
    if not (target.exists() and size and target.stat().st_size == size):
        tmp = target.with_suffix(".part")
        with httpx.stream("GET", url, timeout=600.0, follow_redirects=True) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_bytes():
                    fh.write(chunk)
        tmp.replace(target)
    return {"last_modified": last_modified, "bytes": target.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument("--months", type=int, default=12, help="window length (default 12)")
    parser.add_argument("--to", help="last month of the window, YYYY-MM (default: newest complete)")
    parser.add_argument("--cache-dir", type=Path, help="keep downloaded zips here")
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    end = args.to or latest_complete_month()
    months = window_months(end, args.months)
    print(f"Window: {months[0]} – {months[-1]} ({len(months)} months)")

    with tempfile.TemporaryDirectory() as scratch:
        store = args.cache_dir or Path(scratch)
        store.mkdir(parents=True, exist_ok=True)
        inputs: list[MonthInput] = []
        for month in months:
            tenders_url, orders_url = month_urls(month)
            files: dict[str, object] = {}
            paths: list[Path] = []
            for url, kind in ((tenders_url, "lic"), (orders_url, "oc")):
                path = store / f"{kind}-{month}.zip"
                files[url] = _download(url, path)
                paths.append(path)
                print(f"  {month} {kind}: {files[url]}")
            inputs.append(MonthInput(month, paths[0], paths[1], files))
        meta = build_index(inputs, args.out)

    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
    shown = {k: v for k, v in meta.items() if k != "files"}
    print(json.dumps(shown, indent=2))
    print(f"{args.out}: {args.out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
