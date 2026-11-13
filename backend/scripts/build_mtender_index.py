#!/usr/bin/env python3
"""Build the MTender supplier index from the OCP Data Registry's file and
OpenCheck's own harvest of MTender's API.

Run weekly by ``.github/workflows/refresh-mtender-index.yml``, which first
runs ``scripts/harvest_mtender.py`` and then uploads the gzipped result as
the ``mtender-index`` release asset that production downloads at boot.
With ``--harvest DIR`` the harvest's reading of a procedure replaces the
registry's (``opencheck.mtender_harvest.merged_releases``). Also the way to
build one locally:

    # download the registry's full.jsonl.gz (~110 MB) and build
    python3 scripts/build_mtender_index.py --out mtender_moldova.sqlite

    # from a copy already on disk
    python3 scripts/build_mtender_index.py --out mtender_moldova.sqlite \\
        --input ~/Downloads/full.jsonl.gz

    # with a harvest kept by scripts/harvest_mtender.py
    python3 scripts/build_mtender_index.py --out mtender_moldova.sqlite \\
        --harvest ~/mtender-harvest

Then point ``MTENDER_DB_FILE`` at the output (and set
``MTENDER_DB_URL=`` empty to keep boot from replacing it).
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

from opencheck.mtender_harvest import Store, merged_releases  # noqa: E402
from opencheck.sources.mtender_moldova import (  # noqa: E402
    FULL_FILE_URL,
    build_index,
    iter_releases,
)


def _download(url: str, target: Path) -> dict[str, str]:
    import httpx

    with httpx.stream("GET", url, timeout=900.0, follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
        return {
            "source_url": str(resp.url),
            "source_last_modified": resp.headers.get("last-modified") or "",
            "source_bytes": str(target.stat().st_size),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument("--input", type=Path, help="a full.jsonl.gz already on disk")
    parser.add_argument(
        "--harvest", type=Path, help="a directory kept by scripts/harvest_mtender.py"
    )
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with tempfile.TemporaryDirectory() as scratch:
        if args.input:
            path = args.input
            source = {"source_url": str(path), "source_bytes": str(path.stat().st_size)}
        else:
            path = Path(scratch) / "full.jsonl.gz"
            source = _download(FULL_FILE_URL, path)
            print(f"Downloaded {source}")
        releases = iter_releases(path)
        if args.harvest:
            store = Store.load(args.harvest)
            releases = merged_releases(releases, store)
            source = {
                **source,
                "harvest_releases": str(len(store.releases)),
                "harvest_cursor": store.cursor or "",
                "harvest_started_from": store.started_from or "",
                "harvest_caught_up_at": store.caught_up_at or "",
            }
        meta = build_index(releases, args.out, source_meta=source)

    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
    print(json.dumps(meta, indent=2))
    print(f"{args.out}: {args.out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
