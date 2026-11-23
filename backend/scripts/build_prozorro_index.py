#!/usr/bin/env python3
"""Build the Prozorro supplier index from OpenCheck's harvest of Prozorro.

Run daily by ``.github/workflows/refresh-prozorro-index.yml`` after
``scripts/harvest_prozorro.py``; the gzipped result is the ``prozorro-index``
release asset production downloads at boot. Also the way to build one
locally:

    python3 scripts/harvest_prozorro.py --store ~/prozorro-harvest --budget-minutes 30
    python3 scripts/build_prozorro_index.py --harvest ~/prozorro-harvest \\
        --out prozorro_ukraine.sqlite

Then point ``PROZORRO_DB_FILE`` at the output (and set ``PROZORRO_DB_URL=``
empty to keep boot from replacing it).
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

from opencheck.prozorro_harvest import STORE_NAME  # noqa: E402
from opencheck.sources.prozorro_ukraine import build_index  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument(
        "--harvest", required=True, type=Path, help="a directory kept by scripts/harvest_prozorro.py"
    )
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with tempfile.TemporaryDirectory() as scratch:
        store = args.harvest / STORE_NAME
        if not store.exists():
            packed = args.harvest / f"{STORE_NAME}.gz"
            store = Path(scratch) / STORE_NAME
            with gzip.open(packed, "rb") as src, open(store, "wb") as dst:
                shutil.copyfileobj(src, dst)
        meta = build_index(store, args.out)

    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
    print(json.dumps(meta, indent=2))
    print(f"{args.out}: {args.out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
