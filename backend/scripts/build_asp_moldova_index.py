#!/usr/bin/env python3
"""Build the Moldova State Register (ASP) index by hand.

A live deployment does not need this: ``asp_moldova`` downloads the newest
weekly export and indexes it on its own (see the adapter's docstring). Use it
to build an index for local development, to rebuild from a file you already
have, or to see the build counts.

    # newest export, straight from dataset.gov.md
    python3 scripts/build_asp_moldova_index.py --out asp_moldova.sqlite

    # a file already downloaded
    python3 scripts/build_asp_moldova_index.py --xlsx company_2026.09.14.xlsx \\
        --snapshot-date 2026-09-14 --out asp_moldova.sqlite

Then point ``ASP_MOLDOVA_DB_FILE`` at the output (and set
``ASP_MOLDOVA_SYNC=false`` to keep the adapter from replacing it).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.asp_moldova import (  # noqa: E402
    _download,
    build_index,
    latest_resource,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument("--xlsx", type=Path, help="an export already on disk")
    parser.add_argument(
        "--snapshot-date",
        help="ISO date of a local --xlsx export (read from its title row when omitted)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.xlsx:
        meta = build_index(args.xlsx, args.out, snapshot_date=args.snapshot_date)
    else:
        resource = latest_resource()
        print(f"Newest export: {resource['name']} ({resource['snapshot_date']})")
        with tempfile.TemporaryDirectory() as tmp:
            xlsx = Path(tmp) / "company.xlsx"
            size = _download(resource["url"], xlsx)
            print(f"Downloaded {size:,} bytes")
            meta = build_index(
                xlsx,
                args.out,
                snapshot_date=resource["snapshot_date"],
                source_url=resource["url"],
            )
    print(json.dumps(meta, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
