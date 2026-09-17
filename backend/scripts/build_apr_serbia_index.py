#!/usr/bin/env python3
"""Build the Serbia APR company-register index by hand.

A live deployment does not need this: ``apr_serbia`` downloads the register
and indexes it on its own (see the adapter's docstring). Use it to build an
index for local development, to rebuild from a file you already have, or to
see the build counts.

    # the register as APR serves it now
    python3 scripts/build_apr_serbia_index.py --out apr_serbia.sqlite

    # a file already downloaded
    python3 scripts/build_apr_serbia_index.py --json companies.json --out apr_serbia.sqlite

Then point ``APR_SERBIA_DB_FILE`` at the output (and set
``APR_SERBIA_SYNC=false`` to keep the adapter from replacing it).

Downloading with ``curl`` needs APR's missing intermediate certificate, which
this script's own download already carries — see ``APR_INTERMEDIATE_PEM``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.apr_serbia import COMPANIES_URL, _download, build_index  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument("--json", type=Path, help="a register file already on disk")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.json:
        meta = build_index(args.json, args.out)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "companies.json"
            size = _download(COMPANIES_URL, target)
            print(f"Downloaded {size:,} bytes")
            meta = build_index(target, args.out)
    print(json.dumps(meta, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
