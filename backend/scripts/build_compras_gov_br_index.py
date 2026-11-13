#!/usr/bin/env python3
"""Build the Compras.gov.br supplier index from the OCP Data Registry's file.

Run weekly by ``.github/workflows/refresh-compras-gov-br-index.yml``, which
uploads the gzipped result as the ``compras-gov-br-index`` release asset that
production downloads at boot. Also the way to build one locally:

    # download the registry's full.jsonl.gz (~960 MB) and build
    python3 scripts/build_compras_gov_br_index.py --out compras_gov_br.sqlite

    # from a copy already on disk
    python3 scripts/build_compras_gov_br_index.py --out compras_gov_br.sqlite \\
        --input ~/Downloads/full.jsonl.gz

    # streamed, with no copy on disk ("-" reads decompressed JSON lines)
    curl -sSL --http1.1 "<registry url>" | gunzip | \\
        python3 scripts/build_compras_gov_br_index.py --out compras_gov_br.sqlite --input -

Then point ``COMPRAS_GOV_BR_DB_FILE`` at the output (and set
``COMPRAS_GOV_BR_DB_URL=`` empty to keep boot from replacing it).
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

from opencheck.sources.compras_gov_br import (  # noqa: E402
    FULL_FILE_URL,
    build_index,
    iter_releases,
)

#: Attempts at the download before giving up. The registry's CDN closes a
#: ~1 GB transfer part-way (the first workflow run, 13 Nov 2026, was cut at
#: 40 MB), so each attempt resumes from the bytes already written.
DOWNLOAD_ATTEMPTS = 20


def _download(url: str, target: Path) -> dict[str, str]:
    """Download ``url`` to ``target``, resuming with ``Range`` after a drop.

    The file is pinned by its ETag (``If-Range``): if it changes mid-download
    the server answers 200 and the transfer starts again from byte zero. The
    finished file must be exactly the size the server announced.
    """
    import time

    import httpx

    target.write_bytes(b"")
    total: int | None = None
    etag = ""
    final_url = url
    last_modified = ""
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        have = target.stat().st_size
        headers = {}
        if have and etag:
            headers = {"Range": f"bytes={have}-", "If-Range": etag}
        try:
            with httpx.stream(
                "GET", url, headers=headers, timeout=httpx.Timeout(60.0, read=300.0),
                follow_redirects=True,
            ) as resp:
                resp.raise_for_status()
                final_url = str(resp.url)
                if resp.status_code == 206:
                    mode = "ab"
                else:
                    # A first request, or the file changed (If-Range failed).
                    mode = "wb"
                    total = int(resp.headers.get("content-length") or 0) or None
                    etag = resp.headers.get("etag") or ""
                    last_modified = resp.headers.get("last-modified") or ""
                with open(target, mode) as fh:
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            print(
                f"Download attempt {attempt} stopped at "
                f"{target.stat().st_size:,} of {total or '?'} bytes: {exc}",
                flush=True,
            )
            if not etag:
                # Nothing to resume against: start again.
                target.write_bytes(b"")
            time.sleep(min(60, 5 * attempt))
            continue
        size = target.stat().st_size
        if total is None or size == total:
            return {
                "source_url": final_url,
                "source_last_modified": last_modified,
                "source_bytes": str(size),
            }
        print(f"Download attempt {attempt} ended at {size:,} of {total:,} bytes", flush=True)
    raise SystemExit(f"download incomplete after {DOWNLOAD_ATTEMPTS} attempts: {url}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="SQLite file to write")
    parser.add_argument("--input", type=Path, help="a full.jsonl.gz already on disk")
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with tempfile.TemporaryDirectory() as scratch:
        if args.input:
            path = args.input
            if str(path) == "-":
                source = {"source_url": "stdin"}
            else:
                source = {"source_url": str(path), "source_bytes": str(path.stat().st_size)}
        else:
            path = Path(scratch) / "full.jsonl.gz"
            source = _download(FULL_FILE_URL, path)
            print(f"Downloaded {source}")
        meta = build_index(iter_releases(path), args.out, source_meta=source)

    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
    print(json.dumps(meta, indent=2))
    print(f"{args.out}: {args.out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
