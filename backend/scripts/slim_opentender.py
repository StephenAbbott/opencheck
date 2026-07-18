#!/usr/bin/env python3
"""Slim an existing opentender.db in place by projecting every tender blob down
to the fields OpenCheck actually consumes.

The finalised ``opentender.db`` is ~5 GB, of which ~87 % is the ``tenders.data``
column — raw DIGIWHIST tender JSON averaging ~8.5 KB/record. Only a handful of
those fields are ever read downstream (see
``opencheck.opentender_projection`` for the derivation). This script streams
every row, replaces ``data`` with its projection, then reclaims the freed space
via the build script's ``finalise_db`` (WAL checkpoint + integrity_check +
``VACUUM INTO``), printing the new SHA-256 and before/after sizes.

Idempotent: projecting an already-projected record is a no-op, so re-running the
script leaves the tender payloads unchanged.

Usage
-----
  python scripts/slim_opentender.py /path/to/opentender.db
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sqlite3
import sys
from pathlib import Path

from opencheck.opentender_projection import project_tender

logger = logging.getLogger(__name__)

# ``extract_opentender.py`` is a script, not an importable package member — load
# it by path so we can reuse ``finalise_db`` (WAL checkpoint + integrity_check +
# VACUUM) rather than reimplementing it.
_SCRIPT_DIR = Path(__file__).resolve().parent


def _load_extract_module():
    path = _SCRIPT_DIR / "extract_opentender.py"
    spec = importlib.util.spec_from_file_location("extract_opentender", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def slim_db(db_path: Path | str, *, batch_size: int = 1000) -> str:
    """Project every tender blob in *db_path* in place, finalise, return SHA-256.

    Streams ``SELECT persistent_id, data FROM tenders``, rewrites each ``data``
    blob with its projection, commits in batches, then reuses
    ``extract_opentender.finalise_db`` to checkpoint, integrity-check and VACUUM
    the file so the space freed by the smaller blobs is actually reclaimed.
    """
    db_path = Path(str(db_path))
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    extract = _load_extract_module()

    before_size = db_path.stat().st_size

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        # A separate cursor for the streaming read so writes don't disturb it.
        read_cur = conn.cursor()
        write_cur = conn.cursor()
        read_cur.execute("SELECT persistent_id, data FROM tenders")

        rewritten = 0
        skipped = 0
        pending = 0
        for row in read_cur:
            persistent_id = row["persistent_id"]
            try:
                tender = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                skipped += 1
                continue

            projected = project_tender(tender)
            new_blob = json.dumps(projected, ensure_ascii=False)
            write_cur.execute(
                "UPDATE tenders SET data = ? WHERE persistent_id = ?",
                (new_blob, persistent_id),
            )
            rewritten += 1
            pending += 1
            if pending >= batch_size:
                conn.commit()
                pending = 0
                logger.info("  … %d tenders projected", rewritten)

        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Projected %d tenders (%d skipped as unparseable). Finalising …",
        rewritten,
        skipped,
    )
    digest = extract.finalise_db(db_path)
    after_size = db_path.stat().st_size

    logger.info("Slim complete: %s", db_path)
    logger.info(
        "  size: %d → %d bytes (%.1f MB → %.1f MB, %.1f%% of original)",
        before_size,
        after_size,
        before_size / 1_000_000,
        after_size / 1_000_000,
        (after_size / before_size * 100) if before_size else 0.0,
    )
    logger.info("  SHA-256: %s", digest)
    logger.info("→ set OPENTENDER_DB_SHA256=%s on the host to pin the download.", digest)
    return digest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Slim an existing opentender.db by projecting tender blobs "
        "to the consumed fields, then VACUUM to reclaim the space.",
    )
    parser.add_argument(
        "db",
        metavar="DB",
        help="Path to an existing opentender.db to slim in place.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Commit after every N projected tenders (default: 1000).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    slim_db(db_path, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
