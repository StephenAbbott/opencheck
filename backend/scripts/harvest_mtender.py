#!/usr/bin/env python3
"""Read MTender's OCDS API forward from where the last run stopped.

Run weekly by ``.github/workflows/refresh-mtender-index.yml`` before the
index build; see ``opencheck/mtender_harvest.py`` for what is read and kept.
A run stops when it has caught up or when ``--budget-minutes`` is spent, and
saves what it read either way, so a year's backfill spreads over runs:

    python3 scripts/harvest_mtender.py --store ~/mtender-harvest \\
        --budget-minutes 240

Exits 0 when the store was saved (caught up or not), 1 when the listing
could not be read at all.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.mtender_harvest import WORKERS, HarvestError, Store, client, harvest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--store", required=True, type=Path, help="directory holding the harvest")
    parser.add_argument("--budget-minutes", type=float, default=240.0)
    parser.add_argument("--workers", type=int, default=WORKERS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    store = Store.load(args.store)
    print(f"Loaded {len(store.releases)} procedures; cursor {store.cursor or '(first run)'}")
    deadline = time.monotonic() + args.budget_minutes * 60
    try:
        with client() as http:
            result = harvest(http, store, deadline=deadline, workers=args.workers)
    except HarvestError as exc:
        print(f"Harvest failed: {exc}", file=sys.stderr)
        return 1
    state = store.save(args.store)
    print(json.dumps({**result, **state}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
