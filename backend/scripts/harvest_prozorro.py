#!/usr/bin/env python3
"""Read Prozorro's contracts feed forward from where the last run stopped.

Run daily by ``.github/workflows/refresh-prozorro-index.yml`` before the
index build; see ``opencheck/prozorro_harvest.py`` for what is read and kept.
The store directory holds ``prozorro_harvest.sqlite`` (or its ``.gz``, which
is unpacked first and written again at the end). A run stops when it has
caught up or when ``--budget-minutes`` is spent, and saves what it read
either way, so the twelve-month backfill spreads over runs:

    python3 scripts/harvest_prozorro.py --store ~/prozorro-harvest \\
        --budget-minutes 240

Exits 0 when the store was saved (caught up or not), 1 when the listing
could not be read at all.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import shutil
import sys
import time
from pathlib import Path

#: The share of the time budget the tenders feed may use on one run.
METHODS_SHARE = 0.3
#: The share kept at the end for the procedures the forward read missed.
FILL_SHARE = 0.2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.prozorro_harvest import (  # noqa: E402
    STORE_NAME,
    WORKERS,
    HarvestError,
    Store,
    client,
    fill_missing_methods,
    harvest,
    harvest_methods,
    harvest_methods_back,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--store", required=True, type=Path, help="directory holding the harvest")
    parser.add_argument("--budget-minutes", type=float, default=240.0)
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--no-gzip", action="store_true", help="leave the store unpacked")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    args.store.mkdir(parents=True, exist_ok=True)
    path = args.store / STORE_NAME
    packed = args.store / f"{STORE_NAME}.gz"
    if not path.exists() and packed.exists():
        with gzip.open(packed, "rb") as src, open(path, "wb") as dst:
            shutil.copyfileobj(src, dst)
    store = Store(path)
    print(f"Loaded {store.count()} contracts; cursor {store.get('contracts_cursor') or '(first run)'}")
    deadline = time.monotonic() + args.budget_minutes * 60
    result: dict[str, object] = {}
    status = 0
    try:
        with client(args.workers) as http:
            # The procedures first: cheap (one page per thousand tenders), and
            # then the contracts read this run find their tender's method.
            # At most METHODS_SHARE of the budget, so a backfill reads
            # contracts on every run.
            started = time.monotonic()
            methods_deadline = min(deadline, started + (deadline - started) * METHODS_SHARE)
            result.update(harvest_methods(http, store, deadline=methods_deadline))
            result.update(harvest(http, store, deadline=deadline - (deadline - started) * FILL_SHARE,
                                  workers=args.workers))
            # Then the procedures the forward read cannot give: the tenders
            # feed backwards from the harvest's start, then any tender still
            # missing one at a time. After the contracts, which leave them
            # the last FILL_SHARE of the budget (more once caught up).
            now = time.monotonic()
            result.update(harvest_methods_back(http, store, deadline=now + (deadline - now) / 2))
            result.update(fill_missing_methods(http, store, deadline=deadline,
                                               workers=args.workers))
    except HarvestError as exc:
        print(f"Harvest failed: {exc}", file=sys.stderr)
        status = 1
    state = store.state()
    store.close()
    if not args.no_gzip:
        with open(path, "rb") as src, gzip.open(packed, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
    print(json.dumps({**result, "state": state}, indent=2))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
