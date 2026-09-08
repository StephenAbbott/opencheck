"""Build psc_graph.sqlite from Companies House's daily PSC snapshot (Phase 186).

The store behind ``opencheck/psc_graph.py`` — every *active* PSC record on
the UK register, keyed for a local corporate-PSC chain walk. The snapshot
(``persons-with-significant-control-snapshot-YYYY-MM-DD.zip``, ~2.2 GB) is
streamed straight through zlib into SQLite; the zip is never written to
disk. About seven minutes and 2.2 GB on a 2-vCPU runner; ``gzip -9`` of the
result is ~1.1 GB.

Usage (from backend/):
    uv run python scripts/build_psc_graph.py --out /tmp/psc_graph.sqlite
    uv run python scripts/build_psc_graph.py --out /tmp/psc_graph.sqlite --date 2026-09-08
    uv run python scripts/build_psc_graph.py --out /tmp/sample.sqlite --snapshot-file snapshot.zip

``--date`` names the snapshot; without it the newest one the register has
(up to a week back — none is published on weekends and bank holidays) is
used. ``--snapshot-file`` reads a zip already on disk instead of the
network. Prints the counts the workflow's sanity gate reads.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencheck import psc_graph  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", required=True, type=Path, help="where to write psc_graph.sqlite")
    parser.add_argument("--date", help="snapshot date YYYY-MM-DD (default: the newest published)")
    parser.add_argument("--snapshot-file", type=Path, help="a snapshot zip already on disk")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.snapshot_file:
        url = f"file://{args.snapshot_file.resolve()}"
        date = args.date
    elif args.date:
        url = psc_graph.snapshot_url(args.date)
        date = args.date
    else:
        url = psc_graph.latest_snapshot_url()
        date = psc_graph.snapshot_date_from_url(url)
    print(f"building {args.out} from {url}", file=sys.stderr)
    counts = psc_graph.build_psc_graph(args.out, source_url=url, snapshot_date=date)
    print(json.dumps({"out": str(args.out), "snapshot_date": date, **counts}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
