"""Phase 3 — Memory-ceiling pressure test.

Measures peak RSS (OS-level) and peak Python heap (tracemalloc) for
``walk_subgraph`` + full reconstruction across all nine demo anchors at
hop depths 2, 3, 5, and 8.

The exit criterion for Phase 3 is: *worst-case demo subgraph extracts
under the deployment memory budget (512 MB Render free tier), peak RSS
recorded*.

Usage (from ``backend/``):

    python scripts/profile_memory.py \\
        --gleif /path/to/gleif_version_0_4.db \\
        --uk    /path/to/uk_version_0_4.db

Add ``--hops 2 3 5`` to restrict the hop depths tested (default: 2 3 5 8).
Add ``--anchors bp dmgt`` to run a subset of anchors by short name.
Add ``--baseline`` to print process RSS before any walks (useful to
quantify Python interpreter + loaded module overhead).

Output: a tab-aligned table followed by a worst-case summary line.

Notes on measurement methodology
---------------------------------
* ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` captures the OS-level
  peak resident set size for the whole process.  On Linux this is in
  kilobytes; on macOS it is in bytes — the script normalises to MB.
  This is the number to compare against Render's 512 MB limit; it
  includes the sqlite3 C extension's own memory, which tracemalloc
  misses.
* ``tracemalloc`` captures Python-heap allocations and provides a
  useful cross-check; it's more portable but understates real RSS.
* Each (anchor, hops) measurement is taken in a fresh call so that
  Python's allocator can reclaim memory between runs — this mirrors
  how the extraction script is actually called.
* The walk itself is O(nodes + edges) in both CPU and memory: each
  entity/person/relationship is visited at most once and stored as a
  small dict. The dominant cost is the BFS frontier set and the
  reconstructed statement list.
"""

from __future__ import annotations

import argparse
import gc
import platform
import resource
import sqlite3
import sys
import tracemalloc
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Demo-set anchors (Phase 0 selection, LEI + optional GB-COH).
# ---------------------------------------------------------------------------

DEMO_ANCHORS = [
    {
        "name": "dmgt",
        "label": "Daily Mail & General Trust",
        "lei": "4OFD47D73QFJ1T1MOF29",
        "coh": "00184594",
    },
    {
        "name": "bp",
        "label": "BP P.L.C.",
        "lei": "213800LH1BZH3DI6G760",
        "coh": "00102498",
    },
    {
        "name": "rosneft",
        "label": "Rosneft",
        "lei": "253400JT3MQWNDKMJE44",
        "coh": None,
    },
    {
        "name": "bank_saderat",
        "label": "Bank Saderat PLC",
        "lei": "2138008KTNTDICZU8L25",
        "coh": "01126618",
    },
    {
        "name": "biffa",
        "label": "Biffa PLC",
        "lei": "2138008RB4WDK7HYYS91",
        "coh": "10336040",
    },
    {
        "name": "hornsea",
        "label": "Hornsea 1 Limited",
        "lei": "2138002S3XGZ38WN5Q72",
        "coh": "07640868",
    },
    {
        "name": "care_uk",
        "label": "Care UK Social Care",
        "lei": "213800DBE5Y9ZM58PN63",
        "coh": "07068789",
    },
    {
        "name": "taqa",
        "label": "Taqa Bratani Limited",
        "lei": "213800E11LI1SCETU492",
        "coh": "05975475",
    },
    {
        "name": "newcastle",
        "label": "Newcastle United FC",
        "lei": "213800AG2V6YE68H5N63",
        "coh": "00031014",
    },
]

# ---------------------------------------------------------------------------
# Memory helpers
# ---------------------------------------------------------------------------

_IS_MACOS = platform.system() == "Darwin"


def _peak_rss_mb() -> float:
    """Return the process peak RSS in megabytes.

    Linux: ru_maxrss is in kilobytes.
    macOS: ru_maxrss is in bytes.
    """
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if _IS_MACOS:
        return maxrss / (1024 * 1024)
    return maxrss / 1024


def _reset_tracemalloc() -> None:
    if tracemalloc.is_tracing():
        tracemalloc.stop()
    gc.collect()
    tracemalloc.start()


def _peak_py_mb() -> float:
    """Return the peak Python-heap allocation since the last reset, in MB."""
    _, peak = tracemalloc.get_traced_memory()
    return peak / (1024 * 1024)


# ---------------------------------------------------------------------------
# Import the extraction helpers from the sibling script.
# ---------------------------------------------------------------------------

def _import_extractor() -> Any:
    """Import extract_bods_subgraphs as a module (handles both installed and
    script-relative layouts)."""
    import importlib.util
    script_path = Path(__file__).parent / "extract_bods_subgraphs.py"
    spec = importlib.util.spec_from_file_location("extract_bods_subgraphs", script_path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# ---------------------------------------------------------------------------
# Single-measurement function
# ---------------------------------------------------------------------------

Row = dict[str, Any]


def measure_one(
    ext: Any,
    conn: sqlite3.Connection,
    root_recordid: str,
    hops: int,
) -> dict[str, float]:
    """Run walk_subgraph + extract_for_root once; return memory metrics."""
    _reset_tracemalloc()
    rss_before = _peak_rss_mb()

    statements = ext.extract_for_root(conn, root_recordid, max_hops=hops)

    rss_after = _peak_rss_mb()
    peak_py = _peak_py_mb()
    tracemalloc.stop()
    gc.collect()

    return {
        "n_stmts": len(statements),
        "rss_delta_mb": max(0.0, rss_after - rss_before),
        "peak_rss_mb": rss_after,
        "peak_py_mb": peak_py,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Phase 3 — memory-ceiling pressure test for walk_subgraph."
    )
    parser.add_argument("--gleif", required=True, type=Path, help="gleif_version_0_4.db")
    parser.add_argument("--uk", type=Path, help="uk_version_0_4.db (optional)")
    parser.add_argument(
        "--hops",
        nargs="+",
        type=int,
        default=[2, 3, 5, 8],
        metavar="N",
        help="Hop depths to test (default: 2 3 5 8)",
    )
    parser.add_argument(
        "--anchors",
        nargs="+",
        metavar="NAME",
        help="Restrict to anchor short names, e.g. --anchors bp dmgt taqa",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Print process RSS before any walks (interpreter + module overhead)",
    )
    args = parser.parse_args(argv)

    if not args.gleif.is_file():
        print(f"GLEIF db not found: {args.gleif}", file=sys.stderr)
        return 1
    if args.uk and not args.uk.is_file():
        print(f"UK PSC db not found: {args.uk}", file=sys.stderr)
        return 1

    ext = _import_extractor()

    if args.baseline:
        print(f"Baseline RSS (Python + modules loaded): {_peak_rss_mb():.1f} MB\n")

    # Open connections.
    print(f"Opening GLEIF db: {args.gleif}")
    gleif_conn = sqlite3.connect(str(args.gleif))
    gleif_conn.row_factory = sqlite3.Row
    print("  Ensuring indexes (one-off — may take minutes on first run)…")
    ext.ensure_indexes(gleif_conn, has_persons=False)

    uk_conn: sqlite3.Connection | None = None
    if args.uk:
        print(f"Opening UK PSC db: {args.uk}")
        uk_conn = sqlite3.connect(str(args.uk))
        uk_conn.row_factory = sqlite3.Row
        print("  Ensuring indexes (one-off — may take minutes on first run)…")
        ext.ensure_indexes(uk_conn, has_persons=True)

    # Select anchors.
    anchors = DEMO_ANCHORS
    if args.anchors:
        anchor_set = set(args.anchors)
        anchors = [a for a in DEMO_ANCHORS if a["name"] in anchor_set]
        if not anchors:
            print(f"No matching anchors for: {args.anchors}", file=sys.stderr)
            return 1

    hops_list = sorted(set(args.hops))

    # Header row.
    col_w = 14
    hop_headers = "  ".join(f"hops={h:d}".ljust(col_w) for h in hops_list)
    print(f"\n{'Anchor':<28}  {'Source':<6}  {hop_headers}")
    print("-" * (28 + 2 + 6 + 2 + (col_w + 2) * len(hops_list)))

    results: list[dict[str, Any]] = []
    worst_rss = 0.0

    for anchor in anchors:
        label = anchor["label"][:27]

        for source, conn_ref, identifier, scheme in [
            ("GLEIF", gleif_conn, anchor["lei"], ext.LEI_SCHEME),
            ("UK",    uk_conn,    anchor.get("coh"), ext.GB_COH_SCHEME),
        ]:
            if conn_ref is None:
                continue
            if not identifier:
                continue

            root_recordid = ext.find_entity_recordid_by_identifier(
                conn_ref, identifier, scheme
            )
            if root_recordid is None:
                cells = "  ".join("not found".ljust(col_w) for _ in hops_list)
                print(f"{label:<28}  {source:<6}  {cells}")
                continue

            cells = []
            for hops in hops_list:
                metrics = measure_one(ext, conn_ref, root_recordid, hops)
                worst_rss = max(worst_rss, metrics["peak_rss_mb"])
                cell = (
                    f"{metrics['n_stmts']}s "
                    f"{metrics['peak_rss_mb']:.0f}MB "
                    f"({metrics['peak_py_mb']:.0f}py)"
                )
                cells.append(cell.ljust(col_w))
                results.append({
                    "anchor": anchor["name"],
                    "source": source,
                    "hops": hops,
                    **metrics,
                })
            print(f"{label:<28}  {source:<6}  {'  '.join(cells)}")

    print()
    print(f"Worst-case peak RSS across all runs: {worst_rss:.1f} MB")
    print(f"Render free-tier budget:             512.0 MB")
    budget_ok = worst_rss < 450  # leave 62 MB headroom for the app itself
    print(f"Budget OK (< 450 MB):                {'YES ✓' if budget_ok else 'NO — investigate'}")

    if not budget_ok:
        print(
            "\nWARNING: worst-case RSS exceeds 450 MB safety margin.\n"
            "Consider reducing --max-hops or adding streaming pagination\n"
            "to extract_bods_subgraphs.py.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
