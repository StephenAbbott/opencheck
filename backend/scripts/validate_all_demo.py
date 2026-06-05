"""Batch-validate every extracted demo subgraph and print a coverage table
(Phase 2 of the graph-native de-risking checklist).

For each ``gleif/<LEI>.jsonl`` it locates the matching ``uk/<companyNumber>.jsonl``
(via the subgraph's GB-COH identifier), merges them, runs lib-cove-bods, and
prints one row per anchor: schema verdict, statement count, person count,
jurisdictions and interest types — i.e. the coverage matrix, filled in.

Usage::

    python scripts/validate_all_demo.py [cache_root]

``cache_root`` defaults to the repo's ``data/cache/bods_data`` (where
extract_bods_subgraphs.py writes).
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

# Reuse the single-subgraph validator's helpers.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate_demo_subgraph import load_statements, validate  # noqa: E402


def _gb_coh(statements: list) -> str | None:
    for s in statements:
        if s.get("recordType") == "entity":
            for i in (s.get("recordDetails") or {}).get("identifiers") or []:
                if i.get("scheme") == "GB-COH" and i.get("id"):
                    return i["id"]
    return None


def _profile(statements: list):
    juris: set = set()
    itypes: set = set()
    persons = 0
    n_share = n_altnames = n_dissolved = 0
    for s in statements:
        rd = s.get("recordDetails") or {}
        rt = s.get("recordType")
        if rt == "entity":
            j = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction") or {}
            code = j.get("code") if isinstance(j, dict) else j
            if code:
                juris.add(code)
            if rd.get("alternateNames"):
                n_altnames += 1
            if rd.get("dissolutionDate"):
                n_dissolved += 1
        elif rt == "person":
            persons += 1
        elif rt == "relationship":
            for it in rd.get("interests") or []:
                if it.get("type"):
                    itypes.add(it["type"])
                if it.get("share"):
                    n_share += 1
    extras = []
    if n_share:
        extras.append(f"share×{n_share}")
    if n_altnames:
        extras.append(f"altNames×{n_altnames}")
    if n_dissolved:
        extras.append(f"dissolved×{n_dissolved}")
    return juris, itypes, persons, " ".join(extras) or "-"


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    root = (
        Path(argv[0]).expanduser()
        if argv
        else Path(__file__).resolve().parents[2] / "data" / "cache" / "bods_data"
    )
    gleif_dir, uk_dir = root / "gleif", root / "uk"
    files = sorted(gleif_dir.glob("*.jsonl"))
    if not files:
        print(f"No gleif/*.jsonl under {root}", file=sys.stderr)
        return 1

    print(f"Validating {len(files)} anchor(s) under {root}\n")
    header = f"{'LEI':<22} {'schema':<12} {'stmts':>6} {'ppl':>4} {'extras':<22} jurisdictions | interest types"
    print(header)
    print("-" * len(header))

    n_pass = 0
    for gf in files:
        lei = gf.stem
        stmts = load_statements([str(gf)])
        coh = _gb_coh(stmts)
        uk_note = ""
        if coh:
            ukf = uk_dir / f"{coh}.jsonl"
            if ukf.is_file():
                stmts += load_statements([str(ukf)])
                uk_note = f" (+uk/{coh})"
        try:
            js, additional = validate(stmts)
        except ImportError:
            print("! lib-cove-bods not installed - run: pip install libcovebods", file=sys.stderr)
            return 1
        ok = not js and not additional
        n_pass += int(ok)
        verdict = "PASS" if ok else f"FAIL {len(js)}/{len(additional)}"
        juris, itypes, persons, extras = _profile(stmts)
        print(
            f"{lei:<22} {verdict:<12} {len(stmts):>6} {persons:>4} {extras:<22} "
            f"{','.join(sorted(juris))} | {','.join(sorted(itypes))}{uk_note}"
        )

    print(f"\n{n_pass}/{len(files)} validate clean (0 schema errors + 0 additional checks).")
    return 0 if n_pass == len(files) else 1


if __name__ == "__main__":
    raise SystemExit(main())
