"""Build the compact GEOT project-portfolio artifact from the GEOT xlsx.

The Global Energy Ownership Tracker (GEOT) xlsx is published by Global Energy
Monitor under CC BY 4.0 but sits behind a download form (reCAPTCHA), so it
cannot be fetched at runtime. This script is run manually against each new
GEOT release (roughly twice a year) and the generated artifact is committed:

    python scripts/build_geot_projects.py \
        ~/Downloads/Global-Energy-Ownership-Tracker-May-2026-V1.xlsx \
        --release "May 2026"

Output: ``opencheck/data/geot_projects.json.gz`` — a per-entity summary of
the precomputed ownership closure in the 9 per-tracker sheets:

* Each sheet row is one (ultimate parent, asset unit, ownership path) with an
  *effective* share (product along the path). Shares for one unit sum well
  over 100% by design (every level of each chain is enumerated, plus minority
  shareholders), so rows must never be summed naively.
* "Projects" are distinct project-level asset IDs (plant location / mine /
  pipeline / steel plant / cement plant), not units.
* A project counts as *controlled* when the parent's effective share —
  deduped paths summed per unit, capped at 100, max across the project's
  units — is ≥ 50%.
* Each (parent, project) pair is assigned its best status by priority
  operating > development > mothballed > retired > cancelled > other, so the
  status counts sum to the total distinct projects.

Requires openpyxl (tooling-only dependency, not needed at runtime):
    pip install openpyxl
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
from collections import defaultdict
from pathlib import Path

# Sheet name → (parent ID column, project-level asset ID column, unit-level
# asset ID column, status column, short tracker key).
# Column naming is inconsistent across sheets — verified against May 2026.
SHEET_SPEC: dict[str, tuple[str, str, str, str, str]] = {
    "Coal Plant Ownership": (
        "Owner GEM Entity ID", "GEM location ID", "GEM unit ID", "Status", "coal_plant",
    ),
    "Gas Plant Ownership": (
        "Parent GEM Entity ID", "GEM location ID", "GEM unit ID", "Status", "gas_plant",
    ),
    "Bioenergy Power Ownership": (
        "Parent GEM Entity ID", "GEM location ID", "GEM unit ID", "Status", "bioenergy",
    ),
    "Coal Mine Ownership": (
        "Parent GEM Entity ID", "GEM Mine ID", "GEM Mine ID", "Status", "coal_mine",
    ),
    "Iron Mine Ownership": (
        "Parent GEM Entity ID", "GEM Asset ID", "GEM Asset ID", "Operating status", "iron_mine",
    ),
    "Gas Pipeline Ownership": (
        "Parent GEM Entity ID", "ProjectID", "ProjectID", "Status", "gas_pipeline",
    ),
    "Oil & NGL Pipeline Ownership": (
        "Parent GEM Entity ID", "ProjectID", "ProjectID", "Status", "oil_ngl_pipeline",
    ),
    "Steel Plant Ownership": (
        "Parent GEM Entity ID", "Steel Plant ID", "Steel Plant ID", "Status", "steel_plant",
    ),
    "Cement and Concrete Ownership": (
        "Parent GEM Entity ID", "GEM Plant ID", "GEM Plant ID", "Status", "cement",
    ),
}

CONTROL_THRESHOLD = 50.0

# Status → bucket. Anything unmapped lands in "other".
STATUS_BUCKETS: dict[str, str] = {
    "operating": "operating",
    "operating pre-retirement": "operating",
    "construction": "development",
    "in construction": "development",
    "pre-construction": "development",
    "permitted": "development",
    "pre-permit": "development",
    "announced": "development",
    "proposed": "development",
    "in development": "development",
    "mothballed": "mothballed",
    "idle": "mothballed",
    "retired": "retired",
    "retired - inferred 2 y": "retired",
    "retired - inferred 4 y": "retired",
    "closed": "retired",
    "cancelled": "cancelled",
    "cancelled - inferred 2 y": "cancelled",
    "cancelled - inferred 4 y": "cancelled",
    "shelved": "cancelled",
    "shelved - inferred 2 y": "cancelled",
}
BUCKET_PRIORITY = ["operating", "development", "mothballed", "retired", "cancelled", "other"]
LIVE_BUCKETS = {"operating", "development"}


def _bucket(status: str | None) -> str:
    return STATUS_BUCKETS.get((status or "").strip().lower(), "other")


def build(xlsx_path: Path, release: str) -> dict:
    import openpyxl  # tooling-only dependency

    wb = openpyxl.load_workbook(xlsx_path, read_only=True)

    # (parent, tracker, project) → {"units": {unit: share_sum_or_None},
    #                               "bucket": best status bucket}
    projects: dict[tuple[str, str, str], dict] = {}

    for sheet, (pcol, projcol, unitcol, scol, tracker) in SHEET_SPEC.items():
        ws = wb[sheet]
        rows = ws.iter_rows(values_only=True)
        header = [str(h).strip() if h is not None else "" for h in next(rows)]
        idx = {name: header.index(name) for name in (pcol, projcol, unitcol, scol)}
        try:
            path_idx: int | None = header.index("Ownership Path")
        except ValueError:
            path_idx = None
        share_idx = header.index("Share")

        seen_paths: set[tuple] = set()  # exact-duplicate row guard
        n_rows = 0
        for row in rows:
            parent = (str(row[idx[pcol]] or "")).strip()
            project = (str(row[idx[projcol]] or "")).strip()
            unit = (str(row[idx[unitcol]] or "")).strip() or project
            if not parent or not project:
                continue
            n_rows += 1

            # Dedupe fully identical (parent, unit, path) rows — the coal
            # sheet alone has ~700 exact duplicates.
            path_val = row[path_idx] if path_idx is not None else None
            dedupe_key = (parent, unit, str(path_val or ""), str(row[share_idx] or ""))
            if dedupe_key in seen_paths:
                continue
            seen_paths.add(dedupe_key)

            share_raw = row[share_idx]
            try:
                share: float | None = float(share_raw) if share_raw not in (None, "") else None
            except (TypeError, ValueError):
                share = None

            bucket = _bucket(str(row[idx[scol]] or ""))

            key = (parent, tracker, project)
            rec = projects.get(key)
            if rec is None:
                rec = {"units": {}, "bucket": bucket}
                projects[key] = rec
            else:
                if BUCKET_PRIORITY.index(bucket) < BUCKET_PRIORITY.index(rec["bucket"]):
                    rec["bucket"] = bucket
            # Distinct paths to the same unit add up (e.g. two 30% routes),
            # capped at 100. Unknown shares stay unknown.
            if share is not None:
                prev = rec["units"].get(unit)
                rec["units"][unit] = min(100.0, (prev or 0.0) + share)
            else:
                rec["units"].setdefault(unit, None)
        print(f"{sheet:32s} rows={n_rows:6d}")

    # Aggregate per entity.
    entities: dict[str, dict] = {}
    for (parent, tracker, _project), rec in projects.items():
        ent = entities.setdefault(
            parent,
            {
                "total": [0, 0, 0],  # [live, operating, controlled (live, ≥50%)]
                "statuses": defaultdict(int),
                "trackers": defaultdict(lambda: [0, 0, 0]),
            },
        )
        bucket = rec["bucket"]
        ent["statuses"][bucket] += 1

        shares = [s for s in rec["units"].values() if s is not None]
        controlled = bool(shares) and max(shares) >= CONTROL_THRESHOLD

        if bucket in LIVE_BUCKETS:
            t = ent["trackers"][tracker]
            t[0] += 1
            if bucket == "operating":
                t[1] += 1
            if controlled:
                t[2] += 1
            ent["total"][0] += 1
            if bucket == "operating":
                ent["total"][1] += 1
            if controlled:
                ent["total"][2] += 1

    out_entities = {
        eid: {
            "total": e["total"],
            "statuses": dict(e["statuses"]),
            "trackers": {k: v for k, v in sorted(e["trackers"].items())},
        }
        for eid, e in entities.items()
    }

    return {
        "meta": {
            "release": release,
            "generated": dt.date.today().isoformat(),
            "source": (
                "Global Energy Ownership Tracker, Global Energy Monitor "
                f"({release} release), CC BY 4.0"
            ),
            "control_threshold_pct": CONTROL_THRESHOLD,
            "live_buckets": sorted(LIVE_BUCKETS),
            "entity_count": len(out_entities),
        },
        "entities": out_entities,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("xlsx", type=Path, help="Path to the GEOT release xlsx")
    ap.add_argument("--release", required=True, help='e.g. "May 2026"')
    ap.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "opencheck" / "data" / "geot_projects.json.gz",
    )
    args = ap.parse_args()

    data = build(args.xlsx, args.release)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.out, "wt", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = args.out.stat().st_size / 1024
    print(f"\nWrote {args.out} ({size_kb:.0f} KB, {data['meta']['entity_count']} entities)")


if __name__ == "__main__":
    main()
