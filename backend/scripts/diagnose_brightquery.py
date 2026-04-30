#!/usr/bin/env python3
"""Diagnostic tool for BrightQuery bulk data files.

Run this BEFORE extract_brightquery.py to discover the actual file format.
It will tell you:
  - what file extensions are present
  - what the first record looks like
  - where (if anywhere) LEI data appears

Usage:
    python backend/scripts/diagnose_brightquery.py \
        --org-dir ~/Downloads/brightquery/organisation
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import Counter
from pathlib import Path


def _open_file(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8", errors="replace")


def _try_parse(path: Path) -> list[dict]:
    """Try to parse a file as JSON object, JSON array, or JSONL."""
    records = []
    try:
        with _open_file(path) as fh:
            content = fh.read(200_000)  # read up to 200 KB

        # Try as a single JSON value
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                return [data]
            if isinstance(data, list):
                return [r for r in data if isinstance(r, dict)]
        except json.JSONDecodeError:
            pass

        # Try as JSONL (one JSON object per line)
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    records.append(obj)
            except json.JSONDecodeError:
                pass
    except Exception as exc:
        print(f"  [error reading {path.name}] {exc}")
    return records


def _find_lei_in_record(record: dict) -> str | None:
    """Return the LEI value from a record, or None. Tries multiple patterns."""
    # Pattern 1: top-level bq_lei field
    if record.get("bq_lei"):
        return str(record["bq_lei"])

    features = record.get("FEATURES") or []

    for f in features:
        if not isinstance(f, dict):
            continue

        # Pattern 2: OTHER_ID_TYPE == "LEI"
        if f.get("OTHER_ID_TYPE", "").upper() == "LEI":
            return str(f.get("OTHER_ID_NUMBER", ""))

        # Pattern 3: OTHER_ID_TYPE == "bq_lei"
        if f.get("OTHER_ID_TYPE", "").lower() == "bq_lei":
            return str(f.get("OTHER_ID_NUMBER", ""))

        # Pattern 4: direct bq_lei key in a feature dict
        if "bq_lei" in f:
            return str(f["bq_lei"])

        # Pattern 5: direct LEI key
        if "LEI" in f:
            return str(f["LEI"])

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--org-dir", type=Path,
        default=Path.home() / "Downloads/brightquery/organisation",
        help="BrightQuery organisation data directory",
    )
    parser.add_argument("--max-files", type=int, default=20,
        help="Max files to inspect (default 20)")
    args = parser.parse_args()

    if not args.org_dir.exists():
        print(f"Directory not found: {args.org_dir}")
        return

    print(f"\n=== BrightQuery file format diagnostic ===")
    print(f"Directory: {args.org_dir}\n")

    # Collect all files, any extension
    all_files: list[Path] = []
    ext_counts: Counter = Counter()
    for root, _dirs, files in os.walk(args.org_dir):
        for fname in files:
            p = Path(root) / fname
            all_files.append(p)
            ext = p.suffix.lower() or "(no extension)"
            ext_counts[ext] += 1

    print(f"Total files found: {len(all_files)}")
    print("Extensions:")
    for ext, count in ext_counts.most_common(10):
        print(f"  {ext:20s} {count:,}")

    if not all_files:
        print("\nNo files found in directory.")
        return

    # Inspect first N files
    sample_files = all_files[:args.max_files]
    print(f"\n--- Inspecting first {len(sample_files)} file(s) ---")

    lei_found = 0
    no_lei = 0
    parse_errors = 0

    for path in sample_files:
        records = _try_parse(path)
        if not records:
            parse_errors += 1
            print(f"\n[{path.name}] Could not parse as JSON or JSONL")
            # Show raw first 300 chars
            try:
                with _open_file(path) as fh:
                    raw = fh.read(300)
                print(f"  Raw content: {raw!r}")
            except Exception as e:
                print(f"  Could not read: {e}")
            continue

        for rec in records[:1]:  # show first record per file
            lei = _find_lei_in_record(rec)
            if lei:
                lei_found += 1
                print(f"\n[{path.name}] ✓ LEI found: {lei}")
            else:
                no_lei += 1
                if no_lei <= 3:
                    print(f"\n[{path.name}] No LEI — record structure:")
                    features = rec.get("FEATURES") or []
                    # Show RECORD_ID and all feature types
                    print(f"  RECORD_ID: {rec.get('RECORD_ID')}")
                    print(f"  bq_dataset: {rec.get('bq_dataset')}")
                    print(f"  Top-level keys: {list(rec.keys())}")
                    print(f"  FEATURES ({len(features)} entries):")
                    for f in features[:12]:
                        print(f"    {f}")
                    if len(features) > 12:
                        print(f"    ... ({len(features) - 12} more)")

    print(f"\n--- Summary of {len(sample_files)} files ---")
    print(f"  With LEI   : {lei_found}")
    print(f"  Without LEI: {no_lei}")
    print(f"  Parse errors: {parse_errors}")

    if lei_found == 0 and no_lei > 0:
        print("\n⚠ No LEI found in sample. Check the feature key names above.")
        print("  Common patterns to look for:")
        print("  - {'OTHER_ID_TYPE': 'LEI', 'OTHER_ID_NUMBER': '...'}")
        print("  - {'bq_lei': '...'}")
        print("  - {'LEI': '...'}")

    if parse_errors > 0 and lei_found == 0 and no_lei == 0:
        print("\n⚠ Files could not be parsed. They may be:")
        print("  - A different format (CSV, Parquet, etc.)")
        print("  - Gzip-compressed (try renaming to .gz)")
        print("  - JSONL where lines are split differently")


if __name__ == "__main__":
    main()
