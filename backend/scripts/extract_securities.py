#!/usr/bin/env python3
"""Build OpenCheck's compact sanctioned-securities index from OpenSanctions.

OpenSanctions does not expose sanctioned-securities-by-LEI as a live API scope —
the ``securities`` collection is a packaging of a bulk CSV export. This script
turns that 8.8 MB company-centric ``securities.csv`` into a small JSON index
keyed by LEI:

    { "<LEI>": { "name", "id", "isins": [...], "regimes": [...],
                 "eo_14071": bool, "sanctioned": bool } }

It keeps only rows that have **both an LEI and ISINs** and are **sanctioned or
under the EO 14071 investment ban** — the large majority of sanctioned companies
are private with neither, so the index is a fraction of the source file
(realistically a few hundred KB).

Usage:

    # download the latest export and build the index
    python scripts/extract_securities.py --output ../data/securities/sanctioned_isins.json

    # or build from a local copy
    python scripts/extract_securities.py --input securities.csv --output index.json

Source: https://www.opensanctions.org/datasets/securities/  (CC-BY-NC 4.0)
Data dictionary: https://www.opensanctions.org/docs/data/securities/
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import urllib.request
from typing import Any

_DEFAULT_URL = "https://data.opensanctions.org/datasets/latest/securities/securities.csv"

# OpenSanctions dataset id → human regime label (the watchlist subset in
# `risk_datasets`). Unknown ids fall back to the raw id so nothing is hidden.
_REGIME_LABELS: dict[str, str] = {
    "us_ofac_sdn": "US OFAC SDN",
    "us_ofac_cons": "US OFAC (non-SDN)",
    "eu_fsf": "EU",
    "eu_journal_sanctions": "EU",
    "eu_sanctions_map": "EU",
    "eu_esma_saris": "EU ESMA",
    "gb_hmt_sanctions": "UK HMT",
    "gb_hmt_invbans": "UK investment ban",
    "gb_fcdo_sanctions": "UK FCDO",
    "ch_seco_sanctions": "Swiss SECO",
    "ca_dfatd_sema_sanctions": "Canada",
    "au_dfat_sanctions": "Australia",
    "jp_mof_sanctions": "Japan",
    "ua_nsdc_sanctions": "Ukraine NSDC",
}


def _split(cell: str | None) -> list[str]:
    """Split a multi-value CSV cell (semicolon-delimited) into clean parts."""
    if not cell:
        return []
    return [p.strip() for p in cell.split(";") if p.strip()]


def _regimes(risk_datasets: list[str], eo_14071: bool) -> list[str]:
    labels: list[str] = []
    for ds in risk_datasets:
        labels.append(_REGIME_LABELS.get(ds, ds))
    if eo_14071:
        labels.append("EO 14071 investment ban")
    # De-duplicate, preserve order.
    seen: set[str] = set()
    out: list[str] = []
    for label in labels:
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out


def build_index(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    """Build the LEI → sanctioned-securities index from CSV rows."""
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        leis = [x.upper() for x in _split(row.get("lei"))]
        if not leis:
            continue
        sanctioned = (row.get("sanctioned") or "").strip().lower() == "t"
        eo_14071 = (row.get("eo_14071") or "").strip().lower() == "t"
        if not (sanctioned or eo_14071):
            continue
        isins = _split(row.get("isins"))
        if not isins:
            continue
        regimes = _regimes(_split(row.get("risk_datasets")), eo_14071)
        name = (row.get("caption") or "").strip()
        os_id = (row.get("id") or "").strip()

        for lei in leis:
            entry = index.get(lei)
            if entry is None:
                index[lei] = {
                    "name": name,
                    "id": os_id,
                    "isins": list(dict.fromkeys(isins)),
                    "regimes": regimes,
                    "eo_14071": eo_14071,
                    "sanctioned": sanctioned,
                }
            else:
                # Merge (a company can appear under multiple LEIs / rows).
                merged = dict.fromkeys(entry["isins"])
                for i in isins:
                    merged.setdefault(i)
                entry["isins"] = list(merged)
                entry["regimes"] = list(dict.fromkeys([*entry["regimes"], *regimes]))
                entry["eo_14071"] = entry["eo_14071"] or eo_14071
                entry["sanctioned"] = entry["sanctioned"] or sanctioned
    return index


def _read_rows(input_path: str | None, url: str) -> list[dict[str, str]]:
    if input_path:
        with open(input_path, encoding="utf-8") as fh:
            return list(csv.DictReader(fh))
    print(f"Downloading {url} …", file=sys.stderr)
    with urllib.request.urlopen(url) as resp:  # noqa: S310 — fixed OpenSanctions URL
        text = resp.read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the sanctioned-securities LEI index.")
    ap.add_argument("--input", "-i", help="Local securities.csv (else download latest).")
    ap.add_argument("--output", "-o", required=True, help="Output JSON index path.")
    ap.add_argument("--url", default=_DEFAULT_URL, help="Source CSV URL.")
    args = ap.parse_args()

    rows = _read_rows(args.input, args.url)
    index = build_index(rows)

    out_dir = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(index, fh, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    n_isins = sum(len(v["isins"]) for v in index.values())
    print(
        f"Wrote {len(index):,} LEIs / {n_isins:,} sanctioned ISINs from {len(rows):,} rows "
        f"→ {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
