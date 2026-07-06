#!/usr/bin/env python3
"""Build the vendored OECD-UNSD MEIP lookup tables from the Global Register CSV.

MEIP (the OECD-UNSD Multinational Enterprise Information Platform) publishes an
annual "Global Register" of the subsidiaries of the world's 500 largest
multinational enterprises. OpenCheck uses it as a **signpost** source: when the
subject LEI matches, we show a card that proves the entity is in MEIP, surfaces
its identifiers + MNE context, and links users to the OECD site to download the
full register. The data is NOT mapped to BODS.

This script turns the (~127k-row) CSV into two compact LEI-keyed JSON tables:

* ``data/meip_subsidiaries.json`` — keyed by the subsidiary's LEI (non-head rows
  that carry an LEI), for the "subject is a subsidiary" card.
* ``data/meip_mne_heads.json`` — keyed by the MNE head's LEI, for the "subject is
  one of the 500 largest MNEs" card (carries subsidiary counts).

The two key sets are disjoint. The ``DUNL`` column is intentionally dropped: it
is S&P's DUNL identifier (dunl.org), which only routes to the S&P Capital IQ id
we already keep in its own column.

Usage:  python3 backend/scripts/build_meip.py "<Global Register ....csv>"
Re-run when OECD publishes a new annual Global Register.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
_SUBS_OUT = _DATA / "meip_subsidiaries.json"
_HEADS_OUT = _DATA / "meip_mne_heads.json"

_HEAD = "MNE Head"


def _clean(s: str | None) -> str:
    return (s or "").strip()


def _alt_names(raw: str, name: str) -> list[str]:
    """Split the comma-joined Alternative Names, drop blanks / the primary name."""
    out: list[str] = []
    seen = {name.strip().casefold()}
    for part in (raw or "").split(","):
        p = part.strip()
        key = p.casefold()
        if p and key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _ids(row: dict) -> dict:
    """Key identifiers we surface (LEI is the map key, so not repeated here).
    DUNL is deliberately excluded (see module docstring)."""
    ids = {}
    if v := _clean(row.get("OpenCorporates")):
        ids["opencorporates"] = v
    if v := _clean(row.get("PermID")):
        ids["permid"] = v
    if v := _clean(row.get("CapIQ")):
        ids["capiq"] = v
    return ids


def build(path: Path) -> tuple[dict, dict]:
    rows = list(csv.DictReader(path.open(encoding="utf-8-sig")))

    # Subsidiary counts per parent MNE (excluding the head row itself).
    sub_total: dict[str, int] = defaultdict(int)
    sub_with_lei: dict[str, int] = defaultdict(int)
    for r in rows:
        if _clean(r.get("Hierarchy")) == _HEAD:
            continue
        parent = _clean(r.get("Parent MNE"))
        sub_total[parent] += 1
        if _clean(r.get("LEI")):
            sub_with_lei[parent] += 1

    subs: dict[str, dict] = {}
    heads: dict[str, dict] = {}

    for r in rows:
        lei = _clean(r.get("LEI")).upper()
        if not lei:
            continue
        name = _clean(r.get("Subsidiary Name (Clean)"))
        parent_mne = _clean(r.get("Parent MNE"))
        record = {
            "name": name,
            "iso3": _clean(r.get("ISO3")),
            "parent_mne": parent_mne,
            "address": _clean(r.get("Address")),
            "alt_names": _alt_names(_clean(r.get("Alternative Names")), name),
            "identifiers": _ids(r),
        }
        if _clean(r.get("Hierarchy")) == _HEAD:
            record["subsidiaries_total"] = sub_total.get(parent_mne, 0)
            record["subsidiaries_with_lei"] = sub_with_lei.get(parent_mne, 0)
            heads.setdefault(lei, record)  # first head wins on the rare dup
        else:
            record["immediate_parent"] = _clean(r.get("Parent of Subsidiary"))
            subs.setdefault(lei, record)  # first row wins on duplicate LEIs

    return dict(sorted(subs.items())), dict(sorted(heads.items()))


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    subs, heads = build(Path(sys.argv[1]))
    for path, data in ((_SUBS_OUT, subs), (_HEADS_OUT, heads)):
        path.write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        print(f"wrote {len(data)} entries → {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
