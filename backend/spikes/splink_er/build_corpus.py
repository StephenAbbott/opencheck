"""build_corpus.py — assemble an entity-record corpus for the Splink ER spike.

Phase 1 of the "Splink probabilistic matching for no-shared-identifier pairs"
spike (see the Notion implementation plan). Pulls BODS **entity statements** for
a batch of LEIs from the OpenCheck ``/export`` endpoint and flattens each to one
row with the *soft* comparison features used for matching —

    name_norm, jurisdiction, inc_date, address_norm

— plus the *identifier labels* held out of the model for ground truth:

    lei, nat_reg   (jurisdiction-scoped national registration number)

Records that share an ``lei`` (or ``nat_reg``) are, by definition, the same
real-world entity. That gives us free labels to (a) estimate Splink's m
probabilities and (b) evaluate whether a name/jurisdiction/date/address-only
model recovers those matches *without* seeing the identifiers.

Design notes:

* **Snowball** — each export bundle already contains the subject plus its
  GLEIF parents/subsidiaries and per-source duplicates, so a handful of seed
  LEIs grows into hundreds of rows. New LEIs found in a bundle are queued and
  fetched too, up to ``--n`` total lookups.
* **OpenSanctions excluded** — CC-BY-NC; skipped from the corpus per the spike
  decision (2026-06-29).
* **Resumable** — already-fetched LEIs are recorded in ``corpus/_done_leis.txt``
  and skipped; rows are appended to ``corpus/entities.csv``. Re-run with a
  larger ``--n`` to grow the corpus (Render's free tier is slow, so build it up
  over several runs).

Run (repeatable; accumulates) from ``backend/``::

    uv run python spikes/splink_er/build_corpus.py --n 60 --batch 8

Then (Phase 3) Splink reads ``corpus/entities.csv`` / ``entities.parquet``.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path

import httpx

API_BASE = os.environ.get("OPENCHECK_API_BASE", "https://api.opencheck.world").rstrip("/")
HERE = Path(__file__).parent
CORPUS_DIR = HERE / "corpus"
CSV_PATH = CORPUS_DIR / "entities.csv"
PARQUET_PATH = CORPUS_DIR / "entities.parquet"
DONE_PATH = CORPUS_DIR / "_done_leis.txt"

# Known-good, jurisdiction-diverse seed LEIs (the in-app EXAMPLE_LEIS + Novo
# Nordisk). Each anchors a corporate group, so the snowball reaches many
# multi-source entities.
SEED_LEIS = [
    "213800LH1BZH3DI6G760",
    "253400JT3MQWNDKMJE44",
    "2138008KTNTDICZU8L25",
    "2138002S3XGZ38WN5Q72",
    "213800E11LI1SCETU492",
    "213800AG2V6YE68H5N63",
    "549300DAQ1CVT6CXN342",  # Novo Nordisk A/S
]

LEI_RE = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")
_FIELDNAMES = [
    "record_id", "subject_lei", "source_id",
    "name_raw", "name_norm", "jurisdiction", "inc_date", "address_norm",
    "lei", "nat_reg",
]


def normalise_name(name: str) -> str:
    """Lower, strip diacritics, drop punctuation, collapse whitespace.

    Mirrors ``opencheck.reconcile._normalise_name`` so the baseline comparison
    in Phase 5 is apples-to-apples."""
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_only.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _jurisdiction(rd: dict) -> str:
    # GLEIF entity statements carry `incorporatedInJurisdiction`; OpenSanctions
    # (and some others) use `jurisdiction`. Read both.
    jur = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction") or {}
    code = jur.get("code") if isinstance(jur, dict) else ""
    return str(code or "").strip().upper().split("-")[0]


def _address_norm(rd: dict) -> str:
    addrs = rd.get("addresses") or []
    if not isinstance(addrs, list) or not addrs:
        return ""
    reg = next((a for a in addrs if (a or {}).get("type") == "registered"), addrs[0])
    raw = (reg or {}).get("address") or ""
    return re.sub(r"\s+", " ", str(raw).lower()).strip()


def _identifiers(rd: dict, jurisdiction: str) -> tuple[str, str]:
    """Return (lei, nat_reg) labels. nat_reg is a jurisdiction-scoped key for a
    bare national registration number (scheme empty or ``<JUR>-…``), matching
    the frontend reconciliation key so the two stay comparable."""
    lei = ""
    nat_reg = ""
    for ident in rd.get("identifiers") or []:
        val = str(ident.get("id") or "").strip().upper()
        if not val:
            continue
        scheme = str(ident.get("scheme") or "").strip().upper()
        if LEI_RE.match(val):
            lei = lei or val
        elif jurisdiction and (scheme == "" or scheme.startswith(f"{jurisdiction}-")) and "/" not in val:
            nat_reg = nat_reg or f"JUR:{jurisdiction}:{val}"
    return lei, nat_reg


def fetch_export(client: httpx.Client, lei: str) -> list[dict]:
    r = client.get(f"{API_BASE}/export", params={"lei": lei, "format": "json", "deepen_top": 3})
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def extract_rows(bods: list[dict], subject_lei: str) -> tuple[list[dict], set[str]]:
    """Flatten entity statements to rows; return (rows, discovered_leis).

    Skips OpenSanctions rows (CC-BY-NC) and non-entity statements."""
    rows: list[dict] = []
    discovered: set[str] = set()
    for stmt in bods:
        if stmt.get("recordType") != "entity":
            continue
        src = ((stmt.get("source") or {}).get("description") or "").strip()
        if "opensanctions" in src.lower():
            continue
        rd = stmt.get("recordDetails") or {}
        name = (rd.get("name") or "").strip()
        if not name:
            continue
        jurisdiction = _jurisdiction(rd)
        lei, nat_reg = _identifiers(rd, jurisdiction)
        if lei:
            discovered.add(lei)
        rows.append({
            "record_id": stmt.get("statementId") or "",
            "subject_lei": subject_lei,
            "source_id": src,
            "name_raw": name,
            "name_norm": normalise_name(name),
            "jurisdiction": jurisdiction,
            "inc_date": (rd.get("foundingDate") or "").strip(),
            "address_norm": _address_norm(rd),
            "lei": lei,
            "nat_reg": nat_reg,
        })
    return rows, discovered


def _load_done() -> set[str]:
    if not DONE_PATH.exists():
        return set()
    return {ln.strip() for ln in DONE_PATH.read_text().splitlines() if ln.strip()}


def _append_rows(rows: list[dict]) -> None:
    new_file = not CSV_PATH.exists()
    with CSV_PATH.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        if new_file:
            w.writeheader()
        w.writerows(rows)


def _write_parquet() -> None:
    """Convenience parquet copy for Phase 3. Non-fatal — the CSV is the source
    of truth and Splink reads it directly."""
    try:
        import duckdb

        duckdb.sql(
            f"COPY (SELECT * FROM read_csv('{CSV_PATH}', header=true, "
            f"quote='\"', escape='\"', all_varchar=true, ignore_errors=true)) "
            f"TO '{PARQUET_PATH}' (FORMAT PARQUET)"
        )
    except Exception as e:  # noqa: BLE001 — parquet is optional
        print(f"  (parquet skipped: {type(e).__name__}; CSV is the source of truth)")


def _report() -> None:
    if not CSV_PATH.exists():
        print("no corpus yet")
        return
    rows = list(csv.DictReader(CSV_PATH.open()))
    n = len(rows)
    print(f"\n=== corpus stats ({n} entity rows) ===")
    cols = ["name_norm", "jurisdiction", "inc_date", "address_norm", "lei", "nat_reg"]
    for c in cols:
        filled = sum(1 for r in rows if r.get(c))
        print(f"  {c:13s} non-null: {filled:4d} ({100*filled/n:.0f}%)")
    # entities with >=3 soft features (Splink needs several low-corr columns)
    soft = ["name_norm", "jurisdiction", "inc_date", "address_norm"]
    rich = sum(1 for r in rows if sum(1 for c in soft if r.get(c)) >= 3)
    print(f"  rows with >=3 soft features: {rich} ({100*rich/n:.0f}%)")
    # cross-source duplicates = the matched pairs we can evaluate
    by_lei: dict[str, set[str]] = {}
    for r in rows:
        if r.get("lei"):
            by_lei.setdefault(r["lei"], set()).add(r["source_id"])
    multi = {k: v for k, v in by_lei.items() if len(v) >= 2}
    print(f"  distinct LEIs: {len(by_lei)};  appearing in >=2 sources: {len(multi)}")
    # genuinely-no-identifier rows (the real target — no ground truth)
    no_id = sum(1 for r in rows if not r.get("lei") and not r.get("nat_reg"))
    print(f"  rows with NO identifier label (real ER target): {no_id}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=40, help="total lookups to reach (cumulative)")
    ap.add_argument("--batch", type=int, default=8, help="new lookups per invocation")
    ap.add_argument("--no-snowball", action="store_true", help="seeds only, don't follow discovered LEIs")
    args = ap.parse_args()

    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    done = _load_done()
    # frontier: seeds not yet done, plus (resumed) discovered LEIs from prior CSV
    queue: list[str] = [l for l in SEED_LEIS if l not in done]
    if CSV_PATH.exists() and not args.no_snowball:
        for r in csv.DictReader(CSV_PATH.open()):
            lei = (r.get("lei") or "").strip()
            if lei and lei not in done and lei not in queue:
                queue.append(lei)

    # record_ids already in the corpus — dedupe so a timed-out/re-run can't
    # double-append the same statement.
    seen_ids: set[str] = set()
    if CSV_PATH.exists():
        seen_ids = {r["record_id"] for r in csv.DictReader(CSV_PATH.open()) if r.get("record_id")}

    def _persist_done() -> None:
        DONE_PATH.write_text("\n".join(sorted(done)) + "\n")

    processed = 0
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        while queue and processed < args.batch and len(done) < args.n:
            lei = queue.pop(0)
            if lei in done:
                continue
            t0 = time.time()
            try:
                bods = fetch_export(client, lei)
            except Exception as e:  # noqa: BLE001 — spike: skip & continue
                print(f"  ! {lei} failed: {type(e).__name__}: {e}")
                done.add(lei)  # don't retry forever within a run
                _persist_done()
                continue
            rows, discovered = extract_rows(bods, lei)
            rows = [r for r in rows if r["record_id"] and r["record_id"] not in seen_ids]
            seen_ids.update(r["record_id"] for r in rows)
            _append_rows(rows)
            done.add(lei)
            _persist_done()  # incremental — safe under the 45s sandbox cap / Ctrl-C
            processed += 1
            if not args.no_snowball:
                for d in discovered:
                    if d not in done and d not in queue:
                        queue.append(d)
            print(f"  ✓ {lei}: {len(rows):2d} rows ({time.time()-t0:.1f}s)")

    _write_parquet()
    print(f"\nlookups done: {len(done)} / target {args.n}  (this run: {processed})")
    _report()
    return 0


if __name__ == "__main__":
    sys.exit(main())
