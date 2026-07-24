#!/usr/bin/env python3
"""Build the committed EITI SOE → LEI index for the ``eiti_soe`` adapter.

Run this locally (or anywhere the SOE Datasette host and the GLEIF API are
reachable — the SOE host is robots-restricted to some automated fetchers, so a
normal desktop environment is the right place). It writes
``backend/opencheck/data/eiti_soe_index.json.gz``.

Pipeline
--------
1. Build the SOE roster **directly from the Datasette ``companies`` table** —
   the table that actually carries ``opencorporates_id``, ``eiti_id_company``
   and ``iso_alpha2_code``. Group its payment rows into distinct SOEs (keyed on
   ``eiti_id_company``, falling back to normalised name + ISO). The ``SOE List``
   view is used only as *optional* enrichment for the audited-financial-statement
   and stock-listing fields — never as the roster, because its ``Country`` field
   spells countries out and can't be joined reliably (that mismatch is why the
   first index resolved 0 SOEs via ``opencorporates_id``).
2. Resolve each SOE to an LEI via the public GLEIF API:
     * ``opencorporates_id`` → GLEIF ``entity.registeredAs`` reverse lookup,
       confirmed by country  → confidence "high".
     * else name + country search                                → "medium"/"low".
3. Emit a gzipped, LEI-keyed artifact plus a ``meta`` coverage report (including
   how many SOEs actually carry an ``opencorporates_id``, so a low OC-id hit
   rate is visibly a data gap rather than a silent join bug).

The adapter, schema, mapper and tests do NOT depend on running this — they use
fixtures. This only populates the real artifact and prints resolution coverage.

Usage:
    python3 scripts/build_eiti_soe_index.py [--limit N] [--out PATH] [--sleep S]
"""

from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import httpx

SOE_BASE = "https://soe-database.eiti.org/eiti_database"
# Datasette encodes spaces in table/view names as "~20".
SOE_LIST_URL = f"{SOE_BASE}/SOE~20List.json"
COMPANIES_URL = f"{SOE_BASE}/companies.json"
GLEIF_API = "https://api.gleif.org/api/v1/lei-records"

_DEFAULT_OUT = (
    Path(__file__).resolve().parent.parent
    / "opencheck" / "data" / "eiti_soe_index.json.gz"
)

_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")

# Country-name → ISO alpha-2, only needed for the SOE List *enrichment* view
# (the companies roster carries iso_alpha2_code directly). Extend as needed.
_COUNTRY_ISO = {
    "afghanistan": "AF", "albania": "AL", "argentina": "AR", "chad": "TD",
    "colombia": "CO", "democratic republic of congo": "CD", "ghana": "GH",
    "guyana": "GY", "indonesia": "ID", "iraq": "IQ", "kazakhstan": "KZ",
    "mexico": "MX", "mongolia": "MN", "mozambique": "MZ", "nigeria": "NG",
    "norway": "NO", "philippines": "PH", "senegal": "SN", "seychelles": "SC",
    "tanzania": "TZ", "togo": "TG", "trinidad and tobago": "TT", "uganda": "UG",
    "ukraine": "UA",
}


def _norm_name(value: str) -> str:
    v = (value or "").strip().lower()
    v = _NON_ALNUM_RE.sub(" ", v)
    v = _WS_RE.sub(" ", v).strip()
    # Drop very common company-form tokens so "X LIMITED" == "X".
    for token in (" limited", " ltd", " plc", " inc", " corporation", " corp", " company"):
        if v.endswith(token):
            v = v[: -len(token)].strip()
    return v


def _iso(country: str, fallback: str = "") -> str:
    if country and len(country) == 2:
        return country.upper()
    return _COUNTRY_ISO.get((country or "").strip().lower(), fallback).upper()


def _fetch_datasette(client: httpx.Client, url: str) -> list[dict[str, Any]]:
    """Fetch a Datasette table/view as a JSON array, all rows."""
    r = client.get(url, params={"_shape": "array", "_size": "max"},
                   headers={"Accept": "application/json"})
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("rows", [])


def _resolve_lei(
    client: httpx.Client, *, oc_id: str, name: str, iso: str, sleep: float
) -> tuple[str | None, str, str]:
    """Return (lei, method, confidence). (None, "", "") when unresolved."""
    # 1. opencorporates_id → registeredAs reverse lookup, confirmed by country.
    if oc_id:
        number = oc_id.rstrip("/").split("/")[-1]
        if number:
            try:
                r = client.get(
                    GLEIF_API,
                    params={"filter[entity.registeredAs]": number, "page[size]": 10},
                    headers={"Accept": "application/vnd.api+json"},
                )
                if r.is_success:
                    for rec in r.json().get("data", []):
                        ent = rec.get("attributes", {}).get("entity", {})
                        cc = (ent.get("legalAddress", {}).get("country")
                              or ent.get("jurisdiction") or "")
                        if not iso or cc.upper().startswith(iso):
                            time.sleep(sleep)
                            return rec.get("id"), "opencorporates_id", "high"
            except Exception as exc:  # noqa: BLE001
                print(f"  ! GLEIF registeredAs error for {name}: {exc}", file=sys.stderr)
            time.sleep(sleep)

    # 2. name + country fulltext search.
    if name:
        try:
            params = {"filter[fulltext]": name, "page[size]": 10}
            if iso:
                params["filter[entity.legalAddress.country]"] = iso
            r = client.get(GLEIF_API, params=params,
                           headers={"Accept": "application/vnd.api+json"})
            if r.is_success:
                target = _norm_name(name)
                for rec in r.json().get("data", []):
                    ent = rec.get("attributes", {}).get("entity", {})
                    legal = ent.get("legalName", {}).get("name", "")
                    conf = "medium" if _norm_name(legal) == target else "low"
                    time.sleep(sleep)
                    return rec.get("id"), "name_country", conf
        except Exception as exc:  # noqa: BLE001
            print(f"  ! GLEIF name search error for {name}: {exc}", file=sys.stderr)
        time.sleep(sleep)

    return None, "", ""


def _soe_roster(companies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group companies-table payment rows into distinct SOEs.

    Keyed on ``eiti_id_company`` where present (stable), else normalised
    name + ISO. The representative row prefers one carrying an
    ``opencorporates_id``. Aggregates the set of reporting years.
    """
    soes: dict[str, dict[str, Any]] = {}
    for c in companies:
        name = (c.get("company_name") or c.get("original_company_name") or "").strip()
        if not name:
            continue
        iso = _iso(c.get("iso_alpha2_code") or c.get("country") or "")
        eid = (c.get("eiti_id_company") or "").strip()
        gkey = eid or f"name:{_norm_name(name)}|{iso}"
        agg = soes.setdefault(
            gkey, {"name": name, "iso": iso, "years": set(), "row": c}
        )
        if c.get("year"):
            agg["years"].add(str(c["year"]))
        # Prefer a representative row that carries an opencorporates_id.
        if c.get("opencorporates_id") and not agg["row"].get("opencorporates_id"):
            agg["row"] = c
    return soes


def build(limit: int | None, out: Path, sleep: float) -> None:
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        print("Fetching companies table (roster + identifiers) …", file=sys.stderr)
        companies = _fetch_datasette(client, COMPANIES_URL)
        print(f"  {len(companies)} company payment rows", file=sys.stderr)
        # SOE List is optional enrichment only (audited financials / listings).
        try:
            roster_rows = _fetch_datasette(client, SOE_LIST_URL)
            print(f"  {len(roster_rows)} SOE List rows (enrichment)", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! SOE List fetch failed ({exc}); continuing from companies only",
                  file=sys.stderr)
            roster_rows = []

    # SOE List enrichment keyed by (normalised name, iso): AFS + listing fields.
    listing: dict[tuple[str, str], dict[str, Any]] = {}
    for row in roster_rows:
        k = (_norm_name(row.get("SOE") or ""), _iso(row.get("Country") or ""))
        if k[0]:
            listing.setdefault(k, row)

    soes = _soe_roster(companies)

    index: dict[str, dict[str, Any]] = {}
    counts = {"total": 0, "high": 0, "medium": 0, "low": 0, "unresolved": 0,
              "by_oc_id": 0, "by_name": 0, "with_oc_id": 0}

    items = list(soes.items())
    if limit:
        items = items[:limit]

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for _gkey, agg in items:
            counts["total"] += 1
            c = agg["row"]
            oc_id = (c.get("opencorporates_id") or "").strip()
            if oc_id:
                counts["with_oc_id"] += 1
            name = agg["name"]
            iso = agg["iso"]
            enrich = listing.get((_norm_name(name), iso), {})
            print(f"[{counts['total']}/{len(items)}] {name} ({iso}) "
                  f"{'oc:' + oc_id if oc_id else 'no-oc'} …", file=sys.stderr)
            lei, method, conf = _resolve_lei(
                client, oc_id=oc_id, name=name, iso=iso, sleep=sleep
            )
            if not lei:
                counts["unresolved"] += 1
                continue
            counts[conf] += 1
            counts["by_oc_id" if method == "opencorporates_id" else "by_name"] += 1
            commodities = (
                c.get("company_commodities") or enrich.get("Commodities") or ""
            )
            index[lei.upper()] = {
                "lei": lei.upper(),
                "match_method": method,
                "match_confidence": conf,
                "soe": {
                    "company_name": name,
                    "original_company_name": c.get("original_company_name"),
                    "country": iso,
                    "iso_alpha2": iso,
                    "sector": c.get("company_sector") or enrich.get("Sector"),
                    "commodities": [s.strip() for s in re.split(r"[;,]", commodities) if s.strip()],
                    "company_type": c.get("company_type") or "State-owned enterprise",
                    "government_entity": c.get("government_entity"),
                    "opencorporates_id": oc_id or None,
                    "eiti_id_company": c.get("eiti_id_company"),
                    "eiti_id_government": c.get("eiti_id_government"),
                    "audited_financial_statement": (
                        c.get("company_audited_financial_statement_or_equivalent")
                        or enrich.get("Audited Financial Statement or Equivalent")
                    ),
                    "public_listing_or_website": (
                        c.get("company_public_listing_or_website")
                        or enrich.get("Public Listing or Website")
                    ),
                    "years": sorted(agg["years"]),
                    "soe_list": True,
                },
            }

    payload = {
        "meta": {
            "built": _dt.date.today().isoformat(),
            "source": SOE_BASE,
            "source_snapshot": _dt.date.today().isoformat(),
            "companies": counts["total"],
            "with_opencorporates_id": counts["with_oc_id"],
            "resolved_lei": len(index),
            "resolved_high": counts["high"],
            "resolved_medium": counts["medium"],
            "resolved_low": counts["low"],
            "resolved_by_oc_id": counts["by_oc_id"],
            "resolved_by_name": counts["by_name"],
            "unresolved": counts["unresolved"],
            "license": "EITI open data (free reuse with attribution)",
            "attribution": "EITI International Secretariat, eiti.org",
        },
        "index": index,
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    m = payload["meta"]
    print("\n=== EITI SOE index coverage ===", file=sys.stderr)
    print(f"  SOEs processed     : {m['companies']}", file=sys.stderr)
    print(f"  with OpenCorp. id  : {m['with_opencorporates_id']}", file=sys.stderr)
    print(f"  resolved to LEI    : {m['resolved_lei']} "
          f"(high {m['resolved_high']}, medium {m['resolved_medium']}, low {m['resolved_low']})",
          file=sys.stderr)
    print(f"  by OC id / by name : {m['resolved_by_oc_id']} / {m['resolved_by_name']}",
          file=sys.stderr)
    print(f"  unresolved         : {m['unresolved']}", file=sys.stderr)
    print(f"  written            : {out}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="cap SOEs processed (debug)")
    ap.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    ap.add_argument("--sleep", type=float, default=0.2, help="pause between GLEIF calls")
    args = ap.parse_args()
    build(args.limit, args.out, args.sleep)


if __name__ == "__main__":
    main()
