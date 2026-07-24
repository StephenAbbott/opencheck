#!/usr/bin/env python3
"""Build the committed EITI SOE → LEI index for the ``eiti_soe`` adapter.

Run this locally (or anywhere the SOE Datasette host and the GLEIF API are
reachable — the SOE host is robots-restricted to some automated fetchers, so a
normal desktop environment is the right place). It writes
``backend/opencheck/data/eiti_soe_index.json.gz``.

Pipeline
--------
1. Pull the canonical SOE roster from the Datasette ``SOE List`` view and the
   ``companies`` table (for ``opencorporates_id`` / ``eiti_id_company``), and
   join them on normalised name + ISO country.
2. Resolve each SOE to an LEI via the public GLEIF API:
     * ``opencorporates_id`` → GLEIF ``entity.registeredAs`` reverse lookup,
       confirmed by country  → confidence "high".
     * else name + country search                                → "medium"/"low".
3. Emit a gzipped, LEI-keyed artifact plus a ``meta`` coverage report.

The adapter, schema, mapper and tests do NOT depend on running this — they use
fixtures. This only populates the real artifact and prints resolution coverage,
which is the honest way to learn how many of the ~100 SOEs actually join
OpenCheck's LEI universe before trusting the source in the UI.

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

# Minimal country-name → ISO alpha-2 helper for the SOE List "Country" field.
# The companies table already carries iso_alpha2_code; this only covers the
# view, which spells countries out. Extend as needed.
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


def build(limit: int | None, out: Path, sleep: float) -> None:
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        print("Fetching SOE List view …", file=sys.stderr)
        roster = _fetch_datasette(client, SOE_LIST_URL)
        print(f"  {len(roster)} SOE roster rows", file=sys.stderr)
        print("Fetching companies table (for opencorporates_id / eiti ids) …",
              file=sys.stderr)
        companies = _fetch_datasette(client, COMPANIES_URL)
        print(f"  {len(companies)} company rows", file=sys.stderr)

    # Enrichment lookup: normalised (name, iso) -> best company row.
    enrich: dict[tuple[str, str], dict[str, Any]] = {}
    for c in companies:
        key = (_norm_name(c.get("company_name") or c.get("original_company_name") or ""),
               _iso(c.get("iso_alpha2_code") or c.get("country") or ""))
        if key[0] and (key not in enrich or c.get("opencorporates_id")):
            enrich[key] = c

    # Distinct SOEs from the roster (a company may appear per year).
    soes: dict[tuple[str, str], dict[str, Any]] = {}
    for row in roster:
        name = row.get("SOE") or row.get("company_name") or ""
        iso = _iso(row.get("Country") or row.get("country") or "")
        key = (_norm_name(name), iso)
        if not key[0]:
            continue
        agg = soes.setdefault(key, {"name": name, "iso": iso, "years": set(), "rows": []})
        if row.get("Year"):
            agg["years"].add(str(row["Year"]))
        agg["rows"].append(row)

    index: dict[str, dict[str, Any]] = {}
    counts = {"total": 0, "high": 0, "medium": 0, "low": 0, "unresolved": 0,
              "by_oc_id": 0, "by_name": 0}

    items = list(soes.items())
    if limit:
        items = items[:limit]

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for key, agg in items:
            counts["total"] += 1
            c = enrich.get(key, {})
            oc_id = (c.get("opencorporates_id") or "").strip()
            first = agg["rows"][0]
            name = agg["name"]
            iso = agg["iso"]
            print(f"[{counts['total']}/{len(items)}] {name} ({iso}) …", file=sys.stderr)
            lei, method, conf = _resolve_lei(
                client, oc_id=oc_id, name=name, iso=iso, sleep=sleep
            )
            if not lei:
                counts["unresolved"] += 1
                continue
            counts[conf] += 1
            counts["by_oc_id" if method == "opencorporates_id" else "by_name"] += 1
            commodities = c.get("company_commodities") or first.get("Commodities") or ""
            index[lei.upper()] = {
                "lei": lei.upper(),
                "match_method": method,
                "match_confidence": conf,
                "soe": {
                    "company_name": name,
                    "original_company_name": c.get("original_company_name"),
                    "country": iso,
                    "iso_alpha2": iso,
                    "sector": c.get("company_sector") or first.get("Sector"),
                    "commodities": [s.strip() for s in re.split(r"[;,]", commodities) if s.strip()],
                    "company_type": c.get("company_type") or "State-owned enterprise",
                    "government_entity": c.get("government_entity"),
                    "opencorporates_id": oc_id or None,
                    "eiti_id_company": c.get("eiti_id_company"),
                    "eiti_id_government": c.get("eiti_id_government"),
                    "audited_financial_statement": (
                        first.get("Audited Financial Statement or Equivalent")
                        or c.get("company_audited_financial_statement_or_equivalent")
                    ),
                    "public_listing_or_website": (
                        first.get("Public Listing or Website")
                        or c.get("company_public_listing_or_website")
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
    print(f"  SOEs processed : {m['companies']}", file=sys.stderr)
    print(f"  resolved to LEI: {m['resolved_lei']} "
          f"(high {m['resolved_high']}, medium {m['resolved_medium']}, low {m['resolved_low']})",
          file=sys.stderr)
    print(f"  by OC id / name: {m['resolved_by_oc_id']} / {m['resolved_by_name']}",
          file=sys.stderr)
    print(f"  unresolved     : {m['unresolved']}", file=sys.stderr)
    print(f"  written        : {out}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="cap SOEs processed (debug)")
    ap.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    ap.add_argument("--sleep", type=float, default=0.2, help="pause between GLEIF calls")
    args = ap.parse_args()
    build(args.limit, args.out, args.sleep)


if __name__ == "__main__":
    main()
