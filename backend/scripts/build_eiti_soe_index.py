#!/usr/bin/env python3
"""Build the committed EITI SOE → LEI index for the ``eiti_soe`` adapter.

Run this locally (or anywhere the SOE Datasette host and the GLEIF API are
reachable). It writes ``backend/opencheck/data/eiti_soe_index.json.gz``.

Data model (learned from the live Datasette, 2026-07)
-----------------------------------------------------
* The ``companies`` table is **all declared companies** (private + state-owned;
  ``company_type`` distinguishes them) — ~5,300 distinct companies across
  ~60,000 payment rows. It carries ``iso_alpha2_code`` and, on a *tiny* minority
  of rows (~0.1%), ``opencorporates_id``. It is NOT the SOE roster.
* The ``SOE List`` view is the **curated roster of state-owned enterprises** —
  columns ``Year, SOE, Country, Sector, Commodities, Audited Financial
  Statement or Equivalent, Public Listing or Website``. This is the roster.

Pipeline
--------
1. Fetch the whole ``companies`` table **with pagination** (Datasette caps a
   single response at ~1,000 rows, so ``_size=max`` alone silently truncates —
   that truncation is why an earlier build saw only 176 of 5,332 companies).
   Build a normalised-name → {iso, opencorporates_id, eiti ids, government,
   sector, commodities, company_type} map.
2. Roster from the ``SOE List`` view; join each SOE to the companies map by
   normalised name to inherit its real ISO code and (where present) its
   ``opencorporates_id``.
3. Resolve each SOE to an LEI via GLEIF: ``opencorporates_id`` → ``registeredAs``
   reverse lookup (confidence "high") where available, else a name + country
   search that is only accepted when the normalised legal names actually match
   ("medium"). Same-country hits whose names don't match are dropped as
   unresolved rather than indexed as low-confidence guesses, so every indexed
   SOE is a genuine match. Coverage is expected to be low (most extractive SOEs
   simply have no LEI in GLEIF); the report makes that explicit.

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
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

SOE_BASE = "https://soe-database.eiti.org/eiti_database"
SOE_LIST_URL = f"{SOE_BASE}/SOE~20List.json"   # Datasette encodes the space as ~20
COMPANIES_URL = f"{SOE_BASE}/companies.json"
GLEIF_API = "https://api.gleif.org/api/v1/lei-records"

# Browser-like UA — the bare httpx UA can trip the host's Cloudflare challenge.
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 opencheck-eiti-soe-index"

# The audited-financials column name has a literal space (schema quirk), not an
# underscore — get the key exactly right or the field is silently always empty.
_AFS_COL = "company_audited_financial statement_or_equivalent"

_DEFAULT_OUT = (
    Path(__file__).resolve().parent.parent
    / "opencheck" / "data" / "eiti_soe_index.json.gz"
)

_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")

# Fallback country-name → ISO alpha-2, used only when a SOE doesn't join to a
# companies row (which would otherwise supply iso_alpha2_code directly).
# Covers the EITI implementing countries.
_COUNTRY_ISO = {
    "afghanistan": "AF", "albania": "AL", "argentina": "AR", "armenia": "AM",
    "burkina faso": "BF", "cameroon": "CM", "central african republic": "CF",
    "chad": "TD", "colombia": "CO", "democratic republic of congo": "CD",
    "republic of congo": "CG", "congo": "CG", "cote d'ivoire": "CI",
    "côte d'ivoire": "CI", "dominican republic": "DO", "ecuador": "EC",
    "ethiopia": "ET", "germany": "DE", "ghana": "GH", "guatemala": "GT",
    "guinea": "GN", "guyana": "GY", "honduras": "HN", "indonesia": "ID",
    "iraq": "IQ", "kazakhstan": "KZ", "kyrgyz republic": "KG",
    "kyrgyzstan": "KG", "liberia": "LR", "madagascar": "MG", "malawi": "MW",
    "mali": "ML", "mauritania": "MR", "mexico": "MX", "mongolia": "MN",
    "mozambique": "MZ", "myanmar": "MM", "netherlands": "NL", "niger": "NE",
    "nigeria": "NG", "norway": "NO", "papua new guinea": "PG", "peru": "PE",
    "philippines": "PH", "sao tome and principe": "ST",
    "são tomé and príncipe": "ST", "senegal": "SN", "seychelles": "SC",
    "sierra leone": "SL", "suriname": "SR", "tajikistan": "TJ",
    "tanzania": "TZ", "timor-leste": "TL", "togo": "TG",
    "trinidad and tobago": "TT", "uganda": "UG", "ukraine": "UA",
    "united kingdom": "GB", "zambia": "ZM",
}


def _norm_name(value: str) -> str:
    v = (value or "").strip().lower()
    v = _NON_ALNUM_RE.sub(" ", v)
    v = _WS_RE.sub(" ", v).strip()
    for token in (" limited", " ltd", " plc", " inc", " corporation", " corp", " company"):
        if v.endswith(token):
            v = v[: -len(token)].strip()
    return v


def _iso(country: str) -> str:
    if country and len(country) == 2:
        return country.upper()
    return _COUNTRY_ISO.get((country or "").strip().lower(), "")


def _fetch_all(client: httpx.Client, url: str, sleep: float = 0.3) -> list[dict[str, Any]]:
    """Fetch every row of a Datasette table/view, following pagination.

    Datasette caps a single JSON response at ``max_returned_rows`` (1,000 on
    this host), so we must follow the ``next`` token rather than trusting
    ``_size=max``. Handles both the 1.0 rows-as-objects shape and the older
    rows-as-lists + ``columns`` shape.
    """
    out: list[dict[str, Any]] = []
    nxt: str | None = None
    page = 0
    while True:
        params: dict[str, Any] = {"_size": 1000}
        if nxt:
            params["_next"] = nxt
        r = client.get(url, params=params,
                       headers={"Accept": "application/json", "User-Agent": _UA})
        r.raise_for_status()
        j = r.json()
        if isinstance(j, list):          # _shape=array style — no pagination token
            out.extend(j)
            break
        rows = j.get("rows") or []
        cols = j.get("columns")
        for row in rows:
            out.append(row if isinstance(row, dict) else dict(zip(cols, row)))
        nxt = j.get("next")
        page += 1
        if not nxt:
            break
        time.sleep(sleep)               # be gentle with the host
    return out


def _company_map(companies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """normalised company name → best-known identifiers/attributes."""
    m: dict[str, dict[str, Any]] = {}
    for c in companies:
        name = (c.get("company_name") or c.get("original_company_name") or "").strip()
        if not name:
            continue
        key = _norm_name(name)
        if not key:
            continue
        rec = m.get(key)
        if rec is None:
            rec = {
                "name": name, "iso": "", "oc": "", "company_type": "",
                "eiti_id_company": "", "eiti_id_government": "",
                "government_entity": "", "sector": "", "commodities": "",
                "afs": "", "listing": "", "years": set(),
            }
            m[key] = rec
        if not rec["iso"] and (c.get("iso_alpha2_code") or "").strip():
            rec["iso"] = (c.get("iso_alpha2_code") or "").strip().upper()
        if not rec["oc"] and (c.get("opencorporates_id") or "").strip():
            rec["oc"] = (c.get("opencorporates_id") or "").strip()
        for dst, src in (
            ("eiti_id_company", "eiti_id_company"),
            ("eiti_id_government", "eiti_id_government"),
            ("government_entity", "government_entity"),
            ("sector", "company_sector"),
            ("commodities", "company_commodities"),
            ("afs", _AFS_COL),
            ("listing", "company_public_listing_or_website"),
        ):
            if not rec[dst] and (c.get(src) or "").strip():
                rec[dst] = (c.get(src) or "").strip()
        ct = (c.get("company_type") or "").strip()
        # Prefer a state-owned label if any payment row carries one.
        if ct and (not rec["company_type"] or "state" in ct.lower()):
            rec["company_type"] = ct
        if c.get("year"):
            rec["years"].add(str(c["year"]))
    return m


def _resolve_lei(
    client: httpx.Client, *, oc_id: str, name: str, iso: str, sleep: float
) -> tuple[str | None, str, str]:
    """Return (lei, method, confidence). (None, "", "") when unresolved."""
    if oc_id:
        number = oc_id.rstrip("/").split("/")[-1]
        if number:
            try:
                r = client.get(GLEIF_API,
                               params={"filter[entity.registeredAs]": number, "page[size]": 10},
                               headers={"Accept": "application/vnd.api+json", "User-Agent": _UA})
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

    if name:
        try:
            params: dict[str, Any] = {"filter[fulltext]": name, "page[size]": 10}
            if iso:
                params["filter[entity.legalAddress.country]"] = iso
            r = client.get(GLEIF_API, params=params,
                           headers={"Accept": "application/vnd.api+json", "User-Agent": _UA})
            if r.is_success:
                target = _norm_name(name)
                for rec in r.json().get("data", []):
                    ent = rec.get("attributes", {}).get("entity", {})
                    legal = ent.get("legalName", {}).get("name", "")
                    # Only accept a genuine name match. A same-country fulltext
                    # hit whose name doesn't match is very likely a *different*
                    # company, so treat it as unresolved rather than indexing a
                    # low-confidence guess that would render a wrong SOE card.
                    if _norm_name(legal) == target:
                        time.sleep(sleep)
                        return rec.get("id"), "name_country", "medium"
        except Exception as exc:  # noqa: BLE001
            print(f"  ! GLEIF name search error for {name}: {exc}", file=sys.stderr)
        time.sleep(sleep)

    return None, "", ""


def build(limit: int | None, out: Path, sleep: float) -> None:
    with httpx.Client(timeout=90, follow_redirects=True) as client:
        print("Fetching companies table (paginated) …", file=sys.stderr)
        companies = _fetch_all(client, COMPANIES_URL)
        print(f"  {len(companies)} company payment rows", file=sys.stderr)
        print("Fetching SOE List view …", file=sys.stderr)
        soe_list = _fetch_all(client, SOE_LIST_URL)
        print(f"  {len(soe_list)} SOE List rows", file=sys.stderr)

    cmap = _company_map(companies)
    print(f"  {len(cmap)} distinct companies; "
          f"{sum(1 for r in cmap.values() if r['oc'])} carry an opencorporates_id",
          file=sys.stderr)

    # Distinct SOEs from the curated roster.
    roster: dict[str, dict[str, Any]] = {}
    for row in soe_list:
        name = (row.get("SOE") or "").strip()
        if not name:
            continue
        key = _norm_name(name)
        agg = roster.setdefault(key, {"name": name, "country": row.get("Country") or "",
                                      "years": set(), "row": row})
        if row.get("Year"):
            agg["years"].add(str(row["Year"]))

    index: dict[str, dict[str, Any]] = {}
    counts = {"total": 0, "high": 0, "medium": 0, "low": 0, "unresolved": 0,
              "by_oc_id": 0, "by_name": 0, "joined": 0, "with_oc_id": 0}
    types: Counter[str] = Counter()

    items = list(roster.items())
    if limit:
        items = items[:limit]

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for key, agg in items:
            counts["total"] += 1
            comp = cmap.get(key) or {}
            if comp:
                counts["joined"] += 1
            types[comp.get("company_type") or "(no companies match)"] += 1
            name = agg["name"]
            iso = comp.get("iso") or _iso(agg["country"])
            oc_id = comp.get("oc") or ""
            if oc_id:
                counts["with_oc_id"] += 1
            print(f"[{counts['total']}/{len(items)}] {name} ({iso or '??'}) "
                  f"{'oc' if oc_id else '--'} …", file=sys.stderr)
            lei, method, conf = _resolve_lei(
                client, oc_id=oc_id, name=name, iso=iso, sleep=sleep
            )
            if not lei:
                counts["unresolved"] += 1
                continue
            counts[conf] += 1
            counts["by_oc_id" if method == "opencorporates_id" else "by_name"] += 1
            row = agg["row"]
            commodities = comp.get("commodities") or row.get("Commodities") or ""
            index[lei.upper()] = {
                "lei": lei.upper(),
                "match_method": method,
                "match_confidence": conf,
                "soe": {
                    "company_name": name,
                    "country": iso,
                    "iso_alpha2": iso,
                    "sector": comp.get("sector") or row.get("Sector"),
                    "commodities": [s.strip() for s in re.split(r"[;,]", commodities) if s.strip()],
                    "company_type": comp.get("company_type") or "State-owned enterprise",
                    "government_entity": comp.get("government_entity"),
                    "opencorporates_id": oc_id or None,
                    "eiti_id_company": comp.get("eiti_id_company"),
                    "eiti_id_government": comp.get("eiti_id_government"),
                    "audited_financial_statement": (
                        comp.get("afs") or row.get("Audited Financial Statement or Equivalent")
                    ),
                    "public_listing_or_website": (
                        comp.get("listing") or row.get("Public Listing or Website")
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
            "joined_to_companies_table": counts["joined"],
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
    print(f"  SOEs processed        : {m['companies']}", file=sys.stderr)
    print(f"  joined to companies   : {m['joined_to_companies_table']}", file=sys.stderr)
    print(f"  with OpenCorp. id     : {m['with_opencorporates_id']}", file=sys.stderr)
    print(f"  resolved to LEI       : {m['resolved_lei']} "
          f"(high {m['resolved_high']}, medium {m['resolved_medium']}, low {m['resolved_low']})",
          file=sys.stderr)
    print(f"  by OC id / by name    : {m['resolved_by_oc_id']} / {m['resolved_by_name']}",
          file=sys.stderr)
    print(f"  unresolved            : {m['unresolved']}", file=sys.stderr)
    print(f"  company_type of roster: {dict(types)}", file=sys.stderr)
    print(f"  written               : {out}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="cap SOEs processed (debug)")
    ap.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    ap.add_argument("--sleep", type=float, default=0.2, help="pause between GLEIF calls")
    args = ap.parse_args()
    build(args.limit, args.out, args.sleep)


if __name__ == "__main__":
    main()
