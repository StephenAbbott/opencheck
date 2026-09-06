#!/usr/bin/env python3
"""Build the committed EITI SOE → LEI index for the ``eiti_soe`` adapter.

Run this locally (or anywhere the EITI database and the GLEIF API are
reachable). It writes ``backend/opencheck/data/eiti_soe_index.json.gz``.

Repointed at the new global database (Phase 172)
-------------------------------------------------
The roster now comes from ``eiti-database.eiti.org`` → ``view_soeList``, not
from the old ``soe-database.eiti.org`` → ``SOE List`` view. The old host is
**still live** and still reachable with ``--source old``, kept as a fallback
until the new index has proven itself in production; it is not the default and
its numbers are worse in every dimension:

===================  ==================  ====================================
                     old ``SOE List``    new ``view_soeList``
===================  ==================  ====================================
rows                 200                 462
distinct SOEs        125                 **194**
years                2017–2022           2017–**2024**
keys                 UUIDv4, no dedup    UUIDv5, name variants deduplicated
extras               —                   ``country_iso3``, ``sectors``,
                                         ``audited_statement_url``,
                                         ``public_listing_url``,
                                         ``last_updated``
===================  ==================  ====================================

**The v5 deduplication is the quiet win.** 251 distinct ``soe_name`` values
collapse to 194 distinct ``eiti_id_company`` values, so the roster is keyed on
the id and every spelling EITI holds for a company is kept as a name variant —
which is also what is matched against GLEIF, giving the name search more than
one string to try. The old builder keyed on the normalised name and could not
do either.

What this does NOT fix, and the honest number
----------------------------------------------
**LEI resolution goes from 1 to 2.** That is not a typo, and it is not a
matching failure that better code would fix — it was measured before this was
written:

* All 194 SOEs join to ``metadata_companies`` and **every one** has
  ``legal_entity_id``, ``open_corporates_id`` and ``estma_id`` empty. EITI
  publishes no external identifier for a single state-owned enterprise, so
  there is no reverse-lookup path at all — the old builder's
  ``opencorporates_id`` → GLEIF ``registeredAs`` route has nothing to run on
  and has been removed rather than left in as decoration.
* Name+country search against GLEIF returns **no candidate whatsoever** for 183
  of the 194. Re-running the 40 worst of those *without* the country filter
  produced exactly one extra candidate, and it was wrong (Tanzania's State
  Mining Corporation → a Maharashtra company). The country filter is not what
  is limiting this; these companies do not have LEIs.

So the phase ships on the roster — 1.55× the companies, two years fresher,
deduplicated, with countries and URLs — and says plainly that LEI resolution
did not materially improve. See the "Repoint EITI SOE index" ticket, which set
that success criterion up front, before the number was known.

Matching rules
--------------
Exact normalised-name equality against **any** of EITI's spellings, scoped to
the company's country, confidence ``medium`` and never higher. Two GLEIF filters
are merged and deduplicated by LEI — ``filter[fulltext]`` and
``filter[entity.legalName]`` return different things and each misses heads the
other finds (the lesson from ``build_eiti_assessment_index.py``, where merging
took exact matches from 35 to 51).

There is deliberately no fuzzy tier. A same-country hit whose name does not
match is very likely a *different* company, and the assessment builder's review
gate caught `Teck` → TECK GmbH, an unrelated German company, passing as a plain
exact match — so a *fuzzy* match with nobody reviewing it would be worse than
useless on a card that says "state-owned enterprise".

Datasette 1.0-alpha constraints (see CLAUDE.md)
-----------------------------------------------
* SQL is at ``/eiti_database/-/query.json?sql=``; the legacy
  ``/eiti_database.json?sql=`` **302-redirects** there, so ``follow_redirects``
  is mandatory or the client silently gets nothing.
* ``sql_time_limit_ms`` bites on the wide views. ``select count(*) from
  view_payments_detailed`` is fine (0.6 s) and so is a filtered
  ``view_soeList``, but joining ``view_payments_detailed`` to ``view_soeList``
  on *name* times out at 2.5 s while joining on ``eiti_id_company`` takes
  0.7 s. Join on ids, filter early, and never ask a wide view to scan.
* 1,000-row page cap. This builder pages with LIMIT/OFFSET and then asserts the
  harvested row count against ``count(*)`` — the same guard as the assessment
  builder, and the reason an earlier SOE build could lose 5,156 of its 5,332
  companies without saying anything.

Usage::

    python3 scripts/build_eiti_soe_index.py [--source new|old] [--limit N]
                                            [--out PATH] [--sleep S]
"""

from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import json
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any

# --- endpoints ---------------------------------------------------------------

NEW_QUERY_URL = "https://eiti-database.eiti.org/eiti_database/-/query.json"
OLD_BASE = "https://soe-database.eiti.org/eiti_database"
# Datasette encodes the space in "SOE List" as ``~20``. ``SOE%20List`` returns
# **404** on that host. Do not "fix" this to a percent-encoding.
OLD_SOE_LIST_URL = f"{OLD_BASE}/SOE~20List.json"
GLEIF_API = "https://api.gleif.org/api/v1/lei-records"

# Browser-like UA — the bare httpx UA can trip the host's Cloudflare challenge.
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 opencheck-eiti-soe-index"
)

_DEFAULT_OUT = (
    Path(__file__).resolve().parent.parent / "opencheck" / "data" / "eiti_soe_index.json.gz"
)

_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")

#: Values EITI writes into a column that means "we have nothing here". Treated
#: as absent rather than shown to a reader as a URL or an identifier.
_EMPTY = {"", "not available", "n/v", "n/a", "none", "null"}


def _norm_name(value: str) -> str:
    """Lowercase, unaccent, strip punctuation, collapse whitespace.

    Legal-form suffixes are deliberately **not** stripped. Stripping them makes
    a group head exactly equal to its own subsidiary and to unrelated companies
    (``Glencore`` → GLENCORE AG, ``Teck`` → TECK GmbH), and this comparison is
    the only thing standing between a state-ownership card and the wrong
    company.
    """
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    return _WS_RE.sub(" ", _NON_ALNUM_RE.sub(" ", text.lower())).strip()


def _clean(value: Any) -> str:
    """A column value, or "" when EITI's way of saying nothing is in it."""
    text = str(value or "").strip()
    return "" if text.lower() in _EMPTY else text


# --- ISO 3166 alpha-3 → alpha-2, for the countries this roster actually uses --
#
# A literal map rather than a dependency: the builder must run offline-ish on a
# laptop, and `pycountry` is not in the backend's dependency set.
_ISO3_TO_ISO2 = {
    "AFG": "AF", "ALB": "AL", "ARG": "AR", "ARM": "AM", "BFA": "BF", "CMR": "CM",
    "CAF": "CF", "TCD": "TD", "COL": "CO", "COG": "CG", "COD": "CD", "CIV": "CI",
    "DOM": "DO", "ECU": "EC", "SLV": "SV", "ETH": "ET", "GAB": "GA", "DEU": "DE",
    "GHA": "GH", "GTM": "GT", "GIN": "GN", "GUY": "GY", "HND": "HN", "IDN": "ID",
    "IRQ": "IQ", "KAZ": "KZ", "KGZ": "KG", "LBR": "LR", "MDG": "MG", "MWI": "MW",
    "MLI": "ML", "MRT": "MR", "MEX": "MX", "MNG": "MN", "MOZ": "MZ", "MMR": "MM",
    "NLD": "NL", "NER": "NE", "NGA": "NG", "NOR": "NO", "PNG": "PG", "PER": "PE",
    "PHL": "PH", "STP": "ST", "SEN": "SN", "SYC": "SC", "SLE": "SL", "SUR": "SR",
    "TZA": "TZ", "THA": "TH", "TLS": "TL", "TGO": "TG", "TTO": "TT", "UGA": "UG",
    "UKR": "UA", "GBR": "GB", "USA": "US", "YEM": "YE", "ZMB": "ZM", "AZE": "AZ",
    "BOL": "BO", "COD_": "CD", "GNQ": "GQ", "KEN": "KE", "LKA": "LK", "MDA": "MD",
    "NAM": "NA", "PAK": "PK", "PRY": "PY", "RUS": "RU", "SSD": "SS", "SDN": "SD",
    "TJK": "TJ", "TUN": "TN", "UZB": "UZ", "VEN": "VE", "ZWE": "ZW", "CHL": "CL",
    "BRA": "BR", "AUS": "AU", "CAN": "CA", "FRA": "FR", "CHE": "CH", "ITA": "IT",
    "ESP": "ES", "JPN": "JP", "CHN": "CN", "IND": "IN", "ZAF": "ZA", "TUR": "TR",
}


def _iso2(iso3: str) -> str:
    return _ISO3_TO_ISO2.get((iso3 or "").strip().upper(), "")


# --- the new database --------------------------------------------------------


def _sql(client: Any, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """One SQL query against the new database's Datasette endpoint."""
    query: dict[str, Any] = {"sql": sql, "_shape": "objects"}
    query.update(params or {})
    resp = client.get(
        NEW_QUERY_URL,
        params=query,
        headers={"Accept": "application/json", "User-Agent": _UA},
    )
    if resp.status_code == 400:
        # Datasette reports a timeout as a 400 with the reason in the body. Say
        # which query, or the next person re-derives the wide-view rule.
        raise RuntimeError(f"query refused: {resp.json().get('error')}\n  sql: {sql.strip()[:200]}")
    resp.raise_for_status()
    body = resp.json()
    if isinstance(body, dict) and body.get("error"):
        raise RuntimeError(f"query error: {body['error']}")
    return body.get("rows", []) if isinstance(body, dict) else body


def _fetch_all(client: Any, table: str, columns: str, sleep: float) -> list[dict[str, Any]]:
    """Page a whole table, then assert the row count matches ``count(*)``.

    The page cap is a server setting, not a contract. Harvesting until a short
    page arrives silently accepts whatever the server felt like sending; this
    checks the answer against the count the server itself reports.
    """
    expected = _sql(client, f"select count(*) as n from {table}")[0]["n"]
    rows: list[dict[str, Any]] = []
    page_size = 1000
    while True:
        page = _sql(
            client,
            f"select {columns} from {table} limit {page_size} offset {len(rows)}",
        )
        rows.extend(page)
        if len(page) < page_size:
            break
        time.sleep(sleep)
    if len(rows) != expected:
        raise RuntimeError(
            f"{table}: harvested {len(rows)} rows but count(*) says {expected}"
        )
    return rows


def _roster_new(client: Any, sleep: float) -> dict[str, dict[str, Any]]:
    """The SOE roster from ``view_soeList``, keyed on ``eiti_id_company``."""
    rows = _fetch_all(
        client,
        "view_soeList",
        "eiti_id_company, soe_name, country_iso3, country_name, sectors, year, "
        "audited_statement_url, public_listing_url",
        sleep,
    )
    roster: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _clean(row.get("eiti_id_company"))
        name = _clean(row.get("soe_name"))
        if not key or not name:
            continue
        agg = roster.setdefault(
            key,
            {
                "eiti_id_company": key,
                "names": [],
                "iso3": _clean(row.get("country_iso3")),
                "country_name": _clean(row.get("country_name")),
                "sectors": [],
                "years": set(),
                "afs": "",
                "listing": "",
            },
        )
        # Every spelling EITI holds. The first is the display name; all of them
        # are matched against GLEIF, which is the point of keying on the id.
        if name not in agg["names"]:
            agg["names"].append(name)
        sector = _clean(row.get("sectors"))
        if sector and sector not in agg["sectors"]:
            agg["sectors"].append(sector)
        if row.get("year"):
            agg["years"].add(str(row["year"]))
        for column, field in (("audited_statement_url", "afs"), ("public_listing_url", "listing")):
            value = _clean(row.get(column))
            if value and not agg[field]:
                agg[field] = value
    for agg in roster.values():
        agg["years"] = sorted(agg["years"])
    return roster


def _roster_old(client: Any, sleep: float) -> dict[str, dict[str, Any]]:
    """The old host's roster, kept as a fallback. Keyed on the normalised name,
    because the old view has no usable id — its ``eiti_id_company`` is a UUIDv4
    that the new database does not recognise."""
    rows: list[dict[str, Any]] = []
    nxt: str | None = None
    while True:
        params: dict[str, Any] = {"_size": 1000}
        if nxt:
            params["_next"] = nxt
        resp = client.get(
            OLD_SOE_LIST_URL,
            params=params,
            headers={"Accept": "application/json", "User-Agent": _UA},
        )
        resp.raise_for_status()
        body = resp.json()
        if isinstance(body, list):
            rows.extend(body)
            break
        cols = body.get("columns")
        for row in body.get("rows") or []:
            rows.append(row if isinstance(row, dict) else dict(zip(cols, row)))
        nxt = body.get("next")
        if not nxt:
            break
        time.sleep(sleep)

    roster: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = _clean(row.get("SOE"))
        if not name:
            continue
        key = _norm_name(name)
        agg = roster.setdefault(
            key,
            {
                "eiti_id_company": "",
                "names": [],
                "iso3": "",
                "country_name": _clean(row.get("Country")),
                "sectors": [],
                "years": set(),
                "afs": _clean(row.get("Audited Financial Statement or Equivalent")),
                "listing": _clean(row.get("Public Listing or Website")),
            },
        )
        if name not in agg["names"]:
            agg["names"].append(name)
        sector = _clean(row.get("Sector"))
        if sector and sector not in agg["sectors"]:
            agg["sectors"].append(sector)
        if row.get("Year"):
            agg["years"].add(str(row["Year"]))
    for agg in roster.values():
        agg["years"] = sorted(agg["years"])
    return roster


# --- GLEIF -------------------------------------------------------------------


def _gleif_candidates(client: Any, name: str, iso2: str, sleep: float) -> dict[str, str]:
    """LEI → legal name, merged across both GLEIF name filters.

    ``filter[fulltext]`` and ``filter[entity.legalName]`` genuinely disagree:
    each finds heads the other misses. Merging them took the assessment
    builder's exact matches from 35 to 51, and costs one extra request.
    """
    found: dict[str, str] = {}
    for field in ("filter[fulltext]", "filter[entity.legalName]"):
        params: dict[str, Any] = {field: name, "page[size]": 10}
        if iso2:
            params["filter[entity.legalAddress.country]"] = iso2
        try:
            resp = client.get(
                GLEIF_API,
                params=params,
                headers={"Accept": "application/vnd.api+json", "User-Agent": _UA},
            )
            if resp.is_success:
                for record in resp.json().get("data", []):
                    entity = record.get("attributes", {}).get("entity", {})
                    found[record["id"]] = entity.get("legalName", {}).get("name", "")
        except Exception as exc:  # noqa: BLE001
            print(f"  ! GLEIF error for {name!r}: {exc}", file=sys.stderr)
        time.sleep(sleep)
    return found


def _resolve(
    client: Any, agg: dict[str, Any], sleep: float
) -> tuple[str, str, str, bool]:
    """``(lei, gleif_legal_name, matched_eiti_name, saw_any_candidate)``.

    ``saw_any_candidate`` is reported even when nothing matched, because it is
    the statistic that separates "our matching is weak" from "this company has
    no LEI" — and this index needs to be able to say which.
    """
    iso2 = _iso2(agg["iso3"])
    by_key = {_norm_name(n): n for n in agg["names"]}
    candidates: dict[str, str] = {}
    # Two spellings is enough — the third is almost always a punctuation variant
    # of the first and costs two more requests to learn that.
    for name in agg["names"][:2]:
        candidates.update(_gleif_candidates(client, name, iso2, sleep))
    for lei, legal in candidates.items():
        matched = by_key.get(_norm_name(legal))
        if matched:
            return lei.upper(), legal, matched, True
    return "", "", "", bool(candidates)


# --- build -------------------------------------------------------------------


def build(source: str, limit: int | None, out: Path, sleep: float) -> None:
    import httpx  # imported here so --help works without the dependency

    with httpx.Client(timeout=90, follow_redirects=True) as client:
        print(f"Fetching the SOE roster ({source} database) …", file=sys.stderr)
        roster = _roster_new(client, sleep) if source == "new" else _roster_old(client, sleep)

    total_names = sum(len(a["names"]) for a in roster.values())
    variants = sum(1 for a in roster.values() if len(a["names"]) > 1)
    print(
        f"  {len(roster)} distinct SOEs from {total_names} names "
        f"({variants} carry more than one spelling)",
        file=sys.stderr,
    )

    items = list(roster.items())
    if limit:
        items = items[:limit]

    index: dict[str, dict[str, Any]] = {}
    unresolved: list[str] = []
    no_candidates = 0

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for n, (_key, agg) in enumerate(items, 1):
            name = agg["names"][0]
            print(f"[{n}/{len(items)}] {name} ({agg['iso3'] or '??'}) …", file=sys.stderr)
            lei, legal, matched_name, saw_candidates = _resolve(client, agg, sleep)
            if not lei:
                unresolved.append(name)
                if not saw_candidates:
                    no_candidates += 1
                continue
            # The display name is the spelling that actually matched, not
            # whichever row EITI happened to return first — every variant is
            # kept below, so nothing is lost by preferring the one that is
            # corroborated by a GLEIF legal name.
            name = matched_name
            iso2 = _iso2(agg["iso3"])
            index[lei] = {
                "lei": lei,
                "match_method": "gleif_name_exact",
                # Never "high". Nothing here is corroborated by an identifier
                # either side publishes — only two strings that agree.
                "match_confidence": "medium",
                "gleif_legal_name": legal,
                "soe": {
                    "company_name": name,
                    "name_variants": agg["names"],
                    "country": iso2 or agg["iso3"],
                    "iso_alpha2": iso2,
                    "country_name": agg["country_name"],
                    "sector": "; ".join(agg["sectors"]) or None,
                    "commodities": [],
                    "company_type": "State-owned enterprise",
                    "government_entity": None,
                    # Carried for the live payments query only — see the note in
                    # the adapter. NOT asserted as an identifier anywhere.
                    "eiti_id_company": agg["eiti_id_company"] or None,
                    "eiti_id_government": None,
                    "opencorporates_id": None,
                    "audited_financial_statement": agg["afs"] or None,
                    "public_listing_or_website": agg["listing"] or None,
                    "years": agg["years"],
                    "soe_list": True,
                },
            }

    years = sorted({y for a in roster.values() for y in a["years"]})
    payload = {
        "meta": {
            "built": _dt.date.today().isoformat(),
            "source": (
                "https://eiti-database.eiti.org/eiti_database (view_soeList)"
                if source == "new"
                else OLD_BASE
            ),
            "source_snapshot": _dt.date.today().isoformat(),
            "companies": len(items),
            "name_variants_collapsed": total_names - len(roster),
            "years": f"{years[0]}–{years[-1]}" if years else "",
            "with_published_identifier": 0,
            "resolved_lei": len(index),
            "resolved_high": 0,
            "resolved_medium": len(index),
            "resolved_low": 0,
            "unresolved": len(unresolved),
            "no_gleif_candidate": no_candidates,
            "resolution_note": (
                "EITI publishes no LEI, OpenCorporates id or ESTMA id for any of "
                "the state-owned enterprises, so every match here is a "
                "name-and-country match against GLEIF, graded medium and never "
                "higher. Coverage is low because these companies do not hold "
                "LEIs, not because the matching is weak — measured 2026-09-06."
            ),
            "license": "EITI open data (free reuse with attribution)",
            "attribution": "EITI International Secretariat, eiti.org",
        },
        "index": index,
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    meta = payload["meta"]
    print("\n=== EITI SOE index coverage ===", file=sys.stderr)
    print(f"  source                : {meta['source']}", file=sys.stderr)
    print(f"  SOEs processed        : {meta['companies']}", file=sys.stderr)
    print(f"  name variants merged  : {meta['name_variants_collapsed']}", file=sys.stderr)
    print(f"  reporting years       : {meta['years']}", file=sys.stderr)
    print(f"  resolved to LEI       : {meta['resolved_lei']} (all medium)", file=sys.stderr)
    print(f"  unresolved            : {meta['unresolved']}", file=sys.stderr)
    for lei, rec in index.items():
        print(f"    {lei}  {rec['soe']['company_name']}  →  {rec['gleif_legal_name']}",
              file=sys.stderr)
    print(f"  written               : {out} ({out.stat().st_size:,} bytes)", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--source",
        choices=("new", "old"),
        default="new",
        help="which EITI database to read the roster from (default: new)",
    )
    ap.add_argument("--limit", type=int, default=None, help="cap SOEs processed (debug)")
    ap.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    ap.add_argument("--sleep", type=float, default=0.12, help="pause between GLEIF calls")
    args = ap.parse_args()
    build(args.source, args.limit, args.out, args.sleep)


if __name__ == "__main__":
    main()
