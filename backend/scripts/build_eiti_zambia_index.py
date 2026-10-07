#!/usr/bin/env python3
"""Build the committed, LEI-keyed index for the Zambia EITI data portal.

The Zambia EITI (ZEITI) "Fusion Portal" at https://portal.zambiaeiti.org/
publishes Zambia Revenue Authority (ZRA) tax receipts, the EITI reconciliation
payment report, company employment figures, the mining-rights cadastre and a
Water Resources Management Authority (WARMA) offences list, through a keyless
JSON API at ``/api/public/v1``. This script turns the slice of it that can be
tied to an LEI into ``opencheck/data/eiti_zambia_index.json.gz``.

The join problem, and how it is solved
--------------------------------------
Every portal row is keyed on the ZRA **Taxpayer Identification Number
(TPIN)**. GLEIF's Zambian records carry the **PACRA registration number** in
``registeredAs`` instead, and nothing on the portal relates the two. So the
link is made by **name**, once, offline:

1. ``harvest`` snapshots every GLEIF LEI record with jurisdiction ``ZM``
   (69 on 7 October 2026 — small enough to take whole).
2. For each one, the four ZRA tables that file full legal names
   (``NAME_DATASETS``) are searched, and a row matches when its normalised
   name equals the normalised GLEIF legal name or one of GLEIF's other names.
   The matching rows give the company's TPIN.
3. Every other table is then joined **on the TPIN, never the name**. This
   matters: the EITI reconciliation report files "C.C.S", "KCM" and "CNMC"
   rather than legal names, so a name join would miss most of it.
4. The two tables that carry no TPIN at all — the mining-rights cadastre and
   the WARMA offences list — are joined on name, against the GLEIF names, every
   name the company files under its TPIN, and the short reviewed alias list in
   ``NAME_ALIASES``.

Scope — Stephen's decision of 7 October 2026: option A, the LEIs that can be
matched by name. A company with no name match is not in the index at all.
Lafarge Cement Zambia Plc is the known case: its LEI name matches nothing in
the ZRA tables (the company trades as Chilanga Cement Plc), and the only rows
filed under the Lafarge name carry a malformed TPIN ("1123"). Joining it to
Chilanga Cement's TPIN would be an inference about a rename, not a name match,
so it is left out.

Which tables, and why these
---------------------------
The portal's 69 datasets overlap, and two of them are the same 11,403 rows
uploaded twice (``zra-tax-payment-in-2023-detailed-service-values`` and
``mineral-royalty-receipts-declared-by-zra-in-2023-detailed-service-values``
sum to the same kwacha for every company checked). Summing across tables would
double-count, so the index never does. ZRA tax receipts are taken **one table
per payment year** (``ZRA_TAX_YEARS``); the EITI reconciliation report is kept
separately because it is the companies' own disclosure, not ZRA's receipts.

Two traps the build handles:

* ``zra-tax-revenue-2024`` ("Amount paid (KMW)") has a blank amount on most
  rows, so its amounts are never summed. It is used only to count a company's
  2024 payment records where ``zra-revenue-payments-2024`` (clean, in ZMW
  millions) does not list the company — which is the case for every small
  company in the index.
* The rights cadastre uses ``01/01/2023`` as an empty-date placeholder
  (pending applications carry it as both grant and expiry date, and active
  licences carry it as an expiry before their grant). It is dropped.

What is never stored: individuals. The rights cadastre lists natural persons
as holders; rows are only ever kept when they match a company already
resolved to an LEI, so no individual's row enters the artifact.

Licence: ZEITI's open data policy (2016) adopts the Open Definition — data
"freely used, modified, and shared by anyone for any purpose" — and the 2026
EITI Validation scores Zambia "Very good" on Requirement 7.2, noting the policy
permits unrestricted publication and use. Neither names a specific licence, so
OpenCheck cites the policy itself.

Usage::

    python3 -m scripts.build_eiti_zambia_index harvest   # network; rewrites the raw file
    python3 -m scripts.build_eiti_zambia_index build     # offline, deterministic

``build`` is offline and deterministic given the committed raw artifact.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
RAW = DATA / "eiti_zambia_raw.json.gz"
OUT_INDEX = DATA / "eiti_zambia_index.json.gz"

PORTAL = "https://portal.zambiaeiti.org"
API = f"{PORTAL}/api/public/v1"
GLEIF_API = "https://api.gleif.org/api/v1/lei-records"
POLICY_URL = "https://eiti.org/sites/default/files/attachments/zambia_open_data_policy.pdf"

_UA = {"User-Agent": "OpenCheck eiti_zambia harvester (https://opencheck.world)"}
_PAGE = 200  # the portal's maximum page size

# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

#: ZRA tables that file a company's full legal name beside its TPIN — the only
#: ones the name→TPIN step searches.
NAME_DATASETS: dict[str, tuple[str, str]] = {
    # code: (TPIN column, name column)
    "revenue-payments-eiti-2022-2023": ("TPIN", "Full Name"),
    "zra-tax-payment-in-2023-detailed-service-values": ("TPIN", "Company Name"),
    "zra-revenue-payments-2024": ("TPIN", "Company Name"),
    "zra-tax-revenue-2024": ("TPIN", "Company Name"),
}

#: Tables joined on the TPIN once it is known.
TPIN_DATASETS: dict[str, dict[str, str]] = {
    "payment-report": {"tpin": "TPIN", "name": "Company Name"},
    "revenue-payments-eiti-2022-2023": {"tpin": "TPIN", "name": "Full Name"},
    "zra-tax-payment-in-2023-detailed-service-values": {"tpin": "TPIN", "name": "Company Name"},
    "zra-revenue-payments-2024": {"tpin": "TPIN", "name": "Company Name"},
    "zra-tax-revenue-2024": {"tpin": "TPIN", "name": "Company Name"},
    "company-employment-data-in-2022": {"tpin": "TPIN", "name": "Name of company"},
    "company-employment-data-in-2023": {"tpin": "TPIN", "name": "Name of company"},
}

#: Tables with no TPIN column, joined on name.
NAME_ONLY_DATASETS: dict[str, str] = {
    "mining-and-non-mining-rights-2023-2025-q2": "Company Name",
    "mining-companies-offences": "Client Name",
}

#: ZRA tax receipts, one table per payment year so nothing is counted twice.
#: (year, dataset, date column, amount column, multiplier to kwacha, tax-type column)
#: ``amount`` None = count records only (see the module docstring).
ZRA_TAX_YEARS: list[dict[str, Any]] = [
    {"year": "2022", "dataset": "revenue-payments-eiti-2022-2023",
     "date": "Payment Date", "amount": "Amount", "scale": 1, "type": "Tax Type"},
    {"year": "2023", "dataset": "zra-tax-payment-in-2023-detailed-service-values",
     "date": "Payment Date", "amount": "Amount (ZMW)", "scale": 1, "type": "Tax Type"},
    {"year": "2024", "dataset": "zra-revenue-payments-2024",
     "date": "Compilation Date", "amount": "ZMW (Millions)", "scale": 1_000_000, "type": "Tax Type"},
    # Fallback for 2024 only — used when the clean 2024 table does not list
    # the company. Amounts are blank on most rows, so records are counted.
    {"year": "2024", "dataset": "zra-tax-revenue-2024",
     "date": "Date", "amount": None, "scale": 1, "type": "Tax Type", "fallback": True},
]

#: Reviewed name variants for the two name-only tables. Each entry is a name
#: as the table files it, attached to one LEI after checking it by hand
#: (7 October 2026). Keep this short: every entry is a judgment, not a match.
NAME_ALIASES: dict[str, list[str]] = {
    # ZCCM Investments Holdings Plc — the cadastre files "ZCCM - IH ...".
    "5493005OY00M9G3XSY51": ["ZCCM - IH Investments Holdings Plc"],
    # Maamba Collieries Limited — the cadastre misspells it "Colieries".
    "213800ATOLDC9CX44W14": ["Maamba Colieries Limited"],
    # Chambishi Copper Smelter Limited — WARMA files "Chambishi Copper Smelters".
    "9845008756E7B43CDE48": ["Chambishi Copper Smelters"],
}

#: The cadastre's empty-date placeholder.
_PLACEHOLDER_DATE = "01/01/2023"

# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

_SHARE_RE = re.compile(r"\(\s*(\d+(?:\.\d+)?)\s*%\s*\)")
_LEGAL_FORMS = {"LIMITED", "LTD", "LIMTED", "PLC", "CO", "COMPANY", "THE"}


def holder_share(name: str) -> float | None:
    """The ``(100%)`` holding share the cadastre appends to a holder's name."""
    m = _SHARE_RE.search(name or "")
    return float(m.group(1)) if m else None


def norm_name(name: str) -> str:
    """Normalise a company name for equality matching.

    Strips the cadastre's ``(NN%)`` share, a parenthesised ``(Z)`` /
    ``(ZAMBIA)``, punctuation and legal-form words. Bare ``ZAMBIA`` is kept: it
    distinguishes "X Zambia Ltd" from "X Ltd", which are different companies.
    """
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    s = _SHARE_RE.sub(" ", s.upper())
    s = re.sub(r"\(\s*(Z|ZAMBIA)\s*\)", " ", s)
    s = s.replace("&", " AND ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    tokens = [t for t in s.split() if t not in _LEGAL_FORMS]
    return " ".join(tokens)


def base_tpin(value: Any) -> str | None:
    """The 10-digit TPIN at the start of a cell, or None.

    One payment-report row files ``1001831030/62959`` (TPIN plus the old PACRA
    number); the TPIN is the part before the slash.
    """
    m = re.match(r"\s*(\d{10})(?!\d)", str(value or ""))
    return m.group(1) if m else None


def _search_terms(names: list[str]) -> list[str]:
    """Portal search strings for a list of names.

    The portal's ``search`` is a case-insensitive substring match over the raw
    cell text, so a term must appear verbatim: "ZCCM IH" would miss
    "ZCCM - IH Investments". The term is the first two words exactly as the
    name writes them, separator included. A single word ("ZAMBIA") would page
    through thousands of rows. The results are filtered by exact normalised
    name afterwards, so the term only bounds the search, never decides a match.
    """
    out: list[str] = []
    for n in names:
        m = re.match(r"\s*([A-Za-z0-9]+)(\W+[A-Za-z0-9]+)?", n or "")
        if not m:
            continue
        term = m.group(0).strip()
        if term not in out:
            out.append(term)
    return out


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _get_json(url: str, *, accept: str = "application/json") -> Any:
    last: Exception | None = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={**_UA, "Accept": accept})
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


#: A search returning more rows than this is too broad to be a company's own
#: rows; it is skipped with a warning rather than paged through.
_MAX_SEARCH_ROWS = 3000


def _rows(code: str, search: str) -> list[dict[str, Any]]:
    """Every row of a portal table matching a search string (all pages)."""
    out: list[dict[str, Any]] = []
    start = 1
    while True:
        q = urllib.parse.urlencode({"search": search, "from": start, "size": _PAGE})
        page = _get_json(f"{API}/datasets/{code}/rows?{q}")
        if start == 1 and int(page.get("total") or 0) > _MAX_SEARCH_ROWS:
            print(f"    skip {code} search={search!r}: {page.get('total')} rows", file=sys.stderr)
            return []
        rows = page.get("rows") or []
        out.extend(rows)
        if not rows or int(page.get("to") or 0) >= int(page.get("total") or 0):
            return out
        start = int(page["to"]) + 1
        time.sleep(0.2)


def _write_gz(path: Path, payload: dict[str, Any]) -> None:
    """Write deterministic gzip (no timestamp in the header)."""
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    path.write_bytes(buf.getvalue())


def _read_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# harvest (network)
# ---------------------------------------------------------------------------


def _gleif_zm() -> list[dict[str, Any]]:
    """Every GLEIF LEI record with jurisdiction ZM, trimmed to what matching needs."""
    q = urllib.parse.urlencode({"filter[entity.jurisdiction]": "ZM", "page[size]": 200})
    data = _get_json(f"{GLEIF_API}?{q}", accept="application/vnd.api+json")
    out = []
    for r in data.get("data") or []:
        e = r["attributes"]["entity"]
        out.append({
            "lei": r["id"],
            "legal_name": e["legalName"]["name"],
            "other_names": [o["name"] for o in e.get("otherNames") or []],
            "registered_at": (e.get("registeredAt") or {}).get("id"),
            "registered_as": e.get("registeredAs"),
            "entity_status": e.get("status"),
            "registration_status": r["attributes"]["registration"]["status"],
        })
    return sorted(out, key=lambda x: x["lei"])


def harvest() -> None:
    harvested = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    gleif = _gleif_zm()
    print(f"GLEIF: {len(gleif)} ZM LEI records", file=sys.stderr)

    wanted = set(NAME_DATASETS) | set(TPIN_DATASETS) | set(NAME_ONLY_DATASETS)
    datasets = {
        d["code"]: {
            "name": d.get("name"),
            "service": (d.get("service") or {}).get("name"),
            "row_count": d.get("rowCount"),
            "period": d.get("periodLabel") or None,
            "last_updated": d.get("lastUpdated") or None,
        }
        for d in _get_json(f"{API}/datasets")
        if d.get("code") in wanted
    }
    missing = wanted - set(datasets)
    if missing:
        raise SystemExit(f"portal no longer lists: {sorted(missing)}")

    matches: dict[str, Any] = {}
    for g in gleif:
        names = [g["legal_name"], *g["other_names"]]
        targets = {norm_name(n) for n in names if norm_name(n)}
        # 1. name -> TPIN, on the ZRA tables that file legal names
        tpins: Counter[str] = Counter()
        for code, (tc, nc) in NAME_DATASETS.items():
            for term in _search_terms(names):
                for row in _rows(code, term):
                    if norm_name(str(row.get(nc) or "")) in targets:
                        t = base_tpin(row.get(tc))
                        if t:
                            tpins[t] += 1
        # 2. TPIN -> every TPIN-keyed table
        tpin_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        filed_names: set[str] = set()
        for t in sorted(tpins):
            for code, cols in TPIN_DATASETS.items():
                for row in _rows(code, t):
                    if base_tpin(row.get(cols["tpin"])) == t:
                        tpin_rows[code].append(row)
                        filed_names.add(str(row.get(cols["name"]) or "").strip())
        # 3. name-only tables, on GLEIF names + names filed under the TPIN + aliases
        aliases = NAME_ALIASES.get(g["lei"], [])
        name_targets = targets | {norm_name(n) for n in filed_names | set(aliases) if norm_name(n)}
        name_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for code, nc in NAME_ONLY_DATASETS.items():
            seen: set[str] = set()
            for term in _search_terms(names + aliases):
                for row in _rows(code, term):
                    key = json.dumps(row, sort_keys=True)
                    if key in seen:
                        continue
                    if norm_name(str(row.get(nc) or "")) in name_targets:
                        seen.add(key)
                        name_rows[code].append(row)
        if tpins or name_rows:
            matches[g["lei"]] = {
                "tpins": dict(tpins),
                "aliases": aliases,
                "tpin_rows": dict(tpin_rows),
                "name_rows": dict(name_rows),
            }
            print(f"  {g['lei']} {g['legal_name']}: TPINs {sorted(tpins)}; "
                  f"{sum(map(len, tpin_rows.values()))} TPIN rows, "
                  f"{sum(map(len, name_rows.values()))} name rows", file=sys.stderr)

    _write_gz(RAW, {
        "meta": {"harvested": harvested, "portal": PORTAL, "datasets": datasets},
        "gleif": gleif,
        "matches": matches,
    })
    print(f"wrote {RAW} ({len(matches)} LEIs matched)", file=sys.stderr)


# ---------------------------------------------------------------------------
# build (offline, deterministic)
# ---------------------------------------------------------------------------


def _year(date: Any) -> str | None:
    """The year of a portal ``dd/mm/yyyy`` date."""
    m = re.search(r"(\d{4})\s*$", str(date or ""))
    return m.group(1) if m else None


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _clean_label(value: Any) -> str:
    """Portal labels carry U+FFFD where a non-breaking space was mis-encoded."""
    return re.sub(r"\s+", " ", str(value or "").replace("\ufffd", " ")).strip()


def _round(v: float) -> float:
    return round(v, 2)


def _zra_tax(tpin_rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    covered: set[str] = set()
    for cfg in ZRA_TAX_YEARS:
        year = cfg["year"]
        if cfg.get("fallback") and year in covered:
            continue
        rows = [r for r in tpin_rows.get(cfg["dataset"], []) if _year(r.get(cfg["date"])) == year]
        if not rows:
            continue
        by_type: Counter[str] = Counter()
        counts: Counter[str] = Counter()
        total = 0.0
        for r in rows:
            t = _clean_label(r.get(cfg["type"])) or "Unspecified"
            counts[t] += 1
            if cfg["amount"]:
                v = _num(r.get(cfg["amount"]))
                if v is not None:
                    total += v * cfg["scale"]
                    by_type[t] += v * cfg["scale"]
        entry: dict[str, Any] = {
            "year": year,
            "dataset": cfg["dataset"],
            "payments": len(rows),
            "amounts_summed": bool(cfg["amount"]),
        }
        if cfg["amount"]:
            entry["total_zmw"] = _round(total)
            entry["by_tax_type"] = [
                {"tax_type": k, "zmw": _round(v)}
                for k, v in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0]))
            ]
        else:
            entry["by_tax_type"] = [
                {"tax_type": k, "payments": n}
                for k, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
            ]
        out.append(entry)
        covered.add(year)
    return sorted(out, key=lambda e: e["year"], reverse=True)


def _reconciliation(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The EITI payment report: per reporting year, per receipting entity."""
    by_year: dict[str, dict[str, Any]] = {}
    for r in rows:
        year = _year(r.get("Reporting Date"))
        if not year:
            continue
        y = by_year.setdefault(year, {"zmw": 0.0, "usd": 0.0, "entities": defaultdict(lambda: [0.0, 0.0]), "rows": 0})
        zmw = _num(r.get("Paid in ZMW")) or 0.0
        usd = _num(r.get("Other Payments Made in USD")) or 0.0
        entity = _clean_label(r.get("Receipting Entity")) or "Unspecified"
        y["zmw"] += zmw
        y["usd"] += usd
        y["entities"][entity][0] += zmw
        y["entities"][entity][1] += usd
        y["rows"] += 1
    out = []
    for year, y in by_year.items():
        out.append({
            "year": year,
            "total_zmw": _round(y["zmw"]),
            "total_usd": _round(y["usd"]),
            "lines": y["rows"],
            "by_receiving_entity": [
                {"entity": k, "zmw": _round(v[0]), "usd": _round(v[1])}
                for k, v in sorted(y["entities"].items(), key=lambda kv: (-kv[1][0] - kv[1][1], kv[0]))
                if v[0] or v[1]
            ],
        })
    return sorted(out, key=lambda e: e["year"], reverse=True)


def _employment(tpin_rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for code in ("company-employment-data-in-2023", "company-employment-data-in-2022"):
        for r in tpin_rows.get(code, []):
            year = _year(r.get("Year")) or code.rsplit("-", 1)[-1]
            share = _num(r.get("Share of Women employed"))
            # A row with male and female both 0 filed no gender breakdown
            # (Kansanshi, 2022) — its 0 share is a blank, not "no women".
            if not (_num(r.get("Male")) or _num(r.get("Female"))):
                share = None
            out.append({
                "year": year,
                "dataset": code,
                "employees": int(_num(r.get("No of Employees")) or 0) or None,
                "domestic": int(_num(r.get("Average number of direct domestic employees")) or 0) or None,
                "expatriate": int(_num(r.get("Average number of direct expatriate employees")) or 0) or None,
                "women_share": round(share, 4) if share is not None else None,
            })
    return sorted(out, key=lambda e: e["year"], reverse=True)


def _date_or_none(value: Any) -> str | None:
    v = str(value or "").strip()
    return None if not v or v == _PLACEHOLDER_DATE else v


def _licences(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        holder = str(r.get("Company Name") or "").strip()
        out.append({
            "code": str(r.get("Code") or "").strip(),
            "type": str(r.get("License Type") or "").strip() or None,
            "status": str(r.get("Status") or "").strip() or None,
            "commodities": str(r.get("Commodities") or "").strip() or None,
            "area": str(r.get("Area") or "").strip() or None,
            "location": str(r.get("Map Reference") or "").strip() or None,
            "grant_date": _date_or_none(r.get("Grant Date")),
            "expiry_date": _date_or_none(r.get("Expiry Date")),
            "holder_as_filed": _SHARE_RE.sub("", holder).strip(),
            "holder_share_pct": holder_share(holder),
        })
    return sorted(out, key=lambda x: x["code"])


def _offences(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        out.append({
            "name_as_filed": str(r.get("Client Name") or "").strip(),
            "offence": _clean_label(r.get("Offences")) or None,
            "activity": _clean_label(r.get("Activity")) or None,
            "permit": _clean_label(r.get("Permit")) or None,
        })
    return sorted(out, key=lambda x: (x["offence"] or "", x["activity"] or ""))


def build() -> None:
    raw = _read_gz(RAW)
    meta_in = raw["meta"]
    gleif = {g["lei"]: g for g in raw["gleif"]}
    datasets = meta_in["datasets"]
    index: dict[str, Any] = {}
    for lei, m in sorted(raw["matches"].items()):
        g = gleif[lei]
        tpin_rows = m.get("tpin_rows") or {}
        name_rows = m.get("name_rows") or {}
        tpins = sorted(m.get("tpins") or {})
        filed = sorted({
            str(r.get(TPIN_DATASETS[code]["name"]) or "").strip()
            for code, rs in tpin_rows.items() for r in rs
        } - {""})
        used: set[str] = set(code for code, rs in tpin_rows.items() if rs)
        used |= set(code for code, rs in name_rows.items() if rs)
        record = {
            "lei": lei,
            "gleif_legal_name": g["legal_name"],
            "lei_registration_status": g["registration_status"],
            "tpins": tpins,
            "names_as_filed": filed,
            "match": {
                "method": "tpin_via_name" if tpins else "name_only",
                "confidence": "medium",
                "aliases": m.get("aliases") or [],
            },
            "zra_tax": _zra_tax(tpin_rows),
            "eiti_reconciliation": _reconciliation(tpin_rows.get("payment-report", [])),
            "employment": _employment(tpin_rows),
            "licences": _licences(name_rows.get("mining-and-non-mining-rights-2023-2025-q2", [])),
            "water_offences": _offences(name_rows.get("mining-companies-offences", [])),
            "datasets_used": sorted(used),
        }
        index[lei] = record

    used_all = sorted({c for r in index.values() for c in r["datasets_used"]})
    meta = {
        "built": meta_in["harvested"],
        "portal": PORTAL,
        "api": API,
        "licence": "ZEITI Open Data Policy (2016) — no named licence; see policy",
        "licence_url": POLICY_URL,
        "gleif_zm_records": len(gleif),
        "entities": len(index),
        "datasets": {c: datasets[c] for c in used_all},
    }
    _write_gz(OUT_INDEX, {"meta": meta, "index": index})
    print(f"wrote {OUT_INDEX}: {len(index)} LEIs", file=sys.stderr)
    for lei, r in index.items():
        print(f"  {lei} {r['gleif_legal_name']}: TPIN {','.join(r['tpins']) or '—'}; "
              f"tax years {[e['year'] for e in r['zra_tax']]}; "
              f"EITI report {[e['year'] for e in r['eiti_reconciliation']]}; "
              f"{len(r['licences'])} rights; {len(r['water_offences'])} offences", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["harvest", "build"])
    args = ap.parse_args()
    {"harvest": harvest, "build": build}[args.command]()


if __name__ == "__main__":
    main()
