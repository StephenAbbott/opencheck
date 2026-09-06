#!/usr/bin/env python3
"""Build the committed EITI Company Assessment index for the ``eiti_assessment`` adapter.

The EITI Company Assessment is the *supporting company* half of the new EITI
global database (``eiti-database.eiti.org``, launched 2026). It is the only
ownership-adjacent dataset EITI publishes, and it was absent from the old SOE
database entirely. Two things in it matter to OpenCheck:

* **Expectation 6 — "Company disclose beneficial ownership."** A per-company,
  per-year assessment result (met / partially met / not met / not available)
  with, for the 2023 cohort, links to the disclosure. This is a *beneficial
  ownership transparency posture*, not beneficial ownership data.
* **Expectation 2 — "Company publish a list of controlled subsidiaries."** The
  declared subsidiaries themselves: 1,375 parent→child edges, 1,308 children,
  across 51 implementing countries. 61 companies declared subsidiaries; 97 were
  assessed; the canonicalised union is 99 parents.

Pipeline (three subcommands, run in order)
------------------------------------------
``harvest``
    Pull the three source tables from the live Datasette and write the raw
    artifact. Network. Re-run to refresh.

``resolve``
    Resolve the supporting-company parents to LEIs and write a **review file**
    for a human to check. Network (GLEIF).

``build``
    Read the raw artifact + the reviewed file and write the committed,
    LEI-keyed index. Offline and deterministic.

Why the review gate is not optional
-----------------------------------
There are only ~99 parents, and a name matcher over multinational-enterprise
names produces confident nonsense. Three worked examples, all real:

* "Anglo American" ranks the **ANGLO AMERICAN FOUNDATION** first — the charity.
* "AngloAmerican" ranks **INTERNATIONAL SCHOOL OF TURIN, ANGLO**.
* Stripping legal-form suffixes makes a group head and its own subsidiaries
  *exactly* equal, so "Glencore" matches **GLENCORE AG** (a subsidiary of
  Glencore plc) and "Capricorn Energy" matches **CAPRICORN ENERGY HOLDINGS
  LIMITED**. An exact name match is not a safe match here.

Ninety-nine rows is small enough to read, so ``build`` refuses to write the
index while any row is still marked ``REVIEW``.

Identifier corroboration
------------------------
EITI publishes a ``legal_entity_id`` column, but populated for **3 companies
of 10,116** — so in practice OpenCheck *derives* the LEI here. Per the
corroboration rule in CLAUDE.md the adapter must not assert ``lei`` in
``SourceHit.identifiers``.

Nothing else in this dataset is an identifier either: EITI publishes no id for
the declared subsidiaries, and its ``eiti_id_company`` is a **UUIDv5 over a
name-derived key** — a deduplication key that EITI regenerated wholesale (v4 →
v5) in this release, not a registry number. So the adapter asserts an empty
identifier set. That is the honest outcome, not an oversight.

Datasette 1.0 gotchas (each of these cost real time)
----------------------------------------------------
* SQL lives at ``/eiti_database/-/query.json``. The legacy
  ``/eiti_database.json?sql=`` **302-redirects** there — a client without
  ``follow_redirects`` silently gets nothing.
* ``raw_*`` tables store values **JSON-encoded, quotes included**: a LEI
  arrives as ``'"549300071188HIDJEB11"'``, so ``len()`` is 22, not 20. See
  ``_unjson``.
* ``sql_time_limit_ms`` times out even ``select count(*)`` on the wide views
  (``view_companies``, ``view_commodities``, ``view_countries``). Query the
  narrow tables.
* Use ``raw_company_assessment_subsidiaries`` for the parent→child edges, not
  ``metadata_company_relationships``: the latter's ``country_of_operation_iso3``
  is NULL for every row and its ``assessment_year`` is 0.

Usage::

    python3 scripts/build_eiti_assessment_index.py harvest
    python3 scripts/build_eiti_assessment_index.py resolve
    # ... a human edits the review file, changing REVIEW to accept/reject ...
    python3 scripts/build_eiti_assessment_index.py build

Licence: EITI content-use policy — free republication with credit to
"EITI International Secretariat, eiti.org".
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import gzip
import json
import re
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import httpx

# ``httpx`` is imported inside ``harvest`` and ``resolve`` rather than at module
# scope, because ``build`` is documented as offline and must actually be able to
# run offline — including on a Python that has no HTTP client installed. The
# annotations below are strings (``from __future__ import annotations``), so
# they never evaluate at runtime.

# --------------------------------------------------------------------------
# Endpoints and paths
# --------------------------------------------------------------------------

#: Datasette 1.0 SQL endpoint. The legacy `/eiti_database.json?sql=` 302s here.
QUERY_URL = "https://eiti-database.eiti.org/eiti_database/-/query.json"
GLEIF_API = "https://api.gleif.org/api/v1/lei-records"

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 opencheck-eiti-assessment-index"
)

_ROOT = Path(__file__).resolve().parent.parent
_RAW_PATH = _ROOT / "opencheck" / "data" / "eiti_assessment_raw.json.gz"
_OUT_PATH = _ROOT / "opencheck" / "data" / "eiti_assessment_index.json.gz"
#: The human review file. Lives next to this script, not in the package —
#: it is a working document, not something the adapter reads at runtime.
_REVIEW_PATH = Path(__file__).resolve().parent / "eiti_assessment_review.tsv"

#: Page size for LIMIT/OFFSET paging. Datasette caps a single response at
#: ``max_returned_rows``; paging plus the count assertion in ``_fetch_all``
#: means a raised or lowered cap can never silently truncate a harvest.
_PAGE = 500

ATTRIBUTION = "EITI International Secretariat, eiti.org"
LICENCE = "EITI content-use policy — free republication with attribution"

# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")

#: Legal-form suffixes stripped before comparing names.
_SUFFIXES = (
    "incorporated", "limited", "ltd", "plc", "inc", "corporation", "corp",
    "company", "co", "se", "sa", "spa", "nv", "bv", "ag", "asa", "group",
    "holdings", "holding", "sas", "ab", "llc", "lp", "gmbh", "ltda", "srl",
    "sarl", "pty", "pte", "as", "sac", "saa", "cv", "oyj", "pjsc", "jsc",
)

#: Values EITI writes where a company gave nothing. These are sentinels, not
#: data — an "identifier" of "Not available" is worse than no identifier.
_ABSENT = {"", "not available", "n/v", "na", "n/a", "none", "-", "null"}


def _norm_name(value: str) -> str:
    """Normalised comparison form: lowercase, alphanumeric, no legal suffix."""
    v = (value or "").strip().lower()
    v = _NON_ALNUM_RE.sub(" ", v)
    v = _WS_RE.sub(" ", v).strip()
    parts = v.split()
    while len(parts) > 1 and parts[-1] in _SUFFIXES:
        parts.pop()
    return " ".join(parts)


def _tight(value: str) -> str:
    """``_norm_name`` with spaces removed, for joining across EITI's own tables.

    EITI spells the same supporting company differently in the assessment sheet
    and the subsidiary sheet — "Anglo American" / "AngloAmerican",
    "ArcelorMittal" / "Arcelor Mittal", "Barrick Gold" / "BARRICK". The v5
    deduplication does not collapse these, so without a spaceless join one
    company arrives as two parents and the review file asks a human the same
    question twice.
    """
    return _norm_name(value).replace(" ", "")


def _unjson(value: Any) -> str:
    """Decode a ``raw_*`` table value.

    The ``raw_*`` layer stores every cell as JSON text, quotes included, so a
    LEI arrives as ``'"549300071188HIDJEB11"'`` — 22 characters, which is why a
    naive length check reports zero valid LEIs when there are three. Values in
    the ``resolved_*`` / ``clean_*`` / ``metadata_*`` / ``view_*`` layers are
    already plain, and pass through unchanged.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        return str(value)
    v = value.strip()
    if v[:1] in '"[{' and v[-1:] in '"]}':
        try:
            decoded = json.loads(v)
        except (ValueError, TypeError):
            return v
        if isinstance(decoded, str):
            return decoded.strip()
        if isinstance(decoded, list):
            return ", ".join(str(x) for x in decoded)
        if isinstance(decoded, dict):
            return json.dumps(decoded, ensure_ascii=False)
        return str(decoded)
    return v


def _clean(value: Any) -> str | None:
    """``_unjson`` plus sentinel removal — returns None for 'Not available'."""
    v = _unjson(value)
    return None if v.strip().lower() in _ABSENT else v


def _json_field(value: Any) -> Any:
    """Decode a ``raw_*`` cell that holds a JSON list or object."""
    v = (value or "").strip() if isinstance(value, str) else value
    if not isinstance(v, str) or not v:
        return None
    try:
        return json.loads(v)
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------
# Datasette access
# --------------------------------------------------------------------------


def _query(client: httpx.Client, sql: str) -> list[dict[str, Any]]:
    """Run one SQL statement and return rows as dicts."""
    r = client.get(
        QUERY_URL,
        params={"sql": sql, "_shape": "array"},
        headers={"Accept": "application/json", "User-Agent": _UA},
    )
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict):
        # Datasette reports SQL errors as 400 + {ok: false, error: ...}
        raise RuntimeError(f"Datasette error: {payload.get('error') or payload}")
    return payload


def _fetch_all(client: httpx.Client, table: str, columns: str, sleep: float) -> list[dict]:
    """Page a whole table, then assert the row count matches ``count(*)``.

    Datasette truncates a single response at ``max_returned_rows`` and says so
    only in a field this shape drops, so a harvest that trusted one request
    could silently lose rows — which is exactly how an earlier EITI SOE build
    saw 176 of 5,332 companies. Paging plus the assertion makes that
    impossible to miss.
    """
    expected = _query(client, f"select count(*) as n from {table}")[0]["n"]
    rows: list[dict[str, Any]] = []
    offset = 0
    while offset < expected:
        page = _query(
            client,
            f"select {columns} from {table} limit {_PAGE} offset {offset}",
        )
        if not page:
            break
        rows.extend(page)
        offset += _PAGE
        time.sleep(sleep)
    if len(rows) != expected:
        raise RuntimeError(
            f"{table}: harvested {len(rows)} rows but count(*) says {expected} "
            "— the page loop lost rows, do not build an index from this"
        )
    print(f"  {table}: {len(rows)} rows", file=sys.stderr)
    return rows


# --------------------------------------------------------------------------
# harvest
# --------------------------------------------------------------------------


def harvest(out: Path, sleep: float) -> None:
    """Pull the three Company Assessment tables and write the raw artifact."""
    import httpx

    with httpx.Client(timeout=90, follow_redirects=True) as client:
        print("Harvesting EITI Company Assessment …", file=sys.stderr)

        companies = _fetch_all(
            client,
            "raw_company_assessment_company_reference",
            "company_name, open_corporates_id, legal_entity_id, estma_id, "
            "sectors, headquarters, type, business_activity",
            sleep,
        )
        assessments = _fetch_all(
            client,
            "view_company_assessments_detailed",
            "company_name, company_hq_country, company_hq_country_iso3, "
            "company_hq_city, company_type, sectors, assessment_year, "
            "expectation_shorthand, expectation_label, assessment_result, "
            "response, url, secretariat_comment, evidence_response, evidence_url, "
            "bo_disclosure, bo_url, bo_disclosure_url, stock_exchange, stock_url",
            sleep,
        )
        # Deliberately the raw table: metadata_company_relationships has a NULL
        # country_of_operation_iso3 on every row and assessment_year = 0.
        subsidiaries = _fetch_all(
            client,
            "raw_company_assessment_subsidiaries",
            "eiti_supporting_company, subsidiary_name, eiti_implementing_country, "
            "eiti_report_year, source, comment",
            sleep,
        )

    payload = {
        "meta": {
            "harvested": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "source": "EITI global database — Company Assessment "
                      "(https://eiti-database.eiti.org/eiti_database)",
            "licence": LICENCE,
            "attribution": ATTRIBUTION,
            "counts": {
                "company_reference": len(companies),
                "assessment_rows": len(assessments),
                "subsidiary_rows": len(subsidiaries),
            },
        },
        "companies": companies,
        "assessments": assessments,
        "subsidiaries": subsidiaries,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=True)
    print(f"\nWrote {out} ({out.stat().st_size:,} bytes)", file=sys.stderr)


# --------------------------------------------------------------------------
# resolve
# --------------------------------------------------------------------------


def _canonical_names(raw: dict[str, Any]) -> dict[str, str]:
    """Spaceless key -> the company's name as the *assessment* sheet spells it.

    The assessment sheet carries the curated spellings and the HQ country; the
    subsidiary sheet carries whatever the analyst typed. Canonicalising on the
    assessment sheet collapses "AngloAmerican" onto "Anglo American" and cuts
    the review file from 102 rows to the real company count.
    """
    canon: dict[str, str] = {}
    for row in raw.get("assessments", []):
        name = (row.get("company_name") or "").strip()
        if name:
            canon.setdefault(_tight(name), name)
    return canon


def _parents(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Distinct supporting-company parents, keyed by canonical normalised name."""
    canon = _canonical_names(raw)

    def canonical(name: str) -> str:
        return canon.get(_tight(name), name)

    ref: dict[str, dict[str, Any]] = {}
    for row in raw.get("companies", []):
        name = _unjson(row.get("company_name"))
        if not name:
            continue
        hq = _json_field(row.get("headquarters")) or {}
        ref.setdefault(_tight(name), {
            "hq_country": (hq.get("country") or "") if isinstance(hq, dict) else "",
            "hq_city": (hq.get("city") or "") if isinstance(hq, dict) else "",
            "sectors": _json_field(row.get("sectors")) or [],
            "company_type": _clean(row.get("type")),
            "business_activity": _clean(row.get("business_activity")),
            "open_corporates_id": _clean(row.get("open_corporates_id")),
            "legal_entity_id": _clean(row.get("legal_entity_id")),
            "estma_id": _clean(row.get("estma_id")),
        })

    out: dict[str, dict[str, Any]] = {}

    def bucket(display: str) -> dict[str, Any]:
        name = canonical(display)
        key = _norm_name(name)
        rec = out.setdefault(key, {
            "name": name,
            "subsidiary_count": 0,
            "aliases": set(),
            **ref.get(_tight(name), {}),
        })
        # Compare the DISPLAY strings: the tight forms are equal by
        # construction for exactly the pairs this is meant to record
        # ("AngloAmerican" and "Anglo American" both tighten to the same
        # key, which is why they collapsed into one parent at all).
        if display != name:
            rec["aliases"].add(display)
        return rec

    for row in raw.get("assessments", []):
        name = (row.get("company_name") or "").strip()
        if not name:
            continue
        rec = bucket(name)
        if not rec.get("hq_country"):
            rec["hq_country"] = (row.get("company_hq_country_iso3") or "").strip()
    for row in raw.get("subsidiaries", []):
        name = _unjson(row.get("eiti_supporting_company"))
        if not name:
            continue
        bucket(name)["subsidiary_count"] += 1

    for rec in out.values():
        rec["aliases"] = sorted(rec["aliases"])
    return out


#: ISO 3166 alpha-3 → alpha-2 for the countries the assessment set uses.
#: GLEIF filters on alpha-2; EITI publishes alpha-3.
_ISO3_TO_2 = {
    "ARE": "AE", "ARG": "AR", "AUS": "AU", "AUT": "AT", "BEL": "BE", "BRA": "BR",
    "CAN": "CA", "CHE": "CH", "CHL": "CL", "CHN": "CN", "COL": "CO", "DEU": "DE",
    "DNK": "DK", "ESP": "ES", "FIN": "FI", "FRA": "FR", "GBR": "GB", "IDN": "ID",
    "IND": "IN", "ITA": "IT", "JPN": "JP", "KAZ": "KZ", "KOR": "KR", "LUX": "LU",
    "MAR": "MA", "MEX": "MX", "MNG": "MN", "NGA": "NG", "NLD": "NL", "NOR": "NO",
    "PER": "PE", "POL": "PL", "PRT": "PT", "QAT": "QA", "RUS": "RU", "SAU": "SA",
    "SEN": "SN", "SGP": "SG", "SUR": "SR", "SWE": "SE", "THA": "TH", "TUR": "TR",
    "USA": "US", "ZAF": "ZA",
}

_LEI_RE = re.compile(r"^[A-Z0-9]{18}[0-9]{2}$")


def _gleif_by_lei(client: httpx.Client, lei: str) -> dict[str, Any] | None:
    """Confirm a published LEI exists and return its GLEIF record."""
    try:
        r = client.get(
            f"{GLEIF_API}/{lei}",
            headers={"Accept": "application/vnd.api+json", "User-Agent": _UA},
        )
        if r.is_success:
            return r.json().get("data")
    except Exception as exc:  # noqa: BLE001
        print(f"  ! GLEIF lookup error for {lei}: {exc}", file=sys.stderr)
    return None


def _gleif_candidates(
    client: httpx.Client, name: str, iso2: str
) -> list[tuple[str, str, str]]:
    """Merged candidates from both GLEIF name filters, deduplicated by LEI.

    ``filter[fulltext]`` searches the whole record and ``filter[entity.legalName]``
    searches the name; neither alone is sufficient. Anglo American plc and
    Antofagasta plc appear only under ``legalName``; other heads appear only
    under ``fulltext``. Even merged this is a shortlist, not an answer — for
    short common names ("BP", "Chevron", "Codelco") GLEIF holds thousands of
    matching entities and the group head is often not in the top five at all.
    That is a limit of name search, not something to tune away, and it is why
    the ``lei`` column stays empty for these rows.
    """
    seen: dict[str, tuple[str, str, str]] = {}
    for filt in ("filter[entity.legalName]", "filter[fulltext]"):
        for cand in _gleif_search_one(client, filt, name, iso2):
            if cand[0] and cand[0] not in seen:
                seen[cand[0]] = cand
    return list(seen.values())


def _gleif_search_one(
    client: httpx.Client, filt: str, name: str, iso2: str, *, attempts: int = 3
) -> list[tuple[str, str, str]]:
    """One GLEIF name filter, with retries. See ``_gleif_candidates``."""
    params: dict[str, Any] = {filt: name, "page[size]": 5}
    if iso2:
        params["filter[entity.legalAddress.country]"] = iso2
    for attempt in range(1, attempts + 1):
        try:
            r = client.get(
                GLEIF_API,
                params=params,
                headers={"Accept": "application/vnd.api+json", "User-Agent": _UA},
            )
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After") or 2 * attempt)
                print(f"  · GLEIF 429 on {name}; waiting {wait}s", file=sys.stderr)
                time.sleep(min(wait, 30.0))
                continue
            if not r.is_success:
                return []
            out = []
            for rec in r.json().get("data", []):
                ent = rec.get("attributes", {}).get("entity", {}) or {}
                out.append((
                    rec.get("id") or "",
                    (ent.get("legalName") or {}).get("name", ""),
                    (ent.get("legalAddress") or {}).get("country", "")
                    or (ent.get("jurisdiction") or ""),
                ))
            return out
        except Exception as exc:  # noqa: BLE001
            if attempt == attempts:
                print(f"  ! GLEIF search failed for {name}: {exc}", file=sys.stderr)
                return []
            time.sleep(1.5 * attempt)
    return []


def _gleif_parent(client: httpx.Client, lei: str) -> str:
    """Legal name of this LEI's direct parent in GLEIF, or "" if it reports none.

    The most useful discriminator in the review file. Stripping legal-form
    suffixes makes a group head and its subsidiaries look identical by name —
    "Glencore" matches GLENCORE AG exactly, "Capricorn Energy" matches
    CAPRICORN ENERGY HOLDINGS LIMITED — and in both of those the match is the
    wrong entity.

    It is evidence, not a verdict, and it is weak in both directions:

    * A parent does **not** mean the match is wrong. Hindustan Zinc really is a
      Vedanta subsidiary and PT Pertamina really is owned by the Republic of
      Indonesia; both are the supporting company EITI means.
    * No parent does **not** mean the match is right. GLEIF answers 404 both
      for genuine group heads and for entities that filed a reporting
      exception, so ANGLOGOLD ASHANTI (PTY) LTD. — a subsidiary — comes back
      unflagged.

    So it sorts the file by where to look first; it decides nothing.
    """
    try:
        r = client.get(
            f"{GLEIF_API}/{lei}/direct-parent",
            headers={"Accept": "application/vnd.api+json", "User-Agent": _UA},
        )
        if r.status_code == 404 or not r.is_success:
            return ""
        data = r.json().get("data") or {}
        ent = (data.get("attributes", {}) or {}).get("entity", {}) or {}
        return (ent.get("legalName") or {}).get("name", "") or ""
    except Exception:  # noqa: BLE001
        return ""


_REVIEW_HEADER = [
    "decision", "eiti_name", "hq_country", "subsidiaries", "method",
    "lei", "gleif_legal_name", "gleif_country", "flag", "note",
]
_REVIEW_ROW_BLANK = {k: "" for k in _REVIEW_HEADER}


def resolve(raw_path: Path, review_path: Path, sleep: float) -> None:
    """Propose an LEI per parent and write the human review file."""
    import httpx

    with gzip.open(raw_path, "rt", encoding="utf-8") as f:
        raw = json.load(f)
    parents = _parents(raw)
    print(f"{len(parents)} supporting-company parents", file=sys.stderr)

    # Preserve decisions already made, so re-running after a data refresh does
    # not silently discard a human's work.
    previous: dict[str, dict[str, str]] = {}
    if review_path.exists():
        with review_path.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                if row.get("eiti_name"):
                    previous[_norm_name(row["eiti_name"])] = row
        print(f"  carrying forward {len(previous)} existing decisions", file=sys.stderr)

    rows: list[dict[str, str]] = []
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for key in sorted(parents):
            p = parents[key]
            name = p["name"]
            iso3 = (p.get("hq_country") or "").upper()
            iso2 = _ISO3_TO_2.get(iso3, "")
            prior = previous.get(key)

            base = {
                "eiti_name": name,
                "hq_country": iso3,
                "subsidiaries": str(p.get("subsidiary_count", 0)),
            }

            # A decision already taken is kept verbatim. Re-searching would
            # churn the file and invite a re-tick of work already done.
            if prior and prior.get("decision", "").strip().lower() in {"accept", "reject"}:
                rows.append({**_REVIEW_ROW_BLANK, **prior, **base})
                continue

            published = (p.get("legal_entity_id") or "").strip().upper()
            if _LEI_RE.match(published):
                rec = _gleif_by_lei(client, published)
                time.sleep(sleep)
                ent = ((rec or {}).get("attributes", {}) or {}).get("entity", {}) or {}
                rows.append({
                    **base,
                    # EITI published this LEI itself, so there is nothing for a
                    # human to adjudicate — but it is still recorded here so the
                    # file is the whole picture rather than the doubtful half.
                    "decision": "accept" if rec else "REVIEW",
                    "method": "published_lei",
                    "lei": published,
                    "gleif_legal_name": (ent.get("legalName") or {}).get("name", ""),
                    "gleif_country": (ent.get("legalAddress") or {}).get("country", ""),
                    "note": "" if rec else "published LEI not found in GLEIF",
                })
                continue

            cands = _gleif_candidates(client, name, iso2)
            time.sleep(sleep)
            target = _norm_name(name)
            exact = [c for c in cands if _norm_name(c[1]) == target]

            # Only ever pre-fill an EXACT normalised-name match. A pre-filled
            # near-miss sitting next to the word REVIEW is the single easiest
            # thing to tick through by pattern-matching — and the near-misses
            # here are dangerous: "Anglo American" ranks the ANGLO AMERICAN
            # FOUNDATION first, and "AngloAmerican" ranks INTERNATIONAL SCHOOL
            # OF TURIN. Non-exact candidates go in the note as a shortlist for
            # a human to choose from, and the LEI column starts empty.
            if exact:
                lei, legal, cc = exact[0]
                parent = _gleif_parent(client, lei)
                time.sleep(sleep)
                notes = []
                if len(exact) > 1:
                    notes.append(f"{len(exact)} exact name matches")
                if iso3 and cc and _ISO3_TO_2.get(iso3, "") != cc:
                    notes.append(f"country differs: EITI says {iso3}, GLEIF says {cc}")
                rows.append({
                    **base,
                    "decision": "REVIEW",
                    "method": "gleif_name_exact",
                    "lei": lei,
                    "gleif_legal_name": legal,
                    "gleif_country": cc,
                    # A recorded parent means this is very likely a subsidiary
                    # of the company EITI means, not the company itself.
                    "flag": f"HAS GLEIF PARENT: {parent}" if parent else "",
                    "note": "; ".join(notes),
                })
            else:
                # An EITI supporting company is a group head, so rank the
                # shortlist by whether the candidate reports a GLEIF parent.
                # Without this the top hit is routinely a group *member* —
                # "BP" returns BP TECHNOLOGY VENTURES, "Chevron" returns
                # CHEVRON MASTER PENSION TRUST — and the reviewer has to go
                # and find the real LEI by hand.
                ranked: list[tuple[int, str, str, str, str]] = []
                for lei, legal, cc in cands[:5]:
                    parent = _gleif_parent(client, lei)
                    time.sleep(sleep)
                    ranked.append((1 if parent else 0, lei, legal, cc, parent))
                ranked.sort(key=lambda t: t[0])
                shortlist = "; ".join(
                    f"{lei} {legal} ({cc})" + (f" [under {parent}]" if parent else "")
                    for _, lei, legal, cc, parent in ranked[:3]
                )
                rows.append({
                    **base,
                    "decision": "REVIEW",
                    "method": "candidates_only",
                    "lei": "",
                    "gleif_legal_name": "",
                    "gleif_country": "",
                    "flag": "",
                    "note": (
                        f"no exact match — paste one LEI if right: {shortlist}"
                        if shortlist else "no GLEIF candidate"
                    ),
                })

    # Sort by where a reviewer should look first: flagged proposals, then rows
    # with no proposal at all, then plain exact matches, then the settled ones.
    def risk(row: dict[str, str]) -> tuple[int, str]:
        if row.get("decision", "").strip().lower() in {"accept", "reject"}:
            return (3, row["eiti_name"].lower())
        if row.get("flag"):
            return (0, row["eiti_name"].lower())
        if not row.get("lei"):
            return (1, row["eiti_name"].lower())
        return (2, row["eiti_name"].lower())

    rows.sort(key=risk)

    review_path.parent.mkdir(parents=True, exist_ok=True)
    with review_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_REVIEW_HEADER, delimiter="\t",
                           extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({**_REVIEW_ROW_BLANK, **row})

    todo = [r for r in rows if r.get("decision") == "REVIEW"]
    flagged = sum(1 for r in todo if r.get("flag"))
    blank = sum(1 for r in todo if not r.get("lei"))
    print(f"\nWrote {review_path}", file=sys.stderr)
    print(f"  {len(rows)} parents, {len(todo)} awaiting review", file=sys.stderr)
    print(f"    {flagged} carry a flag (look at these first)", file=sys.stderr)
    print(f"    {blank} have no proposed LEI (paste one, or reject)", file=sys.stderr)
    print(f"    {len(todo) - flagged - blank} are plain exact name matches",
          file=sys.stderr)
    print("\nEdit the file: change each REVIEW to 'accept' or 'reject'.", file=sys.stderr)
    print("A wrong LEI renders a whole card against the wrong company. The name", file=sys.stderr)
    print("matcher is not trustworthy here: 'Anglo American' ranks the ANGLO", file=sys.stderr)
    print("AMERICAN FOUNDATION first and 'Equinor' matched a company sports club.", file=sys.stderr)


# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------


def _canonical_key(raw: dict[str, Any]):
    """Return a function mapping any EITI spelling to the canonical parent key.

    All three of ``_parents`` / ``_assessments_by_company`` /
    ``_subsidiaries_by_parent`` must agree on this key, or a company's
    assessments and its subsidiaries land under different index entries.
    """
    canon = _canonical_names(raw)

    def key(name: str) -> str:
        return _norm_name(canon.get(_tight(name), name))

    return key


def _assessments_by_company(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """canonical parent key -> {year -> {expectation shorthand -> fields}}."""
    key_of = _canonical_key(raw)
    out: dict[str, dict[str, Any]] = {}
    for row in raw.get("assessments", []):
        name = (row.get("company_name") or "").strip()
        short = (row.get("expectation_shorthand") or "").strip()
        if not name or not short:
            continue
        year = str(row.get("assessment_year") or "").strip()
        rec = out.setdefault(key_of(name), {}).setdefault(year, {})
        entry: dict[str, Any] = {
            "label": (row.get("expectation_label") or "").strip() or None,
            "result": (row.get("assessment_result") or "").strip() or None,
            "response": _clean(row.get("response")),
            "url": _clean(row.get("url")),
            "comment": _clean(row.get("secretariat_comment")),
        }
        if short == "exp_6":
            entry.update({
                "bo_disclosure": _clean(row.get("bo_disclosure")),
                "bo_url": _clean(row.get("bo_url")),
                "bo_disclosure_url": _clean(row.get("bo_disclosure_url")),
                "stock_exchange": _clean(row.get("stock_exchange")),
                "stock_url": _clean(row.get("stock_url")),
            })
        rec[short] = {k: v for k, v in entry.items() if v is not None}
    return out


def _subsidiaries_by_parent(raw: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """canonical parent key -> deduplicated child list."""
    key_of = _canonical_key(raw)
    out: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
    for row in raw.get("subsidiaries", []):
        parent = _unjson(row.get("eiti_supporting_company"))
        child = _unjson(row.get("subsidiary_name"))
        if not parent or not child:
            continue
        country = (_clean(row.get("eiti_implementing_country")) or "").upper()
        bucket = out.setdefault(key_of(parent), {})
        # EITI repeats a child across report years; collapse on (name, country)
        # and keep the widest year range rather than emitting duplicate rows.
        key = (_norm_name(child), country)
        rec = bucket.setdefault(key, {
            "name": child, "country": country or None, "years": [], "source": None,
        })
        year = _clean(row.get("eiti_report_year"))
        if year and year not in rec["years"]:
            rec["years"].append(year)
        src = _clean(row.get("source"))
        if src and not rec["source"]:
            rec["source"] = src
    return {
        parent: sorted(
            ({**v, "years": sorted(v["years"])} for v in children.values()),
            key=lambda r: r["name"].lower(),
        )
        for parent, children in out.items()
    }


def build(raw_path: Path, review_path: Path, out: Path) -> None:
    """Write the committed LEI-keyed index. Offline and deterministic."""
    with gzip.open(raw_path, "rt", encoding="utf-8") as f:
        raw = json.load(f)
    if not review_path.exists():
        sys.exit(f"No review file at {review_path} — run 'resolve' first.")

    with review_path.open(encoding="utf-8", newline="") as f:
        review = list(csv.DictReader(f, delimiter="\t"))

    pending = [r["eiti_name"] for r in review
               if (r.get("decision") or "").strip().upper() == "REVIEW"]
    if pending:
        print(f"REFUSING to build: {len(pending)} parents still marked REVIEW.",
              file=sys.stderr)
        for n in pending[:10]:
            print(f"  - {n}", file=sys.stderr)
        if len(pending) > 10:
            print(f"  … and {len(pending) - 10} more", file=sys.stderr)
        sys.exit(
            f"\nEdit {review_path} and set every decision to 'accept' or 'reject'.\n"
            "This gate exists because a name matcher over MNE names produces\n"
            "confident nonsense — see the module docstring."
        )

    parents = _parents(raw)
    assessments = _assessments_by_company(raw)
    subsidiaries = _subsidiaries_by_parent(raw)

    index: dict[str, dict[str, Any]] = {}
    accepted = rejected = 0
    unresolved: list[str] = []
    for row in review:
        decision = (row.get("decision") or "").strip().lower()
        name = (row.get("eiti_name") or "").strip()
        key = _norm_name(name)
        if decision != "accept":
            rejected += 1
            unresolved.append(name)
            continue
        lei = (row.get("lei") or "").strip().upper()
        if not _LEI_RE.match(lei):
            sys.exit(f"Row for {name!r} is accepted but its LEI {lei!r} is malformed.")
        if lei in index:
            sys.exit(
                f"Two accepted parents resolve to the same LEI {lei}: "
                f"{index[lei]['name']!r} and {name!r}. Reject one."
            )
        p = parents.get(key, {})
        accepted += 1
        index[lei] = {
            "name": name,
            "hq_country": row.get("hq_country") or p.get("hq_country") or None,
            "hq_city": p.get("hq_city") or None,
            "sectors": p.get("sectors") or [],
            "company_type": p.get("company_type"),
            "business_activity": p.get("business_activity"),
            # The identifiers EITI itself publishes for this company. Kept for
            # display and provenance; the adapter asserts none of them, and the
            # LEI above is OpenCheck-derived (see the module docstring).
            "eiti_published": {
                "open_corporates_id": p.get("open_corporates_id"),
                "legal_entity_id": p.get("legal_entity_id"),
                "estma_id": p.get("estma_id"),
            },
            "match": {
                "method": row.get("method") or "",
                "reviewed": True,
                "gleif_legal_name": row.get("gleif_legal_name") or None,
            },
            "assessments": assessments.get(key, {}),
            "subsidiaries": subsidiaries.get(key, []),
        }

    payload = {
        "meta": {
            "built": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "source_harvest": (raw.get("meta") or {}).get("harvested"),
            "source": (raw.get("meta") or {}).get("source"),
            "licence": LICENCE,
            "attribution": ATTRIBUTION,
            "parents_total": len(review),
            "parents_accepted": accepted,
            "parents_unresolved": rejected,
            "unresolved_names": sorted(unresolved),
            "subsidiaries_indexed": sum(len(v["subsidiaries"]) for v in index.values()),
            "review_gate": "every accepted LEI was reviewed by a human",
        },
        "index": index,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=True)

    m = payload["meta"]
    print(f"Wrote {out} ({out.stat().st_size:,} bytes)", file=sys.stderr)
    print(f"  parents         : {m['parents_total']}", file=sys.stderr)
    print(f"  resolved to LEI : {m['parents_accepted']}", file=sys.stderr)
    print(f"  unresolved      : {m['parents_unresolved']}", file=sys.stderr)
    print(f"  subsidiaries    : {m['subsidiaries_indexed']}", file=sys.stderr)


# --------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    h = sub.add_parser("harvest", help="pull the assessment tables (network)")
    h.add_argument("--out", type=Path, default=_RAW_PATH)
    h.add_argument("--sleep", type=float, default=0.3)

    r = sub.add_parser("resolve", help="propose LEIs and write the review file (network)")
    r.add_argument("--raw", type=Path, default=_RAW_PATH)
    r.add_argument("--review", type=Path, default=_REVIEW_PATH)
    r.add_argument("--sleep", type=float, default=0.4)

    b = sub.add_parser("build", help="write the committed index (offline)")
    b.add_argument("--raw", type=Path, default=_RAW_PATH)
    b.add_argument("--review", type=Path, default=_REVIEW_PATH)
    b.add_argument("--out", type=Path, default=_OUT_PATH)

    args = ap.parse_args()
    if args.cmd == "harvest":
        harvest(args.out, args.sleep)
    elif args.cmd == "resolve":
        resolve(args.raw, args.review, args.sleep)
    else:
        build(args.raw, args.review, args.out)


if __name__ == "__main__":
    main()
