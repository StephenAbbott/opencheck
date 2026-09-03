#!/usr/bin/env python3
"""Build the committed EITI US ``LEI -> EIN`` crosswalk (issue #26).

Why this exists
---------------
EITI's US identifications are **federal EINs** (``42-1638663``). GLEIF
publishes ``registeredAs`` for US entities as the *state* file number
(Delaware ``2064000``, Texas ``0800335620``) and never the EIN — verified
against GLEIF 2026-09-02 — so a US subject can never join the EITI
organisation index on ``registeredAs`` alone.

``EitiAdapter.fetch_by_registration`` has accepted a derived ``us_ein``
since PR #46, but nothing produced one, so the US path never fired. This
script produces it: a committed ``LEI -> EIN`` map that
``routers/lookup.py::_build_derived()`` reads to populate
``ctx.derived["us_ein"]`` *before* dispatch. No live call is added to the
lookup path.

Why a committed crosswalk rather than live EDGAR derivation
-----------------------------------------------------------
The US withdrew from EITI in November 2017. The US bucket of the
organisation index is 29 identifications from a single 2015 reporting year
and will not grow — a frozen table, not a stream. Two of the 29 are the
sentinel strings ``Private`` and ``Foreign``, not EINs at all. Resolving
this offline once per index refresh costs nothing at lookup time, and it
still covers the entities EDGAR can no longer resolve by name (most of the
2015 filers have since merged, renamed or delisted: Anadarko into Oxy,
Concho into ConocoPhillips, Cimarex into Coterra, Arch Coal into Core
Natural Resources, Apache into APA, …).

Resolution, and what each confidence level actually means
---------------------------------------------------------
1. **EITI EIN -> EDGAR registrant** — candidate CIKs come from
   ``Archives/edgar/cik-lookup-data.txt`` (every registrant EDGAR has ever
   held, ~1.06M names, one 40 MB file), but a candidate is only accepted
   when the ``ein`` field of ``data.sec.gov/submissions/CIK{cik}.json``
   *equals* the EITI EIN. This leg is verified by a federal identifier,
   never by a name. Deliberately not the ``browse-edgar`` company-search
   atom feed: it is a prefix search that misses ``ANADARKO PETROLEUM CORP``
   for "Anadarko Petroleum Corporation", and it 503s under any run long
   enough to resolve the whole bucket.
2. **-> GLEIF LEI** — a country-scoped GLEIF fulltext search, accepted only
   when the normalised legal names match exactly. A same-country hit whose
   name does not match is dropped as unresolved rather than indexed as a
   guess (the rule ``build_eiti_soe_index.py`` already applies).

   ``high``   both legs: the EIN was confirmed against an EDGAR registrant
              and that registrant's conformed name matched a US LEI.
   ``medium`` GLEIF name match only — no current EDGAR registrant carries
              that EIN (defunct or never a registrant), so the EIN itself is
              unverified outside EITI's own filing.

Every row carries its evidence, so a reviewer can check the join without
re-running the script. Unresolved rows are written out too, for the same
reason.

Usage:
    python3 scripts/build_eiti_us_ein_index.py
    python3 scripts/build_eiti_us_ein_index.py --limit 5      # smoke run
    python3 scripts/build_eiti_us_ein_index.py --dry-run      # report only

Re-run after ``build_eiti_index.py`` refreshes the organisation index and
commit the regenerated artifact. Set ``OPENCHECK_EDGAR_CONTACT_EMAIL``
first — EDGAR silently 403s requests whose User-Agent carries no contact
address (https://www.sec.gov/os/webmaster-faq#developers).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

_DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
_EITI_INDEX = _DATA / "eiti_organisations.json.gz"
_DEFAULT_OUT = _DATA / "eiti_us_ein_by_lei.json"

_CIK_LOOKUP_URL = "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt"
_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
_GLEIF_API = "https://api.gleif.org/api/v1/lei-records"

#: EITI publishes US identifications as ``NN-NNNNNNN``. Anything else in the
#: US bucket is not an EIN (see the ``Private`` / ``Foreign`` sentinels).
_EIN_RE = re.compile(r"^\d{2}-?\d{7}$")
_DIGITS_RE = re.compile(r"\D+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")

#: Trailing legal-form tokens stripped before comparing names. Deliberately
#: the same set as ``sources/sec_edgar.py::_normalise_company_name`` so a
#: match here is a match there.
_LEGAL_FORM_SUFFIXES = {
    "inc", "incorporated", "corp", "corporation", "co", "company",
    "plc", "ltd", "limited", "llc", "llp", "lp", "nv", "sa", "ag",
    "se", "ab", "as", "oyj", "spa", "gmbh", "kg", "bv",
}
#: EDGAR appends the state of incorporation to conformed names
#: ("DEVON ENERGY CORP/DE"); it is not part of the company's name.
_EDGAR_STATE_SUFFIX_RE = re.compile(r"/[a-z]{2}/?$")


def _fold_name(value: str) -> str:
    """Case/punctuation fold only — legal-form tokens are kept.

    ``ConocoPhillips`` and ``ConocoPhillips Company`` are different legal
    entities with different LEIs, and the suffix strip below collapses them
    onto the same key. Keeping an unstripped form lets an exact match win
    over a stripped one instead of the pair reading as ambiguous.
    """
    v = (value or "").strip().lower()
    v = _EDGAR_STATE_SUFFIX_RE.sub(" ", v)
    v = _NON_ALNUM_RE.sub(" ", v)
    return _WS_RE.sub(" ", v).strip()


def _norm_name(value: str) -> str:
    tokens = _fold_name(value).split()
    while tokens and tokens[0] == "the":
        tokens = tokens[1:]
    while tokens and tokens[-1] in _LEGAL_FORM_SUFFIXES:
        tokens = tokens[:-1]
    return " ".join(tokens)


def _ein_digits(value: str) -> str:
    return _DIGITS_RE.sub("", value or "").lstrip("0")


def _format_ein(value: str) -> str:
    d = _DIGITS_RE.sub("", value or "").rjust(9, "0")
    return f"{d[:2]}-{d[2:]}" if len(d) == 9 else value


# ----------------------------------------------------------------------
# HTTP (stdlib only — this script must run anywhere the artifact is rebuilt)
# ----------------------------------------------------------------------

def _edgar_ua() -> str:
    email = os.environ.get("OPENCHECK_EDGAR_CONTACT_EMAIL", "").strip()
    if not email:
        print(
            "! OPENCHECK_EDGAR_CONTACT_EMAIL is unset. EDGAR blocks anonymous "
            "server-side requests with a silent 403, so the EIN-verification "
            "leg will fail and every row will fall back to 'medium'.",
            file=sys.stderr,
        )
        email = "opencheck@example.invalid"
    return f"OpenCheck/eiti-us-ein-index ({email})"


def _get(url: str, *, accept: str = "*/*", ua: str) -> str | None:
    req = urllib.request.Request(
        url, headers={"User-Agent": ua, "Accept": accept, "Accept-Encoding": "gzip"}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return body.decode("utf-8", "replace")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"  ! GET {url.split('?')[0]}: {exc}", file=sys.stderr)
        return None


# ----------------------------------------------------------------------
# Leg 1 — EITI EIN -> EDGAR registrant, confirmed by the EIN itself
# ----------------------------------------------------------------------

def _cik_name_index(ua: str) -> dict[str, list[str]]:
    """normalised registrant name -> CIKs, from EDGAR's full lookup file.

    One 40 MB download covers every registrant EDGAR has ever held, including
    the dissolved and delisted 2015 filers that no name-search endpoint will
    return. Several conformed names normalise to the same key
    ("DEVON ENERGY CORP", "DEVON ENERGY CORP /DE/"); all of their CIKs are
    kept as candidates because the EIN, not the name, does the accepting.
    """
    raw = _get(_CIK_LOOKUP_URL, accept="text/plain", ua=ua)
    idx: dict[str, list[str]] = {}
    if not raw:
        return idx
    for line in raw.splitlines():
        name, _, rest = line.partition(":")
        cik = rest.strip(":").strip()
        key = _norm_name(name)
        if not key or not cik.isdigit():
            continue
        bucket = idx.setdefault(key, [])
        if cik not in bucket:
            bucket.append(cik)
    return idx


def _confirm_by_ein(
    ein: str, label: str, ciks_by_name: dict[str, list[str]], ua: str, sleep: float
) -> dict[str, Any] | None:
    """Find the EDGAR registrant whose ``ein`` equals this EITI EIN.

    Names only ever *propose* a candidate; the EIN alone accepts it.
    """
    want = _ein_digits(ein)
    candidates = list(ciks_by_name.get(_norm_name(label)) or [])

    for cik in candidates[:12]:
        raw = _get(_SUBMISSIONS.format(cik=int(cik)), accept="application/json", ua=ua)
        time.sleep(sleep)
        if not raw:
            continue
        try:
            sub = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if _ein_digits(str(sub.get("ein") or "")) != want:
            continue
        return {
            "cik": str(int(cik)),
            "name": sub.get("name") or "",
            "state_of_incorporation": sub.get("stateOfIncorporation") or "",
        }
    return None


# ----------------------------------------------------------------------
# Leg 2 — name -> LEI, accepted only on an exact normalised-name match
# ----------------------------------------------------------------------

#: Registration statuses, best first. A LAPSED LEI is still the right LEI for
#: a company that stopped renewing — most of the 2015 filers did — so it is
#: ranked below ISSUED rather than excluded.
_REG_STATUS_RANK = {"ISSUED": 0, "PENDING_TRANSFER": 1, "LAPSED": 2}


def _gleif_query(params: dict[str, Any], ua: str, sleep: float) -> list[dict[str, Any]]:
    qs = urllib.parse.urlencode(params)
    raw = _get(f"{_GLEIF_API}?{qs}", accept="application/vnd.api+json", ua=ua)
    time.sleep(sleep)
    if not raw:
        return []
    try:
        return json.loads(raw).get("data") or []
    except json.JSONDecodeError:
        return []


def _resolve_lei(
    names: list[str], ua: str, sleep: float, state_of_incorporation: str = ""
) -> tuple[dict[str, Any] | None, str]:
    """Resolve candidate names to a single US LEI. Returns (hit, reason).

    Only an exact normalised-name match on a US record is accepted; a
    same-country hit whose name does not match is a different company, so it
    is dropped rather than indexed as a guess (the rule
    ``build_eiti_soe_index.py`` already applies).

    Two GLEIF quirks the query shape has to respect, both verified 2026-09-02:

    * ``filter[entity.legalName]`` combined with
      ``filter[entity.legalAddress.country]`` returns **zero** rows even when
      the name filter alone returns the company. So neither search filters on
      country server-side; country is applied to the returned records.
    * ``filter[entity.legalName]`` is the precise one ("EXXON MOBIL
      CORPORATION" -> the company); ``filter[fulltext]`` ranks loosely
      ("CHEVRON CORPORATION" -> CHEVRON MASTER PENSION TRUST first), so it is
      only a fallback.

    Candidate names are deduplicated by the **raw query string**, not by its
    normalised form: "EXXON MOBIL CORP" (EDGAR) and "Exxon Mobil Corporation"
    (EITI) normalise identically but are different queries, and only the
    second finds the LEI.
    """
    seen: set[str] = set()
    survivors: dict[str, dict[str, Any]] = {}
    want_juris = f"US-{state_of_incorporation.strip().upper()}" if state_of_incorporation else ""
    for name in names:
        key = (name or "").strip().upper()
        target = _norm_name(name)
        if not target or key in seen:
            continue
        seen.add(key)
        for params in (
            {"filter[entity.legalName]": name, "page[size]": 50},
            {"filter[fulltext]": name, "page[size]": 50},
        ):
            for rec in _gleif_query(params, ua, sleep):
                attrs = rec.get("attributes") or {}
                ent = attrs.get("entity") or {}
                legal = ((ent.get("legalName") or {}).get("name") or "")
                if _norm_name(legal) != target:
                    continue
                country = ((ent.get("legalAddress") or {}).get("country")
                           or ent.get("jurisdiction") or "")
                if not country.upper().startswith("US"):
                    continue
                reg = attrs.get("registration") or {}
                lei = rec.get("id")
                if not lei or lei in survivors:
                    continue
                juris = (ent.get("jurisdiction") or "").upper()
                survivors[lei] = {
                    "lei": lei,
                    "gleif_name": legal,
                    "registered_as": ent.get("registeredAs") or "",
                    "entity_status": ent.get("status") or "",
                    "registration_status": reg.get("status") or "",
                    "jurisdiction": juris,
                    "matched_on": name,
                    # 0 = the legal-form suffix was not needed to make the
                    # names equal; 1 = only equal once it was stripped.
                    "_name_tier": 0 if _fold_name(legal) == _fold_name(name) else 1,
                }
            if survivors:
                break
        if survivors:
            break

    if not survivors:
        return None, "no US LEI whose legal name matches"

    def _rank(h: dict[str, Any]) -> tuple[int, int, int]:
        return (
            h["_name_tier"],
            _REG_STATUS_RANK.get(h["registration_status"], 9),
            0 if want_juris and h["jurisdiction"] == want_juris else 1,
        )

    ranked = sorted(survivors.values(), key=_rank)
    tied = [h for h in ranked if _rank(h) == _rank(ranked[0])]
    if len(tied) > 1:
        # Several LEIs carry the same US legal name, at the same registration
        # status, in the same state. Picking one would attribute a company's
        # payments to a coin flip.
        return None, "ambiguous — " + ", ".join(h["lei"] for h in tied)
    best = dict(ranked[0])
    best.pop("_name_tier", None)
    return best, ""


# ----------------------------------------------------------------------
# Build
# ----------------------------------------------------------------------

def _us_identifications() -> tuple[list[tuple[str, str]], dict[str, Any]]:
    with gzip.open(_EITI_INDEX, "rt", encoding="utf-8") as f:
        data = json.load(f)
    us = (data.get("index") or {}).get("US") or {}
    rows = [
        (ident, (recs[0].get("label") or "") if recs else "")
        for ident, recs in us.items()
    ]
    return rows, data.get("meta") or {}


def build(out: Path, limit: int | None, sleep: float, dry_run: bool) -> int:
    ua = _edgar_ua()
    rows, eiti_meta = _us_identifications()
    print(f"US identifications in the EITI index: {len(rows)}", file=sys.stderr)

    eins = [(i, lbl) for i, lbl in rows if _EIN_RE.match(i)]
    non_eins = [(i, lbl) for i, lbl in rows if not _EIN_RE.match(i)]
    for ident, label in non_eins:
        print(f"  - skipping non-EIN identification {ident!r} ({label})", file=sys.stderr)
    if limit:
        eins = eins[:limit]

    print(f"Resolving {len(eins)} EINs …", file=sys.stderr)
    print("Downloading EDGAR cik-lookup-data.txt (~40 MB) …", file=sys.stderr)
    ciks_by_name = _cik_name_index(ua)
    print(f"  {len(ciks_by_name)} distinct normalised registrant names", file=sys.stderr)

    index: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    counts = {"high": 0, "medium": 0, "unresolved": 0, "ein_confirmed": 0}

    for n, (ident, label) in enumerate(eins, 1):
        print(f"[{n}/{len(eins)}] {ident}  {label}", file=sys.stderr)
        edgar = _confirm_by_ein(ident, label, ciks_by_name, ua, sleep)
        if edgar:
            counts["ein_confirmed"] += 1
            print(f"    EDGAR CIK {edgar['cik']} — EIN confirmed ({edgar['name']})",
                  file=sys.stderr)
        candidates = [label]
        if edgar and edgar["name"]:
            candidates.append(edgar["name"])
        hit, reason = _resolve_lei(
            candidates, ua, sleep,
            state_of_incorporation=(edgar or {}).get("state_of_incorporation", ""),
        )
        if not hit:
            counts["unresolved"] += 1
            unresolved.append({
                "ein": _format_ein(ident),
                "eiti_label": label,
                "edgar_cik": (edgar or {}).get("cik", ""),
                "edgar_name": (edgar or {}).get("name", ""),
                "reason": reason,
            })
            print(f"    unresolved — {reason}", file=sys.stderr)
            continue
        confidence = "high" if edgar else "medium"
        counts[confidence] += 1
        lei = hit["lei"]
        if lei in index:
            print(f"    ! {lei} already mapped to {index[lei]['ein']}; keeping first",
                  file=sys.stderr)
            continue
        index[lei] = {
            "ein": _format_ein(ident),
            "eiti_label": label,
            "gleif_name": hit["gleif_name"],
            "gleif_registered_as": hit["registered_as"],
            "gleif_jurisdiction": hit["jurisdiction"],
            "gleif_registration_status": hit["registration_status"],
            "edgar_cik": (edgar or {}).get("cik", ""),
            "edgar_name": (edgar or {}).get("name", ""),
            "matched_on": hit["matched_on"],
            "confidence": confidence,
            "method": "edgar_ein+gleif_name" if edgar else "gleif_name",
        }
        print(f"    {lei}  ({confidence})", file=sys.stderr)

    payload = {
        "meta": {
            "description": (
                "EITI US identifications (federal EINs) keyed by LEI, so the "
                "lookup pipeline can derive us_ein for a US subject without a "
                "live call. Scope is exactly EITI's US bucket — this is not a "
                "general EIN registry."
            ),
            "generated": _dt.date.today().isoformat(),
            "sources": [
                "EITI API v2.0 organisation index — EITI International Secretariat, eiti.org",
                "SEC EDGAR submissions API (ein, conformed name) — sec.gov",
                "GLEIF LEI records API — gleif.org",
            ],
            "eiti_index_generated": eiti_meta.get("generated", ""),
            "us_identifications": len(rows),
            "non_ein_identifications": [i for i, _ in non_eins],
            "eins": len(eins),
            "resolved": len(index),
            "confidence_high": counts["high"],
            "confidence_medium": counts["medium"],
            "ein_confirmed_against_edgar": counts["ein_confirmed"],
            "note": (
                "The US left EITI in November 2017; its bucket is a frozen "
                "single-year (2015) table and will not grow. Rebuild only when "
                "build_eiti_index.py refreshes the organisation index."
            ),
        },
        "index": dict(sorted(index.items())),
        "unresolved": unresolved,
    }

    print(
        f"\nresolved {len(index)}/{len(eins)} "
        f"(high {counts['high']}, medium {counts['medium']}), "
        f"unresolved {counts['unresolved']}",
        file=sys.stderr,
    )
    if dry_run:
        print("(dry run — nothing written)", file=sys.stderr)
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
                   encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size:,} bytes)", file=sys.stderr)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    p.add_argument("--limit", type=int, default=None,
                   help="resolve only the first N EINs (smoke run)")
    p.add_argument("--sleep", type=float, default=0.2,
                   help="pause between upstream calls (EDGAR fair use)")
    p.add_argument("--dry-run", action="store_true",
                   help="report without writing the artifact")
    a = p.parse_args()
    return build(a.out, a.limit, a.sleep, a.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
