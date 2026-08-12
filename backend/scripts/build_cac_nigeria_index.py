#!/usr/bin/env python3
"""Build the committed, LEI-keyed CAC Nigeria PSC index.

OpenCheck is anchored end-to-end on the LEI, but the Nigerian Corporate Affairs
Commission (CAC) beneficial ownership register (``bor.cac.gov.ng``) is keyed on
the company RC number and publishes **no** LEI. Identity resolution is therefore
done **once, offline** (like the ``eiti_soe`` adapter):

1. A curated set of well-known Nigerian companies is pulled from the CAC BOR
   public search API (``borapp.cac.gov.ng/api/bor-search/{get_psc,get_psc_details}``),
   which the public website itself calls. The register is fully public; there is
   **no** sanctioned third-party API (the documented API is restricted to
   Nigerian government / law-enforcement agencies), so this is treated as a
   small, vendored example harvest — not a live adapter.
2. Each company's RC number is matched to its LEI via GLEIF
   (``registeredAs`` + ``registeredAt=RA000469``, the CAC's GLEIF Registration
   Authority code, verified live 2026-08-12).
3. Owner names — which arrive transposed / duplicated / typo'd in the source —
   are normalised, classified (person / entity / arrangement / unknown), and the
   result is committed to ``opencheck/data/cac_nigeria_psc.json`` keyed by LEI.

At runtime the ``cac_nigeria`` adapter loads that committed index and answers
``fetch_by_lei`` as a dict lookup — no network on the hot path — and
``map_cac_nigeria`` maps each record to BODS v0.4. When a live adapter is built
later (pending engagement with CAC / Oasis Management), it can return the same
per-record shape and reuse the same mapper.

Usage::

    python -m scripts.build_cac_nigeria_index \
        --from-raw opencheck/data/cac_nigeria_raw.json \
        --out opencheck/data/cac_nigeria_psc.json

``--from-raw`` normalises a committed raw harvest (the default path when omitted).
Live re-harvesting from the CAC API is intentionally not implemented here.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
RA_CODE = "RA000469"  # GLEIF Registration Authority code for the CAC (verified 2026-08-12)

# Corporate-suffix heuristic for classifying an owner as an entity when it is not
# in the explicit normalisation table below.
_CORP_SUFFIX = re.compile(
    r"\b(LTD|LIMITED|PLC|INC|INCORPORATED|SA|GMBH|BV|NV|COMPANY|INDUSTRIES|"
    r"HOLDINGS?|GROUP|INTERNATIONAL|NOMINEES|OVERSEAS|FUND|MINISTRY|SIAT)\b",
    re.I,
)


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().upper())


# Explicit owner normalisation for this curated set. Maps the raw (messy) owner
# string as it arrives from the CAC BOR to (canonical name, kind, jurisdiction,
# RC). A production/live adapter would replace this with heuristics.
# kind ∈ {"entity", "person", "arrangement", "unknown"}.
OWNERS: dict[str, tuple[str, str, str | None, str | None]] = {
    "LTD DANGOTE INDUSTRIES": ("Dangote Industries Limited", "entity", "NG", None),
    "DANGOTE INDUSTRIES LIMITED": ("Dangote Industries Limited", "entity", "NG", None),
    "LIMITED ATALANTAF": ("Atalanta Limited", "entity", None, None),
    "OVERSEAS LIMITED GUINNNESS": ("Guinness Overseas Limited", "entity", None, None),
    "STANBIC NOMINEES NIGERIA LIMITED STANBIC NOMINEES NIGERIA LIMITED 375064":
        ("Stanbic Nominees Nigeria Limited", "entity", "NG", "375064"),
    "UNITED ALLIANCE COMPANY OF NIGERIA LIMITED [409034] "
    "UNITED ALLIANCE COMPANY OF NIGERIA LIMITED [409034]":
        ("United Alliance Company of Nigeria Limited", "entity", "NG", "409034"),
    "INCORPORATED MINISTRY OF FINANCE":
        ("Ministry of Finance Incorporated (MOFI)", "entity", "NG", None),
    "BUA INDUSTRIES LIMITED": ("BUA Industries Limited", "entity", "NG", None),
    "SIAT SA": ("SIAT SA", "entity", None, None),
    "ZPC/SIPML RSA FUND II - MAIN A/C":
        ("ZPC/SIPML RSA Fund II — Main A/C", "arrangement", "NG", None),
    "OVIA JIM": ("Jim Ovia", "person", None, None),
    "DANGOTE ALIKO": ("Aliko Dangote", "person", None, None),
    "BOLA-SADIPE FEMI": ("Femi Bola-Sadipe", "person", "NG", None),
    "TAHIR BASHIR": ("Bashir Tahir", "person", "NG", None),
    "PAUL DEON NORMAN": ("Paul Deon Norman", "person", "ZA", None),
    "ABDUL SAMAD RABIU": ("Abdul Samad Rabiu", "person", None, None),
    "OTHERS": ("Others (unspecified)", "unknown", None, None),
}


def _classify(raw_name: str, is_corp_flag: bool) -> tuple[str, str, str | None, str | None]:
    key = _norm_key(raw_name)
    if key in OWNERS:
        return OWNERS[key]
    canon = re.sub(r"\s+", " ", raw_name.title()).strip()
    kind = "entity" if (is_corp_flag or _CORP_SUFFIX.search(key)) else "person"
    return canon, kind, None, None


def normalise(raw: dict) -> dict:
    index: dict[str, dict] = {}
    skipped_blank = 0
    for e in raw["entities"]:
        lei = (e["lei"] or "").strip().upper()
        pscs: list[dict] = []
        for p in e["pscs"]:
            name_raw = (p.get("ownerNameRaw") or "").strip() or (p.get("corpName") or "").strip()
            if not name_raw:
                skipped_blank += 1
                continue
            canon, kind, juris, orc = _classify(name_raw, bool(p.get("isCorpFlag")))
            pscs.append({
                "owner_name": canon,
                "owner_kind": kind,
                "owner_rc": orc or (p.get("ownerRc") or "") or None,
                "owner_jurisdiction": juris,
                "nationality": p.get("nationality") or None,
                "notified": p.get("notified") or None,
                "shares": bool(p.get("c1_shares")),
                "share_pct_direct": p.get("shrDir"),
                "share_pct_indirect": p.get("shrInd"),
                "voting": bool(p.get("c2_voting")),
                "voting_pct_direct": p.get("voteDir"),
                "voting_pct_indirect": p.get("voteInd"),
                "appoint_board": bool(p.get("c3_appoint")),
                "sig_influence_company": bool(p.get("c4_sigInflCo")),
                "sig_influence_trust_firm": bool(p.get("c5_sigInflTrustFirm")),
                "owner_name_raw": name_raw,  # provenance: preserve source string
            })
        index[lei] = {
            "company": e["company"].strip().rstrip("."),
            "rc": e["rc"],
            "lei": lei,
            "lei_status": e.get("leiStatus"),
            "status": e.get("status"),
            "pscs": pscs,
        }
    return {
        "meta": {
            "ra_code": RA_CODE,
            "register": (
                "Corporate Affairs Commission (CAC) — "
                "Persons with Significant Control register"
            ),
            "source_url": "https://bor.cac.gov.ng",
            "harvested": raw.get("harvested"),
            "entities": len(index),
            "psc_rows": sum(len(v["pscs"]) for v in index.values()),
            "skipped_blank_rows": skipped_blank,
            "note": (
                "Curated example set harvested from the CAC BOR public search API "
                "(borapp.cac.gov.ng). Owner names normalised at build time; blank "
                "owner rows (source data gaps) dropped; dates of birth omitted for privacy."
            ),
        },
        "index": index,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-raw", default=str(DATA / "cac_nigeria_raw.json"))
    ap.add_argument("--out", default=str(DATA / "cac_nigeria_psc.json"))
    args = ap.parse_args()

    with open(args.from_raw, encoding="utf-8") as rf:
        raw = json.load(rf)
    built = normalise(raw)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(built, f, indent=2, ensure_ascii=False)
        f.write("\n")
    m = built["meta"]
    print(f"Wrote {args.out}: {m['entities']} entities, {m['psc_rows']} PSC rows, "
          f"{m['skipped_blank_rows']} blank rows skipped.")


if __name__ == "__main__":
    main()
