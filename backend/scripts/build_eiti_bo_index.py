#!/usr/bin/env python3
"""Build the committed, LEI-keyed pooled EITI beneficial ownership index.

OpenCheck treats the national beneficial ownership registers of EITI
implementing countries as **one pooled source** (``eiti_bo``), not one adapter
per register. The 2024 EITI Requirement 2.5 stocktake marks 15 countries'
registers "publicly available"; live verification (2026-08-18) found only four
worth pooling, each needing a different sourcing approach:

* **DRC** — ITIE-RDC "Registre des propriétaires effectifs"
  (https://www.itierdc.net/donnees/), the only EITI BO register anywhere with a
  bulk download. One-click XLSX export; 54 companies / 202 owners; ownership %,
  voting rights and PEP flags; legalised by Loi n°25/048 (1 July 2025).
* **Armenia** — beneficial ownership declarations on the State Register
  (https://old.e-register.am/, moved from e-register.am). Per-declaration
  **BODS v0.2 JSON** download, no auth. The extractives seed list is EITI
  Armenia's declaring-companies page (27 metal-ore mining companies, each
  linked to its e-register entry). Declarations are current — most companies
  filed in 2026.
* **Nigeria** — the committed ``cac_nigeria`` harvest (CAC PSC register,
  RA000469), filtered to the companies NEITI's solid-minerals audits cover.
  The NEITI BO portal itself (bo.neiti.gov.ng) is frozen (~2023 vintage; the
  oil & gas search returns HTTP 500), so it is used as *filter evidence* only,
  never as data — per-company evidence strings below carry the dates.
* **Indonesia** — slot reserved, harvest deferred: the AHU ``getReportBo`` API
  has been in maintenance and the public search needs CSRF/session handling.
  ``harvest-indonesia`` exists so the pipeline shape is complete from day one.

Out of the pool (decisions recorded on the Notion ticket, 2026-08-18):
Tajikistan (pbo.eiti.tj asserts all-rights-reserved) and Trinidad & Tobago
(register frozen ~2021).

Identity resolution is done **once, offline** (the ``eiti_soe`` /
``cac_nigeria`` pattern): ``harvest-gleif`` snapshots every GLEIF LEI record
for AM and CD (80 + 35 records — small enough to take whole), and ``build``
matches each harvested company by registration number first
(``registeredAs`` / ``validatedAs`` equality — e.g. Zangezur's ``27.140.00009``
matches exactly), then by normalised-name equality as a lower-confidence
fallback. **The launch index is LEI-only** — unmatched companies stay in the
committed raw harvests and are counted in the manifest, but are not served.

Usage::

    python3 -m scripts.build_eiti_bo_index harvest-drc
    python3 -m scripts.build_eiti_bo_index harvest-armenia
    python3 -m scripts.build_eiti_bo_index harvest-gleif
    python3 -m scripts.build_eiti_bo_index build

``build`` is offline and deterministic given the committed raw artifacts; the
three harvest subcommands hit the network and rewrite the raws (re-run them to
refresh the source data, then re-run ``build`` and commit the diff).
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"

RAW_DRC = DATA / "eiti_bo_raw_drc.json.gz"
RAW_ARMENIA = DATA / "eiti_bo_raw_armenia.json.gz"
RAW_GLEIF = DATA / "eiti_bo_raw_gleif.json.gz"
OUT_INDEX = DATA / "eiti_bo_index.json.gz"
CAC_INDEX = DATA / "cac_nigeria_psc.json"

_UA = {"User-Agent": "OpenCheck eiti_bo harvester (https://opencheck.world)"}

# ---------------------------------------------------------------------------
# Register metadata (shared by build + adapter via the index meta block)
# ---------------------------------------------------------------------------

REGISTERS: dict[str, dict[str, str]] = {
    "drc_itie": {
        "name": "ITIE-RDC — Registre des propriétaires effectifs",
        "url": "https://www.itierdc.net/donnees/",
        "country": "CD",
        "licence": "No licence stated; public statutory register (Loi n°25/048 du 1 juillet 2025). Included with attribution.",
    },
    "armenia_eregister": {
        "name": "Armenia State Register — beneficial ownership declarations",
        "url": "https://old.e-register.am/",
        "country": "AM",
        "licence": "No licence stated; public register publishing BODS v0.2 declarations. Included with attribution.",
    },
    "nigeria_cac": {
        "name": "Nigeria CAC — Persons with Significant Control register (NEITI solid-minerals subset)",
        "url": "https://bor.cac.gov.ng",
        "country": "NG",
        "licence": "Public register (bor.cac.gov.ng); no non-commercial restriction.",
    },
    "indonesia_ahu": {
        "name": "Indonesia AHU — Pemilik Manfaat (beneficial ownership) register",
        "url": "https://ahu.go.id/pencarian/profil-pemilik-manfaat",
        "country": "ID",
        "licence": "No licence stated. Harvest deferred (API in maintenance).",
    },
}

# Nigeria pool filter: RC number -> dated evidence that the company is covered
# by NEITI's solid-minerals audits (the extractives scope for Requirement 2.5).
# Stephen's condition (Notion ticket, 2026-08-18): the stale NEITI list is
# acceptable as a *filter* as long as the dates are clearly shown.
NEITI_EXTRACTIVES: dict[str, str] = {
    # BUA Cement Plc
    "1193879": (
        "Listed in the NEITI beneficial ownership portal solid-minerals search "
        "(bo.neiti.gov.ng, data vintage ~2023; presence re-verified 2026-08-19) "
        "and covered by NEITI solid-minerals audit reports."
    ),
    # Dangote Cement Plc
    "208767": (
        "Covered by NEITI solid-minerals audit reports (e.g. 2019/2020 Solid "
        "Minerals Audit; NEITI reported Dangote's 31% share of 2019 solid-"
        "minerals revenue). NEITI BO portal data vintage ~2023."
    ),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_gz(path: Path, payload: dict[str, Any]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
        f.write("\n")
    print(f"wrote {path} ({path.stat().st_size:,} bytes)")


def _read_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# harvest-drc
# ---------------------------------------------------------------------------

_DRC_EXPORT = "https://www.itierdc.net/exports/proprietaires-effectifs-excel/"
#: The register's null-date placeholder (a raw epoch-ish sentinel).
_DRC_NULL_DATE = "30/11/-0001"


def harvest_drc() -> None:
    import io

    import openpyxl
    import requests

    r = requests.get(_DRC_EXPORT, timeout=180, headers=_UA)
    r.raise_for_status()
    wb = openpyxl.load_workbook(io.BytesIO(r.content))
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    # Row 0: title; row 1: "Date d'exportation : … | Nombre d'enregistrements: …";
    # row 3: column headers; data from row 4.
    stamp = str(rows[1][0] or "")
    header = [str(h or "") for h in rows[3]]
    companies: dict[str, dict[str, Any]] = {}
    for row in rows[4:]:
        if not row or not row[1]:
            continue
        (_, entreprise, acronyme, nif, secteur, ville, pays, owner, sexe,
         nationalite, residence, controle, n_actions, pct_actions, pct_votes,
         ppe, ppe_fonction, acquis) = row[:18]
        key = str(nif or entreprise)
        comp = companies.setdefault(key, {
            "name": str(entreprise).strip(),
            "acronym": (str(acronyme).strip() or None) if acronyme else None,
            "nif": str(nif).strip() if nif else None,
            "sector": str(secteur).strip() if secteur else None,
            "city": str(ville).strip() if ville else None,
            "country": str(pays).strip() if pays else None,
            "owners": [],
        })
        acquired = str(acquis).strip() if acquis else None
        if acquired == _DRC_NULL_DATE:
            acquired = None
        comp["owners"].append({
            "name": str(owner).strip() if owner else None,
            "sex": str(sexe).strip() if sexe else None,
            "nationality_fr": str(nationalite).strip() if nationalite else None,
            "residence": str(residence).strip() if residence else None,
            "control_type": str(controle).strip() if controle else None,
            "n_shares": n_actions,
            "pct_shares_raw": pct_actions,
            "pct_voting_raw": pct_votes,
            "pep": (str(ppe).strip().lower() == "oui") if ppe else False,
            "pep_role": str(ppe_fonction).strip() if ppe_fonction else None,
            "acquired": acquired,
        })
    payload = {
        "register": REGISTERS["drc_itie"],
        "harvested": _now_iso(),
        "export_stamp": stamp,
        "header": header,
        "companies": sorted(companies.values(), key=lambda c: c["name"]),
    }
    _write_gz(RAW_DRC, payload)
    n_owners = sum(len(c["owners"]) for c in companies.values())
    print(f"DRC: {len(companies)} companies, {n_owners} owner rows. {stamp!r}")


# ---------------------------------------------------------------------------
# harvest-armenia
# ---------------------------------------------------------------------------

_EITI_AM_LIST = (
    "https://www.eiti.am/hy/%D4%BB%D5%8D-%D5%B0%D5%A1%D5%B5%D5%BF%D5%A1"
    "%D6%80%D5%A1%D6%80%D5%A1%D5%A3%D5%A5%D6%80/?tab=88"
)
_EREG = "https://old.e-register.am"


def harvest_armenia() -> None:
    import requests
    from bs4 import BeautifulSoup

    s = requests.Session()
    s.headers.update(_UA)

    r = s.get(_EITI_AM_LIST, timeout=90)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    seed: list[dict[str, Any]] = []
    for tr in soup.find_all("tr"):
        a = tr.find("a", href=re.compile(r"e-register\.am/(?:am|en|ru)/companies/(\d+)"))
        tds = tr.find_all("td")
        if not a or not tds:
            continue
        cid = re.search(r"companies/(\d+)", a["href"]).group(1)
        seed.append({
            "eregister_id": cid,
            "name_am": tds[0].get_text(" ", strip=True),
            "status_am": tds[-1].get_text(" ", strip=True) if len(tds) > 2 else None,
        })
    print(f"EITI Armenia seed list: {len(seed)} companies")

    companies: list[dict[str, Any]] = []
    for c in seed:
        cid = c["eregister_id"]
        entry: dict[str, Any] = dict(c)
        try:
            # Company page: public registration number + TIN (the LEI-match keys).
            rc = s.get(f"{_EREG}/en/companies/{cid}", timeout=60)
            m = re.search(r"Գրանցման համար\D*([\d.]+)", rc.text)
            entry["regnum"] = m.group(1) if m else None
            m = re.search(r"ՀՎՀՀ\D*(\d{8})", rc.text)
            entry["tin"] = m.group(1) if m else None

            # Declarations list ("ԻՇ հայտարարագրեր"): uuid + timestamp rows.
            rb = s.get(f"{_EREG}/en/companies/{cid}/bor", timeout=60)
            uuids = re.findall(r"declaration/([0-9a-f-]{36})", rb.text)
            dates = re.findall(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", rb.text)
            decls = (
                list(zip(uuids, dates))
                if len(uuids) == len(dates)
                else [(u, "") for u in uuids]
            )
            decls.sort(key=lambda x: x[1])
            entry["declarations"] = [{"uuid": u, "date": d} for u, d in decls]
            if decls:
                uuid, ddate = decls[-1]
                rj = s.get(f"{_EREG}/en/declaration/{uuid}/json", timeout=90)
                rj.raise_for_status()
                entry["latest"] = {
                    "uuid": uuid,
                    "date": ddate,
                    "url": f"{_EREG}/en/companies/{cid}/declaration/{uuid}",
                    "bods": rj.json(),
                }
            else:
                entry["latest"] = None
        except Exception as exc:  # noqa: BLE001
            entry["error"] = str(exc)
        companies.append(entry)
        n = len(entry.get("declarations") or [])
        latest = (entry.get("latest") or {}).get("date")
        print(f"  {cid}: {n} declarations, latest {latest}")
        time.sleep(0.4)

    payload = {
        "register": REGISTERS["armenia_eregister"],
        "seed_list": {
            "url": _EITI_AM_LIST,
            "description": (
                "EITI Armenia list of EITI-declaring metal-ore mining companies "
                "(status column dated 2023-07-31); declarations themselves are "
                "fetched live from old.e-register.am and carry their own dates."
            ),
        },
        "harvested": _now_iso(),
        "companies": companies,
    }
    _write_gz(RAW_ARMENIA, payload)
    ok = sum(1 for c in companies if (c.get("latest") or {}).get("bods"))
    print(f"Armenia: {len(companies)} companies, {ok} with a BODS declaration")


# ---------------------------------------------------------------------------
# harvest-gleif
# ---------------------------------------------------------------------------


def harvest_gleif() -> None:
    import requests

    s = requests.Session()
    s.headers.update({**_UA, "Accept": "application/vnd.api+json"})

    def _all_records(cc: str) -> list[dict[str, Any]]:
        recs: list[dict[str, Any]] = []
        page = 1
        while True:
            r = s.get(
                "https://api.gleif.org/api/v1/lei-records",
                params={
                    "filter[entity.legalAddress.country]": cc,
                    "page[size]": 200,
                    "page[number]": page,
                },
                timeout=120,
            )
            r.raise_for_status()
            d = r.json()
            for rec in d.get("data", []):
                a = rec["attributes"]
                e = a["entity"]
                names = [e.get("legalName", {}).get("name", "")]
                names += [o.get("name", "") for o in (e.get("otherNames") or [])]
                names += [
                    o.get("name", "")
                    for o in (e.get("transliteratedOtherNames") or [])
                ]
                recs.append({
                    "lei": a["lei"],
                    "names": [n for n in names if n],
                    "registeredAs": e.get("registeredAs"),
                    "validatedAs": (a.get("registration") or {}).get("validatedAs"),
                    "entity_status": e.get("status"),
                    "registration_status": (a.get("registration") or {}).get("status"),
                })
            last = d.get("meta", {}).get("pagination", {}).get("lastPage", 1)
            if page >= last:
                break
            page += 1
        return recs

    payload: dict[str, Any] = {"harvested": _now_iso(), "countries": {}}
    for cc in ("AM", "CD"):
        recs = _all_records(cc)
        payload["countries"][cc] = recs
        print(f"GLEIF {cc}: {len(recs)} LEI records")
    _write_gz(RAW_GLEIF, payload)


# ---------------------------------------------------------------------------
# build (offline, deterministic)
# ---------------------------------------------------------------------------

#: Legal-form noise stripped before name comparison — includes spelled-out
#: forms (GLEIF often has "Closed Joint Stock Company" where the register has
#: CJSC or an Armenian-script equivalent).
_SUFFIX_RE = re.compile(
    r"\b(CLOSED JOINT[ -]STOCK COMPANY|OPEN JOINT[ -]STOCK COMPANY|"
    r"JOINT[ -]STOCK COMPANY|LIMITED LIABILITY COMPANY|CJSC|OJSC|LLC|LTD|"
    r"LIMITED|COMPANY|CO|PLC|SARLU|SARL|SASU|SAS|SA|SPRL|JSC|INC|"
    r"CORPORATION|CORP|CONCERN|COMBINE)\b",
    re.I,
)


def _norm_name(name: str) -> str:
    n = unicodedata.normalize("NFKD", (name or "").upper())
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = re.sub(r"[«»“”\"'’`.,()/–—-]", " ", n)
    n = _SUFFIX_RE.sub(" ", n)
    return re.sub(r"\s+", " ", n).strip()


def _digits(v: str | None) -> str:
    return re.sub(r"\D+", "", v or "")


def _match_gleif(
    gleif: list[dict[str, Any]],
    *,
    reg_ids: list[str],
    names: list[str],
) -> tuple[dict[str, Any], str, str] | None:
    """Match one company against a country's GLEIF snapshot.

    Registration-number equality (punctuation-insensitive) wins with high
    confidence; exact normalised-name equality is the medium-confidence
    fallback. Returns ``(record, method, confidence)`` or ``None``.
    """
    id_forms = {f for v in reg_ids for f in (v, _digits(v)) if v and f}
    for g in gleif:
        for field in ("registeredAs", "validatedAs"):
            v = g.get(field)
            if v and (v in id_forms or _digits(v) in id_forms):
                return g, f"registration-number ({field})", "high"
    name_forms = {_norm_name(n) for n in names if n}
    name_forms.discard("")
    for g in gleif:
        for gn in g.get("names") or []:
            if _norm_name(gn) in name_forms:
                return g, "name", "medium"
    return None


def _drc_normalise_percentages(comp: dict[str, Any]) -> None:
    """Normalise the register's mixed percentage semantics, keeping the raws.

    Filers are inconsistent: most enter fractions of 1 (a company's owner rows
    sum to ~1.0 = 100%), but some enter literal percentages (Congo Dongfang's
    105 owner rows are the shareholder register of its listed parent, each a
    small literal %). Heuristic: if a company's share values sum to ≤ 1.05 the
    values are fractions (× 100); otherwise they are literal percentages. The
    raw values stay in ``pct_shares_raw`` / ``pct_voting_raw`` and the mapper
    annotates the transformation.
    """
    owners = comp.get("owners") or []
    total = sum(
        o["pct_shares_raw"]
        for o in owners
        if isinstance(o.get("pct_shares_raw"), (int, float))
    )
    fraction = 0 < total <= 1.05
    comp["pct_semantics"] = "fraction-of-1" if fraction else "literal-percent"
    for o in owners:
        for raw_key, out_key in (
            ("pct_shares_raw", "pct_shares"),
            ("pct_voting_raw", "pct_voting"),
        ):
            v = o.get(raw_key)
            if isinstance(v, (int, float)) and v > 0:
                o[out_key] = round(v * 100, 4) if fraction else round(float(v), 4)
            else:
                o[out_key] = None


def _armenia_subject(bods: list[dict[str, Any]], seed_name: str) -> dict[str, Any] | None:
    """Find the declaring (root subject) entity statement in a BODS 0.2 array.

    Primary rule: an entity referenced as an OOC ``subject`` but never as an
    ``interestedParty``. Some declarations have none (chains that loop back);
    fall back to the entity whose name matches the EITI Armenia seed name,
    then to the most frequent subject.
    """
    ents = {
        x.get("statementID"): x
        for x in bods
        if x.get("statementType") == "entityStatement"
    }
    subj: list[str] = []
    ip: set[str] = set()
    for x in bods:
        if x.get("statementType") != "ownershipOrControlStatement":
            continue
        sid = (x.get("subject") or {}).get("describedByEntityStatement")
        if sid:
            subj.append(sid)
        pid = (x.get("interestedParty") or {}).get("describedByEntityStatement")
        if pid:
            ip.add(pid)
    roots = [ents[i] for i in dict.fromkeys(subj) if i not in ip and i in ents]
    if roots:
        return roots[0]
    seed_norm = _norm_name(seed_name)
    for e in ents.values():
        if _norm_name(e.get("name") or "") == seed_norm:
            return e
    counts: dict[str, int] = {}
    for sid in subj:
        counts[sid] = counts.get(sid, 0) + 1
    best = max(counts, key=counts.get) if counts else None  # type: ignore[arg-type]
    return ents.get(best) if best else None


def build() -> None:
    drc = _read_gz(RAW_DRC)
    armenia = _read_gz(RAW_ARMENIA)
    gleif = _read_gz(RAW_GLEIF)
    with open(CAC_INDEX, encoding="utf-8") as f:
        cac = json.load(f)

    index: dict[str, dict[str, Any]] = {}
    manifest: dict[str, Any] = {"registers": {}}

    # ── DRC ────────────────────────────────────────────────────────────────
    gleif_cd = gleif["countries"].get("CD") or []
    matched_cd: list[str] = []
    unmatched_cd: list[str] = []
    # The register's export stamp is "Date d'exportation : DD/MM/YYYY HH:MM | …";
    # records carry no per-record declaration date, so the export date is the
    # register's "current as of" claim for every DRC record.
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", str(drc.get("export_stamp") or ""))
    drc_export_iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None
    for comp in drc["companies"]:
        _drc_normalise_percentages(comp)
        names = [comp["name"]] + ([comp["acronym"]] if comp.get("acronym") else [])
        hit = _match_gleif(gleif_cd, reg_ids=[comp.get("nif") or ""], names=names)
        if hit is None:
            unmatched_cd.append(comp["name"])
            continue
        g, method, confidence = hit
        matched_cd.append(comp["name"])
        index[g["lei"]] = {
            "lei": g["lei"],
            "lei_registration_status": g.get("registration_status"),
            "register_id": "drc_itie",
            "country": "CD",
            "company": comp["name"],
            "local_ids": {"cd_nif": comp.get("nif")},
            "source_date": drc_export_iso,
            "retrieved": drc.get("harvested"),
            "match": {"method": method, "confidence": confidence},
            "drc": comp,
        }
    manifest["registers"]["drc_itie"] = {
        **REGISTERS["drc_itie"],
        "harvested": drc.get("harvested"),
        "export_stamp": drc.get("export_stamp"),
        "companies_harvested": len(drc["companies"]),
        "owner_rows": sum(len(c["owners"]) for c in drc["companies"]),
        "lei_matched": len(matched_cd),
        "lei_matched_names": matched_cd,
        "note": (
            "No harvested company currently matches a GLEIF LEI record — the "
            "only DRC extractive LEI holder (Kamoto Copper Company) is not in "
            "the register export. The full corpus is committed in "
            "eiti_bo_raw_drc.json.gz."
        ) if not matched_cd else None,
    }

    # ── Armenia ────────────────────────────────────────────────────────────
    gleif_am = gleif["countries"].get("AM") or []
    matched_am: list[str] = []
    unmatched_am: list[str] = []
    n_with_decl = 0
    for comp in armenia["companies"]:
        latest = comp.get("latest") or {}
        bods = latest.get("bods") or []
        if not bods:
            unmatched_am.append(comp.get("name_am") or comp["eregister_id"])
            continue
        n_with_decl += 1
        root = _armenia_subject(bods, comp.get("name_am") or "")
        root_names = []
        if root:
            root_names = [root.get("name") or ""] + list(root.get("alternateNames") or [])
        hit = _match_gleif(
            gleif_am,
            reg_ids=[comp.get("regnum") or "", comp.get("tin") or ""],
            names=root_names + [comp.get("name_am") or ""],
        )
        display = next((n for n in root_names if n and re.search(r"[A-Za-z]", n)), None) or (
            comp.get("name_am") or comp["eregister_id"]
        )
        if hit is None:
            unmatched_am.append(display)
            continue
        g, method, confidence = hit
        matched_am.append(display)
        # Latin display name: the declaration's own Latin alternate first, else
        # the GLEIF record's Latin name (safe — the match is the subject's own
        # LEI record). The register's as-filed name stays in "company".
        latin = next(
            (n for n in root_names if n and re.fullmatch(r"[\x20-\x7E]+", n)),
            None,
        ) or next(
            (n for n in (g.get("names") or []) if re.fullmatch(r"[\x20-\x7E]+", n or "")),
            None,
        )
        index[g["lei"]] = {
            "lei": g["lei"],
            "lei_registration_status": g.get("registration_status"),
            "register_id": "armenia_eregister",
            "country": "AM",
            "company": display,
            "company_latin": latin,
            "local_ids": {
                "am_regnum": comp.get("regnum"),
                "am_tin": comp.get("tin"),
            },
            "source_date": (latest.get("date") or "")[:10] or None,
            "retrieved": armenia.get("harvested"),
            "match": {"method": method, "confidence": confidence},
            "armenia": {
                "eregister_id": comp["eregister_id"],
                "declaration_uuid": latest.get("uuid"),
                "declaration_date": latest.get("date"),
                "declaration_url": latest.get("url"),
                "declarations_on_register": len(comp.get("declarations") or []),
                "bods_v02": bods,
            },
        }
    manifest["registers"]["armenia_eregister"] = {
        **REGISTERS["armenia_eregister"],
        "harvested": armenia.get("harvested"),
        "seed_list": armenia.get("seed_list"),
        "companies_harvested": len(armenia["companies"]),
        "companies_with_declaration": n_with_decl,
        "lei_matched": len(matched_am),
        "lei_matched_names": matched_am,
    }

    # ── Nigeria (cac_nigeria harvest ∩ NEITI solid-minerals coverage) ─────
    matched_ng: list[str] = []
    for lei, rec in (cac.get("index") or {}).items():
        rc = str(rec.get("rc") or "")
        if rc not in NEITI_EXTRACTIVES:
            continue
        matched_ng.append(rec.get("company") or lei)
        index[lei] = {
            "lei": lei,
            "lei_registration_status": rec.get("lei_status"),
            "register_id": "nigeria_cac",
            "country": "NG",
            "company": rec.get("company"),
            "local_ids": {"ng_cac_rc": rc},
            "source_date": (cac.get("meta") or {}).get("harvested"),
            "retrieved": (cac.get("meta") or {}).get("harvested"),
            "match": {"method": "cac_nigeria index (GLEIF RA000469)", "confidence": "high"},
            "neiti_filter_evidence": NEITI_EXTRACTIVES[rc],
            "nigeria": rec,
        }
    manifest["registers"]["nigeria_cac"] = {
        **REGISTERS["nigeria_cac"],
        "harvested": (cac.get("meta") or {}).get("harvested"),
        "companies_harvested": len(cac.get("index") or {}),
        "lei_matched": len(matched_ng),
        "lei_matched_names": matched_ng,
        "filter": (
            "cac_nigeria curated harvest filtered to NEITI solid-minerals-"
            "covered companies; see per-record neiti_filter_evidence for dates."
        ),
    }

    # ── Indonesia (deferred) ───────────────────────────────────────────────
    manifest["registers"]["indonesia_ahu"] = {
        **REGISTERS["indonesia_ahu"],
        "companies_harvested": 0,
        "lei_matched": 0,
        "note": (
            "Harvest deferred: ahu.go.id getReportBo API in maintenance "
            "(verified 2026-08-19); public search requires CSRF/session "
            "handling. Slot reserved."
        ),
    }

    built = {
        "meta": {
            "built": _now_iso(),
            "pool_decision": (
                "One pooled source for all EITI national BO registers "
                "(decision recorded 2026-08-18); LEI-only at launch."
            ),
            "gleif_snapshot": gleif.get("harvested"),
            "entities": len(index),
            "registers": manifest["registers"],
        },
        "index": index,
    }
    _write_gz(OUT_INDEX, built)
    print(f"index: {len(index)} LEI-matched entities")
    for reg, m in manifest["registers"].items():
        print(
            f"  {reg}: {m.get('lei_matched', 0)} of "
            f"{m.get('companies_harvested', 0)} harvested"
        )
    if unmatched_cd:
        print(f"  (DRC unmatched: {len(unmatched_cd)})")
    if unmatched_am:
        print(f"  (Armenia unmatched: {len(unmatched_am)}: {', '.join(unmatched_am[:6])}…)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "command",
        choices=["harvest-drc", "harvest-armenia", "harvest-gleif", "harvest-indonesia", "build"],
    )
    args = ap.parse_args()
    if args.command == "harvest-drc":
        harvest_drc()
    elif args.command == "harvest-armenia":
        harvest_armenia()
    elif args.command == "harvest-gleif":
        harvest_gleif()
    elif args.command == "harvest-indonesia":
        print(
            "Indonesia harvest is deferred: the AHU getReportBo API is in "
            "maintenance and the public search needs CSRF/session handling. "
            "This subcommand is a reserved slot — see the module docstring.",
            file=sys.stderr,
        )
        sys.exit(1)
    else:
        build()


if __name__ == "__main__":
    main()
