#!/usr/bin/env python3
"""Harvest and build the committed, LEI-keyed CAC Nigeria PSC index.

OpenCheck is anchored end-to-end on the LEI, but the Nigerian Corporate Affairs
Commission (CAC) beneficial ownership register (``bor.cac.gov.ng``) is keyed on
the company RC number and publishes **no** LEI. Identity resolution is therefore
done **once, offline** (like the ``eiti_soe`` adapter), in two steps:

``harvest``
    Re-pulls a curated set of Nigerian companies from the register's public
    JSON API — the one the website itself calls — and writes
    ``opencheck/data/cac_nigeria_raw.json``. The seed list (RC → LEI) is the
    raw file's own ``entities``, plus any ``--add RC=LEI``. Each LEI is
    re-checked against GLEIF (``registeredAs`` == RC, ``registeredAt`` ==
    ``RA000469``) and a mismatch aborts the harvest.

``build`` (the default)
    Normalises the raw harvest — owner names classified and canonicalised by
    the explicit ``OWNERS`` table — into ``opencheck/data/cac_nigeria_psc.json``
    keyed by LEI. No network.

The register API (verified 2026-09-16, after the site's redesign)
-----------------------------------------------------------------
The redesigned site calls ``https://borapp.cac.gov.ng/api/v1/bor/…``, no login:

* ``GET /companies/search?searchTerm=&page=&size=`` — a **substring** match
  over names and RC numbers (``771`` matches 2,553 companies), ``size`` capped
  at 100, and ``rcNumber`` comes back as ``"RC 2457"`` for some companies and
  a bare number for others. Search ``"RC <n>"`` first, then the company name,
  and accept only an exact normalised RC.
* ``GET /companies/{companyId}/psc`` — every PSC row with ``status``
  ``ACTIVE`` / ``INACTIVE`` / ``CEASED``. The duplicates the August harvest
  had to dedupe by hand are this history.
* ``GET /companies/{companyId}/psc/report.json`` — the same rows as CAC's
  printable report. It confirmed the flag → CAMA condition order used below.

The ``/api/bor-search/get_psc`` endpoints the August 2026 harvest used now
answer 404.

Personal data is excluded at the source
---------------------------------------
``/psc`` returns each owner's email, phone number, full date of birth and
identity-document number to anonymous callers — more than CAC's own report
publishes. The harvester therefore copies an **allowlist** of fields
(``_PSC_FIELDS``), never "everything except", so a personal field CAC adds
later cannot reach the repository by default.

Corporate owners without a name
-------------------------------
The v1 records carry only ``surname`` / ``firstname`` / ``otherName``. A
corporate PSC whose name CAC holds elsewhere comes back with all three blank —
NNPC's two 50% state holders among them, which the August API still named.
Such a row is kept as an **unnamed** owner (``owner_named: false``) so the
stake stays visible instead of vanishing.

Usage::

    python3 -m scripts.build_cac_nigeria_index harvest [--add RC=LEI ...]
    python3 -m scripts.build_cac_nigeria_index build
"""
from __future__ import annotations

import argparse
import json
import re
import time
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
RA_CODE = "RA000469"  # GLEIF Registration Authority code for the CAC (verified 2026-09-16)
API = "https://borapp.cac.gov.ng/api/v1/bor/companies"
GLEIF = "https://api.gleif.org/api/v1/lei-records"

#: The only PSC fields copied into the raw harvest. Personal contact and
#: identity fields (email, phoneNumber, dateOfBirth, identityNumber, address,
#: city, state, lga, occupation, gender) are deliberately absent.
_PSC_FIELDS = (
    "id", "surname", "firstname", "otherName", "nationality", "status",
    "dateOfAppointment", "dateOfPsc", "governingLaw", "register",
    "jurisdiction", "taxResidencyJurisdiction",
    "pscHoldsSharesOrInterest", "pscHoldsSharesOrInterestPercentageDirectly",
    "pscHoldsSharesOrInterestPercentageIndirectly",
    "pscVotingRights", "pscVotingRightsPercentageDirectly",
    "pscVotingRightsPercentageIndirectly",
    "pscRightToAppoint", "pscSignificantInfluence",
    "pscExerciseSignificantInfluence",
    "isPep", "stateOwnedEnterprise", "isSoePlc",
)
_COMPANY_FIELDS = (
    "companyId", "approvedName", "rcNumber", "status", "statusCode",
    "registrationDate", "pscCount",
)

# Corporate-suffix heuristic for classifying an owner as an entity when it is not
# in the explicit normalisation table below.
_CORP_SUFFIX = re.compile(
    r"\b(LTD|LIMITED|PLC|INC|INCORPORATED|SA|S\.A|GMBH|BV|NV|COMPANY|INDUSTRIES|"
    r"HOLDINGS?|GROUP|INTERNATIONAL|NOMINEES|OVERSEAS|FUND|MINISTRY|GOVERNMENT|CORP)\b",
    re.I,
)


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().upper())


def _raw_name(psc: dict[str, Any]) -> str:
    """The owner name as filed: surname, firstname and otherName, joined."""
    parts = (psc.get("surname"), psc.get("firstname"), psc.get("otherName"))
    return re.sub(r"\s+", " ", " ".join(str(p) for p in parts if p)).strip()


# Explicit owner normalisation for this curated set. Maps the owner name as
# filed (surname + firstname + otherName, upper-cased, whitespace collapsed) to
# (canonical name, kind, jurisdiction, RC). Person names are filed in no
# consistent order ("OKON EFFIONG" is surname-first, "JIM OVIA" is not), so
# every person in the set is listed rather than reordered by rule.
# kind ∈ {"entity", "person", "arrangement", "unknown"}.
OWNERS: dict[str, tuple[str, str, str | None, str | None]] = {
    # ── entities ────────────────────────────────────────────────────────
    "DANGOTE INDUSTRIES LTD": ("Dangote Industries Limited", "entity", "NG", None),
    "DANGOTE INDUSTRIES LIMITED": ("Dangote Industries Limited", "entity", "NG", None),
    "STANBIC IBTC NOMINEES NIGERIA LTD":
        ("Stanbic IBTC Nominees Nigeria Limited", "entity", "NG", None),
    "STANBIC NOMINEES NIGERIA LIMITED STANBIC NOMINEES NIGERIA LIMITED 375064":
        ("Stanbic Nominees Nigeria Limited", "entity", "NG", "375064"),
    "UNITED ALLIANCE COMPANY OF NIGERIA LIMITED [409034] "
    "UNITED ALLIANCE COMPANY OF NIGERIA LIMITED [409034]":
        ("United Alliance Company of Nigeria Limited", "entity", "NG", "409034"),
    "MINISTRY OF FINANCE INCORPORATED":
        ("Ministry of Finance Incorporated (MOFI)", "entity", "NG", None),
    "CAPITAL LEISURE AND HOSPITALITY LTD SUBSIDIARY OF TRANSNATIONAL CORPORATION OF NIGERIA PLC":
        ("Capital Leisure and Hospitality Limited", "entity", "NG", None),
    "SOCFINAF S.A": ("Socfinaf SA", "entity", None, None),
    "GREENVIEW INTERNATIONAL CORP.": ("Greenview International Corp.", "entity", None, None),
    "AMPERION POWER DISTRIBUTION COMPANY LIMITED":
        ("Amperion Power Distribution Company Limited", "entity", "NG", None),
    "RIVER STATE GOVERNMENT": ("Rivers State Government", "entity", "NG", None),
    "INDORAMA UNIVERSAL PTE. LIMITED": ("Indorama Universal Pte. Limited", "entity", None, None),
    "PUREBOND LIMITED": ("Purebond Limited", "entity", "NG", None),
    "SIAT SA": ("SIAT SA", "entity", None, None),
    "ZPC/SIPML RSA FUND II - MAIN A/C":
        ("ZPC/SIPML RSA Fund II — Main A/C", "arrangement", "NG", None),
    # ── residual buckets ────────────────────────────────────────────────
    "OTHERS": ("Others (unspecified)", "unknown", None, None),
    "VARIOUS SHAREHOLDERS": ("Various shareholders (unspecified)", "unknown", None, None),
    "PUBLIC --": ("Public shareholders (unspecified)", "unknown", None, None),
    # ── people ──────────────────────────────────────────────────────────
    "BROWN ROGER THOMPSON": ("Roger Thompson Brown", "person", None, None),
    "OKON EFFIONG": ("Effiong Okon", "person", None, None),
    "NDUBISI CHIUGO": ("Chiugo Ndubisi", "person", None, None),
    "ELUMELU TONY": ("Tony Elumelu", "person", None, None),
    "BABATUNDE HASSAN-ODUKALE ODUYIMIKA":
        ("Babatunde Oduyimika Hassan-Odukale", "person", None, None),
    "OLUFEMI OTEDOLA PETER": ("Olufemi Peter Otedola", "person", None, None),
    "OYEDEJI ADEBOWALE": ("Adebowale Oyedeji", "person", None, None),
    "BOUYER MATTHIEU": ("Matthieu Bouyer", "person", None, None),
    "LANGAT BERNARD": ("Bernard Langat", "person", None, None),
    "DOSSOU-AWORET SAMUEL": ("Samuel Dossou-Aworet", "person", None, None),
    "ADEYEMI-BERO ADEMOLA": ("Ademola Adeyemi-Bero", "person", None, None),
    "ISA ABDULRAZAQ": ("Abdulrazaq Isa", "person", None, None),
    "SALEH DANJUMA": ("Saleh Danjuma", "person", None, None),
    "DANJUMA SALEH": ("Saleh Danjuma", "person", None, None),
    "NZEWI MICHAEL OKECHUKWU": ("Michael Okechukwu Nzewi", "person", None, None),
    "UNUIGBE AHONSI": ("Ahonsi Unuigbe", "person", None, None),
    "BABATUNDE FOLAWIYO": ("Babatunde Folawiyo", "person", None, None),
    "AIG IMOUKHUEDE AIGBOJE": ("Aigboje Aig-Imoukhuede", "person", None, None),
    "DANGOTE ALIKO": ("Aliko Dangote", "person", None, None),
    "ALIKO DANGOTE": ("Aliko Dangote", "person", None, None),
    "OBI IBEKWE": ("Obi Ibekwe", "person", None, None),
    "ASWANI SAJEN GHANSHADAS": ("Sajen Ghanshadas Aswani", "person", None, None),
    "ASWANI HARKISHIN GHANSHADAS": ("Harkishin Ghanshadas Aswani", "person", None, None),
    "ASWANI NARINDER KUMAR GHANSHADAS":
        ("Narinder Kumar Ghanshadas Aswani", "person", None, None),
    "VASWANI RAJESHLAL MOHANLAL": ("Rajeshlal Mohanlal Vaswani", "person", None, None),
    "DAVID-BORHA SOLA": ("Sola David-Borha", "person", None, None),
    "VASWANI MOHAN": ("Mohan Vaswani", "person", None, None),
    "ADNANI VISHAMKAR TIKAMDAS": ("Vishamkar Tikamdas Adnani", "person", None, None),
    "MAJIYAGBE BABATUNDE JAMES (REPRESENTING STANBIC IBTC NOMINEES LIMITED)":
        ("Babatunde James Majiyagbe", "person", None, None),
    "JIM OVIA": ("Jim Ovia", "person", None, None),
    "MUPITA RALPH": ("Ralph Mupita", "person", None, None),
    "RABIU ABDUL SAMAD": ("Abdul Samad Rabiu", "person", None, None),
    "FEMI BOLA-SADIPE": ("Femi Bola-Sadipe", "person", "NG", None),
    "BASHIR TAHIR": ("Bashir Tahir", "person", "NG", None),
}


def _classify(raw_name: str) -> tuple[str, str, str | None, str | None]:
    key = _norm_key(raw_name)
    if key in OWNERS:
        return OWNERS[key]
    canon = re.sub(r"\s+", " ", raw_name.title()).strip()
    kind = "entity" if _CORP_SUFFIX.search(key) else "person"
    return canon, kind, None, None


def _pct(v: Any) -> float | int | None:
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def normalise(raw: dict) -> dict:
    """Raw harvest → the committed LEI-keyed index. Pure; no network."""
    index: dict[str, dict] = {}
    unclassified: set[str] = set()
    for e in raw["entities"]:
        lei = (e["lei"] or "").strip().upper()
        company = e.get("company") or {}
        pscs: list[dict] = []
        for p in e["pscs"]:
            name_raw = _raw_name(p)
            named = bool(name_raw)
            if named:
                if _norm_key(name_raw) not in OWNERS:
                    unclassified.add(name_raw)
                canon, kind, juris, orc = _classify(name_raw)
            else:
                # A corporate PSC whose name the register does not publish.
                canon, kind, juris, orc = None, "entity", None, None
            vote_ind = p.get("pscVotingRightsPercentageIndirectly")
            pscs.append({
                "psc_id": p.get("id"),
                "psc_status": p.get("status") or None,
                "owner_name": canon,
                "owner_named": named,
                "owner_kind": kind,
                "owner_rc": orc,
                "owner_jurisdiction": juris,
                "nationality": p.get("nationality") or None,
                "notified": p.get("dateOfPsc") or None,
                "appointed": p.get("dateOfAppointment") or None,
                "shares": bool(p.get("pscHoldsSharesOrInterest")),
                "share_pct_direct": _pct(p.get("pscHoldsSharesOrInterestPercentageDirectly")),
                "share_pct_indirect": _pct(p.get("pscHoldsSharesOrInterestPercentageIndirectly")),
                "voting": bool(p.get("pscVotingRights")),
                "voting_pct_direct": _pct(p.get("pscVotingRightsPercentageDirectly")),
                "voting_pct_indirect": _pct(vote_ind),
                "appoint_board": bool(p.get("pscRightToAppoint")),
                "sig_influence_company": bool(p.get("pscSignificantInfluence")),
                "sig_influence_trust_firm": bool(p.get("pscExerciseSignificantInfluence")),
                "owner_name_raw": name_raw or None,  # provenance: the string as filed
            })
        index[lei] = {
            "company": (company.get("approvedName") or "").strip().rstrip("."),
            "rc": e["rc"],
            "lei": lei,
            "lei_status": e.get("leiStatus"),
            "status": company.get("status"),
            "pscs": pscs,
        }
    if unclassified:
        # Not fatal — the suffix heuristic still classifies — but a curated set
        # should name every owner explicitly.
        print("Owner names not in OWNERS (classified by heuristic):")
        for n in sorted(unclassified):
            print(f"  {n!r}")
    rows = [p for v in index.values() for p in v["pscs"]]
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
            "psc_rows": len(rows),
            "psc_rows_current": sum(1 for p in rows if p["psc_status"] == "ACTIVE"),
            "unnamed_owner_rows": sum(1 for p in rows if not p["owner_named"]),
            "note": (
                "Curated example set harvested from the CAC BOR public API "
                "(borapp.cac.gov.ng/api/v1). Every PSC row is kept with its register "
                "status (ACTIVE / INACTIVE / CEASED); owner names normalised at build "
                "time; rows whose owner name the register does not publish are kept as "
                "unnamed owners. No contact details, dates of birth or identity numbers."
            ),
        },
        "index": index,
    }


# ── harvest (network) ─────────────────────────────────────────────────────


def _get(url: str) -> Any:
    req = urllib.request.Request(
        url, headers={"Accept": "application/json", "Origin": "https://bor.cac.gov.ng"}
    )
    last: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as f:
                return json.load(f)
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def _digits(rc: Any) -> str:
    return re.sub(r"\D", "", str(rc or ""))


def _resolve_company(rc: str, name: str | None) -> dict[str, Any]:
    for term in (f"RC {rc}", name, rc):
        if not term:
            continue
        for page in (1, 2, 3):
            q = urllib.parse.urlencode({"searchTerm": term, "page": page, "size": 100})
            d = _get(f"{API}/search?{q}")
            time.sleep(0.4)
            for c in d.get("companyList") or []:
                if _digits(c.get("rcNumber")) == rc:
                    return c
            if not (d.get("pagination") or {}).get("hasNext"):
                break
    raise RuntimeError(f"RC {rc} ({name}) not found in the CAC BOR")


def _check_leis(seed: dict[str, str]) -> dict[str, tuple[str, str]]:
    """RC → LEI must hold in GLEIF (registeredAs == RC at RA000469).

    Returns RC → (LEI registration status, GLEIF legal name); the name is the
    search fallback for a short RC, whose substring matches run to thousands.
    """
    q = urllib.parse.urlencode({"filter[lei]": ",".join(seed.values()), "page[size]": 200})
    d = _get(f"{GLEIF}?{q}")
    by = {r["id"]: r["attributes"] for r in d.get("data") or []}
    status: dict[str, tuple[str, str]] = {}
    for rc, lei in seed.items():
        a = by.get(lei)
        if a is None:
            raise RuntimeError(f"LEI {lei} (RC {rc}) not found in GLEIF")
        ent = a["entity"]
        if _digits(ent.get("registeredAs")) != rc or (ent.get("registeredAt") or {}).get("id") != RA_CODE:
            raise RuntimeError(
                f"LEI {lei} is registeredAs {ent.get('registeredAs')!r} at "
                f"{(ent.get('registeredAt') or {}).get('id')}, not RC {rc} at {RA_CODE}"
            )
        status[rc] = (a["registration"]["status"], ent["legalName"]["name"])
    return status


def harvest(raw_path: Path, add: list[str]) -> dict:
    with open(raw_path, encoding="utf-8") as f:
        old = json.load(f)
    seed: dict[str, str] = {}
    names: dict[str, str] = {}
    for e in old.get("entities") or []:
        seed[str(e["rc"])] = e["lei"].strip().upper()
        c = e.get("company")
        names[str(e["rc"])] = c.get("approvedName") if isinstance(c, dict) else c
    for item in add:
        rc, _, lei = item.partition("=")
        seed[_digits(rc)] = lei.strip().upper()
    lei_status = _check_leis(seed)

    entities = []
    for rc, lei in seed.items():
        c = _resolve_company(rc, names.get(rc) or lei_status[rc][1])
        detail = _get(f"{API}/{c['companyId']}/psc")
        time.sleep(0.4)
        entities.append({
            "rc": rc,
            "lei": lei,
            "leiStatus": lei_status[rc][0],
            "company": {k: detail.get(k, c.get(k)) for k in _COMPANY_FIELDS},
            "pscs": [{k: p.get(k) for k in _PSC_FIELDS} for p in detail.get("pscList") or []],
        })
        print(f"  RC {rc:>8}  {detail.get('approvedName')}: {len(entities[-1]['pscs'])} PSC rows")
    return {
        "note": (
            "CAC BOR (Nigeria Corporate Affairs Commission) Persons with Significant "
            "Control records, harvested from the register's public API "
            "(borapp.cac.gov.ng/api/v1/bor/companies/{companyId}/psc). Values are as "
            "published, anomalies included; fields are copied from an allowlist, so no "
            "contact details, dates of birth or identity numbers are held."
        ),
        "raCode": RA_CODE,
        "source": "https://bor.cac.gov.ng",
        "api": API,
        "harvested": date.today().isoformat(),
        "conditionsLegend": {
            "pscHoldsSharesOrInterest": "CAMA condition 1 — holds at least 5% of shares or interest",
            "pscVotingRights": "CAMA condition 2 — holds at least 5% of voting rights",
            "pscRightToAppoint": "CAMA condition 3 — right to appoint or remove a majority of directors",
            "pscSignificantInfluence": "CAMA condition 4 — significant influence or control over the company",
            "pscExerciseSignificantInfluence": (
                "CAMA condition 5 — significant influence or control over a trust or firm"
            ),
        },
        "entities": entities,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", nargs="?", default="build", choices=("build", "harvest"))
    ap.add_argument("--raw", "--from-raw", dest="raw", default=str(DATA / "cac_nigeria_raw.json"))
    ap.add_argument("--out", default=str(DATA / "cac_nigeria_psc.json"))
    ap.add_argument("--add", action="append", default=[], metavar="RC=LEI",
                    help="harvest: add a company to the seed list")
    args = ap.parse_args()

    if args.command == "harvest":
        raw = harvest(Path(args.raw), args.add)
        with open(args.raw, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, ensure_ascii=False)
            f.write("\n")
        print(f"Wrote {args.raw}: {len(raw['entities'])} companies.")
        return

    with open(args.raw, encoding="utf-8") as rf:
        raw = json.load(rf)
    built = normalise(raw)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(built, f, indent=2, ensure_ascii=False)
        f.write("\n")
    m = built["meta"]
    print(f"Wrote {args.out}: {m['entities']} entities, {m['psc_rows']} PSC rows "
          f"({m['psc_rows_current']} current, {m['unnamed_owner_rows']} unnamed).")


if __name__ == "__main__":
    main()
