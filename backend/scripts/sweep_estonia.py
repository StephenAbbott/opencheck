#!/usr/bin/env python3
"""Phase 2 sweep: build a BODS v0.4 dataset covering every Estonian LEI holder.

Combines two bulk layers — **no API calls**:

1. **GLEIF layer** — the Open Ownership ``gleif_version_0_4`` *parquet* files
   (downloaded by ``setup_bods_data.py``): latest entity statement for every
   EE-jurisdiction record, all relationship statements touching them (real
   edges *and* reporting-exception statements for EE subjects, kept in OO's
   original ``interestedParty: {reason, description}`` form), plus foreign
   boundary entities at the ends of cross-border edges.
2. **ariregister layer** — ``ariregister.db`` (built by
   ``extract_ariregister.py`` from the RIK bulk open-data files): for every
   LEI holder carrying an ``EE-KMKR`` registry-code identifier, shareholders /
   officers / beneficial owners are mapped through the canonical
   ``map_ariregister()`` from ``opencheck.bods``.

The layers join in graph tools via the shared identifier
(scheme ``EE-KMKR``, id = registry code) on both entity statements.

The build is **staged and resumable** (each stage is cheap to re-run):

* ``--stage select``       — DuckDB selection over parquet → ``work/selection.json``
* ``--stage gleif``        — reconstruct GLEIF BODS JSON → ``work/gleif_*.jsonl``
* ``--stage ariregister``  — map the next ``--chunk-size`` companies
  (repeat until ``done``; progress in ``work/ar_state.json``)
* ``--stage assemble``     — dedup, order (entities/persons before
  relationships), gzip; write ``estonia-YYYY-MM-DD.jsonl.gz`` +
  ``manifest.json`` + ``LICENCES.md`` to ``--out-dir``
* ``--stage all``          — run everything in one process (laptop use)

Usage::

    cd backend
    python3 scripts/sweep_estonia.py --stage all \
        [--parquet-dir data/bods/gleif/parquet] \
        [--ariregister-db ../data/ariregister.db] \
        [--out-dir ../data/estonia] [--work-dir ../data/estonia/work] \
        [--limit N] [--no-beneficial-owners]

Licence of the combined output: **CC-BY-4.0** (GLEIF CC0 + RIK CC-BY-4.0;
most restrictive wins).
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))  # opencheck package

EE_SCHEME = "EE-KMKR"  # OO GLEIF mapping + OpenCheck mapper use the same scheme


# ---------------------------------------------------------------------------
# Stage: select
# ---------------------------------------------------------------------------

def stage_select(args) -> dict:
    import duckdb

    pq = Path(args.parquet_dir)
    d = duckdb.connect()
    d.execute(f"""
        CREATE TEMP TABLE latest_ent AS
        SELECT _link, recordId, statementId, recordStatus,
               recordDetails_jurisdiction_code AS juris
        FROM (SELECT *, row_number() OVER
                (PARTITION BY recordId ORDER BY statementDate DESC, _link DESC) rn
              FROM '{pq}/entity_statement.parquet') WHERE rn = 1""")

    lim = f"LIMIT {int(args.limit)}" if args.limit else ""
    cc = d.execute(f"""
        SELECT _link, recordId FROM latest_ent
        WHERE juris = ? AND recordStatus != 'closed' ORDER BY recordId {lim}""",
        [args.jurisdiction]).fetchall()
    cc_links = [r[0] for r in cc]
    cc_records = {r[1] for r in cc}
    d.execute("CREATE TEMP TABLE cc_records (recordId VARCHAR)")
    d.executemany("INSERT INTO cc_records VALUES (?)", [(r,) for r in cc_records])

    # LEI → registry code
    lei_reg: dict[str, str] = {}
    for rec, reg in d.execute(f"""
        SELECT le.recordId, i.id
        FROM latest_ent le
        JOIN cc_records c ON c.recordId = le.recordId
        JOIN '{pq}/entity_recorddetails_identifiers.parquet' i
          ON i._link_entity_statement = le._link
        WHERE i.scheme = '{EE_SCHEME}'""").fetchall():
        reg = str(reg or "").strip()
        if reg.isdigit():
            lei_reg[rec.replace("XI-LEI-", "")] = reg.lstrip("0") or reg

    rels = d.execute(f"""
        SELECT _link, recordDetails_subject, recordDetails_interestedParty,
               recordDetails_interestedParty_reason
        FROM (SELECT *, row_number() OVER
                (PARTITION BY recordId ORDER BY statementDate DESC, _link DESC) rn
              FROM '{pq}/relationship_statement.parquet') r
        WHERE rn = 1 AND recordStatus != 'closed'
          AND (recordDetails_subject IN (SELECT recordId FROM cc_records)
               OR (recordDetails_interestedParty IS NOT NULL AND
                   recordDetails_interestedParty IN (SELECT recordId FROM cc_records)))
        """).fetchall()

    boundary_records: set[str] = set()
    rel_rows = []
    n_exc = 0
    for link, s, ip, reason in rels:
        if reason is not None:
            if s not in cc_records:
                continue
            n_exc += 1
        rel_rows.append((link, s, ip))
        for endpoint in (s, ip):
            if endpoint and endpoint not in cc_records:
                boundary_records.add(endpoint)

    d.execute("CREATE TEMP TABLE brec (recordId VARCHAR)")
    d.executemany("INSERT INTO brec VALUES (?)", [(r,) for r in sorted(boundary_records)])
    boundary = d.execute("""
        SELECT _link, recordId FROM latest_ent
        WHERE recordId IN (SELECT recordId FROM brec) AND recordStatus != 'closed'
        """).fetchall()
    included = cc_records | {r[1] for r in boundary}

    rel_links, dropped = [], 0
    for link, s, ip in rel_rows:
        if s in included and (ip is None or ip in included):
            rel_links.append(link)
        else:
            dropped += 1

    gleif_max_date = d.execute(
        f"SELECT max(statementDate) FROM '{pq}/entity_statement.parquet'"
    ).fetchone()[0]

    sel = {
        "cc_links": cc_links,
        "boundary_links": [r[0] for r in boundary],
        "rel_links": rel_links,
        "lei_reg": lei_reg,
        "stats": {
            "gleif_entities_in_scope": len(cc_links),
            "gleif_boundary_entities": len(boundary),
            "gleif_relationships_selected": len(rel_links),
            "gleif_exception_statements": n_exc,
            "gleif_dangling_relationships_dropped": dropped,
            "lei_with_registry_code": len(lei_reg),
            "gleif_snapshot_max_statement_date": str(gleif_max_date),
        },
    }
    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    (work / "selection.json").write_text(json.dumps(sel))
    print(json.dumps(sel["stats"], indent=1))
    return sel


# ---------------------------------------------------------------------------
# Stage: gleif — bulk BODS JSON reconstruction from parquet
# ---------------------------------------------------------------------------

def _pub_block(row: dict) -> dict:
    pub = {k2: str(row[k1]) for k1, k2 in (
        ("publicationDetails_publicationDate", "publicationDate"),
        ("publicationDetails_bodsVersion", "bodsVersion"),
        ("publicationDetails_license", "license"),
    ) if row.get(k1) not in (None, "")}
    publisher = {k2: str(row[k1]) for k1, k2 in (
        ("publicationDetails_publisher_name", "name"),
        ("publicationDetails_publisher_url", "url"),
    ) if row.get(k1) not in (None, "")}
    if publisher:
        pub["publisher"] = publisher
    return pub


def _src_block(row: dict, asserted: list[dict] | None = None) -> dict:
    src: dict[str, Any] = {}
    if row.get("source_type"):
        src["type"] = [t for t in str(row["source_type"]).split(",") if t]
    if row.get("source_url"):
        src["url"] = row["source_url"]
    if asserted:
        src["assertedBy"] = asserted
    return src


def _alt_names(v) -> list[str]:
    if v in (None, ""):
        return []
    try:
        parsed = json.loads(v)
    except (TypeError, ValueError):
        parsed = v
    if isinstance(parsed, str):
        return [parsed]
    if isinstance(parsed, list):
        return [str(x) for x in parsed if x]
    return []


def stage_gleif(args) -> None:
    import duckdb

    pq = Path(args.parquet_dir)
    work = Path(args.work_dir)
    sel = json.loads((work / "selection.json").read_text())
    d = duckdb.connect()

    def _tmp(name: str, links: list[str]) -> None:
        d.execute(f"CREATE TEMP TABLE {name} (_link VARCHAR)")
        d.executemany(f"INSERT INTO {name} VALUES (?)", [(x,) for x in links])

    _tmp("sel_ent", sel["cc_links"] + sel["boundary_links"])
    _tmp("sel_rel", sel["rel_links"])

    # entity child tables, grouped by parent link
    ids = defaultdict(list)
    for link, i, sch, schn, uri in d.execute(f"""
        SELECT i._link_entity_statement, i.id, i.scheme, i.schemeName, i.uri
        FROM '{pq}/entity_recorddetails_identifiers.parquet' i
        JOIN sel_ent s ON s._link = i._link_entity_statement""").fetchall():
        if i and (sch or schn or uri):
            ids[link].append({k: v for k, v in
                              (("id", i), ("scheme", sch), ("schemeName", schn),
                               ("uri", uri)) if v})
    addrs = defaultdict(list)
    for link, t, a, pc, cn, cc_ in d.execute(f"""
        SELECT x._link_entity_statement, x.type, x.address, x.postCode,
               x.country_name, x.country_code
        FROM '{pq}/entity_recorddetails_addresses.parquet' x
        JOIN sel_ent s ON s._link = x._link_entity_statement""").fetchall():
        addr: dict[str, Any] = {}
        if t:
            addr["type"] = t if t in ("registered", "business", "alternative") else "alternative"
        if a:
            addr["address"] = a
        if pc:
            addr["postCode"] = pc
        if cc_:
            addr["country"] = {"name": cn or cc_, "code": cc_}
        if addr:
            addrs[link].append(addr)
    asserted = defaultdict(list)
    for link, name, uri in d.execute(f"""
        SELECT x._link_entity_statement, x.name, x.uri
        FROM '{pq}/entity_source_assertedby.parquet' x
        JOIN sel_ent s ON s._link = x._link_entity_statement""").fetchall():
        if name or uri:
            asserted[link].append({k: v for k, v in (("name", name), ("uri", uri)) if v})

    cols = [c[0] for c in d.execute(
        f"DESCRIBE SELECT * FROM '{pq}/entity_statement.parquet'").fetchall()]
    with open(work / "gleif_entities.jsonl", "w", encoding="utf-8") as fh:
        for row_t in d.execute(f"""
            SELECT e.* FROM '{pq}/entity_statement.parquet' e
            JOIN sel_ent s ON s._link = e._link ORDER BY e.recordId""").fetchall():
            e = dict(zip(cols, row_t))
            link = e["_link"]
            rd: dict[str, Any] = {
                "isComponent": bool(e.get("recordDetails_isComponent")),
                "entityType": {
                    "type": e.get("recordDetails_entityType_type") or "registeredEntity",
                    **({"details": e["recordDetails_entityType_details"]}
                       if e.get("recordDetails_entityType_details") else {}),
                },
                "name": e.get("recordDetails_name") or "",
            }
            if e.get("recordDetails_jurisdiction_code") or e.get("recordDetails_jurisdiction_name"):
                rd["incorporatedInJurisdiction"] = {
                    "name": e.get("recordDetails_jurisdiction_name")
                    or e.get("recordDetails_jurisdiction_code"),
                    "code": e.get("recordDetails_jurisdiction_code") or "",
                }
            alt = _alt_names(e.get("recordDetails_alternateNames"))
            if alt:
                rd["alternateNames"] = alt
            if e.get("recordDetails_foundingDate"):
                rd["foundingDate"] = str(e["recordDetails_foundingDate"])
            if e.get("recordDetails_dissolutionDate"):
                rd["dissolutionDate"] = str(e["recordDetails_dissolutionDate"])
            if ids.get(link):
                rd["identifiers"] = ids[link]
            if addrs.get(link):
                rd["addresses"] = addrs[link]
            stmt = {
                "statementId": e["statementId"],
                **({"recordId": e["recordId"]} if e.get("recordId") else {}),
                "declarationSubject": e.get("declarationSubject") or e.get("recordId")
                or e["statementId"],
                "recordType": "entity",
                "recordStatus": e.get("recordStatus") or "new",
                **({"statementDate": str(e["statementDate"])} if e.get("statementDate") else {}),
                "recordDetails": rd,
            }
            pub = _pub_block(e)
            if pub:
                stmt["publicationDetails"] = pub
            src = _src_block(e, asserted.get(link))
            if src:
                stmt["source"] = src
            fh.write(json.dumps(stmt, ensure_ascii=False) + "\n")

    # relationships
    interests = defaultdict(list)
    for link, do_i, t, boc, det, sd in d.execute(f"""
        SELECT x._link_relationship_statement, x.directOrIndirect, x.type,
               x.beneficialOwnershipOrControl, x.details, x.startDate
        FROM '{pq}/relationship_recorddetails_interests.parquet' x
        JOIN sel_rel s ON s._link = x._link_relationship_statement""").fetchall():
        i: dict[str, Any] = {}
        if t:
            i["type"] = t
        if do_i:
            i["directOrIndirect"] = do_i
        if boc is not None:
            i["beneficialOwnershipOrControl"] = bool(boc)
        if det:
            i["details"] = det
        if sd:
            i["startDate"] = str(sd)
        if i:
            interests[link].append(i)

    rcols = [c[0] for c in d.execute(
        f"DESCRIBE SELECT * FROM '{pq}/relationship_statement.parquet'").fetchall()]
    with open(work / "gleif_relationships.jsonl", "w", encoding="utf-8") as fh:
        for row_t in d.execute(f"""
            SELECT r.* FROM '{pq}/relationship_statement.parquet' r
            JOIN sel_rel s ON s._link = r._link ORDER BY r.recordId""").fetchall():
            r = dict(zip(rcols, row_t))
            link = r["_link"]
            rd = {
                "isComponent": bool(r.get("recordDetails_isComponent")),
                "subject": r["recordDetails_subject"],
            }
            if r.get("recordDetails_interestedParty"):
                rd["interestedParty"] = r["recordDetails_interestedParty"]
            else:
                unspec: dict[str, str] = {}
                if r.get("recordDetails_interestedParty_reason"):
                    unspec["reason"] = r["recordDetails_interestedParty_reason"]
                if r.get("recordDetails_interestedParty_description"):
                    unspec["description"] = r["recordDetails_interestedParty_description"]
                rd["interestedParty"] = unspec
            if interests.get(link):
                rd["interests"] = interests[link]
            stmt = {
                "statementId": r["statementId"],
                **({"recordId": r["recordId"]} if r.get("recordId") else {}),
                "declarationSubject": r.get("declarationSubject")
                or r["recordDetails_subject"],
                "recordType": "relationship",
                "recordStatus": r.get("recordStatus") or "new",
                **({"statementDate": str(r["statementDate"])} if r.get("statementDate") else {}),
                "recordDetails": rd,
            }
            pub = _pub_block(r)
            if pub:
                stmt["publicationDetails"] = pub
            src = _src_block(r)
            if src:
                stmt["source"] = src
            fh.write(json.dumps(stmt, ensure_ascii=False) + "\n")
    print("gleif stage done")


# ---------------------------------------------------------------------------
# Stage: ariregister (chunked, resumable)
# ---------------------------------------------------------------------------

def _clean_shareholders(rows: list[dict]) -> list[dict]:
    out = []
    for sh in rows:
        sh = dict(sh)
        name = sh.get("nimi_arinimi") or ""
        if name.startswith("Omanikukonto: "):
            sh["nimi_arinimi"] = name[len("Omanikukonto: "):]
        out.append(sh)
    return out


def bundle_from_row(row: sqlite3.Row, *, include_bo: bool) -> dict[str, Any]:
    reg = str(row["registry_code"]).lstrip("0") or str(row["registry_code"])
    return {
        "source_id": "ariregister",
        "registry_code": reg,
        "name": row["name"],
        "legal_form": row["legal_form"] or None,
        "vat_number": row["vat_number"] or None,
        "status": row["status"] or None,
        "registration_date": row["registration_date"] or None,
        "address": row["address"] or None,
        "link": f"https://ariregister.rik.ee/eng/company/{reg}",
        "shareholders": _clean_shareholders(json.loads(row["shareholders"] or "[]")),
        "officers": json.loads(row["officers"] or "[]"),
        "beneficial_owners": (json.loads(row["beneficial_owners"] or "[]")
                              if include_bo else []),
        "is_stub": False,
    }


def stage_ariregister(args) -> bool:
    """Process the next chunk. Returns True when all companies are done."""
    from opencheck.bods import map_ariregister

    work = Path(args.work_dir)
    sel = json.loads((work / "selection.json").read_text())
    items = sorted(sel["lei_reg"].items())
    state_path = work / "ar_state.json"
    state = (json.loads(state_path.read_text())
             if state_path.exists()
             else {"next": 0, "matched": 0, "missing": 0})
    start = state["next"]
    if start >= len(items):
        print("ariregister stage done:", json.dumps(state))
        return True
    chunk = items[start:start + args.chunk_size]

    conn = sqlite3.connect(str(Path(args.ariregister_db)))
    conn.row_factory = sqlite3.Row
    include_bo = not args.no_beneficial_owners
    parts = work / "ar_parts"
    parts.mkdir(exist_ok=True)
    with open(parts / f"part-{start:07d}.jsonl", "w", encoding="utf-8") as fh:
        for _lei, reg in chunk:
            row = conn.execute(
                "SELECT * FROM entities WHERE registry_code IN (?, ?)",
                (reg, reg.zfill(8))).fetchone()
            if row is None:
                state["missing"] += 1
                continue
            state["matched"] += 1
            for stmt in map_ariregister(bundle_from_row(row, include_bo=include_bo)):
                fh.write(json.dumps(stmt, ensure_ascii=False) + "\n")

    state["next"] = start + len(chunk)
    state["total"] = len(items)
    state_path.write_text(json.dumps(state))
    done = state["next"] >= len(items)
    print(json.dumps(state), "done" if done else "more")
    return done


# ---------------------------------------------------------------------------
# Stage: assemble
# ---------------------------------------------------------------------------

LICENCES_MD = """# Data Licences — OpenCheck Estonia sweep

This dataset combines two bulk sources. The combined output is licensed
under the most restrictive of the two: **CC BY 4.0**.

## GLEIF Level 1 & Level 2 data — CC0 1.0

Source: [GLEIF](https://www.gleif.org/), distributed as BODS v0.4 by
Open Ownership: [bods-data.openownership.org/source/gleif_version_0_4](https://bods-data.openownership.org/source/gleif_version_0_4/)
Licence: [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).
No attribution legally required; acknowledging GLEIF and Open Ownership
is good practice.

## Estonian e-Business Register open data — CC BY 4.0

Source: open data of the e-Business Register, published by the Centre of
Registers and Information Systems (RIK):
[avaandmed.ariregister.rik.ee](https://avaandmed.ariregister.rik.ee/en)
Licence: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

> Data from the Estonian e-Business Register (e-Äriregister), published
> by the Centre of Registers and Information Systems (RIK), CC BY 4.0.

## Personal data / GDPR

The Estonian layer contains names of shareholders, officers, and (where
included) beneficial owners, lawfully published as open data by RIK.
Since 1 November 2024 the open-data files contain no personal
identification numbers. Re-users of this dataset are obliged to comply
with the GDPR when further processing personal data (see RIK's terms:
https://avaandmed.ariregister.rik.ee/en/terms-service). This dataset is
a dated snapshot — always check the live registers for current data.
"""


def stage_assemble(args) -> None:
    work = Path(args.work_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sel = json.loads((work / "selection.json").read_text())
    state = json.loads((work / "ar_state.json").read_text())
    if state["next"] < state.get("total", 0):
        raise SystemExit("ariregister stage not finished — run it to completion first")

    today = dt.date.today().isoformat()
    out_path = out_dir / f"estonia-{today}.jsonl.gz"
    seen: set[str] = set()
    counts: dict[str, int] = {}
    dupes = 0
    rel_buf = work / "rel_buffer.jsonl"

    sources = [work / "gleif_entities.jsonl"]
    sources += sorted((work / "ar_parts").glob("part-*.jsonl"))
    sources += [work / "gleif_relationships.jsonl"]

    with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=6) as gz:
        with open(rel_buf, "w", encoding="utf-8") as rb:
            for path in sources:
                with open(path, encoding="utf-8") as fh:
                    for line in fh:
                        stmt = json.loads(line)
                        sid = stmt.get("statementId")
                        if not sid or sid in seen:
                            dupes += 1
                            continue
                        seen.add(sid)
                        rtype = stmt.get("recordType") or "unknown"
                        counts[rtype] = counts.get(rtype, 0) + 1
                        (rb if rtype == "relationship" else gz).write(line)
        # rel_buf is closed (flushed) before being read back
        with open(rel_buf, encoding="utf-8") as fh:
            for line in fh:
                gz.write(line)
    rel_buf.unlink()

    ar_mtime = dt.datetime.fromtimestamp(
        Path(args.ariregister_db).stat().st_mtime).date().isoformat()
    manifest = {
        "dataset": f"estonia-{today}",
        "generated": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "bods_version": "0.4",
        "licence": "CC-BY-4.0 (combined; GLEIF layer CC0-1.0, RIK layer CC-BY-4.0)",
        "statement_counts": counts,
        "total_statements": sum(counts.values()),
        "duplicate_statements_merged": dupes,
        **sel["stats"],
        "ariregister_companies_matched": state["matched"],
        "ariregister_companies_missing": state["missing"],
        "beneficial_owners_included": not args.no_beneficial_owners,
        "sources": {
            "gleif": {"via": "Open Ownership gleif_version_0_4 (parquet)",
                      "snapshot_max_statement_date":
                          sel["stats"]["gleif_snapshot_max_statement_date"]},
            "ariregister": {"via": "RIK bulk open data (extract_ariregister.py)",
                            "db_built": ar_mtime},
        },
        "notes": [
            "Latest statement per recordId; records with recordStatus=closed dropped.",
            "GLEIF reporting-exception statements retained for in-scope subjects "
            "in OO's interestedParty {reason, description} form.",
            "Foreign endpoints of cross-border edges included as boundary entities; "
            "relationships with unresolvable endpoints dropped.",
            "GLEIF and ariregister layers join on the shared EE-KMKR identifier.",
            "Entity/person statements precede all relationship statements.",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1))
    (out_dir / "LICENCES.md").write_text(LICENCES_MD)
    print(json.dumps(manifest, indent=1))


# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default="all",
                    choices=["select", "gleif", "ariregister", "assemble", "all"])
    ap.add_argument("--parquet-dir", default="data/bods/gleif/parquet")
    ap.add_argument("--ariregister-db", default="../data/ariregister.db")
    ap.add_argument("--out-dir", default="../data/estonia")
    ap.add_argument("--work-dir", default=None)
    ap.add_argument("--jurisdiction", default="EE")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap the number of in-scope GLEIF entities (testing)")
    ap.add_argument("--chunk-size", type=int, default=3000)
    ap.add_argument("--no-beneficial-owners", action="store_true")
    args = ap.parse_args()
    if args.work_dir is None:
        args.work_dir = str(Path(args.out_dir) / "work")

    if args.stage in ("select", "all"):
        stage_select(args)
    if args.stage in ("gleif", "all"):
        stage_gleif(args)
    if args.stage in ("ariregister", "all"):
        if args.stage == "all":
            while not stage_ariregister(args):
                pass
        else:
            stage_ariregister(args)
    if args.stage in ("assemble", "all"):
        stage_assemble(args)


if __name__ == "__main__":
    main()
