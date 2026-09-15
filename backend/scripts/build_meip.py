#!/usr/bin/env python3
"""Build the vendored OECD-UNSD MEIP data from the OECD's own files.

MEIP (the OECD-UNSD Multinational Enterprise Information Platform) publishes an
annual "Global Register" of the subsidiaries of the world's 500 largest
multinational enterprises, as a spreadsheet and — since September 2026 — as a
**BODS v0.4** JSON Lines file. Phase 208 made MEIP a registered OpenCheck
source: the OECD's own statements flow through the lookup unmodified, so this
script does not map anything. It packs the two published files into what the
runtime reads:

* ``meip.sqlite`` (Phase 208, ≈40 MB, **not committed** — published as the
  ``meip-bods-2024`` GitHub release asset and downloaded at boot, see
  ``opencheck/meip.py``): every entity and relationship statement of the BODS
  file, keyed by LEI and by group, plus the immediate parent from the
  spreadsheet's "Parent of Subsidiary" column, which the BODS export drops.
* ``data/meip_subsidiaries.json`` + ``data/meip_mne_heads.json`` (Phase 69,
  committed, ≈10 MB): the LEI-keyed subset from the spreadsheet — the
  Subsidiaries tab's fallback when the SQLite file has not been downloaded.

Statements are stored as their ``recordDetails`` (zlib) plus the columns the
runtime queries on; the envelope every statement shares — ``statementDate``,
``source``, ``publicationDetails``, ``recordStatus`` — is stored once in
``meta`` and the build asserts it really is shared. The one statement-level
annotation in the 2024 file (BP's second "MNE Head" row) rides in its own
column.

Usage::

    python3 backend/scripts/build_meip.py --bods data/meip_bods.jsonl \
        --register data/globalregister2024.xlsx --out data/meip.sqlite

    gzip -k data/meip.sqlite      # → meip.sqlite.gz, the release asset
    gh release create meip-bods-2024 data/meip.sqlite.gz \
        --title "OECD-UNSD MEIP BODS release, 31 Dec 2024 register"

``--register`` accepts the XLSX (needs openpyxl) or a CSV export of its
"Group Register" sheet. ``--json-only`` rebuilds just the two committed JSON
files. ``--check-heads`` asks the GLEIF API, one call a second, whether each
MNE head LEI has an ultimate parent — a head that does is a subsidiary, the
Phase 208 review's finding — and writes ``meip_head_warnings.json`` beside
the output; it changes nothing in the data (Stephen, 15 Sept 2026: show the
register as published).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
import time
import unicodedata
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
_SUBS_OUT = _DATA / "meip_subsidiaries.json"
_HEADS_OUT = _DATA / "meip_mne_heads.json"

_HEAD = "MNE Head"

#: The schema version the runtime checks (``meta.schema``).
SCHEMA_VERSION = "1"


# ---------------------------------------------------------------------------
# Spreadsheet
# ---------------------------------------------------------------------------


def _clean(s: object) -> str:
    return "" if s is None else str(s).strip()


def _norm(s: object) -> str:
    """Name key for within-group matching: NFKD, casefold, alphanumerics only."""
    text = unicodedata.normalize("NFKD", _clean(s)).casefold()
    return re.sub(r"[^\w]", "", text)


def read_register(path: Path) -> list[dict[str, str]]:
    """The Global Register rows as dicts keyed by the sheet's column names."""
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as fh:
            return [{k: _clean(v) for k, v in row.items()} for row in csv.DictReader(fh)]
    import openpyxl  # optional dependency — only the build needs it

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet = None
    for name in wb.sheetnames:
        if "group register" in name.lower():
            sheet = wb[name]
            break
    if sheet is None:
        sheet = wb[wb.sheetnames[0]]
    rows_iter = sheet.iter_rows(values_only=True)
    header = [_clean(c) for c in next(rows_iter)]
    out: list[dict[str, str]] = []
    for values in rows_iter:
        row = {header[i]: _clean(v) for i, v in enumerate(values) if i < len(header) and header[i]}
        if any(row.values()):
            out.append(row)
    return out


def _alt_names(raw: str, name: str) -> list[str]:
    out: list[str] = []
    seen = {name.strip().casefold()}
    for part in (raw or "").split(","):
        p = part.strip()
        key = p.casefold()
        if p and key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _ids(row: dict) -> dict:
    ids = {}
    if v := _clean(row.get("OpenCorporates")):
        ids["opencorporates"] = v
    if v := _clean(row.get("PermID")):
        ids["permid"] = v
    if v := _clean(row.get("CapIQ")):
        ids["capiq"] = v
    return ids


def build_json(rows: list[dict[str, str]]) -> tuple[dict, dict]:
    """The Phase 69 LEI-keyed tables (unchanged shape) from the register rows."""
    sub_total: dict[str, int] = defaultdict(int)
    sub_with_lei: dict[str, int] = defaultdict(int)
    for r in rows:
        if _clean(r.get("Hierarchy")) == _HEAD:
            continue
        parent = _clean(r.get("Parent MNE"))
        sub_total[parent] += 1
        if _clean(r.get("LEI")):
            sub_with_lei[parent] += 1

    subs: dict[str, dict] = {}
    heads: dict[str, dict] = {}
    for r in rows:
        lei = _clean(r.get("LEI")).upper()
        if not lei:
            continue
        name = _clean(r.get("Subsidiary Name (Clean)"))
        parent_mne = _clean(r.get("Parent MNE"))
        record = {
            "name": name,
            "iso3": _clean(r.get("ISO3")),
            "parent_mne": parent_mne,
            "address": _clean(r.get("Address")),
            "alt_names": _alt_names(_clean(r.get("Alternative Names")), name),
            "identifiers": _ids(r),
        }
        if _clean(r.get("Hierarchy")) == _HEAD:
            record["subsidiaries_total"] = sub_total.get(parent_mne, 0)
            record["subsidiaries_with_lei"] = sub_with_lei.get(parent_mne, 0)
            heads.setdefault(lei, record)
        else:
            record["immediate_parent"] = _clean(r.get("Parent of Subsidiary"))
            subs.setdefault(lei, record)
    return dict(sorted(subs.items())), dict(sorted(heads.items()))


# ---------------------------------------------------------------------------
# BODS file → SQLite
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE entity (
    record_id    TEXT PRIMARY KEY,
    statement_id BLOB NOT NULL,
    group_rid    TEXT NOT NULL,
    is_head      INTEGER NOT NULL,
    name         TEXT,
    jurisdiction TEXT,
    lei          TEXT,
    details      BLOB NOT NULL,
    annotations  TEXT,
    via_name     TEXT,
    via_rid      TEXT
);
CREATE INDEX entity_lei ON entity (lei);
CREATE INDEX entity_group ON entity (group_rid);
CREATE TABLE relationship (
    statement_id BLOB NOT NULL,
    record_id    TEXT NOT NULL,
    subject_rid  TEXT NOT NULL,
    ip_rid       TEXT NOT NULL,
    hierarchy    TEXT,
    details_text TEXT,
    annotations  TEXT
);
CREATE INDEX relationship_subject ON relationship (subject_rid);
CREATE INDEX relationship_ip ON relationship (ip_rid);
"""

#: Preset zlib dictionary for the entity ``recordDetails`` blobs. Each blob is
#: a few hundred bytes of JSON that is mostly repeated key and scheme strings,
#: so a shared dictionary shrinks it several-fold where plain zlib barely
#: breaks even. The bytes travel in ``meta.zdict`` (base64) so the runtime
#: never has to assume them.
ZDICT = "".join([
    '{"isComponent": false, "entityType": {"type": "registeredEntity"}, "name": "',
    '", "jurisdiction": {"name": "United States of America", "code": "US"}, "identifiers": [{"id": "',
    '", "schemeName": "OpenCorporates", "uri": "https://opencorporates.com/companies/',
    '"}, {"id": "', '", "scheme": "XI-LEI", "schemeName": "Global Legal Entity Identifier Foundation (GLEIF)"}, {"id": "',
    '", "schemeName": "Refinitiv Permanent Identifier (PermID)", "uri": "https://permid.org/1-',
    '"}, {"id": "', '", "schemeName": "DUNL identifier (OECD-UNSD MEIP)"}, {"id": "',
    '", "schemeName": "S&P Capital IQ Company ID"}], "addresses": [{"type": "business", "address": "',
    '", "country": {"name": "United Kingdom of Great Britain and Northern Ireland", "code": "GB"}}]}',
    '"alternateNames": ["', '"jurisdiction": {"name": "Netherlands (Kingdom of the)", "code": "NL"}',
    '"jurisdiction": {"name": "Germany", "code": "DE"}', '"jurisdiction": {"name": "France", "code": "FR"}',
    '"jurisdiction": {"name": "China", "code": "CN"}', '"jurisdiction": {"name": "Canada", "code": "CA"}',
    '"jurisdiction": {"name": "Australia", "code": "AU"}', ' LIMITED', ' GMBH', ' B.V.', ' INC.', ' LLC',
    ' S.A.', ' PTY LTD', ' LTD', ' HOLDINGS', ' CORPORATION', ' COMPANY', ' INTERNATIONAL', ' SERVICES',
]).encode("utf-8")

_HIERARCHY_RE = re.compile(r"Hierarchy classification:\s*([A-Za-z ]+?)\.")


def _hierarchy(details_text: str) -> str:
    m = _HIERARCHY_RE.search(details_text or "")
    return m.group(1).strip() if m else ""


def _lei_of(rd: dict) -> str:
    for ident in rd.get("identifiers") or []:
        if (ident.get("scheme") or "") == "XI-LEI":
            return _clean(ident.get("id")).upper()
    return ""


def _sid(hex_id: str) -> bytes:
    """Statement ids are 64 hex chars (SHA-256); stored as 32 raw bytes."""
    return bytes.fromhex(hex_id)


def _compress(rd: dict) -> bytes:
    c = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS, 9, zlib.Z_DEFAULT_STRATEGY, ZDICT)
    return c.compress(json.dumps(rd, ensure_ascii=False, separators=(", ", ": ")).encode("utf-8")) + c.flush()


def build_sqlite(
    bods_path: Path,
    rows: list[dict[str, str]] | None,
    out: Path,
    *,
    edition: str | None,
) -> dict:
    sha = hashlib.sha256()
    envelope: dict | None = None
    n_ent = n_rel = 0
    entities: dict[str, dict] = {}  # record_id -> {name, group}
    groups: dict[str, list[str]] = defaultdict(list)

    if out.exists():
        out.unlink()
    conn = sqlite3.connect(out)
    conn.execute("PRAGMA page_size = 4096")
    conn.executescript(_DDL)

    with bods_path.open("rb") as fh:
        for raw in fh:
            sha.update(raw)
            s = json.loads(raw)
            env = {
                "statementDate": s.get("statementDate"),
                "recordStatus": s.get("recordStatus"),
                "source": s.get("source"),
                "publicationDetails": s.get("publicationDetails"),
            }
            if envelope is None:
                envelope = env
            elif env != envelope:
                raise SystemExit(
                    f"statement {s['statementId'][:12]} does not share the file's "
                    f"envelope (statementDate/source/publicationDetails/recordStatus) — "
                    "the runtime stores it once; extend the schema before continuing"
                )
            rd = s["recordDetails"]
            ann = json.dumps(s["annotations"], ensure_ascii=False) if s.get("annotations") else None
            group = s["declarationSubject"]
            if s["recordType"] == "entity":
                rid = s["recordId"]
                conn.execute(
                    "INSERT INTO entity (record_id, statement_id, group_rid, is_head, name, "
                    "jurisdiction, lei, details, annotations) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        rid, _sid(s["statementId"]), group, int(rid == group), rd.get("name"),
                        (rd.get("jurisdiction") or {}).get("code"), _lei_of(rd) or None,
                        _compress(rd), ann,
                    ),
                )
                entities[rid] = {"name": rd.get("name"), "group": group}
                groups[group].append(rid)
                n_ent += 1
            elif s["recordType"] == "relationship":
                subj = rd["subject"] if isinstance(rd["subject"], str) else rd["subject"].get("describedByRecordId")
                ip = rd["interestedParty"] if isinstance(rd["interestedParty"], str) else rd["interestedParty"].get("describedByRecordId")
                interests = rd.get("interests") or []
                if (
                    rd.get("isComponent") is not False or len(interests) != 1
                    or interests[0].get("type") != "unknownInterest"
                    or interests[0].get("directOrIndirect") != "unknown"
                    or set(interests[0]) != {"type", "directOrIndirect", "details"}
                    or set(rd) != {"isComponent", "subject", "interestedParty", "interests"}
                ):
                    raise SystemExit(
                        f"relationship {s['statementId'][:12]} is not the one shape the runtime "
                        "rebuilds (one unknownInterest, directOrIndirect unknown, details) — "
                        "extend the schema before continuing"
                    )
                details_text = interests[0].get("details") or ""
                conn.execute(
                    "INSERT INTO relationship (statement_id, record_id, subject_rid, ip_rid, "
                    "hierarchy, details_text, annotations) VALUES (?,?,?,?,?,?,?)",
                    (_sid(s["statementId"]), s["recordId"], subj, ip, _hierarchy(details_text), details_text, ann),
                )
                n_rel += 1
            else:
                raise SystemExit(f"unexpected recordType {s['recordType']!r}")
    conn.commit()

    # --- immediate parents from the spreadsheet ------------------------------
    via_written = via_resolved = 0
    unmatched = 0
    if rows:
        by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
        head_by_name: dict[str, str] = {}
        for rid, e in entities.items():
            by_key[(e["group"], _norm(e["name"]))].append(rid)
            if rid == e["group"]:
                head_by_name[_norm(e["name"])] = rid
        group_of_mne: dict[str, str] = {}
        for r in rows:
            if _clean(r.get("Hierarchy")) == _HEAD:
                rid = head_by_name.get(_norm(r.get("Subsidiary Name (Clean)")))
                if rid and _clean(r.get("Parent MNE")) not in group_of_mne:
                    group_of_mne[_clean(r.get("Parent MNE"))] = rid
        taken: dict[tuple[str, str], int] = defaultdict(int)
        for r in rows:
            if _clean(r.get("Hierarchy")) == _HEAD:
                continue
            group = group_of_mne.get(_clean(r.get("Parent MNE")))
            via = _clean(r.get("Parent of Subsidiary"))
            if not group or not via:
                continue
            key = (group, _norm(r.get("Subsidiary Name (Clean)")))
            cands = by_key.get(key) or []
            k = taken[key]
            if k >= len(cands):
                unmatched += 1
                continue
            taken[key] += 1
            rid = cands[k]
            via_rid = (by_key.get((group, _norm(via))) or [None])[0]
            conn.execute(
                "UPDATE entity SET via_name = ?, via_rid = ? WHERE record_id = ?",
                (via, via_rid, rid),
            )
            via_written += 1
            if via_rid:
                via_resolved += 1
        conn.commit()

    import base64

    meta = {
        "schema": SCHEMA_VERSION,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "edition": edition or envelope.get("statementDate") or "",
        "source_sha256": sha.hexdigest(),
        "source_file": bods_path.name,
        "register_file": "" if rows is None else "spreadsheet",
        "statements": str(n_ent + n_rel),
        "entities": str(n_ent),
        "relationships": str(n_rel),
        "groups": str(len(groups)),
        "via_written": str(via_written),
        "via_resolved": str(via_resolved),
        "via_unmatched_rows": str(unmatched),
        "envelope": json.dumps(envelope, ensure_ascii=False),
        "zdict": base64.b64encode(ZDICT).decode("ascii"),
    }
    conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta.items())
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    return meta


# ---------------------------------------------------------------------------
# Optional: which MNE-head LEIs are subsidiaries in GLEIF's eyes
# ---------------------------------------------------------------------------


def check_heads(db: Path, report: Path) -> None:
    import httpx

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    heads = conn.execute(
        "SELECT record_id, name, lei FROM entity WHERE is_head = 1 AND lei IS NOT NULL ORDER BY record_id"
    ).fetchall()
    conn.close()
    out: list[dict] = []
    with httpx.Client(timeout=30.0, headers={"Accept": "application/vnd.api+json"}) as client:
        for rid, name, lei in heads:
            parent = None
            status = None
            try:
                r = client.get(f"https://api.gleif.org/api/v1/lei-records/{lei}/ultimate-parent-relationship")
                if r.status_code == 200:
                    parent = r.json()["data"]["attributes"]["relationship"]["endNode"]["id"]
                r2 = client.get(f"https://api.gleif.org/api/v1/lei-records/{lei}")
                if r2.status_code == 200:
                    a = r2.json()["data"]["attributes"]
                    status = a["registration"]["status"]
                    gleif_name = a["entity"]["legalName"]["name"]
                else:
                    gleif_name = None
            except Exception as exc:  # noqa: BLE001
                out.append({"record_id": rid, "name": name, "lei": lei, "error": str(exc)})
                continue
            if parent or status not in ("ISSUED", None):
                out.append({
                    "record_id": rid, "name": name, "lei": lei, "gleif_name": gleif_name,
                    "gleif_ultimate_parent": parent, "registration_status": status,
                })
            time.sleep(1.0)
    report.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"head warnings: {len(out)} of {len(heads)} → {report}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bods", type=Path, help="the OECD BODS v0.4 JSON Lines file")
    ap.add_argument("--register", type=Path, help="the Global Register XLSX or its CSV export")
    ap.add_argument("--out", type=Path, default=Path("data/meip.sqlite"))
    ap.add_argument("--edition", default=None, help="register reference date, e.g. 2024-12-31")
    ap.add_argument("--json-only", action="store_true", help="rebuild only the committed JSON tables")
    ap.add_argument("--data-dir", type=Path, default=_DATA, help="where the two committed JSON tables go")
    ap.add_argument("--check-heads", action="store_true", help="ask GLEIF about each head LEI (slow)")
    args = ap.parse_args()

    rows = read_register(args.register) if args.register else None
    if rows is not None:
        subs, heads = build_json(rows)
        subs_out = args.data_dir / _SUBS_OUT.name
        heads_out = args.data_dir / _HEADS_OUT.name
        for path, data in ((subs_out, subs), (heads_out, heads)):
            path.write_text(
                json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            print(f"wrote {len(data)} entries → {path}")
    if args.json_only:
        return 0
    if not args.bods:
        ap.error("--bods is required unless --json-only")
    meta = build_sqlite(args.bods, rows, args.out, edition=args.edition)
    print(json.dumps({k: v for k, v in meta.items() if k != "envelope"}, indent=1))
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.1f} MB)")
    if args.check_heads:
        check_heads(args.out, args.out.with_name("meip_head_warnings.json"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
