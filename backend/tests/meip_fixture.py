"""A tiny synthetic MEIP register, packed by the real ``scripts/build_meip.py``.

Shared by tests/test_meip.py and tests/test_lookup_pipeline.py so the store,
the adapter and the pipeline are all exercised on one set of statements.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path

from opencheck.meip import MEIP_URL

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "build_meip.py"

HEAD_A = "5493001KJTIIGC8Y1R12"   # ALPHA GROUP PLC (head)
SUB_A1 = "213800F4ETX85XLF5K47"   # ALPHA ONE LTD (LEI; also listed under Beta)
HEAD_B = "969500KMUQ2B6CBAF162"   # BETA SA (head)
NOT_IN = "254900OM12MPDCDH3H35"


def stmt(record_id, group, name, code, lei=None, extra=None, ann=None):
    rd = {
        "isComponent": False,
        "entityType": {"type": "registeredEntity"},
        "name": name,
        "jurisdiction": {"name": code, "code": code},
    }
    ids = [{"id": f"{code.lower()}/{record_id[-3:]}", "schemeName": "OpenCorporates",
            "uri": f"https://opencorporates.com/companies/{code.lower()}/{record_id[-3:]}"}]
    if lei:
        ids.append({"id": lei, "scheme": "XI-LEI",
                    "schemeName": "Global Legal Entity Identifier Foundation (GLEIF)"})
    rd["identifiers"] = ids
    if extra:
        rd.update(extra)
    s = {
        "statementId": hashlib.sha256(record_id.encode()).hexdigest(),
        "statementDate": "2024-12-31",
        "declarationSubject": group,
        "recordId": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "recordDetails": rd,
        "source": {"type": ["thirdParty"], "description": "OECD-UNSD MEIP, 'Group Register' sheet.",
                   "url": MEIP_URL},
        "publicationDetails": {"publicationDate": "2024-12-31", "bodsVersion": "0.4",
                               "publisher": {"name": "OECD"}},
    }
    if ann:
        s["annotations"] = ann
    return s


def rel(n, subject, head, hierarchy):
    s = stmt(f"meip-rel-{n}", head, "", "XX")
    s["recordType"] = "relationship"
    s["recordDetails"] = {
        "isComponent": False,
        "subject": subject,
        "interestedParty": head,
        "interests": [{"type": "unknownInterest", "directOrIndirect": "unknown",
                       "details": f"OECD-UNSD MEIP Hierarchy classification: {hierarchy}."}],
    }
    return s


def statements() -> list[dict]:
    ann = [{"statementPointerTarget": "", "motivation": "commenting",
            "description": "Data quality note: source listed this entity twice."}]
    return [
        stmt("meip-entity-1", "meip-entity-1", "ALPHA GROUP PLC", "GB", HEAD_A),
        stmt("meip-entity-2", "meip-entity-1", "ALPHA ONE LTD", "GB", SUB_A1,
             extra={"alternateNames": ["Alpha One"], "addresses": [
                 {"type": "business", "address": "1 High St, London GB",
                  "country": {"name": "GB", "code": "GB"}}]}),
        rel(2, "meip-entity-2", "meip-entity-1", "Known"),
        stmt("meip-entity-3", "meip-entity-1", "Alpha Two GmbH", "DE"),
        rel(3, "meip-entity-3", "meip-entity-1", "Known"),
        stmt("meip-entity-4", "meip-entity-4", "BETA SA", "FR", HEAD_B),
        stmt("meip-entity-5", "meip-entity-4", "ALPHA ONE LTD", "GB", SUB_A1, ann=ann),
        rel(5, "meip-entity-5", "meip-entity-4", "Unknown"),
    ]


def register_rows() -> list[dict[str, str]]:
    return [
        {"Parent MNE": "Alpha Group", "Hierarchy": "MNE Head", "ISO3": "GBR",
         "Subsidiary Name (Clean)": "ALPHA GROUP PLC", "LEI": HEAD_A, "Parent of Subsidiary": ""},
        {"Parent MNE": "Alpha Group", "Hierarchy": "Known", "ISO3": "GBR",
         "Subsidiary Name (Clean)": "ALPHA ONE LTD", "LEI": SUB_A1, "Parent of Subsidiary": "ALPHA GROUP PLC"},
        {"Parent MNE": "Alpha Group", "Hierarchy": "Known", "ISO3": "DEU",
         "Subsidiary Name (Clean)": "Alpha Two GmbH", "LEI": "", "Parent of Subsidiary": "ALPHA ONE LTD"},
        {"Parent MNE": "Beta", "Hierarchy": "MNE Head", "ISO3": "FRA",
         "Subsidiary Name (Clean)": "BETA SA", "LEI": HEAD_B, "Parent of Subsidiary": ""},
        {"Parent MNE": "Beta", "Hierarchy": "Unknown", "ISO3": "GBR",
         "Subsidiary Name (Clean)": "ALPHA ONE LTD", "LEI": SUB_A1, "Parent of Subsidiary": ""},
    ]


def build(tmp_path: Path) -> tuple[Path, list[dict], dict]:
    """Pack the synthetic register into ``tmp_path/meip.sqlite`` with the real
    build script; returns (db path, the statements, the build's meta)."""
    spec = importlib.util.spec_from_file_location("build_meip", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    stmts = statements()
    bods = tmp_path / "meip_bods.jsonl"
    bods.write_text("".join(json.dumps(s) + "\n" for s in stmts), encoding="utf-8")
    reg = tmp_path / "register.csv"
    with reg.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(register_rows()[0]))
        w.writeheader()
        w.writerows(register_rows())
    out = tmp_path / "meip.sqlite"
    meta = mod.build_sqlite(bods, mod.read_register(reg), out, edition=None)
    return out, stmts, meta
