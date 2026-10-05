"""Phase 288 — ``scripts/build_laundromat_dataset.py``: the pure half.

The fetching half calls the real pipeline and is exercised by running the
script; what is pinned here is everything that decides what the release
*says*: which thesaurus records become subjects, how results merge, and
what the manifest, subjects table and licence notes record.
"""

from __future__ import annotations

import csv
import gzip
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

_had_allow_live = "OPENCHECK_ALLOW_LIVE" in os.environ
import build_laundromat_dataset as bld  # noqa: E402

if not _had_allow_live:
    os.environ.pop("OPENCHECK_ALLOW_LIVE", None)


LEI = "54930007FGRO3F0RZ382"
BANK_LEI = "MAES062Z21O4RZ2U7M96"

THESAURUS = [
    {  # LEI + Companies House number → two subjects, the register one anchored
        "bods:fullName": "BENTCARD IMPORT LLP",
        "lavie:class": "shell",
        "lavie:lei": {"bods:idString": LEI, "bods:scheme": "XI-LEI"},
        "bods:scheme": "GB-COH",
        "bods:idString": "oc369315",
    },
    {  # register only
        "bods:fullName": "POLUX MANAGEMENT LP",
        "lavie:class": "core",
        "bods:scheme": "GB-COH",
        "bods:idString": "SL012725",
    },
    {  # LEI only, class under the typo key
        "bods:fullName": "DANSKE BANK A/S",
        "lavid:class": "bank",
        "lavie:lei": {"bods:idString": BANK_LEI},
    },
    {  # Russian tax id — nothing OpenCheck reads
        "bods:fullName": "INN1325029808",
        "bods:scheme": "RU-FNS",
        "lavid:class": "russian-anon",
    },
    {  # no identifier at all
        "bods:fullName": "BAKTELEKOM MMC",
        "lavie:class": "operating",
    },
    {  # malformed LEI → counted, not a subject
        "bods:fullName": "BROKEN",
        "lavie:lei": {"bods:idString": "NOT-AN-LEI"},
    },
    {  # duplicate company number → counted once
        "bods:fullName": "POLUX MANAGEMENT LLP",
        "bods:scheme": "GB-COH",
        "bods:idString": "SL012725",
    },
]


def test_seed_derives_one_subject_per_actionable_identifier() -> None:
    seed = bld.seed_from_thesaurus(THESAURUS, commit="abc123")
    keys = [s["key"] for s in seed["subjects"]]
    assert keys == [
        f"lei:{LEI}", "GB-COH:OC369315", "GB-COH:SL012725", f"lei:{BANK_LEI}",
    ]
    bent_reg = next(s for s in seed["subjects"] if s["key"] == "GB-COH:OC369315")
    assert bent_reg["kind"] == "register" and bent_reg["lei"] == LEI
    assert bent_reg["class"] == "shell"
    bank = next(s for s in seed["subjects"] if s["key"] == f"lei:{BANK_LEI}")
    assert bank["class"] == "bank"  # read from the lavid: typo key too
    assert seed["skipped"] == {"RU-FNS": 1, "duplicate": 1, "malformed lei": 1, "no identifier": 2}
    assert seed["source"]["commit"] == "abc123"
    assert seed["source"]["records"] == len(THESAURUS)


def _ent(sid: str, name: str, source: str = "gleif") -> dict:
    return {
        "statementId": sid, "recordId": sid, "recordType": "entity",
        "recordStatus": "new", "statementDate": "2026-10-01",
        "recordDetails": {
            "isComponent": False, "entityType": {"type": "registeredEntity"}, "name": name,
            "identifiers": [{"id": sid, "scheme": "TEST"}],
        },
        "source": {
            "type": ["officialRegister"], "description": source,
            "retrievedAt": "2026-10-01T00:00:00Z",
        },
        "publicationDetails": {
            "publicationDate": "2026-10-01", "bodsVersion": "0.4",
            "publisher": {"name": "OpenCheck"},
        },
    }


def _rel(sid: str, subject: str, party: str) -> dict:
    return {
        "statementId": sid, "recordId": sid, "recordType": "relationship",
        "recordStatus": "new", "statementDate": "2026-10-01",
        "recordDetails": {
            "isComponent": False, "subject": subject, "interestedParty": party,
            "interests": [{"type": "shareholding", "directOrIndirect": "direct"}],
        },
        "source": {
            "type": ["officialRegister"], "description": "gleif",
            "retrievedAt": "2026-10-01T00:00:00Z",
        },
        "publicationDetails": {
            "publicationDate": "2026-10-01", "bodsVersion": "0.4",
            "publisher": {"name": "OpenCheck"},
        },
    }


SUBJECTS = [
    {"key": "GB-COH:OC369315", "kind": "register", "scheme": "GB-COH", "id": "OC369315",
     "name": "BENTCARD IMPORT LLP", "class": "shell", "lei": LEI},
    {"key": f"lei:{LEI}", "kind": "lei", "lei": LEI, "name": "BENTCARD IMPORT LLP",
     "class": "shell"},
    {"key": f"lei:{BANK_LEI}", "kind": "lei", "lei": BANK_LEI, "name": "DANSKE BANK A/S",
     "class": "bank"},
    {"key": "GB-COH:SL012725", "kind": "register", "scheme": "GB-COH", "id": "SL012725",
     "name": "POLUX MANAGEMENT LP", "class": "core"},
]

RAWS = {
    f"lei:{LEI}": {
        "key": f"lei:{LEI}", "status": "done",
        "row": {
            "legal_name": "BENTCARD IMPORT LLP", "jurisdiction": "AZ",
            "register_status": {"liveness": "live", "raw": "ACTIVE", "source_id": "gleif"},
            "verdict": "obscured", "risk_codes": ["OPAQUE_OWNERSHIP"], "context_codes": [],
            "coverage": {"with_data_ids": ["gleif"]},
        },
        "bods": [_ent("e-bent", "BENTCARD IMPORT LLP"), _ent("e-parent", "UNKNOWN PARENT"),
                 _rel("r-1", "e-bent", "e-parent")],
        "risk_signals": [{"code": "OPAQUE_OWNERSHIP", "kind": "risk", "statement_ids": ["r-1"],
                          "summary": "x", "sources": ["gleif"]}],
        "degraded_sources": [],
        "license_notices": [],
        "contributing_ids": ["gleif"],
        "subsidiaries": {"statements": 0, "children": 0},
    },
    f"lei:{BANK_LEI}": {
        "key": f"lei:{BANK_LEI}", "status": "done",
        "row": {
            "legal_name": "DANSKE BANK A/S", "jurisdiction": "DK",
            "register_status": {"liveness": "live", "raw": "active", "source_id": "cvr_denmark"},
            "verdict": "pep", "risk_codes": ["RELATED_PEP"],
            "context_codes": ["GLEIF_REPORTING_EXCEPTION"],
            "coverage": {"with_data_ids": ["gleif", "cvr_denmark", "opensanctions"]},
        },
        # e-parent is shared with Bentcard's bundle → collapsed once
        "bods": [_ent("e-danske", "DANSKE BANK A/S"), _ent("e-parent", "UNKNOWN PARENT"),
                 _ent("e-child", "DANSKE BANK EESTI", "gleif"), _rel("r-2", "e-child", "e-danske")],
        "risk_signals": [
            {"code": "RELATED_PEP", "kind": "risk", "statement_ids": ["p-1"], "summary": "y",
             "sources": ["opensanctions"]},
            {"code": "GLEIF_REPORTING_EXCEPTION", "kind": "context", "statement_ids": ["e-danske"],
             "summary": "z", "sources": ["gleif"]},
        ],
        "degraded_sources": [{"source_id": "opencheck", "check": "cross_source_names",
                              "reason": "truncated", "affected_signals": ["RELATED_SANCTIONED"],
                              "detail": "25 of 37"}],
        "license_notices": [{"source_id": "opensanctions", "hit_id": "NK-1", "notice": "CC-BY-NC"}],
        "contributing_ids": ["cvr_denmark", "gleif", "opensanctions"],
        "subsidiaries": {"statements": 2, "children": 1},
    },
    "GB-COH:OC369315": {
        "key": "GB-COH:OC369315", "status": "done", "anchor": "e-bent",
        # the hop's subject statement was remapped onto the GLEIF node id
        "bods": [_ent("e-bent", "BENTCARD IMPORT LLP", "companies_house"),
                 _ent("e-astrocom", "ASTROCOM AG", "companies_house"),
                 _rel("r-3", "e-bent", "e-astrocom")],
        "risk_signals": [{"code": "OFFSHORE_LEAKS", "kind": "risk", "statement_ids": ["e-astrocom"],
                          "summary": "w", "sources": ["icij"]}],
        "degraded_sources": [],
        "contributing_ids": ["companies_house", "icij"],
    },
    "GB-COH:SL012725": {
        "key": "GB-COH:SL012725", "status": "failed", "reason": "boom", "retryable": True,
    },
}


def test_assemble_dedupes_in_lei_first_order_and_keeps_every_subject() -> None:
    out = bld.assemble(SUBJECTS, RAWS)
    ids = [s["statementId"] for s in out["statements"]]
    # LEI subjects first (seed order among them), then register hops; the
    # shared parent and the remapped hop subject each appear once.
    assert ids == ["e-bent", "e-parent", "r-1", "e-danske", "e-child", "r-2", "e-astrocom", "r-3"]
    assert out["duplicate_statements_collapsed"] == 2
    assert out["node_counts"] == {"entity": 5, "relationship": 3}
    assert out["contributing_ids"] == [
        "companies_house", "cvr_denmark", "gleif", "icij", "opensanctions",
    ]
    assert out["license_notices"] == [
        {"source_id": "opensanctions", "hit_id": "NK-1", "notice": "CC-BY-NC"},
    ]
    assert out["subject_status"] == {"done": 3, "failed": 1}
    assert out["degraded_subjects"] == 2  # the failed hop + the truncated screen

    by_key = {r["key"]: r for r in out["rows"]}
    assert by_key["GB-COH:SL012725"]["status"] == "failed"
    assert by_key["GB-COH:SL012725"]["statements"] == 0
    assert by_key["GB-COH:SL012725"]["degraded"] is True
    assert by_key[f"lei:{BANK_LEI}"]["degraded_checks"] == ["cross_source_names:truncated"]
    assert by_key[f"lei:{BANK_LEI}"]["subsidiary_children"] == 1
    # A register row derives its codes from its signals, split by kind.
    assert by_key["GB-COH:OC369315"]["risk_codes"] == ["OFFSHORE_LEAKS"]
    assert by_key["GB-COH:OC369315"]["context_codes"] == []
    # Signals carry the subject that raised them and are deduped by (code, ids, summary).
    assert [s["code"] for s in out["signals"]] == [
        "OPAQUE_OWNERSHIP", "RELATED_PEP", "GLEIF_REPORTING_EXCEPTION", "OFFSHORE_LEAKS",
    ]
    assert out["signals"][0]["subject"] == f"lei:{LEI}"


def test_assemble_names_a_subject_with_no_cached_result() -> None:
    out = bld.assemble(SUBJECTS[:1], {})
    assert out["rows"][0]["status"] == "missing"
    assert out["rows"][0]["degraded"] is True
    assert out["statements"] == []


def test_is_degraded_covers_failed_stub_and_degraded_checks() -> None:
    assert bld._is_degraded({"status": "failed"})
    assert bld._is_degraded({"status": "stub", "degraded_sources": []})
    assert bld._is_degraded({"status": "done", "degraded_sources": [{"check": "x"}]})
    assert not bld._is_degraded({"status": "done", "degraded_sources": []})


def test_status_text_flattens_the_profile_dict() -> None:
    status = {"liveness": "live", "raw": "ACTIVE", "source_id": "gleif"}
    assert bld._status_text(status) == "ACTIVE (gleif, live)"
    assert bld._status_text({"raw": "dissolved"}) == "dissolved"
    assert bld._status_text("active") == "active"
    assert bld._status_text(None) is None


def test_write_release_records_every_artefact_with_checksums(tmp_path: Path) -> None:
    assembled = bld.assemble(SUBJECTS, RAWS)
    seed = {"source": {"commit": "abc123", "records": 7}, "skipped": {"RU-FNS": 1}}
    manifest = bld.write_release(
        assembled, out=tmp_path, seed=seed, stamp="2026-10-05",
        formats=("bods", "rdf", "ftm", "senzing"),  # neo4j needs the external CLI
    )
    names = {p.name for p in tmp_path.iterdir()}
    base = "azerbaijani-laundromat-2026-10-05"
    for suffix in (".bods.jsonl.gz", ".nq.gz", ".ftm.jsonl.gz", ".senzing.jsonl.gz",
                   ".subjects.csv", ".signals.jsonl"):
        assert f"{base}{suffix}" in names
    assert {"manifest.json", "LICENSES.md", "RELEASE_NOTES.md"} <= names

    assert manifest["dataset"] == "azerbaijani-laundromat"
    assert manifest["bods_statement_count"] == 8
    assert manifest["subjects"]["total"] == 4
    assert manifest["subjects"]["failed"] == [{"key": "GB-COH:SL012725", "reason": "boom"}]
    assert manifest["signal_codes"] == {
        "OPAQUE_OWNERSHIP": 1, "RELATED_PEP": 1, "GLEIF_REPORTING_EXCEPTION": 1,
        "OFFSHORE_LEAKS": 1,
    }
    assert set(manifest["artefacts"]) == {"bods", "rdf", "ftm", "senzing", "subjects", "signals"}
    for a in manifest["artefacts"].values():
        p = tmp_path / a["file"]
        assert p.exists() and a["bytes"] == p.stat().st_size
        assert len(a["sha256"]) == 64
    assert manifest["artefacts"]["rdf"]["quads"] > 0
    # OpenSanctions contributed → the licence verdict is non-commercial.
    assert manifest["licensing"]["commercial_use"] == "no"
    assert "opensanctions" in manifest["contributing_source_ids"]

    with gzip.open(tmp_path / f"{base}.bods.jsonl.gz", "rt", encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    assert [r["statementId"] for r in rows] == [s["statementId"] for s in assembled["statements"]]

    with (tmp_path / f"{base}.subjects.csv").open(encoding="utf-8") as fh:
        table = list(csv.DictReader(fh))
    # Rows follow the bundle: LEI subjects first, then register hops.
    assert [r["key"] for r in table] == [
        f"lei:{LEI}", f"lei:{BANK_LEI}", "GB-COH:OC369315", "GB-COH:SL012725",
    ]
    bank = next(r for r in table if r["lei"] == BANK_LEI)
    assert bank["register_status"] == "active (cvr_denmark, live)"
    assert bank["degraded"] == "True"
    assert bank["degraded_checks"] == "cross_source_names:truncated"
    failed = next(r for r in table if r["key"] == "GB-COH:SL012725")
    assert failed["status"] == "failed" and failed["reason"] == "boom"

    notes = (tmp_path / "RELEASE_NOTES.md").read_text(encoding="utf-8")
    assert "**Non-commercial.**" in notes
    assert "8 BODS statements" in notes
    assert f"`{base}.nq.gz`" in notes
    licenses = (tmp_path / "LICENSES.md").read_text(encoding="utf-8")
    assert "opensanctions/NK-1" in licenses


def test_write_release_without_nc_sources_does_not_claim_non_commercial(tmp_path: Path) -> None:
    raws = {f"lei:{LEI}": RAWS[f"lei:{LEI}"]}
    assembled = bld.assemble(SUBJECTS[1:2], raws)
    manifest = bld.write_release(
        assembled, out=tmp_path, seed={"source": {}, "skipped": {}}, stamp="2026-10-05",
        formats=("bods",),
    )
    assert manifest["licensing"]["commercial_use"] != "no"
    notes = (tmp_path / "RELEASE_NOTES.md").read_text(encoding="utf-8")
    assert "**Non-commercial.**" not in notes
    assert "Licence verdict:" in notes


@pytest.mark.parametrize("argv", [["seed"], ["build"]])
def test_cli_requires_its_arguments(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        bld.main(argv)


def test_run_lei_retries_a_momentary_refusal_then_succeeds(monkeypatch) -> None:
    """GLEIF rate-limiting (503) and the lookup budget (429) are timing, not
    answers: the lookup is retried after the wait; a 404 is not."""
    import asyncio

    from fastapi import HTTPException

    import opencheck.routers.lookup as lookup_mod

    calls: list[int] = []

    class _Resp:
        bods = [_ent("e-x", "X")]
        risk_signals: list = []
        degraded_sources: list = []
        license_notices: list = []
        possibly_same_entities: list = []
        hits: list = []
        source_liveness: dict = {}

    async def fake_lookup(lei, deepen_top=5, refresh=False):
        calls.append(1)
        if len(calls) < 3:
            raise HTTPException(status_code=503, detail="GLEIF is rate-limiting")
        return _Resp()

    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(lookup_mod, "_lookup_impl", fake_lookup)
    monkeypatch.setattr("opencheck.mcp.shaping.shape_batch_row", lambda r: {"legal_name": "X"})
    monkeypatch.setattr(bld.asyncio, "sleep", fake_sleep)

    raw = asyncio.run(
        bld._run_lei({"key": f"lei:{LEI}", "lei": LEI}, deepen_top=1, retries=2, retry_wait=7)
    )
    assert raw["status"] == "done" and raw["attempts"] == 3
    assert sleeps == [7, 7]
    assert "subsidiaries" not in raw  # fetched in the second pass, never inline

    calls.clear()

    async def not_found(lei, deepen_top=5, refresh=False):
        calls.append(1)
        raise HTTPException(status_code=404, detail="unknown LEI")

    monkeypatch.setattr(lookup_mod, "_lookup_impl", not_found)
    raw = asyncio.run(
        bld._run_lei({"key": f"lei:{LEI}", "lei": LEI}, deepen_top=1, retries=2, retry_wait=0)
    )
    assert raw["status"] == "failed" and raw["retryable"] is False and len(calls) == 1


def test_run_subsidiaries_folds_new_statements_and_records_partial(monkeypatch) -> None:
    import asyncio

    import opencheck.subsidiaries as subs

    async def fake_assemble(lei, include_bods=False):
        return {
            "bods": [_ent("e-x", "X"), _ent("e-child", "CHILD"), _rel("r-c", "e-child", "e-x")],
            "children": [{"lei": "C"}],
            "direct_available": False,
            "ultimate_available": True,
            "snapshot_date": None,
        }

    monkeypatch.setattr(subs, "assemble_subsidiaries", fake_assemble)
    raw = {"status": "done", "bods": [_ent("e-x", "X")]}
    raw = asyncio.run(bld._run_subsidiaries(raw, LEI))
    assert [s["statementId"] for s in raw["bods"]] == ["e-x", "e-child", "r-c"]
    assert raw["subsidiaries"]["statements"] == 2
    assert raw["subsidiaries"]["direct_available"] is False  # partial, recorded not hidden

    async def boom(lei, include_bods=False):
        raise RuntimeError("throttled")

    monkeypatch.setattr(subs, "assemble_subsidiaries", boom)
    raw = asyncio.run(bld._run_subsidiaries({"status": "done", "bods": []}, LEI))
    assert raw["subsidiaries"]["error"].startswith("RuntimeError")
    assert raw["status"] == "done"
