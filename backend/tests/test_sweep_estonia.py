"""The Estonia dataset build (``scripts/sweep_estonia.py``) — the GLEIF layer
from the Golden Copy mirror, the register layer from the e-Business Register
index, joined on ``EE-ARIREGISTER``.

Both inputs are made by their real builders: the mirror by the loaders the
monthly build and the in-process refresh use, the index by
``build_ariregister_index.py`` from the RIK-shaped fixture files of
``test_ariregister_index``. The build is then run end to end through
``main``, as the release is.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import entity_pages as ep
from opencheck.bods.mappers.estonia import _ee_company_url
from opencheck.config import get_settings
from opencheck.mirror_build import (
    compress_detail_column,
    ensure_schema,
    load_lei2,
    load_repex,
    load_rr,
    write_meta,
)
from opencheck.sources import ariregister
from tests.test_ariregister_index import AGENT, EESTI, HAUDE, HOLD, _build
from tests.test_gleif_mirror_store import (
    LEI2_HEADER,
    REPEX_HEADER,
    RR_HEADER,
    _lei2_row,
    _repex_row,
    _rr_row,
    _write_csv,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "sweep_estonia.py"
_spec = importlib.util.spec_from_file_location("_sweep_estonia", _SCRIPT)
assert _spec and _spec.loader
sweep = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sweep)

# Estonian LEI holders, one per register fixture company.
L_AGENT = "8945000000000000AG01"
L_EESTI = "8945000000000000EE01"
L_HOLD = "8945000000000000HO01"
L_GONE = "8945000000000000GO01"  # a code the register does not hold (deleted)
L_FUND = "8945000000000000FU01"  # filed under another authority
L_DEAD = "8945000000000000DE01"  # GLEIF records it INACTIVE
L_DUP = "8945000000000000DU01"  # a DUPLICATE registration
# Abroad.
L_PARENT = "2138000000000000GB01"  # Eesti Energia's foreign direct parent
L_CHILD = "5299000000000000LV01"  # a Latvian company Eesti Energia consolidates
L_OTHER = "5299000000000000LV02"  # the Latvian company's ultimate parent, not Estonian
L_STRAY = "5299000000000000LV03"  # foreign, nothing to do with Estonia


def _ee(
    lei: str,
    name: str,
    code: str,
    *,
    ra: str = sweep.EE_RA,
    status: str = "ACTIVE",
    reg: str = "ISSUED",
) -> list[str]:
    return _lei2_row(
        **{
            "LEI": lei,
            "Entity.LegalName": name,
            "Entity.LegalAddress.City": "TALLINN",
            "Entity.LegalAddress.Country": "EE",
            "Entity.LegalJurisdiction": "EE",
            "Entity.EntityStatus": status,
            "Entity.RegistrationAuthority.RegistrationAuthorityID": ra,
            "Entity.RegistrationAuthority.RegistrationAuthorityEntityID": code,
            "Registration.InitialRegistrationDate": "2019-05-05T00:00:00Z",
            "Registration.LastUpdateDate": "2026-06-15T08:00:00.000Z",
            "Registration.RegistrationStatus": reg,
        }
    )


def _abroad(lei: str, name: str, country: str) -> list[str]:
    return _lei2_row(
        **{
            "LEI": lei,
            "Entity.LegalName": name,
            "Entity.LegalAddress.City": "RIGA",
            "Entity.LegalAddress.Country": country,
            "Entity.LegalJurisdiction": country,
            "Entity.EntityStatus": "ACTIVE",
            "Registration.InitialRegistrationDate": "2015-01-01T00:00:00Z",
            "Registration.LastUpdateDate": "2026-05-01T00:00:00Z",
            "Registration.RegistrationStatus": "ISSUED",
        }
    )


@pytest.fixture()
def built(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    index, _meta = _build(tmp_path / "index")

    mirror = tmp_path / "entity_pages.sqlite"
    conn = sqlite3.connect(mirror)
    ensure_schema(conn)
    lei2 = [
        _ee(L_AGENT, "007 Agent & Partners OÜ", AGENT),
        _ee(L_EESTI, "Eesti Energia AS", EESTI),
        _ee(L_HOLD, "Holding Põhjala OÜ", HOLD, reg="LAPSED"),
        _ee(L_GONE, "Kustutatud OÜ", "11111111", reg="LAPSED"),
        _ee(L_FUND, "Fond", "FUND-1", ra="RA000999"),
        _ee(L_DEAD, "Lõpetatud OÜ", "22222222", status="INACTIVE"),
        _ee(L_DUP, "Duplikaat OÜ", AGENT, reg="DUPLICATE"),
        _abroad(L_PARENT, "Foreign Parent plc", "GB"),
        _abroad(L_CHILD, "SIA Latvian Child", "LV"),
        _abroad(L_OTHER, "SIA Latvian Top", "LV"),
        _abroad(L_STRAY, "SIA Unrelated", "LV"),
    ]
    rr = [
        _rr_row(L_EESTI, L_PARENT, "IS_DIRECTLY_CONSOLIDATED_BY", reg="PUBLISHED"),
        _rr_row(L_AGENT, L_EESTI, "IS_DIRECTLY_CONSOLIDATED_BY", reg="PUBLISHED"),
        _rr_row(L_CHILD, L_EESTI, "IS_DIRECTLY_CONSOLIDATED_BY", reg="PUBLISHED"),
        _rr_row(L_CHILD, L_OTHER, "IS_ULTIMATELY_CONSOLIDATED_BY", reg="PUBLISHED"),
    ]
    repex = [
        _repex_row(L_HOLD, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NATURAL_PERSONS"),
        _repex_row(L_CHILD, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NO_LEI"),
    ]
    load_lei2(conn, _write_csv(tmp_path / "lei2.csv", LEI2_HEADER, lei2))
    load_rr(conn, _write_csv(tmp_path / "rr.csv", RR_HEADER, rr))
    load_repex(conn, _write_csv(tmp_path / "repex.csv", REPEX_HEADER, repex))
    compress_detail_column(conn)
    write_meta(
        conn,
        source_publish_date="2026-10-12 16:00:00",
        source_publish_datetime="2026-10-12 16:00:00",
        schema_version=ep.SCHEMA_VERSION,
        built_at="2026-10-12T19:00:00+00:00",
    )
    conn.close()

    # main() names the files in the environment; register the keys with
    # monkeypatch first so teardown puts the previous values back.
    for key in ("OPENCHECK_ENTITY_PAGES_DB_FILE", "ARIREGISTER_DB_FILE", "OPENCHECK_ALLOW_LIVE"):
        monkeypatch.setenv(key, "")
    out = tmp_path / "out"
    sweep.main(
        [
            "--stage",
            "all",
            "--entity-pages-db",
            str(mirror),
            "--ariregister-index",
            str(index),
            "--out-dir",
            str(out),
        ]
    )
    yield out
    monkeypatch.undo()
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    ariregister.reset_connection()


def _statements(out: Path) -> list[dict[str, Any]]:
    manifest = json.loads((out / "manifest.json").read_text())
    with gzip.open(out / f"{manifest['dataset']}.jsonl.gz", "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def _by_lei(statements: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for st in statements:
        for ident in (st.get("recordDetails") or {}).get("identifiers") or []:
            if ident.get("scheme") == "XI-LEI":
                out[ident["id"]] = st
    return out


def _register_record(statements: list[dict[str, Any]], code: str) -> dict[str, Any]:
    for st in statements:
        if (st.get("source") or {}).get("opencheckSourceId") != "ariregister":
            continue
        rd = st.get("recordDetails") or {}
        if (
            st["recordType"] == "entity"
            and any(
                i.get("scheme") == "EE-ARIREGISTER" and i.get("id") == code
                for i in rd.get("identifiers") or []
            )
            and rd.get("foundingDate")
        ):
            return st
    raise AssertionError(f"no register record for {code}")


def test_the_scope_is_every_usable_estonian_lei(built: Path) -> None:
    manifest = json.loads((built / "manifest.json").read_text())
    assert manifest["dataset"] == "estonia-2026-10-12"  # the register cut
    assert manifest["gleif_entities_in_scope"] == 5  # AGENT, EESTI, HOLD, GONE, FUND
    assert manifest["gleif_excluded"] == {"entity_inactive": 1, "registration_duplicate": 1}
    assert manifest["lei_with_registry_code"] == 4
    assert manifest["lei_without_ra000181_code"] == 1
    assert manifest["ariregister_companies_matched"] == 3
    assert manifest["ariregister_companies_missing"] == 1
    # The holding's corporate shareholder is carried as its own record.
    assert manifest["ariregister_chain_companies"] == 1
    assert manifest["sources"]["gleif"]["watermark"].startswith("2026-10-12T16:00")
    assert manifest["sources"]["ariregister"]["cut"] == "2026-10-12"
    assert manifest["ownership_history_included"] is False
    by_lei = _by_lei(_statements(built))
    assert L_DEAD not in by_lei and L_DUP not in by_lei and L_STRAY not in by_lei


def test_the_missing_codes_are_listed_and_their_gleif_record_kept(built: Path) -> None:
    rows = (built / "register-missing.csv").read_text().splitlines()
    assert rows[0].startswith("lei,registry_code,")
    assert rows[1:] == [f"{L_GONE},11111111,Kustutatud OÜ,LAPSED,ACTIVE,2026-06-15"]
    assert L_GONE in _by_lei(_statements(built))


def test_every_gleif_edge_is_published_once_from_the_child(built: Path) -> None:
    statements = _statements(built)
    gleif_rels = [
        st
        for st in statements
        if st["recordType"] == "relationship"
        and st["source"]["opencheckSourceId"] == "gleif"
        and "consolidation" in json.dumps(st["recordDetails"]["interests"])
    ]
    by_lei = _by_lei(statements)
    pairs = [
        (st["recordDetails"]["subject"], st["recordDetails"]["interestedParty"])
        for st in gleif_rels
        if "direct" in st["recordDetails"]["interests"][0]["details"]
    ]
    sid = {lei: st["statementId"] for lei, st in by_lei.items()}
    # AGENT → EESTI, EESTI → foreign parent, the Latvian child → EESTI: once each.
    assert sorted(pairs) == sorted(
        [
            (sid[L_AGENT], sid[L_EESTI]),
            (sid[L_EESTI], sid[L_PARENT]),
            (sid[L_CHILD], sid[L_EESTI]),
        ]
    )
    details = " ".join(st["recordDetails"]["interests"][0]["details"] for st in gleif_rels)
    assert "direct-child" not in details
    # The Latvian child's own non-Estonian parent is not this dataset's business.
    assert L_OTHER not in by_lei


def test_the_layers_join_on_the_registry_code_scheme(built: Path) -> None:
    statements = _statements(built)
    gleif_agent = _by_lei(statements)[L_AGENT]
    assert {"id": AGENT, "scheme": "EE-ARIREGISTER"}.items() <= {
        k: v
        for i in gleif_agent["recordDetails"]["identifiers"]
        if i["scheme"] == "EE-ARIREGISTER"
        for k, v in i.items()
    }.items()
    register_agent = _register_record(statements, AGENT)
    assert register_agent["source"]["opencheckSourceId"] == "ariregister"
    # The chain company is a full register record, not the thin statement
    # the holding's shareholder row builds.
    _register_record(statements, HAUDE)
    # No registry code anywhere under the VAT scheme.
    for st in statements:
        for ident in (st.get("recordDetails") or {}).get("identifiers") or []:
            if ident.get("scheme") == "EE-KMKR":
                assert not str(ident["id"]).isdigit() or len(str(ident["id"])) != 8


def test_every_statement_names_its_source_and_dates(built: Path) -> None:
    statements = _statements(built)
    ids = {st["statementId"] for st in statements}
    assert len(ids) == len(statements)
    seen_relationship = False
    for st in statements:
        assert st["source"]["opencheckSourceId"] in {"gleif", "ariregister"}
        assert st["source"].get("retrievedAt")
        if st["recordType"] == "relationship":
            seen_relationship = True
            assert st["recordDetails"]["subject"] in ids
            party = st["recordDetails"]["interestedParty"]
            assert not isinstance(party, str) or party in ids
        else:
            assert not seen_relationship, "parties precede relationships"
    checks = json.loads((built / "checks.json").read_text())
    assert checks["problem_count"] == 0
    assert checks["counts"]["dangling_references"] == 0


def test_the_check_refuses_what_the_release_rules_out() -> None:
    def person(**rd: Any) -> dict[str, Any]:
        return {
            "statementId": "p1",
            "recordId": "p1",
            "recordType": "person",
            "source": {"opencheckSourceId": "ariregister"},
            "recordDetails": {"personType": "knownPerson", **rd},
        }

    assert (
        sweep.check_statements(
            [
                person(
                    addresses=[{"type": "residence", "country": {"code": "EE", "name": "Estonia"}}]
                )
            ]
        )["problem_count"]
        == 0
    )
    street = sweep.check_statements(
        [person(addresses=[{"type": "residence", "address": "Lelle tn 22"}])]
    )
    assert "street address" in street["problems"][0]
    code = sweep.check_statements(
        [person(identifiers=[{"id": "37605030299", "scheme": "EST-IDCARD"}])]
    )
    assert "personal code" in code["problems"][0]
    vat = sweep.check_statements(
        [
            {
                "statementId": "e1",
                "recordType": "entity",
                "source": {},
                "recordDetails": {"identifiers": [{"id": "16752073", "scheme": "EE-KMKR"}]},
            }
        ]
    )
    assert "under EE-KMKR" in vat["problems"][0]
    dangling = sweep.check_statements(
        [
            {
                "statementId": "r1",
                "recordType": "relationship",
                "source": {},
                "recordDetails": {"subject": "x", "interestedParty": "y"},
            }
        ]
    )
    assert dangling["counts"]["dangling_references"] == 2


def test_isikukood_checksum() -> None:
    assert sweep.isikukood_valid("37605030299")  # RIK's published example
    assert not sweep.isikukood_valid("37605030298")  # wrong check digit
    assert not sweep.isikukood_valid("37613320299")  # no 32nd day
    assert not sweep.isikukood_valid("12345678")


def test_a_foreign_company_number_is_not_an_estonian_register_page() -> None:
    assert _ee_company_url([{"scheme": "EE-ARIREGISTER", "id": "16752073"}]) == (
        "https://ariregister.rik.ee/eng/company/16752073"
    )
    assert _ee_company_url([{"scheme": "MT-MBR", "id": "C 53839"}]) is None
    assert _ee_company_url([]) is None
