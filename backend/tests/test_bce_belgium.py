"""The Belgium wiring (29 Oct 2026) — the Crossroads Bank for Enterprises,
read from a monthly index of the KBO Open Data full file.

Every index here is produced by the **real builder**
(``scripts/build_bce_belgium_index.py``) from a ZIP in the portal's exact
published shape (the cookbook's nine CSVs — the five it reads are written;
day-first dates; ``meta.csv`` as ``Variable,Value``) and read back through the
**real adapter** — the JAR / ONRC rule. Phase 35's hand-built fixture encoded
what the adapter expected, not what the file carries.
"""

from __future__ import annotations

import importlib.util
import os
import zipfile
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods import liveness as bods_liveness
from opencheck.bods.mapper import map_bce_belgium
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_bce_belgium
from opencheck.routers.lookup import _LookupCtx, _build_derived, _build_result_hit, _dispatch
from opencheck.sources import REGISTRY, bce_belgium
from opencheck.sources.base import SearchKind
from opencheck.sources.bce_belgium import (
    BCE_RA_CODE,
    format_enterprise_number,
    normalise_enterprise_number,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_bce_belgium_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_bce_belgium_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "04-10-2026"            # KBO writes it day-first
CUT_ISO = "2026-10-04"
PROXIMUS = "0202239951"       # NV, active
BANKRUPT = "0400000001"       # SA in bankruptcy (situation 050)
SOLE = "0700000002"           # a natural-person enterprise
NODOTS = "1000000003"         # a number starting 1 (post-2017 series)
ABSENT = "0999999999"

H = builder.HEADERS


def _enterprise_rows() -> list[str]:
    return [
        f"{PROXIMUS[:4]}.{PROXIMUS[4:7]}.{PROXIMUS[7:]},AC,000,2,014,,19-06-1930",
        f"{BANKRUPT[:4]}.{BANKRUPT[4:7]}.{BANKRUPT[7:]},AC,050,2,014,,01-02-2001",
        f"{SOLE[:4]}.{SOLE[4:7]}.{SOLE[7:]},AC,000,1,,,05-05-2015",
        f"{NODOTS[:4]}.{NODOTS[4:7]}.{NODOTS[7:]},AC,000,2,610,,03-03-2020",
        "not-a-number,AC,000,2,014,,01-01-2000",
    ]


def _denomination_rows() -> list[str]:
    return [
        f"{PROXIMUS[:4]}.{PROXIMUS[4:7]}.{PROXIMUS[7:]},2,001,Proximus",
        f"{PROXIMUS[:4]}.{PROXIMUS[4:7]}.{PROXIMUS[7:]},1,001,Proximus",
        f"{PROXIMUS[:4]}.{PROXIMUS[4:7]}.{PROXIMUS[7:]},2,002,PXS",
        f"{BANKRUPT[:4]}.{BANKRUPT[4:7]}.{BANKRUPT[7:]},1,001,ANCIENNE SOCIETE SA",
        f"{BANKRUPT[:4]}.{BANKRUPT[4:7]}.{BANKRUPT[7:]},1,003,Le Vieux Comptoir",
        f"{SOLE[:4]}.{SOLE[4:7]}.{SOLE[7:]},2,001,Jan Janssens",
        f"{NODOTS[:4]}.{NODOTS[4:7]}.{NODOTS[7:]},2,001,Nieuwe Zaak BV",
        # an establishment unit's name: not the enterprise's
        "2.123.456.789,2,003,Winkel Antwerpen",
    ]


def _address_rows() -> list[str]:
    return [
        f"{PROXIMUS[:4]}.{PROXIMUS[4:7]}.{PROXIMUS[7:]},REGO,België,Belgique,1030,Schaarbeek,Schaerbeek,Koning Albert II-laan,Boulevard du Roi Albert II,27,,,",
        f"{PROXIMUS[:4]}.{PROXIMUS[4:7]}.{PROXIMUS[7:]},OBAD,België,Belgique,1000,Brussel,Bruxelles,Oude straat,Vieille rue,1,,,",
        f"{BANKRUPT[:4]}.{BANKRUPT[4:7]}.{BANKRUPT[7:]},REGO,België,Belgique,4000,Luik,Liège,Rue de la Gare,Rue de la Gare,5,B,,",
        # a struck-off address is not the current one
        f"{NODOTS[:4]}.{NODOTS[4:7]}.{NODOTS[7:]},REGO,België,Belgique,9000,Gent,Gand,Veldstraat,Veldstraat,10,,,31-12-2024",
    ]


def _code_rows() -> list[str]:
    return [
        "JuridicalForm,014,EN,Public limited company",
        "JuridicalForm,014,NL,Naamloze vennootschap",
        "JuridicalForm,014,FR,Société anonyme",
        "JuridicalForm,610,NL,Besloten vennootschap",
        "JuridicalForm,610,FR,Société à responsabilité limitée",
        "JuridicalSituation,000,EN,Normal situation",
        "JuridicalSituation,050,EN,Bankruptcy opened",
        "JuridicalSituation,050,NL,Opening faillissement",
        "Status,AC,EN,Active",
        "Status,AC,NL,Actief",
        "TypeOfEnterprise,1,EN,Natural person",
        "TypeOfEnterprise,2,EN,Legal person",
    ]


def _zip(path: Path, *, meta: list[str] | None = None, enterprise: list[str] | None = None,
         denomination: list[str] | None = None, address: list[str] | None = None,
         drop: str | None = None, headers: dict[str, list[str]] | None = None) -> Path:
    files = {
        "enterprise.csv": _enterprise_rows() if enterprise is None else enterprise,
        "denomination.csv": _denomination_rows() if denomination is None else denomination,
        "address.csv": _address_rows() if address is None else address,
        "code.csv": _code_rows(),
        "meta.csv": [f"SnapshotDate,{CUT}", "ExtractTimestamp,05-10-2026 02:15:00",
                     "ExtractNumber,0140", "ExtractType,full"] if meta is None else meta,
        "activity.csv": [], "branch.csv": [], "contact.csv": [], "establishment.csv": [],
    }
    hdrs = {**H, **(headers or {})}
    with zipfile.ZipFile(path, "w") as zf:
        for name, rows in files.items():
            if name == drop:
                continue
            header = ",".join(hdrs.get(name, ["X"]))
            zf.writestr(name, "﻿" + "\n".join([header, *rows]) + "\n")
    return path


def _build(tmp_path: Path, name: str = "bce_belgium.sqlite", **zip_kwargs) -> tuple[Path, dict]:
    zpath = _zip(tmp_path / "KboOpenData_0140_2026_10_Full.zip", **zip_kwargs)
    out = tmp_path / "data" / name
    stats = builder.build(zpath, out, min_rows={})
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("BCE_BELGIUM_DB_FILE", None)
    else:
        os.environ["BCE_BELGIUM_DB_FILE"] = str(path)
    get_settings.cache_clear()
    bce_belgium.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    out, stats = _build(tmp_path)
    _point_at(out)
    yield out, stats
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    _point_at(None)
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    _point_at(None)


async def _fetch(number: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["bce_belgium"].fetch(number, legal_name=legal_name)


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_reads_the_portals_shape_and_records_kbos_cut(index):
    out, stats = index
    assert stats["enterprises"] == 4 and stats["enterprises_malformed"] == 1
    assert stats["natural_persons"] == 1
    assert stats["names"] == 7  # the establishment unit's row is skipped
    assert stats["addresses"] == 2  # REGO only, and not the struck-off one
    assert stats["codes"] == 12
    assert stats["cut"] == CUT_ISO
    meta = bce_belgium.read_meta()
    assert meta["cut"] == CUT_ISO and meta["schema_version"] == bce_belgium.INDEX_SCHEMA_VERSION
    assert meta["kbo_SnapshotDate"] == CUT  # KBO's own value, verbatim
    assert meta["kbo_ExtractNumber"] == "0140"
    assert provenance.parse_moment(meta["built_at"]) is not None


@pytest.mark.parametrize("kwargs,match", [
    ({"drop": "address.csv"}, "not in the ZIP"),
    ({"headers": {"enterprise.csv": ["EnterpriseNumber", "Status"]}}, "header"),
    ({"meta": ["ExtractNumber,0140"]}, "SnapshotDate"),
])
def test_the_builder_refuses_what_is_not_the_register(tmp_path, kwargs, match):
    zpath = _zip(tmp_path / "x.zip", **kwargs)
    with pytest.raises(builder.RefusedInput, match=match):
        builder.build(zpath, tmp_path / "x.sqlite", min_rows={})
    assert not (tmp_path / "x.sqlite").exists()


def test_the_builder_refuses_a_short_file_at_the_real_floors(tmp_path):
    zpath = _zip(tmp_path / "x.zip")
    with pytest.raises(builder.RefusedInput, match="not the register"):
        builder.build(zpath, tmp_path / "x.sqlite")


def test_the_newest_full_zip_is_picked_off_the_listing():
    html = (
        '<a href="files/KboOpenData_0139_2026_09_Full.zip">x</a>'
        '<a href="files/KboOpenData_0140_2026_10_Update.zip">y</a>'
        '<a href="files/KboOpenData_0140_2026_10_Full.zip">z</a>'
    )
    assert builder.newest_full_zip(html) == ("KboOpenData_0140_2026_10_Full.zip", "2026-10")
    assert builder.newest_full_zip("<html>login</html>") is None


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["0202.239.951", "0202239951", "BE 0202.239.951", " 0202 239 951 "])
def test_every_spelling_of_an_enterprise_number_is_ten_digits(raw):
    assert normalise_enterprise_number(raw) == PROXIMUS
    assert format_enterprise_number(raw) == "0202.239.951"


def test_the_adapter_declares_its_lookup_wiring():
    adapter = REGISTRY["bce_belgium"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({BCE_RA_CODE})
    assert deriver.derived_key == "be_enterprise_number"
    assert deriver.normalise("0202.239.951") == PROXIMUS
    assert adapter.lookup_pass_legal_name is True


# ---------------------------------------------------------------------------
# The adapter over the index
# ---------------------------------------------------------------------------


async def test_an_enterprise_in_the_index_is_a_dated_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch("0202.239.951", legal_name="PROXIMUS")
    assert bundle["is_stub"] is False and not bundle.get("not_found")
    assert bundle["name"] == "Proximus"
    assert bundle["name_nl"] == "Proximus" and bundle["name_fr"] == "Proximus"
    assert bundle["name_type"] == "001"
    assert bundle["dotted"] == "0202.239.951"
    assert bundle["status"] == "AC" and bundle["status_class"] == "live"
    assert bundle["status_label"] == "Active"
    assert bundle["juridical_form"] == "Public limited company"
    assert bundle["juridical_form_native"] == "Naamloze vennootschap"
    assert bundle["juridical_situation"] == "Normal situation"
    assert bundle["enterprise_type"] == "legal person"
    assert bundle["start_date"] == "1930-06-19"
    assert bundle["address"] == "Koning Albert II-laan 27, 1030 Schaarbeek, België"
    assert bundle["cut"] == CUT_ISO
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == CUT_ISO
    assert resolved.retrieved_at is not None


async def test_a_bankrupt_company_carries_the_registers_wording(index):
    bundle = await _fetch(BANKRUPT)
    assert bundle["juridical_situation"] == "Bankruptcy opened"
    assert bundle["name"] == "ANCIENNE SOCIETE SA"  # the official name over the commercial one
    assert bundle["address"] == "Rue de la Gare 5 box B, 4000 Luik, België"


async def test_a_struck_off_registered_office_is_not_the_address(index):
    bundle = await _fetch(NODOTS)
    assert bundle["address"] is None
    assert bundle["juridical_form"] == "Besloten vennootschap"  # NL when there is no EN


async def test_a_number_outside_the_index_is_a_note_card_not_a_degradation(index):
    from opencheck import degradation

    degradation.begin()
    with provenance.recording() as rec:
        bundle = await _fetch(ABSENT, legal_name="GONE NV")
    collected = degradation.collect()
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert "active entities only" in bundle["coverage_note"]
    assert bundle["cut"] == CUT_ISO
    assert collected == []
    assert not rec.observations


async def test_without_an_index_nothing_is_announced(no_index):
    adapter = REGISTRY["bce_belgium"]
    assert bce_belgium.index_available() is False
    assert adapter.covers_lei("549300CWRXC5EP004533") is False
    assert (await _fetch(PROXIMUS))["is_stub"] is True
    assert await adapter.search("Proximus", SearchKind.ENTITY) == []


async def test_with_an_index_the_source_covers_a_belgian_number(index):
    assert REGISTRY["bce_belgium"].covers_lei("549300CWRXC5EP004533") is True


async def test_a_wrong_schema_version_is_no_index(tmp_path):
    import sqlite3

    out, _ = _build(tmp_path)
    con = sqlite3.connect(out)
    con.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    con.commit()
    con.close()
    _point_at(out)
    try:
        assert bce_belgium.index_available() is False
        assert (await _fetch(PROXIMUS))["is_stub"] is True
    finally:
        _point_at(None)


async def test_name_search_finds_legal_persons_only(index):
    hits = await REGISTRY["bce_belgium"].search("Proximus", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [PROXIMUS]
    (hit,) = hits
    assert hit.summary == "BE 0202.239.951 · Public limited company"
    assert hit.identifiers == {"be_enterprise_number": PROXIMUS}
    assert hit.is_stub is False and hit.liveness == "snapshot"
    # A natural-person enterprise is not in the name index.
    assert await REGISTRY["bce_belgium"].search("Janssens", SearchKind.ENTITY) == []
    # A commercial name finds the enterprise.
    assert [h.hit_id for h in await REGISTRY["bce_belgium"].search("Vieux Comptoir", SearchKind.ENTITY)] == [BANKRUPT]
    assert await REGISTRY["bce_belgium"].search("Nobody", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_writes_the_number_as_gleif_files_it(index):
    with provenance.recording():
        (stmt,) = list(map_bce_belgium(await _fetch(PROXIMUS)))
    rd = stmt["recordDetails"]
    assert rd["name"] == "Proximus"
    assert rd["identifiers"] == [
        {"id": "0202.239.951", "scheme": "BE-BCE_KBO", "schemeName": "Crossroads Bank for Enterprises (Belgium)"},
        {"id": "BE0202239951", "scheme": "XI-VAT", "schemeName": "VAT number"},
    ]
    assert rd["jurisdiction"] == {"name": "Belgium", "code": "BE"}
    assert rd["foundingDate"] == "1930-06-19"
    assert rd["entityType"] == {
        "type": "registeredEntity",
        "details": "Public limited company (Naamloze vennootschap)",
    }
    (addr,) = rd["addresses"]
    assert addr["type"] == "registered" and addr["country"]["code"] == "BE"
    assert bods_liveness.read_register_status(stmt)["liveness"] == "live"


async def test_the_mapper_says_the_situation_and_the_kind_of_enterprise(index):
    with provenance.recording():
        (bankrupt,) = list(map_bce_belgium(await _fetch(BANKRUPT)))
        (sole,) = list(map_bce_belgium(await _fetch(SOLE)))
    status = bods_liveness.read_register_status(bankrupt)
    assert status["liveness"] == "live"  # KBO's status is still AC
    assert "Bankruptcy opened" in status["raw"]
    assert sole["recordDetails"]["entityType"]["details"] == "natural-person enterprise"
    assert sole["recordDetails"]["name"] == "Jan Janssens"


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_bce_belgium({"is_stub": True})) == []
    assert list(map_bce_belgium({"is_stub": False, "not_found": True, "enterprise_number": PROXIMUS})) == []


# ---------------------------------------------------------------------------
# The sentence, the card and the pipeline
# ---------------------------------------------------------------------------


async def test_the_finding_sentence(index):
    s = finding_bce_belgium(await _fetch(PROXIMUS))
    assert s == "On the register since 1930-06-19, Public limited company."
    assert len(s) <= MAX_FINDING_CHARS
    assert finding_bce_belgium(await _fetch(BANKRUPT)) == "Bankruptcy opened, Public limited company."
    assert finding_bce_belgium(await _fetch(SOLE)) == "On the register since 2015-05-05, natural-person enterprise."
    assert finding_bce_belgium(await _fetch(ABSENT)).startswith("Not in the KBO open data")
    assert finding_bce_belgium({"is_stub": True}) is None


async def test_the_hit_builder_asserts_the_number_only_when_the_register_published_it(index):
    from opencheck.routers.hit_builders import _bh_bce_belgium

    ctx = _LookupCtx(lei="549300CWRXC5EP004533", legal_name="PROXIMUS")
    hit = _bh_bce_belgium(await _fetch(PROXIMUS), PROXIMUS, ctx)
    assert hit.identifiers == {"be_enterprise_number": PROXIMUS}
    assert hit.summary == "BE 0202.239.951"
    assert hit.finding.startswith("On the register since 1930")
    other = _LookupCtx(lei="549300CWRXC5EP004533", legal_name="GONE NV")
    hit = _bh_bce_belgium(await _fetch(ABSENT, legal_name="GONE NV"), ABSENT, other)
    assert hit.identifiers == {} and hit.name == "GONE NV"
    assert hit.summary == "BE 0999.999.999 · not in the index"
    assert hit.raw["coverage_note"]


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="bce_belgium")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index):
    ctx = _LookupCtx(lei="549300CWRXC5EP004533", jurisdiction="BE", registered_as="0202.239.951")
    _build_derived(ctx, BCE_RA_CODE)
    assert ctx.derived.get("be_enterprise_number") == PROXIMUS
    assert _dispatched(ctx) == {"bce_belgium"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="549300CWRXC5EP004533", jurisdiction="BE", registered_as="0202.239.951")
    _build_derived(ctx, BCE_RA_CODE)
    assert _dispatched(ctx) == set()


async def test_the_note_card_survives_the_pipelines_stub_drop(index):
    bundle = await _fetch(ABSENT, "GONE NV")
    ctx = _LookupCtx(lei="549300CWRXC5EP004533", legal_name="GONE NV", jurisdiction="BE", registered_as="0999.999.999")
    _build_derived(ctx, BCE_RA_CODE)
    hit = _build_result_hit("bce_belgium", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.finding and "Not in the KBO open data" in hit.finding
