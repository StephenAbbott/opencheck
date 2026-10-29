"""The Cyprus wiring (29 Oct 2026) — the Registrar of Companies, read from an
index of data.gov.cy's three CSV distributions.

Every index here is produced by the **real builder**
(``scripts/build_cyprus_drcor_index.py``) from CSVs in the portal's exact
published shape (BOM, day-first dates, trailing spaces inside quoted address
fields) and read back through the **real adapter** — the JAR / ONRC rule: a
hand-written index encodes what the adapter expects, not what the builder
writes.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods import liveness as bods_liveness
from opencheck.bods.mapper import map_cyprus_drcor
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_cyprus_drcor
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _LookupCtx
from opencheck.sources import REGISTRY, cyprus_drcor
from opencheck.sources.base import SearchKind
from opencheck.sources.cyprus_drcor import (
    CY_DRCOR_RA_CODE,
    display_number,
    normalise_he_number,
    register_identifier,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_cyprus_drcor_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_cyprus_drcor_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-09-29"
BERN = "449149"       # HE, registered, a corporate secretary and a director
STRUCK = "1"          # AE (overseas company) struck off in 1974
LIQ = "200200"        # HE, members' voluntary liquidation, dated
BIZNAME = "10001"     # BN, a business name with an owner
PARTNER = "10000"     # S, a general partnership
ABSENT = "999999"     # in no file

ORG_HEADER = ",".join(builder.HEADERS["organisations"])
OFFICE_HEADER = ",".join(builder.HEADERS["registered_office"])
OFFICIALS_HEADER = ",".join(builder.HEADERS["organisation_officials"])


def _org_rows() -> list[str]:
    return [
        f"BERN INVESTMENTS LIMITED,{BERN},C,Εταιρεία,Ιδιωτική,ACR,Τελευταίο Όνομα,13/07/2023,Εγγεγραμμένη,,24",
        f"CYPRUS MINES CORPORATION,{STRUCK},O,Αλλοδαπή Εταιρεία,,ACR,Τελευταίο Όνομα,20/06/1923,Διαγραμμένη,16/08/1974,",
        f"OLD HOLDINGS LIMITED,{LIQ},C,Εταιρεία,Ιδιωτική,ACR,Τελευταίο Όνομα,01/02/2001,Εκούσια Εκκαθάριση από Μετόχους,03/03/2025,12",
        f"TABOO DISCOTHEQUE,{BIZNAME},B,Εμπορική Επωνυμία,,ACR,Τελευταίο Όνομα,05/05/1995,Εγγεγραμμένη,,",
        f"SEA DOO WORLD,{PARTNER},P,Συνεταιρισμός,Ομόρρυθμος,ACR,Τελευταίο Όνομα,06/06/1996,Εγγεγραμμένη,,",
        # malformed: a non-numeric number
        "BROKEN ROW,HEX,C,Εταιρεία,Ιδιωτική,ACR,Τελευταίο Όνομα,01/01/2000,Εγγεγραμμένη,,",
    ]


def _office_rows() -> list[str]:
    return [
        '12,"Meydan Road, ",, ',
        '24,"Ridge Green, 1","MINTHIS HILLS","Τσάδα, 8540, Πάφος, Κύπρος"',
    ]


def _officials_rows() -> list[str]:
    return [
        f"BERN INVESTMENTS LIMITED,{BERN},C,Εταιρεία,M. KYPRIANOU FIDUCIARIES (CYPRUS) LTD,Γραμματέας",
        f"BERN INVESTMENTS LIMITED,{BERN},C,Εταιρεία,SOREN LERBY,Διευθυντής",
        f"BERN INVESTMENTS LIMITED,{BERN},C,Εταιρεία,SOREN LERBY,Αντικαταστάτης Διευθυντής",
        f"OLD HOLDINGS LIMITED,{LIQ},C,Εταιρεία,ANNA CHRISTOU,Διευθυντής",
        f"TABOO DISCOTHEQUE,{BIZNAME},B,Εμπορική Επωνυμία,ANDREAS ANDREOU,Ιδιοκτήτης",
        f"SEA DOO WORLD,{PARTNER},P,Συνεταιρισμός,MARIA GEORGIOU,Ομόρρυθμος Συνέταιρος",
        f"SEA DOO WORLD,{PARTNER},P,Συνεταιρισμός,SEA TRADING LTD,Ετερόρρυθμος Συνέταιρος",
    ]


def _write(path: Path, header: str, rows: list[str]) -> Path:
    path.write_text("﻿" + "\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


def _build(tmp_path: Path, *, orgs: list[str] | None = None, officials: list[str] | None = None,
           name: str = "cyprus_drcor.sqlite") -> tuple[Path, dict]:
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    paths = {
        "organisations": _write(src / "organisations_102.csv", ORG_HEADER, _org_rows() if orgs is None else orgs),
        "registered_office": _write(src / "registered_office_101.csv", OFFICE_HEADER, _office_rows()),
        "organisation_officials": _write(
            src / "organisation_officials_88.csv", OFFICIALS_HEADER,
            _officials_rows() if officials is None else officials,
        ),
    }
    out = tmp_path / "data" / name
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(paths, out, CUT, min_rows={})
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("CYPRUS_DRCOR_DB_FILE", None)
    else:
        os.environ["CYPRUS_DRCOR_DB_FILE"] = str(path)
    get_settings.cache_clear()
    cyprus_drcor.reset_connection()


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


async def _fetch(code: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["cyprus_drcor"].fetch(code, legal_name=legal_name)


def _by_type(statements) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {"entity": [], "person": [], "relationship": []}
    for s in statements:
        out[s["recordType"]].append(s)
    return out


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_reads_the_portals_shape_and_records_its_cut(index):
    out, stats = index
    assert stats["organisations"] == 5 and stats["organisations_malformed"] == 1
    assert stats["addresses"] == 2 and stats["officials"] == 7
    meta = cyprus_drcor.read_meta()
    assert meta["cut"] == CUT and meta["schema_version"] == cyprus_drcor.INDEX_SCHEMA_VERSION
    assert provenance.parse_moment(meta["built_at"]) is not None
    assert meta["count_organisations"] == "5"


def test_the_builder_refuses_a_file_with_another_header(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    bad = _write(src / "organisations_1.csv", "NAME,NUMBER", ["X,1"])
    paths = {
        "organisations": bad,
        "registered_office": _write(src / "registered_office_1.csv", OFFICE_HEADER, _office_rows()),
        "organisation_officials": _write(src / "organisation_officials_1.csv", OFFICIALS_HEADER, []),
    }
    with pytest.raises(builder.RefusedInput):
        builder.build(paths, tmp_path / "x.sqlite", CUT, min_rows={})
    assert not (tmp_path / "x.sqlite").exists()


def test_the_builder_refuses_a_short_file_and_a_bad_cut(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    paths = {
        "organisations": _write(src / "organisations_1.csv", ORG_HEADER, _org_rows()),
        "registered_office": _write(src / "registered_office_1.csv", OFFICE_HEADER, _office_rows()),
        "organisation_officials": _write(src / "organisation_officials_1.csv", OFFICIALS_HEADER, _officials_rows()),
    }
    with pytest.raises(builder.RefusedInput, match="not the register"):
        builder.build(paths, tmp_path / "x.sqlite", CUT)  # the real floors
    with pytest.raises(builder.RefusedInput, match="YYYY-MM-DD"):
        builder.build(paths, tmp_path / "x.sqlite", "29/09/2026", min_rows={})


def test_the_portals_html_shell_is_not_a_csv(tmp_path):
    """A wrong link answers 200 with the SPA's HTML; the header check refuses it."""
    src = tmp_path / "src"
    src.mkdir()
    html = src / "organisations_1.csv"
    html.write_text("<!DOCTYPE html><html><head><title>data.gov.cy</title></head></html>\n")
    paths = {
        "organisations": html,
        "registered_office": _write(src / "registered_office_1.csv", OFFICE_HEADER, _office_rows()),
        "organisation_officials": _write(src / "organisation_officials_1.csv", OFFICIALS_HEADER, []),
    }
    with pytest.raises(builder.RefusedInput):
        builder.build(paths, tmp_path / "x.sqlite", CUT, min_rows={})


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["ΗΕ 449149", "HE 449149", "HE449149", "449149", " ΗΕ449149 "])
def test_every_spelling_of_an_he_number_is_the_digits(raw):
    assert normalise_he_number(raw) == "449149"


def test_the_identifier_is_spelled_as_gleif_files_it():
    """GLEIF files ``ΗΕ 449149`` — Greek eta and epsilon, a space — on 200 of
    200 ISSUED records under RA000161 (29 Oct 2026); the BODS identifier is
    written the same way so the two statements meet on CY-DRCOR."""
    assert register_identifier("C", "449149") == "ΗΕ 449149"
    assert register_identifier("C", "449149")[:2] == "ΗΕ"
    assert display_number("C", "449149") == "HE 449149"
    assert display_number("B", "10001") == "BN 10001"


def test_the_adapter_declares_its_lookup_wiring():
    adapter = REGISTRY["cyprus_drcor"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({CY_DRCOR_RA_CODE})
    assert deriver.derived_key == "cy_he"
    assert deriver.normalise("ΗΕ 449149") == "449149"
    assert adapter.lookup_pass_legal_name is True


# ---------------------------------------------------------------------------
# The adapter over the index
# ---------------------------------------------------------------------------


async def test_a_company_in_the_index_is_a_dated_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch("ΗΕ 449149", legal_name="Bern Investments")
    assert bundle["is_stub"] is False and not bundle.get("not_found")
    assert bundle["name"] == "BERN INVESTMENTS LIMITED"
    assert bundle["type_code"] == "C" and bundle["sub_type"] == "Ιδιωτική"
    assert bundle["registered_on"] == "2023-07-13"
    assert bundle["status"] == "Εγγεγραμμένη" and bundle["status_class"] == "live"
    assert bundle["address"] == {
        "street": "Ridge Green, 1", "building": "MINTHIS HILLS",
        "territory": "Τσάδα, 8540, Πάφος, Κύπρος",
    }
    assert [o["position"] for o in bundle["officials"]] == [
        "Γραμματέας", "Διευθυντής", "Αντικαταστάτης Διευθυντής",
    ]
    assert bundle["cut"] == CUT
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == CUT
    assert resolved.retrieved_at is not None


async def test_a_struck_off_overseas_company_keeps_its_date(index):
    bundle = await _fetch("O:1")
    assert bundle["type_code"] == "O" and bundle["status_class"] == "terminal"
    assert bundle["status_date"] == "1974-08-16"
    assert bundle["registered_on"] == "1923-06-20"
    assert not bundle.get("address")  # no ADDRESS_SEQ_NO


async def test_a_number_outside_the_index_is_a_note_card_not_a_degradation(index):
    from opencheck import degradation

    degradation.begin()
    with provenance.recording() as rec:
        bundle = await _fetch(ABSENT, legal_name="GHOST LTD")
    collected = degradation.collect()
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert "not in the Registrar of Companies' open data" in bundle["coverage_note"]
    assert bundle["cut"] == CUT
    assert collected == []
    assert not rec.observations  # a miss is not a read of data


async def test_without_an_index_nothing_is_announced(no_index):
    adapter = REGISTRY["cyprus_drcor"]
    assert cyprus_drcor.index_available() is False
    assert adapter.covers_lei("254900YI1OJO26HFY821") is False
    bundle = await _fetch("449149")
    assert bundle["is_stub"] is True
    assert await adapter.search("BERN", SearchKind.ENTITY) == []


async def test_with_an_index_the_source_covers_a_cypriot_number(index):
    adapter = REGISTRY["cyprus_drcor"]
    assert adapter.covers_lei("254900YI1OJO26HFY821") is True


async def test_a_wrong_schema_version_is_no_index(tmp_path):
    import sqlite3

    out, _ = _build(tmp_path)
    con = sqlite3.connect(out)
    con.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    con.commit()
    con.close()
    _point_at(out)
    try:
        assert cyprus_drcor.index_available() is False
        assert (await _fetch("449149"))["is_stub"] is True
    finally:
        _point_at(None)


async def test_name_search_ranks_companies_and_exact_names_first(index):
    hits = await REGISTRY["cyprus_drcor"].search("SEA", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == ["P:10000"]
    (hit,) = hits
    assert hit.summary == "CY S 10000 · general partnership"
    assert hit.identifiers == {} and hit.is_stub is False and hit.liveness == "snapshot"
    hits = await REGISTRY["cyprus_drcor"].search("LIMITED", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [BERN, LIQ]
    assert hits[0].identifiers == {"cy_he": BERN}
    assert hits[1].summary == "CY HE 200200 · private company · members' voluntary liquidation"


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_draws_the_company_its_officials_and_their_roles(index):
    with provenance.recording():
        bundle = await _fetch("449149")
        out = _by_type(map_cyprus_drcor(bundle))
    company, secretary_co = out["entity"]
    rd = company["recordDetails"]
    assert rd["name"] == "BERN INVESTMENTS LIMITED"
    assert rd["identifiers"] == [{
        "id": "ΗΕ 449149", "scheme": "CY-DRCOR",
        "schemeName": "Cyprus registration number — Department of Registrar of Companies and Intellectual Property",
    }]
    assert rd["jurisdiction"] == {"name": "Cyprus", "code": "CY"}
    assert rd["foundingDate"] == "2023-07-13"
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Private company (Εταιρεία — Ιδιωτική)"}
    (addr,) = rd["addresses"]
    assert addr["type"] == "registered" and addr["country"]["code"] == "CY"
    assert "MINTHIS HILLS" in addr["address"]
    assert bods_liveness.read_register_status(company)["liveness"] == "live"
    # The corporate secretary: an entity by name, no country assumed.
    assert secretary_co["recordDetails"]["name"] == "M. KYPRIANOU FIDUCIARIES (CYPRUS) LTD"
    assert "jurisdiction" not in secretary_co["recordDetails"]
    # One person for the director who is also alternate director.
    (person,) = out["person"]
    assert person["recordDetails"]["names"][0]["fullName"] == "SOREN LERBY"
    rels = out["relationship"]
    assert len(rels) == 3
    by_party = {}
    for r in rels:
        assert r["recordDetails"]["subject"] == company["recordId"]
        by_party.setdefault(r["recordDetails"]["interestedParty"], []).append(r["recordDetails"]["interests"][0])
    (sec,) = by_party[secretary_co["recordId"]]
    assert sec["type"] == "otherInfluenceOrControl"
    assert sec["opencheckOfficerPosition"] == "Secretary"  # an officer, drawn as a role
    assert sec["beneficialOwnershipOrControl"] is False
    kinds = sorted(i["details"] for i in by_party[person["recordId"]])
    assert kinds == ["Alternate director (Αντικαταστάτης Διευθυντής)", "Director (Διευθυντής)"]
    assert all(i["type"] == "seniorManagingOfficial" for i in by_party[person["recordId"]])
    assert all("opencheckOfficerPosition" not in i for i in by_party[person["recordId"]])


async def test_a_liquidation_and_a_strike_off_reach_the_status(index):
    with provenance.recording():
        liq = _by_type(map_cyprus_drcor(await _fetch("200200")))["entity"][0]
        gone = _by_type(map_cyprus_drcor(await _fetch("O:1")))["entity"][0]
    assert bods_liveness.read_register_status(liq)["liveness"] == "pending"
    assert bods_liveness.read_register_status(gone)["liveness"] == "terminal"
    assert gone["recordDetails"]["dissolutionDate"] == "1974-08-16"
    assert gone["recordDetails"]["identifiers"][0]["id"] == "AE 1"
    assert gone["recordDetails"]["entityType"]["details"] == "Overseas company (Αλλοδαπή Εταιρεία)"


async def test_a_business_name_owner_and_partners_are_not_officers(index):
    with provenance.recording():
        bn = _by_type(map_cyprus_drcor(await _fetch("B:10001")))
        pt = _by_type(map_cyprus_drcor(await _fetch("P:10000")))
    (owner_rel,) = bn["relationship"]
    (i,) = owner_rel["recordDetails"]["interests"]
    assert i["type"] == "otherInfluenceOrControl" and "opencheckOfficerPosition" not in i
    assert i["details"] == "Owner (Ιδιοκτήτης)"
    assert bn["entity"][0]["recordDetails"]["entityType"]["details"] == "Business name (Εμπορική Επωνυμία)"
    assert bn["entity"][0]["recordDetails"]["identifiers"][0]["id"] == "BN 10001"
    kinds = {r["recordDetails"]["interestedParty"]: r["recordDetails"]["interests"][0] for r in pt["relationship"]}
    assert len(kinds) == 2
    assert {i["details"] for i in kinds.values()} == {
        "General partner (Ομόρρυθμος Συνέταιρος)", "Limited partner (Ετερόρρυθμος Συνέταιρος)",
    }
    assert all(i["type"] == "otherInfluenceOrControl" for i in kinds.values())
    # SEA TRADING LTD is a corporate partner.
    assert [e["recordDetails"]["name"] for e in pt["entity"]] == ["SEA DOO WORLD", "SEA TRADING LTD"]


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_cyprus_drcor({"is_stub": True})) == []
    assert list(map_cyprus_drcor({"is_stub": False, "not_found": True, "reg_no": "1"})) == []


# ---------------------------------------------------------------------------
# The sentence and the card
# ---------------------------------------------------------------------------


async def test_the_finding_sentence_counts_in_one_voice(index):
    s = finding_cyprus_drcor(await _fetch("449149"))
    assert s == "On the register since 2023-07-13, private company, 2 directors on file, 1 secretary."
    assert len(s) <= MAX_FINDING_CHARS
    s = finding_cyprus_drcor(await _fetch("200200"))
    assert s == "Members' voluntary liquidation since 2025-03-03, private company, 1 director on file, 0 secretaries."
    s = finding_cyprus_drcor(await _fetch("O:1"))
    assert s == "Struck off since 1974-08-16, overseas company, 0 officials on file."
    s = finding_cyprus_drcor(await _fetch("P:10000"))
    assert s == "On the register since 1996-06-06, general partnership, 2 officials on file."
    assert finding_cyprus_drcor(await _fetch(ABSENT)).startswith("Not in the Registrar of Companies' open data")
    assert finding_cyprus_drcor({"is_stub": True}) is None


async def test_the_hit_builder_asserts_the_number_only_when_the_register_published_it(index):
    from opencheck.routers.hit_builders import _bh_cyprus_drcor

    ctx = _LookupCtx(lei="254900YI1OJO26HFY821", legal_name="BERN INVESTMENTS LIMITED")
    hit = _bh_cyprus_drcor(await _fetch("449149"), "449149", ctx)
    assert hit.identifiers == {"cy_he": "449149"}
    assert hit.summary == "CY HE 449149"
    assert hit.finding.startswith("On the register since 2023-07-13")
    hit = _bh_cyprus_drcor(await _fetch("200200"), "200200", ctx)
    assert hit.summary == "CY HE 200200 · members' voluntary liquidation"
    other = _LookupCtx(lei="254900YI1OJO26HFY821", legal_name="GHOST LTD")
    hit = _bh_cyprus_drcor(await _fetch(ABSENT, legal_name="GHOST LTD"), ABSENT, other)
    assert hit.identifiers == {} and hit.name == "GHOST LTD"
    assert hit.summary == "CY HE 999999 · not in the index"
    assert hit.raw["coverage_note"]


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="cyprus_drcor")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index):
    ctx = _LookupCtx(lei="254900YI1OJO26HFY821", jurisdiction="CY", registered_as="ΗΕ 449149")
    _build_derived(ctx, CY_DRCOR_RA_CODE)
    assert ctx.derived.get("cy_he") == "449149"
    assert _dispatched(ctx) == {"cyprus_drcor"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="254900YI1OJO26HFY821", jurisdiction="CY", registered_as="ΗΕ 449149")
    _build_derived(ctx, CY_DRCOR_RA_CODE)
    assert _dispatched(ctx) == set()


async def test_the_note_card_survives_the_pipelines_stub_drop(index):
    bundle = await _fetch(ABSENT, "GHOST LTD")
    ctx = _LookupCtx(lei="254900YI1OJO26HFY821", legal_name="GHOST LTD", jurisdiction="CY", registered_as=f"ΗΕ {ABSENT}")
    _build_derived(ctx, CY_DRCOR_RA_CODE)
    hit = _build_result_hit("cyprus_drcor", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {}, "the register published no identifier for us"
    assert hit.finding and "Not in the Registrar of Companies" in hit.finding


async def test_a_record_asserts_the_number_it_was_published_under(index):
    bundle = await _fetch("449149")
    ctx = _LookupCtx(lei="254900YI1OJO26HFY821", legal_name="Bern", jurisdiction="CY", registered_as="ΗΕ 449149")
    _build_derived(ctx, CY_DRCOR_RA_CODE)
    hit = _build_result_hit("cyprus_drcor", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {"cy_he": "449149"}
    assert hit.summary == "CY HE 449149"
