"""The Peru RNP wiring (12 Nov 2026) — OECE's *Personas declaradas en la
conformación jurídica de proveedores*, read from one index built from the
monthly pipe-delimited file.

Every index here is produced by the **real builder**
(``scripts/build_oece_rnp_index.py``) from a file in the published shape
(Latin-1, CRLF, a tab before some document numbers) and read back through
the **real adapter** — the JAR / ONRC / Argentina rule.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from opencheck import degradation, provenance
from opencheck.bods.mapper import gleif_registration_scheme, map_oece_rnp
from opencheck.bods.mappers.peru import PE_SELF_DECLARATION_NOTE
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_oece_rnp
from opencheck.identifiers import peruvian_ruc, ruc_check_ok
from opencheck.register_hops import hop_for
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources import oece_rnp as pe
from opencheck.sources.schemas.oece_rnp import OeceRnpBundle

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_oece_rnp_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_oece_rnp_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

_W = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)


def _ruc(base: str) -> str:
    check = 11 - sum(int(d) * w for d, w in zip(base, _W, strict=True)) % 11
    return base + str({10: 0, 11: 1}.get(check, check))


SUNAT_RA = "RA000689"
BCP = "20100047218"            # a S.A.A. with a corporate shareholder, OTROS and a board
GRUPO = "20101247431"          # BCP's declared corporate shareholder, a supplier itself
EIRL = _ruc("2061029885")      # an E.I.R.L. and its titular
ASSOC = _ruc("2050000001")     # an association: its members hold no shares
ABSENT = _ruc("2050000099")    # not a supplier
PERSONAL = _ruc("1012345678")  # a natural person's RUC, filed as a member's document
DNI = "41234567"
PERSON = "CAVERO BELAUNDE DIEGO ANTONIO"
HEADER = "|".join(builder.HEADER)


def _row(ruc: str, role: str, name: str, document: str = DNI, *, form: str = "SOCIEDAD ANONIMA ABIERTA",
         form_id: str = "4", doc_type: str = "DOC. NACIONAL DE IDENTIDAD", since: str = "20241221") -> str:
    return "|".join(["20261102", doc_type, document, name, ruc, role, since, form_id, form])


def _rows() -> list[str]:
    return [
        _row(BCP, "SOCIO_ACCIONISTA_TITULAR", "GRUPO CREDITO S.A.", GRUPO, doc_type="RUC"),
        _row(BCP, "SOCIO_ACCIONISTA_TITULAR", "ACCIONISTAS MINORITARIOS", "NO ESPECIFICADO"),
        _row(BCP, "SOCIO_ACCIONISTA_TITULAR", "SUCESIÓN DE JORGE AVENDAÑO VALDEZ", "\t08765432"),
        _row(BCP, "SOCIO_ACCIONISTA_TITULAR", "Henkel AG & Co. KGaA", "000123456",
             doc_type="CARNET DE EXTRANJERIA"),
        _row(BCP, "SOCIO_ACCIONISTA_TITULAR", "PEREZ GOMEZ ANA", PERSONAL, doc_type="RUC"),
        _row(BCP, "REPRESENTANTE_LEGAL", PERSON, f"\t{DNI}"),
        _row(BCP, "ORGANO_ADMINISTRACION", "ALIÑO PEREZ NURIA", " 07654321 "),
        _row(BCP, "ORGANO_ADMINISTRACION", PERSON.lower(), DNI),
        _row(GRUPO, "ORGANO_ADMINISTRACION", "ROMERO BELISMELIS LUIS ENRIQUE", "07777777",
             form="SOCIEDAD ANONIMA", form_id="2"),
        _row(EIRL, "SOCIO_ACCIONISTA_TITULAR", "SOLIS MALLQUI DANTE TORIBIO", "\t31883251",
             form="EMPRESA INDIVIDUAL DE RESPONSABILIDAD LIMITADA", form_id="3", since="20210401"),
        _row(ASSOC, "SOCIO_ACCIONISTA_TITULAR", "QUISPE CHILON MANUEL CORPUS", "40000000",
             form="ASOCIACION", form_id="7"),
        # malformed: a bad check digit and a short row are counted, not kept
        _row(BCP[:-1] + "0", "REPRESENTANTE_LEGAL", "NADIE", DNI),
        "20261102|DNI|1",
    ]


def _write(tmp_path: Path, rows: list[str] | None = None, header: str = HEADER) -> Path:
    path = tmp_path / "conformacion_juridica.csv"
    path.write_bytes(("\r\n".join([header, *(rows if rows is not None else _rows())]) + "\r\n").encode("latin-1"))
    return path


def _build(tmp_path: Path, **kw: Any) -> tuple[Path, dict[str, Any]]:
    csv_path = _write(tmp_path, **kw)
    out = tmp_path / "data" / "oece_rnp.sqlite"
    out.parent.mkdir(exist_ok=True)
    return out, builder.build(csv_path, out, min_rows=0)


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("OECE_RNP_DB_FILE", None)
    else:
        os.environ["OECE_RNP_DB_FILE"] = str(path)
    get_settings.cache_clear()
    pe.reset_connection()


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


async def _fetch(ruc: str, legal_name: str = "BANCO DE CREDITO DEL PERU") -> dict[str, Any]:
    return await REGISTRY["oece_rnp"].fetch(ruc, legal_name=legal_name)


async def _map(ruc: str, legal_name: str = "BANCO DE CREDITO DEL PERU") -> list[dict[str, Any]]:
    with provenance.recording():
        bundle = await _fetch(ruc, legal_name)
        return list(map_oece_rnp(bundle))


# ---------------------------------------------------------------------------
# Identifiers and parties
# ---------------------------------------------------------------------------


def test_the_ruc_check_digit():
    assert ruc_check_ok(BCP) and ruc_check_ok(GRUPO)
    assert not ruc_check_ok(BCP[:-1] + "0")
    assert peruvian_ruc(" 20100047218 ") == BCP
    assert peruvian_ruc("20-10004721-8") == BCP
    for bad in ("12345678", "2010004721", "SUNARP 11008800", ""):
        assert peruvian_ruc(bad) is None
        with pytest.raises(ValueError):
            pe.normalise_ruc(bad)


@pytest.mark.parametrize(
    ("name", "document", "expected"),
    [
        ("GRUPO CREDITO S.A.", GRUPO, ("E", GRUPO)),
        ("HILASACA MAMANI ALFONSO", _ruc("2061086984"), ("E", _ruc("2061086984"))),
        ("Henkel AG & Co. KGaA", "000123456", ("E", None)),
        ("Siemens.s.a.", DNI, ("E", None)),
        ("CENTRAL REPUBLIC TRADING INC-", DNI, ("E", None)),
        ("cryoholdco de latinoamerica s.a.p.i. de c.v.", "", ("E", None)),
        ("REMOLCADORES ULTRATUG LIMITADA", "", ("E", None)),
        ("SUCESIÓN DE JORGE AVENDAÑO VALDEZ", DNI, ("A", None)),
        ("sociedad conyugal velasque y diaz", DNI, ("A", None)),
        ("ACCIONISTAS MINORITARIOS", "NO ESPECIFICADO", ("U", None)),
        ("otros", "", ("U", None)),
        (PERSON, DNI, ("P", None)),
        ("LUIS EDUARDO SAC ALARCON", DNI, ("P", None)),
        ("VILLA BENAVENTE CORPUS ULDARICO", DNI, ("P", None)),
        # a natural person's RUC is built on the DNI: never kept
        ("PEREZ GOMEZ ANA", PERSONAL, ("P", None)),
    ],
)
def test_classify_party(name, document, expected):
    assert pe.classify_party(name, document) == expected


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_keeps_no_document_number(index):
    out, stats = index
    assert stats["cut"] == "2026-11-02" and stats["malformed"] == "2"
    raw = out.read_bytes()
    for number in (DNI, "08765432", "07654321", "31883251", "000123456", PERSONAL):
        assert number.encode() not in raw
    conn = sqlite3.connect(out)
    columns = {r[1] for r in conn.execute("PRAGMA table_info(party)")}
    assert columns == {"ruc", "seq", "role", "kind", "name", "party_ruc", "since"}
    assert conn.execute("SELECT party_ruc FROM party WHERE party_ruc IS NOT NULL").fetchall() == [(int(GRUPO),)]
    meta = dict(conn.execute("SELECT key, value FROM meta"))
    assert meta["schema_version"] == pe.INDEX_SCHEMA_VERSION
    assert json.loads(meta["kinds"]) == {"E": 2, "A": 1, "U": 1, "P": 7}
    assert meta["built_at"].endswith("Z") and "T" in meta["built_at"]


def test_the_builder_refuses_a_changed_header(tmp_path):
    csv_path = _write(tmp_path, header=HEADER.replace("RUC|", "RUC_PROVEEDOR|"))
    with pytest.raises(builder.RefusedInput):
        builder.build(csv_path, tmp_path / "x.sqlite", min_rows=0)
    assert not (tmp_path / "x.sqlite").exists()


def test_the_builder_refuses_two_cuts_and_a_short_file(tmp_path):
    rows = _rows()[:3] + [_row(EIRL, "REPRESENTANTE_LEGAL", "X Y Z").replace("20261102", "20261001", 1)]
    with pytest.raises(builder.RefusedInput, match="cut"):
        builder.build(_write(tmp_path, rows=rows), tmp_path / "x.sqlite", min_rows=0)
    with pytest.raises(builder.RefusedInput, match="rows"):
        builder.build(_write(tmp_path), tmp_path / "y.sqlite")


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_a_supplier_is_read_from_the_index(index):
    with provenance.recording() as rec:
        bundle = await _fetch(BCP)
    assert bundle["ruc"] == BCP and bundle["form"] == "SOCIEDAD ANONIMA ABIERTA"
    assert bundle["counts"] == {"S": 5, "R": 1, "O": 2}
    assert bundle["cut"] == "2026-11-02"
    assert rec.resolve(is_stub=False).liveness == "snapshot"
    assert [p["ruc"] for p in bundle["parties"] if p["ruc"]] == [GRUPO]


async def test_a_ruc_not_in_the_index_is_a_note_card_not_a_degradation(index):
    with degradation.recording() as degraded:
        bundle = await _fetch(ABSENT, "Ghost S.A.C.")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["coverage_note"] == pe.COVERAGE_NOT_IN_INDEX
    assert list(degraded) == []
    assert list(map_oece_rnp(bundle)) == []


async def test_no_supplier_name_draws_nothing_and_says_so(index):
    with degradation.recording() as degraded:
        bundle = await _fetch(BCP, "")
    assert [d.check for d in degraded] == ["source_read"]
    assert list(map_oece_rnp(bundle)) == []


async def test_without_an_index_nothing_is_read(no_index):
    assert REGISTRY["oece_rnp"].covers_lei("549300EQYQ8SCQZ4BY14") is False
    assert (await _fetch(BCP))["is_stub"] is True


async def test_search_is_by_ruc_only(index):
    hits = await REGISTRY["oece_rnp"].search(BCP, pe.SearchKind.ENTITY)
    assert [h.identifiers for h in hits] == [{"pe_ruc": BCP}]
    assert await REGISTRY["oece_rnp"].search("Banco de Credito", pe.SearchKind.ENTITY) == []


def test_the_schema_refuses_a_document_number():
    with pytest.raises(ValidationError):
        OeceRnpBundle.model_validate({"ruc": BCP, "parties": [{"seq": 1, "role": "R", "kind": "P",
                                                                "name": PERSON, "numero_documento": DNI}]})


# ---------------------------------------------------------------------------
# BODS
# ---------------------------------------------------------------------------


def _by_type(statements: list[dict[str, Any]], record_type: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == record_type]


async def test_the_mapper(index):
    statements = await _map(BCP)
    entities = _by_type(statements, "entity")
    people = _by_type(statements, "person")
    rels = _by_type(statements, "relationship")
    company = entities[0]
    assert company["recordDetails"]["name"] == "BANCO DE CREDITO DEL PERU"
    assert company["recordDetails"]["identifiers"] == [
        {"id": BCP, "scheme": "PE-RUC", "schemeName": pe.PE_RUC_SCHEME_NAME}
    ]
    assert company["recordDetails"]["jurisdiction"]["code"] == "PE"
    assert company["statementDate"] == "2026-11-02"
    # The representative who also sits on the board is one person.
    assert sorted(p["recordDetails"]["names"][0]["fullName"] for p in people) == [
        "ALIÑO PEREZ NURIA", PERSON, "PEREZ GOMEZ ANA",
    ]
    by_name = {e["recordDetails"]["name"]: e for e in entities[1:]}
    assert by_name["GRUPO CREDITO S.A."]["recordDetails"]["identifiers"][0]["id"] == GRUPO
    assert by_name["Henkel AG & Co. KGaA"]["recordDetails"].get("identifiers", []) == []
    assert by_name["SUCESIÓN DE JORGE AVENDAÑO VALDEZ"]["recordDetails"]["entityType"]["type"] == "arrangement"
    types = sorted(r["recordDetails"]["interests"][0]["type"] for r in rels)
    assert types == ["boardMember", "boardMember", "seniorManagingOfficial"] + ["shareholding"] * 5
    for rel in rels:
        for interest in rel["recordDetails"]["interests"]:
            assert "share" not in interest and "startDate" not in interest
            assert interest["beneficialOwnershipOrControl"] is False
    unspecified = [r["recordDetails"]["interestedParty"] for r in rels
                   if isinstance(r["recordDetails"]["interestedParty"], dict)]
    assert unspecified == [
        {"reason": "informationUnknownToPublisher", "description": "ACCIONISTAS MINORITARIOS"}
    ]
    for statement in statements:
        assert any(a["description"] == PE_SELF_DECLARATION_NOTE for a in statement.get("annotations", []))
    text = json.dumps(statements, ensure_ascii=False)
    for number in (DNI, "08765432", PERSONAL):
        assert number not in text


async def test_an_eirl_titular_and_an_association_member(index):
    eirl = _by_type(await _map(EIRL, "SOLIS E.I.R.L."), "relationship")[0]
    interest = eirl["recordDetails"]["interests"][0]
    assert interest["type"] == "shareholding" and "whole enterprise" in interest["details"]
    assert "2021-04-01" in interest["details"]
    assoc = _by_type(await _map(ASSOC, "ASOCIACION X"), "relationship")[0]
    assert assoc["recordDetails"]["interests"][0]["type"] == "otherInfluenceOrControl"


async def test_the_finding(index):
    sentence = finding_oece_rnp(await _fetch(BCP))
    assert sentence.startswith("Declared to OECE's supplier register: 5 partners or shareholders")
    assert "1 legal representative" in sentence and len(sentence) <= MAX_FINDING_CHARS
    assert "owner" not in sentence.lower() and "%" not in sentence
    assert finding_oece_rnp(await _fetch(GRUPO)).startswith(
        "Declared to OECE's supplier register: no partner or shareholder"
    )
    assert finding_oece_rnp(await _fetch(ABSENT, "x")).startswith("Not among the suppliers")


# ---------------------------------------------------------------------------
# Lookup wiring
# ---------------------------------------------------------------------------


def test_gleif_writes_a_valid_ruc_as_pe_ruc_and_anything_else_as_the_ra():
    for ra in sorted(pe.PE_RA_CODES):
        assert gleif_registration_scheme(ra, "PE", None, BCP)[0] == "PE-RUC"
    assert gleif_registration_scheme("RA000482", "PE", None, "11008800")[0] == "RA000482"
    assert gleif_registration_scheme("RA000979", "PE", None, "OE1234")[0] == "RA000979"
    hop = hop_for("PE-RUC")
    assert hop is not None and hop.source_id == "oece_rnp" and hop.derived_key == "pe_ruc"


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="oece_rnp")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_for_a_ruc_with_an_index(index):
    ctx = _LookupCtx(lei="549300EQYQ8SCQZ4BY14", jurisdiction="PE", registered_as=BCP)
    _build_derived(ctx, SUNAT_RA)
    assert ctx.derived.get("pe_ruc") == BCP
    assert _dispatched(ctx) == {"oece_rnp"}
    partida = _LookupCtx(lei="549300EQYQ8SCQZ4BY14", jurisdiction="PE", registered_as="11008800")
    _build_derived(partida, "RA000482")
    assert "pe_ruc" not in partida.derived


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="549300EQYQ8SCQZ4BY14", jurisdiction="PE", registered_as=BCP)
    _build_derived(ctx, SUNAT_RA)
    assert _dispatched(ctx) == set()


async def test_the_hit_builder(index):
    ctx = _LookupCtx(lei="549300EQYQ8SCQZ4BY14", legal_name="BANCO DE CREDITO DEL PERU",
                     jurisdiction="PE", registered_as=BCP)
    _build_derived(ctx, SUNAT_RA)
    hit = _build_result_hit("oece_rnp", await _fetch(BCP), ctx)
    assert hit is not None and hit.identifiers == {"pe_ruc": BCP}
    ghost = _build_result_hit("oece_rnp", await _fetch(ABSENT, "Ghost"), ctx)
    assert ghost is not None and ghost.identifiers == {}
