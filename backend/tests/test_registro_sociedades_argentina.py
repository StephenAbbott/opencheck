"""The Argentina wiring (11 Nov 2026) — the Registro Nacional de Sociedades
(RNS) and the Inspección General de Justicia's entity file (IGJ), read from
one index built from the two ZIPs datos.jus.gob.ar publishes.

Every index here is produced by the **real builder**
(``scripts/build_ar_sociedades_index.py``) from ZIPs in the published shape
(the RNS's one row per CUIT × activity; the IGJ's five monthly CSVs) and read
back through the **real adapter** — the JAR / ONRC / Israel rule.
"""

from __future__ import annotations

import ast
import csv
import importlib.util
import io
import os
import sqlite3
import zipfile
from pathlib import Path
from typing import Any

import pytest

from opencheck import degradation, provenance
from opencheck.bods.mapper import gleif_registration_scheme, map_registro_sociedades_argentina
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_registro_sociedades_argentina
from opencheck.identifiers import argentine_cuit, cuit_check_digit, format_cuit
from opencheck.routers.lookup import (
    _build_derived,
    _build_gleif_hit,
    _build_result_hit,
    _dispatch,
    _LookupCtx,
)
from opencheck.sources import REGISTRY
from opencheck.sources import registro_sociedades_argentina as ar
from opencheck.sources.base import SearchKind
from opencheck.sources.registro_sociedades_argentina import (
    AR_RNS_RA_CODE,
    baja_class,
    names_agree,
    normalise_ar_key,
    normalise_cuit_key,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_ar_sociedades_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_ar_sociedades_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()


def _cuit(base: str) -> str:
    return base + str(cuit_check_digit(base))


ARCA_RA = "RA000009"
TOYOTA = "33679139369"          # RNS + IGJ; a foreign entry shares the CUIT (art. 123)
GALICIA = "30500001735"         # RNS only
REMOVED = _cuit("3070000001")   # RNS + an IGJ entry cancelled
MOVED = _cuit("3070000002")     # IGJ only: moved to another jurisdiction
PERSON = _cuit("2012345678")    # an IGJ sole trader — never indexed
ABSENT = _cuit("3070000099")    # in neither file
BIOSEEDS_IGJ = 1917865          # IGJ entry with no CUIT, reached by its number
DNI = "28123456"
PERSON_NAME = "PEREZ JUAN CARLOS"


# ---------------------------------------------------------------------------
# The published shape
# ---------------------------------------------------------------------------


def _csv(header: tuple[str, ...], rows: list[dict[str, Any]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(header))
    writer.writeheader()
    for row in rows:
        writer.writerow({h: row.get(h, "") for h in header})
    return out.getvalue()


def _rns_row(cuit: str, name: str, **over: Any) -> dict[str, Any]:
    row = {
        "cuit": cuit, "razon_social": name, "tipo_societario": "SOCIEDAD ANONIMA",
        "fecha_hora_contrato_social": "1994-04-22 00:00:00",
        "fecha_hora_actualizacion": "2026-07-14 10:11:12",
        "dom_legal_provincia": "CIUDAD AUTONOMA BUENOS AIRES",
        "dom_legal_localidad": "CAPITAL FEDERAL", "dom_legal_calle": "MADERO EDUARDO AV.",
        "dom_legal_numero": "1020", "dom_legal_piso": "05", "dom_legal_cp": "1106",
        "dom_legal_estado_domicilio": "DECLARADO POR INTERNET",
        "dom_fiscal_provincia": "BUENOS AIRES", "dom_fiscal_localidad": "ZARATE",
        "dom_fiscal_calle": "RUTA 12 KM 81", "dom_fiscal_numero": "0", "dom_fiscal_cp": "2800",
        "dom_fiscal_estado_domicilio": "CONFIRMADO",
        "actividad_codigo": "291000", "actividad_descripcion": "FABRICACIÓN DE VEHÍCULOS AUTOMOTORES",
        "actividad_orden": "1", "actividad_estado": "AC",
    }
    row.update(over)
    return row


def _rns_rows() -> list[dict[str, Any]]:
    return [
        _rns_row(TOYOTA, "TOYOTA ARGENTINA S A"),
        # a second activity row for the same CUIT, secondary
        _rns_row(TOYOTA, "TOYOTA ARGENTINA S A", actividad_codigo="461000",
                 actividad_descripcion="VENTA AL POR MAYOR", actividad_orden="2"),
        # an activity no longer held (BD) is not counted
        _rns_row(TOYOTA, "TOYOTA ARGENTINA S A", actividad_codigo="999999",
                 actividad_descripcion="BAJA", actividad_orden="3", actividad_estado="BD"),
        _rns_row(GALICIA, "BANCO DE GALICIA Y BUENOS AIRES S. A.",
                 fecha_hora_contrato_social="1905-09-28 00:00:00",
                 dom_fiscal_estado_domicilio="INEXISTENTE - DESCONOCIDO",
                 actividad_codigo="641930", actividad_descripcion="SERVICIOS DE LA BANCA MAYORISTA"),
        _rns_row(REMOVED, "CERRADA S.R.L.", tipo_societario="SOCIEDAD DE RESPONSABILIDAD LIMITADA",
                 fecha_hora_contrato_social=""),
        # malformed: a bad check digit
        _rns_row("30500001730", "ROTO SA"),
    ]


def _igj_entities() -> list[dict[str, Any]]:
    def row(n: int, form: int, words: str, name: str, cuit: str = "", baja: str = "", code: str = "", detail: str = ""):
        return {
            "numero_correlativo": n, "tipo_societario": form, "descripcion_tipo_societario": words,
            "razon_social": name, "dada_de_baja": baja, "codigo_baja": code, "detalle_baja": detail,
            "cuit": cuit,
        }

    return [
        row(1589722, 50, "SOCIEDAD ANONIMA", "TOYOTA ARGENTINA", format_cuit(TOYOTA)),
        row(1590906, 80, "SOCIEDAD EXTRANJERA", "TOYOTA MOTOR CORPORATION", TOYOTA),
        row(1600001, 51, "SOCIEDAD DE RESPONSABILIDAD LIMITADA", "CERRADA SRL", REMOVED,
            "S", "00146", "CANCELACION TRAM. ORDINARIO"),
        row(1600002, 50, "SOCIEDAD ANONIMA", "MUDADA SA", MOVED,
            "S", "00190", "CANCELACION A CAMBIO DE JURISDICCION"),
        row(BIOSEEDS_IGJ, 50, "SOCIEDAD ANONIMA", "BIOSEEDS"),
        row(1600003, 210, "EMPRESARIO INDIVIDUAL", PERSON_NAME, PERSON),
    ]


def _igj_files(month: str = "202609") -> dict[str, str]:
    people = [
        {"numero_correlativo": 1589722, "apellido_nombre": PERSON_NAME, "tipo_administrador": "A",
         "descripcion_tipo_administrador": "AUTORIDAD", "tipo_documento": "1",
         "descripcion_tipo_documento": "DNI", "numero_documento": DNI, "genero_autoridad": "M"},
        {"numero_correlativo": 1589722, "apellido_nombre": "GOMEZ ANA", "tipo_administrador": "S",
         "descripcion_tipo_administrador": "SINDICO", "tipo_documento": "1",
         "descripcion_tipo_documento": "DNI", "numero_documento": "30111222", "genero_autoridad": "F"},
        {"numero_correlativo": BIOSEEDS_IGJ, "apellido_nombre": "LOPEZ LUIS", "tipo_administrador": "A",
         "descripcion_tipo_administrador": "AUTORIDAD", "tipo_documento": "1",
         "descripcion_tipo_documento": "DNI", "numero_documento": "25000111", "genero_autoridad": "M"},
    ]
    balances = [
        {"numero_correlativo": 1589722, "fecha_balance": "2024-03-31", "fecha_presentacion": "2024-07-01"},
        {"numero_correlativo": 1589722, "fecha_balance": "2025-03-31", "fecha_presentacion": "2025-07-08"},
    ]
    assemblies = [{"numero_correlativo": 1589722, "fecha_realizacion": "2025-07-29"}]
    addresses = [
        {"numero_correlativo": 1589722, "tipo_domicilio": "L", "calle": "AV. EDUARDO MADERO",
         "numero": "1020", "localidad": "CAPITAL FEDERAL"},
        {"numero_correlativo": BIOSEEDS_IGJ, "tipo_domicilio": "S", "calle": "SEDE SOCIAL", "numero": "1"},
        {"numero_correlativo": BIOSEEDS_IGJ, "tipo_domicilio": "L", "calle": "ALSINA", "numero": "123",
         "localidad": "CAPITAL FEDERAL"},
    ]
    h = builder.IGJ_HEADERS
    return {
        f"igj-entidades-{month}.csv": _csv(h["entidades"], _igj_entities()),
        f"igj-autoridades-{month}.csv": _csv(h["autoridades"], people),
        f"igj-balances-{month}.csv": _csv(h["balances"], balances),
        f"igj-asambleas-{month}.csv": _csv(h["asambleas"], assemblies),
        f"igj-domicilios-{month}.csv": _csv(h["domicilios"], addresses),
    }


def _zip(path: Path, files: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return path


def _zips(tmp_path: Path, *, rns_rows=None, igj_files=None) -> tuple[Path, Path]:
    rns = _zip(
        tmp_path / "rns.zip",
        {"registro-nacional-sociedades-20260918.csv": _csv(builder.RNS_HEADER, rns_rows or _rns_rows())},
    )
    igj = _zip(tmp_path / "igj.zip", igj_files or _igj_files())
    return rns, igj


def _build(tmp_path: Path, **kw: Any) -> tuple[Path, dict[str, Any]]:
    rns, igj = _zips(tmp_path, **kw)
    out = tmp_path / "data" / "ar_sociedades.sqlite"
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(rns, igj, out, min_rns=0, min_igj=0)
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("AR_SOCIEDADES_DB_FILE", None)
    else:
        os.environ["AR_SOCIEDADES_DB_FILE"] = str(path)
    get_settings.cache_clear()
    ar.reset_connection()


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


async def _fetch(key: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["registro_sociedades_argentina"].fetch(key, legal_name=legal_name)


async def _map(key: str, legal_name: str = "") -> list[dict[str, Any]]:
    with provenance.recording():
        bundle = await _fetch(key, legal_name)
        return list(map_registro_sociedades_argentina(bundle))


# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("30500001735", "30500001735"),
        ("30-50000173-5", "30500001735"),
        (" 33-67913936-9 ", "33679139369"),
        ("30.50000173.5", "30500001735"),
        ("30500001730", None),       # wrong check digit
        ("1917865", None),           # an IGJ correlativo
        ("", None),
    ],
)
def test_argentine_cuit_reads_every_spelling_and_checks_the_digit(raw, expected):
    assert argentine_cuit(raw) == expected


def test_the_check_digit_reads_10_as_9():
    assert cuit_check_digit("3367913936") == 9, "Toyota: 11 - 2 = 9 after the 23/33 re-prefixing"
    assert format_cuit(TOYOTA) == "33-67913936-9"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("33-67913936-9", TOYOTA),
        ("1917865", "IGJ:1917865"),
        ("001672", "IGJ:1672"),
        ("IGJ:1672", "IGJ:1672"),  # a key of ours, re-read by fetch
    ],
)
def test_the_dispatch_key(raw, expected):
    assert normalise_ar_key(raw) == expected


@pytest.mark.parametrize("raw", ["1672", "12345678", "30500001730", "Mendoza 123", "IGJ:0", "IGJ:"])
def test_anything_else_is_not_dispatched(raw):
    with pytest.raises(ValueError):
        normalise_ar_key(raw)


def test_the_register_hop_takes_a_cuit_only():
    assert normalise_cuit_key("30-50000173-5") == GALICIA
    with pytest.raises(ValueError):
        normalise_cuit_key("1917865")
    from opencheck.register_hops import hop_for

    hop = hop_for("AR-CUIT")
    assert hop is not None and hop.source_id == "registro_sociedades_argentina"


def test_gleif_writes_ar_cuit_only_when_the_check_digit_agrees():
    def scheme(ra: str, value: str) -> str:
        return gleif_registration_scheme(ra, "AR", registered_as=value)[0]

    assert scheme(ARCA_RA, "33-67913936-9") == "AR-CUIT"
    assert scheme(AR_RNS_RA_CODE, "30500001735") == "AR-CUIT"
    assert scheme(AR_RNS_RA_CODE, "1917865") == AR_RNS_RA_CODE


@pytest.mark.parametrize(
    "flag, words, expected",
    [
        (0, None, "none"),
        (1, "CANCELACION TRAM. ORDINARIO", "terminal"),
        (1, "CANCELACION A CAMBIO DE JURISDICCION", "moved"),
        (1, "TRANSFORMACION", "transformed"),
        (1, "DUPLICIDAD DE INSCRIPCION", "duplicate"),
        (1, "OFICIO", "unstated"),
        (1, None, "unstated"),
    ],
)
def test_baja_class(flag, words, expected):
    assert baja_class(flag, words) == expected


def test_names_agree_without_the_legal_form():
    assert names_agree("TOYOTA ARGENTINA", "TOYOTA ARGENTINA S A")
    assert names_agree("Bioseeds S.A", "BIOSEEDS")
    assert not names_agree("Bayer S.A.", "OTRA COSA SRL")


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_reads_both_files_and_records_both_cuts(index):
    out, stats = index
    assert stats["rns_cuits"] == 3 and stats["rns_malformed"] == 1
    assert stats["igj_entities"] == 5 and stats["igj_natural_persons_left_out"] == 1
    conn = sqlite3.connect(out)
    meta = dict(conn.execute("SELECT key, value FROM meta"))
    assert meta["rns_cut"] == "2026-09-18" == meta["cut"]
    assert meta["igj_month"] == "2026-09"
    assert meta["schema_version"] == ar.INDEX_SCHEMA_VERSION
    assert meta["built_at"].endswith("Z") and "T" in meta["built_at"]
    activities = conn.execute("SELECT activity, activities FROM rns WHERE cuit = ?", (int(TOYOTA),)).fetchone()
    assert activities == ("291000", 2), "the main activity, and current ones only"


def test_the_builder_keeps_no_person_the_igj_names(index):
    out, _ = index
    conn = sqlite3.connect(out)
    dump = "\n".join(conn.iterdump())
    for value in (DNI, PERSON_NAME, "GOMEZ ANA", "30111222", "LOPEZ LUIS", PERSON):
        assert value not in dump
    people = conn.execute("SELECT people FROM igj WHERE correlativo = 1589722").fetchone()[0]
    assert people == 2, "a count, and nothing else"


def test_the_builder_refuses_a_changed_header(tmp_path):
    rows = _rns_rows()
    rns = _zip(tmp_path / "rns.zip", {
        "registro-nacional-sociedades-20260918.csv": _csv(builder.RNS_HEADER[:-3], rows),
    })
    igj = _zip(tmp_path / "igj.zip", _igj_files())
    out = tmp_path / "out.sqlite"
    with pytest.raises(builder.RefusedInput):
        builder.build(rns, igj, out, min_rns=0, min_igj=0)
    assert not out.exists() and not out.with_name(out.name + ".building").exists()


def test_the_builder_refuses_a_short_file_and_an_incomplete_month(tmp_path):
    rns, igj = _zips(tmp_path)
    with pytest.raises(builder.RefusedInput):
        builder.build(rns, igj, tmp_path / "a.sqlite", min_igj=0)
    files = _igj_files()
    files.pop("igj-balances-202609.csv")
    igj = _zip(tmp_path / "igj2.zip", files)
    with pytest.raises(builder.RefusedInput):
        builder.build(rns, igj, tmp_path / "b.sqlite", min_rns=0, min_igj=0)


def test_the_builder_reads_the_newest_complete_month(tmp_path):
    files = {**_igj_files("202608"), **_igj_files("202609")}
    files.pop("igj-asambleas-202609.csv")
    out, stats = _build(tmp_path, igj_files=files)
    assert stats["igj_month"] == "2026-08"


def test_pick_resource_takes_the_newest_year_and_semester():
    resources = [
        {"name": "Registro Nacional de Sociedades - 2025", "url": "a.zip"},
        {"name": "Registro Nacional de Sociedades - 2026", "url": "b.zip"},
        {"name": "Asociaciones sin fines de lucro - 2026", "url": "c.zip"},
    ]
    assert builder.pick_resource(resources, builder.RNS_RESOURCE_RE)["url"] == "b.zip"
    resources = [
        {"name": "IGJ - 2026 semestre 1", "url": "x.zip"},
        {"name": "IGJ - 2026 semestre 2", "url": "y.zip"},
        {"name": "IGJ - 2025 semestre 2", "url": "z.zip"},
    ]
    assert builder.pick_resource(resources, builder.IGJ_RESOURCE_RE)["url"] == "y.zip"


def test_the_bundle_schema_refuses_person_data():
    from opencheck.sources.schemas.registro_sociedades_argentina import (
        RegistroSociedadesArgentinaBundle,
    )

    with pytest.raises(ValueError):
        RegistroSociedadesArgentinaBundle.model_validate({"igj": {"people": [{"apellido_nombre": "X"}]}})


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_a_cuit_reads_both_files_as_a_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch("33-67913936-9", "TOYOTA ARGENTINA S.A.")
    assert bundle["cuit"] == TOYOTA and bundle["name"] == "TOYOTA ARGENTINA S A"
    assert bundle["rns"]["founded"] == "1994-04-22"
    assert bundle["igj"]["correlativo"] == 1589722, "the foreign art. 123 entry is not the company"
    assert bundle["igj"]["people_named"] == 2 and bundle["igj"]["balances"] == 2
    assert bundle["igj"]["last_balance"] == "2025-03-31"
    assert bundle["status_class"] == "unknown", "an unflagged IGJ entry is never 'active'"
    assert rec.resolve().liveness == "snapshot"


async def test_no_name_gate_on_a_cuit(index):
    bundle = await _fetch(GALICIA, "Some Other Name")
    assert bundle.get("not_found") is not True and bundle["igj"] is None


async def test_an_igj_number_is_name_gated(index):
    bundle = await _fetch(str(BIOSEEDS_IGJ), "Bioseeds S.A")
    assert bundle["igj"]["correlativo"] == BIOSEEDS_IGJ and bundle["cuit"] is None
    other = await _fetch(str(BIOSEEDS_IGJ), "Bayer S.A.")
    assert other["not_found"] is True and other["not_found_reason"] == "name_mismatch"


async def test_a_cuit_in_neither_file_is_a_note_card_not_a_degradation(index):
    with degradation.recording() as degraded:
        bundle = await _fetch(ABSENT, "Fondo de Garantia")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["coverage_note"] == ar.COVERAGE_NOT_IN_INDEX
    assert list(degraded) == []
    assert list(map_registro_sociedades_argentina(bundle)) == []


async def test_a_sole_trader_is_not_in_the_index(index):
    bundle = await _fetch(PERSON)
    assert bundle["not_found"] is True


async def test_a_malformed_key_is_a_stub(index):
    assert (await _fetch("1234"))["is_stub"] is True


async def test_without_an_index_nothing_is_announced(no_index):
    adapter = REGISTRY["registro_sociedades_argentina"]
    assert adapter.covers_lei("9845004CFFA9576DC662") is False
    assert (await _fetch(TOYOTA))["is_stub"] is True
    assert await adapter.search("toyota", SearchKind.ENTITY) == []


async def test_a_wrong_schema_version_is_no_index(tmp_path):
    out, _ = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert ar.index_available() is False
    finally:
        _point_at(None)


async def test_search_by_name_and_by_cuit(index):
    adapter = REGISTRY["registro_sociedades_argentina"]
    hits = await adapter.search("toyota argentina", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [TOYOTA]
    assert hits[0].identifiers == {"ar_cuit": TOYOTA}
    hits = await adapter.search("30-50000173-5", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [GALICIA]


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_draws_one_entity_on_the_cuit(index):
    (stmt,) = await _map(TOYOTA)
    rd = stmt["recordDetails"]
    assert rd["identifiers"] == [{
        "id": "33-67913936-9", "scheme": "AR-CUIT",
        "schemeName": "CUIT — Clave Única de Identificación Tributaria (Argentina)",
    }]
    assert rd["jurisdiction"] == {"name": "Argentina", "code": "AR"}
    assert rd["foundingDate"] == "1994-04-22"
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Public limited company (sociedad anónima)"}
    assert "alternateNames" not in rd, "the IGJ's name without the form is the same name"
    assert [a["type"] for a in rd["addresses"]] == ["registered", "alternative"]
    assert stmt["source"]["type"] == ["officialRegister"]
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "2 people are named" in notes and "does not list them" in notes
    assert "291000" in notes and "2 balance sheets" in notes
    assert "is not a statement that the company is active" in notes
    assert DNI not in str(stmt) and PERSON_NAME not in str(stmt)


async def test_a_cancelled_entry_is_terminal_and_says_the_igj_words(index):
    (stmt,) = await _map(REMOVED)
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "CANCELACION TRAM. ORDINARIO" in notes
    assert "dissolutionDate" not in stmt["recordDetails"], "no date is invented"
    assert "foundingDate" not in stmt["recordDetails"]


async def test_a_move_is_not_an_ending(index):
    (stmt,) = await _map(MOVED)
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "does not mean it ceased to exist" in notes
    assert "not in the Registro Nacional de Sociedades" in notes
    bundle = await _fetch(MOVED)
    assert bundle["status_class"] == "unknown"


async def test_an_unknown_fiscal_address_is_said(index):
    (stmt,) = await _map(GALICIA)
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "INEXISTENTE - DESCONOCIDO" in notes


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_registro_sociedades_argentina({"is_stub": True, "cuit": TOYOTA})) == []
    assert list(map_registro_sociedades_argentina({"is_stub": False, "not_found": True})) == []


def test_risk_and_verdict_do_not_read_the_module():
    for module in ("risk", "verdict"):
        src = (Path(__file__).resolve().parents[1] / "opencheck" / f"{module}.py").read_text()
        names = {
            n.module for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ImportFrom) and n.module
        }
        assert not any("registro_sociedades_argentina" in name for name in names)


# ---------------------------------------------------------------------------
# The finding and the hit
# ---------------------------------------------------------------------------


async def test_the_finding_sentences(index):
    toyota = finding_registro_sociedades_argentina(await _fetch(TOYOTA))
    assert toyota == (
        "Founded 22 April 1994, public limited company (sociedad anónima), "
        "2 people named in IGJ filings over time."
    ), "the balance-sheet clause falls off the 140-character cap"
    bioseeds = finding_registro_sociedades_argentina(await _fetch(str(BIOSEEDS_IGJ), "Bioseeds S.A"))
    assert bioseeds == "On the IGJ register, public limited company (sociedad anónima), 1 person named in IGJ filings over time."
    assert finding_registro_sociedades_argentina(await _fetch(REMOVED)).startswith("Removed from the IGJ")
    assert finding_registro_sociedades_argentina(await _fetch(MOVED)).startswith("Moved from the IGJ")
    assert finding_registro_sociedades_argentina(await _fetch(ABSENT)).startswith("Not in the Registro")
    mismatch = await _fetch(str(BIOSEEDS_IGJ), "Bayer S.A.")
    assert "names another entity" in finding_registro_sociedades_argentina(mismatch)
    for key in (TOYOTA, GALICIA, REMOVED, MOVED, str(BIOSEEDS_IGJ)):
        sentence = finding_registro_sociedades_argentina(await _fetch(key, "Bioseeds"))
        assert sentence and len(sentence) <= MAX_FINDING_CHARS
        assert "active" not in sentence.lower()


async def test_the_hit_builder_asserts_the_cuit_the_register_published(index):
    ctx = _LookupCtx(lei="9845004CFFA9576DC662", legal_name="TOYOTA ARGENTINA S.A.", jurisdiction="AR",
                     registered_as="33-67913936-9")
    _build_derived(ctx, ARCA_RA)
    assert ctx.derived.get("ar_cuit") == TOYOTA
    hit = _build_result_hit("registro_sociedades_argentina", await _fetch(TOYOTA), ctx)
    assert hit is not None
    assert hit.identifiers == {"ar_cuit": TOYOTA} and hit.summary == "AR 33-67913936-9"
    removed = _build_result_hit("registro_sociedades_argentina", await _fetch(REMOVED), ctx)
    assert removed is not None and removed.summary.endswith("· removed by the IGJ")
    ghost = _build_result_hit("registro_sociedades_argentina", await _fetch(ABSENT, "Ghost"), ctx)
    assert ghost is not None and ghost.identifiers == {}


def test_the_gleif_hit_never_asserts_an_igj_key():
    ctx = _LookupCtx(lei="529900WVDFP3LKXU6C57", legal_name="Bioseeds S.A", jurisdiction="AR",
                     registered_as=str(BIOSEEDS_IGJ))
    _build_derived(ctx, AR_RNS_RA_CODE)
    assert ctx.derived.get("ar_cuit") == f"IGJ:{BIOSEEDS_IGJ}"
    hit = _build_gleif_hit(ctx, {})
    assert "ar_cuit" not in hit.identifiers


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="registro_sociedades_argentina")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index):
    ctx = _LookupCtx(lei="9845004CFFA9576DC662", jurisdiction="AR", registered_as="33-67913936-9")
    _build_derived(ctx, ARCA_RA)
    assert _dispatched(ctx) == {"registro_sociedades_argentina"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="9845004CFFA9576DC662", jurisdiction="AR", registered_as="33-67913936-9")
    _build_derived(ctx, ARCA_RA)
    assert _dispatched(ctx) == set()
