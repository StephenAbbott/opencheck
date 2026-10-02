"""ChileCompra (Mercado Público procurement) — Phase 280.

The fixtures are shaped from the real monthly files (semicolon-separated,
Latin-1, decimal commas, one purchase-order row per item, one tender row per
bid line, quoted multi-line descriptions) and run through the real builder,
so the index the adapter reads here is one ``build_index`` actually wrote.
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from opencheck import provenance
from opencheck.bods import map_chilecompra
from opencheck.config import get_settings
from opencheck.findings import finding_chilecompra
from opencheck.routers.hit_builders import _LookupCtx
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch
from opencheck.sources import REGISTRY, chilecompra
from opencheck.sources.chilecompra import (
    CL_RA_CODES,
    MonthInput,
    build_index,
    format_rut,
    latest_complete_month,
    month_urls,
    normalise_rut,
    parse_rut,
    rut_check_digit,
    window_months,
)

SIEMENS = "76.481.921-7"  # company supplier: orders and tenders
AGUAS = "61.808.000-5"  # company supplier: orders only
SOLE_TRADER = "15.775.663-K"  # natural person — must never be stored
FOREIGN_AGENCY = "59.303.230-2"  # a foreign company's Chilean agency


# ---------------------------------------------------------------------------
# RUT grammar
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("76.242.192-5", "76242192-5"),
        ("76242192-5", "76242192-5"),
        ("762421925", "76242192-5"),  # the OCDS feed's form
        ("70.017.820-k", "70017820-K"),
        (" 61.808.000-5 ", "61808000-5"),
        (FOREIGN_AGENCY, "59303230-2"),
    ],
)
def test_normalise_rut(raw: str, expected: str) -> None:
    assert normalise_rut(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "76.242.192-4",  # wrong check digit
        "fojas 59592 número 30591",  # a Registro de Comercio inscription (RA000090)
        "10893-6",  # a CMF register number (RA000846)
        "15.775.663-K",  # natural person
        "",
        "abc",
    ],
)
def test_normalise_rut_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalise_rut(raw)


def test_parse_rut_accepts_natural_persons_for_the_builder_to_drop() -> None:
    # parse_rut is the grammar; the natural-person rule is a policy on top.
    assert parse_rut(SOLE_TRADER) == (15775663, "K")


def test_check_digit_examples() -> None:
    assert rut_check_digit(76242192) == "5"
    assert rut_check_digit(70017820) == "K"
    assert rut_check_digit(61808000) == "5"


@pytest.mark.parametrize(
    "raw", ["76.242.192-5", "762421925", "70017820-k", "76.242.192-4", "fojas 1", "10893-6"]
)
def test_the_mapper_and_the_adapter_agree_on_what_a_rut_is(raw: str) -> None:
    # Two copies of the grammar (bods must not import a source at module
    # level); this keeps them from drifting apart.
    from opencheck.bods.mapper import chilean_rut

    parsed = parse_rut(raw)
    assert (chilean_rut(raw) is not None) == (parsed is not None)
    if parsed:
        assert chilean_rut(raw) == format_rut(f"{parsed[0]}-{parsed[1]}")


def test_format_rut() -> None:
    assert format_rut("76242192-5") == "76.242.192-5"
    assert format_rut("70017820-K") == "70.017.820-K"


def test_ra_codes_are_chilean_and_do_not_claim_the_global_other_code() -> None:
    assert {"RA000787", "RA000090", "RA000091", "RA000785", "RA000846"} == CL_RA_CODES
    assert "RA888888" not in CL_RA_CODES
    assert "RA999999" not in CL_RA_CODES


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------


def test_window_months_wraps_the_year() -> None:
    assert window_months("2026-02", 4) == ["2025-11", "2025-12", "2026-01", "2026-02"]
    assert len(window_months("2026-09", 12)) == 12


def test_month_urls_drop_the_leading_zero() -> None:
    tenders, orders = month_urls("2026-08")
    assert tenders.endswith("/lic-da/2026-8.zip")
    assert orders.endswith("/oc-da/2026-8.zip")


def test_latest_complete_month_skips_the_current_month() -> None:
    # Files for the current month exist (they are month-to-date) and must
    # not end the window.
    asked: list[str] = []

    def exists(url: str) -> bool:
        asked.append(url)
        return True

    today = datetime(2026, 10, 2, tzinfo=UTC)
    assert latest_complete_month(today, exists=exists) == "2026-09"
    assert not any("2026-10" in u for u in asked)


def test_latest_complete_month_steps_back_over_a_missing_file() -> None:
    def exists(url: str) -> bool:
        return "2026-9.zip" not in url

    today = datetime(2026, 10, 2, tzinfo=UTC)
    assert latest_complete_month(today, exists=exists) == "2026-08"


def test_latest_complete_month_crosses_january() -> None:
    today = datetime(2027, 1, 3, tzinfo=UTC)
    assert latest_complete_month(today, exists=lambda u: True) == "2026-12"


# ---------------------------------------------------------------------------
# Index fixture — synthetic monthly zips through the real builder
# ---------------------------------------------------------------------------

_ORDER_HEADER = [
    "ID", "Codigo", "Link", "Nombre", "Descripcion/Obervaciones", "codigoEstado",
    "Estado", "FechaEnvio", "MontoTotalOC", "MontoTotalOC_PesosChilenos",
    "CodigoOrganismoPublico", "OrganismoPublico", "RutSucursal",
    "NombreProveedor", "IDItem",
]
_TENDER_HEADER = [
    "Codigo", "Link", "CodigoExterno", "Nombre", "Descripcion", "CodigoOrganismo",
    "NombreOrganismo", "FechaPublicacion", "FechaAdjudicacion", "RutProveedor",
    "NombreProveedor", "RazonSocialProveedor", "Oferta seleccionada",
]


def _zip(path: Path, name: str, header: list[str], rows: list[list[str]]) -> Path:
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    writer.writerow(header)
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(name, buf.getvalue().encode("latin-1"))
    return path


def _order(code: str, rut: str, value: str, buyer: str, buyer_name: str, day: str,
           *, state: str = "6", item: str = "1", name: str = "SIEMENS HEALTHCARE EQUIPOS MEDICOS SPA") -> list[str]:
    return [
        "1", code, f"http://www.mercadopublico.cl/x?codigoOC={code}", "Compra",
        "Línea uno\r\r\nLínea dos", state, "Aceptada", day, "1", value, buyer,
        buyer_name, rut, name, item,
    ]


def _tender(code: str, rut: str, selected: bool, buyer: str, buyer_name: str,
            published: str, awarded: str, name: str = "SIEMENS HEALTHCARE EQUIPOS MEDICOS SPA") -> list[str]:
    return [
        "1", f"http://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion={code}",
        code, "Equipos", "desc", buyer, buyer_name, published, awarded, rut, name, name,
        "Seleccionada" if selected else "No Seleccionada",
    ]


@pytest.fixture()
def built(tmp_path: Path):
    """A two-month index built by the real builder, pointed at by the setting."""
    aug_orders = _zip(tmp_path / "oc-2026-08.zip", "2026-8.csv", _ORDER_HEADER, [
        # One order, two item rows: counted once, value once.
        _order("1-1-SE26", SIEMENS, "1000000,5", "7324", "HOSPITAL GUILLERMO GRANT", "2026-08-10"),
        _order("1-1-SE26", SIEMENS, "1000000,5", "7324", "HOSPITAL GUILLERMO GRANT", "2026-08-10", item="2"),
        _order("1-2-SE26", SIEMENS, "250000", "7203", "SERVICIO DE SALUD SUR ORIENTE", "2026-08-11"),
        # Cancelled: not counted.
        _order("1-3-SE26", SIEMENS, "999999999", "7203", "SERVICIO DE SALUD SUR ORIENTE", "2026-08-12", state="9"),
        _order("2-1-AG26", AGUAS, "38999156", "6935", "PARQUE METROPOLITANO", "2026-08-01", name="AGUAS ANDINAS S A"),
        # A sole trader: dropped at build time.
        _order("3-1-AG26", SOLE_TRADER, "5000", "6935", "PARQUE METROPOLITANO", "2026-08-02", name="JUANA PEREZ"),
        # An invalid RUT: counted as invalid, not stored.
        _order("4-1-AG26", "76.242.192-4", "5000", "6935", "PARQUE METROPOLITANO", "2026-08-02", name="X"),
    ])
    aug_tenders = _zip(tmp_path / "lic-2026-08.zip", "lic_2026-8.csv", _TENDER_HEADER, [
        # Won, with two bid lines (one selected): bid once, won once.
        _tender("1641-221-LE26", SIEMENS, True, "7324", "HOSPITAL GUILLERMO GRANT", "2026-08-01", "2026-08-20"),
        _tender("1641-221-LE26", SIEMENS, False, "7324", "HOSPITAL GUILLERMO GRANT", "2026-08-01", "2026-08-20"),
        # Bid and lost.
        _tender("1057-33-LP26", SIEMENS, False, "7203", "SERVICIO DE SALUD SUR ORIENTE", "2026-08-03", ""),
        _tender("9-9-L126", SOLE_TRADER, True, "6935", "PARQUE METROPOLITANO", "2026-08-03", "2026-08-09", name="JUANA PEREZ"),
    ])
    sep_orders = _zip(tmp_path / "oc-2026-09.zip", "2026-9.csv", _ORDER_HEADER, [
        _order("1-9-SE26", SIEMENS, "3981499477", "7203", "SERVICIO DE SALUD SUR ORIENTE", "2026-09-05"),
    ])
    sep_tenders = _zip(tmp_path / "lic-2026-09.zip", "lic_2026-9.csv", _TENDER_HEADER, [
        # A tender seen again in a later month's file: still one bid.
        _tender("1057-33-LP26", SIEMENS, False, "7203", "SERVICIO DE SALUD SUR ORIENTE", "2026-08-03", ""),
        _tender("1700-1-LE26", SIEMENS, True, "7203", "SERVICIO DE SALUD SUR ORIENTE", "2026-09-01", "2026-09-29"),
    ])
    out = tmp_path / "data" / "chilecompra.sqlite"
    meta = build_index(
        [
            MonthInput("2026-08", aug_tenders, aug_orders, {"u1": {"bytes": 1}}),
            MonthInput("2026-09", sep_tenders, sep_orders, {"u2": {"bytes": 2}}),
        ],
        out,
    )
    os.environ["CHILECOMPRA_DB_FILE"] = str(out)
    get_settings.cache_clear()
    chilecompra.reset_connection()
    yield out, meta
    os.environ.pop("CHILECOMPRA_DB_FILE", None)
    get_settings.cache_clear()
    chilecompra.reset_connection()


@pytest.fixture()
def no_index(tmp_path: Path):
    """No index anywhere — neither named nor at the data-root default."""
    os.environ.pop("CHILECOMPRA_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    chilecompra.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    chilecompra.reset_connection()


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_builder_never_stores_a_natural_person(built) -> None:
    path, meta = built
    conn = sqlite3.connect(path)
    ruts = {r[0] for r in conn.execute("SELECT rut FROM supplier")}
    assert ruts == {76481921, 61808000}
    assert 15775663 not in ruts
    assert meta["natural_persons_dropped"] == "1"
    assert meta["rows_natural_person"] == "2"
    assert meta["rows_invalid_rut"] == "1"


def test_builder_counts_orders_once_and_skips_cancelled(built) -> None:
    path, _ = built
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM supplier WHERE rut = 76481921").fetchone()
    assert row["orders"] == 3  # 1-1 (two items), 1-2, 1-9; 1-3 cancelled
    assert row["order_value_clp"] == 1_000_001 + 250_000 + 3_981_499_477
    assert row["tenders_bid"] == 3  # 1641-221, 1057-33 (seen twice), 1700-1
    assert row["tenders_won"] == 2
    assert row["buyers"] == 2
    assert row["first_date"] == "2026-08-01"
    assert row["last_date"] == "2026-09-29"


def test_builder_meta_records_the_window(built) -> None:
    _, meta = built
    assert meta["data_from"] == "2026-08"
    assert meta["data_to"] == "2026-09"
    assert meta["suppliers"] == "2"
    assert meta["schema_version"] == chilecompra.INDEX_SCHEMA_VERSION


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_fetch_reads_a_supplier(built) -> None:
    adapter = REGISTRY["chilecompra"]
    with provenance.recording() as recorder:
        bundle = await adapter.fetch(SIEMENS, legal_name="Siemens Healthcare")
        resolved = recorder.resolve()
    assert bundle["is_stub"] is False and bundle["not_found"] is False
    assert bundle["rut"] == "76481921-7"
    assert bundle["rut_display"] == "76.481.921-7"
    assert bundle["window"] == "Aug 2026 – Sep 2026"
    assert bundle["supplier"]["name"] == "SIEMENS HEALTHCARE EQUIPOS MEDICOS SPA"
    # Buyers ranked by order value.
    assert [b["code"] for b in bundle["buyers"]] == ["7203", "7324"]
    assert bundle["buyers"][0]["name"] == "SERVICIO DE SALUD SUR ORIENTE"
    largest = bundle["largest_orders"]
    assert largest[0]["code"] == "1-9-SE26"
    assert largest[0]["url"].endswith("DetailsPurchaseOrder.aspx?codigoOC=1-9-SE26")
    awards = bundle["recent_awards"]
    assert [a["code"] for a in awards] == ["1700-1-LE26", "1641-221-LE26"]
    assert awards[0]["url"] == (
        "https://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion=1700-1-LE26"
    )
    # Snapshot, dated from the last month the index covers — not "stub", the
    # ariregister/ONRC failure mode where real rows render as placeholder data.
    assert resolved.liveness == "snapshot"
    assert resolved.retrieved_at == datetime(2026, 9, 1, tzinfo=UTC)


async def test_fetch_a_company_that_sold_nothing(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch("76.242.192-5")
    assert bundle["not_found"] is True
    assert bundle["is_stub"] is False
    assert bundle["supplier"] is None


async def test_fetch_without_an_index_is_a_stub(no_index) -> None:
    bundle = await REGISTRY["chilecompra"].fetch(SIEMENS)
    assert bundle["is_stub"] is True


async def test_fetch_rejects_a_sole_trader_rut(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch(SOLE_TRADER)
    assert bundle["is_stub"] is True
    assert bundle["supplier"] is None


def test_covers_lei_follows_the_index(built, tmp_path: Path) -> None:
    assert REGISTRY["chilecompra"].covers_lei("549300TNI6TCPI0P8860") is True


def test_covers_lei_without_an_index(no_index) -> None:
    assert REGISTRY["chilecompra"].covers_lei("549300TNI6TCPI0P8860") is False
    assert REGISTRY["chilecompra"].info.live_available is False


async def test_search_is_empty(built) -> None:
    from opencheck.sources.base import SearchKind

    assert await REGISTRY["chilecompra"].search("Siemens", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# Mapper, finding, hit
# ---------------------------------------------------------------------------


async def test_mapper_emits_one_entity_with_the_rut(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch(SIEMENS)
    statements = list(map_chilecompra(bundle))
    assert len(statements) == 1
    entity = statements[0]
    assert entity["recordType"] == "entity"
    ids = entity["recordDetails"]["identifiers"]
    assert {"id": "76.481.921-7", "scheme": "CL-RUT"}.items() <= ids[0].items()
    details = entity["recordDetails"]["entityType"]["details"]
    assert "3 purchase orders worth CLP 3,982,749,478" in details
    assert "2 of 3 tenders bid won" in details
    assert entity["source"]["opencheckSourceId"] == "chilecompra"


async def test_mapper_yields_nothing_for_a_non_supplier(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch("76.242.192-5")
    assert list(map_chilecompra(bundle)) == []


async def test_finding(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch(SIEMENS)
    assert finding_chilecompra(bundle) == (
        "Chilean public procurement, Aug 2026 – Sep 2026: 3 purchase orders "
        "worth CLP 3,982,749,478, 2 of 3 tenders bid won, from 2 public bodies."
    )


async def test_finding_orders_only(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch(AGUAS)
    assert finding_chilecompra(bundle) == (
        "Chilean public procurement, Aug 2026 – Sep 2026: 1 purchase order "
        "worth CLP 38,999,156, from 1 public body."
    )


async def test_result_hit_asserts_the_rut(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch(SIEMENS)
    ctx = _LookupCtx(lei="549300TNI6TCPI0P8860", legal_name="Siemens")
    hit = _build_result_hit("chilecompra", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {"cl_rut": "76481921-7"}
    assert hit.summary.startswith("CL-RUT 76.481.921-7 · 3 purchase orders · 2 tenders won")


async def test_no_card_for_a_company_that_sold_nothing(built) -> None:
    bundle = await REGISTRY["chilecompra"].fetch("76.242.192-5")
    ctx = _LookupCtx(lei="X", legal_name="Nobody")
    assert _build_result_hit("chilecompra", bundle, ctx) is None


# ---------------------------------------------------------------------------
# Lookup wiring: derivation and dispatch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ra", "registered_as", "expected"),
    [
        ("RA000787", "76481921-7", "76481921-7"),
        ("RA000090", "76.646.051-8", "76646051-8"),
        ("RA000090", "fojas 59592 número 30591", None),  # an inscription, not a RUT
        ("RA000091", "3723619", None),  # a CBR number
        ("RA888888", "76481921-7", None),  # the global code is not claimed
    ],
)
def test_derivation(ra: str, registered_as: str, expected: str | None) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="CL", registered_as=registered_as)
    _build_derived(ctx, ra)
    assert ctx.derived.get("cl_rut") == expected


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="chilecompra")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_when_the_index_is_present(built) -> None:
    ctx = _LookupCtx(lei="549300TNI6TCPI0P8860", jurisdiction="CL", registered_as=SIEMENS)
    _build_derived(ctx, "RA000787")
    assert _dispatched(ctx) == {"chilecompra"}


def test_not_announced_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="549300TNI6TCPI0P8860", jurisdiction="CL", registered_as=SIEMENS)
    _build_derived(ctx, "RA000787")
    assert _dispatched(ctx) == set()


# ---------------------------------------------------------------------------
# Boot download
# ---------------------------------------------------------------------------


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("CHILECOMPRA_DB_URL", "")
    get_settings.cache_clear()
    outcome = chilecompra.warm_index()
    assert outcome["chilecompra"].startswith("absent")


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(entity_pages, "download_db", boom)
    outcome = chilecompra.warm_index()
    assert outcome["chilecompra"].startswith("failed")


# ---------------------------------------------------------------------------
# BODS validity, and the GLEIF side of the RUT
# ---------------------------------------------------------------------------


async def test_statements_pass_libcovebods(built, tmp_path: Path) -> None:
    pytest.importorskip("libcovebods")
    import json

    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    bundle = await REGISTRY["chilecompra"].fetch(SIEMENS)
    path = tmp_path / "chilecompra.json"
    path.write_text(json.dumps(list(map_chilecompra(bundle))), encoding="utf-8")
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]


@pytest.mark.parametrize(
    ("ra", "filed", "scheme", "written"),
    [
        # Siemens Healthcare's real record files the RUT under RA000090.
        ("RA000090", "76481921-7", "CL-RUT", "76.481.921-7"),
        ("RA000787", "76.338.588-4", "CL-RUT", "76.338.588-4"),
        ("RA000090", "fojas 59592 número 30591", "RA000090", "fojas 59592 número 30591"),
    ],
)
def test_gleif_statement_writes_a_rut_the_way_chilecompra_does(
    ra: str, filed: str, scheme: str, written: str
) -> None:
    from opencheck.bods.mapper import _gleif_entity_statement

    entity = _gleif_entity_statement(
        "549300TNI6TCPI0P8860",
        {
            "legalName": {"name": "SIEMENS HEALTHCARE EQUIPOS MEDICOS SPA"},
            "registeredAs": filed,
            "registeredAt": {"id": ra, "other": None},
            "jurisdiction": "CL",
        },
        "https://api.gleif.org/api/v1/lei-records/549300TNI6TCPI0P8860",
    )
    national = [i for i in entity["recordDetails"]["identifiers"] if i["scheme"] != "XI-LEI"]
    assert national[0]["scheme"] == scheme
    assert national[0]["id"] == written
