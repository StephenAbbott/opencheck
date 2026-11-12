"""Compras.gov.br (Brazilian public procurement awards).

The fixture releases are shaped from the OCP Data Registry's compiled
releases of publication 157 (retrieved 27 Oct 2026): the buyer in
``parties`` with its CNPJ under ``identifier``, its UASG under
``additionalIdentifiers`` ("0" when none) and its sphere under the
``BRA-ESFERA`` classification; suppliers on the award as ``BR-CNPJ-<n>``
whether the number is a CNPJ or a CPF; award dates as midnight UTC. They are
run through the real builder, so the index the adapter reads is one
``build_index`` actually wrote.
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
from pathlib import Path

import pytest

from opencheck.bods import map_compras_gov_br
from opencheck.config import get_settings
from opencheck.findings import finding_compras_gov_br
from opencheck.routers.hit_builders import _LookupCtx
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch
from opencheck.sources import REGISTRY, compras_gov_br
from opencheck.sources.compras_gov_br import (
    _W1,
    _W2,
    _check_digit,
    build_index,
    candidate_cnpj,
    clean_text,
    cnpj_valid,
    format_cnpj,
    format_root,
    iter_releases,
    legal_basis,
)


def _cnpj(root: str, branch: str = "0001") -> str:
    body = root + branch
    body += _check_digit(body, _W1)
    return body + _check_digit(body, _W2)


BB = "00000000000191"  # Banco do Brasil, head office
PETRO = "33000167000101"  # Petrobras, head office
PETRO_BRANCH = _cnpj("33000167", "0042")
OTHER = _cnpj("12345678")
MUNICIPIO = "02056729000105"  # Município de Rio Verde (a buyer)
UNIAO = "00394445000101"
CPF = "12345678909"  # a valid-shaped CPF: a person, never read


# ---------------------------------------------------------------------------
# The CNPJ
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cnpj", [BB, PETRO, PETRO_BRANCH, MUNICIPIO])
def test_real_cnpjs_pass_the_check(cnpj: str) -> None:
    assert cnpj_valid(cnpj)


@pytest.mark.parametrize("cnpj", ["00000000000192", "3300016700010", "", CPF, "abc"])
def test_the_check_rejects(cnpj: str) -> None:
    assert not cnpj_valid(cnpj)


def test_candidate_cnpj_by_registration_authority() -> None:
    assert candidate_cnpj("RA000681", "00.000.000/0001-91") == BB
    # Decision B: a Junta Comercial, the CVM or the Central Bank filing a CNPJ.
    assert candidate_cnpj("RA000047", "33.000.167/0001-01") == PETRO
    assert candidate_cnpj("RA000822", PETRO) == PETRO
    assert candidate_cnpj("RA000823", BB) == BB
    # A number that fails the check, a CPF, or a non-Brazilian authority.
    assert candidate_cnpj("RA000681", "00000000000192") is None
    assert candidate_cnpj("RA000681", CPF) is None
    assert candidate_cnpj("RA000585", "00102498") is None


def test_formatting() -> None:
    assert format_cnpj(BB) == "00.000.000/0001-91"
    assert format_root("33000167") == "33.000.167"


def test_clean_text_removes_cpf_shaped_numbers() -> None:
    assert clean_text("Serviços  de  JOAO 123.456.789-09  MEI", 140) == (
        "Serviços de JOAO [number removed] MEI"
    )
    assert clean_text("x" * 200, 10) == "x" * 9 + "…"


def test_legal_basis() -> None:
    assert legal_basis(
        "amparoLegalCodigo:1; amparoLegalNome: Lei 14.133/2021, Art. 75, II; "
        "amparoLegalDescricao: dispensa"
    ) == "Lei 14.133/2021, Art. 75, II"
    assert legal_basis("") == ""


# ---------------------------------------------------------------------------
# Index fixture
# ---------------------------------------------------------------------------


def _buyer(name: str, cnpj: str, uasg: str, sphere: str) -> dict:
    return {
        "id": f"BR-CNPJ-{cnpj}",
        "name": name,
        "roles": ["buyer", "procuringEntity"],
        # An address and contact point the index must never keep.
        "address": {"streetAddress": "Rua das Flores 1", "locality": "RIO VERDE"},
        "contactPoint": {"email": "compras@example.gov.br"},
        "identifier": {"id": cnpj, "scheme": "BR-CNPJ"},
        "additionalIdentifiers": [{"id": uasg}],
        "details": {"classifications": [{"id": f"BRA-ESFERA-{sphere}", "scheme": "BRA-ESFERA"}]},
    }


def _release(
    process: str,
    awards: list[dict],
    *,
    buyer: tuple[str, str, str, str] = ("MUNICIPIO DE RIO VERDE", MUNICIPIO, "0", "M"),
    basis: str = "amparoLegalCodigo:1; amparoLegalNome: Lei 14.133/2021, Art. 28, I",
) -> dict:
    party = _buyer(*buyer)
    return {
        "ocid": f"ocds-x-{process}",
        "parties": [party],
        "buyer": {"id": party["id"], "name": party["name"]},
        "tender": {
            "id": process,
            "description": f"Aquisição {process}",
            "procurementMethodRationale": basis,
        },
        "awards": awards,
    }


def _award(
    aid: str,
    suppliers: list[tuple[str, str]],
    *,
    date: str = "2026-09-01T00:00:00Z",
    value: float = 1000.0,
    title: str = "Edital",
    description: str | None = None,
) -> dict:
    award = {
        "id": aid,
        "title": title,
        "date": date,
        "value": {"amount": value, "currency": "BRL"},
        "suppliers": [{"id": f"BR-CNPJ-{n}", "name": name} for name, n in suppliers],
    }
    if description is not None:
        award["description"] = description
    return award


RELEASES = [
    _release("P1", [
        _award("1", [("PETROLEO BRASILEIRO S A PETROBRAS", PETRO)], date="2021-08-16T00:00:00Z",
               value=1249027.65),
        # A branch of the same company: the same root, one supplier.
        _award("2", [("PETROBRAS FILIAL", PETRO_BRANCH)], date="2026-09-29T00:00:00Z",
               value=0, title="Ato que autoriza a Contratação Direta",
               description="Serviço prestado por JOAO 123.456.789-09"),
    ]),
    _release("P2", [
        # A joint award with another company: counted for both.
        _award("1", [("PETROBRAS", PETRO), ("OUTRA LTDA", OTHER)], date="2025-01-10T00:00:00Z",
               value=500.5, title="Aviso de Contratação Direta"),
    ], buyer=("UNIAO FEDERAL", UNIAO, "160030", "F"),
       basis="amparoLegalNome: Lei 14.133/2021, Art. 75, II"),
    # A CPF supplier (an MEI's owner, a person): counted, never stored.
    _release("P3", [_award("1", [("MARIA DA SILVA 12345678909", CPF)])]),
]


@pytest.fixture()
def built(tmp_path: Path):
    src = tmp_path / "full.jsonl.gz"
    with gzip.open(src, "wt", encoding="utf-8") as fh:
        for r in RELEASES:
            fh.write(json.dumps(r) + "\n")
    out = tmp_path / "data" / "compras_gov_br.sqlite"
    meta = build_index(
        iter_releases(src), out,
        source_meta={"source_last_modified": "Tue, 27 Oct 2026 03:12:09 GMT"},
    )
    os.environ["COMPRAS_GOV_BR_DB_FILE"] = str(out)
    get_settings.cache_clear()
    compras_gov_br.reset_connection()
    yield out, meta
    os.environ.pop("COMPRAS_GOV_BR_DB_FILE", None)
    get_settings.cache_clear()
    compras_gov_br.reset_connection()


@pytest.fixture()
def no_index(tmp_path: Path):
    os.environ.pop("COMPRAS_GOV_BR_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    compras_gov_br.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    compras_gov_br.reset_connection()


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_builder_counts(built) -> None:
    path, meta = built
    assert meta["releases"] == "3"
    assert meta["awards"] == "4"
    assert meta["supplier_entries_cpf"] == "1"
    assert meta["awards_without_cnpj"] == "1"
    assert meta["awards_shared"] == "1"
    assert meta["awards_zero_value"] == "1"
    assert meta["data_to"] == "2026-09-29"
    conn = sqlite3.connect(path)
    assert {r[0] for r in conn.execute("SELECT root FROM supplier")} == {33000167, 12345678}


def test_builder_stores_no_name_cpf_address_or_contact(built) -> None:
    path, _ = built
    dump = "\n".join(sqlite3.connect(path).iterdump())
    for text in ("PETROLEO BRASILEIRO", "OUTRA LTDA", "MARIA DA SILVA", CPF,
                 "123.456.789-09", "Rua das Flores", "compras@example.gov.br"):
        assert text not in dump


def test_a_branch_is_the_same_supplier(built) -> None:
    path, _ = built
    row = sqlite3.connect(path).execute(
        "SELECT awards, value_brl, buyers, zero_value, direct, establishments, first_date, last_date"
        " FROM supplier WHERE root = 33000167"
    ).fetchone()
    assert row == (3, 1249028 + 0 + 501, 2, 1, 2, 2, "2021-08-16", "2026-09-29")


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_fetch_by_identifiers(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch_by_identifiers(
        "X", "33.000.167/0001-01", "RA000681", legal_name="PETROLEO BRASILEIRO S.A. - PETROBRAS"
    )
    assert bundle["root"] == "33000167" and bundle["root_display"] == "33.000.167"
    assert bundle["supplier"]["awards"] == 3
    awards = bundle["awards"]
    assert [a["key"] for a in awards] == ["P1/2", "P2/1", "P1/1"]
    latest = awards[0]
    assert latest["procedure"] == "Direct contracting (authorising act)"
    assert latest["establishment"] == format_cnpj(PETRO_BRANCH)
    assert latest["object"] == "Serviço prestado por JOAO [number removed]"
    assert latest["value_brl"] == 0
    joint = awards[1]
    assert joint["buyer"] == "UNIAO FEDERAL" and joint["buyer_sphere"] == "Federal"
    assert joint["buyer_uasg"] == "160030"
    assert joint["legal_basis"] == "Lei 14.133/2021, Art. 75, II"
    assert joint["other_suppliers"] == 1
    assert awards[2]["buyer_uasg"] == ""  # "0" is no UASG
    assert awards[2]["buyer_sphere"] == "Municipal"
    assert {e["cnpj"]: e["head_office"] for e in bundle["establishments"]} == {
        format_cnpj(PETRO): True, format_cnpj(PETRO_BRANCH): False,
    }
    assert bundle["link"] == "https://www.gov.br/compras/pt-br"


async def test_a_junta_filing_is_read(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch_by_identifiers("X", PETRO, "RA000047")
    assert bundle["supplier"]["awards"] == 3


async def test_fetch_a_company_with_no_awards(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(BB)
    assert bundle["not_found"] is True and bundle["is_stub"] is False


async def test_fetch_without_an_index_is_a_stub(no_index) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(PETRO)
    assert bundle["is_stub"] is True


async def test_search_is_empty(built) -> None:
    from opencheck.sources.base import SearchKind

    assert await REGISTRY["compras_gov_br"].search("Petrobras", SearchKind.ENTITY) == []


def test_not_a_register_adapter() -> None:
    adapter = REGISTRY["compras_gov_br"]
    assert adapter.lookup_derivers == ()
    assert adapter.info.is_national_register is False
    assert adapter.info.license == "CC-BY-4.0"


# ---------------------------------------------------------------------------
# Finding, hit and BODS
# ---------------------------------------------------------------------------


async def test_finding(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(PETRO)
    assert finding_compras_gov_br(bundle) == (
        "Brazilian public procurement awards 2021–2026: 3 awards, published values "
        "totalling BRL 1,249,529, from 2 public bodies."
    )


async def test_result_hit_asserts_no_identifier(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(PETRO, legal_name="PETROBRAS")
    hit = _build_result_hit("compras_gov_br", bundle, _LookupCtx(lei="X", legal_name="Petrobras"))
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.name == "PETROBRAS"
    assert hit.summary == "3 Brazilian public procurement awards · 2021–2026 · CNPJ root 33.000.167"


async def test_no_card_for_a_company_with_no_awards(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(BB)
    assert _build_result_hit("compras_gov_br", bundle, _LookupCtx(lei="X")) is None


async def test_mapper_emits_one_entity_with_the_cnpj_it_names(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(PETRO, legal_name="PETROBRAS")
    statements = list(map_compras_gov_br(bundle))
    assert len(statements) == 1
    rd = statements[0]["recordDetails"]
    assert rd["name"] == "PETROBRAS"
    assert rd["identifiers"][0]["id"] == PETRO and rd["identifiers"][0]["scheme"] == "BR-CNPJ"
    assert "relationship" not in statements[0]["recordType"]
    assert "BRL 1,249,529" in rd["entityType"]["details"]
    assert "not money paid" in rd["entityType"]["details"]
    assert statements[0]["statementDate"] == "2026-09-29"


async def test_mapper_asserts_no_cnpj_the_file_never_named(built) -> None:
    """GLEIF's CNPJ is another establishment than the one that won."""
    head_office_less = _cnpj("12345678", "0007")
    bundle = await REGISTRY["compras_gov_br"].fetch(head_office_less, legal_name="OUTRA LTDA")
    rd = next(iter(map_compras_gov_br(bundle)))["recordDetails"]
    assert not rd.get("identifiers")
    assert "CNPJ root 12.345.678" in rd["entityType"]["details"]


async def test_mapper_draws_nothing_without_a_name(built) -> None:
    bundle = await REGISTRY["compras_gov_br"].fetch(PETRO)
    assert list(map_compras_gov_br(bundle)) == []


async def test_statements_pass_libcovebods(built, tmp_path: Path) -> None:
    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    bundle = await REGISTRY["compras_gov_br"].fetch(PETRO, legal_name="PETROBRAS")
    path = tmp_path / "compras.json"
    path.write_text(json.dumps(list(map_compras_gov_br(bundle))), encoding="utf-8")
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]


# ---------------------------------------------------------------------------
# Lookup wiring
# ---------------------------------------------------------------------------


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="compras_gov_br")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


@pytest.mark.parametrize("ra", ["RA000681", "RA000047", "RA000822"])
def test_dispatched_for_a_cnpj(built, ra) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="BR", registered_as="33.000.167/0001-01")
    _build_derived(ctx, ra)
    assert _dispatched(ctx) == {"compras_gov_br"}


@pytest.mark.parametrize(
    ("jurisdiction", "ra", "registered_as"),
    [
        ("EE", "RA000181", "10421629"),
        ("BR", "RA000681", "00000000000192"),  # fails the check
        ("BR", "RA000681", CPF),  # a sole proprietor's CPF
    ],
)
def test_not_announced_for_an_anchor_it_cannot_answer(built, jurisdiction, ra, registered_as) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction=jurisdiction, registered_as=registered_as)
    _build_derived(ctx, ra)
    assert _dispatched(ctx) == set()


def test_not_announced_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="BR", registered_as=PETRO)
    _build_derived(ctx, "RA000681")
    assert _dispatched(ctx) == set()


def test_a_register_anchored_lookup_asks_by_the_cnpj(built) -> None:
    from opencheck.routers.lookup import _register_procurement

    asked = _register_procurement("BR-CNPJ", PETRO, "PETROBRAS")
    for _, coro in asked:
        coro.close()
    assert [sid for sid, _ in asked] == ["compras_gov_br"]


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("COMPRAS_GOV_BR_DB_URL", "")
    get_settings.cache_clear()
    assert compras_gov_br.warm_index()["compras_gov_br"].startswith("absent")


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(entity_pages, "download_db", boom)
    assert compras_gov_br.warm_index()["compras_gov_br"].startswith("failed")
