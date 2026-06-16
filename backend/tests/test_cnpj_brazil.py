"""Tests for the Brazilian CNPJ adapter (Receita Federal) and BODS mapper.

Covers the provider-agnostic normaliser (OpenCNPJ + BrasilAPI field variants),
the OpenCNPJ → BrasilAPI fallback, and the QSA → BODS person/entity +
ownership-or-control mapping.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.bods.mapper import map_cnpj_brazil
from opencheck.sources.base import SearchKind
from opencheck.sources.cnpj_brazil import (
    BR_RA_CODE,
    CnpjBrazilAdapter,
    _partner_kind,
    normalise_cnpj,
)

# OpenCNPJ-shaped payload: human-readable labels, capital as comma-decimal.
OPENCNPJ: dict[str, Any] = {
    "cnpj": "33000167000101",
    "razao_social": "PETROLEO BRASILEIRO S A PETROBRAS",
    "nome_fantasia": "PETROBRAS",
    "situacao_cadastral": "Ativa",
    "natureza_juridica": "Sociedade de Economia Mista",
    "data_inicio_atividade": "1966-09-28",
    "capital_social": "205431960490,52",
    "logradouro": "AV REPUBLICA DO CHILE",
    "numero": "65",
    "bairro": "CENTRO",
    "municipio": "RIO DE JANEIRO",
    "uf": "RJ",
    "cep": "20031912",
    "QSA": [
        {
            "nome_socio": "MARIA DA SILVA",
            "cnpj_cpf_socio": "***123456**",
            "qualificacao_socio": "Diretor",
            "identificador_socio": "Pessoa Física",
            "data_entrada_sociedade": "2021-04-15",
            "pais": {"codigo": "105", "descricao": "BRASIL"},
        },
        {
            "nome_socio": "UNIAO HOLDING LTDA",
            "cnpj_cpf_socio": "11222333000181",
            "qualificacao_socio": "Sócio",
            "identificador_socio": "Pessoa Jurídica",
            "data_entrada_sociedade": "2010-01-20",
            "pais": "",
        },
    ],
}

# BrasilAPI-shaped payload for the same entity: numeric type codes, lowercase qsa.
BRASILAPI: dict[str, Any] = {
    "cnpj": "33000167000101",
    "razao_social": "PETROLEO BRASILEIRO S A PETROBRAS",
    "nome_fantasia": "PETROBRAS",
    "descricao_situacao_cadastral": "ATIVA",
    "natureza_juridica": "Sociedade de Economia Mista",
    "data_inicio_atividade": "1966-09-28",
    "logradouro": "AV REPUBLICA DO CHILE",
    "numero": "65",
    "bairro": "CENTRO",
    "municipio": "RIO DE JANEIRO",
    "uf": "RJ",
    "cep": "20031912",
    "qsa": [
        {
            "nome_socio": "MARIA DA SILVA",
            "cnpj_cpf_do_socio": "***123456**",
            "qualificacao_socio": "Diretor",
            "identificador_de_socio": 2,
            "data_entrada_sociedade": "2021-04-15",
        },
        {
            "nome_socio": "UNIAO HOLDING LTDA",
            "cnpj_cpf_do_socio": "11222333000181",
            "qualificacao_socio": "Sócio",
            "identificador_de_socio": 1,
            "data_entrada_sociedade": "2010-01-20",
        },
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestNormaliseCnpj:
    def test_strips_punctuation(self) -> None:
        assert normalise_cnpj("33.000.167/0001-01") == "33000167000101"

    def test_passthrough_digits(self) -> None:
        assert normalise_cnpj("33000167000101") == "33000167000101"

    def test_zero_pads(self) -> None:
        assert normalise_cnpj("167000101") == "00000167000101"


class TestPartnerKind:
    def test_string_labels(self) -> None:
        assert _partner_kind("Pessoa Jurídica") == "entity"
        assert _partner_kind("Pessoa Física") == "person"
        assert _partner_kind("Pessoa Física Residente no Exterior") == "foreign"

    def test_numeric_codes(self) -> None:
        assert _partner_kind(1) == "entity"
        assert _partner_kind(2) == "person"
        assert _partner_kind(3) == "foreign"


def test_ra_code() -> None:
    assert BR_RA_CODE == "RA000681"


# ---------------------------------------------------------------------------
# Adapter normaliser + fetch
# ---------------------------------------------------------------------------


def _make_client(route):
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get = route
    return client


def _resp(status: int, payload: dict | None = None) -> MagicMock:
    m = MagicMock()
    m.status_code = status
    m.is_success = 200 <= status < 300
    m.json.return_value = payload if payload is not None else {}
    return m


def _live(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()


def _normalised_bundle() -> dict[str, Any]:
    """Run the adapter normaliser directly on the OpenCNPJ payload."""
    adapter = CnpjBrazilAdapter()
    return adapter._normalise("33000167000101", OPENCNPJ, legal_name="")


@pytest.mark.asyncio
async def test_search_returns_empty() -> None:
    assert await CnpjBrazilAdapter().search("Petrobras", SearchKind.ENTITY) == []


@pytest.mark.asyncio
async def test_fetch_opencnpj_primary(monkeypatch, tmp_path) -> None:
    _live(monkeypatch, tmp_path)
    mock_cache = MagicMock(); mock_cache.has.return_value = False
    mock_cache.get_payload.return_value = None; mock_cache.put.return_value = None

    async def route(url, **kw):
        return _resp(200, OPENCNPJ) if "opencnpj" in url else _resp(500)

    with (
        patch("opencheck.sources.cnpj_brazil.Cache", return_value=mock_cache),
        patch("opencheck.sources.cnpj_brazil.build_client", return_value=_make_client(route)),
    ):
        bundle = await CnpjBrazilAdapter().fetch("33.000.167/0001-01", legal_name="Petrobras")

    assert bundle["is_stub"] is False
    assert bundle["br_cnpj"] == "33000167000101"
    assert bundle["company"]["name"].startswith("PETROLEO")
    assert len(bundle["partners"]) == 2
    pj = next(p for p in bundle["partners"] if p["kind"] == "entity")
    assert pj["cnpj"] == "11222333000181"            # PJ partner exposes full CNPJ
    pf = next(p for p in bundle["partners"] if p["kind"] == "person")
    assert pf["cnpj"] is None                         # masked CPF is not an identifier
    # OpenCNPJ encodes pais as {codigo, descricao} — must not crash .strip()
    assert pf["country"] == "BRASIL"


@pytest.mark.asyncio
async def test_fetch_falls_back_to_brasilapi(monkeypatch, tmp_path) -> None:
    _live(monkeypatch, tmp_path)
    mock_cache = MagicMock(); mock_cache.has.return_value = False
    mock_cache.get_payload.return_value = None; mock_cache.put.return_value = None
    calls = {"opencnpj": 0, "brasilapi": 0}

    async def route(url, **kw):
        if "opencnpj" in url:
            calls["opencnpj"] += 1
            return _resp(404)            # not in OpenCNPJ snapshot
        calls["brasilapi"] += 1
        return _resp(200, BRASILAPI)

    with (
        patch("opencheck.sources.cnpj_brazil.Cache", return_value=mock_cache),
        patch("opencheck.sources.cnpj_brazil.build_client", return_value=_make_client(route)),
    ):
        bundle = await CnpjBrazilAdapter().fetch("33000167000101", legal_name="Petrobras")

    assert calls["opencnpj"] == 1 and calls["brasilapi"] == 1
    assert bundle["is_stub"] is False
    assert len(bundle["partners"]) == 2               # numeric type codes normalised
    assert {p["kind"] for p in bundle["partners"]} == {"person", "entity"}


@pytest.mark.asyncio
async def test_fetch_both_providers_fail_returns_partial(monkeypatch, tmp_path) -> None:
    _live(monkeypatch, tmp_path)
    mock_cache = MagicMock(); mock_cache.has.return_value = False
    mock_cache.get_payload.return_value = None; mock_cache.put.return_value = None

    async def route(url, **kw):
        return _resp(404)

    with (
        patch("opencheck.sources.cnpj_brazil.Cache", return_value=mock_cache),
        patch("opencheck.sources.cnpj_brazil.build_client", return_value=_make_client(route)),
    ):
        bundle = await CnpjBrazilAdapter().fetch("33000167000101", legal_name="Petrobras")

    # non-stub partial using the GLEIF name, so the source card still shows
    assert bundle["is_stub"] is False
    assert bundle["company"] == {"name": "Petrobras"}
    assert bundle["partners"] == []


@pytest.mark.asyncio
async def test_fetch_stub_when_live_disabled(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    mock_cache = MagicMock(); mock_cache.has.return_value = False
    mock_cache.get_payload.return_value = None

    with patch("opencheck.sources.cnpj_brazil.Cache", return_value=mock_cache):
        bundle = await CnpjBrazilAdapter().fetch("33000167000101", legal_name="Petrobras")
    get_settings.cache_clear()

    assert bundle["is_stub"] is True
    assert bundle["company"] is None


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


class TestMapCnpjBrazil:
    def test_stub_yields_nothing(self) -> None:
        assert list(map_cnpj_brazil({"is_stub": True, "br_cnpj": "33000167000101"})) == []

    def test_company_entity_statement(self) -> None:
        stmts = list(map_cnpj_brazil(_normalised_bundle()))
        ent = next(s for s in stmts if s["recordType"] == "entity" and
                   s["recordDetails"]["name"].startswith("PETROLEO"))
        rd = ent["recordDetails"]
        assert rd["jurisdiction"]["code"] == "BR"
        ids = {i["scheme"]: i["id"] for i in rd["identifiers"]}
        assert ids["BR-RFB"] == "33000167000101"
        assert rd.get("foundingDate") == "1966-09-28"
        assert rd["entityType"]["details"] == "Sociedade de Economia Mista"
        assert any(n.get("fullName") == "PETROBRAS" for n in rd.get("names", [])) or \
            "PETROBRAS" in [a for a in rd.get("alternateNames", [])]

    def test_pf_partner_is_person_with_role(self) -> None:
        stmts = list(map_cnpj_brazil(_normalised_bundle()))
        persons = [s for s in stmts if s["recordType"] == "person"]
        assert len(persons) == 1
        assert persons[0]["recordDetails"]["names"][0]["fullName"] == "MARIA DA SILVA"
        # the relationship for the director → seniorManagingOfficial
        rels = [s for s in stmts if s["recordType"] == "relationship"]
        director_rel = next(
            r for r in rels
            if r["recordDetails"]["interests"][0].get("details") == "Diretor"
        )
        assert director_rel["recordDetails"]["interests"][0]["type"] == "seniorManagingOfficial"

    def test_pj_partner_is_entity_with_cnpj_and_ownership(self) -> None:
        stmts = list(map_cnpj_brazil(_normalised_bundle()))
        # the PJ partner is a second entity statement carrying its own CNPJ
        partner = next(
            s for s in stmts if s["recordType"] == "entity"
            and s["recordDetails"]["name"] == "UNIAO HOLDING LTDA"
        )
        ids = {i["scheme"]: i["id"] for i in partner["recordDetails"]["identifiers"]}
        assert ids["BR-RFB"] == "11222333000181"
        rels = [s for s in stmts if s["recordType"] == "relationship"]
        socio_rel = next(
            r for r in rels
            if r["recordDetails"]["interests"][0].get("details") == "Sócio"
        )
        assert socio_rel["recordDetails"]["interests"][0]["type"] == "shareholding"
        assert socio_rel["recordDetails"]["interests"][0]["startDate"] == "2010-01-20"

    def test_official_register_source(self) -> None:
        for s in map_cnpj_brazil(_normalised_bundle()):
            assert s["source"]["type"] == ["officialRegister"]

    def test_deterministic_ids(self) -> None:
        a = [s["statementId"] for s in map_cnpj_brazil(_normalised_bundle())]
        b = [s["statementId"] for s in map_cnpj_brazil(_normalised_bundle())]
        assert a == b and a
