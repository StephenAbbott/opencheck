"""Tests for the Colombia RUES adapter, its schema, mapper, finding and hit.

Fixtures in ``tests/fixtures/rues_colombia/`` are **real SODA 2.1 answers**
from datos.gov.co (dataset ``c82u-588k``), captured on 10 Nov 2026 with the
adapter's own ``$select``:

* ``nit_890903938`` — BANCOLOMBIA S.A., whose razón social goes on to list
  the other name it may use ("…, ADEMÁS PODRÁ GIRAR BAJO LA DENOMINACIÓN
  BANCO DE COLOMBIA S.A.").
* ``nit_860002964`` — BANCO DE BOGOTA, one ACTIVA matrícula at Bogotá.
* ``matricula_759723`` — matrícula 759723 is FIBRATECH INTERNATIONAL S.A.S.
  at Barranquilla *and* a cancelled sole trader at Cali (each chamber numbers
  its own). The sole trader's name and the representative's are replaced.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck import degradation
from opencheck.bods import map_rues_colombia
from opencheck.bods.mapper import gleif_registration_scheme
from opencheck.cache import Cache
from opencheck.findings import MAX_FINDING_CHARS, finding_rues_colombia
from opencheck.identifiers import colombian_nit, nit_check_digit
from opencheck.licensing import classify
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_rues_colombia, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources.rues_colombia import (
    CO_RA_CODES,
    CO_RUE_SCHEME,
    RuesColombiaAdapter,
    current_row,
    legal_name,
    names_agree,
    normalise_rues_key,
    parse_registered_as,
    summarise,
    ymd,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.rues_colombia import RuesColombiaBundle

_FIX = Path(__file__).parent / "fixtures" / "rues_colombia"


def _rows(name: str) -> list[dict[str, Any]]:
    return json.loads((_FIX / f"{name}.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


class TestNit:
    @pytest.mark.parametrize(
        "base, dv",
        [("890903938", 8), ("860002964", 4), ("900396759", 5), ("800128735", 8)],
    )
    def test_the_dian_check_digit(self, base, dv):
        assert nit_check_digit(base) == dv

    @pytest.mark.parametrize(
        "raw",
        ["900396759-5", "900.396.759 - 5", "9003967595", "00000900396759 - 5"],
    )
    def test_every_spelling_gleif_files_is_one_nit(self, raw):
        assert colombian_nit(raw) == "900396759-5"

    @pytest.mark.parametrize("raw", ["900396759-4", "0000759723", "21-075155-12", ""])
    def test_a_wrong_check_digit_or_a_matricula_is_not_a_nit(self, raw):
        assert colombian_nit(raw) is None


class TestRegisteredAs:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("860002964-4", ("nit", "860002964")),
            ("890933815", ("nit", "890933815")),
            ("0000759723", ("matricula", "759723")),
            ("02045154", ("matricula", "2045154")),
            ("21-075155-12", ("matricula", "7515512")),
            ("080001177589 / Matrícula 1177589", ("matricula", "1177589")),
        ],
    )
    def test_shapes(self, raw, expected):
        assert parse_registered_as(raw) == expected

    @pytest.mark.parametrize("raw", ["", "0000", "ABC-123"])
    def test_rejects(self, raw):
        with pytest.raises(ValueError):
            parse_registered_as(raw)

    def test_the_lookup_key(self):
        assert normalise_rues_key("860002964-4") == "860002964"
        assert normalise_rues_key("21-075155-12") == "M7515512"
        assert normalise_rues_key("M7515512") == "M7515512"

    def test_the_deriver_covers_the_rue_every_chamber_and_the_superfinanciera(self):
        assert {"RA000093", "RA000094", "RA000101", "RA000150", "RA000848"} <= CO_RA_CODES
        assert "RA000151" not in CO_RA_CODES

    def test_gleif_writes_a_valid_nit_under_co_rue_and_a_matricula_under_its_ra(self):
        assert gleif_registration_scheme("RA000093", "CO", registered_as="860002964-4")[0] == CO_RUE_SCHEME
        assert gleif_registration_scheme("RA000101", "CO", registered_as="0000759723")[0] == "RA000101"

    def test_co_rue_is_a_register_hop(self):
        hop = hop_for("CO-RUE")
        assert hop is not None and hop.source_id == "rues_colombia"
        assert hop.normalise("860002964-4") == "860002964"


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def test_ymd():
    assert ymd("19841122") == "1984-11-22"
    assert ymd("0") is None
    assert ymd("99991231") is None


def test_the_alias_clause_is_cut_and_kept():
    name, clause = legal_name(_rows("nit_890903938")[0]["razon_social"])
    assert name == "BANCOLOMBIA S.A"
    assert clause and clause.startswith("ADEMÁS")


def test_a_plain_name_is_left_alone():
    assert legal_name("BANCO DE BOGOTA") == ("BANCO DE BOGOTA", None)


def test_the_current_row_prefers_an_active_one():
    moved = {"nit": "1", "estado_matricula": "MATRÍCULA CANCELADA POR TRASLADO DE DOMICILIO",
             "fecha_actualizacion": "2026/10/01"}
    active = {"nit": "1", "estado_matricula": "ACTIVA", "fecha_actualizacion": "2020/01/01"}
    assert current_row([moved, active]) is active


def test_the_name_gate():
    fibratech, _sole_trader = _rows("matricula_759723")
    assert names_agree("FIBRATECH INTERNATIONAL S.A.S.", fibratech)
    assert not names_agree("UNIVAR SOLUTIONS COLOMBIA S.A.S.", fibratech)


def test_summarise_bancolombia():
    s = summarise({"rows": _rows("nit_890903938")})
    assert s["nit_display"] == "890903938-8"
    assert s["name"] == "BANCOLOMBIA S.A"
    assert s["first_registered_on"] == "1984-11-22"
    assert s["chamber"] == "MEDELLIN PARA ANTIOQUIA"
    assert s["representative"]


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------


class TestPrivacy:
    def test_the_select_never_asks_for_a_persons_number(self):
        from opencheck.sources.rues_colombia import _SELECT

        columns = set(_SELECT.split(","))
        assert "num_identificacion_representante_legal" not in columns
        assert "numero_identificacion" not in columns
        assert not columns & {"primer_apellido", "segundo_apellido", "primer_nombre", "segundo_nombre"}

    def test_the_schema_refuses_the_representatives_number(self):
        row = dict(_rows("nit_860002964")[0])
        row["num_identificacion_representante_legal"] = "12345678"
        with pytest.raises(SourceSchemaError):
            validate_raw("rues_colombia", RuesColombiaBundle, {"key": "1", "rows": [row]})

    def test_the_schema_refuses_a_natural_persons_row(self):
        _fibratech, sole_trader = _rows("matricula_759723")
        with pytest.raises(SourceSchemaError):
            validate_raw("rues_colombia", RuesColombiaBundle, {"key": "1", "rows": [sole_trader]})


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def _response(payload: Any, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.json.return_value = payload
    return response


def _get(routes: dict[str, Any], *, status: int = 200) -> AsyncMock:
    async def get(url: str, params=None, headers=None):  # noqa: ANN001
        if status != 200:
            return _response({}, status)
        where = params["$where"]
        for needle, payload in routes.items():
            if needle in where:
                return _response(payload)
        return _response([])

    return AsyncMock(side_effect=get)


def _fetch(
    key: str, routes: dict[str, Any], *, legal_name: str = "", status: int = 200,
    token: str | None = None, cache: Cache | None = None,
) -> tuple[dict[str, Any], list, AsyncMock]:
    adapter = RuesColombiaAdapter()
    if cache is not None:
        adapter._cache = cache
    settings = MagicMock(allow_live=True, socrata_app_token=token)
    get = _get(routes, status=status)
    patches = [
        patch("opencheck.sources.rues_colombia.get_settings", return_value=settings),
        patch("opencheck.sources.rues_colombia.build_client"),
    ]
    if cache is None:
        patches += [
            patch.object(adapter._cache, "get_payload", return_value=None),
            patch.object(adapter._cache, "put", MagicMock()),
        ]
    with contextlib.ExitStack() as stack:
        stack.enter_context(patches[0])
        client = stack.enter_context(patches[1])
        for extra in patches[2:]:
            stack.enter_context(extra)
        client.return_value.__aenter__.return_value.get = get
        with degradation.recording() as recorded:
            result = asyncio.run(adapter.fetch(key, legal_name=legal_name))
    return result, list(recorded), get


class TestFetch:
    def test_a_nit_is_one_query_and_no_name_gate(self):
        bundle, recorded, get = _fetch(
            "890903938-8", {"nit='890903938'": _rows("nit_890903938")},
            legal_name="SOMETHING ELSE ENTIRELY",
        )
        assert recorded == []
        assert bundle["matched_by"] == "nit"
        assert len(bundle["rows"]) == 1
        assert get.await_count == 1

    def test_the_app_token_goes_in_a_header(self):
        _, _, get = _fetch("860002964", {"nit='860002964'": _rows("nit_860002964")}, token="tok")
        call = get.await_args_list[0]
        assert call.kwargs["headers"]["X-App-Token"] == "tok"
        assert "tok" not in json.dumps(call.kwargs["params"])

    def test_a_matricula_is_searched_across_chambers_and_name_gated(self):
        bundle, recorded, get = _fetch(
            "0000759723",
            {"matricula='759723'": _rows("matricula_759723"), "nit='901380825'": _rows("matricula_759723")[:1]},
            legal_name="FIBRATECH INTERNATIONAL S.A.S.",
        )
        assert recorded == []
        assert bundle["matched_by"] == "matricula"
        assert {r["nit"] for r in bundle["rows"]} == {"901380825"}
        # The sole trader at Cali never reaches the bundle.
        assert all(r["categoria_matricula"] != "PERSONA NATURAL" for r in bundle["rows"])

    def test_a_matricula_for_another_company_is_a_note_card(self):
        bundle, _, _ = _fetch(
            "0000759723", {"matricula='759723'": _rows("matricula_759723")},
            legal_name="UNIVAR SOLUTIONS COLOMBIA S.A.S.",
        )
        assert bundle["not_found"] and bundle["name_mismatch"]
        assert bundle["is_stub"] is False
        assert bundle["rows"] == []

    def test_a_matricula_two_companies_share_is_ambiguous(self):
        a = dict(_rows("matricula_759723")[0])
        b = dict(a, nit="900000001", camara_comercio="CALI")
        bundle, _, _ = _fetch(
            "0000759723", {"matricula='759723'": [a, b]},
            legal_name="FIBRATECH INTERNATIONAL S.A.S.",
        )
        assert bundle["ambiguous"] is True and bundle["rows"] == []

    def test_a_nit_rues_does_not_hold(self):
        bundle, recorded, _ = _fetch("900396759-5", {})
        assert recorded == []
        assert bundle["not_in_register"] and bundle["not_found"]
        assert bundle["is_stub"] is False

    def test_a_failed_query_is_a_degradation_and_a_stub(self, tmp_path):
        cache = Cache(root=tmp_path)
        bundle, recorded, _ = _fetch("860002964", {}, status=503, cache=cache)
        assert bundle["is_stub"] is True
        assert recorded
        assert cache.get_payload("rues_colombia/nit/860002964") is None

    def test_a_throttle_is_a_rate_limit(self):
        _, recorded, _ = _fetch("860002964", {}, status=429)
        assert recorded[0].reason == degradation.REASON_RATE_LIMITED

    def test_a_malformed_key_never_reaches_the_network(self):
        bundle, _, get = _fetch("ABC", {})
        assert bundle["is_stub"] is True
        assert get.await_count == 0

    def test_a_second_fetch_is_served_from_the_cache(self, tmp_path):
        cache = Cache(root=tmp_path)
        _fetch("860002964", {"nit='860002964'": _rows("nit_860002964")}, cache=cache)
        bundle, _, get = _fetch("860002964", {}, cache=cache)
        assert get.await_count == 0
        assert bundle["rows"]


# ---------------------------------------------------------------------------
# Mapper, finding, hit
# ---------------------------------------------------------------------------


def _bundle(name: str) -> dict[str, Any]:
    return {"source_id": "rues_colombia", "key": "x", "rows": _rows(name),
            "matched_by": "nit", "is_stub": False}


class TestMapper:
    def test_bancolombia(self):
        statements = list(map_rues_colombia(_bundle("nit_890903938")))
        entities = [s for s in statements if s["recordType"] == "entity"]
        people = [s for s in statements if s["recordType"] == "person"]
        rels = [s for s in statements if s["recordType"] == "relationship"]
        assert len(entities) == 1 and len(people) == 1 and len(rels) == 1
        rd = entities[0]["recordDetails"]
        assert rd["name"] == "BANCOLOMBIA S.A"
        assert rd["identifiers"] == [
            {"id": "890903938-8", "scheme": "CO-RUE",
             "schemeName": "NIT — Registro Único Empresarial y Social (RUES), Colombia"}
        ]
        assert rd["jurisdiction"]["code"] == "CO"
        assert rd["foundingDate"] == "1984-11-22"
        pointers = {a["statementPointerTarget"] for a in entities[0].get("annotations", [])}
        assert "/recordDetails/name" in pointers
        assert "/recordDetails/foundingDate" in pointers
        interest = rels[0]["recordDetails"]["interests"][0]
        assert interest["type"] == "seniorManagingOfficial"
        assert "beneficialOwnershipOrControl" not in interest or interest["beneficialOwnershipOrControl"] is False

    def test_the_person_carries_a_name_and_nothing_that_identifies_them(self):
        statements = list(map_rues_colombia(_bundle("nit_860002964")))
        person = next(s for s in statements if s["recordType"] == "person")
        assert not person["recordDetails"].get("identifiers")
        assert "CESAR PRADO VILLEGAS" in json.dumps(person)

    def test_a_stub_maps_to_nothing(self):
        assert list(map_rues_colombia({"rows": [], "is_stub": True})) == []


class TestFindingAndHit:
    def test_a_record(self):
        sentence = finding_rues_colombia(_bundle("nit_860002964"))
        assert sentence and sentence.startswith("Registered at the Bogota chamber of commerce")
        assert "legal representative on file" in sentence
        assert len(sentence) <= MAX_FINDING_CHARS

    @pytest.mark.parametrize("flag", ["name_mismatch", "ambiguous", "not_in_register"])
    def test_every_note_card_has_a_sentence(self, flag):
        assert finding_rues_colombia({flag: True, "not_found": True, "rows": []})

    def test_a_cancelled_registration_leads(self):
        rows = _rows("nit_860002964")
        rows[0] = dict(rows[0], estado_matricula="CANCELADA", fecha_cancelacion="20190403")
        sentence = finding_rues_colombia({"rows": rows})
        assert sentence and sentence.startswith("Registration cancelled in RUES on 3 April 2019")

    def test_the_hit_asserts_the_nit_rues_carries(self):
        hit = _bh_rues_colombia(_bundle("nit_860002964"), "860002964", _LookupCtx(lei="549300LYHE7LWSKDZT53"))
        assert hit.identifiers == {"co_nit": "860002964-4"}
        assert hit.finding

    def test_a_note_card_asserts_nothing(self):
        bundle = {"not_found": True, "name_mismatch": True, "rows": []}
        hit = _bh_rues_colombia(bundle, "M759723", _LookupCtx(lei="x"))
        assert hit.identifiers == {}


def test_registered_and_share_alike():
    info = REGISTRY["rues_colombia"].info
    assert info.country == "CO" and info.is_national_register
    terms = classify(info.license)
    assert terms.share_alike is True and terms.commercial_use == "yes"
