"""Tests for the Pennsylvania Department of State adapter (``pa_dos``).

Fixtures in ``tests/fixtures/pa_dos/`` are **real SODA 2.1 answers** from
data.pa.gov (dataset ``xvd7-5r2c``), captured on 12 Nov 2026 with the
adapter's own ``$select``:

* ``filing_0003039985`` — COMCAST CORPORATION, a domestic business
  corporation: six rows, four people, one of them with two titles.
* ``filing_0000754401`` — THE PNC FINANCIAL SERVICES GROUP, INC.
* ``filing_0000676862`` — FORTA, LLC, the ticket's example: GLEIF files
  ``676862``, the dataset ``0000676862``.
* ``filing_0007282567`` — Forta Blocker, Inc., a foreign business
  corporation.
* ``filing_0006775138`` — RiskMetrics Solutions, LLC: a "Governor" row with
  no name, and a member that is a company ("RiskMetrics Group" +
  "Holdings, LLC" split across first and last name).
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
from opencheck.bods import map_pa_dos
from opencheck.bods.mapper import gleif_registration_scheme, map_gleif
from opencheck.bods.officer_roles import officer_position
from opencheck.cache import Cache
from opencheck.findings import MAX_FINDING_CHARS, finding_pa_dos
from opencheck.licensing import classify
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_pa_dos, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources.pa_dos import (
    COVERAGE_NOT_ON_LIST,
    PA_DOS_RA_CODE,
    PA_DOS_SCHEME,
    PaDosAdapter,
    cut_from_header,
    day_of,
    is_domestic,
    names_agree,
    normalise_filing_number,
    padded,
    summarise,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.pa_dos import PaDosBundle

_FIX = Path(__file__).parent / "fixtures" / "pa_dos"


def _rows(number: str) -> list[dict[str, Any]]:
    return json.loads((_FIX / f"filing_{number}.json").read_text(encoding="utf-8"))


def _bundle(number: str, **extra: Any) -> dict[str, Any]:
    return {
        "source_id": "pa_dos", "key": number.lstrip("0"), "rows": _rows(number),
        "data_cut": "2026-11-03", "is_stub": False, **extra,
    }


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


class TestFilingNumber:
    @pytest.mark.parametrize(
        "raw, bare",
        [("676862", "676862"), ("0000676862", "676862"), (" 3039985 ", "3039985"),
         ("0015735802", "15735802"), ("1", "1")],
    )
    def test_every_spelling_is_one_number(self, raw, bare):
        assert normalise_filing_number(raw) == bare

    @pytest.mark.parametrize("raw", ["", "0000000000", "12345678901", "AB123", "123-45"])
    def test_rejects(self, raw):
        with pytest.raises(ValueError):
            normalise_filing_number(raw)

    def test_the_dataset_pads_to_ten_digits(self):
        assert padded("676862") == "0000676862"

    def test_the_deriver_keys_on_the_ra_code(self):
        derivers = REGISTRY["pa_dos"].lookup_derivers
        assert derivers[0].ra_codes == frozenset({PA_DOS_RA_CODE})
        assert derivers[0].derived_key == "us_pa_filing_number"

    def test_gleif_writes_us_pa_whatever_the_jurisdiction(self):
        # One ISSUED record under RA000632 is a Connecticut company.
        assert gleif_registration_scheme("RA000632", "US-CT", None, "123")[0] == "US-PA"
        assert gleif_registration_scheme("RA000632", "US-PA", None, "676862")[0] == "US-PA"

    def test_us_pa_is_a_register_hop_with_no_country_alias(self):
        hop = hop_for("US-PA")
        assert hop is not None and hop.source_id == "pa_dos"
        assert hop.normalise("0000676862") == "676862"
        alias = hop_for("REG-US")
        assert alias is None or alias.source_id != "pa_dos"


def test_the_gleif_mapper_writes_a_padded_number_bare():
    record = {
        "lei": "5493001ABCDEFGHIJK12",
        "entity": {
            "legalName": {"name": "Master Tenant Clearfield, LLC"},
            "jurisdiction": "US-PA",
            "registeredAt": {"id": "RA000632"},
            "registeredAs": "0015735802",
            "legalAddress": {"country": "US"},
            "status": "ACTIVE",
        },
    }
    statements = list(map_gleif({"record": record, "lei": record["lei"]}))
    entity = next(s for s in statements if s.get("recordType") == "entity")
    ids = {(i.get("scheme"), i["id"]) for i in entity["recordDetails"]["identifiers"]}
    assert ("US-PA", "15735802") in ids


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------


def test_day_of():
    assert day_of("1978-10-19T00:00:00.000") == "1978-10-19"
    assert day_of("1753-01-01T00:00:00.000") is None
    assert day_of(None) is None


def test_the_cut_is_the_last_modified_day():
    assert cut_from_header("Tue, 03 Nov 2026 17:58:16 GMT") == "2026-11-03"
    assert cut_from_header(None) is None
    assert cut_from_header("not a date") is None


def test_domestic_is_every_type_but_foreign():
    assert is_domestic("Domestic Business Corporation")
    assert is_domestic("Authority")
    assert not is_domestic("Foreign Limited Liability Company")


def test_the_name_gate():
    assert names_agree("COMCAST CORPORATION", "COMCAST CORPORATION")
    assert names_agree("FORTA, LLC", "Forta, Llc")
    assert not names_agree("EVERETT CASH MUTUAL INSURANCE CO.", "ECM Insurance Company")


def test_summarise_comcast():
    s = summarise(_bundle("0003039985"))
    assert s["filing_number"] == "3039985"
    assert s["name"] == "COMCAST CORPORATION"
    assert s["domestic"] is True
    assert s["created_on"] == "2001-12-07"
    by_name = {p["name"]: p["titles"] for p in s["parties"]}
    assert len(by_name) == 4
    assert by_name["Thomas Reid"] == ["Secretary", "Director"]
    assert by_name["Thomas Donnelly"] == ["Governor", "SVP State & Local Tax"]


def test_a_title_with_no_name_is_no_one_and_a_company_is_an_entity():
    s = summarise(_bundle("0006775138"))
    assert [p["name"] for p in s["parties"]] == ["RiskMetrics Group Holdings, LLC"]
    assert s["parties"][0]["is_entity"] is True


def test_the_schema_holds_the_rows():
    validate_raw("pa_dos", PaDosBundle, _bundle("0003039985"))
    with pytest.raises(SourceSchemaError):
        validate_raw("pa_dos", PaDosBundle, {"key": "1", "rows": [{"business_name_source_": "X"}]})


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def _response(payload: Any, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.json.return_value = payload
    response.headers = {"Last-Modified": "Tue, 03 Nov 2026 17:58:16 GMT"}
    return response


def _get(routes: dict[str, Any], *, status: int = 200) -> AsyncMock:
    async def get(url: str, params=None, headers=None):  # noqa: ANN001
        if status != 200:
            return _response({}, status)
        return _response(routes.get(params.get("filing_number"), []))

    return AsyncMock(side_effect=get)


def _fetch(
    key: str, routes: dict[str, Any], *, legal_name: str = "", status: int = 200,
    token: str | None = None, cache: Cache | None = None,
) -> tuple[dict[str, Any], list, AsyncMock]:
    adapter = PaDosAdapter()
    if cache is not None:
        adapter._cache = cache
    settings = MagicMock(allow_live=True, socrata_app_token=token)
    get = _get(routes, status=status)
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch("opencheck.sources.pa_dos.get_settings", return_value=settings))
        client = stack.enter_context(patch("opencheck.sources.pa_dos.build_client"))
        if cache is None:
            stack.enter_context(patch.object(adapter._cache, "get_payload", return_value=None))
            stack.enter_context(patch.object(adapter._cache, "put", MagicMock()))
        client.return_value.__aenter__.return_value.get = get
        with degradation.recording() as recorded:
            result = asyncio.run(adapter.fetch(key, legal_name=legal_name))
    return result, list(recorded), get


class TestFetch:
    def test_the_number_gleif_files_is_padded_for_the_query(self):
        bundle, recorded, get = _fetch(
            "676862", {"0000676862": _rows("0000676862")}, legal_name="FORTA, LLC",
        )
        assert recorded == []
        assert get.await_args_list[0].kwargs["params"]["filing_number"] == "0000676862"
        assert len(bundle["rows"]) == 1
        assert bundle["data_cut"] == "2026-11-03"

    def test_the_app_token_goes_in_a_header(self):
        _, _, get = _fetch("3039985", {"0003039985": _rows("0003039985")}, token="tok")
        call = get.await_args_list[0]
        assert call.kwargs["headers"]["X-App-Token"] == "tok"
        assert "tok" not in json.dumps(call.kwargs["params"])

    def test_a_number_not_on_the_list_is_a_note_card_never_dissolved(self):
        bundle, recorded, _ = _fetch("15735802", {}, legal_name="Master Tenant Clearfield, LLC")
        assert recorded == []
        assert bundle["not_on_list"] and bundle["not_found"]
        assert bundle["is_stub"] is False
        assert bundle["coverage_note"] == COVERAGE_NOT_ON_LIST
        assert "dissolved" not in (finding_pa_dos(bundle) or "").replace("no dissolved", "")

    def test_another_name_on_the_number_is_a_note_card(self):
        bundle, _, _ = _fetch(
            "3039985", {"0003039985": _rows("0003039985")}, legal_name="EVERETT CASH MUTUAL",
        )
        assert bundle["name_mismatch"] and bundle["rows"] == []
        assert bundle["registered_name"] == "COMCAST CORPORATION"

    def test_a_failed_query_is_a_degradation_and_a_stub(self, tmp_path):
        cache = Cache(root=tmp_path)
        bundle, recorded, _ = _fetch("3039985", {}, status=503, cache=cache)
        assert bundle["is_stub"] is True
        assert recorded
        assert cache.get_payload("pa_dos/filing/3039985") is None

    def test_a_throttle_is_a_rate_limit(self):
        _, recorded, _ = _fetch("3039985", {}, status=429)
        assert recorded[0].reason == degradation.REASON_RATE_LIMITED

    def test_a_malformed_key_never_reaches_the_network(self):
        bundle, _, get = _fetch("ABC", {})
        assert bundle["is_stub"] is True
        assert get.await_count == 0

    def test_a_second_fetch_is_served_from_the_cache(self, tmp_path):
        cache = Cache(root=tmp_path)
        _fetch("3039985", {"0003039985": _rows("0003039985")}, cache=cache)
        bundle, _, get = _fetch("3039985", {}, cache=cache)
        assert get.await_count == 0
        assert bundle["rows"] and bundle["data_cut"] == "2026-11-03"


# ---------------------------------------------------------------------------
# Mapper, finding, hit
# ---------------------------------------------------------------------------


def _by_type(statements: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == kind]


class TestMapper:
    def test_comcast(self):
        statements = list(map_pa_dos(_bundle("0003039985")))
        (company,) = _by_type(statements, "entity")
        rd = company["recordDetails"]
        assert rd["name"] == "COMCAST CORPORATION"
        assert rd["identifiers"] == [
            {"id": "3039985", "scheme": PA_DOS_SCHEME,
             "schemeName": "Pennsylvania Department of State, Bureau of Corporations"}
        ]
        assert rd["jurisdiction"]["code"] == "US"
        assert rd["foundingDate"] == "2001-12-07"
        assert "dissolutionDate" not in rd
        assert company["statementDate"] == "2026-11-03"
        notes = [a["description"] for a in company.get("annotations", [])]
        assert any("states no status" in n for n in notes)
        assert len(_by_type(statements, "person")) == 4
        assert len(_by_type(statements, "relationship")) == 4

    def test_titles_are_roles_never_ownership(self):
        rows = [dict(_rows("0000676862")[0], party_type=t) for t in
                ("Owner", "Stockholder", "100% Owner", "General Partner", "Member")]
        statements = list(map_pa_dos({"rows": rows, "key": "676862", "data_cut": "2026-11-03"}))
        (rel,) = _by_type(statements, "relationship")
        for interest in rel["recordDetails"]["interests"]:
            assert interest["type"] == "seniorManagingOfficial"
            assert "share" not in interest
            assert not interest.get("beneficialOwnershipOrControl")

    def test_an_organizer_is_an_officer_marked_role(self):
        rows = [dict(_rows("0000676862")[0], party_type="Organizer")]
        statements = list(map_pa_dos({"rows": rows, "key": "676862"}))
        (rel,) = _by_type(statements, "relationship")
        (interest,) = rel["recordDetails"]["interests"]
        assert interest["type"] == "otherInfluenceOrControl"
        assert officer_position(interest) == "Organizer"

    def test_a_governor_is_glossed(self):
        statements = list(map_pa_dos(_bundle("0000676862")))
        (rel,) = _by_type(statements, "relationship")
        assert "15 Pa.C.S. § 102" in rel["recordDetails"]["interests"][0]["details"]

    def test_a_foreign_registration_has_no_jurisdiction_or_founding(self):
        statements = list(map_pa_dos(_bundle("0007282567")))
        (company,) = _by_type(statements, "entity")
        rd = company["recordDetails"]
        assert "jurisdiction" not in rd and "foundingDate" not in rd
        assert any("registered in Pennsylvania" in a["description"] for a in company["annotations"])

    def test_a_company_party_is_an_entity_with_no_jurisdiction(self):
        statements = list(map_pa_dos(_bundle("0006775138")))
        parties = [s for s in _by_type(statements, "entity")
                   if s["recordDetails"]["name"] != "RiskMetrics Solutions, LLC"]
        assert [p["recordDetails"]["name"] for p in parties] == ["RiskMetrics Group Holdings, LLC"]
        assert "jurisdiction" not in parties[0]["recordDetails"]
        assert _by_type(statements, "person") == []

    def test_a_note_card_and_a_stub_map_to_nothing(self):
        assert list(map_pa_dos({"not_found": True, "rows": []})) == []
        assert list(map_pa_dos({"is_stub": True, "rows": []})) == []


class TestFindingAndHit:
    def test_a_record(self):
        sentence = finding_pa_dos(_bundle("0003039985"))
        assert sentence and sentence.startswith(
            "On the Pennsylvania Department of State's list of active registrations"
        )
        assert len(sentence) <= MAX_FINDING_CHARS
        assert " active." not in sentence and "is active" not in sentence

    def test_one_person_is_named(self):
        sentence = finding_pa_dos(_bundle("0000676862"))
        assert "one person listed: Lauren Camp (governor)" in sentence

    @pytest.mark.parametrize("flag", ["not_on_list", "name_mismatch"])
    def test_every_note_card_has_a_sentence(self, flag):
        assert finding_pa_dos({flag: True, "not_found": True, "rows": []})

    def test_the_hit_asserts_the_filing_number(self):
        hit = _bh_pa_dos(_bundle("0003039985"), "3039985", _LookupCtx(lei="51M0QTTNCGUN7KFCFZ59"))
        assert hit.identifiers == {"us_pa_filing_number": "3039985"}
        assert hit.finding

    def test_a_note_card_asserts_nothing(self):
        hit = _bh_pa_dos({"not_found": True, "not_on_list": True, "rows": []}, "1", _LookupCtx(lei="x"))
        assert hit.identifiers == {}


def test_registered_and_free_to_reuse():
    info = REGISTRY["pa_dos"].info
    assert info.country == "US" and info.is_national_register
    terms = classify(info.license)
    assert terms.commercial_use == "yes" and terms.share_alike is False
    assert terms.license == "PA-Open-Data"
