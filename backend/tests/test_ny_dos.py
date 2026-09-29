"""Tests for the New York Department of State adapter, mapper and History emitter.

Fixtures in ``tests/data/ny_dos_live.json`` are **real SODA 2.1 responses**
captured from data.ny.gov on 2026-09-29 with the exact queries the adapter
issues (see its ``_captured`` note). Keys are ``<entity>/<dataset>``:

* ``corning`` — CORNING INCORPORATED (``49779``), a 1936 consolidation with 91
  filings, a former name (CORNING GLASS WORKS until 1989), three chief
  executives across its statements, and a 1983 assumed-name row for
  J & M COFFEE SHOP that is **not Corning's** (the colliding numbering).
* ``maybank`` — MAYBANK SECURITIES USA INC. (``1484459``): two name changes,
  so GLEIF's older "Maybank Kim Eng Securities USA Inc." agrees only with a
  former name; a CEO filed as "CHAN KENG LOKE, PRESIDENT"; two CEOs at once.
* ``alta`` — ALTA PARTNERS, LLC (``4081330``): an LLC, which names no CEO.
* ``lentor`` — LENTOR CAPITAL LLC (``5810913``): dissolved 24 July 2026 while
  its LEI was still ISSUED — absent from the Active Corporations snapshot.
* ``quantexa`` — QUANTEXA INC. (``5215193``): a Delaware corporation
  authorised in New York; GLEIF files it under RA000628 with jurisdiction
  US-DE.
* ``collision_8512`` — DOS ``8512`` is THE MUNICIPAL WASTE PAPER RECEPTACLE
  COMPANY; GLEIF files ``8512`` for Salt City Federal Credit Union.
* ``missing`` — an ID DOS does not hold: four empty lists.
* ``_soql_error_400`` — the body Socrata returns for a rejected query.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck import degradation, outbound_rate
from opencheck.bods import liveness
from opencheck.bods.mapper import (
    _GLEIF_RA_TO_ORG_ID,
    gleif_registration_scheme,
    map_ny_dos,
    ny_ceo_statement_id,
)
from opencheck.cache import Cache
from opencheck.findings import MAX_FINDING_CHARS, finding_ny_dos
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_ny_dos, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources import ny_dos as nydos
from opencheck.sources.base import SearchKind
from opencheck.sources.gleif import GleifAdapter
from opencheck.sources.ny_dos import (
    NY_DOS_RA_CODE,
    NY_DOS_SCHEME,
    NyDosAdapter,
    ceo_name,
    jurisdiction_of,
    names_agree,
    normalise_dos_id,
    summarise,
    us_date,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.ny_dos import NyDosBundle
from opencheck.timeline import ChangeType, DateBasis, Tier, ny_dos_change_events

_DATA = json.loads(
    (Path(__file__).parent / "data" / "ny_dos_live.json").read_text(encoding="utf-8")
)
_R: dict[str, Any] = _DATA["responses"]
_IDS: dict[str, str] = _DATA["dos_ids"]

_DATASET_PART = {
    nydos.DATASET_FILINGS: "filings",
    nydos.DATASET_STATUS: "status",
    nydos.DATASET_NAMES: "names",
    nydos.DATASET_ADDRESSES: "addresses",
}


def _rows(label: str) -> dict[str, list[dict[str, Any]]]:
    return {
        "filings": _R[f"{label}/filings"],
        "status_history": _R[f"{label}/status"],
        "name_history": _R[f"{label}/names"],
        "addresses": _R[f"{label}/addresses"],
    }


def _bundle(label: str = "corning", **extra: Any) -> dict[str, Any]:
    """A bundle in the shape ``NyDosAdapter.fetch`` returns for a record."""
    bundle = {
        "source_id": "ny_dos",
        "dos_id": _IDS[label],
        "legal_name": "",
        **_rows(label),
        "truncated": False,
        "is_stub": False,
    }
    bundle.update(extra)
    return bundle


# ----------------------------------------------------------------------
# Identifiers and small parsers
# ----------------------------------------------------------------------


class TestNormaliseDosId:
    @pytest.mark.parametrize("raw,expected", [("49779", "49779"), (" 8512 ", "8512"), ("5 215 193", "5215193")])
    def test_whitespace_goes_and_nothing_else_changes(self, raw, expected):
        assert normalise_dos_id(raw) == expected

    def test_a_leading_zero_is_kept(self):
        """DOS stores the ID unpadded, so a zero would be a different number."""
        assert normalise_dos_id("0049779") == "0049779"

    @pytest.mark.parametrize("raw", ["", "  ", "ABC123", "1234567890", "12-34", None])
    def test_rejects_what_cannot_be_a_dos_id(self, raw):
        with pytest.raises(ValueError):
            normalise_dos_id(raw)


class TestCeoName:
    def test_a_trailing_title_is_removed(self):
        """Maybank's 1992 statement, verbatim."""
        assert ceo_name("CHAN KENG LOKE, PRESIDENT") == "CHAN KENG LOKE"

    def test_a_surname_first_name_is_left_alone(self):
        assert ceo_name("CHOI, JEONG EUM") == "CHOI, JEONG EUM"

    @pytest.mark.parametrize("raw", ["VACANT VACANT", "THE CORPORATION", "N/A", "", None])
    def test_a_placeholder_is_no_one(self, raw):
        assert ceo_name(raw) == ""


class TestJurisdictionOf:
    def test_a_us_state_is_the_united_states(self):
        assert jurisdiction_of("DE") == ("United States", "US")

    def test_a_canadian_province_is_canada(self):
        assert jurisdiction_of("BC") == ("Canada", "CA")

    def test_a_territory_is_its_own_country(self):
        assert jurisdiction_of("PR")[1] == "PR"

    @pytest.mark.parametrize("code", ["EN", "EW", "WL", "CZ", "", None])
    def test_a_dos_code_that_is_not_iso_resolves_to_nothing(self, code):
        """The risk engine's contract: no stand-in country code."""
        assert jurisdiction_of(code) is None


def test_us_date_reads_dos_text_dates():
    assert us_date("10/05/2017") == "2017-10-05"
    assert us_date("13/05/2017") is None
    assert us_date("") is None


# ----------------------------------------------------------------------
# summarise — the one reading of the rows
# ----------------------------------------------------------------------


class TestSummarise:
    def test_corning(self):
        s = summarise(_bundle("corning"))
        assert s["name"] == "CORNING INCORPORATED"
        assert s["former_names"] == ["CORNING GLASS WORKS"]
        assert s["domestic"] is True
        assert s["formed_on"] == "1936-12-24"
        assert s["status"] == "Active"
        assert s["ceos"] == ["WENDELL P WEEKS"]
        assert s["ceos_filed_on"] == "2024-12-02"

    def test_an_assumed_name_row_is_not_the_entitys(self):
        """Corning's 49779 carries a 1983 assumed-name filing for J & M COFFEE
        SHOP — a row from the separate assumed-name numbering that collides
        with DOS IDs. It must not become one of Corning's names."""
        assert any("COFFEE" in (f.get("corp_name") or "") for f in _R["corning/filings"])
        s = summarise(_bundle("corning"))
        assert "J & M COFFEE SHOP" not in s["assumed_names"]
        assert "J & M COFFEE SHOP" not in s["former_names"]

    def test_the_current_name_comes_from_the_name_history(self):
        s = summarise(_bundle("maybank"))
        assert s["name"] == "MAYBANK SECURITIES USA INC."
        assert s["former_names"] == [
            "KIM ENG SECURITIES U.S.A. INC.",
            "MAYBANK KIM ENG SECURITIES USA INC.",
        ]

    def test_every_ceo_on_the_latest_statement_is_current(self):
        s = summarise(_bundle("maybank"))
        assert s["ceos"] == ["JESSICA KIM", "AHMAD HAMDI BIN ABDULLAH"]

    def test_an_llc_names_no_ceo(self):
        assert summarise(_bundle("alta"))["ceos"] == []

    def test_a_dissolution_is_dated_by_its_filing(self):
        s = summarise(_bundle("lentor"))
        assert s["status"] == "Inactive"
        assert s["status_since"] == "2026-07-24"
        assert s["status_document"] == "CERTIFICATE OF DISSOLUTION-CANCELLATION"

    def test_a_foreign_entity(self):
        s = summarise(_bundle("quantexa"))
        assert s["domestic"] is False
        assert s["juris"] == "DE"
        assert s["authority_on"] == "2017-10-10"
        assert s["incorporated_on"] == "2017-10-05"
        # One CEO filed with three addresses on one statement is one person.
        assert s["ceos"] == ["VISHAL MARRIA"]


class TestNamesAgree:
    def test_the_live_collision_is_caught(self):
        assert names_agree("Salt City Federal Credit Union", _rows("collision_8512")) is False

    def test_a_former_name_agrees(self):
        """GLEIF still holds Maybank's 2012 name; DOS renamed it in 2023."""
        assert names_agree("Maybank Kim Eng Securities USA Inc.", _rows("maybank")) is True

    def test_the_current_name_agrees_across_a_legal_form_suffix(self):
        assert names_agree("Corning Inc", _rows("corning")) is True

    def test_an_assumed_name_row_never_passes_the_gate(self):
        assert names_agree("J & M Coffee Shop", _rows("corning")) is False

    def test_an_empty_legal_name_cannot_contradict_the_identifier(self):
        assert names_agree("", _rows("collision_8512")) is True


# ----------------------------------------------------------------------
# The adapter's HTTP path
# ----------------------------------------------------------------------


def _response(payload: Any, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.json.return_value = payload
    return response


def _routed_get(label: str, *, status: int = 200, failing: str | None = None) -> AsyncMock:
    """A client ``get`` that answers each dataset with its fixture.

    Routed by the dataset in the URL, because the four reads go out together
    and their order is not the test's business.
    """

    async def get(url: str, params=None, headers=None):  # noqa: ANN001
        dataset = url.rsplit("/", 1)[-1].removesuffix(".json")
        if failing and dataset == failing:
            return _response(_R["_soql_error_400"]["body"], 400)
        if status != 200:
            return _response({}, status)
        return _response(_R[f"{label}/{_DATASET_PART[dataset]}"])

    return AsyncMock(side_effect=get)


def _run_fetch(
    label: str,
    *,
    legal_name: str = "",
    dos_id: str | None = None,
    status: int = 200,
    failing: str | None = None,
    token: str | None = None,
) -> tuple[dict[str, Any], list, AsyncMock]:
    adapter = NyDosAdapter()
    settings = MagicMock(allow_live=True, socrata_app_token=token)
    get = _routed_get(label, status=status, failing=failing)
    with patch("opencheck.sources.ny_dos.get_settings", return_value=settings), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch.object(adapter._cache, "put", MagicMock()), \
         patch("opencheck.sources.ny_dos.build_client") as client:
        client.return_value.__aenter__.return_value.get = get
        with degradation.recording() as recorded:
            result = asyncio.run(adapter.fetch(dos_id or _IDS[label], legal_name=legal_name))
    return result, list(recorded), get


class TestFetch:
    def test_a_record_carries_all_four_datasets(self):
        bundle, recorded, get = _run_fetch("corning", legal_name="CORNING INCORPORATED")
        assert recorded == []
        assert bundle["is_stub"] is False
        assert len(bundle["filings"]) == 91
        assert len(bundle["name_history"]) == 2
        assert get.await_count == 4

    def test_every_query_is_scoped_to_the_id_and_ordered(self):
        _, _, get = _run_fetch("corning", legal_name="CORNING INCORPORATED")
        for call in get.await_args_list:
            params = call.kwargs["params"]
            assert params["$where"].startswith("corpid_num='49779'")
            assert params["$order"] == "date_filed,film_num"

    def test_only_ceo_and_principal_office_addresses_are_asked_for(self):
        """The service-of-process and registered-agent rows are never read."""
        _, _, get = _run_fetch("corning", legal_name="CORNING INCORPORATED")
        address_calls = [
            c for c in get.await_args_list if nydos.DATASET_ADDRESSES in c.args[0]
        ]
        assert "addr_type in('3','4')" in address_calls[0].kwargs["params"]["$where"]

    def test_the_app_token_goes_in_a_header_never_the_url(self):
        _, _, get = _run_fetch("corning", legal_name="CORNING INCORPORATED", token="tok-123")
        for call in get.await_args_list:
            assert call.kwargs["headers"]["X-App-Token"] == "tok-123"
            assert "tok-123" not in json.dumps(call.kwargs["params"])

    def test_a_former_name_passes_the_gate(self):
        bundle, _, _ = _run_fetch("maybank", legal_name="Maybank Kim Eng Securities USA Inc.")
        assert bundle["is_stub"] is False
        assert "name_mismatch" not in bundle

    def test_a_wrong_number_is_a_note_card_not_a_stub(self):
        """Salt City FCU's 8512 is another company. The card says so."""
        bundle, recorded, _ = _run_fetch(
            "collision_8512", legal_name="Salt City Federal Credit Union"
        )
        assert recorded == []
        assert bundle["name_mismatch"] is True
        assert bundle["not_found"] is True
        assert bundle["is_stub"] is False  # the pipeline builds a hit for it
        assert bundle["filings"] == []  # none of the other company's rows travel
        assert bundle["registered_name"] == "THE MUNICIPAL WASTE PAPER RECEPTACLE COMPANY"
        assert bundle["coverage_note"] == nydos.COVERAGE_NAME_MISMATCH

    def test_an_id_the_register_does_not_hold(self):
        bundle, recorded, _ = _run_fetch("missing", legal_name="Nothing Here Inc")
        assert recorded == []
        assert bundle["not_in_register"] is True
        assert bundle["not_found"] is True
        assert bundle["is_stub"] is False

    def test_a_malformed_id_never_reaches_the_network(self):
        bundle, _, get = _run_fetch("corning", dos_id="ABC")
        assert get.await_count == 0
        assert bundle["is_stub"] is True

    def test_a_rejected_query_is_a_degradation_not_an_empty_register(self):
        bundle, recorded, _ = _run_fetch(
            "corning", legal_name="CORNING INCORPORATED", failing=nydos.DATASET_STATUS
        )
        assert bundle["is_stub"] is True
        assert bundle.get("not_found") is None
        assert [d.source_id for d in recorded] == ["ny_dos"]
        assert "query.soql.no-such-column" in recorded[0].detail

    def test_a_throttle_is_a_rate_limit_degradation(self):
        bundle, recorded, _ = _run_fetch("corning", status=429)
        assert bundle["is_stub"] is True
        assert {d.reason for d in recorded} == {degradation.REASON_RATE_LIMITED}

    def test_search_returns_nothing(self):
        assert asyncio.run(NyDosAdapter().search("x", SearchKind.ENTITY)) == []


def _live_adapter(tmp_path: Path, label: str):
    adapter = NyDosAdapter()
    adapter._cache = Cache(root=tmp_path)
    return adapter, _routed_get(label)


class TestCacheAndBudget:
    def test_the_pipelines_second_pass_spends_no_further_calls(self, tmp_path):
        """Dispatch fetches with the legal name; ``_count_only`` / ``_safe_deepen``
        fetch without it. The cache is keyed on the DOS ID alone (the Phase 228
        rule), so the second pass is free."""
        adapter, get = _live_adapter(tmp_path, "corning")
        settings = MagicMock(allow_live=True, socrata_app_token=None)
        with patch("opencheck.sources.ny_dos.get_settings", return_value=settings), \
             patch("opencheck.sources.ny_dos.build_client") as client:
            client.return_value.__aenter__.return_value.get = get
            with outbound_rate.budget_scope() as budgets:
                first = asyncio.run(adapter.fetch("49779", legal_name="CORNING INCORPORATED"))
                second = asyncio.run(adapter.fetch("49779"))
        assert get.await_count == 4
        assert budgets["ny_dos"].spent == 4
        assert second["filings"] == first["filings"]

    def test_the_gate_runs_over_the_cache_not_behind_it(self, tmp_path):
        adapter, get = _live_adapter(tmp_path, "collision_8512")
        settings = MagicMock(allow_live=True, socrata_app_token=None)
        with patch("opencheck.sources.ny_dos.get_settings", return_value=settings), \
             patch("opencheck.sources.ny_dos.build_client") as client:
            client.return_value.__aenter__.return_value.get = get
            asyncio.run(adapter.fetch("8512"))  # no name: warms the cache
            gated = asyncio.run(
                adapter.fetch("8512", legal_name="Salt City Federal Credit Union")
            )
        assert get.await_count == 4
        assert gated["name_mismatch"] is True

    def test_a_failed_read_is_never_cached(self, tmp_path):
        """Phase 262: a failure is not remembered as "no record"."""
        adapter = NyDosAdapter()
        adapter._cache = Cache(root=tmp_path)
        settings = MagicMock(allow_live=True, socrata_app_token=None)
        with patch("opencheck.sources.ny_dos.get_settings", return_value=settings), \
             patch("opencheck.sources.ny_dos.build_client") as client:
            client.return_value.__aenter__.return_value.get = _routed_get(
                "corning", failing=nydos.DATASET_NAMES
            )
            with degradation.recording():
                asyncio.run(adapter.fetch("49779", legal_name="CORNING INCORPORATED"))
        assert adapter._cache.get_payload("ny_dos/entity/49779") is None

    def test_the_budget_covers_two_entities(self):
        assert nydos._LOOKUP_CALL_BUDGET >= 8


# ----------------------------------------------------------------------
# The BODS mapping
# ----------------------------------------------------------------------


def _by_type(statements: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [s for s in statements if s.get("recordType") == kind]


class TestMapper:
    def test_one_entity_and_one_ceo_relationship(self):
        statements = list(map_ny_dos(_bundle("corning")))
        assert len(_by_type(statements, "entity")) == 1
        assert len(_by_type(statements, "person")) == 1
        assert len(_by_type(statements, "relationship")) == 1

    def test_the_identifier_uses_the_shared_us_ny_scheme(self):
        entity = _by_type(list(map_ny_dos(_bundle("corning"))), "entity")[0]
        assert entity["recordDetails"]["identifiers"] == [
            {
                "id": "49779",
                "scheme": NY_DOS_SCHEME,
                "schemeName": "New York Department of State, Division of Corporations",
            }
        ]

    def test_former_names_become_alternate_names(self):
        entity = _by_type(list(map_ny_dos(_bundle("maybank"))), "entity")[0]
        assert entity["recordDetails"]["alternateNames"] == [
            "KIM ENG SECURITIES U.S.A. INC.",
            "MAYBANK KIM ENG SECURITIES USA INC.",
        ]

    def test_the_principal_office_is_the_business_address(self):
        entity = _by_type(list(map_ny_dos(_bundle("corning"))), "entity")[0]
        (address,) = entity["recordDetails"]["addresses"]
        assert address["type"] == "business"
        assert address["postCode"] == "14831-0001"
        assert address["country"] == {"name": "United States", "code": "US"}

    def test_the_ceo_is_name_and_role_only(self):
        """Stephen, 29 Sept 2026: no CEO address."""
        statements = list(map_ny_dos(_bundle("corning")))
        person = _by_type(statements, "person")[0]
        assert person["recordDetails"]["names"][0]["fullName"] == "WENDELL P WEEKS"
        assert "addresses" not in person["recordDetails"]
        (interest,) = _by_type(statements, "relationship")[0]["recordDetails"]["interests"]
        assert interest["type"] == "seniorManagingOfficial"
        assert "2024-12-02" in interest["details"]
        # Not a beneficial ownership register, so the flag is not stated.
        assert "beneficialOwnershipOrControl" not in interest

    def test_the_ceo_person_id_is_the_one_the_history_tab_points_at(self):
        person = _by_type(list(map_ny_dos(_bundle("corning"))), "person")[0]
        assert person["statementId"] == ny_ceo_statement_id("49779", "WENDELL P WEEKS")

    def test_an_llc_has_no_people(self):
        assert _by_type(list(map_ny_dos(_bundle("alta"))), "person") == []

    def test_a_dissolved_domestic_entity_is_terminal_and_dated(self):
        entity = _by_type(list(map_ny_dos(_bundle("lentor"))), "entity")[0]
        assert entity["recordDetails"]["dissolutionDate"] == "2026-07-24"
        assert liveness.read_register_status(entity)["liveness"] == liveness.TERMINAL

    def test_an_active_entity_is_live(self):
        entity = _by_type(list(map_ny_dos(_bundle("corning"))), "entity")[0]
        assert liveness.read_register_status(entity)["liveness"] == liveness.LIVE

    def test_a_foreign_entity_is_founded_at_home_not_in_new_york(self):
        entity = _by_type(list(map_ny_dos(_bundle("quantexa"))), "entity")[0]
        assert entity["recordDetails"]["foundingDate"] == "2017-10-05"
        assert entity["recordDetails"]["jurisdiction"] == {"name": "United States", "code": "US"}

    def test_a_foreign_entity_that_left_new_york_is_not_called_dissolved(self):
        bundle = _bundle("quantexa")
        bundle["status_history"] = list(bundle["status_history"]) + [
            {"corpid_num": "5215193", "film_num": "X1", "date_filed": "2026-01-01T00:00:00.000", "status": "Inactive"}
        ]
        entity = _by_type(list(map_ny_dos(bundle)), "entity")[0]
        assert "dissolutionDate" not in entity["recordDetails"]

    def test_a_note_card_maps_to_nothing(self):
        bundle = _bundle("collision_8512", filings=[], status_history=[], name_history=[], addresses=[], not_found=True)
        assert list(map_ny_dos(bundle)) == []

    def test_statement_ids_are_stable_across_runs(self):
        a = [s["statementId"] for s in map_ny_dos(_bundle("maybank"))]
        b = [s["statementId"] for s in map_ny_dos(_bundle("maybank"))]
        assert a == b


# ----------------------------------------------------------------------
# Finding and hit builder
# ----------------------------------------------------------------------


class TestFinding:
    def test_names_the_ceo_and_the_former_name(self):
        assert finding_ny_dos(_bundle("corning")) == (
            "Formed in New York 1936-12-24, chief executive officer on file: "
            "WENDELL P WEEKS, formerly CORNING GLASS WORKS."
        )

    def test_an_inactive_domestic_entity_leads_with_it(self):
        assert finding_ny_dos(_bundle("lentor")).startswith(
            "Inactive at the New York Department of State since 2026-07-24"
        )

    def test_a_foreign_entity_says_authorised_not_formed(self):
        assert finding_ny_dos(_bundle("quantexa")).startswith(
            "Authorised to do business in New York since 2017-10-10"
        )

    def test_a_corporation_with_no_ceo_says_so_and_an_llc_says_nothing(self):
        corporation = _bundle("corning", addresses=[])
        assert "no chief executive officer on file" in finding_ny_dos(corporation)
        assert "chief executive" not in finding_ny_dos(_bundle("alta"))

    def test_a_wrong_number_explains_the_empty_card(self):
        text = finding_ny_dos({"name_mismatch": True, "not_found": True})
        assert "belongs to a different entity" in text

    def test_a_miss_is_about_the_register_not_the_company(self):
        assert "No entity with this DOS ID" in finding_ny_dos({"not_in_register": True})

    @pytest.mark.parametrize("label", ["corning", "maybank", "alta", "lentor", "quantexa"])
    def test_it_asserts_no_risk_and_fits_the_cap(self, label):
        text = finding_ny_dos(_bundle(label))
        assert len(text) <= MAX_FINDING_CHARS
        for word in ("risk", "suspicious", "sanction", "owner"):
            assert word not in text.lower()

    def test_it_says_records_not_is(self):
        """DOS: the data is not "the current legal status of the entity"."""
        assert "dissolved" not in finding_ny_dos(_bundle("lentor")).lower()


def _ctx() -> _LookupCtx:
    ctx = MagicMock(spec=_LookupCtx)
    ctx.legal_name = "GLEIF NAME"
    return ctx


class TestHitBuilder:
    def test_it_asserts_only_the_dos_id_the_rows_carry(self):
        hit = _bh_ny_dos(_bundle("corning"), "49779", _ctx())
        assert hit.identifiers == {"us_ny_dos_id": "49779"}
        assert hit.name == "CORNING INCORPORATED"
        assert hit.summary == "US-NY 49779 · Active"
        assert hit.raw["chief_executive_officers"] == ["WENDELL P WEEKS"]

    def test_a_note_card_asserts_no_identifier(self):
        bundle = {"not_found": True, "name_mismatch": True, "coverage_note": nydos.COVERAGE_NAME_MISMATCH}
        hit = _bh_ny_dos(bundle, "8512", _ctx())
        assert hit.identifiers == {}
        assert hit.raw["not_found"] is True
        assert hit.finding and "different entity" in hit.finding
        assert hit.is_stub is False


class TestSchema:
    def test_a_real_bundle_validates(self):
        validate_raw("ny_dos", NyDosBundle, _bundle("maybank"))

    def test_a_missing_dos_id_is_a_schema_change(self):
        bundle = _bundle("corning")
        bundle.pop("dos_id")
        with pytest.raises(SourceSchemaError):
            validate_raw("ny_dos", NyDosBundle, bundle)

    def test_an_unknown_column_does_not_fail_a_lookup(self):
        bundle = _bundle("alta")
        bundle["filings"] = [dict(bundle["filings"][0], new_dos_column="x")]
        validate_raw("ny_dos", NyDosBundle, bundle)


# ----------------------------------------------------------------------
# Wiring
# ----------------------------------------------------------------------


class TestWiring:
    def test_the_adapter_is_registered_and_declares_its_deriver(self):
        adapter = REGISTRY["ny_dos"]
        assert isinstance(adapter, NyDosAdapter)
        (deriver,) = adapter.lookup_derivers
        assert NY_DOS_RA_CODE in deriver.ra_codes
        assert adapter.lookup_pass_legal_name is True

    def test_gleif_exposes_the_dos_id_for_the_reconciler_on_ra_not_jurisdiction(self):
        """Quantexa is US-DE and still files its NY DOS ID under RA000628."""

        def item(registered_as: str) -> dict[str, Any]:
            return {
                "id": "213800RJ7XZXYDMHMF35",
                "attributes": {
                    "lei": "213800RJ7XZXYDMHMF35",
                    "entity": {
                        "legalName": {"name": "Quantexa Inc"},
                        "jurisdiction": "US-DE",
                        "status": "ACTIVE",
                        "registeredAs": registered_as,
                        "registeredAt": {"id": NY_DOS_RA_CODE},
                    },
                },
            }

        hit = GleifAdapter._entity_hit(item("5215193"))
        assert hit.identifiers["us_ny_dos_id"] == "5215193"
        assert "us_ny_dos_id" not in GleifAdapter._entity_hit(item("C-12")).identifiers

    def test_the_ra_code_maps_to_us_ny_whatever_the_jurisdiction(self):
        assert _GLEIF_RA_TO_ORG_ID[NY_DOS_RA_CODE][0] == "US-NY"
        # Before Phase 263 the jurisdiction fallback called this a Delaware
        # file number.
        assert gleif_registration_scheme(NY_DOS_RA_CODE, "US-DE")[0] == "US-NY"

    def test_the_insurance_registry_is_not_mistaken_for_dos(self):
        """RA000747 files NAIC codes; as US-NY it would hop to DOS."""
        assert gleif_registration_scheme("RA000747", "US-NY")[0] == "RA000747"
        assert hop_for("RA000747") is None

    def test_us_ny_is_an_expandable_register_hop(self):
        hop = hop_for("US-NY")
        assert hop is not None and hop.source_id == "ny_dos"
        assert hop.normalise(" 49779 ") == "49779"

    def test_us_ny_never_stands_in_for_the_whole_united_states(self):
        """New York is one of fifty-odd US registers; with DC and New York both
        hops, a ``REG-US`` number must still not be looked up in either."""
        assert hop_for("REG-US") is None

    def test_the_source_info_declares_the_terms_and_attribution(self):
        info = REGISTRY["ny_dos"].info
        assert info.license == "OPEN-NY-Terms"
        assert "data.ny.gov" in info.attribution
        assert info.is_national_register is True
        assert info.country == "US"


# ----------------------------------------------------------------------
# The History emitter
# ----------------------------------------------------------------------


class TestHistory:
    def test_a_name_change_is_dated_by_its_filing(self):
        events = ny_dos_change_events(_bundle("maybank"))
        names = [e for e in events if e.change_type == ChangeType.LEGAL_NAME_CHANGE]
        assert [(e.event_date, e.value_old, e.value_new) for e in names] == [
            ("2012-02-21", "KIM ENG SECURITIES U.S.A. INC.", "MAYBANK KIM ENG SECURITIES USA INC."),
            ("2023-06-16", "MAYBANK KIM ENG SECURITIES USA INC.", "MAYBANK SECURITIES USA INC."),
        ]
        assert all(e.date_basis == DateBasis.EFFECTIVE for e in names)
        assert all(e.tier == Tier.IDENTITY_STATUS for e in names)

    def test_a_dissolution_is_a_status_change(self):
        events = ny_dos_change_events(_bundle("lentor"))
        (status,) = [e for e in events if e.tier == Tier.IDENTITY_STATUS]
        assert status.change_type == ChangeType.STATUS_CHANGED
        assert (status.value_old, status.value_new, status.event_date) == (
            "Active", "Inactive", "2026-07-24",
        )

    def test_a_merger_that_ends_a_domestic_entity_is_a_succession(self):
        bundle = _bundle("alta")
        bundle["filings"] = list(bundle["filings"]) + [
            {"corpid_num": "4081330", "film_num": "M1", "date_filed": "2026-01-05T00:00:00.000",
             "eff_date": "2026-01-01T00:00:00.000", "documenttype": "CERTIFICATE OF MERGER",
             "entitytype": "DOMESTIC LIMITED LIABILITY COMPANY", "juris": "NY"}
        ]
        bundle["status_history"] = list(bundle["status_history"]) + [
            {"corpid_num": "4081330", "film_num": "M1", "date_filed": "2026-01-05T00:00:00.000", "status": "Inactive"}
        ]
        (event,) = [e for e in ny_dos_change_events(bundle) if e.tier == Tier.IDENTITY_STATUS]
        assert event.change_type == ChangeType.SUCCESSION
        assert event.event_date == "2026-01-01"

    def test_ceo_changes_are_the_board_stream_and_approximate(self):
        """DOS publishes the statement that names a CEO, not the day they took
        office: each change is known to fall between two statements."""
        board = [e for e in ny_dos_change_events(_bundle("corning")) if e.tier == Tier.BOARD_CHANGE]
        assert [(e.change_type, e.counterparty, e.event_date) for e in board] == [
            (ChangeType.OFFICER_APPOINTED, "ROGER G ACKERMAN", "1996-12-20"),
            (ChangeType.OFFICER_APPOINTED, "JAMES R HOUGHTON", "2002-12-04"),
            (ChangeType.OFFICER_RESIGNED, "ROGER G ACKERMAN", "2002-12-04"),
            (ChangeType.OFFICER_APPOINTED, "WENDELL P WEEKS", "2006-12-08"),
            (ChangeType.OFFICER_RESIGNED, "JAMES R HOUGHTON", "2006-12-08"),
        ]
        assert all(e.date_basis == DateBasis.SNAPSHOT_WINDOW for e in board)
        assert board[3].date_range == ("2002-12-04", "2006-12-08")

    def test_only_a_current_ceo_row_points_at_a_person_node(self):
        board = [e for e in ny_dos_change_events(_bundle("corning")) if e.tier == Tier.BOARD_CHANGE]
        linked = {e.counterparty for e in board if e.party_statement_id}
        assert linked == {"WENDELL P WEEKS"}

    def test_a_titled_ceo_is_the_same_person_on_the_next_statement(self):
        """"CHAN KENG LOKE, PRESIDENT" (1992) and "CHAN KENG LOKE" (1993) are
        one appointment, not an appointment and a departure."""
        board = [e for e in ny_dos_change_events(_bundle("maybank")) if e.counterparty == "CHAN KENG LOKE"]
        assert [e.change_type for e in board] == [
            ChangeType.OFFICER_APPOINTED, ChangeType.OFFICER_RESIGNED,
        ]

    def test_other_filings_are_administrative_and_not_repeated(self):
        events = ny_dos_change_events(_bundle("lentor"))
        admin = [e for e in events if e.tier == Tier.ADMIN_NOISE]
        # Nine filings, one of which is the dissolution already told as a
        # status change.
        assert len(admin) == 8
        assert all(e.change_type is None for e in admin)

    def test_assumed_name_rows_never_reach_the_history(self):
        events = ny_dos_change_events(_bundle("corning"))
        assert not any("ASSUMED" in e.raw_change_type for e in events)

    def test_a_note_card_has_no_history(self):
        assert ny_dos_change_events({"not_found": True, "filings": []}) == []


# ----------------------------------------------------------------------
# /history — New York as the sixth change-log register
# ----------------------------------------------------------------------

_CORNING_LEI = "549300X2937PB0CJ7I56"


def _gleif_record(name: str, registered_as: str, jurisdiction: str = "US-NY") -> dict[str, Any]:
    return {
        "data": {
            "attributes": {
                "entity": {
                    "legalName": {"name": name},
                    "registeredAs": registered_as,
                    "registeredAt": {"id": NY_DOS_RA_CODE},
                    "jurisdiction": jurisdiction,
                }
            }
        }
    }


async def _history_for(monkeypatch, record: dict[str, Any], bundle: dict[str, Any] | None):
    import respx
    from httpx import Response

    from opencheck.config import get_settings
    from opencheck.routers.history import history

    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("COMPANIES_HOUSE_HISTORY_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    get_settings.cache_clear()
    fetch = AsyncMock(return_value=bundle)
    monkeypatch.setattr(REGISTRY["ny_dos"], "fetch_timeline_data", fetch)
    try:
        with respx.mock:
            respx.get(f"https://api.gleif.org/api/v1/lei-records/{_CORNING_LEI}").mock(
                return_value=Response(200, json=record)
            )
            respx.get(
                url__regex=rf"https://api\.gleif\.org/api/v1/lei-records/{_CORNING_LEI}/field-modifications"
            ).mock(return_value=Response(200, json={"data": [], "meta": {"pagination": {"lastPage": 1}}}))
            resp = await history(request=None, response=None, lei=_CORNING_LEI, include_noise=True)
    finally:
        get_settings.cache_clear()
    return resp, fetch


@pytest.mark.asyncio
async def test_history_reads_new_york_and_names_its_number(monkeypatch):
    resp, fetch = await _history_for(
        monkeypatch, _gleif_record("CORNING INCORPORATED", "49779"), _bundle("corning")
    )
    # The DOS read is name-gated with GLEIF's legal name.
    assert fetch.await_args.kwargs["legal_name"] == "CORNING INCORPORATED"
    assert fetch.await_args.args[0] == "49779"
    assert resp.registry_numbers == {"ny_dos": "49779"}
    assert "ny_dos" in resp.sources
    notable = [e for e in resp.notable if "ny_dos" in e.sources]
    assert [e.change_type for e in notable] == ["LEGAL_NAME_CHANGE"]
    board = [e for e in resp.events if e.source_id == "ny_dos" and e.tier == 4]
    assert len(board) == 5


@pytest.mark.asyncio
async def test_history_keys_on_the_ra_code_not_the_jurisdiction(monkeypatch):
    """Quantexa is US-DE; its New York history is still New York's."""
    resp, fetch = await _history_for(
        monkeypatch, _gleif_record("Quantexa Inc", "5215193", "US-DE"), _bundle("quantexa")
    )
    assert fetch.await_count == 1
    assert resp.registry_numbers == {"ny_dos": "5215193"}


@pytest.mark.asyncio
async def test_a_wrong_number_contributes_no_history(monkeypatch):
    """The adapter's gate returns no record for 8512 under Salt City's name,
    and the timeline says the register knows the number but published nothing
    — never the other company's filings."""
    resp, _ = await _history_for(
        monkeypatch, _gleif_record("Salt City Federal Credit Union", "8512"), None
    )
    assert resp.registry_numbers == {"ny_dos": "8512"}
    assert "ny_dos" not in resp.sources
