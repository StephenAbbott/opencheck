"""Tests for the Washington DC DLCP adapter and its mapper.

Fixtures in ``tests/data/dlcp_dc_live.json`` are **real ArcGIS FeatureServer
responses** captured from maps2.dcgis.dc.gov on 2026-09-18 (see its
``_captured`` note):

* ``acs_company`` / ``acs_trade_names`` / ``acs_owners`` — the American
  Chemical Society (file number ``000347``), a Domestic Act of Congress
  Corporation with one trade name and 16 owner rows. The nonprofit-board case
  the § 29-102.11(a)(6) governance test produces.
* ``collision_company`` — DLCP's ``L21249``, CONNIE-19 STREET LLC. GLEIF files
  ``L21249`` as the ``registeredAs`` for *American Foreign Policy Council*, so
  this is the live cross-entity collision the name gate exists to catch.
* ``afpc_by_name`` / ``afpc_owners`` / ``afpc_trade_names`` — AFPC's real
  record (``825027``), reached by name, which is the recovery path.
* ``revoked_company`` / ``revoked_owners`` / ``revoked_trade_names`` — a
  Revoked LLC, for the liveness mapping and the terminal-status finding.
* ``empty_result`` — a query that matched nothing (a clean miss, not a
  failure).
* ``_arcgis_error_http_200`` — ArcGIS reporting a rejected query **inside a
  200 body**, which is the failure mode a status-code check cannot see.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck import degradation
from opencheck.bods import liveness
from opencheck.bods.bo_regimes import REGIMES, boc_policy
from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, map_dlcp_dc
from opencheck.findings import MAX_FINDING_CHARS, finding_dlcp_dc
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_dlcp_dc, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources import dlcp_dc as dlcp
from opencheck.sources.base import SearchKind
from opencheck.sources.dlcp_dc import (
    DC_FILE_NUMBER_SCHEME,
    DLCP_RA_CODE,
    DlcpDcAdapter,
    classify_owner,
    clean_field,
    is_live_status,
    names_agree,
    normalise_file_number,
    parse_owner_address,
    trade_name_list,
)
from opencheck.sources.gleif import GleifAdapter
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.dlcp_dc import DlcpDcBundle

_FIXTURES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "data" / "dlcp_dc_live.json").read_text(encoding="utf-8")
)["responses"]


def _rows(key: str) -> list[dict[str, Any]]:
    return [f["attributes"] for f in _FIXTURES[key].get("features", [])]


def _company(key: str = "acs_company") -> dict[str, Any]:
    return _rows(key)[0]


def _bundle(
    *,
    company_key: str = "acs_company",
    owners_key: str = "acs_owners",
    trade_key: str = "acs_trade_names",
    **extra: Any,
) -> dict[str, Any]:
    """A bundle in the shape ``DlcpDcAdapter.fetch`` returns."""
    company = _company(company_key)
    bundle = {
        "source_id": "dlcp_dc",
        "file_number": company["FILE_NUMBER"],
        "company": company,
        "owners": _rows(owners_key),
        "trade_names": trade_name_list(_rows(trade_key)),
        "matched_by": "file_number",
        "legal_name": company["BUSINESS_NAME"],
        "is_stub": False,
    }
    bundle.update(extra)
    return bundle


# ----------------------------------------------------------------------
# File numbers are opaque
# ----------------------------------------------------------------------


class TestNormaliseFileNumber:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("000347", "000347"),
            (" 000347 ", "000347"),
            ("l00005029230", "L00005029230"),
            ("US-DC-LL012601299", "US-DC-LL012601299"),
            ("N00008414776", "N00008414776"),
            ("P00454", "P00454"),
        ],
    )
    def test_live_shapes_survive_unchanged(self, raw, expected):
        """Every shape the register actually issues, 2026-09-18."""
        assert normalise_file_number(raw) == expected

    def test_leading_zeros_are_never_stripped(self):
        """``000347`` and ``347`` are different companies to DLCP."""
        assert normalise_file_number("000347") == "000347"

    @pytest.mark.parametrize("raw", ["", "   ", "AB", "ab cd!", "L/00005029230", None])
    def test_rejects_what_cannot_be_a_file_number(self, raw):
        with pytest.raises(ValueError):
            normalise_file_number(raw)


# ----------------------------------------------------------------------
# Owner classification
# ----------------------------------------------------------------------


class TestClassifyOwner:
    @pytest.mark.parametrize(
        "name",
        [
            "Smug Industries, LLC",
            "RAHF IV FPW LP, LLC",
            "Lowell School, Inc.",
            "Munich Columbia Square Corp.",
            "Army Distaff Foundation Board",
            "DW PARTNERSHIP",
            "Carver Terrace Limited Liability Company",
            "EJF OpZone Fund I LP",
        ],
    )
    def test_legal_form_markers_make_an_entity(self, name):
        assert classify_owner(name) == "entity"

    @pytest.mark.parametrize(
        "name",
        [
            "Albert M. Horvath",
            "Dr. Rigoberto Hernandez",
            "Jonathan Hinds, MD",
            "Korey Neal, Sr.",
            "Gaurdie E. Banister, Jr.",
            "",
            None,
        ],
    )
    def test_everything_else_is_a_person(self, name):
        assert classify_owner(name) == "person"

    def test_a_surname_that_is_also_an_org_word_stays_a_person(self):
        """``_ENTITY_MARKERS`` holds forms of incorporation, never words that
        are also surnames — misfiling a named individual as a company is the
        error this ordering refuses."""
        for surname in ("Sarah Church", "Michael Bank", "Peter Fund", "Anna Group"):
            assert classify_owner(surname) == "person"


# ----------------------------------------------------------------------
# Addresses
# ----------------------------------------------------------------------


class TestParseOwnerAddress:
    def test_splits_the_registers_own_tail(self):
        got = parse_owner_address("1155 16th Street, NW, Washington, DC, 20036, USA")
        assert got == {
            "type": "residence",
            "address": "1155 16th Street, NW, Washington, DC",
            "postCode": "20036",
            "country": {"name": "United States", "code": "US"},
        }

    def test_zip_plus_four_and_a_non_us_country(self):
        got = parse_owner_address("1 High Street, London, ENG, GBR")
        assert got["country"] == {"name": "United Kingdom", "code": "GB"}
        assert "GBR" not in got["address"]
        assert "postCode" not in got

    def test_entity_owners_take_a_business_address(self):
        """BODS v0.4 allows ``residence`` only on a person record."""
        got = parse_owner_address(
            "222 Trolley Car Way, Morrisville, NC, 27560, USA",
            address_type="business",
        )
        assert got["type"] == "business"

    def test_an_unresolvable_country_code_is_kept_as_the_name(self):
        got = parse_owner_address("1 Somewhere, Nowhere, ZZZ")
        assert got["country"] == {"name": "ZZZ"}

    @pytest.mark.parametrize("raw", ["", "   ", None])
    def test_empty_is_none(self, raw):
        assert parse_owner_address(raw) is None


# ----------------------------------------------------------------------
# The name gate
# ----------------------------------------------------------------------


class TestNamesAgree:
    def test_the_live_collision_is_caught(self):
        """GLEIF files ``L21249`` for American Foreign Policy Council; DLCP's
        ``L21249`` is CONNIE-19 STREET LLC (both live, 2026-09-18)."""
        assert (
            names_agree("American Foreign Policy Council", _company("collision_company"))
            is False
        )

    def test_the_registers_own_name_agrees(self):
        assert names_agree("AMERICAN CHEMICAL SOCIETY", _company()) is True

    def test_a_rename_is_tolerated_through_a_trade_name(self):
        """DLCP publishes no name history, so trade names do the job former
        names do for other registers."""
        assert names_agree("ACS", _company(), ["ACS"]) is True

    def test_an_empty_legal_name_cannot_contradict_the_identifier(self):
        assert names_agree("", _company("collision_company")) is True
        assert names_agree(None, _company("collision_company")) is True

    def test_a_legal_form_suffix_difference_still_agrees(self):
        company = {"BUSINESS_NAME": "STEPTOE LLP"}
        assert names_agree("STEPTOE & JOHNSON LLP", company) is True


def test_is_live_status():
    assert is_live_status("Active - In Good Standing") is True
    assert is_live_status("Active - Not in Good Standing") is True
    assert is_live_status("Revoked") is False
    assert is_live_status("") is False


def test_trade_name_list_dedupes_and_keeps_order():
    rows = [
        {"TRADE_NAME": "WAMU 88.5"},
        {"TRADE_NAME": "WAMU 88.5"},
        {"TRADE_NAME": " BRIDGE CAFE (THE) "},
        {"TRADE_NAME": ""},
    ]
    assert trade_name_list(rows) == ["WAMU 88.5", "BRIDGE CAFE (THE)"]


# ----------------------------------------------------------------------
# The adapter's HTTP path
# ----------------------------------------------------------------------


def _response(payload: dict[str, Any], status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.json.return_value = payload
    return response


def _run_fetch(
    *payloads: dict[str, Any],
    file_number: str,
    legal_name: str = "",
    status: int = 200,
) -> tuple[dict[str, Any], list, MagicMock]:
    """``fetch`` against canned FeatureServer responses, one per call.

    The on-disk cache is patched out on every path: it is shared with the rest
    of the process, so a test that let it through would answer from whatever a
    previous run stored.
    """
    adapter = DlcpDcAdapter()
    settings = MagicMock(allow_live=True)
    put = MagicMock()
    get = AsyncMock(side_effect=[_response(p, status) for p in payloads])
    with patch("opencheck.sources.dlcp_dc.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=False), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch.object(adapter._cache, "put", put), \
         patch("opencheck.sources.dlcp_dc.build_client") as client:
        client.return_value.__aenter__.return_value.get = get
        with degradation.recording() as recorded:
            result = asyncio.run(adapter.fetch(file_number, legal_name=legal_name))
    return result, list(recorded), get


class TestFetch:
    def test_a_file_number_hit_carries_owners_and_trade_names(self):
        bundle, recorded, get = _run_fetch(
            _FIXTURES["acs_company"],
            _FIXTURES["acs_trade_names"],
            _FIXTURES["acs_owners"],
            file_number="000347",
            legal_name="AMERICAN CHEMICAL SOCIETY",
        )
        assert recorded == []
        assert bundle["is_stub"] is False
        assert bundle["matched_by"] == "file_number"
        assert bundle["company"]["BUSINESS_NAME"] == "AMERICAN CHEMICAL SOCIETY"
        assert len(bundle["owners"]) == 16
        assert bundle["trade_names"] == ["ACS"]
        assert get.await_count == 3

    def test_a_collision_falls_back_to_the_name_and_says_so(self):
        """The L21249 case: the filed identifier reaches another company, the
        name gate rejects it, and the register is asked for the name instead."""
        bundle, _, _ = _run_fetch(
            _FIXTURES["collision_company"],   # Table 0 by file number
            _FIXTURES["afpc_trade_names"],    # its trade names
            _FIXTURES["afpc_owners"],         # its owner rows
            _FIXTURES["afpc_by_name"],        # the name query
            _FIXTURES["afpc_trade_names"],
            _FIXTURES["afpc_owners"],
            file_number="L21249",
            legal_name="American Foreign Policy Council",
        )
        assert bundle["matched_by"] == "name"
        assert bundle["company"]["FILE_NUMBER"] == "825027"
        assert bundle["company"]["BUSINESS_NAME"] == "AMERICAN FOREIGN POLICY COUNCIL"

    def test_a_collision_with_no_safe_name_match_is_dropped(self):
        bundle, _, _ = _run_fetch(
            _FIXTURES["collision_company"],
            _FIXTURES["afpc_trade_names"],
            _FIXTURES["afpc_owners"],
            _FIXTURES["empty_result"],  # the name query finds nothing
            file_number="L21249",
            legal_name="American Foreign Policy Council",
        )
        assert bundle["name_mismatch"] is True
        assert bundle["company"] is None
        assert bundle["is_stub"] is True

    def test_a_file_number_the_register_does_not_hold(self):
        bundle, _, _ = _run_fetch(
            _FIXTURES["empty_result"],  # bare
            _FIXTURES["empty_result"],  # US-DC- prefixed retry
            _FIXTURES["empty_result"],  # the name query
            file_number="ZZ99999999",
            legal_name="Nothing Here Ltd",
        )
        assert bundle["not_in_register"] is True
        assert bundle["is_stub"] is True

    def test_a_malformed_file_number_never_reaches_the_network(self):
        bundle, _, get = _run_fetch(file_number="!!", legal_name="Whatever")
        assert get.await_count == 0
        assert bundle["is_stub"] is True

    def test_an_arcgis_error_inside_http_200_is_a_degradation(self):
        """The failure a status-code check cannot see: ArcGIS reports a
        rejected query in the body of a 200."""
        bundle, recorded, _ = _run_fetch(
            _FIXTURES["_arcgis_error_http_200"],
            file_number="000347",
            legal_name="AMERICAN CHEMICAL SOCIETY",
        )
        assert bundle["is_stub"] is True
        assert bundle.get("not_in_register") is None
        assert [d.source_id for d in recorded] == ["dlcp_dc"]

    def test_an_http_failure_is_a_degradation_not_an_empty_register(self):
        bundle, recorded, _ = _run_fetch(
            {},
            file_number="000347",
            legal_name="AMERICAN CHEMICAL SOCIETY",
            status=503,
        )
        assert bundle["is_stub"] is True
        assert bundle.get("not_in_register") is None
        assert [d.source_id for d in recorded] == ["dlcp_dc"]

    def test_search_returns_nothing(self):
        assert asyncio.run(DlcpDcAdapter().search("x", SearchKind.ENTITY)) == []


# ----------------------------------------------------------------------
# The BODS mapping
# ----------------------------------------------------------------------


def _by_type(statements: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [s for s in statements if s.get("recordType") == kind]


class TestMapper:
    @pytest.fixture
    def statements(self):
        return list(map_dlcp_dc(_bundle()))

    def test_one_entity_per_company_and_one_relationship_per_owner(self, statements):
        assert len(_by_type(statements, "entity")) == 1
        assert len(_by_type(statements, "person")) == 16
        assert len(_by_type(statements, "relationship")) == 16

    def test_the_identifier_uses_the_shared_us_dc_scheme(self, statements):
        entity = _by_type(statements, "entity")[0]
        identifier = entity["recordDetails"]["identifiers"][0]
        assert identifier["scheme"] == DC_FILE_NUMBER_SCHEME == "US-DC"
        assert identifier["id"] == "000347"

    def test_the_registers_own_wording_goes_to_entity_details(self, statements):
        entity_type = _by_type(statements, "entity")[0]["recordDetails"][
            "entityType"
        ]
        assert entity_type["type"] == "registeredEntity"
        assert "Domestic Act of Congress Corporation" in entity_type["details"]
        assert "subtype" not in entity_type

    def test_trade_names_become_alternate_names(self, statements):
        entity = _by_type(statements, "entity")[0]
        assert any(
            n.get("fullName") == "ACS" for n in entity["recordDetails"].get("names", [])
        ) or "ACS" in json.dumps(entity["recordDetails"])

    def test_every_interest_is_unknown_with_no_share(self, statements):
        for rel in _by_type(statements, "relationship"):
            for interest in rel["recordDetails"]["interests"]:
                assert interest["type"] == "unknownInterest"
                assert interest["directOrIndirect"] == "unknown"
                assert "share" not in interest

    def test_a_person_owner_asserts_beneficial_ownership(self, statements):
        for rel in _by_type(statements, "relationship"):
            for interest in rel["recordDetails"]["interests"]:
                assert interest["beneficialOwnershipOrControl"] is True

    def test_an_entity_owner_never_asserts_beneficial_ownership(self):
        bundle = _bundle()
        bundle["owners"] = [
            {
                "NAME": "RAHF IV FPW LP, LLC",
                "ADDRESS": "222 Trolley Car Way, Morrisville, NC, 27560, USA",
                "INITIALFILENUMBER": "000347",
                "STATUS": "Active - In Good Standing",
            }
        ]
        statements = list(map_dlcp_dc(bundle))
        parties = _by_type(statements, "entity")
        assert len(parties) == 2  # the company and the corporate owner
        rel = _by_type(statements, "relationship")[0]
        assert rel["recordDetails"]["interests"][0]["beneficialOwnershipOrControl"] is False
        owner = [e for e in parties if e["recordDetails"]["name"] != "AMERICAN CHEMICAL SOCIETY"][0]
        assert owner["recordDetails"]["addresses"][0]["type"] == "business"

    def test_the_interest_says_what_the_register_actually_asked(self, statements):
        details = statements[-1]["recordDetails"]["interests"][0]["details"]
        assert "29-102.11(a)(6)" in details
        assert "governance" in details
        assert "no role and no percentage" in details

    def test_a_blank_owner_name_becomes_an_unknown_person_not_a_dropped_row(self):
        bundle = _bundle()
        bundle["owners"] = [
            {"NAME": "  ", "ADDRESS": "1 A St, Washington, DC, 20001, USA"},
            {"NAME": "", "ADDRESS": "2 B St, Washington, DC, 20001, USA"},
        ]
        statements = list(map_dlcp_dc(bundle))
        people = _by_type(statements, "person")
        assert len(people) == 2, "two blank rows are two filings, not one"
        for person in people:
            assert person["recordDetails"]["personType"] == "unknownPerson"
            assert "anonymous" not in json.dumps(person).lower()

    def test_a_duplicate_owner_row_is_one_relationship(self):
        bundle = _bundle()
        row = {
            "NAME": "WB RECYCLING SOLUTIONS, LLC",
            "ADDRESS": "1 A St, Washington, DC, 20001, USA",
        }
        bundle["owners"] = [row, dict(row)]
        statements = list(map_dlcp_dc(bundle))
        assert len(_by_type(statements, "relationship")) == 1

    def test_a_revoked_company_is_terminal(self):
        statements = list(
            map_dlcp_dc(
                _bundle(
                    company_key="revoked_company",
                    owners_key="revoked_owners",
                    trade_key="revoked_trade_names",
                )
            )
        )
        entity = _by_type(statements, "entity")[0]
        status = liveness.read_register_status(entity)
        assert status["liveness"] == liveness.TERMINAL
        assert status["raw"] == "Revoked"

    def test_an_active_company_is_live(self, statements):
        entity = _by_type(statements, "entity")[0]
        status = liveness.read_register_status(entity)
        assert status["liveness"] == liveness.LIVE
        assert status["raw"] == "Active - In Good Standing"

    def test_a_pre_1970_effective_date_survives(self):
        bundle = _bundle()
        bundle["company"] = dict(bundle["company"], EFFECTIVE_DATE=-1359831600000)
        entity = list(map_dlcp_dc(bundle))[0]
        assert entity["recordDetails"]["foundingDate"].startswith("19")

    def test_a_stub_maps_to_nothing(self):
        assert list(map_dlcp_dc({"is_stub": True})) == []
        assert list(map_dlcp_dc({})) == []

    def test_statement_ids_are_stable_across_runs(self):
        a = list(map_dlcp_dc(_bundle()))
        b = list(map_dlcp_dc(_bundle()))
        assert [s["statementId"] for s in a] == [s["statementId"] for s in b]


# ----------------------------------------------------------------------
# The BO regime
# ----------------------------------------------------------------------


class TestRegime:
    def test_the_regime_is_registered_with_the_statutory_threshold(self):
        regime = REGIMES["dlcp_dc"]
        assert regime.jurisdiction_code == "US-DC"
        assert regime.threshold_operator == ">"
        assert regime.threshold_value == 10.0
        assert any("29-102.11(a)(6)" in basis for basis in regime.legal_basis)

    def test_record_kinds_route_the_flag(self):
        assert boc_policy("dlcp_dc", "bo_person") == "assert_true"
        assert boc_policy("dlcp_dc", "bo_entity") == "assert_false"
        assert boc_policy("dlcp_dc", "registered_agent") == "omit"

    def test_the_regime_records_that_this_is_not_a_fatf_bo_list(self):
        """The reporting basis has to carry the governance-interest caveat —
        it is why nonprofit boards appear in the data."""
        basis = REGIMES["dlcp_dc"].reporting_basis.lower()
        assert "governance" in basis
        assert "no role" in basis and "no percentage" in basis


# ----------------------------------------------------------------------
# Wiring
# ----------------------------------------------------------------------


class TestWiring:
    def test_the_adapter_is_registered_and_declares_its_deriver(self):
        adapter = REGISTRY["dlcp_dc"]
        assert adapter.lookup_pass_legal_name is True
        assert adapter.lookup_keys() == ("us_dc_file_number",)
        assert DLCP_RA_CODE in adapter.lookup_derivers[0].ra_codes

    def test_gleif_exposes_the_file_number_for_the_reconciler(self):
        def item(registered_as: str) -> dict[str, Any]:
            return {
                "id": "8FL3W96L346X3ZZXY355",
                "attributes": {
                    "lei": "8FL3W96L346X3ZZXY355",
                    "entity": {
                        "legalName": {"name": "AMERICAN CHEMICAL SOCIETY"},
                        "jurisdiction": "US-DC",
                        "status": "ACTIVE",
                        "registeredAs": registered_as,
                        "registeredAt": {"id": DLCP_RA_CODE},
                    },
                },
            }

        hit = GleifAdapter._entity_hit(item("000347"))
        assert hit.identifiers["us_dc_file_number"] == "000347"
        # Not a file number at all: no bridge is offered rather than a guess.
        assert "us_dc_file_number" not in GleifAdapter._entity_hit(item("!!")).identifiers

    def test_the_ra_code_maps_to_the_shared_us_dc_scheme(self):
        assert _GLEIF_RA_TO_ORG_ID[DLCP_RA_CODE][0] == "US-DC"

    def test_us_dc_is_an_expandable_register_hop(self):
        hop = hop_for("US-DC")
        assert hop is not None and hop.source_id == "dlcp_dc"

    def test_us_dc_never_stands_in_for_the_whole_united_states(self):
        """DC is one of fifty-odd US registers, so a ``REG-US`` number must not
        be looked up there."""
        assert hop_for("REG-US") is None

    def test_the_source_info_declares_the_licence_and_attribution(self):
        info = REGISTRY["dlcp_dc"].info
        assert info.license == "CC-BY-4.0"
        assert "creativecommons.org/licenses/by/4.0" in info.attribution
        assert "District of Columbia" in info.attribution
        assert info.is_national_register is True
        assert info.requires_api_key is False


# ----------------------------------------------------------------------
# The row sentence
# ----------------------------------------------------------------------


class TestFinding:
    def test_it_counts_owners_and_controllers_never_beneficial_owners(self):
        sentence = finding_dlcp_dc(_bundle())
        assert "16 owners and controllers filed" in sentence
        assert "beneficial owner" not in sentence.lower()

    def test_a_terminal_status_leads(self):
        sentence = finding_dlcp_dc(
            _bundle(
                company_key="revoked_company",
                owners_key="revoked_owners",
                trade_key="revoked_trade_names",
            )
        )
        assert sentence.startswith("Revoked")

    def test_a_name_match_is_disclosed(self):
        sentence = finding_dlcp_dc(_bundle(matched_by="name"))
        assert "matched by name" in sentence

    def test_absence_is_stated_in_the_same_voice_as_presence(self):
        bundle = _bundle()
        bundle["owners"] = []
        sentence = finding_dlcp_dc(bundle)
        assert "no owners or controllers" in sentence

    def test_a_collision_says_why_nothing_is_attached(self):
        sentence = finding_dlcp_dc({"name_mismatch": True})
        assert "different company" in sentence

    def test_a_miss_is_about_the_register_not_the_company(self):
        sentence = finding_dlcp_dc({"not_in_register": True})
        assert "No such file number" in sentence

    def test_it_asserts_no_risk_and_fits_the_cap(self):
        for bundle in (
            _bundle(),
            _bundle(matched_by="name"),
            _bundle(
                company_key="revoked_company",
                owners_key="revoked_owners",
                trade_key="revoked_trade_names",
            ),
        ):
            sentence = finding_dlcp_dc(bundle)
            assert sentence and len(sentence) <= MAX_FINDING_CHARS
            assert not any(
                word in sentence.lower()
                for word in ("risk", "sanction", "suspicious", "confirmed")
            )

    def test_a_stub_has_no_sentence(self):
        assert finding_dlcp_dc({"is_stub": True}) is None
        assert finding_dlcp_dc({}) is None


# ----------------------------------------------------------------------
# The hit row
# ----------------------------------------------------------------------


class TestHitBuilder:
    def test_it_asserts_only_the_file_number_the_row_carries(self):
        ctx = _LookupCtx(lei="8FL3W96L346X3ZZXY355", legal_name="AMERICAN CHEMICAL SOCIETY")
        hit = _bh_dlcp_dc(_bundle(), "000347", ctx)
        assert hit.identifiers == {"us_dc_file_number": "000347"}
        assert "lei" not in hit.identifiers

    def test_a_name_matched_record_asserts_the_registers_number_not_gleifs(self):
        """The whole point of the fallback: GLEIF's ``L21249`` is exactly the
        identifier this source disagrees with, so it must not be echoed back."""
        bundle = _bundle(matched_by="name")
        bundle["file_number"] = "L21249"
        ctx = _LookupCtx(lei="549300W96W2VKSMVDF81", legal_name="AMERICAN CHEMICAL SOCIETY")
        hit = _bh_dlcp_dc(bundle, "L21249", ctx)
        assert hit.identifiers == {"us_dc_file_number": "000347"}

    def test_the_summary_carries_the_scheme_and_status(self):
        ctx = _LookupCtx(lei="8FL3W96L346X3ZZXY355", legal_name="AMERICAN CHEMICAL SOCIETY")
        hit = _bh_dlcp_dc(_bundle(), "000347", ctx)
        assert hit.summary == "US-DC 000347 · Active - In Good Standing"


# ----------------------------------------------------------------------
# The schema gate
# ----------------------------------------------------------------------


class TestSchema:
    def test_a_real_bundle_validates(self):
        validate_raw("dlcp_dc", DlcpDcBundle, _bundle())

    def test_a_missing_file_number_is_a_schema_change(self):
        bundle = _bundle()
        bundle["company"] = {k: v for k, v in bundle["company"].items() if k != "FILE_NUMBER"}
        with pytest.raises(SourceSchemaError):
            validate_raw("dlcp_dc", DlcpDcBundle, bundle)

    def test_an_unknown_column_does_not_fail_a_lookup(self):
        bundle = _bundle()
        bundle["company"] = dict(bundle["company"], SOME_NEW_DLCP_COLUMN="x")
        validate_raw("dlcp_dc", DlcpDcBundle, bundle)


def test_clean_field_collapses_whitespace():
    assert clean_field("  a   b  ") == "a b"
    assert clean_field(None) == ""
