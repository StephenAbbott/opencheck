"""Tests for the Connecticut Secretary of the State adapter and mapper.

Fixtures in ``tests/data/ct_sots_live.json`` are **real SODA 2.1 responses**
captured from data.ct.gov on 2026-11-12 with the exact ``$select`` lists the
adapter sends. Keys are ``<business>/<dataset>``:

* ``eurpac`` — EURPAC SERVICE INCORPORATED (``0016116``), a 1961 stock
  corporation with eleven directors and officers.
* ``eder`` — E-G Holdings, LLC (``0926053``): renamed from Eder-Goodman, LLC
  on 1 April 2026 (the change is recorded twice), so GLEIF's older name
  agrees only with a recorded former name.
* ``riverridge`` — ``0637799``: GLEIF files 38 TOWN LINE ROAD LIMITED
  PARTNERSHIP, the register holds RIVER RIDGE APARTMENTS LIMITED PARTNERSHIP
  with no name change recorded.
* ``ubs`` — UBS AG (``0597268``), a Swiss bank registered in Connecticut.
* ``dissolved`` — GLOBAL HELP DESK SERVICES, INC. (``0697044``), dissolved
  5 November 2026, one officer listed twice at two addresses.
* ``forfeited`` — THE SHOPPES AT EASTVIEW, LLC (``0769144``), forfeited, no
  dissolution date.
* ``apella`` — APELLA CAPITAL, LLC (``1169705``), a Delaware LLC.
* ``glenstone`` — Glenstone Builders LLC (``1094606``): three companies as its
  principals and three recorded renames.
* ``missing`` — an ID the register does not hold.
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
from opencheck.bods.mapper import (
    _GLEIF_RA_TO_ORG_ID,
    ct_principal_record_id,
    gleif_registration_scheme,
    map_ct_sots,
)
from opencheck.findings import MAX_FINDING_CHARS, finding_ct_sots
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_ct_sots, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources import ct_sots as ct
from opencheck.sources.ct_sots import (
    CT_SCHEME,
    CT_SOTS_RA_CODE,
    CtSotsAdapter,
    jurisdiction_of,
    names_agree,
    normalise_business_id,
    principals_of,
    summarise,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.ct_sots import CtSotsBundle

_DATA = json.loads(
    (Path(__file__).parent / "data" / "ct_sots_live.json").read_text(encoding="utf-8")
)
_R: dict[str, Any] = _DATA["responses"]
_IDS: dict[str, str] = _DATA["account_numbers"]

_DATASET_PART = {
    ct.DATASET_MASTER: "master",
    ct.DATASET_PRINCIPALS: "principals",
    ct.DATASET_NAME_CHANGES: "name_changes",
}


def _bundle(label: str = "eurpac", **extra: Any) -> dict[str, Any]:
    """A bundle in the shape ``CtSotsAdapter.fetch`` returns for a record."""
    bundle = {
        "source_id": "ct_sots",
        "business_id": _IDS[label],
        "legal_name": "",
        "master": _R[f"{label}/master"][0],
        "principals": _R[f"{label}/principals"],
        "name_changes": _R[f"{label}/name_changes"],
        "truncated": False,
        "is_stub": False,
    }
    bundle.update(extra)
    return bundle


def _response(payload: Any, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.json.return_value = payload
    return response


def _routed_get(label: str, *, status: int = 200, failing: str | None = None) -> AsyncMock:
    async def get(url: str, params=None, headers=None):  # noqa: ANN001
        dataset = url.rsplit("/", 1)[-1].removesuffix(".json")
        if failing and dataset == failing:
            return _response({"errorCode": "query.soql.invalid"}, 400)
        if status != 200:
            return _response({}, status)
        return _response(_R.get(f"{label}/{_DATASET_PART[dataset]}", []))

    return AsyncMock(side_effect=get)


def _run_fetch(
    label: str,
    *,
    legal_name: str = "",
    business_id: str | None = None,
    status: int = 200,
    failing: str | None = None,
    token: str | None = None,
) -> tuple[dict[str, Any], list, AsyncMock, MagicMock]:
    adapter = CtSotsAdapter()
    settings = MagicMock(allow_live=True, socrata_app_token=token)
    get = _routed_get(label, status=status, failing=failing)
    put = MagicMock()
    with patch("opencheck.sources.ct_sots.get_settings", return_value=settings), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch.object(adapter._cache, "put", put), \
         patch("opencheck.sources.ct_sots.build_client") as client:
        client.return_value.__aenter__.return_value.get = get
        with degradation.recording() as recorded:
            result = asyncio.run(
                adapter.fetch(business_id or _IDS[label], legal_name=legal_name)
            )
    return result, list(recorded), get, put


# ----------------------------------------------------------------------
# Identifiers and parsers
# ----------------------------------------------------------------------


class TestNormaliseBusinessId:
    @pytest.mark.parametrize(
        "raw,expected",
        [("0016116", "0016116"), ("16116", "0016116"), (" 0597268 ", "0597268"), ("1169705", "1169705")],
    )
    def test_whitespace_goes_and_the_id_is_padded_to_seven(self, raw, expected):
        assert normalise_business_id(raw) == expected

    @pytest.mark.parametrize("raw", ["", "ABC", "12345678", "0000000", "001-6116", None])
    def test_rejects_what_cannot_be_a_business_id(self, raw):
        with pytest.raises(ValueError):
            normalise_business_id(raw)


class TestJurisdiction:
    def test_a_domestic_business_is_united_states(self):
        assert jurisdiction_of(_R["eurpac/master"][0]) == ("United States", "US")

    def test_a_swiss_bank_is_switzerland(self):
        assert jurisdiction_of(_R["ubs/master"][0]) == ("Switzerland", "CH")

    def test_de_is_delaware_never_germany(self):
        row = {"citizenship": "Foreign", "state_or_territory_formation": "DE"}
        assert jurisdiction_of(row) == ("United States", "US")

    def test_a_state_written_in_full(self):
        row = {"citizenship": "Foreign", "state_or_territory_formation": "DELAWARE"}
        assert jurisdiction_of(row) == ("United States", "US")

    def test_an_unreadable_place_is_no_jurisdiction(self):
        assert jurisdiction_of({"citizenship": "Foreign", "formation_place": "None"}) is None
        assert jurisdiction_of({"citizenship": "Foreign", "state_or_territory_formation": "QQ"}) is None


class TestPrincipals:
    def test_a_principal_listed_at_two_addresses_is_one_party(self):
        people = principals_of(_bundle("dissolved"))
        assert [p["name"] for p in people] == ["STEVE WETHERELL"]
        assert people[0]["designations"] == ["Officer"]

    def test_officer_and_director_are_both_kept(self):
        people = {p["name"]: p for p in principals_of(_bundle("ubs"))}
        assert people["Sergio P. Ermotti"]["designations"] == ["Officer", "Director"]

    def test_a_company_with_no_first_or_last_name_is_an_entity(self):
        people = principals_of(_bundle("glenstone"))
        assert {p["name"] for p in people if p["entity"]} == {
            "USBA LLC", "No Border Rentals LLC", "Renchy Private Holdings LLC",
        }

    def test_a_person_typed_only_into_the_full_name_stays_a_person(self):
        row = {"business_id": "x", "name__c": "Mehran Moghadami"}
        assert ct.principal_is_entity(row) is False


# ----------------------------------------------------------------------
# The name gate
# ----------------------------------------------------------------------


class TestNameGate:
    def test_the_current_name_agrees(self):
        assert names_agree("EURPAC SERVICE INCORPORATED", _bundle("eurpac"))

    def test_a_recorded_former_name_agrees(self):
        assert names_agree("Eder-Goodman, LLC", _bundle("eder"))

    def test_a_different_business_does_not(self):
        assert not names_agree("38 TOWN LINE ROAD LIMITED PARTNERSHIP", _bundle("riverridge"))

    def test_no_legal_name_cannot_contradict(self):
        assert names_agree("", _bundle("riverridge"))


# ----------------------------------------------------------------------
# Fetch
# ----------------------------------------------------------------------


class TestFetch:
    def test_a_record_carries_three_datasets(self):
        bundle, recorded, get, put = _run_fetch("eurpac", legal_name="EURPAC SERVICE INCORPORATED")
        assert recorded == []
        assert bundle["is_stub"] is False
        assert bundle["master"]["accountnumber"] == "0016116"
        assert len(bundle["principals"]) == 11
        assert get.await_count == 3
        put.assert_called_once()

    def test_principals_and_names_are_asked_for_by_the_record_id(self):
        _, _, get, _ = _run_fetch("eurpac", legal_name="EURPAC SERVICE INCORPORATED")
        wheres = {c.args[0].rsplit("/", 1)[-1]: c.kwargs["params"]["$where"] for c in get.await_args_list}
        assert wheres[f"{ct.DATASET_MASTER}.json"] == "accountnumber='0016116'"
        assert wheres[f"{ct.DATASET_PRINCIPALS}.json"] == "business_id='001t000000WmSc6AAF'"
        assert wheres[f"{ct.DATASET_NAME_CHANGES}.json"] == "unique_key='001t000000WmSc6AAF'"

    def test_no_residence_address_or_email_is_ever_asked_for(self):
        _, _, get, _ = _run_fetch("eurpac", legal_name="EURPAC SERVICE INCORPORATED")
        for call in get.await_args_list:
            select = call.kwargs["params"]["$select"]
            assert "residence" not in select
            assert "email" not in select
            assert "street_address" not in select

    def test_the_app_token_goes_in_a_header_never_the_url(self):
        _, _, get, _ = _run_fetch("eurpac", legal_name="EURPAC SERVICE INCORPORATED", token="tok-9")
        for call in get.await_args_list:
            assert call.kwargs["headers"]["X-App-Token"] == "tok-9"
            assert "tok-9" not in json.dumps(call.kwargs["params"])

    def test_a_renamed_business_passes_the_gate(self):
        bundle, _, _, _ = _run_fetch("eder", legal_name="Eder-Goodman, LLC")
        assert bundle["is_stub"] is False
        assert "name_mismatch" not in bundle

    def test_a_wrong_number_is_a_note_card(self):
        bundle, recorded, _, _ = _run_fetch(
            "riverridge", legal_name="38 TOWN LINE ROAD LIMITED PARTNERSHIP"
        )
        assert recorded == []
        assert bundle["name_mismatch"] is True
        assert bundle["not_found"] is True
        assert bundle["is_stub"] is False
        assert bundle["master"] is None and bundle["principals"] == []
        assert bundle["registered_name"] == "RIVER RIDGE APARTMENTS LIMITED PARTNERSHIP"
        assert bundle["coverage_note"] == ct.COVERAGE_NAME_MISMATCH

    def test_an_id_the_register_does_not_hold(self):
        bundle, recorded, get, put = _run_fetch("missing", legal_name="Nothing LLC")
        assert recorded == []
        assert bundle["not_in_register"] is True
        assert bundle["is_stub"] is False
        assert get.await_count == 1  # no record id, so nothing else to ask
        put.assert_called_once()  # "no such business" is an answer

    def test_a_malformed_id_never_reaches_the_network(self):
        bundle, _, get, _ = _run_fetch("eurpac", business_id="ABC")
        assert get.await_count == 0
        assert bundle["is_stub"] is True

    def test_a_failed_read_is_a_degradation_and_never_cached(self):
        bundle, recorded, _, put = _run_fetch(
            "eurpac", legal_name="EURPAC SERVICE INCORPORATED", failing=ct.DATASET_PRINCIPALS
        )
        assert bundle["is_stub"] is True
        assert len(recorded) == 1
        put.assert_not_called()

    def test_a_429_is_rate_limited(self):
        bundle, recorded, _, put = _run_fetch("eurpac", status=429)
        assert bundle["is_stub"] is True
        assert recorded[0].reason == degradation.REASON_RATE_LIMITED
        put.assert_not_called()


class TestSchema:
    def test_a_residence_column_is_refused(self):
        bundle = _bundle("eurpac")
        bundle["principals"] = [dict(bundle["principals"][0], residence_city="Wilton")]
        with pytest.raises(SourceSchemaError):
            validate_raw("ct_sots", CtSotsBundle, bundle)

    def test_the_business_email_is_refused(self):
        bundle = _bundle("eurpac")
        bundle["master"] = dict(bundle["master"], business_email_address="x@example.com")
        with pytest.raises(SourceSchemaError):
            validate_raw("ct_sots", CtSotsBundle, bundle)


# ----------------------------------------------------------------------
# Mapper
# ----------------------------------------------------------------------


def _by_type(statements: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == kind]


class TestMapper:
    def test_the_business_carries_its_id_as_us_ct(self):
        stmts = list(map_ct_sots(_bundle("eurpac")))
        entity = _by_type(stmts, "entity")[0]["recordDetails"]
        assert entity["identifiers"] == [
            {"id": "0016116", "scheme": CT_SCHEME, "schemeName": ct.CT_SCHEME_NAME}
        ]
        assert entity["foundingDate"] == "1961-09-19"
        assert entity["jurisdiction"] == {"name": "United States", "code": "US"}

    def test_directors_are_board_members_and_officers_senior_officials(self):
        stmts = list(map_ct_sots(_bundle("ubs")))
        rels = _by_type(stmts, "relationship")
        kinds = sorted(tuple(i["type"] for i in r["recordDetails"]["interests"]) for r in rels)
        assert kinds.count(("boardMember",)) == 10
        assert kinds.count(("seniorManagingOfficial",)) == 1
        assert kinds.count(("seniorManagingOfficial", "boardMember")) == 2

    def test_no_principal_is_ever_an_owner(self):
        for label in ("eurpac", "eder", "ubs", "glenstone", "apella", "forfeited"):
            for rel in _by_type(list(map_ct_sots(_bundle(label))), "relationship"):
                for interest in rel["recordDetails"]["interests"]:
                    assert interest["type"] in {"boardMember", "seniorManagingOfficial"}
                    assert "beneficialOwnershipOrControl" not in interest
                    assert "share" not in interest

    def test_an_undesignated_principal_says_the_register_does_not_say_which(self):
        rel = _by_type(list(map_ct_sots(_bundle("apella"))), "relationship")[0]
        interest = rel["recordDetails"]["interests"][0]
        assert interest["type"] == "seniorManagingOfficial"
        assert "member, manager or partner" in interest["details"]

    def test_a_company_principal_is_an_entity_with_no_jurisdiction(self):
        stmts = list(map_ct_sots(_bundle("glenstone")))
        parties = [s for s in _by_type(stmts, "entity") if s["recordDetails"]["name"] == "USBA LLC"]
        assert len(parties) == 1
        assert "jurisdiction" not in parties[0]["recordDetails"]
        assert not parties[0]["recordDetails"].get("identifiers")

    def test_people_carry_name_and_role_only(self):
        stmts = list(map_ct_sots(_bundle("eurpac")))
        for person in _by_type(stmts, "person"):
            assert not person["recordDetails"].get("addresses")
        ids = {s["recordId"] for s in _by_type(stmts, "person")}
        assert ct_principal_record_id("0016116", "FREDERICK STOHR") in ids

    def test_former_names_are_alternate_names(self):
        entity = _by_type(list(map_ct_sots(_bundle("glenstone"))), "entity")[0]["recordDetails"]
        assert entity["alternateNames"] == [
            "JUNKIN' IT, LLC", "GLENSTONE CONSTRUCTION LLC", "Glenstone Home Builders LLC",
        ]

    def test_a_dissolved_business_is_terminal_and_dated(self):
        entity = _by_type(list(map_ct_sots(_bundle("dissolved"))), "entity")[0]
        status = liveness.read_register_status(entity)
        assert status["liveness"] == liveness.TERMINAL
        assert entity["recordDetails"]["dissolutionDate"] == "2026-11-05"

    def test_a_forfeited_business_is_terminal_with_no_invented_date(self):
        entity = _by_type(list(map_ct_sots(_bundle("forfeited"))), "entity")[0]
        assert liveness.read_register_status(entity)["liveness"] == liveness.TERMINAL
        assert "dissolutionDate" not in entity["recordDetails"]

    def test_a_foreign_business_has_no_founding_date_from_its_ct_registration(self):
        entity = _by_type(list(map_ct_sots(_bundle("ubs"))), "entity")[0]["recordDetails"]
        assert "foundingDate" not in entity
        assert entity["jurisdiction"]["code"] == "CH"

    def test_a_foreign_withdrawal_is_never_terminal(self):
        bundle = _bundle("apella")
        bundle["master"] = dict(bundle["master"], status="Withdrawn")
        entity = _by_type(list(map_ct_sots(bundle)), "entity")[0]
        status = liveness.read_register_status(entity)
        assert status is None or status["liveness"] != liveness.TERMINAL
        assert "dissolutionDate" not in entity["recordDetails"]

    def test_a_note_card_maps_to_nothing(self):
        assert list(map_ct_sots({"is_stub": False, "not_found": True, "master": None})) == []


# ----------------------------------------------------------------------
# Finding and hit
# ----------------------------------------------------------------------


class TestFinding:
    def test_a_corporation(self):
        assert finding_ct_sots(_bundle("eurpac")) == (
            "Active in Connecticut, registered 1961-09-19, 11 officers and directors on file."
        )

    def test_members_are_never_called_owners(self):
        text = finding_ct_sots(_bundle("glenstone"))
        assert "3 members, managers or partners on file" in text
        assert "owner" not in text.lower()

    def test_a_dissolved_business_leads_with_it(self):
        assert finding_ct_sots(_bundle("dissolved")).startswith(
            "Dissolved at the Connecticut Secretary of the State on 2026-11-05"
        )

    def test_a_foreign_withdrawal(self):
        bundle = _bundle("apella")
        bundle["master"] = dict(bundle["master"], status="Withdrawn")
        assert finding_ct_sots(bundle).startswith("No longer registered in Connecticut (withdrawn)")

    def test_note_cards_have_their_own_sentence(self):
        assert "different business" in finding_ct_sots({"name_mismatch": True})
        assert "No business with this ID" in finding_ct_sots({"not_in_register": True})

    @pytest.mark.parametrize("label", sorted(k for k in _IDS if k not in {"missing"}))
    def test_every_sentence_fits(self, label):
        text = finding_ct_sots(_bundle(label))
        assert text and len(text) <= MAX_FINDING_CHARS


class TestHit:
    def test_a_record_asserts_the_business_id_never_the_lei(self):
        hit = _bh_ct_sots(_bundle("eurpac"), "0016116", _LookupCtx(lei="984500D8C63E065B8705"))
        assert hit.identifiers == {"us_ct_business_id": "0016116"}
        assert hit.summary == "US-CT 0016116 · Active"

    def test_a_note_card_asserts_nothing(self):
        bundle = {"not_found": True, "name_mismatch": True, "coverage_note": ct.COVERAGE_NAME_MISMATCH}
        hit = _bh_ct_sots(bundle, "0637799", _LookupCtx(lei="X", legal_name="38 TOWN LINE"))
        assert hit.identifiers == {}
        assert hit.raw["not_found"] is True


# ----------------------------------------------------------------------
# Wiring
# ----------------------------------------------------------------------


class TestWiring:
    def test_registered(self):
        assert isinstance(REGISTRY["ct_sots"], CtSotsAdapter)
        assert REGISTRY["ct_sots"].info.license == "CT-Open-Data"

    def test_the_deriver_keys_on_the_ra_code(self):
        (deriver,) = CtSotsAdapter.lookup_derivers
        assert deriver.ra_codes == frozenset({CT_SOTS_RA_CODE})

    def test_gleif_writes_the_same_scheme(self):
        assert _GLEIF_RA_TO_ORG_ID[CT_SOTS_RA_CODE][0] == CT_SCHEME
        # A foreign business registered in CT keeps the CT scheme.
        assert gleif_registration_scheme(CT_SOTS_RA_CODE, "CH", None, "0597268")[0] == CT_SCHEME

    def test_us_ct_is_a_register_hop_without_a_country_alias(self):
        hop = hop_for(CT_SCHEME)
        assert hop is not None and hop.source_id == "ct_sots"
        assert hop_for("REG-US") is None or hop_for("REG-US").source_id != "ct_sots"
