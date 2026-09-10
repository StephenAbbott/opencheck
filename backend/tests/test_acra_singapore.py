"""Tests for the Singapore ACRA adapter (data.gov.sg) and its mapper.

Fixtures in ``tests/data/acra_singapore_live.json`` are **real responses**
captured from data.gov.sg on 2026-09-10 (see its ``_captured`` note):

* ``collection_1_metadata`` / ``collection_2_metadata`` — the two collection
  metadata responses (``?withDatasetMetadata=true``), trimmed to each
  dataset's id and name.
* ``dbs_entity`` / ``dbs_detail`` — DBS Bank Ltd., a public company with one
  former name, from the ACRA dataset and the collection-2 'D' file.
* ``kibu_entity`` / ``kibu_detail`` — ``201532108K``, which GLEIF still calls
  FRINSA SINGAPORE PTE.LTD. and ACRA renamed KIBU PTE. LTD.: the routing and
  former-name case.
* ``vcc_acra_miss`` / ``vcc_other_agencies`` — K-Wave Fund VCC, absent from the
  ACRA dataset and present in the other-agencies one.
* ``rhb_branch_detail`` — a foreign company branch.
* ``struck_off_detail``, ``gazetted_detail``, ``liquidation_detail`` — one
  local company in each of three statuses.
* ``digit_name_detail`` — 3M SINGAPORE PTE. LTD., filed under 'Others'.
* ``_rate_limited_http_429`` — the 429 body (there is no Retry-After header).
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
from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, _acra_liveness, map_acra_singapore
from opencheck.findings import MAX_FINDING_CHARS, finding_acra_singapore
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_acra_singapore, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources import acra_singapore as acra
from opencheck.sources.acra_singapore import (
    ACRA_DATASET_NAME,
    ACRA_RA_CODE,
    OTHER_AGENCIES_DATASET_NAME,
    SG_UEN_SCHEME,
    AcraSingaporeAdapter,
    clean_field,
    detail_file_for,
    former_names,
    names_agree,
    normalise_uen,
)
from opencheck.sources.base import SearchKind
from opencheck.sources.gleif import GleifAdapter
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.acra_singapore import AcraSingaporeBundle

_FIXTURES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "data" / "acra_singapore_live.json").read_text(encoding="utf-8")
)["responses"]


def _fixture(key: str) -> Any:
    return json.loads(json.dumps(_FIXTURES[key]))


def _row(key: str) -> dict[str, Any]:
    return _fixture(key)["result"]["records"][0]


def _dataset_ids(collection: str) -> dict[str, str]:
    return {
        d["name"]: d["datasetId"]
        for d in _FIXTURES[f"collection_{collection}_metadata"]["data"]["datasetMetadata"]
    }


_C1 = _dataset_ids("1")
_C2 = {name.split("('")[1].rstrip("')"): rid for name, rid in _dataset_ids("2").items()}
_ACRA_ID = _C1[ACRA_DATASET_NAME]
_OTHER_ID = _C1[OTHER_AGENCIES_DATASET_NAME]


def _bundle(entity_key: str, detail_key: str | None = None, **overrides: Any) -> dict[str, Any]:
    entity = _row(entity_key)
    detail = _row(detail_key) if detail_key else None
    return {
        "source_id": "acra_singapore",
        "uen": entity["uen"],
        "entity": entity,
        "detail": detail,
        "dataset": "acra",
        "record_resource_id": _C2["D"] if detail_key else _ACRA_ID,
        "legal_name": "",
        "is_stub": False,
        **overrides,
    }


def _detail_only_bundle(detail_key: str) -> dict[str, Any]:
    """A bundle whose collection-1 row is derived from a collection-2 row —
    for status fixtures, where only the detail row was captured."""
    detail = _row(detail_key)
    entity = {"uen": detail["uen"], "entity_name": detail["entity_name"]}
    return {
        "source_id": "acra_singapore",
        "uen": detail["uen"],
        "entity": entity,
        "detail": detail,
        "dataset": "acra",
        "legal_name": "",
        "is_stub": False,
    }


@pytest.fixture(autouse=True)
def _fresh_metadata_and_no_pacing():
    """Dataset ids are cached per process, and the real token bucket would
    space test calls seconds apart."""

    class _NoBucket:
        async def acquire(self) -> float:
            return 0.0

    acra._METADATA.clear()
    with patch.object(acra, "_bucket", lambda keyed: _NoBucket()):
        yield
    acra._METADATA.clear()


# ---------------------------------------------------------------------------
# The identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("196800306E", "196800306E"),     # local company (10 characters)
        ("53250767c", "53250767C"),       # business (9 characters), lower case
        (" T23VC0219C ", "T23VC0219C"),   # VCC
        ("S99FC5710J", "S99FC5710J"),     # foreign company branch
        ("T21LP 0078G", "T21LP0078G"),    # limited partnership, stray space
    ],
)
def test_normalise_uen(raw: str, expected: str) -> None:
    assert normalise_uen(raw) == expected


def test_a_vcc_sub_fund_is_rejected_by_name() -> None:
    with pytest.raises(ValueError, match="sub-fund"):
        normalise_uen("T21VC0144D-SF001")


@pytest.mark.parametrize("bad", ["", "12345", "ABC-D-123456-1234", "CHE123456789", "19680030E6"])
def test_normalise_uen_rejects_what_is_not_a_uen(bad: str) -> None:
    with pytest.raises(ValueError):
        normalise_uen(bad)


def test_ra_code_and_deriver() -> None:
    (deriver,) = AcraSingaporeAdapter.lookup_derivers
    assert deriver.ra_codes == frozenset({ACRA_RA_CODE}) == frozenset({"RA000523"})
    assert deriver.derived_key == "sg_uen"
    assert AcraSingaporeAdapter.lookup_pass_legal_name is True
    assert AcraSingaporeAdapter.lookup_timeout_s <= 120


def test_ra_code_maps_to_the_sg_acra_scheme() -> None:
    assert _GLEIF_RA_TO_ORG_ID[ACRA_RA_CODE][0] == SG_UEN_SCHEME == "SG-ACRA"


def test_sg_acra_is_a_register_hop() -> None:
    """FullCheck can expand a Singapore node that carries a UEN and no LEI."""
    hop = hop_for("SG-ACRA")
    assert hop is not None and hop.source_id == "acra_singapore"
    assert hop.normalise("196800306e") == "196800306E"
    assert hop.pass_legal_name is True


def test_gleif_hit_bridges_on_the_uen_but_not_on_a_sub_fund() -> None:
    def item(registered_as: str) -> dict[str, Any]:
        return {
            "id": "ATUEL7OJR5057F2PV266",
            "attributes": {
                "lei": "ATUEL7OJR5057F2PV266",
                "entity": {
                    "legalName": {"name": "DBS BANK LTD."},
                    "jurisdiction": "SG",
                    "status": "ACTIVE",
                    "registeredAs": registered_as,
                    "registeredAt": {"id": "RA000523"},
                },
            },
        }

    assert GleifAdapter._entity_hit(item("196800306E")).identifiers["sg_uen"] == "196800306E"
    assert "sg_uen" not in GleifAdapter._entity_hit(item("T21VC0144D-SF001")).identifiers


def test_info() -> None:
    info = REGISTRY["acra_singapore"].info
    assert info.is_national_register is True
    assert info.country == "SG"
    assert info.requires_api_key is False
    assert info.license == "Singapore-OGL-1.0"
    # The Singapore Open Data Licence's required attribution statement.
    assert info.attribution.startswith("Contains information from ")
    assert "Singapore Open Data Licence version 1.0" in info.attribution
    assert "https://data.gov.sg/open-data-licence" in info.attribution


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------


def test_the_literal_na_is_read_as_empty() -> None:
    assert clean_field("na") == ""
    assert clean_field(" NA ") == ""
    assert clean_field("  MARINA   BOULEVARD ") == "MARINA BOULEVARD"
    assert clean_field(None) == ""


@pytest.mark.parametrize(
    ("name", "file"),
    [
        ("KIBU PTE. LTD.", "K"),
        ("kibu pte. ltd.", "K"),
        ("3M SINGAPORE PTE. LTD.", "Others"),
        ("'Z' LYRA WEDDING SERVICES", "Others"),
        ("@ ZONE COMMUNICATIONS", "Others"),
        ("", "Others"),
        (None, "Others"),
    ],
)
def test_detail_file_for(name: str | None, file: str) -> None:
    assert detail_file_for(name) == file


def test_the_digit_named_fixture_really_is_in_others() -> None:
    assert detail_file_for(_row("digit_name_detail")["entity_name"]) == "Others"


def test_former_names_in_order_without_na_or_repeats() -> None:
    assert former_names(_row("dbs_detail")) == ["THE DEVELOPMENT BANK OF SINGAPORE"]
    assert former_names({"former_entity_name1": "A", "former_entity_name2": "na", "former_entity_name3": "A", "former_entity_name4": "B"}) == ["A", "B"]
    assert former_names(None) == []


def test_names_agree_through_a_former_name() -> None:
    """GLEIF's name is ACRA's former name for 201532108K."""
    entity, detail = _row("kibu_entity"), _row("kibu_detail")
    assert names_agree("FRINSA SINGAPORE PTE.LTD.", entity, detail) is True


def test_without_the_former_names_the_renamed_company_does_not_agree() -> None:
    assert names_agree("FRINSA SINGAPORE PTE.LTD.", _row("kibu_entity")) is False


def test_names_agree_on_the_same_name_written_differently() -> None:
    assert names_agree("DBS Bank Ltd", _row("dbs_entity")) is True


def test_a_legal_name_with_no_latin_letters_cannot_contradict() -> None:
    """Seen live: GLEIF's legal name for 202309119M is 经典商品有限公司; ACRA's
    is CLASSIC COMMODITIES PTE. LTD. A translation is not a mismatch."""
    assert names_agree("经典商品有限公司", {"entity_name": "CLASSIC COMMODITIES PTE. LTD."}) is True


def test_a_cyrillic_legal_name_is_still_compared() -> None:
    assert names_agree("ГАЗПРОМ", _row("dbs_entity")) is False


def test_an_unrelated_name_disagrees() -> None:
    assert names_agree("SINGAPORE AIRLINES LIMITED", _row("dbs_entity"), _row("dbs_detail")) is False


def test_an_empty_legal_name_cannot_contradict() -> None:
    assert names_agree("", _row("dbs_entity")) is True


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("entity_key", "detail_key"),
    [("dbs_entity", "dbs_detail"), ("kibu_entity", "kibu_detail"), ("vcc_other_agencies", None)],
)
def test_schema_accepts_live_payloads(entity_key: str, detail_key: str | None) -> None:
    validate_raw("acra_singapore", AcraSingaporeBundle, _bundle(entity_key, detail_key))


def test_schema_requires_uen() -> None:
    with pytest.raises(SourceSchemaError):
        validate_raw("acra_singapore", AcraSingaporeBundle, {"entity": {"entity_name": "X"}})


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------


def _one(bundle: dict[str, Any]) -> dict[str, Any]:
    statements = list(map_acra_singapore(bundle))
    assert len(statements) == 1
    return statements[0]


def test_maps_dbs_to_one_entity_statement() -> None:
    stmt = _one(_bundle("dbs_entity", "dbs_detail"))
    rd = stmt["recordDetails"]
    assert stmt["recordType"] == "entity"
    assert rd["name"] == "DBS BANK LTD."
    assert rd["identifiers"] == [
        {"id": "196800306E", "scheme": "SG-ACRA", "schemeName": acra.SG_UEN_SCHEME_NAME}
    ]
    assert rd["jurisdiction"] == {"name": "Singapore", "code": "SG"}
    assert rd["foundingDate"] == "1968-07-16"
    assert rd["alternateNames"] == ["THE DEVELOPMENT BANK OF SINGAPORE"]
    assert rd["addresses"] == [
        {
            "type": "registered",
            "address": "12 MARINA BOULEVARD, MARINA BAY FINANCIAL CENTRE, SINGAPORE 018982",
            "country": {"name": "Singapore", "code": "SG"},
        }
    ]
    assert stmt["source"]["url"].startswith("https://data.gov.sg/api/action/datastore_search?")
    assert stmt["source"]["type"] == ["officialRegister"]


def test_the_register_type_goes_to_details_never_subtype() -> None:
    """entityType.subtype is a closed codelist in BODS 0.4."""
    rd = _one(_bundle("dbs_entity", "dbs_detail"))["recordDetails"]
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Public Company Limited by Shares"}


@pytest.mark.parametrize(
    "bundle",
    [
        _bundle("dbs_entity", "dbs_detail"),
        _bundle("kibu_entity", "kibu_detail"),
        _bundle("vcc_other_agencies", dataset="other_agencies"),
        _detail_only_bundle("rhb_branch_detail"),
        _detail_only_bundle("struck_off_detail"),
        _detail_only_bundle("gazetted_detail"),
        _detail_only_bundle("liquidation_detail"),
        _detail_only_bundle("digit_name_detail"),
    ],
)
def test_no_statement_ever_carries_a_subtype(bundle: dict[str, Any]) -> None:
    for stmt in map_acra_singapore(bundle):
        assert "subtype" not in stmt["recordDetails"]["entityType"]


def test_renamed_company_keeps_its_current_name_and_lists_the_former() -> None:
    rd = _one(_bundle("kibu_entity", "kibu_detail"))["recordDetails"]
    assert rd["name"] == "KIBU PTE. LTD."
    assert rd["alternateNames"] == ["FRINSA SINGAPORE"]
    assert rd["addresses"][0]["address"] == "144 ROBINSON ROAD, #14-01, ROBINSON SQUARE, SINGAPORE 068908"


def test_a_vcc_maps_from_the_uen_row_alone() -> None:
    rd = _one(_bundle("vcc_other_agencies", dataset="other_agencies"))["recordDetails"]
    assert rd["name"] == "K-WAVE FUND VCC"
    assert rd["entityType"]["details"] == "Variable Capital Companies"
    assert rd["addresses"][0]["address"] == "BATTERY ROAD, SINGAPORE 049907"
    # The UEN issue date is not promoted to a founding date.
    assert "foundingDate" not in rd
    assert "alternateNames" not in rd or rd["alternateNames"] == []


@pytest.mark.parametrize(
    ("bundle", "expected", "raw"),
    [
        (_bundle("dbs_entity", "dbs_detail"), liveness.LIVE, "Live Company"),
        (_bundle("vcc_other_agencies", dataset="other_agencies"), liveness.LIVE, "Registered"),
        (_detail_only_bundle("struck_off_detail"), liveness.TERMINAL, "Struck Off"),
        (_detail_only_bundle("gazetted_detail"), liveness.PENDING, "Gazetted To Be Struck Off"),
        (
            _detail_only_bundle("liquidation_detail"),
            liveness.PENDING,
            "In Liquidation - Members voluntary winding up",
        ),
    ],
)
def test_register_status_is_classified_from_the_live_labels(
    bundle: dict[str, Any], expected: str, raw: str
) -> None:
    stmt = _one(bundle)
    status = liveness.read_register_status(stmt)
    assert status is not None and status["liveness"] == expected
    assert status["raw"] == raw
    # ACRA publishes no status date, so nothing is ever dissolved *on* a date.
    assert "dissolutionDate" not in stmt["recordDetails"]


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("Live Company", liveness.LIVE),
        ("Live", liveness.LIVE),
        ("Registered", liveness.LIVE),
        ("Deregistered", liveness.TERMINAL),
        ("Terminated", liveness.TERMINAL),
        ("Cancelled (Non-Renewal)", liveness.TERMINAL),
        ("Converted To LLP", liveness.TERMINAL),
        ("Dissolved - Pursuant to Section 212(1)(D) Of The Companies Act", liveness.TERMINAL),
        ("Struck Off (Early Dissolution - simplified winding up)", liveness.TERMINAL),
        ("Gazetted To Be Struck Off", liveness.PENDING),
        ("Cancellation In Progress", liveness.PENDING),
        ("In Liquidation - Simplified Winding Up", liveness.PENDING),
        ("Live (Receiver or Receiver and Manager appointed)", liveness.PENDING),
        ("Something ACRA has never said", liveness.UNKNOWN),
        ("", liveness.UNKNOWN),
    ],
)
def test_acra_liveness(label: str, expected: str) -> None:
    assert _acra_liveness(label) == expected


def test_an_unknown_status_writes_no_annotation() -> None:
    bundle = _bundle("dbs_entity", "dbs_detail")
    bundle["detail"]["entity_status_description"] = "Something new"
    bundle["entity"]["uen_status_desc"] = "na"
    assert liveness.read_register_status(_one(bundle)) is None


def test_never_asserts_people_or_relationships() -> None:
    for key in ("dbs_detail", "struck_off_detail", "digit_name_detail"):
        for stmt in map_acra_singapore(_detail_only_bundle(key)):
            assert stmt["recordType"] == "entity"


@pytest.mark.parametrize(
    "bundle",
    [
        {},
        {"is_stub": True, "uen": "196800306E"},
        {"uen": "196800306E", "entity": {"uen": "196800306E"}, "detail": None},
    ],
)
def test_mapper_is_defensive(bundle: dict[str, Any]) -> None:
    assert list(map_acra_singapore(bundle)) == []


# ---------------------------------------------------------------------------
# Finding
# ---------------------------------------------------------------------------


def test_finding_dbs() -> None:
    assert finding_acra_singapore(_bundle("dbs_entity", "dbs_detail")) == (
        "Live public company limited by shares, incorporated 16 July 1968, "
        "formerly THE DEVELOPMENT BANK OF SINGAPORE."
    )


def test_finding_renamed_company() -> None:
    assert finding_acra_singapore(_bundle("kibu_entity", "kibu_detail")) == (
        "Live private company limited by shares, incorporated 18 August 2015, "
        "formerly FRINSA SINGAPORE."
    )


def test_finding_vcc_says_uen_issued_not_incorporated() -> None:
    assert finding_acra_singapore(_bundle("vcc_other_agencies", dataset="other_agencies")) == (
        "Live variable capital company, UEN issued 23 November 2023."
    )


def test_finding_leads_with_a_non_live_status_verbatim() -> None:
    sentence = finding_acra_singapore(_detail_only_bundle("struck_off_detail"))
    assert sentence is not None and sentence.startswith("Struck off, ")


def test_finding_counts_several_former_names_rather_than_ordering_them() -> None:
    bundle = _bundle("dbs_entity", "dbs_detail")
    bundle["detail"]["former_entity_name2"] = "DBS LIMITED"
    sentence = finding_acra_singapore(bundle)
    assert sentence is not None and "2 former names on file" in sentence
    assert "formerly" not in sentence


@pytest.mark.parametrize(
    "key", ["dbs_detail", "struck_off_detail", "gazetted_detail", "liquidation_detail", "rhb_branch_detail", "digit_name_detail"]
)
def test_finding_fits_the_cap_and_never_mentions_absent_people(key: str) -> None:
    sentence = finding_acra_singapore(_detail_only_bundle(key))
    assert sentence is not None and len(sentence) <= MAX_FINDING_CHARS
    for word in ("officer", "owner", "shareholder", "director"):
        assert word not in sentence.lower()


def test_finding_is_none_for_a_stub() -> None:
    assert finding_acra_singapore({"is_stub": True, "uen": "196800306E"}) is None


# ---------------------------------------------------------------------------
# Hit builder
# ---------------------------------------------------------------------------


def test_hit_builder_asserts_only_the_uen_the_register_returned() -> None:
    hit = _bh_acra_singapore(
        _bundle("dbs_entity", "dbs_detail"), "196800306E", _LookupCtx(lei="ATUEL7OJR5057F2PV266")
    )
    assert hit.identifiers == {"sg_uen": "196800306E"}
    assert hit.summary == "SG-ACRA 196800306E · Live Company"
    assert hit.name == "DBS BANK LTD."
    assert hit.finding is not None and hit.finding.startswith("Live public company")


# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------


def _response(status: int, payload: Any) -> MagicMock:
    response = MagicMock(status_code=status, is_success=200 <= status < 300)
    response.json.return_value = payload
    return response


def _miss(resource_id: str) -> dict[str, Any]:
    return {"success": True, "result": {"resource_id": resource_id, "records": [], "total": 0}}


class _Upstream:
    """Routes GETs the way data.gov.sg would, from the captured fixtures.

    ``rows`` maps a dataset id to the datastore payload for the UEN under test
    (anything unlisted is a clean miss); ``overrides`` maps a dataset id or a
    collection id to a list of responses served in order."""

    def __init__(
        self,
        rows: dict[str, Any] | None = None,
        overrides: dict[str, list[MagicMock]] | None = None,
    ) -> None:
        self.rows = rows or {}
        self.overrides = {k: list(v) for k, v in (overrides or {}).items()}
        self.calls: list[tuple[str, dict[str, str], dict[str, str]]] = []

    async def get(self, url: str, params: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> MagicMock:
        params = params or {}
        self.calls.append((url, params, headers or {}))
        if "/collections/" in url:
            collection = url.split("/collections/")[1].split("/")[0]
            if self.overrides.get(f"collection:{collection}"):
                return self.overrides[f"collection:{collection}"].pop(0)
            return _response(200, _fixture(f"collection_{collection}_metadata"))
        resource_id = params["resource_id"]
        if self.overrides.get(resource_id):
            return self.overrides[resource_id].pop(0)
        return _response(200, self.rows.get(resource_id) or _miss(resource_id))

    def queried(self) -> list[str]:
        return [p.get("resource_id", "metadata") for _, p, _ in self.calls]


def _run_fetch(
    upstream: _Upstream,
    *,
    uen: str,
    legal_name: str = "",
    api_key: str | None = None,
    cached: Any = None,
    budget_scope: bool = False,
) -> tuple[dict[str, Any], list, MagicMock]:
    adapter = AcraSingaporeAdapter()
    settings = MagicMock(allow_live=True, data_gov_sg_api_key=api_key)
    put = MagicMock()
    with patch("opencheck.sources.acra_singapore.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=cached is not None), \
         patch.object(adapter._cache, "get_payload", return_value=cached), \
         patch.object(adapter._cache, "put", put), \
         patch("opencheck.sources.acra_singapore.asyncio.sleep", AsyncMock()), \
         patch("opencheck.sources.acra_singapore.build_client") as client:
        client.return_value.__aenter__.return_value.get = upstream.get
        with degradation.recording() as recorded:
            if budget_scope:
                with outbound_rate.budget_scope():
                    result = asyncio.run(adapter.fetch(uen, legal_name=legal_name))
            else:
                result = asyncio.run(adapter.fetch(uen, legal_name=legal_name))
    return result, list(recorded), put


def test_fetch_returns_both_rows_for_a_local_company() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")})
    result, recorded, put = _run_fetch(upstream, uen="196800306E", legal_name="DBS BANK LTD.")
    assert result["is_stub"] is False
    assert result["entity"]["uen"] == result["detail"]["uen"] == "196800306E"
    assert result["dataset"] == "acra"
    assert result["record_resource_id"] == _C2["D"]
    assert recorded == []
    assert upstream.queried() == ["metadata", _ACRA_ID, "metadata", _C2["D"]]
    # Exact-key filter, never a text query.
    datastore_params = [p for _, p, _ in upstream.calls if "resource_id" in p]
    assert all(p["filters"] == '{"uen":"196800306E"}' and "q" not in p for p in datastore_params)
    put.assert_called_once()


def test_the_api_key_is_sent_as_x_api_key_when_configured() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")})
    _run_fetch(upstream, uen="196800306E", api_key="v2:test-key")
    assert all(h == {"x-api-key": "v2:test-key"} for _, _, h in upstream.calls)


def test_no_key_sends_no_header() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")})
    _run_fetch(upstream, uen="196800306E")
    assert all(h == {} for _, _, h in upstream.calls)


def test_detail_is_routed_by_the_registers_name_not_gleifs() -> None:
    """GLEIF says FRINSA (the 'F' file); ACRA's current name is KIBU ('K')."""
    upstream = _Upstream({_ACRA_ID: _fixture("kibu_entity"), _C2["K"]: _fixture("kibu_detail")})
    result, recorded, _ = _run_fetch(upstream, uen="201532108K", legal_name="FRINSA SINGAPORE PTE.LTD.")
    assert _C2["F"] not in upstream.queried()
    assert result["detail"]["entity_name"] == "KIBU PTE. LTD."
    assert result.get("name_mismatch") is None
    assert recorded == []


def test_a_digit_name_is_looked_up_in_others() -> None:
    row = _fixture("digit_name_detail")
    uen = row["result"]["records"][0]["uen"]
    entity = {"success": True, "result": {"records": [{"uen": uen, "entity_name": "3M SINGAPORE PTE. LTD.", "issuance_agency_desc": "ACRA", "uen_status_desc": "Registered"}]}}
    upstream = _Upstream({_ACRA_ID: entity, _C2["Others"]: row})
    result, _, _ = _run_fetch(upstream, uen=uen)
    assert upstream.queried()[-1] == _C2["Others"]
    assert result["detail"]["entity_name"] == "3M SINGAPORE PTE. LTD."


def test_a_miss_in_the_letter_file_retries_others() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("dbs_entity")})
    result, recorded, put = _run_fetch(upstream, uen="196800306E")
    assert upstream.queried()[-2:] == [_C2["D"], _C2["Others"]]
    assert result["entity"] is not None and result["detail"] is None
    assert recorded == []
    put.assert_called_once()  # a clean answer, cached


def test_a_vcc_comes_from_the_other_agencies_dataset_with_no_detail_lookup() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("vcc_acra_miss"), _OTHER_ID: _fixture("vcc_other_agencies")})
    result, recorded, _ = _run_fetch(upstream, uen="T23VC0219C", legal_name="K-Wave Fund VCC")
    assert result["dataset"] == "other_agencies"
    assert result["entity"]["entity_name"] == "K-WAVE FUND VCC"
    assert result["detail"] is None
    assert upstream.queried() == ["metadata", _ACRA_ID, _OTHER_ID]
    assert recorded == []


def test_an_other_agencies_row_not_issued_by_acra_is_not_accepted() -> None:
    row = _fixture("vcc_other_agencies")
    row["result"]["records"][0]["issuance_agency_desc"] = "Registry of Societies"
    upstream = _Upstream({_OTHER_ID: row})
    result, recorded, _ = _run_fetch(upstream, uen="T23VC0219C")
    assert result["is_stub"] is True and result["not_in_register"] is True
    assert recorded == []


def test_a_clean_miss_everywhere_is_not_in_register_and_not_a_degradation() -> None:
    upstream = _Upstream()
    result, recorded, put = _run_fetch(upstream, uen="199999999Z")
    assert result["is_stub"] is True
    assert result["not_in_register"] is True
    assert recorded == []
    put.assert_called_once()


def test_name_mismatch_drops_the_record() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")})
    result, recorded, _ = _run_fetch(upstream, uen="196800306E", legal_name="SINGAPORE AIRLINES LIMITED")
    assert result["is_stub"] is True
    assert result["entity"] is None
    assert result["name_mismatch"] is True
    assert recorded == []


def test_a_429_backs_off_once_and_then_succeeds() -> None:
    limited = _FIXTURES["_rate_limited_http_429"]
    upstream = _Upstream(
        {_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")},
        overrides={_ACRA_ID: [_response(429, limited["body"])]},
    )
    result, recorded, _ = _run_fetch(upstream, uen="196800306E")
    assert result["is_stub"] is False
    assert recorded == []
    assert upstream.queried().count(_ACRA_ID) == 2


def test_two_429s_degrade_as_rate_limited() -> None:
    limited = _FIXTURES["_rate_limited_http_429"]
    upstream = _Upstream(
        overrides={_ACRA_ID: [_response(429, limited["body"]), _response(429, limited["body"])]},
    )
    result, recorded, put = _run_fetch(upstream, uen="196800306E")
    assert result["is_stub"] is True
    assert result.get("not_in_register") is None
    assert [d.reason for d in recorded] == [degradation.REASON_RATE_LIMITED]
    put.assert_not_called()


@pytest.mark.parametrize("status", [403, 500, 503])
def test_upstream_failure_degrades_and_is_never_a_miss(status: int) -> None:
    upstream = _Upstream(overrides={_ACRA_ID: [_response(status, {})]})
    result, recorded, put = _run_fetch(upstream, uen="196800306E")
    assert result["is_stub"] is True and result.get("not_in_register") is None
    assert any(str(status) in d.detail for d in recorded)
    put.assert_not_called()


def test_a_datastore_answer_without_success_degrades() -> None:
    upstream = _Upstream(overrides={_ACRA_ID: [_response(200, {"success": False, "error": {}})]})
    result, recorded, _ = _run_fetch(upstream, uen="196800306E")
    assert result["is_stub"] is True
    assert recorded and recorded[0].source_id == "acra_singapore"


def test_a_failed_detail_lookup_serves_the_uen_row_but_is_not_cached() -> None:
    upstream = _Upstream(
        {_ACRA_ID: _fixture("dbs_entity")},
        overrides={_C2["D"]: [_response(503, {})]},
    )
    result, recorded, put = _run_fetch(upstream, uen="196800306E", legal_name="DBS BANK LTD.")
    assert result["is_stub"] is False
    assert result["entity"]["uen"] == "196800306E" and result["detail"] is None
    assert any("503" in d.detail for d in recorded)
    put.assert_not_called()


def test_metadata_without_the_acra_dataset_degrades() -> None:
    renamed = _fixture("collection_1_metadata")
    for d in renamed["data"]["datasetMetadata"]:
        d["name"] = d["name"] + " (renamed)"
    upstream = _Upstream(overrides={"collection:1": [_response(200, renamed)]})
    result, recorded, _ = _run_fetch(upstream, uen="196800306E")
    assert result["is_stub"] is True
    assert any("ACRA entities dataset" in d.detail for d in recorded)


def test_dataset_ids_are_resolved_once_and_reused() -> None:
    first = _Upstream({_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")})
    _run_fetch(first, uen="196800306E")
    second = _Upstream({_ACRA_ID: _fixture("kibu_entity"), _C2["K"]: _fixture("kibu_detail")})
    _run_fetch(second, uen="201532108K")
    assert "metadata" not in second.queried()


def test_the_per_lookup_budget_degrades_rather_than_stalls() -> None:
    upstream = _Upstream({_ACRA_ID: _fixture("dbs_entity"), _C2["D"]: _fixture("dbs_detail")})
    with patch.object(acra, "_LOOKUP_CALL_BUDGET", 2):
        result, recorded, _ = _run_fetch(upstream, uen="196800306E", budget_scope=True)
    assert len(upstream.calls) == 2
    assert any(d.reason == degradation.REASON_RATE_LIMITED for d in recorded)
    # The UEN row arrived inside the budget; only the detail was cut off.
    assert result["entity"] is not None and result["detail"] is None


def test_a_cached_answer_makes_no_call() -> None:
    upstream = _Upstream()
    cached = (
        {"entity": _row("dbs_entity"), "detail": _row("dbs_detail"), "dataset": "acra", "resource_id": _C2["D"]},
        "live",
    )
    result, recorded, _ = _run_fetch(upstream, uen="196800306E", cached=cached)
    assert upstream.calls == []
    assert result["detail"]["entity_name"] == "DBS BANK LTD."
    assert recorded == []


@pytest.mark.parametrize("uen", ["T21VC0144D-SF001", "not-a-uen", ""])
def test_a_malformed_uen_or_sub_fund_makes_no_call(uen: str) -> None:
    upstream = _Upstream()
    result, recorded, _ = _run_fetch(upstream, uen=uen)
    assert result["is_stub"] is True
    assert upstream.calls == []
    assert recorded == []


def test_search_is_lei_flow_only_and_makes_no_call() -> None:
    adapter = AcraSingaporeAdapter()
    with patch("opencheck.sources.acra_singapore.build_client") as client:
        assert asyncio.run(adapter.search("DBS Bank", SearchKind.ENTITY)) == []
        client.assert_not_called()
