"""Tests for the Guernsey Registry adapter and its mapper.

Fixtures in ``tests/fixtures/guernsey_registry/`` are **real answers** of the
registry's public search API (``portal.guernseyregistry.com/api``) captured on
30 Oct 2026, with one change: every **individual** official's name is replaced
by "Person Placeholder X" (honorific kept), because OpenCheck does not
republish individuals' names from this register — not in its output and not
in its repository. Corporate officials are as filed.

* ``cmp48295`` — Partners Group Finance EUR IC Limited, an incorporated cell
  with four individual directors.
* ``cmp70222`` — Davos Investments Limited: a corporate director (Praxis
  Directors One Limited, ``CMP36291``) and resident agent (``CMP22546``).
* ``lp2820`` — Naudi Investments LP: a corporate general partner and an LP as
  resident agent. ``CMP2820`` is a different entity.
* ``cmp2820`` — Crouchley de la Fosse Associates Ltd., compulsorily wound up
  in 1980, no officials; its number search also returns ``CMP28200…``.
* ``fnd84`` — a foundation whose one individual is councillor and resident
  agent.
* ``llp70`` — iCON GP LLP: a Guernsey member, an English LLP member (the
  registry's "not publicly available" answer), a corporate resident agent, and
  a former name.
* ``lp5558`` — an incorporated LP GLEIF files under Alderney's RA code.
* ``cmp46442`` — Sirius Real Estate Limited: eight individual directors and a
  former name (DAWNAY, DAY SIRIUS LIMITED).
* ``cmp5309`` / ``lp5309`` — one digits, two entities: Olympus Sportswear
  (Guernsey) Limited and Hedosophia Defence Technologies L.P.

Stephen's decisions of 30 Oct 2026 (Notion: "Guernsey / Guernsey Registry
public API (officers)") are what these pin.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods import annotations as ann
from opencheck.bods import mapper as bods_mapper
from opencheck.bods.mapper import (
    _GLEIF_RA_TO_ORG_ID,
    gleif_registration_scheme,
    map_gleif,
    map_guernsey_registry,
)
from opencheck.bods.mappers.guernsey import gg_liveness, gg_role
from opencheck.bods.officer_roles import OFFICER_POSITION_KEY
from opencheck.cache import Cache
from opencheck.findings import MAX_FINDING_CHARS, finding_guernsey_registry
from opencheck.host_throttle import (
    GUERNSEY_REGISTRY_HOST,
    HostRateLimitedError,
    get_host_throttle,
)
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_guernsey_registry, _LookupCtx
from opencheck.sources import REGISTRY
from opencheck.sources import guernsey_registry as gg
from opencheck.sources.base import SearchKind
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.guernsey_registry import GuernseyRegistryBundle

_FIXTURES = Path(__file__).parent / "fixtures" / "guernsey_registry"
_API = gg.API_BASE
_NOT_PUBLIC = {
    "data": None,
    "message": "These information are not publicly available. Please refer to the Register.",
    "statusCode": 400,
    "success": False,
}


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# The number
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("CMP48295", "CMP48295"),
        ("cmp 48295", "CMP48295"),
        ("LLP70", "LLP70"),
        ("LP5558", "LP5558"),
        ("FND84", "FND84"),
        ("CH133", "CH133"),
        ("48295", "48295"),
        ("0048295", "48295"),
        ("C/563", "C/563"),
    ],
)
def test_numbers_normalise(raw: str, expected: str) -> None:
    assert gg.normalise_registry_number(raw) == expected


@pytest.mark.parametrize("bad", ["", "GGGT123", "CMP", "3084584A", "12345678", "C563/"])
def test_a_value_that_cannot_be_a_registry_number_is_refused(bad: str) -> None:
    with pytest.raises(ValueError):
        gg.normalise_registry_number(bad)


def test_an_explicit_prefix_is_the_only_candidate() -> None:
    assert gg.candidate_numbers("LP5558", registered_at="RA000777", legal_form="E6J3") == ["LP5558"]


def test_the_legal_form_comes_before_the_ra_code() -> None:
    # Hedosophia's LPs are filed under the companies register's RA code.
    order = gg.candidate_numbers("5309", registered_at="RA000383", legal_form="75XK")
    assert order[:2] == ["LP5309", "CMP5309"]
    assert sorted(order) == sorted(f"{p}5309" for p in gg.PREFIXES)


def test_without_an_anchor_every_register_is_tried_in_order() -> None:
    assert gg.candidate_numbers("91") == [f"{p}91" for p in gg.PREFIXES]


@pytest.mark.parametrize("elf", sorted(gg.ALDERNEY_ELF))
def test_an_alderney_form_is_alderney(elf: str) -> None:
    assert gg.is_alderney("1953", legal_form=elf)
    # …unless GLEIF filed a Guernsey prefix, which is then believed.
    assert not gg.is_alderney("LP5558", legal_form=elf)
    assert gg.is_alderney("C/563")


def test_every_elf_prefix_is_a_register() -> None:
    assert set(gg.ELF_PREFIX.values()) <= set(gg.PREFIXES)
    assert not set(gg.ELF_PREFIX) & gg.ALDERNEY_ELF


# ---------------------------------------------------------------------------
# GLEIF: the RA codes, the scheme and decision 4
# ---------------------------------------------------------------------------


def test_the_deriver_dispatches_on_the_registry_codes_and_alderney_only() -> None:
    adapter = REGISTRY["guernsey_registry"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == gg.DISPATCH_RA_CODES
    assert gg.RA_ALDERNEY in deriver.ra_codes
    # Decision 3: no dispatch on GFSC licence numbers or RA999999.
    assert "RA000886" not in deriver.ra_codes and "RA999999" not in deriver.ra_codes
    assert deriver.derived_key == "gg_registry_number"
    assert adapter.lookup_pass_anchor and adapter.lookup_pass_legal_name


def test_the_mappers_copy_of_the_ra_codes_is_the_adapters() -> None:
    assert bods_mapper._GG_RCE_RA_CODES == gg.GG_REGISTRY_RA_CODES
    for ra in gg.GG_REGISTRY_RA_CODES:
        assert _GLEIF_RA_TO_ORG_ID[ra][0] == gg.GG_SCHEME == "GG-RCE"
    assert "RA000777" not in _GLEIF_RA_TO_ORG_ID and "RA000886" not in _GLEIF_RA_TO_ORG_ID


def test_a_bare_number_keeps_the_ra_code_as_its_scheme() -> None:
    assert gleif_registration_scheme("RA000383", "GG", None, "46442")[0] == "RA000383"
    assert gleif_registration_scheme("RA000383", "GG", None, "CMP46442")[0] == "GG-RCE"


@pytest.mark.parametrize(
    ("ra", "elf", "number", "expected"),
    [
        ("RA000383", "E6J3", "46442", "CMP46442"),   # Sirius: both say company
        ("RA000667", "75XK", "2820", "LP2820"),      # Naudi: both say LP
        ("RA000666", "9XFK", "84", "FND84"),
        ("RA000668", "Q49K", "70", "LLP70"),
        ("RA000668", "E6J3", "66648", None),         # Polten: LLP RA, company form
        ("RA000383", "75XK", "5309", None),          # Hedosophia: company RA, LP form
        ("RA001134", "8888", "133", None),           # charities: no single prefix
        ("RA000777", "MCQP", "LP5558", "LP5558"),    # a prefix GLEIF filed is kept
        ("RA000777", "NDC3", "C/563", None),
    ],
)
def test_decision_4_the_prefix_only_where_ra_and_elf_agree(
    ra: str, elf: str, number: str, expected: str | None
) -> None:
    assert gg.gleif_prefixed_number(number, registered_at=ra, legal_form=elf) == expected


def _gleif_record(ra: str, elf: str, number: str) -> dict[str, Any]:
    return {
        "id": "213800NURUF5W8QSK566",
        "attributes": {
            "lei": "213800NURUF5W8QSK566",
            "entity": {
                "legalName": {"name": "SIRIUS REAL ESTATE LIMITED"},
                "jurisdiction": "GG",
                "status": "ACTIVE",
                "registeredAs": number,
                "registeredAt": {"id": ra},
                "legalForm": {"id": elf},
            },
        },
    }


def _gleif_registration(ra: str, elf: str, number: str) -> list[dict[str, str]]:
    record = _gleif_record(ra, elf, number)
    statements = list(map_gleif({"lei": record["id"], "record": record}))
    entity = next(s for s in statements if s.get("recordType") == "entity")
    return [i for i in entity["recordDetails"]["identifiers"] if i["scheme"] != "XI-LEI"]


def test_the_gleif_statement_says_gg_rce_where_both_codes_agree() -> None:
    assert _gleif_registration("RA000383", "E6J3", "46442") == [
        {"id": "CMP46442", "scheme": "GG-RCE", "schemeName": _GLEIF_RA_TO_ORG_ID["RA000383"][1]}
    ]


def test_alderney_is_value_schemed() -> None:
    """A Guernsey number GLEIF files under Alderney's code with its prefix is
    GG-RCE; Alderney's own numbers keep the RA code."""
    assert "RA000777" in bods_mapper._GLEIF_VALUE_SCHEMED_RA
    assert gleif_registration_scheme("RA000777", "GG", None, "LP5558")[0] == "GG-RCE"
    assert gleif_registration_scheme("RA000777", "GG", None, "1953")[0] == "RA000777"
    (ident,) = _gleif_registration("RA000777", "MCQP", "LP5558")
    assert ident == {"id": "LP5558", "scheme": "GG-RCE", "schemeName": _GLEIF_RA_TO_ORG_ID["RA000383"][1]}


def test_the_gleif_statement_keeps_the_ra_code_where_they_disagree() -> None:
    (ident,) = _gleif_registration("RA000668", "E6J3", "66648")
    assert ident["scheme"] == "RA000668" and ident["id"] == "66648"


def test_gg_rce_is_a_register_hop_with_a_country_alias() -> None:
    hop = hop_for("GG-RCE")
    assert hop is not None and hop.source_id == "guernsey_registry"
    assert hop.normalise("cmp 22546") == "CMP22546"
    assert hop_for("REG-GG") is hop


# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter(tmp_path: Path) -> gg.GuernseyRegistryAdapter:
    instance = gg.GuernseyRegistryAdapter()
    instance._cache = Cache(root=tmp_path)
    return instance


def _live():
    from opencheck.config import get_settings

    settings = get_settings().model_copy(update={"allow_live": True})
    return patch("opencheck.sources.guernsey_registry.get_settings", return_value=settings)


def _mock_registry(router: respx.MockRouter, *names: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Route the registry's endpoints to the named fixtures.

    Returns a dict of the official-read routes by id so a test can count them.
    """
    overrides = overrides or {}
    searches: dict[str, Any] = {}
    gets: dict[str, tuple[int, Any]] = {}
    for name in names:
        data = _fixture(name)
        searches[data["number"]] = data["search"]
        hit = next((r for r in data["search"].get("data") or [] if r["registeredNumber"] == data["number"]), None)
        if hit and "entity" in data:
            eid = hit["entityId"]
            gets[f"entity/{eid}"] = (200, data["entity"])
            gets[f"entity/relationships/{eid}"] = (200, data["relationships"])
            gets[f"entity/names/{eid}"] = (200, data["names"])
            for oid, answer in data["officials"].items():
                gets[f"entity/{oid}"] = (answer["status"], answer["body"])
    gets.update(overrides)

    def search(request: httpx.Request) -> httpx.Response:
        text = json.loads(request.content)["searchText"]
        body = searches.get(text, {"data": [], "message": None, "statusCode": 200, "success": True})
        return httpx.Response(200, json=body)

    router.post(f"{_API}/entity/quick-search").mock(side_effect=search)
    routes = {}
    for path, (status, body) in gets.items():
        routes[path] = router.get(f"{_API}/{path}").mock(return_value=httpx.Response(status, json=body))
    return routes


def _fetch(adapter: gg.GuernseyRegistryAdapter, number: str, **kwargs: Any) -> tuple[dict[str, Any], list[Any]]:
    with degradation.recording() as recorded, provenance.recording():
        result = asyncio.run(adapter.fetch(number, **kwargs))
    return result, list(recorded)


def test_a_corporate_record_names_its_corporate_officials(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "cmp70222")
        result, recorded = _fetch(adapter, "CMP70222")
    assert recorded == []
    assert result["is_stub"] is False and result["entity"]["number"] == "CMP70222"
    officials = {o["role"]: o for o in result["officials"]}
    assert officials["Director"]["kind"] == gg.KIND_GG_ENTITY
    assert officials["Director"]["number"] == "CMP36291"
    assert officials["Director"]["name"] == "Praxis Directors One Limited"
    assert officials["Resident Agent"]["number"] == "CMP22546"
    # Cached whole: a second fetch sends nothing.
    with _live(), respx.mock(assert_all_called=False) as router:
        route = router.route(url__startswith=_API)
        again, _ = _fetch(adapter, "CMP70222")
        assert route.call_count == 0
    assert again["officials"] == result["officials"]


def test_individuals_are_never_named_and_cost_no_call(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        routes = _mock_registry(router, "cmp48295")
        result, recorded = _fetch(adapter, "CMP48295")
        official_reads = [r for p, r in routes.items() if p.count("/") == 1 and p != "entity/98287"]
        assert all(r.call_count == 0 for r in official_reads)
    assert recorded == []
    assert [o["kind"] for o in result["officials"]] == [gg.KIND_PERSON] * 4
    assert all("name" not in o and "official_key" not in o for o in result["officials"])
    dumped = json.dumps(result) + json.dumps(list(adapter._cache._root().rglob("*")), default=str)
    assert "Placeholder" not in dumped
    for path in adapter._cache._root().rglob("*.json"):
        assert "Placeholder" not in path.read_text(encoding="utf-8")


def test_one_individual_in_two_roles_stays_unnamed(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "fnd84")
        result, _ = _fetch(adapter, "FND84")
    assert sorted(o["role"] for o in result["officials"]) == ["Councillor", "Resident Agent"]
    assert {o["kind"] for o in result["officials"]} == {gg.KIND_PERSON}


def test_a_body_registered_elsewhere_is_named_without_a_number(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "llp70")
        result, _ = _fetch(adapter, "LLP70")
    by_name = {o["name"]: o for o in result["officials"]}
    assert by_name["iCON Infrastructure LLP"]["kind"] == gg.KIND_BODY
    assert "number" not in by_name["iCON Infrastructure LLP"]
    assert by_name["iCON Infrastructure Management IV Limited"]["number"] == "CMP63250"
    assert result["former_names"] == [{"name": "iCON GP IV LLP", "since": "2017-03-10"}]


def test_a_bare_name_the_registry_withholds_is_an_individual(adapter: gg.GuernseyRegistryAdapter) -> None:
    data = _fixture("fnd84")
    rel = json.loads(json.dumps(data["relationships"]))
    rel["data"] = [dict(rel["data"][0], displayName="Nigel Lewis Westgarth")]
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "fnd84", overrides={"entity/relationships/119540": (200, rel)})
        result, _ = _fetch(adapter, "FND84")
    (official,) = result["officials"]
    assert official["kind"] == gg.KIND_PERSON and "name" not in official
    assert "Westgarth" not in json.dumps(result)


def test_the_search_prefix_match_is_not_accepted(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "cmp2820")
        result, _ = _fetch(adapter, "CMP2820")
    assert result["entity"]["name"] == "Crouchley de la Fosse Associates Ltd."
    assert result["officials"] == [] and result["officials_read"] is True


def test_the_name_gate_moves_past_another_registers_entity(adapter: gg.GuernseyRegistryAdapter) -> None:
    """GLEIF's form code wrong-foots the order: CMP5309 is tried first, is
    Olympus Sportswear, and LP5309 — Hedosophia — is the answer."""
    lp = _fixture("lp5309")["search"]["data"][0]
    entity = {
        "data": {
            "entityRegNumber": "LP5309", "entityRegName": lp["registeredName"],
            "entityRegDate": "2023-01-01T00:00:00", "entityTypeDesc": lp["entityTypeDesc"],
            "entityStatusDesc": lp["entityStatusDesc"], "entityRegEmail": "someone@example.gg",
        }
    }
    empty = {"data": [], "message": None, "statusCode": 200, "success": True}
    olympus = next(r for r in _fixture("cmp5309")["search"]["data"] if r["registeredNumber"] == "CMP5309")
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(
            router, "cmp5309", "lp5309",
            overrides={
                f"entity/names/{olympus['entityId']}": (200, empty),
                f"entity/{lp['entityId']}": (200, entity),
                f"entity/relationships/{lp['entityId']}": (200, empty),
                f"entity/names/{lp['entityId']}": (200, empty),
            },
        )
        result, recorded = _fetch(
            adapter, "5309", legal_name="Hedosophia Defence Technologies L.P.",
            anchor={"registered_at": "RA000383", "legal_form": "E6J3"},
        )
    assert recorded == []
    assert result["gg_registry_number"] == "LP5309"
    assert result["number_as_filed"] == "5309"
    # The allowlist: the record's e-mail address is not carried.
    assert "someone@example.gg" not in json.dumps(result)


def test_a_number_that_names_another_entity_is_a_note_card(adapter: gg.GuernseyRegistryAdapter) -> None:
    empty = {"data": [], "message": None, "statusCode": 200, "success": True}
    olympus = next(r for r in _fixture("cmp5309")["search"]["data"] if r["registeredNumber"] == "CMP5309")
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "cmp5309", overrides={f"entity/names/{olympus['entityId']}": (200, empty)})
        result, recorded = _fetch(
            adapter, "5309", legal_name="Elizabeth College Foundation",
            anchor={"registered_at": "RA000383", "legal_form": "E6J3"},
        )
    assert recorded == []
    assert result["not_found"] and result["name_mismatch"] and not result["is_stub"]
    assert result["registered_name"] == "Olympus Sportswear (Guernsey) Limited"
    assert result["coverage_note"] == gg.COVERAGE_NAME_MISMATCH


def test_alderney_is_a_note_card_and_nothing_is_sent(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        route = router.route(url__startswith=_API)
        first, _ = _fetch(adapter, "1953", legal_name="ISQ ALDERNEY LIMITED",
                          anchor={"registered_at": "RA000777", "legal_form": "NDC3"})
        second, _ = _fetch(adapter, "C/563", legal_name="PIPPARD INVESTMENTS LIMITED")
        assert route.call_count == 0
    for result in (first, second):
        assert result["alderney"] and result["not_found"] and not result["is_stub"]
        assert result["coverage_note"] == gg.COVERAGE_ALDERNEY


def test_bare_digits_with_no_name_are_never_guessed(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        route = router.route(url__startswith=_API)
        result, _ = _fetch(adapter, "2820")
        assert route.call_count == 0
    assert result["ambiguous"] and result["coverage_note"] == gg.COVERAGE_AMBIGUOUS


def test_officials_beyond_the_cap_are_classified_by_name_only(adapter: gg.GuernseyRegistryAdapter) -> None:
    data = _fixture("cmp70222")
    rel = {"data": [
        {"relationshipId": i, "displayName": f"Trustee Company {i} Limited", "effectiveDate": "2020-01-01T00:00:00",
         "relationshipTypeId": 201, "relationshipTypeDesc": "Director", "relationshipToEntityId": 900000 + i}
        for i in range(12)
    ], "success": True}
    overrides: dict[str, Any] = {"entity/relationships/126610": (200, rel)}
    for i in range(12):
        overrides[f"entity/{900000 + i}"] = (400, _NOT_PUBLIC)
    with _live(), respx.mock(assert_all_called=False) as router:
        routes = _mock_registry(router, "cmp70222", overrides=overrides)
        result, recorded = _fetch(adapter, "CMP70222")
        calls = sum(routes[f"entity/{900000 + i}"].call_count for i in range(12))
    assert calls == gg.OFFICIALS_RESOLVE_CAP == 10
    assert result["officials_resolve_capped"] is True
    assert [o.get("resolved") for o in result["officials"]].count(False) == 2
    assert any(d.reason == degradation.REASON_TRUNCATED for d in recorded)
    assert data["number"] == "CMP70222"


def test_officials_that_did_not_answer_are_said_and_nothing_is_cached(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _mock_registry(router, "cmp70222", overrides={"entity/relationships/126610": (503, {"oops": 1})})
        result, recorded = _fetch(adapter, "CMP70222")
    assert result["is_stub"] is False and result["officials_read"] is False
    assert {d.check for d in recorded} >= {degradation.CHECK_SOURCE_READ}
    assert not list(adapter._cache._root().rglob("*record*"))
    assert finding_guernsey_registry(result).endswith("officials not read.")


def test_a_429_is_rate_limited_and_never_cached(adapter: gg.GuernseyRegistryAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        router.post(f"{_API}/entity/quick-search").mock(return_value=httpx.Response(429))
        result, recorded = _fetch(adapter, "CMP70222")
    assert result["is_stub"] is True
    assert recorded and recorded[0].reason == degradation.REASON_RATE_LIMITED
    assert not list(adapter._cache._root().rglob("*.json"))


def test_a_request_the_pacing_refuses_is_rate_limited(adapter: gg.GuernseyRegistryAdapter) -> None:
    async def refuse(*_a: Any, **_k: Any) -> Any:
        raise HostRateLimitedError(GUERNSEY_REGISTRY_HOST, "no slot")

    with _live(), patch.object(httpx.AsyncClient, "post", refuse):
        result, recorded = _fetch(adapter, "CMP70222")
    assert result["is_stub"] is True
    assert recorded and recorded[0].reason == degradation.REASON_RATE_LIMITED


def test_offline_nothing_is_sent(adapter: gg.GuernseyRegistryAdapter) -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.route(url__startswith=_API)
        result, _ = _fetch(adapter, "CMP70222")
        assert route.call_count == 0
    assert result["is_stub"] is True


def test_the_registry_host_is_paced() -> None:
    assert get_host_throttle(GUERNSEY_REGISTRY_HOST) is not None
    assert GUERNSEY_REGISTRY_HOST in _API


def test_search_returns_registered_entities_only(adapter: gg.GuernseyRegistryAdapter) -> None:
    body = {"data": [
        {"entityId": 1, "registeredNumber": "CMP36291", "registeredName": "Praxis Directors One Limited",
         "entityTypeDesc": "Non Cellular Company", "entityStatusDesc": "Normal"},
        {"entityId": 2, "registeredNumber": "GGGT1234", "registeredName": "PRAXIS",
         "entityTypeDesc": "Trade Mark", "entityStatusDesc": "Registered"},
    ], "success": True}
    with _live(), respx.mock(assert_all_called=False) as router:
        router.post(f"{_API}/entity/quick-search").mock(return_value=httpx.Response(200, json=body))
        hits = asyncio.run(adapter.search("Praxis Directors", SearchKind.ENTITY))
    assert [h.hit_id for h in hits] == ["CMP36291"]
    assert hits[0].identifiers == {"gg_registry_number": "CMP36291"}


def test_the_schema_refuses_a_named_individual() -> None:
    bundle = {
        "gg_registry_number": "CMP1",
        "entity": {"number": "CMP1", "name": "X Limited"},
        "officials": [{"role": "Director", "kind": "person", "name": "Mr A Person"}],
    }
    with pytest.raises(SourceSchemaError):
        validate_raw("guernsey_registry", GuernseyRegistryBundle, bundle)
    bundle["officials"] = [{"role": "Director", "kind": "person", "official_key": "174000"}]
    with pytest.raises(SourceSchemaError):
        validate_raw("guernsey_registry", GuernseyRegistryBundle, bundle)


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


def _bundle(name: str, adapter_factory: Any = None) -> dict[str, Any]:
    """A fetch bundle built through the real adapter from the fixture."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        instance = gg.GuernseyRegistryAdapter()
        instance._cache = Cache(root=Path(tmp))
        with _live(), respx.mock(assert_all_called=False) as router:
            _mock_registry(router, name)
            result, _ = _fetch(instance, _fixture(name)["number"])
    return result


def _map(name: str) -> list[dict[str, Any]]:
    with provenance.recording():
        return list(map_guernsey_registry(_bundle(name)))


def _by_type(statements: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == kind]


def _name(stmt: dict[str, Any]) -> str:
    return stmt["recordDetails"].get("name") or ""


def test_the_entity_statement() -> None:
    (company, *_rest) = _map("cmp70222")
    rd = company["recordDetails"]
    assert rd["name"] == "Davos Investments Limited"
    assert rd["identifiers"] == [{"id": "CMP70222", "scheme": "GG-RCE", "schemeName": gg.GG_SCHEME_NAME}]
    assert rd["jurisdiction"] == {"name": "Guernsey", "code": "GG"}
    assert rd["foundingDate"] == "2022-01-20"
    assert rd["entityType"]["details"] == "Non Cellular Company"
    assert rd["addresses"][0]["type"] == "registered"
    assert rd["addresses"][0]["country"]["code"] == "GG"


def test_corporate_officials_are_guernsey_entities_with_their_numbers() -> None:
    statements = _map("cmp70222")
    entities = {_name(s): s for s in _by_type(statements, "entity")}
    director = entities["Praxis Directors One Limited"]["recordDetails"]
    assert director["identifiers"][0]["id"] == "CMP36291"
    assert director["jurisdiction"]["code"] == "GG"
    rels = _by_type(statements, "relationship")
    interests = {r["recordDetails"]["interests"][0]["details"]: r["recordDetails"]["interests"][0] for r in rels}
    assert interests["Director"]["type"] == "seniorManagingOfficial"
    assert OFFICER_POSITION_KEY not in interests["Director"]
    assert interests["Resident Agent"]["type"] == "otherInfluenceOrControl"
    assert interests["Resident Agent"][OFFICER_POSITION_KEY] == "Resident Agent"
    assert interests["Director"]["startDate"] == "2022-01-20"


def test_a_general_partner_is_control_not_an_office() -> None:
    rels = _by_type(_map("lp2820"), "relationship")
    gp = next(r["recordDetails"]["interests"][0] for r in rels
              if "General partner" in r["recordDetails"]["interests"][0]["details"])
    assert gp["type"] == "otherInfluenceOrControl" and OFFICER_POSITION_KEY not in gp


def test_individuals_are_a_count_not_statements() -> None:
    statements = _map("cmp46442")
    assert _by_type(statements, "person") == []
    assert _by_type(statements, "relationship") == []
    (company,) = statements
    notes = [a["description"] for a in company.get("annotations") or [] if a["motivation"] == "commenting"]
    assert any("8 individuals" in n and "8 directors" in n and "does not republish" in n for n in notes)
    assert "Placeholder" not in json.dumps(statements)
    assert company["recordDetails"]["alternateNames"] == ["DAWNAY, DAY SIRIUS LIMITED"]


def test_a_body_registered_elsewhere_has_no_jurisdiction_or_identifier() -> None:
    entities = {_name(s): s for s in _by_type(_map("llp70"), "entity")}
    icon = entities["iCON Infrastructure LLP"]["recordDetails"]
    assert "jurisdiction" not in icon and not icon.get("identifiers")


@pytest.mark.parametrize(
    ("status", "cls"),
    [
        ("Normal", "live"),
        ("Strike Off Listed", "pending"),
        ("Voluntary winding up Part 1", "pending"),
        ("Compulsorily Wound Up", "terminal"),
        ("Dissolved (voluntary strike off)", "terminal"),
        ("Dissolved (registrar compulsory strike off)", "terminal"),
        ("In Default", "unknown"),
        ("Something New", "unknown"),
    ],
)
def test_status_words(status: str, cls: str) -> None:
    assert gg_liveness(status) == cls


def test_a_wound_up_company_is_dated() -> None:
    (company,) = _map("cmp2820")
    assert company["recordDetails"]["dissolutionDate"] == "1980-07-21"


@pytest.mark.parametrize(
    ("role", "expected"),
    [("Director", "seniorManagingOfficial"), ("Councillor", "boardMember"),
     ("Chairperson", "boardChair"), ("Member", "otherInfluenceOrControl"),
     ("Secretary", "otherInfluenceOrControl"), ("Something Else", "otherInfluenceOrControl")],
)
def test_roles(role: str, expected: str) -> None:
    assert gg_role(role)[0] == expected


@pytest.mark.parametrize("fixture", ["cmp48295", "cmp70222", "lp2820", "cmp2820", "fnd84", "llp70", "lp5558", "cmp46442"])
def test_every_mapping_is_well_formed(fixture: str) -> None:
    statements = _map(fixture)
    for stmt in statements:
        assert not ann.validate_annotations(stmt), stmt.get("annotations")
        for interest in stmt["recordDetails"].get("interests") or []:
            assert interest.get("beneficialOwnershipOrControl") is not True
    assert _by_type(statements, "person") == []


def test_a_stub_or_note_card_maps_to_nothing() -> None:
    assert list(map_guernsey_registry({"gg_registry_number": "CMP1", "is_stub": True})) == []
    assert list(map_guernsey_registry({"gg_registry_number": "1953", "is_stub": False, "not_found": True})) == []


# ---------------------------------------------------------------------------
# The finding and the hit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        ("cmp70222", "Non cellular company registered 20 January 2022, 1 resident agent and 1 director on file."),
        ("cmp46442", "Non cellular company registered 20 February 2007, 8 directors on file, "
                     "individuals not named, formerly DAWNAY, DAY SIRIUS LIMITED."),
        ("cmp2820", "Non cellular company registered 5 January 1973, status Compulsorily Wound Up "
                    "since 21 July 1980, no current officials published."),
    ],
)
def test_finding(fixture: str, expected: str) -> None:
    assert finding_guernsey_registry(_bundle(fixture)) == expected


def test_a_charity_with_many_roles_keeps_its_officials_clause() -> None:
    bundle = {
        "entity": {"number": "CH133", "type": "Charity: Domestic - compulsorily registered",
                   "status": "Normal", "registered_on": "2009-01-19"},
        "officials_read": True,
        "officials": [{"role": r, "kind": "person"} for r in (
            "Chairperson", "Other Managing Official", "Other Managing Official", "Secretary",
            "Other Managing Official", "Other Managing Official", "Treasurer")],
    }
    assert finding_guernsey_registry(bundle) == (
        "Charity registered 19 January 2009, 7 officials on file, individuals not named."
    )
    assert gg_role("Other Managing Official")[0] == "seniorManagingOfficial"


@pytest.mark.parametrize("fixture", ["cmp48295", "cmp70222", "lp2820", "cmp2820", "fnd84", "llp70", "lp5558", "cmp46442"])
def test_the_finding_never_claims_ownership(fixture: str) -> None:
    sentence = finding_guernsey_registry(_bundle(fixture)) or ""
    assert sentence and len(sentence) <= MAX_FINDING_CHARS
    for word in ("owner", "owns", "owned", "beneficial", "shareholder"):
        assert word not in sentence.lower(), (word, sentence)


def test_the_hit_asserts_the_registry_number_only() -> None:
    ctx = _LookupCtx(lei="213800NURUF5W8QSK566", legal_name="SIRIUS REAL ESTATE LIMITED")
    hit = _bh_guernsey_registry(_bundle("cmp46442"), "46442", ctx)
    assert hit.hit_id == "CMP46442"
    assert hit.identifiers == {"gg_registry_number": "CMP46442"}
    note = _bh_guernsey_registry(
        {"not_found": True, "alderney": True, "coverage_note": gg.COVERAGE_ALDERNEY}, "1953", ctx
    )
    assert note.identifiers == {} and note.finding == gg.COVERAGE_ALDERNEY


def test_the_lookup_passes_the_anchor() -> None:
    from opencheck.routers import lookup

    seen: dict[str, Any] = {}

    async def fake_fetch(local_id: str, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs, local_id=local_id)
        return {"is_stub": True}

    ctx = _LookupCtx(lei="213800NURUF5W8QSK566", legal_name="SIRIUS REAL ESTATE LIMITED")
    ctx.registered_as = "46442"
    ctx.legal_form = "E6J3"
    ctx.jurisdiction = "GG"
    lookup._build_derived(ctx, "RA000383")
    assert ctx.derived["gg_registry_number"] == "46442"
    with patch.object(REGISTRY["guernsey_registry"], "fetch", fake_fetch):
        tasks = lookup._dispatch(ctx, only="guernsey_registry")
        assert [t[0] for t in tasks] == ["guernsey_registry"]
        asyncio.run(tasks[0][1])
    assert seen == {
        "local_id": "46442",
        "legal_name": "SIRIUS REAL ESTATE LIMITED",
        "anchor": {"registered_at": "RA000383", "legal_form": "E6J3"},
    }
