"""Phase 177 — Companies House corporate-PSC chains are followed as filed.

The register keys every company on an eight-character number, but a PSC
filing's ``identification.registration_number`` is whatever the filer typed,
and leading zeros are routinely dropped. Verified against the live PSC API on
2026-09-07 for Vosper Thornycroft (UK) Limited (00070274): its two corporate
PSCs are filed as ``2999029`` (Babcock Defence Systems Limited) and
``1915771`` (Babcock Southern Holdings Limited), and the layer above keeps the
habit (``2669327``). The adapter gated the walk on "exactly 8 alphanumerics",
so both failed it, the recursion never left the subject, and a six-layer UK
holding stack rendered as two upward nodes — while the 2025 UK PSC bulk held
all six. These tests pin the fix: normalise the filed number, accept the
country strings filers use, follow the chain to the configured depth, and
record every corporate PSC that was *not* followed with a reason.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from opencheck import degradation, signalstats
from opencheck.bods import mapper
from opencheck.config import get_settings
from opencheck.identifiers import ch_identification_is_uk, normalise_ch_company_number
from opencheck.sources.companies_house import CompaniesHouseAdapter

_API = "https://api.company-information.service.gov.uk"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_CH_PSC_MAX_DEPTH", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The number normaliser
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filed", "expected"),
    [
        ("2999029", "02999029"),  # Babcock Defence Systems, as filed 2026-09-07
        ("1915771", "01915771"),  # Babcock Southern Holdings, as filed
        ("2669327", "02669327"),  # Babcock Overseas Investments, as filed
        ("00070274", "00070274"),  # canonical form is unchanged
        ("SC123456", "SC123456"),
        ("sc 12345", "SC012345"),  # lower-case, spaced, one zero dropped
        ("ni1234", "NI001234"),
        ("OC403762", "OC403762"),
        ("  09507569 ", "09507569"),
    ],
)
def test_normalise_ch_company_number_accepts_filed_shapes(filed: str, expected: str) -> None:
    assert normalise_ch_company_number(filed) == expected


@pytest.mark.parametrize(
    "filed",
    ["", "   ", None, "Uk", "123456789", "12-34", "ABC12345", "N/A", "zS_RY9pRYlJ9XwGJEOFtkJgrf8s"],
)
def test_normalise_ch_company_number_rejects_non_numbers(filed: Any) -> None:
    assert normalise_ch_company_number(filed) is None


# ---------------------------------------------------------------------------
# The UK-register predicate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "identification",
    [
        {"country_registered": "England"},
        {"country_registered": "England And Wales"},
        {"country_registered": "England & Wales"},
        {"country_registered": "United Kingdom"},
        {"country_registered": "United Kingdom (England)"},
        {"country_registered": "Great Britain"},
        {"country_registered": "Uk"},
        {"country_registered": "GB"},
        {"country_registered": "Scotland"},
        {"country_registered": "Northern Ireland"},
        {"country_registered": "", "place_registered": "Companies House"},
        {"place_registered": "Register Of Companies (England And Wales)"},
        {"legal_authority": "United Kingdom (England)"},
    ],
)
def test_ch_identification_is_uk_accepts_filed_variants(identification: dict) -> None:
    assert ch_identification_is_uk(identification) is True


@pytest.mark.parametrize(
    "identification",
    [
        {},
        None,
        {"country_registered": "Jersey"},
        {"country_registered": "Guernsey", "place_registered": "Guernsey Registry"},
        {"country_registered": "Isle Of Man"},
        {"country_registered": "New South Wales"},
        {"country_registered": "Delaware", "place_registered": "Delaware Secretary Of State"},
        {"country_registered": "Netherlands", "place_registered": "Kamer Van Koophandel"},
        {"place_registered": "Registrar Of Companies, New South Wales"},
    ],
)
def test_ch_identification_is_uk_rejects_other_registers(identification: Any) -> None:
    assert ch_identification_is_uk(identification) is False


# ---------------------------------------------------------------------------
# The adapter walk — the Babcock stack, mocked with the numbers as filed
# ---------------------------------------------------------------------------

# subject → parent chains, keyed by the canonical number the register answers
# on; the value is the PSC block the register returns, with the number spelled
# the way the filer typed it (no leading zero), exactly as the live API does.
_CHAIN: dict[str, list[dict[str, Any]]] = {
    "00070274": [{
        "kind": "corporate-entity-person-with-significant-control",
        "name": "Babcock Defence Systems Limited",
        "etag": "e1",
        "notified_on": "2016-04-06",
        "natures_of_control": ["voting-rights-25-to-50-percent"],
        "identification": {
            "legal_form": "Limited By Shares",
            "legal_authority": "United Kingdom (England)",
            "country_registered": "England",
            "place_registered": "Companies House",
            "registration_number": "2999029",
        },
    }],
    "02999029": [{
        "kind": "corporate-entity-person-with-significant-control",
        "name": "Babcock Southern Holdings Limited",
        "etag": "e2",
        "notified_on": "2016-04-06",
        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
        "identification": {
            "country_registered": "England",
            "place_registered": "Companies House",
            "registration_number": "1915771",
        },
    }],
    "01915771": [{
        "kind": "corporate-entity-person-with-significant-control",
        "name": "Babcock Overseas Investments Limited",
        "etag": "e3",
        "notified_on": "2016-04-06",
        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
        "identification": {
            "country_registered": "England And Wales",
            "place_registered": "Companies House",
            "registration_number": "2669327",
        },
    }],
    "02669327": [{
        "kind": "corporate-entity-person-with-significant-control",
        "name": "Babcock International Group PLC",
        "etag": "e4",
        "notified_on": "2016-04-06",
        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
        "identification": {
            "country_registered": "United Kingdom",
            "place_registered": "Companies House",
            "registration_number": "2342138",
        },
    }],
    "02342138": [],  # the listed group head: no PSC (statement filed instead)
}


def _mock_company(httpx_mock: HTTPXMock, number: str, pscs: list[dict[str, Any]]) -> None:
    httpx_mock.add_response(
        url=f"{_API}/company/{number}",
        json={"company_number": number, "company_name": f"Company {number}"},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/officers?items_per_page=100&start_index=0", json={"items": []}
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control?items_per_page=100&start_index=0",
        json={"items": pscs},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control-statements?items_per_page=100&start_index=0",
        status_code=404,
        json={"errors": [{"error": "not-found"}]},
    )


def _mock_chain(httpx_mock: HTTPXMock, chain: dict[str, list[dict[str, Any]]]) -> None:
    for number, pscs in chain.items():
        _mock_company(httpx_mock, number, pscs)


async def test_walk_follows_numbers_filed_without_leading_zeros(httpx_mock: HTTPXMock) -> None:
    """The whole Babcock stack arrives in one bundle, keyed on canonical numbers."""
    _mock_chain(httpx_mock, _CHAIN)
    with degradation.recording() as degraded:
        bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert set(bundle["related_companies"]) == {"02999029", "01915771", "02669327", "02342138"}
    assert bundle["unfollowed_pscs"] == []
    assert degraded == []
    # Every register call was made against the canonical number, never the
    # filed spelling — the register would 404 on ``/company/2999029``.
    requested = {str(r.url) for r in httpx_mock.get_requests()}
    assert f"{_API}/company/02999029" in requested
    assert not any("/company/2999029" in u for u in requested)


async def test_walk_was_truncated_before_the_fix(httpx_mock: HTTPXMock) -> None:
    """The regression guard: the same filing shape must not stop at hop one.

    Before Phase 177 ``related_companies`` came back empty for this subject
    (the 7-character number failed the 8-character gate silently).
    """
    _mock_chain(httpx_mock, _CHAIN)
    bundle = await CompaniesHouseAdapter().fetch("00070274")
    assert len(bundle["related_companies"]) >= 4


async def test_depth_cap_is_configurable_and_reported(httpx_mock: HTTPXMock, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_CH_PSC_MAX_DEPTH", "2")
    get_settings.cache_clear()
    _mock_chain(httpx_mock, {k: _CHAIN[k] for k in ("00070274", "02999029", "01915771")})

    with degradation.recording() as degraded:
        bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert set(bundle["related_companies"]) == {"02999029", "01915771"}
    stopped = bundle["unfollowed_pscs"]
    assert len(stopped) == 1
    assert stopped[0]["reason"] == "max_depth_reached"
    assert stopped[0]["registration_number"] == "2669327"
    assert stopped[0]["subject_company_number"] == "01915771"
    assert [d.source_id for d in degraded] == ["companies_house"]
    assert "depth of 2 hops" in degraded[0].detail
    # Register calls are the four-per-hop the docs promise: subject + 2 hops.
    assert len(httpx_mock.get_requests()) == 12


async def test_default_depth_is_six(httpx_mock: HTTPXMock) -> None:
    assert get_settings().ch_psc_max_depth == 6


async def test_non_uk_and_unparseable_numbers_are_reported_not_guessed(
    httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
) -> None:
    subject = "00000001"
    _mock_company(
        httpx_mock,
        subject,
        [
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "Jersey Holdco Limited",
                "etag": "j1",
                "identification": {
                    "country_registered": "Jersey",
                    "place_registered": "Jersey Financial Services Commission",
                    "registration_number": "123456",
                },
            },
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "Mystery UK Limited",
                "etag": "m1",
                "identification": {
                    "country_registered": "England",
                    "place_registered": "Companies House",
                    "registration_number": "Uk",
                },
            },
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "Ceased Parent Limited",
                "etag": "c1",
                "ceased_on": "2020-01-01",
                "identification": {
                    "country_registered": "England",
                    "registration_number": "1234567",
                },
            },
        ],
    )

    with (
        caplog.at_level(logging.INFO, logger="opencheck.sources.companies_house"),
        degradation.recording() as degraded,
    ):
        bundle = await CompaniesHouseAdapter().fetch(subject)

    assert bundle["related_companies"] == {}
    reasons = {u["name"]: u["reason"] for u in bundle["unfollowed_pscs"]}
    assert reasons == {
        "Jersey Holdco Limited": "not_uk_registered",
        "Mystery UK Limited": "registration_number_not_a_company_number",
    }
    # A ceased PSC is neither followed nor reported — it is not on the chain.
    assert "Ceased Parent Limited" not in reasons
    # Only the UK-but-unparseable case is a degradation; Jersey is the register.
    assert len(degraded) == 1
    assert "not a Companies House number" in degraded[0].detail
    assert "Jersey" not in degraded[0].detail and "Mystery" not in degraded[0].detail
    # The residue is logged so it can be counted in production.
    logged = [r.getMessage() for r in caplog.records if "not followed" in r.getMessage()]
    assert len(logged) == 2
    # No register call was fired for either — nothing to fetch.
    assert len(httpx_mock.get_requests()) == 4


async def test_a_parent_that_cannot_be_fetched_does_not_sink_the_subject(
    httpx_mock: HTTPXMock,
) -> None:
    subject = "00000002"
    _mock_company(
        httpx_mock,
        subject,
        [
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "Misfiled Parent Limited",
                "etag": "p1",
                "identification": {
                    "country_registered": "England",
                    "registration_number": "9999999",
                },
            }
        ],
    )
    httpx_mock.add_response(
        url=f"{_API}/company/09999999",
        status_code=404,
        json={"errors": [{"error": "company-profile-not-found"}]},
    )

    with degradation.recording() as degraded:
        bundle = await CompaniesHouseAdapter().fetch(subject)

    assert bundle["company_number"] == subject
    assert bundle["related_companies"] == {}
    assert bundle["unfollowed_pscs"][0]["reason"] == "fetch_failed"
    assert any("could not be fetched" in d.detail for d in degraded)


async def test_cycles_and_shared_parents_are_walked_once(httpx_mock: HTTPXMock) -> None:
    """A ↔ B cross-holding plus a parent both name: each company fetched once."""
    a, b, top = "00000010", "00000011", "00000012"

    def corp(name: str, number_as_filed: str) -> dict[str, Any]:
        return {
            "kind": "corporate-entity-person-with-significant-control",
            "name": name,
            "etag": name,
            "identification": {
                "country_registered": "England",
                "registration_number": number_as_filed,
            },
        }

    _mock_company(httpx_mock, a, [corp("B", "11"), corp("Top", "12")])
    _mock_company(httpx_mock, b, [corp("A", "10"), corp("Top", "12")])
    _mock_company(httpx_mock, top, [])

    bundle = await CompaniesHouseAdapter().fetch(a)

    assert set(bundle["related_companies"]) == {b, top}
    assert bundle["unfollowed_pscs"] == []
    profile_calls = [r for r in httpx_mock.get_requests() if r.url.path.count("/") == 2]
    assert sorted(r.url.path for r in profile_calls) == [
        f"/company/{a}", f"/company/{b}", f"/company/{top}",
    ]


# ---------------------------------------------------------------------------
# The mapper — the PSC node and the related-company root must be one node
# ---------------------------------------------------------------------------


def _bundle(number: str, pscs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "source_id": "companies_house",
        "company_number": number,
        "profile": {"company_number": number, "company_name": f"Company {number}"},
        "officers": {"items": []},
        "pscs": {"items": pscs},
        "psc_statements": {"items": []},
    }


def test_mapper_aligns_psc_node_with_related_company_on_filed_number() -> None:
    root = _bundle("00070274", _CHAIN["00070274"])
    root["related_companies"] = {"02999029": _bundle("02999029", [])}
    root["unfollowed_pscs"] = []

    statements = mapper.map_companies_house(root).statements
    entities = [s for s in statements if s["recordDetails"].get("entityType")]
    # Three entity statements would mean the corporate PSC and the related
    # company root were emitted as two different nodes — the pre-177 shape,
    # where the graph could not connect the PSC to the subgraph above it.
    names = sorted(s["recordDetails"]["name"] for s in entities)
    assert names == ["Babcock Defence Systems Limited", "Company 00070274"]
    ids = {s["statementId"] for s in entities}
    assert len(ids) == 2

    psc_entity = next(
        s for s in entities if s["recordDetails"]["name"] == "Babcock Defence Systems Limited"
    )
    # The identifier is published in the register's canonical form, which is
    # what GLEIF's ``registeredAs`` carries and what the reconciler merges on.
    assert psc_entity["recordDetails"]["identifiers"] == [
        {"id": "02999029", "scheme": "GB-COH", "schemeName": "UK Companies House"}
    ]

    relationships = [s for s in statements if "interestedParty" in s["recordDetails"]]
    assert len(relationships) == 1
    # v0.4: ``interestedParty`` carries the record id, which for entity
    # statements OpenCheck makes identical to the statement id.
    assert relationships[0]["recordDetails"]["interestedParty"] == psc_entity["recordId"]
    assert relationships[0]["recordDetails"]["subject"] != psc_entity["recordId"]


def test_mapper_keeps_non_uk_corporate_psc_identifier_as_filed() -> None:
    root = _bundle(
        "00000003",
        [
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "Jersey Holdco Limited",
                "etag": "j1",
                "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                "identification": {
                    "country_registered": "Jersey",
                    "place_registered": "Jersey Financial Services Commission",
                    "registration_number": "123456",
                },
            }
        ],
    )
    root["related_companies"] = {}
    statements = mapper.map_companies_house(root).statements
    holdco = next(
        s for s in statements if s["recordDetails"].get("name") == "Jersey Holdco Limited"
    )
    assert holdco["recordDetails"]["identifiers"][0]["id"] == "123456"
    assert holdco["recordDetails"]["identifiers"][0]["scheme"] == "REG-JE"


# ---------------------------------------------------------------------------
# Phase 184 — every walk is measured (the PSC-graph ticket's "measure first")
# ---------------------------------------------------------------------------


@pytest.fixture
def _clean_walk_counters():
    signalstats.reset()
    yield
    signalstats.reset()


async def test_walk_records_its_shape_and_cost(
    httpx_mock: HTTPXMock, _clean_walk_counters
) -> None:
    """The Babcock stack: four related companies, depth four, twenty live
    register calls, nothing unfollowed — filed under the lookup origin."""
    _mock_chain(httpx_mock, _CHAIN)
    await CompaniesHouseAdapter().fetch("00070274")

    w = signalstats.stats()["companies_house_walks"]
    lookup = w["by_origin"]["lookup"]
    assert lookup["walks"] == 1
    assert lookup["calls_live"] == 20 and lookup["calls_cached"] == 0
    assert lookup["max_related"] == 4 and lookup["max_depth"] == 4
    assert lookup["seconds"] >= 0
    assert w["related"] == {"lookup|4-6": 1}
    assert w["depth"] == {"lookup|4": 1}
    assert w["unfollowed"] == {}
    assert "hop" not in w["by_origin"]


async def test_walk_records_cache_hits_separately(
    httpx_mock: HTTPXMock, _clean_walk_counters
) -> None:
    """A second walk over the same stack is answered from the cache: the
    register was not called again, and the counters say so.

    Phase 188 also caches "no PSC statements filed". Before it, the 404 was
    an exception path that cached nothing, so every re-walk spent one live
    call per company on a question the register had already answered — five
    of the twenty-five calls this test used to record. Twenty and twenty now:
    the second walk costs the register nothing at all.
    """
    _mock_chain(httpx_mock, _CHAIN)
    adapter = CompaniesHouseAdapter()
    await adapter.fetch("00070274")
    await adapter.fetch("00070274")
    lookup = signalstats.stats()["companies_house_walks"]["by_origin"]["lookup"]
    assert lookup["walks"] == 2
    assert lookup["calls_live"] == 20
    assert lookup["calls_cached"] == 20


async def test_walk_records_why_it_stopped(
    httpx_mock: HTTPXMock, monkeypatch, _clean_walk_counters
) -> None:
    monkeypatch.setenv("OPENCHECK_CH_PSC_MAX_DEPTH", "2")
    get_settings.cache_clear()
    _mock_chain(httpx_mock, {k: _CHAIN[k] for k in ("00070274", "02999029", "01915771")})
    await CompaniesHouseAdapter().fetch("00070274")
    w = signalstats.stats()["companies_house_walks"]
    assert w["unfollowed"] == {"lookup|max_depth_reached": 1}
    assert w["depth"] == {"lookup|2": 1}
    assert w["by_origin"]["lookup"]["calls_live"] == 12


async def test_walk_under_the_hop_origin(
    httpx_mock: HTTPXMock, _clean_walk_counters
) -> None:
    """``_register_one_layer`` sets the origin; the adapter reads it."""
    _mock_chain(httpx_mock, _CHAIN)
    token = signalstats.walk_origin.set("hop")
    try:
        await CompaniesHouseAdapter().fetch("00070274")
    finally:
        signalstats.walk_origin.reset(token)
    w = signalstats.stats()["companies_house_walks"]
    assert list(w["by_origin"]) == ["hop"]
    assert w["related"] == {"hop|4-6": 1}


async def test_a_walk_that_fails_is_still_counted(
    httpx_mock: HTTPXMock, _clean_walk_counters
) -> None:
    """The subject's own profile 500s: the walk raises, and the one call it
    spent is on the books — a walk that costs budget is never invisible."""
    httpx_mock.add_response(url=f"{_API}/company/00000099", status_code=500, json={})
    import httpx

    with pytest.raises(httpx.HTTPStatusError):
        await CompaniesHouseAdapter().fetch("00000099")
    lookup = signalstats.stats()["companies_house_walks"]["by_origin"]["lookup"]
    assert lookup == {
        "walks": 1, "calls_live": 1, "calls_cached": 0, "seconds": lookup["seconds"],
        "max_related": 0, "max_depth": 0,
    }


def test_walk_counters_carry_no_names_or_numbers(
    httpx_mock: HTTPXMock, _clean_walk_counters
) -> None:
    """Names and filed numbers from an unfollowed PSC stay on the bundle; the
    counters hold the reason code and nothing else."""
    import asyncio
    import json

    subject = "00000003"
    _mock_company(
        httpx_mock,
        subject,
        [
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "Zaltan Quirrelmort Holdings SARL",
                "etag": "z1",
                "identification": {"country_registered": "Luxembourg", "registration_number": "B999999"},
            }
        ],
    )
    asyncio.run(CompaniesHouseAdapter().fetch(subject))
    body = json.dumps(signalstats.stats())
    assert "Quirrelmort" not in body and "B999999" not in body and subject not in body
    assert signalstats.stats()["companies_house_walks"]["unfollowed"] == {
        "lookup|not_uk_registered": 1
    }
