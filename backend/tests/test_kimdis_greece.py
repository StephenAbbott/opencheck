"""ΚΗΜΔΗΣ — Greek public contracts, read live by the ΑΦΜ ΓΕΜΗ publishes.

Record shapes are cut from real answers of 12–13 Nov 2026 (ΑΚΤΩΡ ΑΤΕ, ΑΦΜ
094207780; OTE, 094019245), with the authority officer's e-mail and the
signer replaced by placeholders.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from opencheck import degradation
from opencheck.bods import map_kimdis_greece
from opencheck.findings import finding_kimdis_greece
from opencheck.sources import REGISTRY
from opencheck.sources import kimdis_greece as k
from opencheck.sources.schemas import kimdis_greece as schema

AKTOR = "094207780"
OTE = "094019245"


def record(adam: str, **over: Any) -> dict[str, Any]:
    """One ΚΗΜΔΗΣ contract record, in the API's shape."""
    base: dict[str, Any] = {
        "title": f"Σύμβαση {adam}",
        "referenceNumber": adam,
        "prevReferenceNo": None,
        "submissionDate": "2026-10-12T10:00:00.000",
        "contractSignedDate": "2026-10-07",
        "startDate": "2026-10-07",
        "noEndDate": False,
        "endDate": "2027-10-06",
        "cancelled": False,
        "contractNumber": "12/2026",
        "authorEmail": "officer@example.gr",
        "organizationVatNumber": "090000045",
        "organization": {"key": "6006", "value": "ΔΗΜΟΣ ΘΕΣΣΑΛΟΝΙΚΗΣ"},
        "contractBudget": None,
        "totalCostWithoutVAT": 1000.4,
        "bidsSubmitted": 3,
        "procedureType": {"key": "1", "value": "Ανοιχτή διαδικασία"},
        "contractType": {"key": "1", "value": "Έργα"},
        "contractingDataDetails": {
            "signers": {"key": "1", "value": "Placeholder Person - Δήμαρχος"},
            "unitsOperator": {"key": "2", "value": "ΔΗΜΑΡΧΟΣ"},
            "contractingMembersDataList": [
                {"vatNumber": AKTOR, "name": "ΑΚΤΩΡ ΑΤΕ", "country": {"key": "GR"}},
            ],
        },
    }
    base.update(over)
    return base


def page(content: list[dict[str, Any]], total_pages: int = 1) -> dict[str, Any]:
    return {
        "content": content,
        "totalPages": total_pages,
        "totalElements": len(content),
        "last": True,
        "size": 50,
    }


class _MemoryCache:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}

    def get_payload(self, key: str, max_age_days: float | None = None) -> Any:
        return (self.store[key], "2026-11-13T00:00:00Z") if key in self.store else None

    def put(self, key: str, payload: Any) -> None:
        self.store[key] = payload


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> k.KimdisGreeceAdapter:
    settings = MagicMock(allow_live=True, gemi_api_key="test-key")
    monkeypatch.setattr(k, "get_settings", lambda: settings)
    monkeypatch.setattr(k, "_RATE_PER_MINUTE", 60_000.0)
    k._BUCKETS.clear()
    a = k.KimdisGreeceAdapter()
    a._cache = _MemoryCache()  # type: ignore[assignment]
    return a


def _serve(answers: dict[tuple[str, int], Any]):
    """A respx side effect: (dateFrom, page) → payload, an int status, or an
    exception. Anything unlisted answers 404 ("No contracts found")."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        key = (body["dateFrom"], int(request.url.params["page"]))
        answer = answers.get(key)
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, int):
            return httpx.Response(answer, json={"status": answer})
        if answer is None:
            return httpx.Response(
                404, json={"message": "No contracts found for the given criteria", "status": 404}
            )
        return httpx.Response(200, json=answer)

    return handler


def _newest() -> str:
    return k.windows(date.today())[0][0].isoformat()


# ---------------------------------------------------------------------------
# The ΑΦΜ and the windows
# ---------------------------------------------------------------------------


def test_the_afm_check_digit() -> None:
    assert k.afm_valid(AKTOR) and k.afm_valid(OTE)
    assert not k.afm_valid("094207781")
    assert not k.afm_valid("000000000")
    assert k.normalise_afm("EL094019245") == OTE
    assert k.normalise_afm("94019245") == OTE
    assert k.normalise_afm("094207781") == ""
    assert k.normalise_afm(None) == ""


def test_a_gemi_number_under_either_greek_authority() -> None:
    assert k.gemi_number("RA000685", "006556101000") == "006556101000"
    assert k.gemi_number("RA000376", "000251501000") == "000251501000"
    assert k.gemi_number("RA000585", "006556101000") == ""
    assert k.gemi_number("RA000685", "ΑΒΓ") == ""
    assert k.KimdisGreeceAdapter.applies_to("RA000685", "006556101000")
    assert not k.KimdisGreeceAdapter.applies_to("RA000014", "086174781")


def test_windows_are_180_days_newest_first_from_2017() -> None:
    spans = k.windows(date(2026, 11, 13))
    assert spans[0][1] == date(2026, 11, 13)
    assert spans[-1][0] == date(2017, 1, 1)
    assert all((end - start).days <= 179 for start, end in spans)
    # Contiguous: each window starts the day after the older one ends.
    for newer, older in zip(spans, spans[1:]):
        assert (newer[0] - older[1]).days == 1


# ---------------------------------------------------------------------------
# Reading records
# ---------------------------------------------------------------------------


def test_a_record_keeps_no_person_and_no_other_contractor() -> None:
    joint = record(
        "26SYMV1",
        contractingDataDetails={
            "signers": {"value": "Placeholder Person"},
            "contractingMembersDataList": [
                {"vatNumber": AKTOR, "name": "ΑΚΤΩΡ ΑΤΕ"},
                {"vatNumber": "123456783", "name": "Placeholder Sole Trader"},
            ],
        },
    )
    reduced = k.reduce_record(joint, AKTOR)
    assert reduced is not None
    text = json.dumps(reduced, ensure_ascii=False)
    assert "officer@example.gr" not in text
    assert "Placeholder" not in text
    assert "123456783" not in text
    assert reduced["other_contractors"] == 1
    assert reduced["value_eur"] == 1000


def test_a_record_not_naming_the_afm_is_dropped() -> None:
    other = record(
        "26SYMV2",
        contractingDataDetails={"contractingMembersDataList": [{"vatNumber": OTE}]},
    )
    assert k.reduce_record(other, AKTOR) is None


def test_an_amended_contract_is_counted_once_at_its_latest_version() -> None:
    rows = [
        k.reduce_record(record("A1", totalCostWithoutVAT=100), AKTOR),
        k.reduce_record(record("A2", prevReferenceNo="A1", totalCostWithoutVAT=150), AKTOR),
        k.reduce_record(record("A3", prevReferenceNo="A2", totalCostWithoutVAT=175), AKTOR),
        k.reduce_record(
            record("B1", totalCostWithoutVAT=50, organization={"key": "9", "value": "ΠΑΝΕΠΙΣΤΗΜΙΟ"}),
            AKTOR,
        ),
    ]
    summary = k.summarise([r for r in rows if r], AKTOR)
    supplier = summary["supplier"]
    assert supplier["contracts"] == 2
    assert supplier["value_eur"] == 225
    assert supplier["amended"] == 1
    assert supplier["records_read"] == 4
    assert {c["adam"] for c in summary["contracts"]} == {"A3", "B1"}
    assert summary["buyers"][0]["name"] == "ΔΗΜΟΣ ΘΕΣΣΑΛΟΝΙΚΗΣ"


def test_the_schema_refuses_a_person_field_on_a_contract() -> None:
    with pytest.raises(ValueError):
        schema.KimdisContract.model_validate({"adam": "X", "authorEmail": "a@b.gr"})


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_reads_every_page_and_caches(adapter: k.KimdisGreeceAdapter) -> None:
    newest = _newest()
    route = respx.post(k.CONTRACT_URL).mock(side_effect=_serve({
        (newest, 0): page([record("P0")], total_pages=2),
        (newest, 1): page([record("P1", totalCostWithoutVAT=2000)], total_pages=2),
    }))
    with degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch(AKTOR, legal_name="ΑΚΤΩΡ"))
    assert bundle["is_stub"] is False
    assert bundle["supplier"]["contracts"] == 2
    assert bundle["supplier"]["value_eur"] == 3000
    assert bundle["read_from"] == "2017-01-01"
    assert bundle["truncated"] is False and bundle["gaps"] == []
    assert recorded == []
    assert all(c["url"].endswith(c["adam"]) for c in bundle["contracts"])
    calls = route.call_count
    asyncio.run(adapter.fetch(AKTOR))
    assert route.call_count == calls  # the second read is the cache


@respx.mock
def test_no_contracts_is_an_answer_with_no_card(adapter: k.KimdisGreeceAdapter) -> None:
    respx.post(k.CONTRACT_URL).mock(side_effect=_serve({}))
    with degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch(AKTOR))
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["supplier"] is None
    assert recorded == []
    assert list(map_kimdis_greece(bundle)) == []
    assert finding_kimdis_greece(bundle) is None


@respx.mock
def test_a_window_that_never_answers_is_a_stated_gap(adapter: k.KimdisGreeceAdapter) -> None:
    spans = k.windows(date.today())
    bad = spans[5][0].isoformat()
    respx.post(k.CONTRACT_URL).mock(side_effect=_serve({
        (spans[0][0].isoformat(), 0): page([record("N0")]),
        (bad, 0): httpx.ReadTimeout("hung"),
        (spans[8][0].isoformat(), 0): page([record("O0", submissionDate="2019-01-01T00:00:00")]),
    }))
    with degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch(AKTOR))
    assert bundle["supplier"]["contracts"] == 2  # windows either side still read
    assert bundle["gaps"] == [{"from": bad, "to": spans[5][1].isoformat()}]
    assert bundle["incomplete"] is True
    assert [(d.check, d.reason) for d in recorded] == [
        (degradation.CHECK_SOURCE_READ, degradation.REASON_TIMEOUT)
    ]
    assert adapter._cache.store == {}  # type: ignore[attr-defined]


@respx.mock
def test_no_window_answering_is_a_failure_not_no_contracts(
    adapter: k.KimdisGreeceAdapter,
) -> None:
    respx.post(k.CONTRACT_URL).mock(return_value=httpx.Response(503))
    with degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch(AKTOR))
    assert bundle["is_stub"] is True and bundle["not_found"] is False
    assert [d.check for d in recorded] == [degradation.CHECK_SOURCE_FETCH]
    assert adapter._cache.store == {}  # type: ignore[attr-defined]


@respx.mock
def test_the_call_cap_stops_the_read_and_says_from_when(
    adapter: k.KimdisGreeceAdapter, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(k, "MAX_CALLS", 4)
    monkeypatch.setattr(k, "_CONCURRENCY", 1)
    spans = k.windows(date.today())
    respx.post(k.CONTRACT_URL).mock(side_effect=_serve({
        (spans[0][0].isoformat(), 0): page([record("N0")]),
    }))
    with degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch(AKTOR))
    assert bundle["truncated"] is True
    assert bundle["read_from"] is not None and bundle["read_from"] > "2017-01-01"
    assert recorded == []
    assert "contracts posted since" in (finding_kimdis_greece(bundle) or "")


def test_no_live_reads_is_a_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(k, "get_settings", lambda: MagicMock(allow_live=False, gemi_api_key=""))
    a = k.KimdisGreeceAdapter()
    a._cache = _MemoryCache()  # type: ignore[assignment]
    assert asyncio.run(a.fetch(AKTOR))["is_stub"] is True
    assert a.info.live_available is False


# ---------------------------------------------------------------------------
# The ΑΦΜ from ΓΕΜΗ
# ---------------------------------------------------------------------------


class _Gemi:
    def __init__(self, company: dict[str, Any] | None, fail: bool = False) -> None:
        self.company = company
        self.fail = fail
        self.calls = 0

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        self.calls += 1
        if self.fail:
            degradation.record("gemi_greece", "ΓΕΜΗ returned HTTP 503")
        return {"company": self.company, "is_stub": self.company is None}


def _with_gemi(gemi: _Gemi):
    return patch.dict(REGISTRY, {"gemi_greece": gemi})


@respx.mock
def test_the_afm_comes_from_gemi(adapter: k.KimdisGreeceAdapter) -> None:
    respx.post(k.CONTRACT_URL).mock(side_effect=_serve({(_newest(), 0): page([record("G0")])}))
    gemi = _Gemi({"afm": AKTOR, "arGemi": "6556101000"})
    with _with_gemi(gemi), degradation.recording():
        bundle = asyncio.run(
            adapter.fetch_by_identifiers("213800W5DCON145PRT93", "006556101000", "RA000685")
        )
    assert gemi.calls == 1
    assert bundle["afm"] == AKTOR and bundle["gr_argemi"] == "006556101000"
    assert bundle["supplier"]["contracts"] == 1


def test_gemi_not_answering_is_a_degradation_never_no_contracts(
    adapter: k.KimdisGreeceAdapter,
) -> None:
    with _with_gemi(_Gemi(None, fail=True)), degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch_by_identifiers("L", "006556101000", "RA000685"))
    assert bundle["is_stub"] is True and bundle["not_found"] is False
    assert any(
        d.source_id == "kimdis_greece" and d.check == degradation.CHECK_SOURCE_FETCH
        for d in recorded
    )


def test_gemi_with_no_afm_is_an_answer(adapter: k.KimdisGreeceAdapter) -> None:
    with _with_gemi(_Gemi({"afm": "", "arGemi": "1"})), degradation.recording() as recorded:
        bundle = asyncio.run(adapter.fetch_by_identifiers("L", "000000001000", "RA000685"))
    assert bundle["no_afm"] is True and bundle["is_stub"] is False
    assert not [d for d in recorded if d.source_id == "kimdis_greece"]


def test_a_non_greek_filing_is_not_dispatched(adapter: k.KimdisGreeceAdapter) -> None:
    assert asyncio.run(adapter.fetch_by_identifiers("L", "086174781", "RA000014")) is None


# ---------------------------------------------------------------------------
# BODS, finding, hit
# ---------------------------------------------------------------------------


def _bundle(**over: Any) -> dict[str, Any]:
    summary = k.summarise([k.reduce_record(record("Z1"), AKTOR)], AKTOR)  # type: ignore[list-item]
    bundle = {
        "source_id": "kimdis_greece",
        "afm": AKTOR,
        "legal_name": "ΑΚΤΩΡ ΑΝΩΝΥΜΗ ΤΕΧΝΙΚΗ ΕΤΑΙΡΕΙΑ",
        "read_from": "2017-01-01",
        "truncated": False,
        "gaps": [],
        "link": k.HOMEPAGE,
        "is_stub": False,
        "not_found": False,
        **summary,
    }
    bundle.update(over)
    return bundle


def test_the_mapper_writes_one_entity_with_the_afm() -> None:
    statements = list(map_kimdis_greece(_bundle()))
    assert len(statements) == 1
    rd = statements[0]["recordDetails"]
    assert statements[0]["recordType"] == "entity"
    assert rd["identifiers"] == [
        {"id": AKTOR, "scheme": "GR-AFM", "schemeName": "Greek Tax Registry Number (ΑΦΜ)"}
    ]
    assert "EUR 1,000" in rd["entityType"]["details"]
    assert statements[0]["statementDate"] == "2026-10-12"


def test_the_finding_is_a_sum_of_values_never_ownership() -> None:
    sentence = finding_kimdis_greece(_bundle()) or ""
    assert sentence.startswith("Greek public contracts 2026: 1 contract")
    assert "EUR 1,000" in sentence
    for word in ("own", "control", "paid"):
        assert word not in sentence.lower()


def test_the_hit_asserts_no_identifier() -> None:
    from opencheck.routers.lookup import _build_result_hit

    ctx = MagicMock(lei="213800W5DCON145PRT93", legal_name="ΑΚΤΩΡ")
    hit = _build_result_hit("kimdis_greece", _bundle(), ctx)
    assert hit is not None and hit.identifiers == {}
    assert "ΑΦΜ 094207780" in hit.summary
    assert _build_result_hit("kimdis_greece", _bundle(supplier=None), ctx) is None
