"""Prozorro (Ukrainian public procurement contracts): the harvest, the index
and the adapter.

The fixture contracts are shaped from ``GET /contracts/{id}`` answers read on
23 Nov 2026: one supplier under ``identifier`` (scheme ``UA-EDR``), a
``buyer``, ``value`` in UAH, ``dateSigned`` in Kyiv time, and the fields that
are never read (``signerInfo`` naming a signatory, addresses, a buyer's
``contactPoint``). Names of people, contact details and addresses are
invented. The feed is served by an ``httpx.MockTransport``, and the index the
adapter reads is one ``build_index`` actually wrote from that harvest.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest

from opencheck import prozorro_harvest as ph
from opencheck.bods import map_prozorro_ukraine
from opencheck.config import get_settings
from opencheck.findings import finding_prozorro_ukraine
from opencheck.licensing import classify
from opencheck.routers.hit_builders import _LookupCtx
from opencheck.routers.lookup import (
    _build_derived,
    _build_result_hit,
    _dispatch,
    _register_procurement,
)
from opencheck.sources import REGISTRY, prozorro_ukraine
from opencheck.sources.prozorro_ukraine import (
    build_index,
    candidate_edrpou,
    contract_url,
    method_code,
)

EPICENTR = "32490244"
EPICENTR_LEI = "254900DZQ8GYA8D33K19"
ORANTA = "00034186"  # filed as 34186 by some buyers: padded
BUYER = "26371051"
PERSON = "3090800000"  # a ten-digit РНОКПП: never kept
SIGNATORY = "Петренко Іван Іванович"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ph.time, "sleep", lambda _s: None)


# ---------------------------------------------------------------------------
# Identifiers and words
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(EPICENTR, EPICENTR), ("34186", ORANTA), ("1234", None), ("0034186", ORANTA), (" 32490244 ", EPICENTR),
     (PERSON, None), ("", None), (None, None), ("UA32490244", None)],
)
def test_supplier_edrpou(raw, expected) -> None:
    assert ph.supplier_edrpou(raw) == expected


def test_person_codes() -> None:
    assert ph.is_person_code(PERSON)
    assert not ph.is_person_code(EPICENTR)


@pytest.mark.parametrize(
    ("code", "name", "masked"),
    [("88888888", "Оборонний постачальник", True),
     ("00000000", "ТОВ Будь-що", True),
     ("12345678", "  ОБОРОННИЙ   постачальник ", True),
     (EPICENTR, 'ТОВ "ЕПІЦЕНТР К"', False)],
)
def test_masked_suppliers(code, name, masked) -> None:
    assert ph.is_masked_supplier(code, name) is masked


def test_kyiv_day() -> None:
    # 22:30Z on 20 Nov is 21 Nov in Kyiv (UTC+2 in winter).
    assert ph.kyiv_day("2026-11-20T22:30:00Z") == "2026-11-21"
    assert ph.kyiv_day("2026-11-20T10:00:00+02:00") == "2026-11-20"
    assert ph.kyiv_day("") is None


def test_candidate_edrpou_by_registration_authority() -> None:
    assert candidate_edrpou("RA000567", EPICENTR) == EPICENTR
    assert candidate_edrpou("RA001027", EPICENTR) == EPICENTR
    assert candidate_edrpou("RA000181", EPICENTR) is None
    assert candidate_edrpou("RA000567", PERSON) is None


def test_method_codes() -> None:
    assert method_code("reporting") == "R"
    assert method_code("aboveThresholdUA.defense") == "O"
    assert method_code("somethingNew") == "X"
    assert method_code(None) == "U"


def test_contract_url() -> None:
    assert contract_url("UA-2026-11-23-009789-a-a1") == (
        "https://prozorro.gov.ua/contract/UA-2026-11-23-009789-a-a1"
    )
    assert contract_url("") == "https://prozorro.gov.ua/"


def test_the_licence_is_cc_by() -> None:
    terms = classify(REGISTRY["prozorro_ukraine"].info.license)
    assert terms.license == "CC-BY-4.0"
    assert terms.attribution_required is True


# ---------------------------------------------------------------------------
# One contract → one row
# ---------------------------------------------------------------------------


def _contract(
    cid: str,
    supplier: str,
    *,
    name: str = 'ТОВ "ЕПІЦЕНТР К"',
    status: str = "active",
    amount: Any = "1000.50",
    currency: str = "UAH",
    signed: str = "2026-03-02T10:00:00+02:00",
    tender: str = "a" * 32,
    title: str | None = "Будівельні матеріали",
    buyer: tuple[str, str] = (BUYER, "Київська міська рада"),
    ref: str | None = None,
) -> dict:
    return {
        "id": cid,
        "contractID": ref or f"UA-2026-03-02-{cid[:6]}-a-a1",
        "status": status,
        "dateSigned": signed,
        "title": title,
        "items": [{"description": "Цемент М500"}],
        "value": {"amount": amount, "currency": currency},
        "tender_id": tender,
        "mainProcurementCategory": "goods",
        "suppliers": [{
            "name": name,
            "identifier": {"scheme": "UA-EDR", "id": supplier, "legalName": name},
            "address": {"streetAddress": "вул. Прикладна, 1", "locality": "Київ"},
            "contactPoint": {"name": "Оксана Менеджерка", "telephone": "+380440000000"},
        }],
        "buyer": {
            "name": buyer[1],
            "identifier": {"scheme": "UA-EDR", "id": buyer[0], "legalName": buyer[1]},
            "contactPoint": {"name": "Марія Закупівельниця", "email": "zakupivli@example.ua"},
        },
        "signerInfo": {"name": SIGNATORY, "position": "Директор"},
    }


def test_reduce_keeps_a_legal_person() -> None:
    row, reason = ph.reduce_contract(_contract("c" * 32, EPICENTR))
    assert reason == "kept" and row is not None
    assert row.edrpou == int(EPICENTR) and row.value == 1001 and row.signed == "2026-03-02"
    assert row.buyer_edrpou == BUYER and row.title == "Будівельні матеріали"


@pytest.mark.parametrize(
    ("supplier", "name", "reason"),
    [(PERSON, "ФОП Петренко", "person"),
     ("88888888", "Оборонний постачальник", "masked"),
     ("1234", "ТОВ Коротке", "invalid_id")],
)
def test_reduce_drops(supplier, name, reason) -> None:
    assert ph.reduce_contract(_contract("c" * 32, supplier, name=name)) == (None, reason)


def test_reduce_drops_joint_and_missing_suppliers() -> None:
    doc = _contract("c" * 32, EPICENTR)
    doc["suppliers"] = doc["suppliers"] * 2
    assert ph.reduce_contract(doc) == (None, "joint")
    doc["suppliers"] = []
    assert ph.reduce_contract(doc) == (None, "no_supplier")


def test_reduce_falls_back_to_the_first_item() -> None:
    row, _ = ph.reduce_contract(_contract("c" * 32, EPICENTR, title=None))
    assert row is not None and row.title == "Цемент М500"


# ---------------------------------------------------------------------------
# The harvest, against a fake API
# ---------------------------------------------------------------------------


class FakeProzorro:
    """Two feeds and the contracts behind them, paged by an offset."""

    def __init__(self, contract_pages: list[list[dict]], tender_pages: list[list[dict]]):
        self.contract_pages = contract_pages
        self.tender_pages = tender_pages
        #: The tenders feed read backwards (``descending=1``): page i, with
        #: the timestamp its next offset stands at.
        self.back_pages: list[tuple[list[dict], int]] = []
        self.docs: dict[str, dict] = {}
        self.tender_docs: dict[str, dict] = {}
        self.fail_contract: set[str] = set()
        self.calls: list[str] = []

    def _back(self, offset: str) -> httpx.Response:
        index = int(offset.split(".")[1]) if offset.count(".") >= 2 else 0
        if index >= len(self.back_pages):
            return httpx.Response(200, json={"data": [], "next_page": {"offset": offset}})
        rows, moment = self.back_pages[index]
        return httpx.Response(200, json={"data": rows, "next_page": {"offset": f"{moment}.{index + 1}.1.y"}})

    def _feed(self, pages: list[list[dict]], offset: str) -> httpx.Response:
        index = int(offset.split(".")[1]) if offset.count(".") else 0
        rows = pages[index] if index < len(pages) else []
        nxt = f"1780000000.{index + 1}.1.x"
        return httpx.Response(200, json={"data": rows, "next_page": {"offset": nxt}})

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append(path)
        offset = request.url.params.get("offset", "")
        if path.endswith("/tenders"):
            if request.url.params.get("descending") == "1":
                return self._back(offset)
            return self._feed(self.tender_pages, offset)
        if "/tenders/" in path:
            tid = path.rsplit("/", 1)[-1]
            if tid in self.fail_contract:
                return httpx.Response(503)
            tdoc = self.tender_docs.get(tid)
            return httpx.Response(404) if tdoc is None else httpx.Response(200, json={"data": tdoc})
        if path.endswith("/contracts"):
            assert "opt_fields" not in request.url.params
            return self._feed(self.contract_pages, offset)
        cid = path.rsplit("/", 1)[-1]
        if cid in self.fail_contract:
            return httpx.Response(503)
        doc = self.docs.get(cid)
        return httpx.Response(404) if doc is None else httpx.Response(200, json={"data": doc})


def _ids(n: int, start: int = 0) -> list[str]:
    return [f"{i:032x}" for i in range(start, start + n)]


T1, T2 = "1" * 32, "2" * 32


def _fake() -> FakeProzorro:
    c = _ids(7, 16)
    fake = FakeProzorro(
        contract_pages=[[{"id": c[0]}, {"id": c[1]}, {"id": c[2]}], [{"id": c[3]}, {"id": c[4]}, {"id": c[5]}, {"id": c[6]}]],
        tender_pages=[[{"id": T1, "procurementMethodType": "reporting", "contracts": [{}]},
                       {"id": T2, "procurementMethodType": "aboveThreshold", "contracts": [{}]},
                       {"id": "3" * 32, "procurementMethodType": "belowThreshold"}]],
    )
    fake.docs = {
        c[0]: _contract(c[0], EPICENTR, amount="10559", tender=T1, signed="2026-01-13T09:00:00+02:00"),
        # Signed late in the evening UTC: the next day in Kyiv.
        c[1]: _contract(c[1], EPICENTR, amount="3656", tender=T2, signed="2026-03-02T22:30:00Z",
                        buyer=("04011111", "Львівська обласна рада"), title="Продукти харчування"),
        c[2]: _contract(c[2], PERSON, name="ФОП Петренко"),
        c[3]: _contract(c[3], EPICENTR, status="terminated", amount="700", tender="9" * 32,
                        signed="2026-02-01T09:00:00+02:00"),
        c[4]: _contract(c[4], EPICENTR, status="cancelled", amount="999999"),
        c[5]: _contract(c[5], "88888888", name="Оборонний постачальник"),
        c[6]: _contract(c[6], "0034186", name='НАСК "ОРАНТА"', amount="5000", currency="EUR"),
    }
    return fake


def _harvest(store_dir: Path, fake: FakeProzorro) -> tuple[ph.Store, dict]:
    store = ph.Store(store_dir / ph.STORE_NAME)
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        result = {**ph.harvest_methods(http, store), **ph.harvest(http, store, workers=2)}
    return store, result


def test_harvest_reads_the_feed_and_keeps_legal_persons(tmp_path: Path) -> None:
    store, result = _harvest(tmp_path, _fake())
    assert result["outcome"] == "caught_up" and result["tenders_outcome"] == "caught_up"
    assert result["contracts_read"] == 7
    assert result["kept"] == 5  # active ×2, terminated, cancelled, EUR
    assert result["skipped_person"] == 1 and result["skipped_masked"] == 1
    assert result["tenders_with_contracts"] == 2
    assert store.get("started_from") and store.get("caught_up_at")
    assert store.get("contracts_cursor") == "1780000000.2.1.x"
    store.close()


def test_harvest_never_stores_people_or_contacts(tmp_path: Path) -> None:
    store, _ = _harvest(tmp_path, _fake())
    dump = "\n".join(store.conn.iterdump())
    store.close()
    for text in (SIGNATORY, "Оксана", "Марія", "zakupivli@", "+380", "Прикладна", "Петренко", PERSON,
                 "Оборонний"):
        assert text not in dump


def test_a_second_run_reads_from_the_cursor_and_removes_a_contract_turned_person(tmp_path: Path) -> None:
    fake = _fake()
    store, _ = _harvest(tmp_path, fake)
    store.close()
    first = _ids(1, 16)[0]
    # The contract changes: now names a person. It comes back on a new page.
    fake.contract_pages.append([{"id": first}])
    fake.docs[first] = _contract(first, PERSON, name="ФОП Петренко")
    fake.calls.clear()
    store, result = _harvest(tmp_path, fake)
    assert result["contracts_read"] == 1 and result["removed"] == 1
    assert store.count() == 4
    store.close()


def test_a_failed_contract_does_not_move_the_cursor(tmp_path: Path) -> None:
    fake = _fake()
    fake.fail_contract.add(_ids(1, 19)[0])  # on the second page
    store, result = _harvest(tmp_path, fake)
    assert result["outcome"] == "contract_failed"
    assert result["pages"] == 1
    assert store.get("contracts_cursor") == "1780000000.1.1.x"
    store.close()


def test_a_listing_that_cannot_be_read_raises(tmp_path: Path) -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502)

    store = ph.Store(tmp_path / ph.STORE_NAME)
    with httpx.Client(transport=httpx.MockTransport(broken)) as http, pytest.raises(ph.HarvestError):
        ph.harvest(http, store)
    store.close()


def test_the_store_refuses_another_schema(tmp_path: Path) -> None:
    store = ph.Store(tmp_path / ph.STORE_NAME)
    store.set("schema_version", "0")
    store.close()
    with pytest.raises(ph.HarvestError):
        ph.Store(tmp_path / ph.STORE_NAME)


# ---------------------------------------------------------------------------
# Procedures the forward read misses (24 Nov 2026)
# ---------------------------------------------------------------------------

T9 = "9" * 32  # the terminated contract's tender, last changed before the start
TA = "a" * 32  # ОРАНТА's tender


def _start_moment(store: ph.Store) -> int:
    return int(float(str(store.get("started_from")).split(".")[0]))


def test_the_tenders_feed_is_read_backwards_to_the_floor(tmp_path: Path) -> None:
    fake = _fake()
    store, _ = _harvest(tmp_path, fake)
    start = _start_moment(store)
    day = 86400
    fake.back_pages = [
        # T1 again with another procedure: the forward read's value stands.
        ([{"id": T9, "procurementMethodType": "reporting", "contracts": [{}]},
          {"id": T1, "procurementMethodType": "belowThreshold", "contracts": [{}]},
          {"id": "4" * 32, "procurementMethodType": "reporting"}], start - 30 * day),
        ([{"id": TA, "procurementMethodType": "priceQuotation", "contracts": [{}]}],
         start - (ph.BACK_DAYS + 1) * day),
        ([{"id": "5" * 32, "procurementMethodType": "reporting", "contracts": [{}]}], start - 800 * day),
    ]
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        result = ph.harvest_methods_back(http, store)
    assert result["tenders_back_outcome"] == "done"
    assert result["tender_back_pages"] == 2  # stopped once past the floor
    assert store.get("tenders_back_done")
    methods = {ph.hex_id(k): m for k, m in store.conn.execute("SELECT id, method FROM tender_method")}
    assert methods[T9] == "reporting" and methods[TA] == "priceQuotation"
    assert methods[T1] == "reporting"  # not overwritten
    assert "4" * 32 not in methods and "5" * 32 not in methods
    assert store.missing_methods(10) == []
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        assert ph.harvest_methods_back(http, store)["tenders_back_outcome"] == "done"
    store.close()


def test_the_backward_read_resumes_from_its_cursor(tmp_path: Path) -> None:
    fake = _fake()
    store, _ = _harvest(tmp_path, fake)
    start = _start_moment(store)
    fake.back_pages = [([{"id": T9, "procurementMethodType": "reporting", "contracts": [{}]}], start - 86400)]
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        first = ph.harvest_methods_back(http, store, deadline=0.0)
    assert first["tenders_back_outcome"] == "deadline" and first["tender_back_pages"] == 0
    assert not store.get("tenders_back_done")
    store.close()


def test_missing_tenders_are_read_one_at_a_time(tmp_path: Path) -> None:
    fake = _fake()
    store, _ = _harvest(tmp_path, fake)
    assert set(store.missing_methods(10)) == {T9, TA}
    fake.tender_docs = {T9: {"id": T9, "procurementMethodType": "reporting"}}  # TA: 404
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        result = ph.fill_missing_methods(http, store, workers=2)
    assert result == {"fill_outcome": "done", "tenders_filled": 1, "tenders_gone": 1,
                      "tenders_fill_failed": 0}
    methods = {ph.hex_id(k): m for k, m in store.conn.execute("SELECT id, method FROM tender_method")}
    assert methods[T9] == "reporting" and methods[TA] == ""  # gone: never asked again
    assert store.missing_methods(10) == []
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        assert ph.fill_missing_methods(http, store)["fill_outcome"] == "none_missing"
    store.close()


def test_a_tender_that_fails_is_left_for_the_next_run(tmp_path: Path) -> None:
    fake = _fake()
    store, _ = _harvest(tmp_path, fake)
    fake.tender_docs = {T9: {"id": T9, "procurementMethodType": "reporting"}}
    fake.fail_contract.add(TA)
    with httpx.Client(transport=httpx.MockTransport(fake)) as http:
        result = ph.fill_missing_methods(http, store, workers=2)
    assert result["tenders_filled"] == 1 and result["tenders_fill_failed"] == 1
    assert store.missing_methods(10) == [TA]
    store.close()


# ---------------------------------------------------------------------------
# The index
# ---------------------------------------------------------------------------


@pytest.fixture()
def built(tmp_path: Path):
    store, _ = _harvest(tmp_path / "harvest", _fake())
    store.close()
    out = tmp_path / "data" / "prozorro_ukraine.sqlite"
    meta = build_index(tmp_path / "harvest" / ph.STORE_NAME, out)
    os.environ["PROZORRO_DB_FILE"] = str(out)
    get_settings.cache_clear()
    prozorro_ukraine.reset_connection()
    yield out, meta
    os.environ.pop("PROZORRO_DB_FILE", None)
    get_settings.cache_clear()
    prozorro_ukraine.reset_connection()


@pytest.fixture()
def no_index(tmp_path: Path):
    os.environ.pop("PROZORRO_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    prozorro_ukraine.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    prozorro_ukraine.reset_connection()


def test_builder_counts(built) -> None:
    path, meta = built
    assert meta["contracts_in_harvest"] == "5"
    assert meta["contracts_counted"] == "4"
    assert meta["contracts_cancelled"] == "1"
    assert meta["method_not_read"] == "2"  # the terminated one and ОРАНТА's
    assert meta["data_from"] == "2026-01-13" and meta["data_to"] == "2026-03-03"
    assert meta["harvest_started_from"] and meta["harvest_cursor"]
    conn = sqlite3.connect(path)
    assert {r[0] for r in conn.execute("SELECT edrpou FROM supplier")} == {int(EPICENTR), int(ORANTA)}


def test_cancelled_contracts_are_not_counted_and_terminated_are(built) -> None:
    path, _ = built
    row = sqlite3.connect(path).execute(
        "SELECT contracts, terminated, value_uah, buyers, methods FROM supplier WHERE edrpou = ?",
        (int(EPICENTR),),
    ).fetchone()
    assert row[:4] == (3, 1, 10559 + 3656 + 700, 2)
    assert json.loads(row[4]) == {"O": 1, "R": 1, "U": 1}


@pytest.mark.parametrize(
    ("day", "expected"),
    [("2026-01-13", "2026-01-13"), ("2015-01-01", "2015-01-01"), ("2004-03-16", None),
     ("2014-12-31", None), ("2027-01-01", None), ("", None), (None, None), ("2026-1-3", None)],
)
def test_plausible_signing_days(day, expected) -> None:
    assert prozorro_ukraine.plausible_day(day, "2026-11-25") == expected


def test_an_impossible_signing_day_is_counted_without_its_date(tmp_path: Path) -> None:
    fake = _fake()
    old = _ids(1, 40)[0]
    fake.contract_pages.append([{"id": old}])
    fake.docs[old] = _contract(old, EPICENTR, amount="100", signed="2004-03-16T10:00:00+02:00")
    store, _ = _harvest(tmp_path / "harvest", fake)
    store.close()
    out = tmp_path / "prozorro_ukraine.sqlite"
    meta = build_index(tmp_path / "harvest" / ph.STORE_NAME, out)
    assert meta["signed_date_implausible"] == "1"
    assert meta["data_from"] == "2026-01-13"
    conn = sqlite3.connect(out)
    assert conn.execute(
        "SELECT contracts, first_date FROM supplier WHERE edrpou = ?", (int(EPICENTR),)
    ).fetchone() == (4, "2026-01-13")
    assert conn.execute("SELECT signed FROM contract WHERE id = ?", (old,)).fetchone() == (None,)


def test_another_currency_is_counted_not_converted(built) -> None:
    path, _ = built
    row = sqlite3.connect(path).execute(
        "SELECT contracts, value_uah, other_currency FROM supplier WHERE edrpou = ?", (int(ORANTA),)
    ).fetchone()
    assert row == (1, 0, 1)


def test_an_empty_harvest_refuses_to_build(tmp_path: Path) -> None:
    store = ph.Store(tmp_path / ph.STORE_NAME)
    store.close()
    with pytest.raises(prozorro_ukraine.IndexBuildError):
        build_index(tmp_path / ph.STORE_NAME, tmp_path / "out.sqlite")
    assert not (tmp_path / "out.sqlite").exists()


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_fetch_by_identifiers(built) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch_by_identifiers(
        EPICENTR_LEI, EPICENTR, "RA000567", legal_name='ТОВ "ЕПІЦЕНТР К"'
    )
    assert bundle["edrpou"] == EPICENTR and bundle["is_stub"] is False
    supplier = bundle["supplier"]
    assert supplier["name"] == 'ТОВ "ЕПІЦЕНТР К"'
    assert supplier["terminated"] == 1
    assert supplier["methods"] == {
        "Purchase without the electronic system, reported (direct contract)": 1,
        "Open or competitive tender": 1,
        "Procedure not read": 1,
    }
    contracts = bundle["contracts"]
    assert [c["signed"] for c in contracts] == ["2026-03-03", "2026-02-01", "2026-01-13"]
    latest = contracts[0]
    assert latest["buyer"] == "Львівська обласна рада"
    assert latest["procedure"] == "Open or competitive tender"
    assert latest["url"].startswith("https://prozorro.gov.ua/contract/UA-2026-03-02-")
    assert contracts[1]["status"] == "terminated"
    assert [b["name"] for b in bundle["buyers"]] == ["Київська міська рада", "Львівська обласна рада"]
    assert bundle["harvest_started_from"]


async def test_fetch_a_company_with_no_contracts(built) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch(BUYER)
    assert bundle["not_found"] is True and bundle["is_stub"] is False


async def test_fetch_without_an_index_is_a_stub(no_index) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch(EPICENTR)
    assert bundle["is_stub"] is True


async def test_search_is_empty(built) -> None:
    from opencheck.sources.base import SearchKind

    assert await REGISTRY["prozorro_ukraine"].search("Епіцентр", SearchKind.ENTITY) == []


def test_not_a_register_adapter() -> None:
    adapter = REGISTRY["prozorro_ukraine"]
    assert adapter.lookup_derivers == ()
    assert adapter.info.is_national_register is False


# ---------------------------------------------------------------------------
# Finding, hit and BODS
# ---------------------------------------------------------------------------


async def test_finding(built) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch(EPICENTR)
    assert finding_prozorro_ukraine(bundle) == (
        "Signed 3 Ukrainian public contracts January 2026 to March 2026, 1 since ended, "
        "UAH values totalling 14,915, with 2 buyers."
    )


def test_finding_keeps_the_ended_count_on_a_large_supplier() -> None:
    # Epicentr K on production, 24 Nov 2026: the ended count fell off the
    # 140-character cap when it came last. Now the buyers go first.
    bundle = {"supplier": {"contracts": 10055, "terminated": 3311, "value_uah": 187844514,
                           "buyers": 2187, "first_date": "2020-10-09", "last_date": "2026-01-02"}}
    sentence = finding_prozorro_ukraine(bundle)
    assert sentence == (
        "Signed 10,055 Ukrainian public contracts October 2020 to January 2026, "
        "3,311 since ended, UAH values totalling 187,844,514."
    )
    assert "terminated" not in sentence


async def test_result_hit_asserts_no_identifier(built) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch(EPICENTR)
    hit = _build_result_hit("prozorro_ukraine", bundle, _LookupCtx(lei="X", legal_name="Epicentr"))
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.summary == f"3 Ukrainian public contracts · 2026 · UA-EDR {EPICENTR}"


async def test_no_card_for_a_company_with_no_contracts(built) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch(BUYER)
    assert _build_result_hit("prozorro_ukraine", bundle, _LookupCtx(lei="X")) is None


async def test_mapper_emits_one_entity_with_the_edrpou(built) -> None:
    bundle = await REGISTRY["prozorro_ukraine"].fetch(EPICENTR)
    statements = list(map_prozorro_ukraine(bundle))
    assert len(statements) == 1
    rd = statements[0]["recordDetails"]
    assert rd["identifiers"][0]["id"] == EPICENTR
    assert rd["identifiers"][0]["scheme"] == "UA-EDR"
    assert rd["jurisdiction"]["code"] == "UA"
    assert "UAH 14,915" in rd["entityType"]["details"]
    assert "1 of them since ended" in rd["entityType"]["details"]
    assert statements[0]["statementDate"] == "2026-03-03"


async def test_statements_pass_libcovebods(built, tmp_path: Path) -> None:
    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    bundle = await REGISTRY["prozorro_ukraine"].fetch(EPICENTR)
    path = tmp_path / "prozorro.json"
    path.write_text(json.dumps(list(map_prozorro_ukraine(bundle))), encoding="utf-8")
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]


# ---------------------------------------------------------------------------
# Lookup wiring
# ---------------------------------------------------------------------------


def _close(tasks) -> set[str]:
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_for_an_edrpou(built) -> None:
    ctx = _LookupCtx(lei=EPICENTR_LEI, jurisdiction="UA", registered_as=EPICENTR)
    _build_derived(ctx, "RA000567")
    assert _close(_dispatch(ctx, only="prozorro_ukraine")) == {"prozorro_ukraine"}


@pytest.mark.parametrize(
    ("jurisdiction", "ra", "registered_as"),
    [("EE", "RA000181", "10421629"), ("UA", "RA000567", PERSON), ("UA", "RA000567", "1234")],
)
def test_not_announced_for_an_anchor_it_cannot_answer(built, jurisdiction, ra, registered_as) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction=jurisdiction, registered_as=registered_as)
    _build_derived(ctx, ra)
    assert _close(_dispatch(ctx, only="prozorro_ukraine")) == set()


def test_not_announced_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="UA", registered_as=EPICENTR)
    _build_derived(ctx, "RA000567")
    assert _close(_dispatch(ctx, only="prozorro_ukraine")) == set()


def test_a_register_lookup_on_the_edrpou_asks_prozorro(built) -> None:
    assert _close(_register_procurement("UA-EDR", EPICENTR, "Epicentr")) == {"prozorro_ukraine"}
    assert _close(_register_procurement("UA-EDR", PERSON, "")) == set()


def test_a_register_lookup_without_an_index_does_not(no_index) -> None:
    assert _close(_register_procurement("UA-EDR", EPICENTR, "Epicentr")) == set()


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("PROZORRO_DB_URL", "")
    get_settings.cache_clear()
    assert prozorro_ukraine.warm_index()["prozorro_ukraine"].startswith("absent")


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(entity_pages, "download_db", boom)
    assert prozorro_ukraine.warm_index()["prozorro_ukraine"].startswith("failed")
