"""AusTender (Australian Government contract notices).

The fixture releases are shaped from the OCP Data Registry's compiled
releases of 12 Nov 2026 (one contract per release, the ABN in
``parties/additionalIdentifiers``, buyers only in ``parties.roles``, period
dates as Canberra midnights in UTC) and run through the real builder, so the
index the adapter reads is one ``build_index`` actually wrote.
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
from pathlib import Path

import pytest

from opencheck.bods import map_austender
from opencheck.config import get_settings
from opencheck.findings import finding_austender
from opencheck.routers.hit_builders import _LookupCtx
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch
from opencheck.sources import REGISTRY, austender
from opencheck.sources.austender import (
    abn_valid,
    abns_for_acn,
    build_index,
    candidate_abns,
    format_abn,
    iter_releases,
    local_day,
    notice_uuid,
)

TELSTRA_ACN = "086174781"
TELSTRA_ABN = "64086174781"
MADDOCKS_ABN = "63478951337"
DEFENCE_ABN = "62950639680"
UUID_HEX = "c8309686aa5a4c6ebfa571028c844fbb"


# ---------------------------------------------------------------------------
# ABN / ACN grammar
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("abn", [TELSTRA_ABN, MADDOCKS_ABN, DEFENCE_ABN, "33090555052"])
def test_real_abns_pass_the_check(abn: str) -> None:
    assert abn_valid(abn)


@pytest.mark.parametrize("abn", ["64086174782", "6408617478", "", "abc", "04086174781"])
def test_abn_check_rejects(abn: str) -> None:
    assert not abn_valid(abn)


def test_the_abn_is_derived_from_the_acn() -> None:
    assert abns_for_acn(TELSTRA_ACN) == [TELSTRA_ABN]
    assert abns_for_acn("086 174 781") == [TELSTRA_ABN]
    assert abns_for_acn("12345") == []


def test_an_acn_can_take_two_prefixes() -> None:
    """When 10 is a valid prefix, 99 is too (it adds 89 to the weighted sum)."""
    acn = next(f"{n:09d}" for n in range(1, 10_000) if abns_for_acn(f"{n:09d}")[0][:2] == "10")
    assert [a[:2] for a in abns_for_acn(acn)] == ["10", "99"]


def test_candidate_abns_by_registration_authority() -> None:
    assert candidate_abns("RA000014", "086 174 781") == [TELSTRA_ABN]
    assert candidate_abns("RA000013", "63 478 951 337") == [MADDOCKS_ABN]
    assert candidate_abns("RA000013", "63478951338") == []  # fails the check
    assert candidate_abns("RA001192", "703 703 164") == []  # an ARSN, not an ACN
    assert candidate_abns("RA000585", "00102498") == []


def test_format_abn() -> None:
    assert format_abn(TELSTRA_ABN) == "64 086 174 781"


def test_local_day_reads_canberra_time() -> None:
    assert local_day("2020-01-06T13:00:00Z") == "2020-01-07"  # AEDT midnight
    assert local_day("2022-06-29T14:00:00Z") == "2022-06-30"  # AEST midnight
    assert local_day("2020-01-28T06:02:08Z") == "2020-01-28"
    assert local_day("") is None


def test_notice_uuid_from_award_id() -> None:
    assert notice_uuid(f"CN3827867-{UUID_HEX}") == "c8309686-aa5a-4c6e-bfa5-71028c844fbb"
    assert notice_uuid(f"CN773631-A2-{UUID_HEX}") == "c8309686-aa5a-4c6e-bfa5-71028c844fbb"
    assert notice_uuid("CN3827867") is None


# ---------------------------------------------------------------------------
# Index fixture
# ---------------------------------------------------------------------------


def _release(
    cn: str,
    suppliers: list[tuple[str, str | None]],
    *,
    buyer: tuple[str, str] = ("Department of Defence", DEFENCE_ABN),
    published: str = "2026-10-01T01:00:00Z",
    value: str = "100000.00",
    status: str = "active",
    amendments: int = 0,
    method: str = "Open tender",
    reason: str = "",
) -> dict:
    parties = [
        {
            "id": f"s{i}",
            "name": name,
            "roles": ["supplier"],
            # The buying agency's officer, filed on the supplier party: never read.
            "contactPoint": {"name": "Tenders Officer", "email": "tenders@defence.gov.au"},
            **({"additionalIdentifiers": [{"id": abn, "scheme": "AU-ABN"}]} if abn else {}),
        }
        for i, (name, abn) in enumerate(suppliers)
    ]
    parties.append(
        {
            "id": "b",
            "name": buyer[0],
            "roles": ["procuringEntity"],
            "additionalIdentifiers": [{"id": buyer[1], "scheme": "AU-ABN"}],
        }
    )
    tender = {"procurementMethodDetails": method}
    if reason:
        tender["limitedTenderReason"] = reason
    return {
        "ocid": f"prod-{cn}",
        "parties": parties,
        "tender": tender,
        "awards": [{"id": f"{cn}-{UUID_HEX}", "suppliers": [{"id": "s0"}]}],
        "contracts": [
            {
                "id": cn,
                "awardID": f"{cn}-{UUID_HEX}",
                "dateSigned": published,
                "description": f"Services {cn}",
                "title": "PO 4600004521",
                "value": {"amount": value, "currency": "AUD"},
                "period": {"startDate": "2026-09-30T14:00:00Z", "endDate": "2027-09-29T14:00:00Z"},
                "status": status,
                **(
                    {"amendments": [{"id": f"{cn}-A{n + 1}"} for n in range(amendments)]}
                    if amendments
                    else {}
                ),
            }
        ],
    }


RELEASES = [
    _release("CN1", [("TELSTRA LIMITED", TELSTRA_ABN)], value="1000.40", published="2013-03-01T01:00:00Z"),
    _release("CN2", [("Telstra Corporation Limited", TELSTRA_ABN)], value="5000000",
             published="2026-10-20T23:30:00Z", status="cancelled", amendments=2),
    # A rename under one ABN: the old supplier party stays — one contract.
    _release("CN3", [("Telstra Ltd", TELSTRA_ABN), ("TELSTRA LIMITED", TELSTRA_ABN)],
             buyer=("Services Australia", "90794605008"), value="250000",
             method="Limited tender", reason="10.3.a.i. No submissions received."),
    # A joint award with another ABN: counted for both, flagged shared.
    _release("CN4", [("Maddocks", MADDOCKS_ABN), ("TELSTRA LIMITED", TELSTRA_ABN)], value="70000"),
    # A supplier with no ABN (an overseas EXEMPTUUID party): not indexed.
    _release("CN5", [("SATS LTD", None)]),
    # An ABN that fails the check: counted, not indexed.
    _release("CN6", [("Bad", "64086174782")]),
]


@pytest.fixture()
def built(tmp_path: Path):
    src = tmp_path / "full.jsonl.gz"
    with gzip.open(src, "wt", encoding="utf-8") as fh:
        for r in RELEASES:
            fh.write(json.dumps(r) + "\n")
    out = tmp_path / "data" / "austender.sqlite"
    meta = build_index(
        iter_releases(src), out,
        source_meta={"source_last_modified": "Thu, 12 Nov 2026 00:41:23 GMT"},
    )
    os.environ["AUSTENDER_DB_FILE"] = str(out)
    get_settings.cache_clear()
    austender.reset_connection()
    yield out, meta
    os.environ.pop("AUSTENDER_DB_FILE", None)
    get_settings.cache_clear()
    austender.reset_connection()


@pytest.fixture()
def no_index(tmp_path: Path):
    os.environ.pop("AUSTENDER_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    austender.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    austender.reset_connection()


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_builder_counts(built) -> None:
    path, meta = built
    assert meta["releases"] == "6"
    assert meta["supplier_parties_invalid_abn"] == "1"
    assert meta["contracts_without_abn"] == "2"
    assert meta["contracts_marked_cancelled"] == "1"
    assert meta["contracts_shared"] == "1"
    assert meta["data_to"] == "2026-10-21"  # 23:30Z is the next day in Canberra
    conn = sqlite3.connect(path)
    assert {r[0] for r in conn.execute("SELECT abn FROM supplier")} == {
        int(TELSTRA_ABN), int(MADDOCKS_ABN),
    }


def test_builder_never_stores_the_supplier_contact_point(built) -> None:
    path, _ = built
    dump = "\n".join(sqlite3.connect(path).iterdump())
    assert "tenders@defence.gov.au" not in dump
    assert "Tenders Officer" not in dump


def test_a_rename_under_one_abn_is_one_contract(built) -> None:
    path, _ = built
    row = sqlite3.connect(path).execute(
        "SELECT contracts, value_aud, shared, marked_cancelled, amended, name FROM supplier WHERE abn = ?",
        (int(TELSTRA_ABN),),
    ).fetchone()
    assert row == (4, 1000 + 5000000 + 250000 + 70000, 1, 1, 1, "Telstra Corporation Limited")


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_fetch_by_acn(built) -> None:
    bundle = await REGISTRY["austender"].fetch_by_identifiers(
        "894500U4L7JONMULJE04", "086 174 781", "RA000014", legal_name="TELSTRA LIMITED"
    )
    assert bundle["abn"] == TELSTRA_ABN
    assert bundle["matched_via"] == "acn"
    assert bundle["supplier"]["contracts"] == 4
    contracts = bundle["contracts"]
    assert [c["cn"] for c in contracts] == ["CN2", "CN4", "CN3", "CN1"]
    cancelled = contracts[0]
    assert cancelled["marked_cancelled"] is True and cancelled["amendments"] == 2
    assert cancelled["published"] == "2026-10-21"
    assert cancelled["start_date"] == "2026-10-01" and cancelled["end_date"] == "2027-09-30"
    assert cancelled["url"] == "https://www.tenders.gov.au/Cn/Show/c8309686-aa5a-4c6e-bfa5-71028c844fbb"
    limited = next(c for c in contracts if c["cn"] == "CN3")
    assert limited["procedure"] == "Limited tender"
    assert limited["limited_tender_reason"] == "10.3.a.i. No submissions received."
    assert next(c for c in contracts if c["cn"] == "CN4")["other_suppliers"] == 1
    assert [b["name"] for b in bundle["buyers"]] == ["Department of Defence", "Services Australia"]


async def test_fetch_by_abn(built) -> None:
    bundle = await REGISTRY["austender"].fetch_by_identifiers("X", MADDOCKS_ABN, "RA000013")
    assert bundle["matched_via"] == "abn"
    assert bundle["supplier"]["contracts"] == 1


async def test_fetch_a_company_with_no_contracts(built) -> None:
    bundle = await REGISTRY["austender"].fetch(DEFENCE_ABN)
    assert bundle["not_found"] is True and bundle["is_stub"] is False


async def test_fetch_without_an_index_is_a_stub(no_index) -> None:
    bundle = await REGISTRY["austender"].fetch(TELSTRA_ABN)
    assert bundle["is_stub"] is True


async def test_search_is_empty(built) -> None:
    from opencheck.sources.base import SearchKind

    assert await REGISTRY["austender"].search("Telstra", SearchKind.ENTITY) == []


def test_not_a_register_adapter() -> None:
    adapter = REGISTRY["austender"]
    assert adapter.lookup_derivers == ()
    assert adapter.info.is_national_register is False


# ---------------------------------------------------------------------------
# Finding, hit and BODS
# ---------------------------------------------------------------------------


async def test_finding(built) -> None:
    bundle = await REGISTRY["austender"].fetch_by_identifiers("X", TELSTRA_ACN, "RA000014")
    assert finding_austender(bundle) == (
        "Australian Government contracts 2013–2026: 4 notices, published values "
        "totalling AUD 5,321,000, from 2 agencies."
    )


async def test_result_hit_asserts_no_identifier(built) -> None:
    bundle = await REGISTRY["austender"].fetch(TELSTRA_ABN)
    hit = _build_result_hit("austender", bundle, _LookupCtx(lei="X", legal_name="Telstra"))
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.summary == "4 Australian Government contracts · 2013–2026 · AU-ABN 64 086 174 781"


async def test_no_card_for_a_company_with_no_contracts(built) -> None:
    bundle = await REGISTRY["austender"].fetch(DEFENCE_ABN)
    assert _build_result_hit("austender", bundle, _LookupCtx(lei="X")) is None


async def test_mapper_emits_one_entity_with_the_abn(built) -> None:
    bundle = await REGISTRY["austender"].fetch(TELSTRA_ABN)
    statements = list(map_austender(bundle))
    assert len(statements) == 1
    rd = statements[0]["recordDetails"]
    assert rd["identifiers"][0] == {
        "id": TELSTRA_ABN,
        "scheme": "AU-ABN",
        "schemeName": "Australian Business Number — Australian Business Register",
    }
    assert "relationship" not in statements[0]["recordType"]
    assert "AUD 5,321,000" in rd["entityType"]["details"]


async def test_statements_pass_libcovebods(built, tmp_path: Path) -> None:
    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    bundle = await REGISTRY["austender"].fetch(TELSTRA_ABN)
    path = tmp_path / "austender.json"
    path.write_text(json.dumps(list(map_austender(bundle))), encoding="utf-8")
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]


# ---------------------------------------------------------------------------
# Lookup wiring
# ---------------------------------------------------------------------------


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="austender")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_for_an_acn(built) -> None:
    ctx = _LookupCtx(lei="894500U4L7JONMULJE04", jurisdiction="AU", registered_as=TELSTRA_ACN)
    _build_derived(ctx, "RA000014")
    assert _dispatched(ctx) == {"austender"}


@pytest.mark.parametrize(
    ("jurisdiction", "ra", "registered_as"),
    [
        ("EE", "RA000181", "10421629"),
        ("AU", "RA001192", "703703164"),  # a registered scheme's ARSN
        ("AU", "RA000013", "63478951338"),  # an ABN failing the check
    ],
)
def test_not_announced_for_an_anchor_it_cannot_answer(built, jurisdiction, ra, registered_as) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction=jurisdiction, registered_as=registered_as)
    _build_derived(ctx, ra)
    assert _dispatched(ctx) == set()


def test_not_announced_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="AU", registered_as=TELSTRA_ACN)
    _build_derived(ctx, "RA000014")
    assert _dispatched(ctx) == set()


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("AUSTENDER_DB_URL", "")
    get_settings.cache_clear()
    assert austender.warm_index()["austender"].startswith("absent")


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(entity_pages, "download_db", boom)
    assert austender.warm_index()["austender"].startswith("failed")
