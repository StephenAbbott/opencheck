"""Find a Tender (UK public procurement notices).

The fixture releases are shaped from the OCP Data Registry's compiled releases
of the Find a Tender feed (publication 41, read 7 Nov 2026): a Procurement Act
award whose contract the buyer later cancelled and re-awarded, a framework lot
shared by several suppliers, a pre-Act award identified only through the
award id, a supplier named only by its PPON, and a party carrying the
"connected persons" text the builder must never copy. They run through the
real builder, so the index the adapter reads here is one ``build_index``
actually wrote.
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods import map_find_tender_uk
from opencheck.config import get_settings
from opencheck.findings import finding_find_tender_uk
from opencheck.routers.hit_builders import _LookupCtx
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _register_procurement
from opencheck.sources import REGISTRY, find_tender_uk
from opencheck.sources.find_tender_uk import (
    GB_CH_RA_CODES,
    IndexBuildError,
    build_index,
    normalise_ch,
)

TORSION = "09483417"
MCPHILLIPS = "00832117"
PPON_ONLY = "SC123456"  # reached only through the PPON crosswalk
CONNECTED_PERSON = "Jane Example, 1 Example Street, born March 1970"


def _party(pid: str, name: str, roles: list[str], *, coh: str | None = None,
           ppon: str | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    ids = []
    if coh:
        ids.append({"scheme": "GB-COH", "id": coh})
    if ppon:
        ids.append({"scheme": "GB-PPON", "id": ppon})
    party: dict[str, Any] = {"id": pid, "name": name, "roles": roles}
    if ids:
        party["identifier"] = ids[0]
        if len(ids) > 1:
            party["additionalIdentifiers"] = ids[1:]
    party.update(extra or {})
    return party


def _releases() -> list[dict[str, Any]]:
    stoke = {"id": "GB-PPON-PDQZ-1442-RZTZ", "name": "Stoke on Trent City Council"}
    act = {"legalBasis": {"scheme": "UKPGA"}, "title": "Doris Robinson Court", "aboveThreshold": True}
    return [
        # Awarded to Torsion, contract cancelled (administration), re-awarded.
        {
            "ocid": "ocds-h6vhtk-04e7a5", "date": "2026-09-21T15:37:44+01:00",
            "buyer": stoke, "tender": act,
            "parties": [
                _party(stoke["id"], stoke["name"], ["buyer"], ppon="PDQZ-1442-RZTZ"),
                _party("GB-COH-09483417", "TORSION CONSTRUCTION LIMITED", ["tenderer"],
                       coh="9483417", ppon="PMDG-5911-MWYD",
                       extra={"contactPoint": {"email": "x@example.com"},
                              "address": {"streetAddress": "1 Site Road"},
                              "control": {"description": CONNECTED_PERSON}}),
                _party("GB-COH-00832117", "MCPHILLIPS (WELLINGTON) LIMITED", ["supplier"],
                       coh="00832117", ppon="PLNQ-5888-QGXV"),
            ],
            "awards": [
                {"id": "1", "date": "2026-03-10T00:00:00Z", "status": "active",
                 "value": {"amount": 9630343.49, "currency": "GBP"},
                 "documents": [{"url": "https://www.find-tender.service.gov.uk/Notice/021754-2026",
                                "documentType": "awardNotice", "noticeType": "UK6"}],
                 "suppliers": [{"id": "GB-COH-09483417", "name": "TORSION CONSTRUCTION LIMITED"}]},
                {"id": "2", "status": "active",
                 "suppliers": [{"id": "GB-COH-00832117", "name": "MCPHILLIPS (WELLINGTON) LIMITED"}]},
                # Never stored: not an award to anyone.
                {"id": "3", "status": "cancelled",
                 "suppliers": [{"id": "GB-COH-00832117", "name": "MCPHILLIPS (WELLINGTON) LIMITED"}]},
            ],
            "contracts": [
                {"id": "1", "awardID": "1", "status": "cancelled"},
                {"id": "2", "awardID": "2", "status": "active", "dateSigned": "2026-09-18T00:00:00Z",
                 "value": {"amount": 9201834, "currency": "GBP"},
                 "documents": [{"url": "https://www.find-tender.service.gov.uk/Notice/089288-2026",
                                "noticeType": "UK7"}]},
            ],
        },
        # A framework: two lots to McPhillips, shared with another supplier.
        {
            "ocid": "ocds-h6vhtk-0aaaaa", "date": "2026-10-02T10:00:00Z",
            "buyer": {"id": "GB-PPON-LHC", "name": "LHC Procurement Group"},
            "tender": {"legalBasis": {"scheme": "UKPGA"}, "title": "Building framework"},
            "parties": [
                _party("GB-COH-00832117", "MCPHILLIPS (WELLINGTON) LIMITED", ["supplier"], coh="832117"),
                _party("GB-COH-01234567", "OTHER BUILDER LIMITED", ["supplier"], coh="01234567"),
            ],
            "awards": [
                {"id": f"lot-{n}", "date": "2026-10-02T00:00:00Z", "status": "pending",
                 "value": {"amount": 1_950_000_000, "currency": "GBP"},
                 "documents": [{"url": "https://www.find-tender.service.gov.uk/Notice/094489-2026",
                                "documentType": "awardNotice"}],
                 "suppliers": [{"id": "GB-COH-00832117", "name": "MCPHILLIPS (WELLINGTON) LIMITED"},
                               {"id": "GB-COH-01234567", "name": "OTHER BUILDER LIMITED"}]}
                for n in (1, 2)
            ],
        },
        # Pre-Act: no award documents or date — the notice is the award id's
        # prefix, the date the contract's signature, the value the contract's.
        {
            "ocid": "ocds-h6vhtk-0287ac", "date": "2021-01-05T11:48:03Z",
            "buyer": {"id": "GB-FTS-1", "name": "Kent County Council"},
            "tender": {"legalBasis": {"scheme": "CELEX"}, "title": "Adult Social Services"},
            "parties": [_party("GB-FTS-225", "MCPHILLIPS (WELLINGTON) LIMITED", ["supplier"], coh="00832117"),
                        _party("GB-FTS-226", "UNNAMED LLP", ["supplier"])],
            "awards": [
                {"id": "000102-2021-1", "status": "active",
                 "suppliers": [{"id": "GB-FTS-225", "name": "MCPHILLIPS (WELLINGTON) LIMITED"},
                               {"id": "GB-FTS-226", "name": "UNNAMED LLP"}]},
                {"id": "000102-2021-2", "status": "unsuccessful", "suppliers": []},
            ],
            "contracts": [{"awardID": "000102-2021-1", "status": "active", "dateSigned": "2020-12-11T00:00:00Z",
                           "value": {"amount": 60000000, "currency": "GBP"}}],
        },
        # The PPON crosswalk: one notice pairs the PPON with a CH number...
        {
            "ocid": "ocds-h6vhtk-0bbbbb", "date": "2026-05-01T00:00:00Z",
            "buyer": {"id": "B1", "name": "A Council"},
            "tender": {"legalBasis": {"scheme": "UKPGA"}},
            "parties": [_party("P1", "SCOTTISH CO LIMITED", ["tenderer"], coh="SC123456", ppon="pabc-1111-aaaa"),
                        # An ambiguous PPON: paired with two CH numbers.
                        _party("P2", "A LIMITED", ["tenderer"], coh="11111111", ppon="PAMB-0000-0000"),
                        _party("P3", "B LIMITED", ["tenderer"], coh="22222222", ppon="PAMB-0000-0000")],
        },
        # ...and another names the supplier by its PPON alone.
        {
            "ocid": "ocds-h6vhtk-0ccccc", "date": "2026-06-01T00:00:00Z",
            "buyer": {"id": "B1", "name": "A Council"},
            "tender": {"legalBasis": {"scheme": "UKPGA"}, "title": "Cleaning"},
            "parties": [_party("P9", "Scottish Co", ["supplier"], ppon="PABC-1111-AAAA"),
                        _party("P8", "Ambiguous", ["supplier"], ppon="PAMB-0000-0000"),
                        # A CH-shaped number with no scheme is ignored (decision F).
                        _party("P7", "Schemeless", ["supplier"], extra={"identifier": {"id": "33333333"}})],
            "awards": [{"id": "1", "date": "2026-06-01T00:00:00Z", "status": "active",
                        "value": {"amount": 1000, "currency": "GBP"},
                        "suppliers": [{"id": "P9", "name": "Scottish Co"},
                                      {"id": "P8", "name": "Ambiguous"},
                                      {"id": "P7", "name": "Schemeless"}]}],
        },
    ]


def _write(path: Path, releases: list[dict[str, Any]], *, junk: int = 0) -> Path:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for r in releases:
            fh.write(json.dumps(r) + "\n")
        for _ in range(junk):
            fh.write("{not json\n")
    return path


def _build(tmp_path: Path, **kwargs: Any) -> tuple[Path, dict[str, str]]:
    src = _write(tmp_path / "full.jsonl.gz", _releases())
    out = tmp_path / "data" / "find_tender_uk.sqlite"
    meta = build_index(src, out, source_as_of="2026-11-07T00:00:00+00:00",
                       min_releases=1, min_suppliers=1, **kwargs)
    return out, meta


@pytest.fixture()
def built(tmp_path: Path):
    out, meta = _build(tmp_path)
    os.environ["FIND_TENDER_DB_FILE"] = str(out)
    get_settings.cache_clear()
    find_tender_uk.reset_connection()
    yield out, meta
    os.environ.pop("FIND_TENDER_DB_FILE", None)
    get_settings.cache_clear()
    find_tender_uk.reset_connection()


@pytest.fixture()
def no_index(tmp_path: Path):
    os.environ.pop("FIND_TENDER_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    find_tender_uk.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    find_tender_uk.reset_connection()


# ---------------------------------------------------------------------------
# The Companies House number
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("9483417", "09483417"), ("09483417", "09483417"), (" sc651281 ", "SC651281"),
     ("OC303675", "OC303675"), ("NI 012345", "NI012345")],
)
def test_normalise_ch(raw: str, expected: str) -> None:
    assert normalise_ch(raw) == expected


@pytest.mark.parametrize("raw", ["", "00000000", "123456789", "S1234567", "PDQZ-1442-RZTZ"])
def test_normalise_ch_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalise_ch(raw)


def test_ra_codes_are_the_three_companies_house_authorities() -> None:
    from opencheck import ra_codes

    gb = {ra_codes.RA_BY_COUNTRY["GB"], *(s.ra_code for s in ra_codes.SUB_REGISTRIES["GB"])}
    assert GB_CH_RA_CODES == gb


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def _rows(path: Path, sql: str, *args: Any) -> list[tuple[Any, ...]]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def test_builder_keeps_awards_and_drops_cancelled_and_unsuccessful(built) -> None:
    path, meta = built
    ids = {r[0] for r in _rows(path, "SELECT award_id FROM award")}
    assert "3" not in ids and "000102-2021-2" not in ids
    assert meta["awards_cancelled"] == "1" and meta["awards_unsuccessful"] == "1"
    # The cancelled contract's award keeps its row, with the contract's status.
    assert _rows(path, "SELECT contract_status FROM award WHERE ch = ? AND award_id = '1'", TORSION) == [
        ("cancelled",)
    ]


def test_builder_never_copies_contacts_addresses_or_connected_persons(built) -> None:
    path, _ = built
    dump = "\n".join(sqlite3.connect(path).iterdump())
    assert "Jane Example" not in dump
    assert "Example Street" not in dump
    assert "x@example.com" not in dump
    assert "1 Site Road" not in dump


def test_builder_pads_ch_numbers_and_reads_the_ppon_crosswalk(built) -> None:
    path, meta = built
    suppliers = {r[0] for r in _rows(path, "SELECT ch FROM supplier")}
    assert {TORSION, MCPHILLIPS, PPON_ONLY, "01234567"} <= suppliers
    # The ambiguous PPON and the scheme-less number reach no company.
    assert "11111111" not in suppliers and "22222222" not in suppliers
    assert "33333333" not in suppliers
    assert _rows(path, "SELECT via FROM award WHERE ch = ?", PPON_ONLY) == [("ppon",)]
    assert meta["supplier_refs_unidentified"] == "3"  # UNNAMED LLP, ambiguous, schemeless


def test_builder_dates_notices_and_values_by_fallback(built) -> None:
    path, _ = built
    (pre_act,) = _rows(
        path, "SELECT notice, date, date_basis, value, regime, suppliers FROM award WHERE award_id = ?",
        "000102-2021-1",
    )
    assert pre_act == ("000102-2021", "2020-12-11", "signed", 60000000.0, "pre_act", 2)
    (reawarded,) = _rows(
        path, "SELECT notice, date, date_basis, value FROM award WHERE ch = ? AND award_id = '2'", MCPHILLIPS
    )
    assert reawarded == ("089288-2026", "2026-09-18", "signed", 9201834.0)


def test_builder_meta(built) -> None:
    _, meta = built
    assert meta["schema_version"] == find_tender_uk.INDEX_SCHEMA_VERSION
    assert meta["source_as_of"] == "2026-11-07T00:00:00+00:00"
    assert meta["data_from"] == "2021-01-05" and meta["data_to"] == "2026-10-02"
    assert meta["built_at"].endswith("+00:00")


def test_builder_refuses_a_short_file(tmp_path: Path) -> None:
    src = _write(tmp_path / "full.jsonl.gz", _releases())
    out = tmp_path / "out.sqlite"
    with pytest.raises(IndexBuildError):
        build_index(src, out)  # the real minimums
    assert not out.exists()


def test_builder_refuses_a_broken_file(tmp_path: Path) -> None:
    src = _write(tmp_path / "full.jsonl.gz", _releases(), junk=find_tender_uk.MAX_MALFORMED + 1)
    with pytest.raises(IndexBuildError):
        build_index(src, tmp_path / "out.sqlite", min_releases=1, min_suppliers=1)


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_fetch_reads_a_supplier(built) -> None:
    with provenance.recording() as recorder:
        bundle = await REGISTRY["find_tender_uk"].fetch("832117")
    prov = recorder.resolve()
    assert prov.liveness == "snapshot"
    assert prov.source_as_of is not None and prov.source_as_of.date().isoformat() == "2026-11-07"
    assert bundle["company_number"] == MCPHILLIPS
    assert bundle["supplier"]["awards"] == 4
    assert bundle["supplier"]["act_awards"] == 3
    assert bundle["pre_act_awards"] == 1
    assert bundle["source_as_of"] == "2026-11-07"
    records = bundle["records"]
    # One row per procurement: the framework's two lots are one row.
    assert [r["notice"] for r in records if r["kept_as"] == "latest"] == [
        "094489-2026", "089288-2026", "000102-2021",
    ]
    framework = records[0]
    assert framework["lots"] == 2 and framework["suppliers"] == 2
    assert framework["award_status"] == "pending"
    assert framework["url"] == "https://www.find-tender.service.gov.uk/Notice/094489-2026"
    # Largest lists only awards to this company alone, never a shared one.
    assert all(r["suppliers"] == 1 for r in records if r["kept_as"] == "largest")
    assert bundle["buyers"][0]["name"] in {"LHC Procurement Group", "Stoke on Trent City Council"}


async def test_fetch_a_company_that_won_nothing(built) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch("SC651281")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["supplier"] is None


async def test_fetch_without_an_index_is_a_stub(no_index) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch(MCPHILLIPS)
    assert bundle["is_stub"] is True


def test_covers_lei_follows_the_index(built) -> None:
    assert REGISTRY["find_tender_uk"].covers_lei("X") is True


def test_covers_lei_without_an_index(no_index) -> None:
    assert REGISTRY["find_tender_uk"].covers_lei("X") is False


@pytest.mark.parametrize(
    ("ra", "registered_as", "reached"),
    [
        ("RA000585", "00832117", True),
        ("RA000585", "832117", True),
        ("RA000587", "SC651281", True),
        ("RA000591", "00832117", False),  # The Pensions Regulator
        ("RA000170", "61126228", False),  # Danish CVR
    ],
)
async def test_fetch_by_identifiers(built, ra: str, registered_as: str, reached: bool) -> None:
    result = await REGISTRY["find_tender_uk"].fetch_by_identifiers(
        "LEI", registered_as, ra, legal_name="X"
    )
    assert (result is not None) is reached


# ---------------------------------------------------------------------------
# BODS, finding, hit
# ---------------------------------------------------------------------------


async def test_mapper_emits_one_entity_with_the_ch_number(built) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch(MCPHILLIPS)
    statements = list(map_find_tender_uk(bundle))
    assert len(statements) == 1
    (entity,) = statements
    assert entity["recordType"] == "entity"
    assert entity["recordDetails"]["identifiers"] == [
        {"id": MCPHILLIPS, "scheme": "GB-COH", "schemeName": "Companies House"}
    ]
    assert entity["statementDate"] == "2026-10-02"
    assert "never" not in entity["recordDetails"]["entityType"].get("details", "")


async def test_mapper_yields_nothing_for_a_non_supplier(built) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch("SC651281")
    assert list(map_find_tender_uk(bundle)) == []


async def test_finding_counts_and_never_sums(built) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch(MCPHILLIPS)
    sentence = finding_find_tender_uk(bundle)
    assert sentence == "4 UK public contract awards from 3 buyers, December 2020 to October 2026."
    assert "£" not in sentence and "GBP" not in sentence


async def test_finding_says_when_every_award_predates_the_act(built, tmp_path: Path) -> None:
    bundle = {"supplier": {"awards": 1, "buyers": 1, "first_date": "2021-02-01", "last_date": "2021-02-01"},
              "pre_act_awards": 1}
    assert "before the Procurement Act" in finding_find_tender_uk(bundle)


async def test_result_hit_asserts_no_identifier(built) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch(MCPHILLIPS)
    hit = _build_result_hit("find_tender_uk", bundle, _LookupCtx(lei="L", legal_name="McPhillips"))
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.summary == "4 UK contract awards · 3 buyers · GB-COH 00832117"


async def test_no_card_for_a_company_that_won_nothing(built) -> None:
    bundle = await REGISTRY["find_tender_uk"].fetch("SC651281")
    assert _build_result_hit("find_tender_uk", bundle, _LookupCtx(lei="L")) is None


async def test_statements_pass_libcovebods(built, tmp_path: Path) -> None:
    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    bundle = await REGISTRY["find_tender_uk"].fetch(MCPHILLIPS)
    path = tmp_path / "fts.json"
    path.write_text(json.dumps(list(map_find_tender_uk(bundle))), encoding="utf-8")
    reader = DataReader(str(path))
    errors = JSONSchemaValidator(SchemaBODS(reader)).validate(reader)
    assert errors == [], [e.json()["message"] for e in errors]


def test_gleif_statement_writes_the_ch_number_the_same_way() -> None:
    from opencheck.bods.mapper import _gleif_entity_statement

    entity = _gleif_entity_statement(
        "213800N42YZLR9GLVC42",
        {"legalName": {"name": "SOFTCAT PLC"}, "registeredAs": "02174990",
         "registeredAt": {"id": "RA000585", "other": None}, "jurisdiction": "GB"},
        "https://api.gleif.org/api/v1/lei-records/213800N42YZLR9GLVC42",
    )
    assert {"scheme": "GB-COH", "id": "02174990"}.items() <= next(
        i for i in entity["recordDetails"]["identifiers"] if i.get("scheme") == "GB-COH"
    ).items()


# ---------------------------------------------------------------------------
# Lookup wiring
# ---------------------------------------------------------------------------


def test_not_a_register_adapter() -> None:
    adapter = REGISTRY["find_tender_uk"]
    assert adapter.lookup_derivers == ()
    assert adapter.info.is_national_register is False
    assert adapter.info.country is None


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="find_tender_uk")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_for_a_companies_house_anchor(built) -> None:
    ctx = _LookupCtx(lei="213800N42YZLR9GLVC42", jurisdiction="GB", registered_as="02174990")
    _build_derived(ctx, "RA000585")
    assert _dispatched(ctx) == {"find_tender_uk"}


@pytest.mark.parametrize(
    ("jurisdiction", "ra", "registered_as"),
    [("DK", "RA000170", "61126228"), ("GB", "RA000591", "00832117"), ("GB", "RA000585", "")],
)
def test_not_announced_for_an_anchor_it_cannot_answer(built, jurisdiction: str, ra: str, registered_as: str) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction=jurisdiction, registered_as=registered_as)
    _build_derived(ctx, ra)
    assert _dispatched(ctx) == set()


def test_not_announced_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="GB", registered_as="02174990")
    _build_derived(ctx, "RA000585")
    assert _dispatched(ctx) == set()


def _close(tasks: list[tuple[str, Any]]) -> list[str]:
    for _, awaitable in tasks:
        awaitable.close()
    return [sid for sid, _ in tasks]


def test_register_lookup_asks_find_a_tender_for_a_companies_house_number(built) -> None:
    assert _close(_register_procurement("GB-COH", MCPHILLIPS, "X")) == ["find_tender_uk"]
    assert _close(_register_procurement("DK-CVR", "61126228", "X")) == []


def test_register_lookup_skips_it_without_an_index(no_index) -> None:
    assert _close(_register_procurement("GB-COH", MCPHILLIPS, "X")) == []


# ---------------------------------------------------------------------------
# Boot download
# ---------------------------------------------------------------------------


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("FIND_TENDER_DB_URL", "")
    get_settings.cache_clear()
    assert "no URL" in find_tender_uk.warm_index()["find_tender_uk"]


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*_a: Any, **_k: Any) -> Any:
        raise OSError("network down")

    monkeypatch.setenv("FIND_TENDER_DB_URL", "https://example.invalid/x.gz")
    get_settings.cache_clear()
    monkeypatch.setattr(entity_pages, "download_db", boom)
    assert find_tender_uk.warm_index()["find_tender_uk"].startswith("failed")
