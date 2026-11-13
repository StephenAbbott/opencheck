"""MTender (Moldovan public procurement awards).

The fixture releases are shaped from the OCP Data Registry's compiled
releases (publication 34, retrieved 21 Oct 2026): awards with ``status``
``pending`` and the real state in ``statusDetails``, suppliers named by
``MD-IDNO`` in ``parties/identifier``, a ``contactPoint`` with a named person
on every party, lots that can be cancelled. They are run through the real
builder, so the index the adapter reads is one ``build_index`` actually
wrote. Contact details are invented.
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
from pathlib import Path

import pytest

from opencheck.bods import map_mtender_moldova
from opencheck.config import get_settings
from opencheck.findings import finding_mtender_moldova
from opencheck.licensing import classify
from opencheck.routers.hit_builders import _LookupCtx
from opencheck.routers.lookup import (
    _build_derived,
    _build_result_hit,
    _dispatch,
    _register_procurement,
)
from opencheck.sources import REGISTRY, mtender_moldova
from opencheck.sources.mtender_moldova import (
    build_index,
    candidate_idno,
    iter_releases,
    legal_person_idno,
    local_day,
    sole_trader_name,
    tender_url,
)

KAUFLAND = "1016600004811"
MOLDTELECOM = "1002600048836"
PREMIER = "1014600043319"
BUYER = "1007602005520"
PERSON = "2004000123459"  # an IDNP shape: never kept, whatever its check digit
KAUFLAND_LEI = "529900IQDQ6X3VTMWN63"


# ---------------------------------------------------------------------------
# Identifiers and words
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("idno", [KAUFLAND, MOLDTELECOM, PREMIER, " 1016600004811 "])
def test_legal_person_idnos_are_kept(idno: str) -> None:
    assert legal_person_idno(idno) == idno.strip()


@pytest.mark.parametrize(
    "idno", ["1016600004812", PERSON, "0912345678901", "101660000481", "", None, "abc"]
)
def test_other_numbers_are_not(idno) -> None:
    assert legal_person_idno(idno) is None


@pytest.mark.parametrize(
    "name",
    [
        "Î.I. \"Popescu Ion\"",
        "I.I GORI VIOREL",
        "II MIRON OLGA",
        "Gospodăria Țărănească Rotari Vasile",
        "G.Ț. Cernat Sergiu",
        "Întreprindere Individuală Prohor Aliona",
        "Antreprenor independent Inga Burlacu",
    ],
)
def test_sole_trader_forms(name: str) -> None:
    assert sole_trader_name(name)


@pytest.mark.parametrize(
    "name", ['S.R.L. "KAUFLAND"', 'Societatea pe Acţiuni "Moldtelecom"', "GTA Auto SRL", ""]
)
def test_companies_are_not_sole_traders(name: str) -> None:
    assert not sole_trader_name(name)


def test_candidate_idno_by_registration_authority() -> None:
    assert candidate_idno("RA000451", KAUFLAND) == KAUFLAND
    assert candidate_idno("RA000950", KAUFLAND) == KAUFLAND
    assert candidate_idno("RA000585", KAUFLAND) is None
    assert candidate_idno("RA000451", "16479") is None


def test_local_day_reads_chisinau_time() -> None:
    assert local_day("2026-01-13T22:30:00Z") == "2026-01-14"  # EET, UTC+2
    assert local_day("2025-07-01T21:30:00Z") == "2025-07-02"  # EEST, UTC+3
    assert local_day("2025-07-01T20:30:00Z") == "2025-07-01"
    assert local_day("") is None


def test_tender_url() -> None:
    assert tender_url("ocds-b3wdp1-MD-1") == "https://mtender.gov.md/tenders/ocds-b3wdp1-MD-1"


def test_the_licence_is_amber_until_confirmed() -> None:
    terms = classify(REGISTRY["mtender_moldova"].info.license)
    assert terms.license == "MTender-Open-Data"
    assert terms.commercial_use == "conditional"
    assert terms.attribution_required is True


# ---------------------------------------------------------------------------
# Index fixture
# ---------------------------------------------------------------------------


def _party(pid: str, name: str, idno: str, role: str) -> dict:
    return {
        "id": pid,
        "name": name,
        "roles": [role],
        "identifier": {"scheme": "MD-IDNO", "id": idno, "legalName": name},
        # A named person, e-mail and phone on every party: never read.
        "contactPoint": {"name": "Ion Contabilul", "email": "ion@example.md", "telephone": "+37322000000"},
        "address": {"streetAddress": "str. Exemplu 1", "locality": "Chișinău"},
    }


def _award(supplier: str, *, status: str = "active", amount: str = "1000.40",
           currency: str = "MDL", date: str = "2026-01-13T08:00:00Z", lot: str = "L1") -> dict:
    return {
        "id": f"a-{supplier}-{lot}",
        "status": "pending",
        "statusDetails": status,
        "date": date,
        "value": {"amount": amount, "currency": currency},
        "suppliers": [{"id": supplier, "name": "ignored"}],
        "relatedLots": [lot],
    }


def _release(ocid: str, awards: list[dict], suppliers: list[tuple[str, str, str]], *,
             method: str = "directAward", title: str = "Produse igienice",
             tender_status: str = "complete", lots: dict[str, str] | None = None,
             buyer: tuple[str, str] = ("Grădinița nr. 6", BUYER)) -> dict:
    parties = [_party(pid, name, idno, "supplier") for pid, name, idno in suppliers]
    parties.append(_party("b", buyer[0], buyer[1], "buyer"))
    return {
        "ocid": f"ocds-b3wdp1-MD-{ocid}",
        "date": "2026-01-20T10:00:00Z",
        "title": title,
        "parties": parties,
        "tender": {
            "status": tender_status,
            "procurementMethodDetails": method,
            "mainProcurementCategory": "goods",
            "procuringEntity": {"id": "b", "name": buyer[0]},
            "lots": [{"id": k, "status": v} for k, v in (lots or {"L1": "active"}).items()],
        },
        "awards": awards,
    }


KAUFLAND_PARTY = ("s1", 'S.R.L. "KAUFLAND"', KAUFLAND)
RELEASES = [
    # Two winning lots in one procurement: one procurement, two lots.
    _release("1", [_award("s1", amount="10559", lot="L1"), _award("s1", amount="694.50", lot="L2")],
             [KAUFLAND_PARTY], lots={"L1": "active", "L2": "active"}),
    # A later open tender from another buyer, decided late evening UTC.
    _release("2", [_award("s1", amount="3656", date="2026-03-02T22:30:00Z"),
                   _award("s2", status="unsuccessful")],
             [KAUFLAND_PARTY, ("s2", "SA Moldtelecom", MOLDTELECOM)],
             method="openTender", title="Produse alimentare", buyer=("Primăria Chișinău", MOLDTELECOM)),
    # A winning award on a cancelled lot: skipped.
    _release("3", [_award("s1", amount="999999", lot="L9")], [KAUFLAND_PARTY], lots={"L9": "cancelled"}),
    # MTender's own test procedure: skipped.
    _release("4", [_award("s1", amount="5")], [KAUFLAND_PARTY], method="testOpenTender"),
    # A sole trader with a legal-person IDNO: dropped.
    _release("5", [_award("s3")], [("s3", 'Î.I. "Popescu Ion"', PREMIER)]),
    # A natural person's IDNP: dropped.
    _release("6", [_award("s4")], [("s4", "Ion Popescu", PERSON)]),
    # Another currency: counted, never converted.
    _release("7", [_award("s2", amount="5000", currency="EUR")],
             [("s2", 'Societatea pe Acţiuni "Moldtelecom"', MOLDTELECOM)]),
]


@pytest.fixture()
def built(tmp_path: Path):
    src = tmp_path / "full.jsonl.gz"
    with gzip.open(src, "wt", encoding="utf-8") as fh:
        for r in RELEASES:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    out = tmp_path / "data" / "mtender_moldova.sqlite"
    meta = build_index(
        iter_releases(src), out,
        source_meta={"source_last_modified": "Wed, 21 Oct 2026 03:12:00 GMT"},
    )
    os.environ["MTENDER_DB_FILE"] = str(out)
    get_settings.cache_clear()
    mtender_moldova.reset_connection()
    yield out, meta
    os.environ.pop("MTENDER_DB_FILE", None)
    get_settings.cache_clear()
    mtender_moldova.reset_connection()


@pytest.fixture()
def no_index(tmp_path: Path):
    os.environ.pop("MTENDER_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    mtender_moldova.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    mtender_moldova.reset_connection()


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_builder_counts(built) -> None:
    path, meta = built
    assert meta["releases"] == "7"
    assert meta["awards_active"] == "8"
    assert meta["awards_on_cancelled"] == "1"
    assert meta["awards_test"] == "1"
    assert meta["awards_sole_trader"] == "1"
    assert meta["awards_person_or_invalid_id"] == "1"
    assert meta["data_from"] == "2026-01-13"
    assert meta["data_to"] == "2026-03-03"  # 22:30Z is the next day in Chișinău
    conn = sqlite3.connect(path)
    assert {r[0] for r in conn.execute("SELECT idno FROM supplier")} == {
        int(KAUFLAND), int(MOLDTELECOM),
    }


def test_builder_never_stores_contacts_people_or_addresses(built) -> None:
    path, _ = built
    dump = "\n".join(sqlite3.connect(path).iterdump())
    for text in ("Ion Contabilul", "ion@example.md", "+37322000000", "Exemplu", "Popescu", PERSON, PREMIER):
        assert text not in dump


def test_lots_of_one_procurement_are_one_award(built) -> None:
    path, _ = built
    row = sqlite3.connect(path).execute(
        "SELECT awards, lots, value_mdl, buyers, method_direct, method_open, other_currency "
        "FROM supplier WHERE idno = ?",
        (int(KAUFLAND),),
    ).fetchone()
    assert row == (2, 3, 10559 + 695 + 3656, 2, 1, 1, 0)


def test_another_currency_is_counted_not_converted(built) -> None:
    path, _ = built
    row = sqlite3.connect(path).execute(
        "SELECT awards, value_mdl, other_currency FROM supplier WHERE idno = ?", (int(MOLDTELECOM),)
    ).fetchone()
    assert row == (1, 0, 1)


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


async def test_fetch_by_identifiers(built) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch_by_identifiers(
        KAUFLAND_LEI, KAUFLAND, "RA000451", legal_name='"Kaufland" S.R.L.'
    )
    assert bundle["idno"] == KAUFLAND and bundle["is_stub"] is False
    supplier = bundle["supplier"]
    assert supplier["name"] == 'S.R.L. "KAUFLAND"'
    assert supplier["methods"] == {
        "Low-value purchase, reported (direct award)": 1,
        "Open tender": 1,
    }
    assert bundle["data_from"] == "2026-01-13" and bundle["data_to"] == "2026-03-03"
    awards = bundle["awards"]
    assert [a["ocid"] for a in awards] == ["ocds-b3wdp1-MD-2", "ocds-b3wdp1-MD-1"]
    latest = awards[0]
    assert latest["awarded"] == "2026-03-03"
    assert latest["buyer"] == "Primăria Chișinău"
    assert latest["procedure"] == "Open tender"
    assert latest["url"] == "https://mtender.gov.md/tenders/ocds-b3wdp1-MD-2"
    assert awards[1]["lots"] == 2 and awards[1]["value"] == 11254
    assert [b["name"] for b in bundle["buyers"]] == ["Grădinița nr. 6", "Primăria Chișinău"]


async def test_fetch_a_company_with_no_awards(built) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch(BUYER)
    assert bundle["not_found"] is True and bundle["is_stub"] is False


async def test_fetch_without_an_index_is_a_stub(no_index) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch(KAUFLAND)
    assert bundle["is_stub"] is True


async def test_search_is_empty(built) -> None:
    from opencheck.sources.base import SearchKind

    assert await REGISTRY["mtender_moldova"].search("Kaufland", SearchKind.ENTITY) == []


def test_not_a_register_adapter() -> None:
    adapter = REGISTRY["mtender_moldova"]
    assert adapter.lookup_derivers == ()
    assert adapter.info.is_national_register is False


# ---------------------------------------------------------------------------
# Finding, hit and BODS
# ---------------------------------------------------------------------------


async def test_finding(built) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch(KAUFLAND)
    assert finding_mtender_moldova(bundle) == (
        "Awarded 2 Moldovan public procurements January 2026 to March 2026, "
        "MDL values totalling 14,910, from 2 buyers."
    )


async def test_result_hit_asserts_no_identifier(built) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch(KAUFLAND)
    hit = _build_result_hit("mtender_moldova", bundle, _LookupCtx(lei="X", legal_name="Kaufland"))
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.summary == f"2 Moldovan public procurements awarded · 2026 · MD-IDNO {KAUFLAND}"


async def test_no_card_for_a_company_with_no_awards(built) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch(BUYER)
    assert _build_result_hit("mtender_moldova", bundle, _LookupCtx(lei="X")) is None


async def test_mapper_emits_one_entity_with_the_idno(built) -> None:
    bundle = await REGISTRY["mtender_moldova"].fetch(KAUFLAND)
    statements = list(map_mtender_moldova(bundle))
    assert len(statements) == 1
    rd = statements[0]["recordDetails"]
    assert rd["identifiers"][0] == {
        "id": KAUFLAND,
        "scheme": "MD-IDNO",
        "schemeName": "IDNO — State Register of Legal Entities (Moldova)",
    }
    assert statements[0]["recordType"] == "entity"
    assert "MDL 14,910" in rd["entityType"]["details"]
    assert statements[0]["statementDate"] == "2026-03-03"


async def test_statements_pass_libcovebods(built, tmp_path: Path) -> None:
    pytest.importorskip("libcovebods")
    from libcovebods.data_reader import DataReader
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.schema import SchemaBODS

    bundle = await REGISTRY["mtender_moldova"].fetch(KAUFLAND)
    path = tmp_path / "mtender.json"
    path.write_text(json.dumps(list(map_mtender_moldova(bundle))), encoding="utf-8")
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


def test_dispatched_for_an_idno(built) -> None:
    ctx = _LookupCtx(lei=KAUFLAND_LEI, jurisdiction="MD", registered_as=KAUFLAND)
    _build_derived(ctx, "RA000451")
    assert _close(_dispatch(ctx, only="mtender_moldova")) == {"mtender_moldova"}


@pytest.mark.parametrize(
    ("jurisdiction", "ra", "registered_as"),
    [
        ("EE", "RA000181", "10421629"),
        ("MD", "RA000451", "16479"),  # not an IDNO
        ("MD", "RA000451", "1016600004812"),  # fails the check digit
    ],
)
def test_not_announced_for_an_anchor_it_cannot_answer(built, jurisdiction, ra, registered_as) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction=jurisdiction, registered_as=registered_as)
    _build_derived(ctx, ra)
    assert _close(_dispatch(ctx, only="mtender_moldova")) == set()


def test_not_announced_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="X", jurisdiction="MD", registered_as=KAUFLAND)
    _build_derived(ctx, "RA000451")
    assert _close(_dispatch(ctx, only="mtender_moldova")) == set()


def test_a_register_lookup_on_the_idno_asks_mtender(built) -> None:
    assert _close(_register_procurement("MD-IDNO", KAUFLAND, "Kaufland")) == {"mtender_moldova"}
    assert _close(_register_procurement("MD-IDNO", PERSON, "")) == set()


def test_a_register_lookup_without_an_index_does_not(no_index) -> None:
    assert _close(_register_procurement("MD-IDNO", KAUFLAND, "Kaufland")) == set()


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("MTENDER_DB_URL", "")
    get_settings.cache_clear()
    assert mtender_moldova.warm_index()["mtender_moldova"].startswith("absent")


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(entity_pages, "download_db", boom)
    assert mtender_moldova.warm_index()["mtender_moldova"].startswith("failed")
