"""The NTA Japan wiring (16 Nov 2026) — the National Tax Agency's Corporate
Number (法人番号) register, read from an index of its monthly nationwide file
and, with an application ID, from the Web-API.

Every index here is produced by the **real builder**
(``scripts/build_nta_japan_index.py``) from rows in the published shape — no
header, thirty columns, UTF-8 — and read back through the **real adapter**
(the JAR / ONRC rule). The numbers are real (their check digits hold); the
rows are trimmed copies of the 30 Oct 2026 file's.
"""

from __future__ import annotations

import csv
import importlib.util
import io
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods.mapper import (
    _GLEIF_RA_TO_ORG_ID,
    _GLEIF_VALUE_SCHEMED_RA,
    gleif_registration_scheme,
    map_gleif,
    map_nta_japan,
)
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_nta_japan
from opencheck.register_hops import hop_for
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _LookupCtx
from opencheck.sources import REGISTRY, nta_japan
from opencheck.sources.base import SearchKind
from opencheck.sources.nta_japan import (
    API_BASE,
    COLUMNS,
    JP_COMPANY_REGISTRATION_RA,
    JP_CORPORATE_NUMBER_RA,
    JP_FUND_RA,
    JP_RA_CODES,
    check_digit,
    corporate_number,
    corporate_number_spellings,
    normalise_corporate_number,
    registration_number,
    status_class,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_nta_japan_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_nta_japan_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-10-30"
ANRITSU = "2021001021865"    # 株式会社, open; GLEIF files 0210-01-021865
STAR = "9080001002327"       # closed by merger, names its successor
STAR_NEW = "8010401194003"   # the successor
FOREIGN = "1700150004547"    # 401, an address abroad, English name
HIHYOJI = "1430001008980"    # closed by the registrar, address no longer exists
STRUCK = "5011102003364"     # latest change 81: trade name struck off
TOKYO = "8000020130001"      # 201, a local public body (東京都)
ABSENT = "4011001005165"     # a valid number the index does not hold


def _fresh(body: str) -> str:
    """A valid number the index does not hold, one per Web-API test: the
    response cache is shared across the session."""
    return f"{check_digit(body)}{body}"


def _row(number: str, name: str, kind: str, *, process: str = "01", pref: str = "東京都",
         city: str = "千代田区", street: str = "丸の内１丁目１番１号", post: str = "1000005",
         outside: str = "", close_date: str = "", close_cause: str = "", successor: str = "",
         update: str = "2015-11-02", en_name: str = "", hihyoji: str = "0") -> list[str]:
    values = {
        "sequenceNumber": "1", "corporateNumber": number, "process": process,
        "correct": "0", "updateDate": update, "changeDate": update, "name": name,
        "kind": kind, "prefectureName": pref, "cityName": city, "streetNumber": street,
        "postCode": post, "addressOutside": outside, "closeDate": close_date,
        "closeCause": close_cause, "successorCorporateNumber": successor,
        "assignmentDate": "2015-10-05", "latest": "1", "enName": en_name, "hihyoji": hihyoji,
    }
    return [values.get(c, "") for c in COLUMNS]


def _rows() -> list[list[str]]:
    return [
        _row(ANRITSU, "アンリツ株式会社", "301", pref="神奈川県", city="厚木市",
             street="恩名５丁目１番１号", post="2430016", update="2024-04-02",
             en_name="ANRITSU CORPORATION"),
        _row(STAR, "スター精密株式会社", "301", process="21", pref="静岡県",
             city="静岡市駿河区", street="中吉田２０番１０号", post="4228001",
             close_date="2026-09-17", close_cause="11", successor=STAR_NEW,
             update="2026-09-28"),
        _row(STAR_NEW, "スター精密株式会社", "301", process="71"),
        _row(FOREIGN, "ジェイピーモルガン・チェース・バンク・ナショナル・アソシエーション", "401",
             city="港区", street="六本木１丁目４番１号", outside="アメリカ合衆国オハイオ州",
             en_name="JPMorgan Chase Bank, National Association"),
        _row(HIHYOJI, "株式会社例示", "301", process="21", pref="", city="", street="",
             post="", close_date="2023-01-24", close_cause="21", hihyoji="1"),
        _row(STRUCK, "有限会社例示商店", "302", process="81", city="新宿区"),
        _row(TOKYO, "東京都", "201", city="新宿区", street="西新宿２丁目８番１号"),
    ]


def _csv_text(rows: list[list[str]]) -> io.StringIO:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(rows)
    buf.seek(0)
    return buf


def _build(tmp_path: Path, rows: list[list[str]] | None = None) -> tuple[Path, dict]:
    out = tmp_path / "data" / "nta_japan.sqlite"
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(_csv_text(_rows() if rows is None else rows), out, CUT, min_rows=0)
    return out, stats


def _point_at(path: Path | None, *, app_id: str | None = None, live: bool = False) -> None:
    for key, value in (("NTA_JAPAN_DB_FILE", str(path) if path else None),
                       ("NTA_JAPAN_APP_ID", app_id),
                       ("OPENCHECK_ALLOW_LIVE", "true" if live else None)):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    get_settings.cache_clear()
    nta_japan.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    out, stats = _build(tmp_path)
    _point_at(out)
    yield out, stats
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    _point_at(None)
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    _point_at(None)


async def _fetch(number: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["nta_japan"].fetch(number, legal_name=legal_name)


async def _map(number: str) -> list[dict[str, Any]]:
    with provenance.recording():
        bundle = await _fetch(number)
        return list(map_nta_japan(bundle))


# ---------------------------------------------------------------------------
# The number
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("number", [ANRITSU, STAR, STAR_NEW, FOREIGN, HIHYOJI, STRUCK, TOKYO])
def test_real_numbers_carry_their_check_digit(number):
    assert int(number[0]) == check_digit(number[1:])
    assert corporate_number(number) == number


@pytest.mark.parametrize(("raw", "want"), [
    ("0210-01-021865", ANRITSU), ("021001021865", ANRITSU), (ANRITSU, ANRITSU),
    ("０２１０－０１－０２１８６５", ANRITSU), (" 0210 01 021865 ", ANRITSU),
    ("1021001021865", None), ("02100102186", None), ("", None), ("ABC", None),
])
def test_the_number_reads_every_spelling(raw, want):
    assert corporate_number(raw) == want


def test_spellings_and_the_registration_number():
    assert registration_number(ANRITSU) == "0210-01-021865"
    assert corporate_number_spellings("0210-01-021865") == (ANRITSU, "0210-01-021865", "021001021865")
    with pytest.raises(ValueError):
        normalise_corporate_number("1021001021865")


def test_the_adapter_declares_its_lookup_wiring():
    adapter = REGISTRY["nta_japan"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == JP_RA_CODES == frozenset({"RA000412", "RA001075"})
    assert deriver.derived_key == "jp_jcn"
    assert adapter.lookup_pass_legal_name is True
    assert adapter.info.country == "JP" and adapter.info.is_national_register


def test_the_fsa_fund_register_is_never_dispatched():
    """RA000413 files fund codes; a check digit in front of one can land on an
    unrelated association, so nothing derives a corporate number from it."""
    for adapter in REGISTRY.values():
        for deriver in adapter.lookup_derivers:
            assert JP_FUND_RA not in deriver.ra_codes
    ctx = _LookupCtx(lei="5493000000000000FUND", jurisdiction="JP", registered_as="021001021865")
    _build_derived(ctx, JP_FUND_RA)
    assert "jp_jcn" not in ctx.derived


# ---------------------------------------------------------------------------
# GLEIF writes the same number
# ---------------------------------------------------------------------------


def test_ra000412_is_value_schemed_and_ra001075_is_mapped():
    assert JP_COMPANY_REGISTRATION_RA in _GLEIF_VALUE_SCHEMED_RA
    assert JP_COMPANY_REGISTRATION_RA not in _GLEIF_RA_TO_ORG_ID
    assert _GLEIF_RA_TO_ORG_ID[JP_CORPORATE_NUMBER_RA][0] == "JP-JCN"
    assert gleif_registration_scheme("RA000412", "JP", None, "0210-01-021865")[0] == "JP-JCN"
    assert gleif_registration_scheme("RA000412", "JP", None, "not-a-number")[0] == "RA000412"


def _gleif_record(registered_as: str, ra: str) -> dict[str, Any]:
    return {
        "lei": "353800886SENISUY0G83",
        "record": {"attributes": {
            "lei": "353800886SENISUY0G83",
            "entity": {
                "legalName": {"name": "アンリツ株式会社"},
                "jurisdiction": "JP",
                "registeredAs": registered_as,
                "registeredAt": {"id": ra},
                "status": "ACTIVE",
                "legalAddress": {"addressLines": ["恩名5-1-1"], "city": "厚木市", "country": "JP"},
            },
            "registration": {"status": "ISSUED"},
        }},
    }


def test_the_gleif_statement_writes_the_13_digits_and_says_what_was_filed():
    with provenance.recording():
        (stmt, *_) = [s for s in map_gleif(_gleif_record("0210-01-021865", "RA000412"))
                      if s["recordType"] == "entity"]
    ident = next(i for i in stmt["recordDetails"]["identifiers"] if i["scheme"] == "JP-JCN")
    assert ident["id"] == ANRITSU
    notes = [a for a in stmt.get("annotations", []) if a["motivation"] == "transformation"
             and "0210-01-021865" in a["description"]]
    assert len(notes) == 1
    index = stmt["recordDetails"]["identifiers"].index(ident)
    assert notes[0]["statementPointerTarget"] == f"/recordDetails/identifiers/{index}/id"


def test_a_13_digit_filing_is_written_as_filed_with_no_note():
    with provenance.recording():
        (stmt, *_) = [s for s in map_gleif(_gleif_record(ANRITSU, "RA001075"))
                      if s["recordType"] == "entity"]
    ident = next(i for i in stmt["recordDetails"]["identifiers"] if i["scheme"] == "JP-JCN")
    assert ident["id"] == ANRITSU
    assert not any("0210" in a["description"] for a in stmt.get("annotations", []))


def test_jp_jcn_is_a_register_hop_to_this_source():
    hop = hop_for("JP-JCN")
    assert hop is not None and hop.source_id == "nta_japan" and hop.derived_key == "jp_jcn"
    assert hop.normalise("0210-01-021865") == ANRITSU
    assert hop_for("REG-JP") is hop


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_records_its_cut_and_counts(index):
    out, stats = index
    assert stats["rows"] == 7 and stats["malformed"] == 0
    assert stats["closed"] == 2 and stats["searchable"] == 5 and stats["successors"] == 1
    meta = dict(sqlite3.connect(out).execute("SELECT key, value FROM meta"))
    assert meta["cut"] == CUT and meta["schema_version"] == nta_japan.INDEX_SCHEMA_VERSION
    assert meta["built_at"].endswith("Z") and "T" in meta["built_at"]


def test_the_builder_refuses_a_repeat_a_bad_number_a_short_file_and_a_bad_cut(tmp_path):
    out = tmp_path / "x.sqlite"
    with pytest.raises(builder.RefusedInput, match="repeats"):
        builder.build(_csv_text(_rows() + [_rows()[0]]), out, CUT, min_rows=0)
    bad = [_row("1" + ANRITSU[1:], "壊れた", "301")] * (builder.MAX_MALFORMED + 1)
    with pytest.raises(builder.RefusedInput, match="malformed"):
        builder.build(_csv_text(bad), out, CUT, min_rows=0)
    with pytest.raises(builder.RefusedInput, match="not the register"):
        builder.build(_csv_text(_rows()), out, CUT)
    with pytest.raises(builder.RefusedInput, match="YYYY-MM-DD"):
        builder.build(_csv_text(_rows()), out, "20261030", min_rows=0)
    assert not out.exists() and not out.with_name("x.sqlite.building").exists()


def test_the_cut_comes_from_the_file_name():
    assert builder.cut_from_name("00_zenkoku_all_20261030.zip") == "2026-10-30"
    assert builder.cut_from_name("something.zip") is None


def test_the_download_form_is_read_from_the_page():
    page = (
        '<input type="hidden" name="jp.go.nta.houjin_bangou.framework.web.common.'
        'CNSFWTokenProcessor.request.token" value="tok-123"/>'
        '<a onclick="return doDownload(11111);">sjis</a>'
        '<h2 class="title" id="csv-unicode">CSV形式・Unicode</h2>'
        '<a onclick="return doDownload(28341);">全国</a><a onclick="return doDownload(28342);">北海道</a>'
    )
    assert builder.form_fields(page) == ("tok-123", "28341")
    with pytest.raises(builder.RefusedInput):
        builder.form_fields("<html>maintenance</html>")


def test_the_committed_key_is_the_ntas():
    key = builder.KEY_FILE.read_text()
    assert key.startswith("-----BEGIN PGP PUBLIC KEY BLOCK-----")
    assert builder.NTA_KEY_FINGERPRINT == "F4109E821F0DE67C4BA1947C4BCD87A24D615230"


@pytest.mark.skipif(shutil.which("gpg") is None, reason="gpg not installed")
def test_an_unsigned_file_is_refused(tmp_path):
    with pytest.raises(builder.RefusedInput, match="signature"):
        builder.verify(iter([b"data"]), b"-----BEGIN PGP SIGNATURE-----\nnope\n")


# ---------------------------------------------------------------------------
# The adapter over the index
# ---------------------------------------------------------------------------


async def test_a_body_in_the_index_is_a_dated_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch("0210-01-021865", legal_name="ANRITSU CORPORATION")
    assert bundle["is_stub"] is False and not bundle.get("not_found")
    assert bundle["number"] == ANRITSU and bundle["registration_number"] == "0210-01-021865"
    assert bundle["name"] == "アンリツ株式会社" and bundle["en_name"] == "ANRITSU CORPORATION"
    assert bundle["status_class"] == "live" and bundle["tier"] == "index" and bundle["cut"] == CUT
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == CUT


async def test_a_merged_body_names_its_successor(index):
    bundle = await _fetch(STAR)
    assert bundle["status_class"] == "terminal"
    assert bundle["successor"] == STAR_NEW and bundle["successor_name"] == "スター精密株式会社"


def test_status_classes():
    assert status_class({"close_date": "2026-01-01"}) == "terminal"
    assert status_class({"close_cause": "21"}) == "terminal"
    assert status_class({"process": "81"}) == "unknown"
    assert status_class({"process": "01"}) == "live"


async def test_a_number_outside_the_index_is_a_note_card_not_a_degradation(index):
    with degradation.recording() as degraded:
        bundle = await _fetch(ABSENT, legal_name="Somebody KK")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["coverage_note"] == nta_japan.COVERAGE_NOT_IN_INDEX
    assert list(degraded) == []
    assert list(map_nta_japan(bundle)) == []


async def test_a_malformed_number_is_a_stub(index):
    assert (await _fetch("12345"))["is_stub"] is True


async def test_without_an_index_or_an_id_nothing_is_announced(no_index):
    adapter = REGISTRY["nta_japan"]
    assert adapter.covers_lei("353800886SENISUY0G83") is False
    assert (await _fetch(ANRITSU))["is_stub"] is True
    assert await adapter.search("アンリツ", SearchKind.ENTITY) == []


async def test_name_search_covers_open_records_in_either_script(index):
    adapter = REGISTRY["nta_japan"]
    hits = await adapter.search("アンリツ", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [ANRITSU]
    assert hits[0].identifiers == {"jp_jcn": ANRITSU}
    assert [h.hit_id for h in await adapter.search("anritsu", SearchKind.ENTITY)] == [ANRITSU]
    assert [h.hit_id for h in await adapter.search("ＡＮＲＩＴＳＵ", SearchKind.ENTITY)] == [ANRITSU]
    # A closed record is reached by its number, not by its name.
    hits = await adapter.search("スター精密", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [STAR_NEW]
    assert [h.hit_id for h in await adapter.search(STAR, SearchKind.ENTITY)] == [STAR]
    assert await adapter.search("アン", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# The Web-API tier
# ---------------------------------------------------------------------------


def _api_answer(*rows: list[str]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["2026-11-14", str(len(rows)), "1", "1"])
    writer.writerows(rows)
    return buf.getvalue()


@pytest.fixture()
def api_with_index(tmp_path: Path):
    out, _ = _build(tmp_path)
    _point_at(out, app_id="test-app-id", live=True)
    yield
    _point_at(None)


@respx.mock
async def test_a_number_the_index_lacks_is_read_from_the_web_api(api_with_index):
    number = _fresh("011001900001")
    route = respx.get(f"{API_BASE}/num").mock(return_value=httpx.Response(
        200, text=_api_answer(_row(number, "新設株式会社", "301", update="2026-11-10")),
    ))
    bundle = await _fetch(number)
    assert bundle["tier"] == "api" and bundle["name"] == "新設株式会社"
    assert bundle["api_notice"] == nta_japan.API_NOTICE
    params = dict(route.calls.last.request.url.params)
    assert params == {"id": "test-app-id", "number": number, "type": "02", "history": "0"}
    assert "read live from the Web-API" in finding_nta_japan(bundle)
    assert nta_japan.API_NOTICE in REGISTRY["nta_japan"].info.attribution
    # Cached a day: a second read sends nothing.
    await _fetch(number)
    assert route.call_count == 1


@respx.mock
async def test_the_web_api_saying_no_record_is_a_note_card(api_with_index):
    respx.get(f"{API_BASE}/num").mock(return_value=httpx.Response(200, text=_api_answer()))
    with degradation.recording() as degraded:
        bundle = await _fetch(_fresh("011001900002"))
    assert bundle["not_found"] is True and bundle["coverage_note"] == nta_japan.COVERAGE_NOT_ASSIGNED
    assert list(degraded) == []


@respx.mock
async def test_a_web_api_failure_is_a_degradation_never_cached_and_never_leaks_the_id(api_with_index):
    number = _fresh("011001900003")
    route = respx.get(f"{API_BASE}/num").mock(return_value=httpx.Response(503))
    with degradation.recording() as degraded:
        bundle = await _fetch(number)
    assert bundle["not_found"] is True and bundle["coverage_note"] == nta_japan.COVERAGE_NOT_IN_INDEX
    (entry,) = degraded
    assert entry.source_id == "nta_japan" and entry.check == "source_read"
    assert "test-app-id" not in entry.detail
    await _fetch(number)
    assert route.call_count == 2, "a failure is never cached"


@respx.mock
async def test_the_index_answers_first_even_with_an_id(api_with_index):
    route = respx.get(f"{API_BASE}/num")
    bundle = await _fetch(ANRITSU)
    assert bundle["tier"] == "index" and route.call_count == 0


def test_the_api_csv_needs_its_header():
    with pytest.raises(nta_japan.NtaApiUnavailable):
        nta_japan.parse_api_csv("<html>error</html>")
    assert nta_japan.parse_api_csv(_api_answer()) == []


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_draws_one_entity_on_the_scheme_gleif_uses(index):
    (stmt,) = await _map(ANRITSU)
    rd = stmt["recordDetails"]
    assert rd["identifiers"] == [{
        "id": ANRITSU, "scheme": "JP-JCN",
        "schemeName": "Corporate Number (法人番号), National Tax Agency (Japan)",
    }]
    assert rd["jurisdiction"] == {"name": "Japan", "code": "JP"}
    assert rd["alternateNames"] == ["ANRITSU CORPORATION"]
    assert rd["entityType"] == {
        "type": "registeredEntity", "details": "Stock company (kabushiki kaisha) (株式会社)",
    }
    (address,) = rd["addresses"]
    assert address["address"] == "神奈川県厚木市恩名５丁目１番１号"
    assert address["postCode"] == "2430016" and address["country"]["code"] == "JP"
    assert "foundingDate" not in rd, "assignmentDate is never a founding date"
    assert stmt["source"]["type"] == ["officialRegister"]
    assert stmt["statementDate"] == "2024-04-02"
    assert "legalFormCode" not in rd and "officialOrganizationalRole" not in str(stmt)


async def test_a_merger_closes_the_record_and_names_the_successor_without_a_relationship(index):
    statements = await _map(STAR)
    assert [s["recordType"] for s in statements] == ["entity", "entity"]
    company, successor = statements
    assert company["recordDetails"]["dissolutionDate"] == "2026-09-17"
    assert successor["recordDetails"]["identifiers"][0]["id"] == STAR_NEW
    assert "jurisdiction" not in successor["recordDetails"]
    notes = " ".join(a["description"] for a in company["annotations"])
    assert STAR_NEW in notes and "not an owner" in notes


async def test_a_foreign_body_has_no_jurisdiction_and_its_address_abroad_is_registered(index):
    (stmt,) = await _map(FOREIGN)
    rd = stmt["recordDetails"]
    assert "jurisdiction" not in rd
    types = {a["type"]: a for a in rd["addresses"]}
    assert types["registered"]["address"] == "アメリカ合衆国オハイオ州"
    assert "country" not in types["registered"]
    assert types["business"]["country"]["code"] == "JP"


async def test_a_local_public_body_is_a_state_body(index):
    (stmt,) = await _map(TOKYO)
    assert stmt["recordDetails"]["entityType"]["type"] == "stateBody"


async def test_hihyoji_and_a_struck_off_trade_name_are_not_statuses(index):
    (closed,) = await _map(HIHYOJI)
    assert closed["recordDetails"]["dissolutionDate"] == "2023-01-24"
    (struck,) = await _map(STRUCK)
    assert "dissolutionDate" not in struck["recordDetails"]
    assert finding_nta_japan(await _fetch(STRUCK)).startswith("Trade-name registration struck off")


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_nta_japan({"is_stub": True, "number": ANRITSU})) == []
    assert list(map_nta_japan({"is_stub": False, "not_found": True, "number": ANRITSU})) == []


# ---------------------------------------------------------------------------
# The finding and the hit
# ---------------------------------------------------------------------------


async def test_the_finding_sentences(index):
    assert finding_nta_japan(await _fetch(ANRITSU)).startswith(
        "Registration record open, stock company (kabushiki kaisha), 神奈川県厚木市"
    )
    star = finding_nta_japan(await _fetch(STAR))
    assert star.startswith("Registration record closed on 17 September 2026 (dissolved by merger)")
    assert "successor スター精密株式会社" in star
    hihyoji = finding_nta_japan(await _fetch(HIHYOJI))
    assert "address no longer exists" in hihyoji
    for number in (ANRITSU, STAR, STAR_NEW, FOREIGN, HIHYOJI, STRUCK, TOKYO, ABSENT):
        text = finding_nta_japan(await _fetch(number)) or ""
        assert len(text) <= MAX_FINDING_CHARS
        for word in ("owner", "shareholder", "officer", "active"):
            assert word not in text.lower()


async def test_the_hit_builder_asserts_the_number_only_when_the_register_published_it(index):
    ctx = _LookupCtx(lei="353800886SENISUY0G83", legal_name="ANRITSU CORPORATION",
                     jurisdiction="JP", registered_as="0210-01-021865")
    _build_derived(ctx, JP_COMPANY_REGISTRATION_RA)
    assert ctx.derived.get("jp_jcn") == ANRITSU
    hit = _build_result_hit("nta_japan", await _fetch(ANRITSU), ctx)
    assert hit is not None and hit.identifiers == {"jp_jcn": ANRITSU}
    ghost = _build_result_hit("nta_japan", await _fetch(ABSENT, "Ghost"), ctx)
    assert ghost is not None and ghost.identifiers == {}


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="nta_japan")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index):
    ctx = _LookupCtx(lei="353800886SENISUY0G83", jurisdiction="JP", registered_as="0210-01-021865")
    _build_derived(ctx, JP_COMPANY_REGISTRATION_RA)
    assert _dispatched(ctx) == {"nta_japan"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="353800886SENISUY0G83", jurisdiction="JP", registered_as="0210-01-021865")
    _build_derived(ctx, JP_COMPANY_REGISTRATION_RA)
    assert _dispatched(ctx) == set()
