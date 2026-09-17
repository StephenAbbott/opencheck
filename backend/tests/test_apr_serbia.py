"""Serbia — APR company register (openapi.apr.gov.rs monthly open data).

The fixture file is written in the shape of the real 31 August 2026 cut: a
pretty-printed object with ``DatumPreseka`` first and ``Podaci`` keyed on the
8-digit matični broj, names in both scripts, and all four statuses APR
publishes. The builder is driven into a real SQLite file and read back
through the real adapter, so the two sides cannot disagree about the index.
"""

from __future__ import annotations

import io
import json
import os
import ssl
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from opencheck import degradation, provenance
from opencheck.bods import liveness, map_apr_serbia
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_apr_serbia
from opencheck.licensing import classify
from opencheck.routers.hit_builders import _bh_apr_serbia, _LookupCtx
from opencheck.sources import apr_serbia
from opencheck.sources.apr_serbia import (
    APR_INTERMEDIATE_PEM,
    COVERAGE_NOT_IN_REGISTER,
    RS_RA_CODES,
    AprSerbiaAdapter,
    IndexBuildError,
    build_index,
    fold,
    iter_register,
    name_key,
    normalise_mb,
    to_latin,
)

TELEKOM = "17162543"
EPS = "20053658"
LIQUIDATING = "21685712"
BANKRUPT = "20251743"
FORCED = "17334492"
ABSENT = "07000529"  # the Chamber of Commerce: not a company on this register

RECORDS: dict[str, dict[str, Any]] = {
    TELEKOM: {
        "PoslovnoIme": "Preduzeće za telekomunikacije Telekom Srbija akcionarsko društvo, Beograd",
        "SifraOpstine": "70114",
        "NazivOpstine": "ПАЛИЛУЛА",
        "NazivStatus": "Активан",
        "DatumOsnivanja": "1997-05-23",
        "NazivPravneForme": "Акционарско друштво",
        "SifraDelatnosti": "6110",
    },
    EPS: {
        "PoslovnoIme": "Акционарско друштво Електропривреда Србије, Београд",
        "SifraOpstine": "70220",
        "NazivOpstine": "СТАРИ ГРАД",
        "NazivStatus": "Активан",
        "DatumOsnivanja": "2005-07-01",
        "NazivPravneForme": "Акционарско друштво",
        "SifraDelatnosti": "3511",
    },
    LIQUIDATING: {
        "PoslovnoIme": "HILL DOO UB - U LIKVIDACIJI",
        "SifraOpstine": "70246",
        "NazivOpstine": "УБ",
        "NazivStatus": "У ликвидацији",
        "DatumOsnivanja": "2014-03-11",
        "NazivPravneForme": "Друштво са ограниченом одговорношћу",
        "SifraDelatnosti": "4690",
    },
    BANKRUPT: {
        "PoslovnoIme": "ТРАНСПОРТНО ПРЕДУЗЕЋЕ ВУЧКОВИЋ-ТРАНС ДОО ЗАБРЕГА - У СТЕЧАЈУ",
        "SifraOpstine": "70653",
        "NazivOpstine": "ПАРАЋИН",
        "NazivStatus": "У стечају",
        "DatumOsnivanja": "2006-10-02",
        "NazivPravneForme": "Друштво са ограниченом одговорношћу",
        "SifraDelatnosti": "4941",
    },
    FORCED: {
        "PoslovnoIme": "LCC CAPITAL DOO, BEOGRAD",
        "SifraOpstine": "70157",
        "NazivOpstine": "ЗЕМУН",
        "NazivStatus": "У принудној ликвидацији",
        "DatumOsnivanja": "2011-01-20",
        "NazivPravneForme": "Друштво са ограниченом одговорношћу",
        "SifraDelatnosti": "7022",
    },
}


def _write_register(path: Path, records: dict[str, Any] | None = None, *, cut: str | None = "2026-08-31") -> Path:
    payload: dict[str, Any] = {}
    if cut is not None:
        payload["DatumPreseka"] = cut
    payload["Podaci"] = RECORDS if records is None else records
    # Pretty-printed with raw UTF-8, as APR serves it.
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=4), encoding="utf-8")
    return path


@pytest.fixture()
def register_file(tmp_path: Path) -> Path:
    return _write_register(tmp_path / "companies.json")


@pytest.fixture()
def built(register_file: Path, tmp_path: Path) -> tuple[Path, dict[str, str]]:
    out = tmp_path / "apr_serbia.sqlite"
    return out, build_index(register_file, out)


@pytest.fixture()
def index(built: tuple[Path, dict[str, str]]):
    """The built index, pointed at by the setting, with downloads off."""
    path, _ = built
    get_settings.cache_clear()
    os.environ["APR_SERBIA_DB_FILE"] = str(path)
    os.environ["APR_SERBIA_SYNC"] = "false"
    apr_serbia.reset_connection()
    yield path
    os.environ.pop("APR_SERBIA_DB_FILE", None)
    os.environ.pop("APR_SERBIA_SYNC", None)
    get_settings.cache_clear()
    apr_serbia.reset_connection()


# ---------------------------------------------------------------------------
# Identifier grammar and names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", [TELEKOM, f" {EPS} ", "07000529"])
def test_normalise_mb_accepts_eight_digits(raw: str) -> None:
    assert normalise_mb(raw) == raw.strip()


@pytest.mark.parametrize("raw", ["5/0-44-3581/4-10", "1716254", "171625430", "", "SR17162543"])
def test_normalise_mb_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalise_mb(raw)


def test_ra_codes_are_apr_only() -> None:
    assert RS_RA_CODES == {"RA000517"}


def test_to_latin_is_the_serbian_letter_table() -> None:
    assert to_latin("Електропривреда Србије") == "Elektroprivreda Srbije"
    assert to_latin("ђак ћевап џеп љубав њива") == "đak ćevap džep ljubav njiva"
    # A digraph capital is title-case inside a word and all-caps in capitals.
    assert to_latin("Љубица") == "Ljubica"
    assert to_latin("ЉУБИЦА ЏЕП") == "LJUBICA DŽEP"
    # Latin text passes through untouched.
    assert to_latin("HILL DOO UB") == "HILL DOO UB"


def test_fold_and_name_key_meet_across_scripts() -> None:
    assert fold("Друштво са ограниченом одговорношћу ВЕТРОПАРК") == fold(
        "Društvo sa ograničenom odgovornošću VETROPARK"
    )
    assert name_key("VIS-ALT DOO") == name_key("ДРУШТВО СА ОГРАНИЧЕНОМ ОДГОВОРНОШЋУ VIS-ALT") == "VIS ALT"
    assert fold("Đorđević") == "DJORDJEVIC"


# ---------------------------------------------------------------------------
# Reading the register as a stream
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chunk", [1, 7, 64, 1 << 16])
def test_iter_register_matches_json_load_at_any_chunk_size(register_file: Path, chunk: int) -> None:
    """Records split across every possible buffer boundary still parse whole."""
    expected = json.loads(register_file.read_text(encoding="utf-8"))
    with open(register_file, "rb") as fh:
        items = list(iter_register(fh, chunk=chunk))
    assert items[0] == ("DatumPreseka", "2026-08-31")
    assert dict(v for k, v in items if k == "Podaci") == expected["Podaci"]


def test_iter_register_reads_numbers_at_a_buffer_edge() -> None:
    raw = b'{"DatumPreseka": "2026-08-31", "Podaci": {"12345678": {"SifraOpstine": 70157}}}'
    items = list(iter_register(io.BytesIO(raw), chunk=1))
    assert items[1] == ("Podaci", ("12345678", {"SifraOpstine": 70157}))


def test_iter_register_rejects_a_truncated_file() -> None:
    raw = b'{"DatumPreseka": "2026-08-31", "Podaci": {"12345678": {"PoslovnoIme": "AB'
    with pytest.raises(IndexBuildError):
        list(iter_register(io.BytesIO(raw), chunk=8))


def test_iter_register_stops_after_the_cut_date() -> None:
    """The peek needs only the opening bytes: nothing past them is read."""

    class Opening(io.BytesIO):
        def read(self, n: int = -1) -> bytes:
            data = super().read(n)
            if self.tell() > 80:
                raise AssertionError("read past the opening")
            return data

    raw = b'{\n    "DatumPreseka": "2026-08-31",\n    "Podaci": {' + b" " * 500
    reader = iter_register(Opening(raw), chunk=16)
    assert next(reader) == ("DatumPreseka", "2026-08-31")


# ---------------------------------------------------------------------------
# Building the index
# ---------------------------------------------------------------------------


def test_build_index_counts(built: tuple[Path, dict[str, str]]) -> None:
    _, meta = built
    assert meta["companies"] == "5"
    assert meta["snapshot_date"] == "2026-08-31"
    assert meta["status:Активан"] == "2"
    assert meta["status:У стечају"] == "1"


def test_build_index_fails_loudly_on_an_unknown_status(tmp_path: Path, built: tuple[Path, dict[str, str]]) -> None:
    good, _ = built
    before = good.read_bytes()
    changed = dict(RECORDS)
    changed["99999999"] = {**RECORDS[TELEKOM], "NazivStatus": "Брисан"}
    source = _write_register(tmp_path / "changed.json", changed)
    with pytest.raises(IndexBuildError, match="unknown company status"):
        build_index(source, good)
    # The working index is untouched and no half-built file is left behind.
    assert good.read_bytes() == before
    assert not good.with_name(good.name + ".building").exists()


def test_build_index_requires_the_cut_date(tmp_path: Path) -> None:
    source = _write_register(tmp_path / "nocut.json", cut=None)
    with pytest.raises(IndexBuildError, match="DatumPreseka"):
        build_index(source, tmp_path / "out.sqlite")


def test_build_index_fails_when_the_feed_changes_shape(tmp_path: Path) -> None:
    source = _write_register(tmp_path / "shape.json", {TELEKOM: {"Naziv": "X", "NazivStatus": "Активан"}})
    with pytest.raises(IndexBuildError, match="changed shape"):
        build_index(source, tmp_path / "out.sqlite")


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


async def test_fetch_returns_the_record_and_records_the_snapshot(index: Path) -> None:
    with provenance.recording() as observed:
        bundle = await AprSerbiaAdapter().fetch(EPS, legal_name="Akcionarsko drunstvo Elektroprivreda Srbije, Beograd")
    assert bundle["is_stub"] is False
    company = bundle["company"]
    assert company["name"] == "Акционарско друштво Електропривреда Србије, Београд"
    assert company["name_latin"] == "Akcionarsko društvo Elektroprivreda Srbije, Beograd"
    assert company["status"] == "Активан"
    assert "name_key" not in company
    assert bundle["snapshot_date"] == "2026-08-31"
    resolved = observed.resolve()
    assert resolved.liveness == "snapshot"
    assert resolved.retrieved_at.date() == date(2026, 8, 31)


async def test_fetch_not_in_register_is_an_answer_not_a_stub(index: Path) -> None:
    with degradation.recording() as degraded:
        bundle = await AprSerbiaAdapter().fetch(ABSENT)
    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert bundle["coverage_note"] == COVERAGE_NOT_IN_REGISTER
    assert degraded == []
    assert list(map_apr_serbia(bundle)) == []
    hit = _bh_apr_serbia(bundle, ABSENT, _LookupCtx(lei="X" * 20, legal_name="SERBIAN CHAMBER OF COMMERCE"))
    assert hit.identifiers == {}
    assert "deleted companies" in (hit.finding or "")


async def test_fetch_rejects_a_non_number(index: Path) -> None:
    bundle = await AprSerbiaAdapter().fetch("5/0-44-3581/4-10")
    assert bundle["is_stub"] is True


async def test_fetch_without_an_index_offline_is_a_quiet_stub(tmp_path: Path) -> None:
    get_settings.cache_clear()
    os.environ["APR_SERBIA_DB_FILE"] = str(tmp_path / "absent.sqlite")
    apr_serbia.reset_connection()
    try:
        with degradation.recording() as degraded:
            bundle = await AprSerbiaAdapter().fetch(TELEKOM)
        assert bundle["is_stub"] is True
        assert degraded == []
    finally:
        os.environ.pop("APR_SERBIA_DB_FILE", None)
        get_settings.cache_clear()
        apr_serbia.reset_connection()


async def test_fetch_without_an_index_live_degrades(tmp_path: Path, monkeypatch) -> None:
    get_settings.cache_clear()
    os.environ["APR_SERBIA_DB_FILE"] = str(tmp_path / "absent.sqlite")
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    apr_serbia.reset_connection()
    monkeypatch.setattr(apr_serbia, "_start_background_sync", lambda: None)
    try:
        with degradation.recording() as degraded:
            bundle = await AprSerbiaAdapter().fetch(TELEKOM)
        assert bundle["is_stub"] is True
        assert [d.source_id for d in degraded] == ["apr_serbia"]
        assert TELEKOM not in degraded[0].detail
    finally:
        os.environ.pop("APR_SERBIA_DB_FILE", None)
        os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
        get_settings.cache_clear()
        apr_serbia.reset_connection()


async def test_search_reads_the_index_in_either_script(index: Path) -> None:
    from opencheck.sources.base import SearchKind

    hits = await AprSerbiaAdapter().search("Elektroprivreda Srbije", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [EPS]
    assert hits[0].identifiers == {"rs_mb": EPS}
    hits = await AprSerbiaAdapter().search("вучковић", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [BANKRUPT]


def test_info_declares_the_register(index: Path) -> None:
    info = AprSerbiaAdapter().info
    assert info.is_national_register is True
    assert info.country == "RS"
    assert info.requires_api_key is False
    assert info.license == "SODL-1.0"


def test_licence_is_classified_as_commercial_reuse() -> None:
    terms = classify("SODL-1.0")
    assert terms.commercial_use == "yes"
    assert terms.color == "green"


def test_tls_context_trusts_apr_through_the_pinned_intermediate() -> None:
    """Verification stays on; the intermediate APR fails to send is added."""
    context = apr_serbia.tls_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    subjects = [dict(x[0] for x in cert["subject"]).get("commonName") for cert in context.get_ca_certs()]
    assert "SSL2BUY EMEA RSA Domain Validation Secure Server CA" in subjects
    assert APR_INTERMEDIATE_PEM.count("BEGIN CERTIFICATE") == 1


# ---------------------------------------------------------------------------
# BODS, finding, hit
# ---------------------------------------------------------------------------


async def test_mapper_entity_statement(index: Path) -> None:
    bundle = await AprSerbiaAdapter().fetch(EPS)
    statements = list(map_apr_serbia(bundle))
    assert len(statements) == 1
    entity = statements[0]
    details = entity["recordDetails"]
    assert entity["recordType"] == "entity"
    assert details["name"] == "Акционарско друштво Електропривреда Србије, Београд"
    # The official Serbian Latin form comes first. make_entity_statement then
    # adds the house-wide ASCII transliteration every Cyrillic name gets, for
    # screening and search; it is not Serbian orthography and is not ours.
    assert details["alternateNames"][0] == "Akcionarsko društvo Elektroprivreda Srbije, Beograd"
    assert details["identifiers"] == [
        {"id": EPS, "scheme": "RS-APR", "schemeName": "Matični broj — Business Registers Agency (Serbia)"}
    ]
    assert details["jurisdiction"] == {"name": "Serbia", "code": "RS"}
    assert details["foundingDate"] == "2005-07-01"
    assert details["entityType"] == {"type": "registeredEntity", "details": "Акционарско друштво"}
    assert details["addresses"] == [
        {"type": "registered", "address": "СТАРИ ГРАД", "country": {"name": "Serbia", "code": "RS"}}
    ]
    status = liveness.read_register_status(entity)
    assert status is not None and status["liveness"] == "live"
    assert "dissolutionDate" not in details


async def test_mapper_latin_name_has_no_alternate(index: Path) -> None:
    entity = next(iter(map_apr_serbia(await AprSerbiaAdapter().fetch(TELEKOM))))
    assert "alternateNames" not in entity["recordDetails"] or entity["recordDetails"]["alternateNames"] == []


@pytest.mark.parametrize("mb", [LIQUIDATING, BANKRUPT, FORCED])
async def test_mapper_insolvency_is_pending_never_terminal(index: Path, mb: str) -> None:
    entity = next(iter(map_apr_serbia(await AprSerbiaAdapter().fetch(mb))))
    status = liveness.read_register_status(entity)
    assert status is not None
    assert status["liveness"] == "pending"
    assert status["raw"] == RECORDS[mb]["NazivStatus"]
    assert "dissolutionDate" not in entity["recordDetails"]


async def test_finding_sentences(index: Path) -> None:
    eps = finding_apr_serbia(await AprSerbiaAdapter().fetch(EPS))
    assert eps == "Founded 2005-07-01, акционарско друштво."
    bankrupt = finding_apr_serbia(await AprSerbiaAdapter().fetch(BANKRUPT))
    assert bankrupt is not None and bankrupt.startswith("In bankruptcy (У стечају)")
    for mb in (*RECORDS, ABSENT):
        sentence = finding_apr_serbia(await AprSerbiaAdapter().fetch(mb))
        assert sentence and len(sentence) <= MAX_FINDING_CHARS
        # The feed publishes no people; the finding must not say so as a fact.
        assert "director" not in sentence.lower() and "owner" not in sentence.lower()


async def test_hit_builder_asserts_the_number(index: Path) -> None:
    bundle = await AprSerbiaAdapter().fetch(TELEKOM)
    hit = _bh_apr_serbia(bundle, TELEKOM, _LookupCtx(lei="X" * 20))
    assert hit.identifiers == {"rs_mb": TELEKOM}
    assert hit.summary == f"RS-APR {TELEKOM}"


def test_build_derived_maps_apr() -> None:
    from opencheck.routers.lookup import _build_derived

    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "RS"
    ctx.registered_as = TELEKOM
    _build_derived(ctx, "RA000517")
    assert ctx.derived.get("rs_mb") == TELEKOM


def test_rs_apr_is_a_register_hop() -> None:
    from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID
    from opencheck.register_hops import hop_for

    assert _GLEIF_RA_TO_ORG_ID["RA000517"][0] == "RS-APR"
    hop = hop_for("RS-APR")
    assert hop is not None and hop.source_id == "apr_serbia"


def test_national_id_reverse_lookup_is_scoped_to_apr() -> None:
    from opencheck.ra_codes import ra_code_for

    assert ra_code_for("RS", TELEKOM) == "RA000517"


# ---------------------------------------------------------------------------
# Keeping the index current
# ---------------------------------------------------------------------------


def _live_env(target: Path) -> None:
    get_settings.cache_clear()
    os.environ["APR_SERBIA_DB_FILE"] = str(target)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    apr_serbia.reset_connection()
    apr_serbia._STATE["last_check"] = None


def _clear_env() -> None:
    os.environ.pop("APR_SERBIA_DB_FILE", None)
    os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
    get_settings.cache_clear()
    apr_serbia.reset_connection()


def test_sync_downloads_and_builds_when_absent(register_file: Path, tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "synced.sqlite"
    _live_env(target)
    calls: list[str] = []

    def fake_download(url: str, dest: Path) -> int:
        calls.append(url)
        dest.write_bytes(register_file.read_bytes())
        return dest.stat().st_size

    monkeypatch.setattr(apr_serbia, "_download", fake_download)
    monkeypatch.setattr(apr_serbia, "peek_cut_date", lambda url=None: pytest.fail("no index: nothing to compare"))
    monkeypatch.setattr(apr_serbia, "snapshot_age_days", lambda meta: 17 if meta else None)
    try:
        first = apr_serbia.sync_index()
        assert first["apr_serbia"] == "built"
        assert first["companies"] == "5"
        assert not target.with_name(target.name + ".json").exists()
        # Fresh now: the second call keeps the file and asks nobody.
        assert apr_serbia.sync_index()["apr_serbia"] == "kept"
        assert calls == [apr_serbia.COMPANIES_URL]
        assert apr_serbia.index_available()
    finally:
        _clear_env()


def test_sync_keeps_a_stale_index_when_apr_serves_the_same_cut(built: tuple[Path, dict[str, str]], monkeypatch) -> None:
    path, _ = built
    _live_env(path)
    monkeypatch.setattr(apr_serbia, "snapshot_age_days", lambda meta: 40)
    monkeypatch.setattr(apr_serbia, "peek_cut_date", lambda url=None: "2026-08-31")
    monkeypatch.setattr(apr_serbia, "_download", lambda url, dest: pytest.fail("same cut: no download"))
    try:
        assert apr_serbia.sync_index()["apr_serbia"] == "kept"
    finally:
        _clear_env()


def test_sync_rebuilds_when_apr_serves_a_newer_cut(built: tuple[Path, dict[str, str]], tmp_path: Path, monkeypatch) -> None:
    path, _ = built
    _live_env(path)
    newer = _write_register(tmp_path / "newer.json", {TELEKOM: RECORDS[TELEKOM]}, cut="2026-09-30")
    monkeypatch.setattr(apr_serbia, "snapshot_age_days", lambda meta: 40)
    monkeypatch.setattr(apr_serbia, "peek_cut_date", lambda url=None: "2026-09-30")
    monkeypatch.setattr(apr_serbia, "_download", lambda url, dest: dest.write_bytes(newer.read_bytes()))
    try:
        outcome = apr_serbia.sync_index()
        assert outcome["apr_serbia"] == "built"
        assert outcome["snapshot_date"] == "2026-09-30"
        assert outcome["companies"] == "1"
    finally:
        _clear_env()


def test_sync_failure_keeps_the_old_index(built: tuple[Path, dict[str, str]], monkeypatch) -> None:
    path, _ = built
    _live_env(path)

    def boom(url=None):
        raise OSError("APR unreachable")

    monkeypatch.setattr(apr_serbia, "peek_cut_date", boom)
    monkeypatch.setattr(apr_serbia, "snapshot_age_days", lambda meta: 40)
    try:
        outcome = apr_serbia.sync_index()
        assert outcome["apr_serbia"] == "failed"
        assert "APR unreachable" in outcome["error"]
        assert apr_serbia.index_available()
    finally:
        _clear_env()


def test_peek_cut_date_reads_the_opening_of_the_response(httpx_mock) -> None:
    body = b'{\n    "DatumPreseka": "2026-08-31",\n    "Podaci": {\n        "07533209": {}\n    }\n}'
    httpx_mock.add_response(url=apr_serbia.COMPANIES_URL, content=body)
    assert apr_serbia.peek_cut_date() == "2026-08-31"


def test_peek_cut_date_rejects_an_unexpected_opening(httpx_mock) -> None:
    httpx_mock.add_response(url=apr_serbia.COMPANIES_URL, content=b'{"Podaci": {}}')
    with pytest.raises(IndexBuildError):
        apr_serbia.peek_cut_date()
