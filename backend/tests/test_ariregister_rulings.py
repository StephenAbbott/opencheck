"""Rulings and commercial pledges as register-compliance context (the
rulings phase).

Two more daily files about the company's standing with the register, not who
owns it. Measured 16 Oct 2026: 2,886,044 rulings, of which 148,469 are the
compliance types kept and in force (``J``) on 40,684 companies — and 100,163
on 19,162 companies once a filed annual report has answered the ones about a
report; 8,068 current commercial pledges on 4,945 companies.

Stephen's decisions (16 Oct 2026): a finding sentence, never a signal; the
ticket's types plus X1 and every compulsory-dissolution warning, always in
words; a BODS note listing the rulings; one note per pledge (a bank by name
and registry code, a person as "a private individual") and a count in the
finding.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods.mappers.estonia import EE_RULING_LABELS, EE_RULING_SHORT
from opencheck.bods.mapper import map_ariregister
from opencheck.findings import finding_ariregister
from opencheck.knowability import BANNED_VOCABULARY
from opencheck.sources import ariregister
from opencheck.sources.ariregister import AriregisterAdapter, rulings_in_force
from tests.test_ariregister_index import (
    AGENT,
    EESTI,
    HAUDE,
    HOLD,
    STATE,
    _build,
    _point_at,
    _ruling,
    builder,
)


@pytest.fixture()
def index(tmp_path: Path):
    out, meta = _build(tmp_path)
    _point_at(out)
    yield out, meta
    _point_at(None)


async def _statements(code: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with provenance.recording() as rec:
        bundle = await AriregisterAdapter().fetch(code)
    with provenance.mapping_provenance(rec.resolve(is_stub=False)):
        return bundle, list(map_ariregister(bundle))


def _subject(statements: list[dict[str, Any]], code: str) -> dict[str, Any]:
    return next(
        s for s in statements if s["recordType"] == "entity"
        and any(i.get("id") == code for i in s["recordDetails"].get("identifiers") or [])
    )


def _notes(stmt: dict[str, Any]) -> list[str]:
    return [a["description"] for a in stmt.get("annotations") or [] if a["motivation"] == "commenting"]


# --- the builder ------------------------------------------------------------


def test_only_the_compliance_rulings_in_force_are_kept(index) -> None:
    out, meta = index
    conn = sqlite3.connect(out)
    held = dict(conn.execute("SELECT code, rulings FROM ruling"))
    # 007 Agent's are an entry ruling and a remedied deficiency ruling.
    assert AGENT not in held
    assert set(held) == {EESTI, HAUDE, STATE}
    # The remedied fine warning (L) is not kept.
    assert meta["rulings"] == "4" and meta["ruled_companies"] == "3"


def test_the_reader_gives_the_registers_wording_newest_first(index) -> None:
    rows = ariregister.rulings(HAUDE)
    assert [(r["type"], r["day"]) for r in rows] == [("X2", "2026-03-12"), ("H", "2024-06-04")]
    assert rows[0]["number"] == "Ä 2 / X2" and rows[0]["deadline"] == "2026-05-12"
    assert rows[0]["type_text"] == "Hoiatusmäärus registrist kustutamiseks: majandusaasta aruanne esitamata"
    assert ariregister.rulings(AGENT) == []


def test_pledges_keep_the_current_ones_and_never_a_persons_code(index) -> None:
    out, meta = index
    pledges = ariregister.pledges(HOLD)
    assert [p["number"] for p in pledges] == [1, 2]  # the ended third pledge is not current
    assert pledges[0]["pledgees"] == [{"kind": "J", "name": "AS LHV Pank", "code": "10539549"}]
    assert pledges[1]["pledgees"] == [{"kind": "F"}]
    assert pledges[1]["currency"] == "EEK"  # as filed
    assert meta["pledges"] == "2" and meta["pledged_companies"] == "1"
    raw = out.read_bytes()
    assert b"34701014283" not in raw and "Juku".encode() not in raw
    assert ariregister.pledges(AGENT) == []


@pytest.mark.parametrize("change, match", [
    ({"maaruse_kpv": None}, "undated"),
    ({"maaruse_kpv": "2026-03-12"}, "DD.MM.YYYY|undated"),
])
def test_the_builder_refuses_an_undated_ruling(tmp_path: Path, change: dict, match: str) -> None:
    bad = {**_ruling("Ä 9 / X2", "12.03.2026", "X2", "Hoiatusmäärus"), **change}
    with pytest.raises(builder.BuildRefused, match=match):
        _build(tmp_path, rulings=[{"ariregistri_kood": int(HAUDE), "nimi": "Haudemaja OÜ", "maarused": [bad]}])


def test_the_builder_refuses_a_company_the_csv_lacks(tmp_path: Path) -> None:
    with pytest.raises(builder.BuildRefused, match="is not in"):
        _build(tmp_path, pledges=[{"ariregistri_kood": 11111111, "nimi": "Ghost OÜ", "kommertspandiandmed": []}])


def test_the_builder_and_the_reader_keep_the_same_types() -> None:
    assert builder.RULING_TYPES == ariregister.RULING_TYPES
    assert ariregister.ANNUAL_REPORT_RULINGS <= ariregister.RULING_TYPES
    assert set(EE_RULING_LABELS) == set(EE_RULING_SHORT) == set(ariregister.RULING_TYPES)


def test_an_index_built_before_the_tables_reads_none(tmp_path: Path) -> None:
    out, _meta = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("DROP TABLE ruling")
    conn.execute("DROP TABLE pledge")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert ariregister.rulings(HAUDE) is None and ariregister.pledges(HOLD) is None
    finally:
        _point_at(None)


# --- what counts as current ---------------------------------------------------


@pytest.mark.parametrize("kind, day, filed, shown", [
    # A report for a year ending within 18 months before the ruling answers it.
    ("H", "2025-09-01", "2025-12-31", False),
    ("X2", "2026-03-12", "2024-12-31", False),
    # The report on file is older than the year the ruling asked about.
    ("X2", "2026-03-12", "2023-12-31", True),
    ("HM", "2024-02-02", None, True),
    # Not about a report: a net-assets warning stands whatever was filed.
    ("Y3", "2026-01-05", "2025-12-31", True),
    ("GY", "2026-01-05", "2025-12-31", True),
])
def test_a_ruling_answered_by_a_later_report_is_not_current(kind: str, day: str, filed: str | None, shown: bool) -> None:
    rows = [{"type": kind, "day": day}]
    assert bool(rulings_in_force(rows, filed)) is shown


async def test_the_bundle_carries_the_current_rulings_and_pledges(index) -> None:
    eesti, _ = await _statements(EESTI)
    assert eesti["rulings"] == []  # its fine is answered by the 2025 report
    haude, _ = await _statements(HAUDE)
    assert [r["type"] for r in haude["rulings"]] == ["X2", "H"]
    hold, _ = await _statements(HOLD)
    assert len(hold["pledges"]) == 2 and hold["rulings"] == []


# --- the finding ----------------------------------------------------------------


async def test_the_latest_ruling_takes_the_registration_lead(index) -> None:
    haude, _ = await _statements(HAUDE)
    sentence = finding_ariregister(haude) or ""
    assert sentence.startswith("Annual-report deletion warning 12 March 2026; "), sentence
    assert "In the register since" not in sentence
    state, _ = await _statements(STATE)
    sentence = finding_ariregister(state) or ""
    assert sentence.startswith("Net-assets dissolution warning 5 January 2026; ")
    assert "beneficial owners not required" in sentence


async def test_a_company_with_no_current_ruling_reads_as_before(index) -> None:
    eesti, _ = await _statements(EESTI)
    assert "warning" not in (finding_ariregister(eesti) or "")
    assert "fine" not in (finding_ariregister(eesti) or "")


async def test_the_counts_are_never_pushed_out_by_a_ruling(index) -> None:
    haude, _ = await _statements(HAUDE)
    long = {**haude, "rulings": [{"type": "Y3", "day": "2026-01-05"}], "status": "In liquidation"}
    sentence = finding_ariregister(long) or ""
    assert sentence.startswith("In liquidation; ")
    assert "beneficial owners" in sentence


async def test_the_finding_counts_the_pledges_where_there_is_room(index) -> None:
    hold, _ = await _statements(HOLD)
    short = {**hold, "chain": {}, "history": {}, "general": {}}
    assert "2 commercial pledges" in (finding_ariregister(short) or "")
    one = {**short, "pledges": short["pledges"][:1]}
    assert "1 commercial pledge" in (finding_ariregister(one) or "")


# --- the BODS ----------------------------------------------------------------------


async def test_the_rulings_note_lists_every_ruling_in_words(index) -> None:
    _bundle, statements = await _statements(HAUDE)
    notes = [n for n in _notes(_subject(statements, HAUDE)) if n.startswith("Rulings of the e-Business Register")]
    assert len(notes) == 1
    note = notes[0]
    assert "Deletion warning: annual report not filed (hoiatusmäärus registrist kustutamiseks), 2026-03-12 (ruling Ä 2 / X2)" in note
    assert "Annual-report fine (aruandetrahvimäärus), 2024-06-04" in note
    assert note.index("2026-03-12") < note.index("2024-06-04")


async def test_a_pledge_is_a_note_never_a_party(index) -> None:
    _bundle, statements = await _statements(HOLD)
    notes = [n for n in _notes(_subject(statements, HOLD)) if n.startswith("Commercial pledge")]
    assert notes == [
        "Commercial pledge (kommertspant) of 520,000.00 EUR, first rank, entered on 2018-06-19, in favour "
        "of AS LHV Pank (registry code 10539549), as recorded by the e-Business Register. A pledgee holds "
        "security over the company's assets; it is not an owner.",
        "Commercial pledge (kommertspant) of 100,000.00 EEK, second rank, entered on 2009-05-02, in favour "
        "of a private individual, as recorded by the e-Business Register. A pledgee holds security over the "
        "company's assets; it is not an owner.",
    ]
    names = [s["recordDetails"].get("name") for s in statements if s["recordType"] != "relationship"]
    assert "AS LHV Pank" not in names
    text = repr(statements)
    assert "34701014283" not in text and "Juku" not in text


def test_no_word_judges() -> None:
    words = [*EE_RULING_LABELS.values(), *EE_RULING_SHORT.values()]
    for text in words:
        for banned in BANNED_VOCABULARY:
            assert banned not in text.lower(), (banned, text)


async def test_the_notes_pass_the_vocabulary_too(index) -> None:
    for code in (HAUDE, HOLD, STATE):
        _bundle, statements = await _statements(code)
        for note in _notes(_subject(statements, code)):
            if note.startswith(("Rulings", "Commercial pledge")):
                for banned in BANNED_VOCABULARY:
                    assert banned not in note.lower(), (banned, note)


# --- the download ---------------------------------------------------------------


class _Resp:
    def __init__(self, body: bytes, length: int) -> None:
        import io

        self._io = io.BytesIO(body)
        self.headers = {"Content-Type": "application/zip", "Content-Length": str(length),
                        "Last-Modified": "Fri, 16 Oct 2026 09:30:26 GMT"}

    def read(self, n: int = -1) -> bytes:
        return self._io.read(n)

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def test_a_download_cut_short_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """16 Oct 2026: the rulings file arrived as 73,269,248 of 74,262,843
    bytes — a zip's first bytes, no directory at the end. The magic-number
    check passed it; the build then refused the whole index."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("x.json", "[]")
    whole = buf.getvalue()
    answers = [_Resp(whole[: len(whole) // 2], len(whole)), _Resp(whole, len(whole))]
    monkeypatch.setattr(builder.urllib.request, "urlopen", lambda req, timeout=0: answers.pop(0))
    monkeypatch.setattr(builder.time, "sleep", lambda s: None)
    dest = tmp_path / builder.RULINGS_FILE
    assert builder.download(builder.RULINGS_FILE, dest) == "Fri, 16 Oct 2026 09:30:26 GMT"
    assert dest.read_bytes() == whole and answers == []


def test_a_download_that_never_arrives_whole_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    whole = b"PK" + b"\0" * 100
    monkeypatch.setattr(builder.urllib.request, "urlopen", lambda req, timeout=0: _Resp(whole, len(whole)))
    monkeypatch.setattr(builder.time, "sleep", lambda s: None)
    with pytest.raises(builder.BuildRefused, match="could not download"):
        builder.download(builder.RULINGS_FILE, tmp_path / builder.RULINGS_FILE, attempts=2)
