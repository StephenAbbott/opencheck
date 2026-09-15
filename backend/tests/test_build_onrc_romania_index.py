"""Tests for ``scripts/build_onrc_romania_index.py``.

Both cases here are failures the builder shipped with and that the 15 September
2026 index build hit for real. Neither raised anything at the time: one produced
an index quietly missing 667 companies, the other would have produced one built
from 26% of the register. A wrong index reports success exactly like a right
one, so these are pinned at the two functions that decide what data gets in.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_builder():
    """Import the shipped script by path, rather than reimplementing it."""
    path = (
        Path(__file__).resolve().parent.parent
        / "scripts"
        / "build_onrc_romania_index.py"
    )
    spec = importlib.util.spec_from_file_location("_build_onrc_romania_index", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


builder = _load_builder()


# ---------------------------------------------------------------------------
# rows() — a literal double quote must not swallow the rest of the file
# ---------------------------------------------------------------------------

#: Shaped like the real export: BOM, ``^`` delimiter, CRLF, and two real
#: quoting patterns taken verbatim from the 2 September 2026 ``OD_FIRME.CSV``.
#:
#: Row 1 is the common case — 1,116 rows carry a name whose **first character**
#: is a quote, because ONRC registers the trade name in quotes. Python's default
#: reader consumes them as field delimiters and hands back ``LEMNLIND SRL``, so
#: the index stores a name the register never published.
#:
#: Row 2 is the rare, expensive case — a field-initial quote with no closing
#: partner on the line. The reader stays in quoted mode and eats every following
#: line until the next quote. Two of these in the real file absorbed 667
#: companies, which then failed the corporate-form filter on their shifted
#: columns and vanished without a warning.
_FIRME_WITH_REAL_QUOTING = (
    "﻿"
    "DENUMIRE^CUI^COD_INMATRICULARE^FORMA_JURIDICA^ADR_DEN_STRADA\r\n"
    '"LEMNLIND" SRL^13735411^J14/33/2001^SRL^B-DUL GRIGORE BALAN\r\n'
    '" CARDIOFORCE SRL^22724420^J40/21161/2007^SRL^STR CANDIANO POPESCU\r\n'
    "CAMPINGROD S.R.L.^52089877^J2025048519000^SRL^STRADA LUNGA\r\n"
    "PRAVA ESSENTIA S.R.L.^52132438^J2025050703008^SRL^CALEA SCURTA\r\n"
)


@pytest.fixture()
def firme_csv(tmp_path: Path) -> Path:
    path = tmp_path / "od_firme.csv"
    path.write_bytes(_FIRME_WITH_REAL_QUOTING.encode("utf-8"))
    return path


def test_rows_does_not_let_an_unclosed_quote_swallow_later_companies(
    firme_csv: Path,
) -> None:
    """The expensive regression: four data lines must parse as four records.

    Under default quoting the unclosed quote on ``" CARDIOFORCE`` absorbs both
    companies beneath it. This is the bug that cost the 15 September 2026 index
    667 companies — including CAMPINGROD S.R.L. and PRAVA ESSENTIA S.R.L.,
    which are named here for that reason.
    """
    records = list(builder.rows(firme_csv))
    assert len(records) == 4
    assert [r["COD_INMATRICULARE"] for r in records] == [
        "J14/33/2001",
        "J40/21161/2007",
        "J2025048519000",
        "J2025050703008",
    ]


def test_rows_preserves_quotes_that_are_part_of_the_registered_name(
    firme_csv: Path,
) -> None:
    """The quiet regression: ONRC registers trade names *in quotes*.

    1,116 rows of the real file open a name with one. Stripping them stores a
    name the register never published — and does it silently, which is worse
    than the merge: a lookup for the published name simply misses.
    """
    first = next(iter(builder.rows(firme_csv)))
    assert first["DENUMIRE"] == '"LEMNLIND" SRL'


def test_rows_does_not_shift_columns_on_a_quoted_name(firme_csv: Path) -> None:
    """Column shift is what made the merge invisible rather than loud.

    Swallowed rows failed the corporate-form filter on their displaced
    ``FORMA_JURIDICA`` and were skipped as sole traders, so the build reported
    a plausible count and no warning at all.
    """
    records = list(builder.rows(firme_csv))
    assert records[0]["CUI"] == "13735411"
    assert records[0]["FORMA_JURIDICA"] == "SRL"
    assert records[0]["ADR_DEN_STRADA"] == "B-DUL GRIGORE BALAN"
    # The row *after* the unclosed quote is the one that moved: under default
    # quoting it is not a record at all, it is text inside the one above.
    assert records[2]["DENUMIRE"] == "CAMPINGROD S.R.L."
    assert records[2]["CUI"] == "52089877"
    assert records[2]["FORMA_JURIDICA"] == "SRL"


def test_rows_never_yields_a_field_containing_a_newline(firme_csv: Path) -> None:
    """A newline inside a field is the signature of the merge bug.

    ONRC's format has no quoting, so no field can legitimately span lines.
    """
    for record in builder.rows(firme_csv):
        for value in record.values():
            assert "\n" not in (value or "")


# ---------------------------------------------------------------------------
# download() — a short transfer must not be mistaken for a complete file
# ---------------------------------------------------------------------------


def test_download_rejects_a_short_transfer(tmp_path: Path, monkeypatch) -> None:
    """data.gov.ro drops long transfers; a partial CSV still parses cleanly."""
    dest = tmp_path / "od_firme.csv"
    monkeypatch.setattr(builder, "urllib", _stub_urllib(b"only-the-first-part"))
    with pytest.raises(SystemExit) as excinfo:
        builder.download("https://example.invalid/x.csv", dest, expected_size=1000)
    assert "expected 1000" in str(excinfo.value)


def test_download_discards_a_partial_left_by_an_earlier_run(
    tmp_path: Path, monkeypatch
) -> None:
    """The trap: a non-empty file used to be accepted as done, unconditionally.

    That is what made the failure survive a re-run — the second attempt saw
    bytes on disk and skipped straight to building.
    """
    dest = tmp_path / "od_firme.csv"
    dest.write_bytes(b"truncated")
    monkeypatch.setattr(builder, "urllib", _stub_urllib(b"the-whole-file"))
    builder.download("https://example.invalid/x.csv", dest, expected_size=14)
    assert dest.read_bytes() == b"the-whole-file"


def test_download_keeps_a_file_that_is_already_the_published_size(
    tmp_path: Path, monkeypatch
) -> None:
    """Re-running must not refetch a gigabyte that is already correct."""
    dest = tmp_path / "od_firme.csv"
    dest.write_bytes(b"the-whole-file")

    def _explode(*args, **kwargs):  # pragma: no cover - must not be reached
        raise AssertionError("download refetched a file that was already complete")

    monkeypatch.setattr(builder, "urllib", _stub_urllib(b"", on_open=_explode))
    builder.download("https://example.invalid/x.csv", dest, expected_size=14)
    assert dest.read_bytes() == b"the-whole-file"


def test_download_without_a_published_size_keeps_the_old_behaviour(
    tmp_path: Path, monkeypatch
) -> None:
    """CKAN may omit ``size``; absence must not become a hard failure."""
    dest = tmp_path / "n_stare_firma.csv"
    monkeypatch.setattr(builder, "urllib", _stub_urllib(b"anything"))
    builder.download("https://example.invalid/x.csv", dest, expected_size=None)
    assert dest.read_bytes() == b"anything"


def _stub_urllib(payload: bytes, *, on_open=None):
    """A stand-in for the one seam ``download`` uses, ``urllib.request``."""

    class _Response:
        def __init__(self) -> None:
            self._data = payload

        def read(self, size: int = -1) -> bytes:
            if not self._data:
                return b""
            chunk, self._data = self._data[:size], self._data[size:]
            return chunk

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Request:
        @staticmethod
        def Request(url, headers=None):  # noqa: N802 - mirrors urllib's name
            return url

        @staticmethod
        def urlopen(req, timeout=None):
            if on_open is not None:
                on_open()
            return _Response()

    class _Urllib:
        request = _Request

    return _Urllib
