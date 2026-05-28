"""Phase 8 — Unit tests for BODS mapper date helper functions.

Tests every input format, edge case, and None/empty branch for the four
private date-normalisation helpers:

  * _ee_date              — Estonian dates (DD.MM.YYYY or ISO/API)
  * _normalise_wikidata_date — Wikidata XSD dateTime (±YYYY-MM-DDTHH:MM:SSZ)
  * _lv_date              — Latvian CKAN datetime (YYYY-MM-DDThh:mm:ss…)
  * _at_date_iso          — Austrian DD.MM.YYYY → ISO YYYY-MM-DD

All helpers must return either None or a string that matches YYYY-MM-DD.
"""

from __future__ import annotations

import re

import pytest

from opencheck.bods.mapper import (
    _at_date_iso,
    _ee_date,
    _lv_date,
    _normalise_wikidata_date,
)

# Pattern every non-None return value must satisfy.
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _assert_iso_or_none(value: str | None, *, allow_passthrough: bool = False) -> None:
    """Assert that *value* is either None or a YYYY-MM-DD string."""
    if value is None:
        return
    if allow_passthrough:
        return  # function documented to pass through unrecognised strings
    assert _ISO_DATE_RE.match(value), (
        f"Expected YYYY-MM-DD or None, got {value!r}"
    )


# ---------------------------------------------------------------------------
# _ee_date — Estonian dates
# ---------------------------------------------------------------------------


class TestEeDate:
    """Tests for _ee_date (opencheck.bods.mapper)."""

    # --- None / empty → None ---------------------------------------------------

    def test_none_returns_none(self):
        assert _ee_date(None) is None

    def test_empty_string_returns_none(self):
        assert _ee_date("") is None

    def test_whitespace_only_returns_none(self):
        """Leading/trailing whitespace is stripped; blank string → None."""
        assert _ee_date("   ") is None

    # --- DD.MM.YYYY (Estonian bulk-export format) --------------------------------

    def test_dd_mm_yyyy_standard(self):
        assert _ee_date("01.03.2010") == "2010-03-01"

    def test_dd_mm_yyyy_december(self):
        assert _ee_date("31.12.1999") == "1999-12-31"

    def test_dd_mm_yyyy_single_digit_day_and_month(self):
        """Single-digit day and month must be zero-padded in output."""
        assert _ee_date("1.3.2010") == "2010-03-01"

    def test_dd_mm_yyyy_single_digit_month_only(self):
        assert _ee_date("15.6.2005") == "2005-06-15"

    def test_dd_mm_yyyy_single_digit_day_only(self):
        assert _ee_date("5.11.2020") == "2020-11-05"

    # --- YYYY-MM-DD and ISO variants (API responses) ----------------------------

    def test_iso_date_passthrough(self):
        assert _ee_date("2010-03-01") == "2010-03-01"

    def test_iso_date_with_trailing_z(self):
        """YYYY-MM-DDZ: strip Z suffix."""
        assert _ee_date("2010-03-01Z") == "2010-03-01"

    def test_iso_datetime_api_format(self):
        """YYYY-MM-DDTHH:MM:SSZ: truncate to date portion."""
        assert _ee_date("2010-03-01T12:00:00Z") == "2010-03-01"

    def test_iso_datetime_no_trailing_z(self):
        assert _ee_date("2010-03-01T00:00:00") == "2010-03-01"

    def test_iso_date_with_time_and_millis(self):
        """Some API responses include milliseconds."""
        assert _ee_date("2023-07-14T09:30:00.000Z") == "2023-07-14"

    # --- Edge cases and malformed input -----------------------------------------

    def test_malformed_dot_separated_two_parts_returns_none(self):
        """Two-part dot string (MM.YYYY) can't be parsed as DD.MM.YYYY."""
        assert _ee_date("03.2010") is None

    def test_non_date_string_returns_none(self):
        assert _ee_date("notadate") is None

    def test_numeric_string_returns_none(self):
        assert _ee_date("20230714") is None

    # --- All return values are ISO or None --------------------------------------

    @pytest.mark.parametrize("s, expected", [
        ("01.03.2010", "2010-03-01"),
        ("2010-03-01", "2010-03-01"),
        ("2010-03-01Z", "2010-03-01"),
        ("2010-03-01T12:00:00Z", "2010-03-01"),
        ("1.3.2010", "2010-03-01"),
        (None, None),
        ("", None),
    ])
    def test_parametrized_cases(self, s, expected):
        assert _ee_date(s) == expected


# ---------------------------------------------------------------------------
# _normalise_wikidata_date — Wikidata XSD dateTime
# ---------------------------------------------------------------------------


class TestNormaliseWikidataDate:
    """Tests for _normalise_wikidata_date (opencheck.bods.mapper)."""

    # --- None / empty → None ---------------------------------------------------

    def test_none_returns_none(self):
        assert _normalise_wikidata_date(None) is None

    def test_empty_string_returns_none(self):
        assert _normalise_wikidata_date("") is None

    # --- Wikidata datetime formats -----------------------------------------------

    def test_plus_prefix_with_full_datetime(self):
        """Canonical Wikidata form: +YYYY-MM-DDTHH:MM:SSZ."""
        assert _normalise_wikidata_date("+1952-10-07T00:00:00Z") == "1952-10-07"

    def test_no_plus_prefix_with_full_datetime(self):
        """Some Wikidata values omit the leading +."""
        assert _normalise_wikidata_date("1952-10-07T00:00:00Z") == "1952-10-07"

    def test_plus_prefix_date_only(self):
        """Date-only Wikidata value still has + stripped."""
        assert _normalise_wikidata_date("+2001-01-01") == "2001-01-01"

    def test_already_clean_iso_date(self):
        """Already-clean YYYY-MM-DD passes through unchanged."""
        assert _normalise_wikidata_date("2001-01-01") == "2001-01-01"

    def test_midnight_utc_datetime(self):
        assert _normalise_wikidata_date("+1925-06-30T00:00:00Z") == "1925-06-30"

    def test_century_boundary_date(self):
        assert _normalise_wikidata_date("+2000-01-01T00:00:00Z") == "2000-01-01"

    # --- Parametrized round-trip ------------------------------------------------

    @pytest.mark.parametrize("value, expected", [
        ("+1952-10-07T00:00:00Z", "1952-10-07"),
        ("1952-10-07T00:00:00Z", "1952-10-07"),
        ("+2001-01-01", "2001-01-01"),
        ("2001-01-01", "2001-01-01"),
        (None, None),
        ("", None),
    ])
    def test_parametrized_cases(self, value, expected):
        assert _normalise_wikidata_date(value) == expected

    # --- All return values are ISO or None --------------------------------------

    @pytest.mark.parametrize("value", [
        "+1952-10-07T00:00:00Z",
        "1900-01-01T00:00:00Z",
        "+2025-12-31T23:59:59Z",
        "+2000-06-15",
        "1984-07-04",
    ])
    def test_return_is_iso_date(self, value):
        result = _normalise_wikidata_date(value)
        assert result is not None
        _assert_iso_or_none(result)


# ---------------------------------------------------------------------------
# _lv_date — Latvian CKAN datetime
# ---------------------------------------------------------------------------


class TestLvDate:
    """Tests for _lv_date (opencheck.bods.mapper)."""

    # --- None / empty → None ---------------------------------------------------

    def test_none_returns_none(self):
        assert _lv_date(None) is None

    def test_empty_string_returns_none(self):
        assert _lv_date("") is None

    # --- Latvian CKAN format ----------------------------------------------------

    def test_iso_datetime_T_separator(self):
        """YYYY-MM-DDTHH:MM:SS — truncate at character 10."""
        assert _lv_date("2023-04-15T00:00:00") == "2023-04-15"

    def test_iso_datetime_space_separator(self):
        """YYYY-MM-DD HH:MM:SS — truncate at character 10."""
        assert _lv_date("2023-04-15 10:30:00") == "2023-04-15"

    def test_iso_date_only(self):
        """Already YYYY-MM-DD — returned unchanged."""
        assert _lv_date("2023-04-15") == "2023-04-15"

    def test_datetime_with_timezone(self):
        """Datetime with timezone offset — truncate to date."""
        assert _lv_date("2010-01-01T00:00:00+02:00") == "2010-01-01"

    def test_datetime_with_z_suffix(self):
        assert _lv_date("2010-01-01T00:00:00Z") == "2010-01-01"

    def test_year_only_short_string_returns_year(self):
        """Strings shorter than 10 chars are truncated to what's available."""
        result = _lv_date("2023")
        assert result == "2023"  # truncated at [:10], not None

    # --- Parametrized round-trip ------------------------------------------------

    @pytest.mark.parametrize("dt_str, expected", [
        ("2023-04-15T00:00:00", "2023-04-15"),
        ("2023-04-15", "2023-04-15"),
        ("2023-04-15 10:30:00", "2023-04-15"),
        ("2010-01-01T00:00:00Z", "2010-01-01"),
        (None, None),
        ("", None),
    ])
    def test_parametrized_cases(self, dt_str, expected):
        assert _lv_date(dt_str) == expected


# ---------------------------------------------------------------------------
# _at_date_iso — Austrian DD.MM.YYYY → ISO
# ---------------------------------------------------------------------------


class TestAtDateIso:
    """Tests for _at_date_iso (opencheck.bods.mapper)."""

    # --- None / empty → None ---------------------------------------------------

    def test_none_returns_none(self):
        """None is coerced to '' via (raw or ''), then returns None."""
        assert _at_date_iso(None) is None  # type: ignore[arg-type]

    def test_empty_string_returns_none(self):
        assert _at_date_iso("") is None

    def test_whitespace_only_returns_none(self):
        assert _at_date_iso("   ") is None

    # --- Austrian DD.MM.YYYY format ---------------------------------------------

    def test_standard_date(self):
        assert _at_date_iso("01.03.2010") == "2010-03-01"

    def test_december_date(self):
        assert _at_date_iso("15.12.1999") == "1999-12-15"

    def test_single_digit_day_and_month(self):
        """Single-digit components are zero-padded via int() + :02d format."""
        assert _at_date_iso("1.3.2010") == "2010-03-01"

    def test_single_digit_day_only(self):
        assert _at_date_iso("5.11.2020") == "2020-11-05"

    def test_single_digit_month_only(self):
        assert _at_date_iso("15.6.2005") == "2005-06-15"

    def test_new_year(self):
        assert _at_date_iso("01.01.2000") == "2000-01-01"

    def test_century_boundary(self):
        assert _at_date_iso("31.12.1899") == "1899-12-31"

    # --- Passthrough for already-ISO or unrecognised strings --------------------

    def test_already_iso_passes_through(self):
        """YYYY-MM-DD has no '.' so it falls through to the return raw clause."""
        assert _at_date_iso("2010-03-01") == "2010-03-01"

    def test_unrecognised_string_passes_through(self):
        """The function documents 'already ISO or unrecognised — pass through'."""
        assert _at_date_iso("bogus") == "bogus"

    def test_two_dot_parts_passes_through(self):
        """Only exactly 3 dot-separated parts trigger DD.MM.YYYY parsing."""
        assert _at_date_iso("03.2010") == "03.2010"

    # --- Parametrized round-trip ------------------------------------------------

    @pytest.mark.parametrize("raw, expected", [
        ("01.03.2010", "2010-03-01"),
        ("15.12.1999", "1999-12-15"),
        ("1.3.2010", "2010-03-01"),
        ("", None),
        (None, None),
        ("2010-03-01", "2010-03-01"),   # passthrough
    ])
    def test_parametrized_cases(self, raw, expected):
        assert _at_date_iso(raw) == expected  # type: ignore[arg-type]

    # --- All non-passthrough return values are ISO -------------------------------

    @pytest.mark.parametrize("raw", [
        "01.03.2010",
        "31.12.1999",
        "01.01.2000",
        "15.06.2005",
        "5.11.2020",
    ])
    def test_return_is_iso_date(self, raw):
        result = _at_date_iso(raw)
        _assert_iso_or_none(result)


# ---------------------------------------------------------------------------
# Cross-helper consistency
# ---------------------------------------------------------------------------


class TestDateHelperContract:
    """High-level contract tests: all helpers accept None and return ISO or None."""

    @pytest.mark.parametrize("helper, value", [
        (_ee_date, None),
        (_ee_date, ""),
        (_normalise_wikidata_date, None),
        (_normalise_wikidata_date, ""),
        (_lv_date, None),
        (_lv_date, ""),
        (_at_date_iso, None),
        (_at_date_iso, ""),
    ])
    def test_null_or_empty_always_returns_none(self, helper, value):
        """Every helper must return None for None or empty-string input."""
        assert helper(value) is None  # type: ignore[arg-type]

    @pytest.mark.parametrize("helper, valid_input", [
        (_ee_date, "01.03.2010"),
        (_ee_date, "2023-07-14T09:30:00Z"),
        (_normalise_wikidata_date, "+1952-10-07T00:00:00Z"),
        (_normalise_wikidata_date, "2001-01-01"),
        (_lv_date, "2023-04-15T00:00:00"),
        (_lv_date, "2023-04-15"),
        (_at_date_iso, "01.03.2010"),
        (_at_date_iso, "15.12.1999"),
    ])
    def test_valid_input_returns_iso_date(self, helper, valid_input):
        """All helpers return YYYY-MM-DD for recognised valid input."""
        result = helper(valid_input)
        assert result is not None
        assert _ISO_DATE_RE.match(result), (
            f"{helper.__name__}({valid_input!r}) → {result!r} "
            "does not match YYYY-MM-DD"
        )
