"""Phase A of the rigour adoption plan: shared identifier validation.

The suite-wide default (conftest.py) is checksum enforcement OFF, matching
the long-standing fixtures' fake LEIs. These tests flip the switch on
per-fixture — env var + ``get_settings.cache_clear()``, the same pattern as
the rate-limiter tests — to pin the enforced behaviour.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck import identifiers
from opencheck.app import app
from opencheck.config import get_settings

# A real LEI (Ørsted A/S's HORNSEA 1 LIMITED anchor from the demo corpus) and
# the same LEI with its final check digit bumped — shape-identical, mod-97
# invalid.
VALID_LEI = "2138002S3XGZ38WN5Q72"
BAD_CHECKSUM_LEI = "2138002S3XGZ38WN5Q73"
# Shape-valid fixture LEIs (used all over the suite) that fail mod-97.
FAKE_LEI = "2138000000000000A001"


@pytest.fixture
def enforced(monkeypatch):
    """Checksum enforcement ON for one test (suite default is off)."""
    monkeypatch.setenv("OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED", "1")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# --- LEI ---------------------------------------------------------------


def test_mod97_matches_rigour_when_available():
    """The pure-Python fallback and rigour must agree — dev == prod."""
    cases = [VALID_LEI, BAD_CHECKSUM_LEI, FAKE_LEI, "5493001KJTIIGC8Y1R12"]
    for lei in cases:
        pure = identifiers._mod97_ok(lei)
        if identifiers._HAS_RIGOUR:
            assert pure == identifiers._RIGOUR_LEI.is_valid(lei), lei
    assert identifiers._mod97_ok(VALID_LEI)
    assert not identifiers._mod97_ok(BAD_CHECKSUM_LEI)


def test_is_valid_lei_shape_only_when_not_enforced():
    assert identifiers.is_valid_lei(FAKE_LEI)  # suite default: checksum off
    assert not identifiers.is_valid_lei("LEI0000000000000ACME")  # strict shape
    assert not identifiers.is_valid_lei("too-short")
    assert identifiers.is_valid_lei(" 2138002s3xgz38wn5q72 ")  # normalised


def test_is_valid_lei_checksum_when_enforced(enforced):
    assert identifiers.is_valid_lei(VALID_LEI)
    assert not identifiers.is_valid_lei(BAD_CHECKSUM_LEI)
    assert not identifiers.is_valid_lei(FAKE_LEI)


def test_explicit_checksum_argument_overrides_settings():
    assert not identifiers.is_valid_lei(FAKE_LEI, checksum=True)
    assert identifiers.is_valid_lei(FAKE_LEI, checksum=False)


def test_canonical_lei():
    assert identifiers.canonical_lei(f"  {VALID_LEI.lower()} ") == VALID_LEI
    assert identifiers.canonical_lei("nope") is None


def test_lei_check_digit_error_off_by_default():
    assert identifiers.lei_check_digit_error(BAD_CHECKSUM_LEI) is None


def test_lei_check_digit_error_when_enforced(enforced):
    assert identifiers.lei_check_digit_error(VALID_LEI) is None
    msg = identifiers.lei_check_digit_error(BAD_CHECKSUM_LEI)
    assert msg is not None and "check digits" in msg
    # Permissive-shape values that can't be ISO 17442 (letters in the check
    # digit positions) also fail once enforcement is on.
    assert identifiers.lei_check_digit_error("STUB000000000000LEI0") is not None


# --- API boundary fast-fail ---------------------------------------------


def test_lookup_route_rejects_bad_check_digits(enforced):
    client = TestClient(app)
    resp = client.get("/lookup", params={"lei": BAD_CHECKSUM_LEI})
    assert resp.status_code == 400
    assert "check digits" in resp.json()["detail"]


def test_history_route_rejects_bad_check_digits(enforced):
    client = TestClient(app)
    resp = client.get("/history", params={"lei": BAD_CHECKSUM_LEI})
    assert resp.status_code == 400
    assert "check digits" in resp.json()["detail"]


def test_securities_route_rejects_bad_check_digits(enforced):
    client = TestClient(app)
    resp = client.get("/securities", params={"lei": BAD_CHECKSUM_LEI})
    assert resp.status_code == 400
    assert "check digits" in resp.json()["detail"]


def test_subsidiaries_route_rejects_bad_check_digits(enforced):
    client = TestClient(app)
    resp = client.get(
        "/subsidiaries", params={"lei": BAD_CHECKSUM_LEI, "format": "summary"}
    )
    assert resp.status_code == 400
    assert "check digits" in resp.json()["detail"]


def test_share_route_404s_bad_check_digits(enforced):
    client = TestClient(app)
    resp = client.get(f"/share/{BAD_CHECKSUM_LEI}")
    assert resp.status_code == 404


def test_shape_error_message_unchanged():
    """The historical shape message survives (clients/tests match on it)."""
    client = TestClient(app)
    resp = client.get("/lookup", params={"lei": "NOT-AN-LEI"})
    assert resp.status_code == 400
    assert "20-character alphanumeric" in resp.json()["detail"]


# --- ISIN ---------------------------------------------------------------


@pytest.mark.parametrize(
    "isin,ok",
    [
        ("US0378331005", True),
        ("GB0002374006", True),
        ("US0378331006", False),  # bad Luhn digit
        ("GB00CLEAN001", False),  # shape-valid fixture, bad Luhn
        ("BAD", False),  # malformed
    ],
)
def test_isin_checksum(isin, ok):
    assert identifiers.is_valid_isin(isin, checksum=True) is ok


def test_isin_luhn_matches_rigour_when_available():
    cases = ["US0378331005", "US0378331006", "GB0002374006", "GB00CLEAN001"]
    for isin in cases:
        if identifiers._HAS_RIGOUR:
            assert identifiers._isin_luhn_ok(isin) == identifiers._RIGOUR_ISIN.is_valid(
                isin
            ), isin


def test_isin_shape_only_when_not_enforced():
    assert identifiers.is_valid_isin("GB00CLEAN001")  # suite default: off
    assert not identifiers.is_valid_isin("not-an-isin")


# --- Wikidata QID -------------------------------------------------------


def test_qid_validation():
    assert identifiers.is_valid_qid("Q42")
    assert identifiers.is_valid_qid(" q9545 ")
    assert not identifiers.is_valid_qid("Q0")  # OpenCheck stub sentinel
    assert not identifiers.is_valid_qid("Q007")  # leading zero
    assert not identifiers.is_valid_qid("42")
    assert not identifiers.is_valid_qid(None)


# --- National registration numbers --------------------------------------


def test_national_checksum_gates_pass_when_not_enforced():
    assert identifiers.national_checksum_ok("fi_ytunnus", "0112038-8")
    assert identifiers.national_checksum_ok("se_orgnr", "5560078970")


@pytest.mark.skipif(
    not identifiers._HAS_STDNUM, reason="python-stdnum not installed (ftm extra)"
)
def test_national_checksum_gates_when_enforced(enforced):
    assert identifiers.national_checksum_ok("fi_ytunnus", "0112038-9")
    assert not identifiers.national_checksum_ok("fi_ytunnus", "0112038-8")
    assert identifiers.national_checksum_ok("se_orgnr", "5560160680")
    assert not identifiers.national_checksum_ok("se_orgnr", "5560078970")
    assert identifiers.national_checksum_ok("br_cnpj", "19131243000197")
    assert not identifiers.national_checksum_ok("br_cnpj", "00000167000101")
    # Unknown scheme: gate passes.
    assert identifiers.national_checksum_ok("xx_unknown", "12345")


@pytest.mark.skipif(
    not identifiers._HAS_STDNUM, reason="python-stdnum not installed (ftm extra)"
)
def test_national_id_checksum_warning(enforced):
    assert identifiers.national_id_checksum_warning("FI", "0112038-9") is None
    warn = identifiers.national_id_checksum_warning("FI", "0112038-8")
    assert warn is not None and "check digit" in warn
    # No validator mapped (KRS court numbers have no check digit): no warning.
    assert identifiers.national_id_checksum_warning("PL", "0000012345") is None


def test_national_id_warning_off_by_default():
    assert identifiers.national_id_checksum_warning("FI", "0112038-8") is None
