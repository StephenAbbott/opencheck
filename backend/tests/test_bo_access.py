"""Tests for the EU/EEA beneficial-ownership access notices."""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bo_access import (
    BO_ACCESS,
    EU_EEA_COUNTRY_NAMES,
    notice_for,
)


def test_data_file_loads_and_is_all_eu_eea() -> None:
    assert BO_ACCESS, "expected at least one country entry"
    for code in BO_ACCESS:
        assert code in EU_EEA_COUNTRY_NAMES, f"{code} is not an EU/EEA country"
        assert code == code.upper()


def test_past_date_is_restricted() -> None:
    n = notice_for("FR", date(2026, 7, 1))
    assert n is not None
    assert n.status == "restricted"
    assert n.effective_date is None
    assert n.country_name == "France"
    assert n.access_url and n.access_url.startswith("https://")


def test_future_date_is_becoming_restricted_with_date() -> None:
    n = notice_for("EE", date(2026, 7, 1))
    assert n is not None
    assert n.status == "becoming_restricted"
    assert n.effective_date == "2026-07-10"


def test_effective_date_boundary_flips_to_restricted_on_the_day() -> None:
    # On the restriction date itself the message is already "restricted".
    on_day = notice_for("EE", date(2026, 7, 10))
    assert on_day is not None and on_day.status == "restricted"
    day_before = notice_for("EE", date(2026, 7, 9))
    assert day_before is not None and day_before.status == "becoming_restricted"


def test_null_date_is_restricted() -> None:
    n = notice_for("FI", date(2026, 7, 1))
    assert n is not None and n.status == "restricted"
    assert n.effective_date is None


def test_missing_access_url_is_none_not_error() -> None:
    # Slovakia has no announced link — the notice still renders, without a link.
    n = notice_for("SK", date(2026, 7, 1))
    assert n is not None and n.status == "restricted"
    assert n.access_url is None


def test_country_without_entry_returns_none() -> None:
    # GB is a national register but not in the EU BO-access list.
    assert notice_for("GB") is None
    # Latvia is deliberately omitted (keeping its register public).
    assert notice_for("LV") is None
    assert notice_for(None) is None
    assert notice_for("") is None


def test_case_insensitive_lookup() -> None:
    assert notice_for("fr", date(2026, 7, 1)) == notice_for("FR", date(2026, 7, 1))


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_sources_endpoint_exposes_bo_access(client: TestClient) -> None:
    data = client.get("/sources").json()
    by_id = {s["id"]: s for s in data["sources"]}

    # A national register in the list carries its country + a computed notice.
    inpi = by_id["inpi"]
    assert inpi["country"] == "FR"
    assert inpi["bo_access"] is not None
    assert inpi["bo_access"]["status"] in {"restricted", "becoming_restricted"}
    assert inpi["bo_access"]["country_name"] == "France"

    # A national register not in the EU BO-access list has no notice.
    ch = by_id["companies_house"]
    assert ch["country"] == "GB"
    assert ch["bo_access"] is None
