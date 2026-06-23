"""Tests for the NZ director/shareholder associations enrichment.

Focus is the address-tiered matching in ``summarise_person`` (the heart of the
nominee / mass-directorship red flag), plus endpoint gating. No network.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from opencheck.config import get_settings
from opencheck.nz_associations import assemble_associations, summarise_person
from opencheck.routers.nz_associations import nz_associations

_SUBJECT = "1166320"

_RH = {
    "name": "John Doe",
    "paf_id": "580631",
    "address": "1 Queen St, Auckland, 1010",
    "roles_here": {"director"},
}


def _phys(paf=None, lines=None, postcode=None):
    a = {}
    if paf is not None:
        a["pafId"] = paf
    if lines is not None:
        a["addressLines"] = lines
    if postcode is not None:
        a["postCode"] = postcode
    return a


def _director_rec(company_number, name, *, phys, nzbn=None, resigned=None):
    rec = {
        "roleType": "Director", "status": "active",
        "associatedCompanyNumber": company_number, "associatedCompanyName": name,
        "physicalAddress": phys,
    }
    if nzbn:
        rec["associatedCompanyNzbn"] = nzbn
    if resigned:
        rec["resignationDate"] = resigned
    return rec


def _shareholder_rec(company_number, name, pct, *, phys, nzbn=None):
    return {
        "roleType": "IndividualShareholder", "status": "active",
        "physicalAddress": phys,
        "shareholdings": [{
            "associatedCompanyNumber": company_number, "associatedCompanyName": name,
            "associatedCompanyNzbn": nzbn, "sharePercentage": pct,
        }],
    }


def test_tiering_dedup_split_and_subject_exclusion():
    records = [
        # high — exact PAF match, director of ALPHA
        _director_rec("111", "ALPHA LTD", phys=_phys(paf="580631"), nzbn="9429000000111"),
        # high — exact PAF match, shareholder of BETA
        _shareholder_rec("222", "BETA LTD", 50, phys=_phys(paf="580631"), nzbn="9429000000222"),
        # medium — same address lines, no PAF, director of GAMMA
        _director_rec("444", "GAMMA LTD",
                      phys=_phys(lines=["1 Queen St", "Auckland"], postcode="1010")),
        # low — different PAF + address → name-only, but still counted
        _director_rec("333", "DELTA LTD", phys=_phys(paf="999", lines=["99 Other Rd"])),
        # the subject company itself → excluded
        _director_rec(_SUBJECT, "SUBJECT LTD", phys=_phys(paf="580631")),
    ]
    p = summarise_person(_RH, records, _SUBJECT)

    # Every name match counts now (incl. the name-only DELTA), minus the subject.
    assert p["other_company_count"] == 4            # 111, 222, 444, 333
    assert p["high_confidence_count"] == 2          # 111, 222
    assert p["address_match_count"] == 3            # 111, 222 (high) + 444 (medium)
    assert p["name_only_count"] == 1               # 333
    assert p["as_director"] == 3                    # 111, 444, 333
    assert p["as_shareholder"] == 1                # 222

    nums = {c["number"] for c in p["companies"]}
    assert nums == {"111", "222", "444", "333"}
    assert _SUBJECT not in nums
    gamma = next(c for c in p["companies"] if c["number"] == "444")
    assert gamma["confidence"] == "medium"
    assert gamma["basis"] == "Overlapping address"
    delta = next(c for c in p["companies"] if c["number"] == "333")
    assert delta["confidence"] == "low"
    # Address-matched companies sort ahead of name-only.
    assert p["companies"][-1]["number"] == "333"


def test_ceased_directorship_is_skipped():
    records = [
        _director_rec("555", "OLD CO", phys=_phys(paf="580631"), resigned="2015-01-01"),
    ]
    p = summarise_person(_RH, records, _SUBJECT)
    assert p["other_company_count"] == 0


def test_no_subject_address_means_name_only_is_counted():
    rh = {"name": "Common Name", "paf_id": None, "address": None, "roles_here": {"director"}}
    records = [_director_rec("777", "SOME LTD", phys=_phys(paf="580631"))]
    p = summarise_person(rh, records, _SUBJECT)
    # No subject address to corroborate against → counts as a name-only match
    # (shown, clearly labelled) rather than being dropped.
    assert p["other_company_count"] == 1
    assert p["name_only_count"] == 1
    assert p["address_match_count"] == 0
    assert p["companies"][0]["confidence"] == "low"


def test_prolific_name_surfaces_total_records():
    records = [_director_rec("111", "ALPHA LTD", phys=_phys(paf="580631"))]
    p = summarise_person(_RH, records, _SUBJECT, total_records=200)
    assert p["total_records_under_name"] == 200
    assert p["truncated"] is True  # 200 records exist, only 1 fetched/tiered here


def test_career_director_with_differing_addresses_surfaces_as_name_only():
    # Regression: a career director files a different address on each board, so
    # nothing corroborates by PAF/address. Previously every match was hidden
    # ("weaker, not counted") leaving the panel empty; now they surface as
    # name-only matches that are clearly labelled.
    records = [
        _director_rec("201", "BOARD ONE LTD", phys=_phys(paf="111", lines=["1 First St"])),
        _director_rec("202", "BOARD TWO LTD", phys=_phys(paf="222", lines=["2 Second St"])),
        _shareholder_rec("203", "BOARD THREE LTD", 10, phys=_phys(paf="333")),
    ]
    p = summarise_person(_RH, records, _SUBJECT)  # _RH has paf 580631 / Queen St
    assert p["other_company_count"] == 3
    assert p["address_match_count"] == 0
    assert p["name_only_count"] == 3
    assert all(c["confidence"] == "low" for c in p["companies"])


def test_company_role_merges_director_and_shareholder():
    records = [
        _director_rec("888", "DUAL LTD", phys=_phys(paf="580631")),
        _shareholder_rec("888", "DUAL LTD", 100, phys=_phys(paf="580631")),
    ]
    p = summarise_person(_RH, records, _SUBJECT)
    assert p["other_company_count"] == 1
    dual = p["companies"][0]
    assert set(dual["roles"]) == {"director", "shareholder"}


# ---------------------------------------------------------------------------
# Gating + endpoint
# ---------------------------------------------------------------------------

async def test_assemble_unavailable_without_key(monkeypatch):
    monkeypatch.delenv("NZBN_ROLE_SEARCH_API_KEY", raising=False)
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    res = await assemble_associations("1166320")
    get_settings.cache_clear()
    assert res["available"] is False
    assert res["people"] == []


async def test_endpoint_rejects_bad_number():
    with pytest.raises(HTTPException) as exc:
        await nz_associations(company_number="not-a-number")
    assert exc.value.status_code == 400


async def test_endpoint_accepts_13_digit_nzbn(monkeypatch):
    # Some NZ entities carry the 13-digit NZBN in GLEIF registeredAs (e.g. ADT
    # Security 9429040916057) — must not 400.
    monkeypatch.delenv("NZBN_ROLE_SEARCH_API_KEY", raising=False)
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    res = await nz_associations(company_number="9429040916057")
    get_settings.cache_clear()
    assert res["company_number"] == "9429040916057"  # accepted, not rejected
