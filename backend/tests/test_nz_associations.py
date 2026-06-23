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
        # low — different person (different PAF + address) → weaker only
        _director_rec("333", "DELTA LTD", phys=_phys(paf="999", lines=["99 Other Rd"])),
        # the subject company itself → excluded
        _director_rec(_SUBJECT, "SUBJECT LTD", phys=_phys(paf="580631")),
    ]
    p = summarise_person(_RH, records, _SUBJECT)

    assert p["other_company_count"] == 3            # 111, 222, 444
    assert p["high_confidence_count"] == 2          # 111, 222
    assert p["as_director"] == 2                    # 111, 444
    assert p["as_shareholder"] == 1                # 222
    assert p["weaker_count"] == 1                  # 333 (name-only)

    nums = {c["number"] for c in p["companies"]}
    assert nums == {"111", "222", "444"}
    assert _SUBJECT not in nums
    gamma = next(c for c in p["companies"] if c["number"] == "444")
    assert gamma["confidence"] == "medium"
    assert gamma["basis"] == "Same address"


def test_ceased_directorship_is_skipped():
    records = [
        _director_rec("555", "OLD CO", phys=_phys(paf="580631"), resigned="2015-01-01"),
    ]
    p = summarise_person(_RH, records, _SUBJECT)
    assert p["other_company_count"] == 0


def test_no_subject_address_means_name_only_is_weak():
    rh = {"name": "Common Name", "paf_id": None, "address": None, "roles_here": {"director"}}
    records = [_director_rec("777", "SOME LTD", phys=_phys(paf="580631"))]
    p = summarise_person(rh, records, _SUBJECT)
    # Can't corroborate by address → not counted, surfaced as a weaker match.
    assert p["other_company_count"] == 0
    assert p["weaker_count"] == 1


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
