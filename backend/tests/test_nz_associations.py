"""Tests for the NZ director/shareholder associations enrichment.

Focus is the address-tiered matching in ``summarise_person`` (the heart of the
nominee / mass-directorship red flag), plus endpoint gating. No network.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from opencheck.config import get_settings
from opencheck.nz_associations import (
    _collect_role_holders,
    _extract_records,
    _role_search,
    _to_pct,
    assemble_associations,
    summarise_person,
)
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
# Role Search request shaping — the required role-type param + recommended name
# order. These were the cause of zero associations: role-type is mandatory and
# was missing, and first-name-first names match less of the register.
# ---------------------------------------------------------------------------


class _CaptureResponse:
    is_success = True
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _CaptureClient:
    """Async client stand-in that records the URLs it is asked to GET."""

    def __init__(self):
        self.urls: list[str] = []

    async def get(self, url, headers=None):  # noqa: ANN001
        self.urls.append(url)
        return _CaptureResponse({"totalResults": 0, "roles": []})


async def test_role_search_sends_required_role_type_and_name():
    client = _CaptureClient()
    await _role_search(client, "KRAMER Holly Suzanna", key="k")
    assert client.urls, "expected at least one request"
    first = client.urls[0]
    # role-type is required by the API; omitting it returned nothing.
    assert "role-type=ALL" in first
    assert "registered-only=true" in first
    # Name is URL-encoded into the query.
    assert "KRAMER" in first and "Holly" in first


def test_collect_role_holders_uses_surname_first_search_name():
    bundle = {
        "roles": [
            {"kind": "person", "name": "Holly Suzanna Kramer",
             "search_name": "Kramer Holly Suzanna", "paf_id": "1", "address": "x"},
        ],
        "shareholders": [
            # Organisation shareholder — no search_name → falls back to display name.
            {"kind": "entity", "name": "BIG HOLDINGS LTD", "paf_id": None, "address": None},
        ],
    }
    holders = _collect_role_holders(bundle)
    person = next(h for h in holders if h["name"] == "Holly Suzanna Kramer")
    assert person["search_name"] == "Kramer Holly Suzanna"
    org = next(h for h in holders if h["name"] == "BIG HOLDINGS LTD")
    assert org["search_name"] == "BIG HOLDINGS LTD"  # fallback


# ---------------------------------------------------------------------------
# Response-shape robustness — the live API shape isn't pinned by a schema, so
# parsing must be tolerant and must never raise (panel-only → no 500s).
# ---------------------------------------------------------------------------


def test_extract_records_tolerates_alternate_keys_and_fallback():
    assert _extract_records({"roles": [{"a": 1}]}) == [{"a": 1}]
    # Alternate array key the API might use instead of "roles".
    assert _extract_records({"items": [{"b": 2}]}) == [{"b": 2}]
    # Unknown key → fall back to the first list-of-dicts value.
    assert _extract_records({"weird": [{"c": 3}]}) == [{"c": 3}]
    # Nothing usable → empty, not an error.
    assert _extract_records({"totalResults": 0}) == []
    assert _extract_records("not a dict") == []


def test_to_pct_coerces_any_shape():
    assert _to_pct(50) == 50.0
    assert _to_pct(50.5) == 50.5
    assert _to_pct("50") == 50.0
    assert _to_pct("50.0") == 50.0
    assert _to_pct("50%") == 50.0
    assert _to_pct(None) is None
    assert _to_pct("n/a") is None
    assert _to_pct(True) is None  # don't treat bool as a number


def test_summarise_person_never_raises_on_malformed_records():
    # physicalAddress as a list, shareholdings as a dict, percentage as junk —
    # exactly the kind of unexpected shape that previously 500'd.
    records = [
        {"roleType": "Director", "associatedCompanyNumber": "900",
         "physicalAddress": ["unexpected", "list"]},
        {"roleType": "Shareholder", "shareholdings": {"not": "a list"}},
        {"roleType": "Shareholder", "shareholdings": [
            {"associatedCompanyNumber": "901", "sharePercentage": "33%"}]},
        "a bare string that is not a dict",  # type: ignore[list-item]
    ]
    p = summarise_person(_RH, [r for r in records if isinstance(r, dict)], _SUBJECT)
    # 900 (director) + 901 (shareholder) survive; the malformed ones are skipped.
    nums = {c["number"] for c in p["companies"]}
    assert nums == {"900", "901"}
    sh = next(c for c in p["companies"] if c["number"] == "901")
    assert sh["share_percentage"] == 33.0  # "33%" coerced


def test_official_swagger_example_record_parses():
    # The exact RoleInEntity example from the v3 OpenAPI definition, with numeric
    # values returned as bare integers (as the live API does) even though the
    # schema types them as strings — this is what previously 500'd. Director +
    # shareholder of the SAME company → one merged company with both roles.
    rec = {
        "middleName": "Martin", "lastName": "SMITH", "firstName": "John",
        "appointmentDate": "2013-04-03",
        "associatedCompanyNumber": 1884264,            # int, not string
        "associatedCompanyNzbn": 123456789012,
        "associatedCompanyStatusCode": 80,
        "status": "active",                             # active (no resignation)
        "associatedCompanyName": "TIMARU BUS SERVICES LIMITED",
        "roleType": "Director",
        "physicalAddress": {
            "addressLines": ["1 Queen St"], "postCode": 1025,
            "countryCode": "NZ", "pafId": 580631,       # int, matches _RH paf
        },
        "shareholdings": [{
            "associatedCompanyNumber": 1884264, "associatedCompanyNzbn": 123456789012,
            "jointlyHeld": False, "sharePercentage": 100,
            "associatedCompanyName": "TIMARU BUS SERVICES LIMITED",
            "associatedCompanyStatusCode": 80, "numberOfShares": 150,
        }],
    }
    p = summarise_person(_RH, [rec], subject_number="999")
    assert p["other_company_count"] == 1
    co = p["companies"][0]
    assert co["number"] == "1884264"
    assert set(co["roles"]) == {"director", "shareholder"}
    assert co["share_percentage"] == 100.0
    assert co["confidence"] == "high"   # pafId 580631 matches the subject holder


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
