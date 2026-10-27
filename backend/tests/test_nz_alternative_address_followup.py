"""Role Search withholds an alternative address; the company's record supplies it.

From 18 November 2026 (sandbox since 27 Oct 2026), Companies Entity Role
Search returns ``"physicalAddress": {"addressLines": []}`` for a role holder
who has an alternative address on file — no ``pafId``, no postcode — and no
``addressType`` for anyone. MBIE's documented route to the address is the
company's own NZBN record (``associatedCompanyNzbn`` → ``GET /entities/{nzbn}``).
Stephen's decisions (27 Oct 2026): the full entity through ``nz_companies``'
fetch and cache; 25 companies per person, the rest counted as not checked; a
match where either side is ``ALTERNATIVE`` is at most medium.

The record shapes below are the sandbox's, read on 27 Oct 2026 for MBIE's test
director "Firstname Middle Name LASTNAME" on 9429050923540 and 9429050923557.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import nz_associations as nza
from opencheck.config import get_settings
from opencheck.nz_associations import (
    _address_withheld,
    _find_record_holder,
    follow_up_withheld,
    summarise_person,
    withheld_companies,
)

_SUBJECT = "3091118"

# The subject company's own filing of the director: an alternative address.
_RH_ALT = {
    "name": "Firstname Middle Name LASTNAME",
    "paf_id": "3161519",
    "address": "25 The Terrace, Wellington Central, Wellington, 6011",
    "address_type": "ALTERNATIVE",
    "roles_here": {"director"},
}
_RH_HOME = {
    "name": "John Doe",
    "paf_id": "156820",
    "address": "165 Victoria Road, Devonport, Auckland, 0624",
    "address_type": None,
    "roles_here": {"director"},
}

_WITHHELD = {"addressLines": []}


def _director(number: str, nzbn: str | None, name: str = "MANDATORY CO LIMITED",
              phys: dict[str, Any] | None = None, **person: str) -> dict[str, Any]:
    rec = {
        "firstName": person.get("first", "Firstname"),
        "middleName": person.get("middle", "Middle Name"),
        "lastName": person.get("last", "LASTNAME"),
        "roleType": "Director", "status": "active",
        "associatedCompanyName": name, "associatedCompanyNumber": number,
        "associatedCompanyStatusCode": "50",
        "physicalAddress": _WITHHELD if phys is None else phys,
    }
    if nzbn:
        rec["associatedCompanyNzbn"] = nzbn
    return rec


def _director_shareholder(number: str, nzbn: str) -> dict[str, Any]:
    """The sandbox's DirectorShareholder record: the company's NZBN is only on
    the shareholding, not at the top level."""
    return {
        "firstName": "Firstname", "middleName": "Middle Name", "lastName": "LASTNAME",
        "roleType": "DirectorShareholder", "status": "active",
        "physicalAddress": dict(_WITHHELD),
        "shareholdings": [{
            "associatedCompanyName": "MANDATORY ASYNCHRONOUS MODERATOR LIMITED",
            "associatedCompanyNumber": number, "associatedCompanyNzbn": nzbn,
            "associatedCompanyStatusCode": "50", "jointlyHeld": False, "numberOfShares": 50,
        }],
    }


def _company_bundle(name: str, *, paf: str | None, address: str | None,
                    address_type: str | None, holder: str = "Firstname Middle Name LASTNAME",
                    role: str = "roles") -> dict[str, Any]:
    row = {"name": holder, "search_name": "LASTNAME Firstname Middle Name",
           "paf_id": paf, "address": address, "address_type": address_type}
    return {
        "company": {"name": name}, "is_stub": False,
        "roles": [row] if role == "roles" else [],
        "shareholders": [row] if role == "shareholders" else [],
    }


@pytest.fixture
def adapter_fetch(monkeypatch):
    """Stub ``nz_companies.fetch``: NZBN → bundle, or an exception to raise."""
    answers: dict[str, Any] = {}
    calls: list[str] = []

    async def fake_fetch(number, legal_name=""):
        calls.append(number)
        answer = answers.get(number)
        if isinstance(answer, BaseException):
            raise answer
        if answer is None:
            raise AssertionError(f"unexpected NZBN read {number}")
        return answer

    monkeypatch.setattr(nza.REGISTRY["nz_companies"], "fetch", fake_fetch)
    return answers, calls


# --------------------------------------------------------------------------- #
# The withheld block
# --------------------------------------------------------------------------- #

def test_the_sandbox_block_is_withheld():
    assert _address_withheld({"addressLines": []})
    assert _address_withheld({"addressLines": ["", "  "]})
    assert _address_withheld({})
    # A postcode alone is not something to match on.
    assert _address_withheld({"addressLines": [], "postCode": "6011"})


def test_a_published_block_is_not_withheld():
    assert not _address_withheld({"postCode": "0624", "countryCode": "NZ", "pafId": "156820",
                                  "addressLines": ["165 Victoria Road", "Devonport", "Auckland"]})
    assert not _address_withheld({"pafId": "156820"})
    assert not _address_withheld({"addressLines": ["1 Queen St"]})
    assert not _address_withheld(None)


def test_a_withheld_record_is_never_compared_without_a_follow_up():
    """Without a follow-up the company is name-only and says why, even when the
    subject's own postcode would have overlapped the block's."""
    rec = _director("3091120", "9429050923557",
                    phys={"addressLines": [], "postCode": "6011"})
    p = summarise_person(_RH_ALT, [rec], _SUBJECT)
    c = p["companies"][0]
    assert c["confidence"] == "low"
    assert c["address_source"] == "not_checked"
    assert c["basis"] == "Same name — address not published by Role Search, not checked"
    assert p["address_withheld_count"] == 1 and p["withheld_not_checked"] == 1


def test_a_published_record_is_graded_as_before():
    rec = _director("9999", "9429000000999", phys={
        "pafId": "156820", "addressLines": ["165 Victoria Road", "Devonport", "Auckland"],
        "postCode": "0624"})
    p = summarise_person(_RH_HOME, [rec], _SUBJECT)
    c = p["companies"][0]
    assert (c["confidence"], c["basis"], c["address_source"]) == (
        "high", "Same registered address", "role_search")
    assert p["address_withheld_count"] == 0


# --------------------------------------------------------------------------- #
# Which companies are followed up
# --------------------------------------------------------------------------- #

def test_followups_read_the_nzbn_from_a_shareholding_and_skip_the_subject():
    records = [
        _director(_SUBJECT, "9429050923540"),                 # the subject itself
        _director_shareholder("3091120", "9429050923557"),    # NZBN only on the shareholding
        _director("3091120", "9429050923557"),                # same company again
        _director("4000", None),                              # no NZBN → cannot follow up
    ]
    assert [n for n, _ in withheld_companies(records, _SUBJECT)] == ["9429050923557"]


async def test_followup_cap_reads_at_most_25_and_counts_the_rest(adapter_fetch):
    answers, calls = adapter_fetch
    records = []
    for i in range(30):
        nzbn = f"94290000{i:05d}"
        records.append(_director(str(5000 + i), nzbn, name=f"CO {i} LIMITED"))
        answers[nzbn] = _company_bundle(f"CO {i} LIMITED", paf="1", address="1 Elsewhere St",
                                        address_type=None)
    fu = await follow_up_withheld(records, _SUBJECT)
    assert len(calls) == 25 and len(fu) == 25
    p = summarise_person(_RH_ALT, records, _SUBJECT, followups=fu)
    assert p["withheld_checked"] == 25
    assert p["withheld_not_checked"] == 5
    assert p["address_withheld_count"] == 30


# --------------------------------------------------------------------------- #
# Grading on the company's own record
# --------------------------------------------------------------------------- #

async def test_shared_alternative_address_is_medium_and_names_where_it_was_read(adapter_fetch):
    answers, _ = adapter_fetch
    answers["9429050923557"] = _company_bundle(
        "MANDATORY ASYNCHRONOUS MODERATOR LIMITED", paf="3161519",
        address="25 The Terrace, Wellington Central, Wellington, 6011",
        address_type="ALTERNATIVE")
    records = [_director("3091120", "9429050923557")]
    p = summarise_person(_RH_ALT, records, _SUBJECT,
                         followups=await follow_up_withheld(records, _SUBJECT))
    c = p["companies"][0]
    assert c["confidence"] == "medium"
    assert c["address_source"] == "company_record"
    assert c["basis"] == ("Same alternative address — may be a shared service address"
                          " (read from the company's own record)")
    assert p["address_match_count"] == 1 and p["high_confidence_count"] == 0


async def test_an_alternative_address_on_the_other_side_alone_also_caps_at_medium(adapter_fetch):
    """The subject filed a residential address here, the other company holds an
    alternative one that happens to share its pafId: still not high."""
    answers, _ = adapter_fetch
    answers["9429000000777"] = _company_bundle(
        "OTHER LIMITED", paf="156820", address="165 Victoria Road, Devonport, Auckland, 0624",
        address_type="ALTERNATIVE", holder="John Doe")
    records = [_director("777", "9429000000777", first="John", middle="", last="Doe")]
    p = summarise_person(_RH_HOME, records, _SUBJECT,
                         followups=await follow_up_withheld(records, _SUBJECT))
    assert p["companies"][0]["confidence"] == "medium"


async def test_a_residential_match_from_the_company_record_stays_high(adapter_fetch):
    answers, _ = adapter_fetch
    answers["9429000000777"] = _company_bundle(
        "OTHER LIMITED", paf="156820", address="165 Victoria Road, Devonport, Auckland, 0624",
        address_type=None, holder="John Doe")
    records = [_director("777", "9429000000777", first="John", middle="", last="Doe")]
    p = summarise_person(_RH_HOME, records, _SUBJECT,
                         followups=await follow_up_withheld(records, _SUBJECT))
    c = p["companies"][0]
    assert c["confidence"] == "high"
    assert c["basis"] == "Same registered address (read from the company's own record)"


async def test_a_holder_the_company_record_does_not_name_is_name_only(adapter_fetch):
    answers, _ = adapter_fetch
    answers["9429050923557"] = _company_bundle("X LIMITED", paf="1", address="1 St",
                                               address_type=None, holder="Somebody Else")
    records = [_director("3091120", "9429050923557")]
    p = summarise_person(_RH_ALT, records, _SUBJECT,
                         followups=await follow_up_withheld(records, _SUBJECT))
    c = p["companies"][0]
    assert (c["confidence"], c["basis"], c["address_source"]) == (
        "low", "Same name — may differ", "company_record")


def test_the_holder_is_found_on_first_and_last_name_when_middle_names_differ():
    holders = [{"name": "Firstname LASTNAME", "paf_id": "1"}]
    rec = _director("1", "9429000000001")
    assert _find_record_holder(holders, rec) is holders[0]
    assert _find_record_holder([{"name": "Other LASTNAME"}], rec) is None


async def test_a_shareholding_is_found_on_the_company_record_too(adapter_fetch):
    answers, _ = adapter_fetch
    answers["9429050923557"] = _company_bundle(
        "MANDATORY ASYNCHRONOUS MODERATOR LIMITED", paf="3161519", address="25 The Terrace",
        address_type="ALTERNATIVE", role="shareholders")
    records = [_director_shareholder("3091120", "9429050923557")]
    p = summarise_person(_RH_ALT, records, _SUBJECT,
                         followups=await follow_up_withheld(records, _SUBJECT))
    c = p["companies"][0]
    assert c["roles"] == ["shareholder"]
    assert c["address_source"] == "company_record" and c["confidence"] == "medium"


# --------------------------------------------------------------------------- #
# Failures are said, and never cached
# --------------------------------------------------------------------------- #

async def test_a_failed_company_read_is_said_as_such(adapter_fetch):
    from opencheck.sources.nz_companies import NzbnUnavailable

    answers, _ = adapter_fetch
    answers["9429050923557"] = NzbnUnavailable("NZBN entity read answered HTTP 503")
    records = [_director("3091120", "9429050923557")]
    p = summarise_person(_RH_ALT, records, _SUBJECT,
                         followups=await follow_up_withheld(records, _SUBJECT))
    c = p["companies"][0]
    assert (c["confidence"], c["address_source"]) == ("low", "check_failed")
    assert c["basis"] == "Same name — the company's own record could not be read"
    assert p["withheld_check_failed"] == 1


async def test_a_stub_company_bundle_counts_as_a_failed_read(adapter_fetch):
    answers, _ = adapter_fetch
    answers["9429050923557"] = {"company": None, "is_stub": True, "roles": [], "shareholders": []}
    fu = await follow_up_withheld([_director("3091120", "9429050923557")], _SUBJECT)
    assert fu == {"9429050923557": {"status": "failed"}}


class _MemCache:
    def __init__(self):
        self.store: dict[str, Any] = {}

    def get_payload(self, key, *a, **k):
        return (self.store[key], None) if key in self.store else None

    def put(self, key, value, *a, **k):
        self.store[key] = value


@pytest.fixture
def live_person(monkeypatch, adapter_fetch):
    monkeypatch.setenv("NZBN_ROLE_SEARCH_API_KEY", "k")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    get_settings.cache_clear()
    cache = _MemCache()
    monkeypatch.setattr(nza, "_cache", cache)
    answers, calls = adapter_fetch
    answers[_SUBJECT] = {
        "nz_company_number": _SUBJECT, "is_stub": False,
        "company": {"name": "MANDATORY CONTEXTUALLY-BASED SUCCESS LIMITED"},
        "roles": [{"name": _RH_ALT["name"], "search_name": "LASTNAME Firstname Middle Name",
                   "paf_id": _RH_ALT["paf_id"], "address": _RH_ALT["address"],
                   "address_type": "ALTERNATIVE"}],
        "shareholders": [],
    }

    async def fake_search(client, name, key):
        return [_director(_SUBJECT, "9429050923540"),
                _director_shareholder("3091120", "9429050923557")], 2

    monkeypatch.setattr(nza, "_role_search", fake_search)
    yield cache, answers, calls
    get_settings.cache_clear()


async def test_person_view_follows_up_and_caches_a_complete_answer(live_person):
    cache, answers, calls = live_person
    answers["9429050923557"] = _company_bundle(
        "MANDATORY ASYNCHRONOUS MODERATOR LIMITED", paf="3161519", address="25 The Terrace",
        address_type="ALTERNATIVE", role="shareholders")
    res = await nza.person_associations(_SUBJECT, _RH_ALT["name"])
    c = res["person"]["companies"][0]
    assert c["number"] == "3091120" and c["address_source"] == "company_record"
    assert calls == [_SUBJECT, "9429050923557"]
    assert any(k.startswith("nz_associations/person-v2/") for k in cache.store)


async def test_person_view_does_not_cache_an_answer_with_a_failed_read(live_person):
    from opencheck.sources.nz_companies import NzbnUnavailable

    cache, answers, _ = live_person
    answers["9429050923557"] = NzbnUnavailable("NZBN entity read did not answer")
    res = await nza.person_associations(_SUBJECT, _RH_ALT["name"])
    assert res["available"] is True
    assert res["person"]["withheld_check_failed"] == 1
    assert not any(k.startswith("nz_associations/person-v2/") for k in cache.store)


async def test_a_company_view_with_unchecked_addresses_does_not_answer_for_one_person(live_person):
    """The company-wide view makes no follow-ups, so a cached copy that left a
    withheld address unchecked must not stand in for the person view."""
    cache, answers, calls = live_person
    answers["9429050923557"] = _company_bundle("M LIMITED", paf="3161519", address="25 The Terrace",
                                               address_type="ALTERNATIVE", role="shareholders")
    cache.store[f"nz_associations/v2/{_SUBJECT}"] = {
        "company_number": _SUBJECT, "available": True, "subject_name": "M",
        "people": [{"name": _RH_ALT["name"], "other_company_count": 1,
                    "withheld_not_checked": 1, "companies": []}],
    }
    res = await nza.person_associations(_SUBJECT, _RH_ALT["name"])
    assert res["person"]["withheld_checked"] == 1
    assert "9429050923557" in calls


async def test_company_wide_view_makes_no_followups(monkeypatch, adapter_fetch):
    monkeypatch.setenv("NZBN_ROLE_SEARCH_API_KEY", "k")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "1")
    get_settings.cache_clear()
    monkeypatch.setattr(nza, "_cache", _MemCache())
    answers, calls = adapter_fetch
    answers[_SUBJECT] = {
        "nz_company_number": _SUBJECT, "is_stub": False, "company": {"name": "S"},
        "roles": [{"name": _RH_ALT["name"], "paf_id": "3161519", "address": "25 The Terrace",
                   "address_type": "ALTERNATIVE"}],
        "shareholders": [],
    }

    async def fake_search(client, name, key):
        return [_director("3091120", "9429050923557")], 1

    monkeypatch.setattr(nza, "_role_search", fake_search)
    res = await nza.assemble_associations(_SUBJECT)
    get_settings.cache_clear()
    assert calls == [_SUBJECT]
    assert res["people"][0]["withheld_not_checked"] == 1
