"""Tests for ``opencheck.findings`` — the one-sentence source findings.

Every per-adapter test asserts the **exact** string a template produces for
that adapter's existing fixture, plus the degraded string it produces when the
fields the sentence wants are absent (rule 10). Fixtures are imported from the
adapter's own test module wherever one already exists, so a change to the
payload shape breaks both tests together rather than leaving this file
asserting against a shape nothing else believes in.
"""

from __future__ import annotations

import re

import pytest

from opencheck.findings import (
    MAX_FINDING_CHARS,
    clauses_to_sentence,
    finding_bods_gleif,
    finding_climatetrace,
    finding_companies_house,
    finding_gleif,
    finding_openaleph,
    finding_opencorporates,
    finding_opensanctions,
    finding_ted_eu,
    finding_wikidata,
    holding_clause,
    human_date,
    percent,
    plural,
    psc_nature_phrase,
)
from opencheck.sources import SearchKind, SourceHit
from opencheck.sources.bods_gleif import (
    _build_entity_statement,
    _build_relationship_statement,
)
from tests.test_bods_gleif_ftm import _child_record as gleif_child_record
from tests.test_bods_gleif_ftm import (
    _gleif_bundle_with_direct_parent as gleif_direct_parent_bundle,
)
from tests.test_bods_mapper import _sample_bundle as ch_sample_bundle
from tests.test_bods_opencorporates import _minimal_bundle as oc_minimal_bundle
from tests.test_bods_opencorporates import _officer as oc_officer
from tests.test_bods_wikidata import _entity_bundle as wd_entity_bundle
from tests.test_bods_wikidata import _entity_with_parent_bundle as wd_parent_bundle
from tests.test_bods_wikidata import _person_bundle as wd_person_bundle
from tests.test_openaleph_live import _ERICSSON_ENTITY
from tests.test_ted_eu import _won_bundle as ted_won_bundle

# ---------------------------------------------------------------------------
# Fixtures that have no home in an adapter test module
# ---------------------------------------------------------------------------

# The OpenSanctions search item from test_opensanctions_live's
# ``test_entity_search_maps_results`` — that module builds it inline inside an
# httpx_mock response, so it cannot be imported.
_OS_ROSNEFT = {
    "id": "NK-rosneft",
    "schema": "Company",
    "caption": "Rosneft Oil Company",
    "properties": {
        "leiCode": ["253400VC22A0KFSOPB29"],
        "wikidataId": ["Q219617"],
    },
    "datasets": ["eu_fsf", "us_ofac_sdn"],
    "topics": ["sanction"],
}

# The Companies House PSC-statement payload from test_bods_mapper's
# ``test_psc_statements_map_to_unspecified_ooc`` (inline there too): a company
# with no PSCs that has filed "no registrable person exists".
_CH_STATEMENTS_ONLY = {
    "company_number": "00088888",
    "profile": {"company_name": "Gap Co Ltd", "company_number": "00088888"},
    "officers": {"items": []},
    "pscs": {"items": []},
    "psc_statements": {
        "items": [
            {
                "statement": "no-individual-or-entity-with-signficant-control",
                "notified_on": "2016-04-06",
                "etag": "s1",
            }
        ]
    },
}

# The /mentions payload from test_openaleph_live's
# ``test_fetch_mentions_parses_documents``, reduced to the two keys the
# template reads.
_OA_MENTIONS = {
    "total": 33,
    "collections": [
        {"label": "AskTheEU FOI documents", "count": 20},
        {"label": "Leak X", "count": 13},
    ],
}

# bods_gleif has no offline bundle fixture anywhere in the suite (its only
# fetch tests are the stub path and the env-gated live Parquet path), so this
# one is reconstructed with the adapter's OWN statement builders rather than
# hand-written — the shape cannot drift from what ``_parquet_fetch`` returns.
_GLEIF_STATEMENT_ID = "oo-gleif-ent-1"


def _gleif_payload(*, parents: int = 1, children: int = 2, jurisdiction: str = "Sweden") -> dict:
    entity_row = {
        "statementid": _GLEIF_STATEMENT_ID,
        "recorddetails_name": "Ericsson AB",
        "recorddetails_entitytype_type": "registeredEntity",
        "recorddetails_jurisdiction_name": jurisdiction,
        "recorddetails_jurisdiction_code": "SE",
        "statementdate": "2026-01-05",
    }
    statements = [_build_entity_statement(entity_row, [], [])]
    for i in range(parents):
        statements.append(
            _build_relationship_statement(
                (f"oo-gleif-rel-p{i}", _GLEIF_STATEMENT_ID, f"oo-gleif-parent-{i}", "2026-01-05"),
                [{"directOrIndirect": "direct", "type": "otherInfluenceOrControl"}],
            )
        )
    for i in range(children):
        statements.append(
            _build_relationship_statement(
                (f"oo-gleif-rel-c{i}", f"oo-gleif-child-{i}", _GLEIF_STATEMENT_ID, "2026-01-05"),
                [{"directOrIndirect": "direct"}],
            )
        )
    return {"source_id": "bods_gleif", "hit_id": _GLEIF_STATEMENT_ID, "bods_statements": statements}


# The live GLEIF bundle. ``_gleif_bundle_with_direct_parent`` from
# test_bods_gleif_ftm is the BP record with a direct parent; the keys the
# adapter always emits but that fixture omits are defaulted here, and each
# case overrides only what it is about.
_GLEIF_ULTIMATE_PARENT = {
    "id": "ULTIMATEXXXXXXXXXXXX",
    "attributes": {
        "lei": "ULTIMATEXXXXXXXXXXXX",
        "entity": {"legalName": {"name": "BP p.l.c."}, "jurisdiction": "GB"},
    },
}


def _gleif_bundle(**overrides: object) -> dict:
    bundle = gleif_direct_parent_bundle()
    bundle.setdefault("direct_parent_exception", None)
    bundle.setdefault("ultimate_parent_exception", None)
    bundle.setdefault("direct_children", [])
    bundle.setdefault("direct_children_total", 0)
    bundle.update(overrides)
    return bundle


def _gleif_parent(lei: str, name: str) -> dict:
    """A parent endpoint's response body — the parent's **own Level 1 record**,
    which is why the parent is nameable at all. Confirmed live 2026-08-22: the
    `attributes` object carries `lei`, `entity`, `registration`, `bic`, `mic`,
    `ocid`, `gem`, `qcc`, `spglobal`, `conformityFlag`."""
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {"lei": lei, "entity": {"legalName": {"name": name}, "jurisdiction": "US"}},
    }


def _gleif_exception(reason: str, *, ultimate: bool = False) -> dict:
    """A GLEIF reporting-exception payload in the **dump** spelling.

    Open Ownership's SQLite dump writes ``exceptionReason``; the live API
    writes ``reason`` — see ``_gleif_exception_live`` below, and the same split
    at ``mapper.py``'s own reader. Both spellings must render identically, so
    both are fixtured.
    """
    category = (
        "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT"
        if ultimate
        else "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT"
    )
    return {"attributes": {"exceptionCategory": category, "exceptionReason": reason}}


def _gleif_exception_live(reason: str, *, ultimate: bool = False) -> dict:
    """The exception payload **exactly as the live API returned it** for BP
    (`213800LH1BZH3DI6G760`), Rosneft and SEB, captured 2026-08-22.

    Note ``reason`` rather than ``exceptionReason``, the null-valued
    ``validFrom`` / ``validTo`` / ``reference`` keys, and the JSON:API
    ``id`` / ``type`` / ``relationships`` siblings of ``attributes`` — the
    reader must walk past all of them.
    """
    category = (
        "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT"
        if ultimate
        else "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT"
    )
    return {
        "type": "reporting-exceptions",
        "id": f"213800LH1BZH3DI6G760-{category}",
        "relationships": {"lei-record": {"links": {}}},
        "attributes": {
            "validFrom": None,
            "validTo": None,
            "lei": "213800LH1BZH3DI6G760",
            "category": category,
            "reason": reason,
            "reference": None,
        },
    }


def _oc_bundle() -> dict:
    """The OpenCorporates minimal bundle with the fields a live payload adds."""
    bundle = oc_minimal_bundle(
        officers=[
            oc_officer("Jane Doe", "director"),
            oc_officer("Bo Li", "secretary", officer_id="124"),
        ]
    )
    bundle["company"]["current_status"] = "Active"
    bundle["company"]["company_type"] = "Public Limited Company"
    return bundle


# ---------------------------------------------------------------------------
# clauses_to_sentence — the degradation contract every template inherits
# ---------------------------------------------------------------------------


def test_missing_clauses_shorten_the_sentence_instead_of_showing_none() -> None:
    assert clauses_to_sentence(["6 officers on file", None, ""]) == "6 officers on file."


def test_a_sentence_with_nothing_left_to_say_is_none() -> None:
    """None, not an empty string: the hit then renders its summary as before."""
    assert clauses_to_sentence([None, "", "   "]) is None
    assert clauses_to_sentence([]) is None


def test_the_first_letter_is_capitalised_and_a_full_stop_is_added() -> None:
    assert clauses_to_sentence(["active since 2000"]) == "Active since 2000."


def test_an_existing_terminator_is_not_doubled() -> None:
    assert clauses_to_sentence(["who owns this?"]) == "Who owns this?"


def test_a_leading_digit_survives_capitalisation() -> None:
    assert clauses_to_sentence(["33 documents mention this name"]).startswith("33 ")


def test_clauses_join_with_the_separator_the_template_chose() -> None:
    assert clauses_to_sentence(["a", "b"], sep="; ") == "A; b."


def test_trailing_clauses_are_dropped_to_stay_under_the_hard_cap() -> None:
    """Rule 1's cap is enforced centrally, which is why templates order their
    clauses most-important-first — the tail is what goes."""
    sentence = clauses_to_sentence(["x" * 100, "y" * 30, "z" * 30])
    assert sentence is not None
    assert len(sentence) <= MAX_FINDING_CHARS
    assert "y" * 30 in sentence
    assert "z" * 30 not in sentence


def test_a_lead_clause_longer_than_the_cap_is_kept_whole() -> None:
    """Truncating mid-word would be a slot marker by another name (rule 8)."""
    sentence = clauses_to_sentence(["x" * 200])
    assert sentence == "X" + "x" * 199 + "."


# ---------------------------------------------------------------------------
# House-style helpers (rule 9)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2026-06-03", "3 June 2026"),
        ("1909-04-14", "14 April 1909"),
        ("1952-10-07T00:00:00Z", "7 October 1952"),
    ],
)
def test_dates_read_as_house_style(value: str, expected: str) -> None:
    assert human_date(value) == expected


@pytest.mark.parametrize("value", [None, "", "not a date", "2026-13-01"])
def test_an_unparseable_date_drops_its_clause(value: str | None) -> None:
    assert human_date(value) is None


def test_plurals_are_handled() -> None:
    assert plural(1, "subsidiary", "subsidiaries") == "1 subsidiary"
    assert plural(2, "subsidiary", "subsidiaries") == "2 subsidiaries"
    assert plural(0, "officer") == "0 officers"


def test_numbers_are_rendered_exactly_as_filed() -> None:
    """40.4% stays 40.4% — no rounding to a friendlier number (rule 5)."""
    assert percent(40.4) == "40.4%"
    assert percent(19.75) == "19.75%"
    assert percent(50.0) == "50%"
    assert percent(None) is None
    assert percent("nope") is None


# ---------------------------------------------------------------------------
# Rule 5 — a sub-50% stake never gets a control word
# ---------------------------------------------------------------------------

_CONTROL_WORDS = ("majority", "controls", "controlling", "outright", "wholly")


def test_a_minority_stake_is_never_described_as_control() -> None:
    clause = holding_clause("Rosneftegaz JSC", 40.4)
    assert clause == "Rosneftegaz JSC holds 40.4%"
    assert not any(word in clause.lower() for word in _CONTROL_WORDS)


@pytest.mark.parametrize("share", [0.1, 25, 33.3333, 49.9])
def test_no_sub_50_percentage_reaches_a_control_word(share: float) -> None:
    clause = holding_clause("Acme Holdings", share)
    assert clause is not None
    assert not any(word in clause.lower() for word in _CONTROL_WORDS)


def test_a_stake_of_50_or_more_may_say_majority() -> None:
    assert holding_clause("Acme Holdings", 50) == "Acme Holdings holds a majority stake of 50%"


def test_a_holder_with_no_published_share_is_an_owner_not_a_controller() -> None:
    clause = holding_clause("Acme Holdings", None)
    assert clause == "Acme Holdings is recorded as an owner"
    assert not any(word in clause.lower() for word in _CONTROL_WORDS)


def test_a_sub_50_psc_band_is_stated_as_a_band_not_as_control() -> None:
    """Companies House bands its natures of control; the band is repeated as
    filed and never promoted to a midpoint or to a control word."""
    clause = psc_nature_phrase(["ownership-of-shares-25-to-50-percent"])
    assert clause == "25% to 50% of shares"
    assert not any(word in clause.lower() for word in _CONTROL_WORDS)


def test_non_numeric_psc_natures_are_rendered_as_prose() -> None:
    assert psc_nature_phrase(["significant-influence-or-control"]) == (
        "significant influence or control"
    )
    assert psc_nature_phrase(["voting-rights-75-to-100-percent-as-trust"]) == (
        "75% to 100% of voting rights"
    )
    assert psc_nature_phrase(["something-companies-house-invented-later"]) is None
    assert psc_nature_phrase(None) is None


# ---------------------------------------------------------------------------
# Per-adapter templates
# ---------------------------------------------------------------------------


def test_gleif_finding_names_the_consolidating_parent() -> None:
    assert finding_gleif(_gleif_bundle()) == "Consolidated by BP Group Holdings."


def test_gleif_finding_counts_subsidiaries_that_report_to_it() -> None:
    """``direct_children_total`` is GLEIF's own count of entities naming this
    one as their consolidating parent — a reporting relationship, not assets
    it owns."""
    assert finding_gleif(_gleif_bundle(direct_children_total=12)) == (
        "Consolidated by BP Group Holdings; 12 direct subsidiaries report to it."
    )


def test_gleif_finding_agrees_in_number_for_one_subsidiary() -> None:
    assert finding_gleif(_gleif_bundle(direct_children_total=1)) == (
        "Consolidated by BP Group Holdings; 1 direct subsidiary reports to it."
    )


def test_gleif_finding_separates_the_direct_and_ultimate_parent() -> None:
    assert finding_gleif(_gleif_bundle(ultimate_parent=_GLEIF_ULTIMATE_PARENT)) == (
        "Consolidated by BP Group Holdings; ultimately by BP p.l.c."
    )


def test_gleif_finding_says_one_parent_once_when_it_fills_both_levels() -> None:
    """The same LEI filed as direct and ultimate parent must not be named
    twice as if two entities were involved."""
    bundle = _gleif_bundle()
    bundle["ultimate_parent"] = gleif_direct_parent_bundle()["direct_parent"]
    assert finding_gleif(bundle) == (
        "Consolidated by BP Group Holdings, its direct and ultimate parent."
    )


def test_gleif_finding_handles_an_ultimate_parent_with_no_direct_one() -> None:
    bundle = _gleif_bundle(direct_parent=None, ultimate_parent=_GLEIF_ULTIMATE_PARENT)
    assert finding_gleif(bundle) == "Ultimately consolidated by BP p.l.c."


def test_gleif_finding_reads_a_reporting_exception_as_a_permitted_filing() -> None:
    """A reporting exception is a permitted reason defined by the LEI ROC
    policy — not a failure to disclose — and must read that way (rule 6/7)."""
    bundle = _gleif_bundle(
        direct_parent=None,
        direct_parent_exception=_gleif_exception("NATURAL_PERSONS"),
    )
    assert finding_gleif(bundle) == (
        "No consolidating parent is reported: control rests with natural "
        "persons, a permitted exception."
    )


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("NO_LEI", "the parent has no LEI"),
        ("NON_CONSOLIDATING", "the parent prepares no consolidated accounts"),
        ("NON_PUBLIC", "the relationship is not public"),
        # Deprecated since Reporting Exceptions Format 2.1; records still
        # carrying the old code mean what NON_PUBLIC means.
        ("CONSENT_NOT_OBTAINED", "the relationship is not public"),
    ],
)
def test_gleif_finding_words_each_exception_reason(reason: str, expected: str) -> None:
    bundle = _gleif_bundle(
        direct_parent=None, direct_parent_exception=_gleif_exception(reason)
    )
    finding = finding_gleif(bundle)
    assert finding is not None
    assert expected in finding
    assert "a permitted exception" in finding


def test_gleif_finding_collapses_the_same_exception_at_both_levels() -> None:
    bundle = _gleif_bundle(
        direct_parent=None,
        direct_parent_exception=_gleif_exception("NATURAL_PERSONS"),
        ultimate_parent_exception=_gleif_exception("NATURAL_PERSONS", ultimate=True),
    )
    assert finding_gleif(bundle) == (
        "No consolidating parent is reported at either level: control rests "
        "with natural persons, a permitted exception."
    )


def test_gleif_finding_keeps_an_unknown_exception_code_as_a_permitted_filing() -> None:
    """GLEIF may add a reason code we have no wording for; the filing is still
    permitted, so say that much rather than guessing at its meaning."""
    bundle = _gleif_bundle(
        direct_parent=None, direct_parent_exception=_gleif_exception("SOMETHING_NEW")
    )
    assert finding_gleif(bundle) == (
        "No consolidating parent is reported; a permitted exception is filed instead."
    )


def test_gleif_finding_states_neither_parent_nor_exception_in_the_same_voice() -> None:
    assert finding_gleif(_gleif_bundle(direct_parent=None)) == (
        "No consolidating parent and no reporting exception are on file."
    )


def test_gleif_finding_reads_the_live_reason_spelling_not_just_the_dump_one() -> None:
    """The live API writes ``reason``; the OO dump writes ``exceptionReason``.
    Reading only the dump spelling would have left every live report with a
    parent-less GLEIF row and no sentence at all — the same class of mistake
    as templating the wrong adapter."""
    dump = _gleif_bundle(
        direct_parent=None,
        ultimate_parent=None,
        direct_parent_exception=_gleif_exception("NO_KNOWN_PERSON"),
        ultimate_parent_exception=_gleif_exception("NO_KNOWN_PERSON", ultimate=True),
    )
    live = _gleif_bundle(
        direct_parent=None,
        ultimate_parent=None,
        direct_parent_exception=_gleif_exception_live("NO_KNOWN_PERSON"),
        ultimate_parent_exception=_gleif_exception_live("NO_KNOWN_PERSON", ultimate=True),
    )
    assert finding_gleif(live) == finding_gleif(dump)
    assert finding_gleif(live) == (
        "No consolidating parent is reported at either level: no controlling "
        "person is known, a permitted exception."
    )


def test_gleif_finding_matches_what_the_live_api_returned_for_real_entities() -> None:
    """Four sentences captured from live GLEIF on 2026-08-22, so a template
    change that only satisfies hand-built fixtures still has to satisfy the
    shapes the API actually serves."""
    # Bloomberg Finance L.P. — different direct and ultimate parents.
    split = _gleif_bundle(
        direct_parent=_gleif_parent("549300B56MD0ZC402L06", "Bloomberg L.P."),
        ultimate_parent=_gleif_parent("549300RMUDWPHCUQNE66", "Bloomberg Inc."),
    )
    assert finding_gleif(split) == "Consolidated by Bloomberg L.P.; ultimately by Bloomberg Inc."

    # Bloomberg L.P. — one entity at both levels, plus 27 direct children.
    same = _gleif_bundle(
        direct_parent=_gleif_parent("549300RMUDWPHCUQNE66", "Bloomberg Inc."),
        ultimate_parent=_gleif_parent("549300RMUDWPHCUQNE66", "Bloomberg Inc."),
        direct_children_total=27,
    )
    assert finding_gleif(same) == (
        "Consolidated by Bloomberg Inc., its direct and ultimate parent; "
        "27 direct subsidiaries report to it."
    )

    # Skandinaviska Enskilda Banken AB — NON_CONSOLIDATING at both levels.
    seb = _gleif_bundle(
        direct_parent=None,
        ultimate_parent=None,
        direct_parent_exception=_gleif_exception_live("NON_CONSOLIDATING"),
        ultimate_parent_exception=_gleif_exception_live("NON_CONSOLIDATING", ultimate=True),
    )
    assert finding_gleif(seb) == (
        "No consolidating parent is reported at either level: the parent "
        "prepares no consolidated accounts, a permitted exception."
    )

    # BP P.L.C. — 32 direct children, but the exception clause fills the
    # sentence, so the cap drops the child count. That is the cap working:
    # who consolidates the entity outranks how many report to it (rule 3).
    bp = _gleif_bundle(
        direct_parent=None,
        ultimate_parent=None,
        direct_parent_exception=_gleif_exception_live("NO_KNOWN_PERSON"),
        ultimate_parent_exception=_gleif_exception_live("NO_KNOWN_PERSON", ultimate=True),
        direct_children_total=32,
    )
    finding = finding_gleif(bp)
    assert finding is not None
    assert "32 direct subsidiaries" not in finding
    assert finding.startswith("No consolidating parent is reported at either level")
    assert len(finding) <= MAX_FINDING_CHARS


def test_gleif_finding_is_none_for_a_stub_bundle() -> None:
    assert finding_gleif({"source_id": "gleif", "hit_id": "X", "is_stub": True}) is None
    assert finding_gleif({}) is None


def test_bods_gleif_finding_uses_the_same_vocabulary_as_the_live_path() -> None:
    """The curated Parquet examples and every live lookup must not describe
    the same Level 2 relationship with two different verbs."""
    assert finding_bods_gleif(_gleif_payload(), _GLEIF_STATEMENT_ID) == (
        "Reports 1 direct consolidating parent; 2 direct subsidiaries report "
        "to it; incorporated in Sweden."
    )


def test_bods_gleif_finding_does_not_call_an_indirect_child_a_direct_one() -> None:
    """The live path counts GLEIF's ``/direct-children`` endpoint, so "direct"
    is true by construction there. The Parquet relationship table holds
    ultimate links too, so the word is only used when every child interest is
    filed as direct."""
    payload = _gleif_payload(children=0)
    payload["bods_statements"].append(
        _build_relationship_statement(
            ("oo-gleif-rel-ci", "oo-gleif-child-i", _GLEIF_STATEMENT_ID, "2026-01-05"),
            [{"directOrIndirect": "indirect"}],
        )
    )
    finding = finding_bods_gleif(payload, _GLEIF_STATEMENT_ID)
    assert finding is not None
    assert "1 subsidiary reports to it" in finding
    assert "direct subsidiary" not in finding


def test_bods_gleif_finding_states_a_missing_parent_in_the_same_voice() -> None:
    """No parent filed is a fact about the filing, not an empty card (rule 6)."""
    payload = _gleif_payload(parents=0, children=0, jurisdiction="")
    assert finding_bods_gleif(payload, _GLEIF_STATEMENT_ID) == (
        "No consolidating parent is reported."
    )


def test_opensanctions_finding_counts_listings_without_judging_them() -> None:
    assert finding_opensanctions(_OS_ROSNEFT) == (
        'Appears on 2 published listings, recorded under the topic '
        '"sanctions listing".'
    )


def test_opensanctions_finding_names_topics_in_english_and_quotes_them() -> None:
    """Reversed in Phase 133, deliberately.

    This test used to pin ``role.pep`` reaching the reader as ``role.pep``,
    defended on rule 7: turning it into "politically exposed" would move a
    source classification into OpenCheck's voice. The concern is right about
    the *adjective* and does not follow for the *slug* — `role.pep` is a key
    in a taxonomy, and on a live Rosneft lookup a reader was shown
    "recorded under the topics corp.disqual and 5 others".

    Two things keep rule 7 satisfied. The labels are noun phrases naming the
    topic ("politically exposed person"), never predicates about the subject.
    And they are quoted, so the frame says plainly that the words are
    OpenSanctions' name for a category rather than OpenCheck's description of
    the company.
    """
    item = {"id": "NK-putin", "schema": "Person", "topics": ["role.pep", "sanction"]}
    assert finding_opensanctions(item) == (
        "Held as a Person record, recorded under the topics "
        '"politically exposed person" and "sanctions listing".'
    )
    assert "role.pep" not in finding_opensanctions(item)


def test_opensanctions_finding_is_none_when_the_record_says_nothing_countable() -> None:
    assert finding_opensanctions({"id": "NK-empty"}) is None


def test_companies_house_finding_names_who_is_on_the_filing() -> None:
    assert finding_companies_house(ch_sample_bundle()) == (
        "2 people with significant control on file, including Jane SMITH with "
        "50% to 75% of shares."
    )


def test_companies_house_finding_states_an_absent_psc_in_the_same_voice() -> None:
    assert finding_companies_house(_CH_STATEMENTS_ONLY) == (
        "No person with significant control named; the filing states that no "
        "registrable person exists."
    )


def test_companies_house_finding_degrades_to_the_bare_absence() -> None:
    """An empty bundle still says something true rather than nothing at all."""
    assert finding_companies_house({"company_number": "00000000", "profile": {}}) == (
        "No person with significant control named."
    )


def test_opencorporates_finding_leads_with_whether_it_is_still_trading() -> None:
    assert finding_opencorporates(_oc_bundle()) == (
        "Active since 1 January 2000, 2 officers on file, registered as a "
        "Public Limited Company."
    )


def test_opencorporates_finding_falls_back_to_the_incorporation_date() -> None:
    assert finding_opencorporates(oc_minimal_bundle()) == "Incorporated 1 January 2000."


def test_opencorporates_finding_is_none_when_the_company_block_is_empty() -> None:
    assert finding_opencorporates({"ocid": "gb/00102498", "company": {}, "officers": []}) is None


def test_openaleph_finding_says_mentions_are_not_identity_matches() -> None:
    assert finding_openaleph(_ERICSSON_ENTITY, _OA_MENTIONS) == (
        "33 documents mention this name across 2 collections — mentions, not "
        "identity matches; indexed in Bureau van Dijk Orbis."
    )


def test_openaleph_finding_without_mentions_names_only_the_archive() -> None:
    """Mentions are fetched after the hit is built, so this is what the adapter
    itself can say; the pipeline rebuilds the sentence once they arrive."""
    assert finding_openaleph(_ERICSSON_ENTITY) == "Indexed in Bureau van Dijk Orbis."


def test_openaleph_finding_is_none_for_a_record_with_no_collection() -> None:
    assert finding_openaleph({"id": "aleph-1"}) is None


def test_ted_finding_counts_notices_and_dates_the_most_recent() -> None:
    assert finding_ted_eu(ted_won_bundle(["380129866"])) == (
        "Named in 5 EU procurement notices, 3 confirmed as contracts won, "
        "most recently 4 February 2025."
    )


def test_ted_finding_states_an_empty_result_in_the_same_voice() -> None:
    assert finding_ted_eu({"source_id": "ted_eu", "total_notice_count": 0, "notices": []}) == (
        "No EU procurement notice matched this party."
    )


def _ct_live_bundle(**overrides) -> dict:
    """A live climatetrace bundle shaped like _fetch_entity_data output."""
    bundle = {
        "source_id": "climatetrace",
        "entity_id": "E100000001096",
        "entity_name": "BP p.l.c.",
        "projects": {"total": [137, 106, 14], "statuses": {}, "trackers": {}},
        "emissions": {"total_co2e_tonnes": 200_800_000.0, "year": 2024},
        "entity_status": None,
        "is_stub": False,
    }
    bundle.update(overrides)
    return bundle


def test_climatetrace_finding_counts_projects_and_emissions() -> None:
    assert finding_climatetrace(_ct_live_bundle()) == (
        "137 live energy projects on file (106 operating); "
        "2024 emissions estimated at 200.8 Mt CO\u2082e."
    )


def test_climatetrace_finding_leads_with_amalgamation() -> None:
    """Rule 3 — a dead entity changes a decision more than any asset count."""
    assert finding_climatetrace(
        _ct_live_bundle(
            projects={"total": [0, 0, 0], "statuses": {}, "trackers": {}},
            emissions={},
            entity_status={
                "status": "amalgamated",
                "merged_into": "E100001014363",
                "merged_into_name": "Delek Logistics Partners LP",
            },
        )
    ) == (
        "Recorded as amalgamated into Delek Logistics Partners LP; "
        "no live energy projects on file."
    )


def test_climatetrace_finding_dissolved_and_joint_venture() -> None:
    assert finding_climatetrace(
        _ct_live_bundle(
            projects=None,
            emissions={},
            entity_status={"status": "dissolved", "jv": True},
        )
    ) == "Recorded as dissolved; identified as a joint venture."


def test_climatetrace_finding_is_none_for_stub_and_empty_bundles() -> None:
    assert finding_climatetrace({}) is None
    assert finding_climatetrace(_ct_live_bundle(is_stub=True)) is None
    assert finding_climatetrace(
        _ct_live_bundle(projects=None, emissions={}, entity_status=None)
    ) is None


def test_wikidata_finding_leads_with_the_parent_organisation() -> None:
    assert finding_wikidata(wd_parent_bundle()["summary"]) == (
        "Part of Telefonaktiebolaget LM Ericsson; described as a public company; "
        "1 cross-identifier published."
    )


def test_wikidata_finding_drops_the_parent_clause_when_none_is_recorded() -> None:
    assert finding_wikidata(wd_entity_bundle()["summary"]) == (
        "Described as a public company; 2 cross-identifiers published."
    )


def test_wikidata_finding_for_a_person_leads_with_the_position_held() -> None:
    assert finding_wikidata(wd_person_bundle()["summary"]) == (
        "Recorded as President of Russia since 7 May 2012; citizen of Soviet "
        "Union and Russia."
    )


def test_wikidata_finding_words_an_indicative_share_without_a_control_word() -> None:
    """Wikidata's own note is that P1107 is indicative — it conflates capital,
    voting and time — so a 40.4% holding is stated, never characterised."""
    summary = dict(
        wd_entity_bundle()["summary"],
        controlling_owners=[{"name": "Rosneftegaz JSC", "share_percent": 40.4}],
    )
    finding = finding_wikidata(summary)
    assert finding == (
        "Rosneftegaz JSC holds 40.4%; described as a public company; "
        "2 cross-identifiers published."
    )
    assert not any(word in finding.lower() for word in _CONTROL_WORDS)


def test_wikidata_finding_is_none_for_an_empty_summary() -> None:
    assert finding_wikidata({}) is None


# ---------------------------------------------------------------------------
# GLEIF Level 2 is consolidation, not ownership
# ---------------------------------------------------------------------------

# Every sentence either GLEIF template can produce, live path and Parquet
# path together. Both feed the vocabulary guard below.
_ALL_GLEIF_FINDINGS: dict[str, str | None] = {
    "direct": finding_gleif(_gleif_bundle()),
    "direct+children": finding_gleif(_gleif_bundle(direct_children_total=12)),
    "direct+one-child": finding_gleif(_gleif_bundle(direct_children_total=1)),
    "direct+ultimate": finding_gleif(_gleif_bundle(ultimate_parent=_GLEIF_ULTIMATE_PARENT)),
    "ultimate-only": finding_gleif(
        _gleif_bundle(direct_parent=None, ultimate_parent=_GLEIF_ULTIMATE_PARENT)
    ),
    "direct+ultimate-exception": finding_gleif(
        _gleif_bundle(
            ultimate_parent_exception=_gleif_exception("NON_PUBLIC", ultimate=True)
        )
    ),
    "children-with-record": finding_gleif(
        _gleif_bundle(
            direct_children=[gleif_child_record("CHILDXXXXXXXXXXXXXXX", "BP France SAS")],
            direct_children_total=1,
        )
    ),
    **{
        f"exception/{reason}": finding_gleif(
            _gleif_bundle(direct_parent=None, direct_parent_exception=_gleif_exception(reason))
        )
        for reason in (
            "NATURAL_PERSONS",
            "NO_KNOWN_PERSON",
            "NO_LEI",
            "NON_CONSOLIDATING",
            "NON_PUBLIC",
            "BINDING_LEGAL_COMMITMENTS",
            "LEGAL_OBSTACLES",
            "DISCLOSURE_DETRIMENTAL",
            "DETRIMENT_NOT_EXCLUDED",
            "CONSENT_NOT_OBTAINED",
            "SOMETHING_NEW",
        )
    },
    "exception/both-levels": finding_gleif(
        _gleif_bundle(
            direct_parent=None,
            direct_parent_exception=_gleif_exception("NATURAL_PERSONS"),
            ultimate_parent_exception=_gleif_exception("NATURAL_PERSONS", ultimate=True),
        )
    ),
    "exception/differing-levels": finding_gleif(
        _gleif_bundle(
            direct_parent=None,
            direct_parent_exception=_gleif_exception("NON_CONSOLIDATING"),
            ultimate_parent_exception=_gleif_exception("NO_LEI", ultimate=True),
        )
    ),
    "neither": finding_gleif(_gleif_bundle(direct_parent=None)),
    "parquet": finding_bods_gleif(_gleif_payload(), _GLEIF_STATEMENT_ID),
    "parquet/no-parent": finding_bods_gleif(
        _gleif_payload(parents=0, children=0, jurisdiction=""), _GLEIF_STATEMENT_ID
    ),
    "parquet/indirect": finding_bods_gleif(_gleif_payload(children=0), _GLEIF_STATEMENT_ID),
}

#: GLEIF Level 2 is an accounting consolidation link (IS_DIRECTLY_CONSOLIDATED_BY),
#: not a shareholding, and GLEIF publishes no percentage anywhere in it. A
#: sentence that reached for ownership vocabulary would be asserting something
#: the source never said.
_OWNERSHIP_WORDS = (
    "owns", "own", "owned", "holds", "hold", "held",
    "shareholder", "shareholding", "stake", "equity", "majority",
)

#: Legal names the fixtures supply. A parent genuinely called "BP Group
#: Holdings" is the source's word, not OpenCheck's, so it is removed before
#: the vocabulary check — otherwise the guard fails on real company names
#: (and every Nordic "… Holding AB" would trip it in production).
_GLEIF_FIXTURE_NAMES = ("BP Group Holdings", "BP p.l.c.", "BP France SAS", "Ericsson AB")


def _template_wording(finding: str) -> str:
    """The sentence with source-supplied names stripped — what is left is
    OpenCheck's own wording, which is what these rules govern."""
    text = finding
    for name in _GLEIF_FIXTURE_NAMES:
        text = text.replace(name, " ")
    return text.lower()


@pytest.mark.parametrize("case", sorted(_ALL_GLEIF_FINDINGS))
def test_no_gleif_finding_claims_ownership_or_a_percentage(case: str) -> None:
    finding = _ALL_GLEIF_FINDINGS[case]
    assert finding is not None, case
    assert "%" not in finding, f"{case}: GLEIF publishes no percentages"
    wording = _template_wording(finding)
    for word in _OWNERSHIP_WORDS:
        assert not re.search(rf"\b{word}\b", wording), (
            f"{case}: {word!r} — Level 2 is consolidation, not ownership"
        )


@pytest.mark.parametrize("case", sorted(_ALL_GLEIF_FINDINGS))
def test_every_gleif_finding_uses_the_consolidation_verb(case: str) -> None:
    """One vocabulary across the live path and the curated Parquet path."""
    finding = _ALL_GLEIF_FINDINGS[case]
    assert finding is not None, case
    assert "consolidat" in finding.lower(), case


@pytest.mark.parametrize("case", sorted(_ALL_GLEIF_FINDINGS))
def test_no_gleif_finding_exceeds_the_hard_cap(case: str) -> None:
    finding = _ALL_GLEIF_FINDINGS[case]
    assert finding is not None
    assert len(finding) <= MAX_FINDING_CHARS, f"{case}: {len(finding)} chars"


@pytest.mark.parametrize(
    "case", sorted(k for k in _ALL_GLEIF_FINDINGS if k.startswith("exception/"))
)
def test_every_reporting_exception_reads_as_permitted(case: str) -> None:
    """An exception is a permitted filing under the LEI ROC policy. None of
    these sentences may read as concealment or as a compliance failure."""
    finding = _ALL_GLEIF_FINDINGS[case]
    assert finding is not None
    assert "permitted exception" in finding
    for word in ("fail", "refus", "conceal", "hidden", "missing", "undisclosed"):
        assert word not in finding.lower(), f"{case}: {word!r}"


# ---------------------------------------------------------------------------
# Cross-cutting rules
# ---------------------------------------------------------------------------

_ALL_FIXTURE_FINDINGS: dict[str, str | None] = {
    "gleif": finding_gleif(_gleif_bundle(direct_children_total=12)),
    "gleif/ultimate": finding_gleif(_gleif_bundle(ultimate_parent=_GLEIF_ULTIMATE_PARENT)),
    "gleif/exception": finding_gleif(
        _gleif_bundle(
            direct_parent=None, direct_parent_exception=_gleif_exception("NATURAL_PERSONS")
        )
    ),
    "gleif/neither": finding_gleif(_gleif_bundle(direct_parent=None)),
    "bods_gleif": finding_bods_gleif(_gleif_payload(), _GLEIF_STATEMENT_ID),
    "bods_gleif/degraded": finding_bods_gleif(
        _gleif_payload(parents=0, children=0, jurisdiction=""), _GLEIF_STATEMENT_ID
    ),
    "opensanctions": finding_opensanctions(_OS_ROSNEFT),
    "companies_house": finding_companies_house(ch_sample_bundle()),
    "companies_house/statements": finding_companies_house(_CH_STATEMENTS_ONLY),
    "opencorporates": finding_opencorporates(_oc_bundle()),
    "openaleph": finding_openaleph(_ERICSSON_ENTITY, _OA_MENTIONS),
    "ted_eu": finding_ted_eu(ted_won_bundle(["380129866"])),
    "climatetrace": finding_climatetrace(_ct_live_bundle()),
    "climatetrace/amalgamated": finding_climatetrace(
        _ct_live_bundle(
            projects={"total": [0, 0, 0], "statuses": {}, "trackers": {}},
            emissions={},
            entity_status={
                "status": "amalgamated",
                "merged_into_name": "Delek Logistics Partners LP",
            },
        )
    ),
    "wikidata": finding_wikidata(wd_parent_bundle()["summary"]),
    "wikidata/person": finding_wikidata(wd_person_bundle()["summary"]),
}


@pytest.mark.parametrize("source_id", sorted(_ALL_FIXTURE_FINDINGS))
def test_no_template_exceeds_the_hard_cap_on_its_fixture(source_id: str) -> None:
    finding = _ALL_FIXTURE_FINDINGS[source_id]
    assert finding is not None
    assert len(finding) <= MAX_FINDING_CHARS, f"{source_id}: {len(finding)} chars"


@pytest.mark.parametrize("source_id", sorted(_ALL_FIXTURE_FINDINGS))
def test_every_finding_is_one_full_stopped_sentence(source_id: str) -> None:
    finding = _ALL_FIXTURE_FINDINGS[source_id]
    assert finding is not None
    assert finding[0] == finding[0].upper()
    assert finding.endswith(".")
    assert ". " not in finding, "a second sentence has crept in"


@pytest.mark.parametrize("source_id", sorted(_ALL_FIXTURE_FINDINGS))
def test_no_finding_leaks_a_slot_marker(source_id: str) -> None:
    """Rule 8 — a missing field shortens the sentence; it never renders."""
    finding = _ALL_FIXTURE_FINDINGS[source_id]
    assert finding is not None
    lowered = finding.lower()
    for marker in ("none", "{", "}", "  ", " ,", " ;", "at %"):
        assert marker not in lowered, f"{source_id}: {marker!r}"


@pytest.mark.parametrize("source_id", sorted(_ALL_FIXTURE_FINDINGS))
def test_no_finding_hyphenates_beneficial_ownership(source_id: str) -> None:
    """House style, rule 9."""
    finding = _ALL_FIXTURE_FINDINGS[source_id]
    assert finding is not None
    assert "beneficial-ownership" not in finding.lower()


# ---------------------------------------------------------------------------
# The field itself
# ---------------------------------------------------------------------------


def test_finding_is_optional_on_source_hit() -> None:
    hit = SourceHit(
        source_id="companies_house",
        hit_id="00102498",
        kind=SearchKind.ENTITY,
        name="BP P.L.C.",
        summary="GB-COH 00102498",
    )
    assert hit.finding is None
    assert hit.model_dump()["finding"] is None


def test_finding_crosses_the_wire_alongside_summary() -> None:
    """``summary`` keeps its identifier-fragment shape; the sentence rides
    beside it rather than replacing it."""
    hit = SourceHit(
        source_id="companies_house",
        hit_id="00102498",
        kind=SearchKind.ENTITY,
        name="BP P.L.C.",
        summary="GB-COH 00102498",
        finding="2 people with significant control on file.",
    )
    dumped = hit.model_dump()
    assert dumped["summary"] == "GB-COH 00102498"
    assert dumped["finding"] == "2 people with significant control on file."
    assert '"finding"' in hit.model_dump_json()
