"""Structural complexity and jurisdiction risk-signal tests.

Phase 272 re-based these on the FINAL draft of AMLA's CDD RTS (Art 28(1)
AMLR, 30 Sep 2026). Article 11(4) lists four elements that may increase
complexity — and sets no definition or threshold:

  (a) the number of intermediate layers          -> COMPLEX_OWNERSHIP_LAYERS
  (b) legal arrangements in any of those layers  -> element "arrangement"
  (c) customer or layer entity registered in a
      high-risk jurisdiction (FATF / EU lists)   -> element "high_risk_jurisdiction"
  (d) nominees involved in the structure         -> element "nominee"

(b) and (c) are path-scoped; (d) is bundle-wide. The elements ride on the
layers signal; the consultation draft's composite COMPLEX_CORPORATE_STRUCTURE
is retired and must never be emitted, and non-EU status feeds nothing.

Plus the two list-based jurisdiction RISK signals (FATF, EU Article 29),
NON_EU_JURISDICTION as kind="context", the ``POSSIBLE_OBFUSCATION`` advisory
(opacity + layering), and the operator-tunable ``OPENCHECK_AMLA_*`` /
``OPENCHECK_HIGH_RISK_JURISDICTION_LISTS`` settings.
"""

from __future__ import annotations

import pytest

from opencheck.config import get_settings
from opencheck.risk import (
    COMPLEX_CORPORATE_STRUCTURE,
    COMPLEX_OWNERSHIP_LAYERS,
    DEFAULT_EU_EEA_COUNTRY_CODES,
    EU_EEA_COUNTRY_CODES,
    EU_HIGH_RISK_THIRD_COUNTRY,
    EU_HIGH_RISK_THIRD_COUNTRY_CODES,
    EU_HRTC_INSTRUMENT,
    EU_HRTC_SECTION_IV_CODES,
    FATF_BLACK_LIST,
    FATF_BLACK_LIST_CODES,
    FATF_GREY_LIST,
    FATF_GREY_LIST_CODES,
    NOMINEE,
    NON_EU_JURISDICTION,
    OPAQUE_OWNERSHIP,
    POSSIBLE_OBFUSCATION,
    RETIRED_SIGNAL_CODES,
    TRUST_OR_ARRANGEMENT,
    _eu_eea_codes,
    assess_amla,
    assess_bundle,
    assess_structure,
    high_risk_lists_for,
)


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------
# Helpers — build small BODS bundles in v0.4 nested shape
# ---------------------------------------------------------------------


def _entity(sid: str, *, entity_type: str = "registeredEntity",
            jurisdiction_code: str | None = None,
            jurisdiction_name: str | None = None,
            legal_form: str | None = None,
            identifier: str | None = None,
            name: str = "Acme") -> dict:
    rd: dict = {
        "entityType": {"type": entity_type},
        "name": name,
    }
    if jurisdiction_code:
        rd["jurisdiction"] = {
            "code": jurisdiction_code,
            "name": jurisdiction_name or jurisdiction_code,
        }
    if legal_form:
        rd["legalForm"] = legal_form
    if identifier:
        rd["identifiers"] = [{"id": identifier, "scheme": "XI-LEI"}]
    return {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": rd,
    }


def _person(sid: str, *, person_type: str = "knownPerson",
            full_name: str = "Jane Smith") -> dict:
    return {
        "statementId": sid,
        "recordType": "person",
        "recordDetails": {
            "personType": person_type,
            "names": [{"type": "individual", "fullName": full_name}],
        },
    }


def _rel(sid: str, subject: str, ip: str, *, ip_kind: str = "entity",
         interests: list | None = None) -> dict:
    return {
        "statementId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": ip,
            "interests": interests or [
                {"type": "shareholding", "directOrIndirect": "direct"}
            ],
        },
    }


# ---------------------------------------------------------------------
# (a) Trust / legal arrangement
# ---------------------------------------------------------------------


def test_trust_or_arrangement_fires_on_arrangement_entity_type() -> None:
    bods = [_entity("E1", entity_type="arrangement", name="The Smith Family Trust")]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    codes = {s.code for s in signals}
    assert TRUST_OR_ARRANGEMENT in codes
    sig = next(s for s in signals if s.code == TRUST_OR_ARRANGEMENT)
    assert sig.confidence == "high"
    assert "AMLA" not in sig.summary  # Phase 272: regime-neutral wording
    assert sig.evidence["matches"][0]["match"] == "entityType=arrangement"


def test_trust_or_arrangement_fires_on_legal_form_keyword() -> None:
    bods = [
        _entity(
            "E1",
            entity_type="legalEntity",
            legal_form="Liechtenstein Stiftung",
        )
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    codes = {s.code for s in signals}
    assert TRUST_OR_ARRANGEMENT in codes
    sig = next(s for s in signals if s.code == TRUST_OR_ARRANGEMENT)
    assert "stiftung" in sig.evidence["matches"][0]["match"].lower()


def test_no_trust_signal_for_plain_company() -> None:
    bods = [_entity("E1", legal_form="Limited company")]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert TRUST_OR_ARRANGEMENT not in {s.code for s in signals}


def test_trust_signal_does_not_fire_on_name_alone() -> None:
    """A trust/foundation keyword in the *name* must not fire the signal — only
    the legal form counts. Regression: GLEIF ("…Identifier Foundation") is a
    registered company, not a foundation arrangement, by its name."""
    bods = [
        _entity(
            "E1",
            entity_type="registeredEntity",
            name="Global Legal Entity Identifier Foundation",
        )
    ]
    signals = assess_amla("zefix", {"entity_id": "X"}, bods)
    assert TRUST_OR_ARRANGEMENT not in {s.code for s in signals}


def test_trust_signal_fires_on_legal_form_label_annotation() -> None:
    """The `legalFormLabel` annotation (what mappers attach) is a matched field,
    and the evidence names that field rather than mislabelling it 'legalForm'."""
    bods = [
        _entity("E1", entity_type="registeredEntity", name="Some Foundation")
    ]
    bods[0]["recordDetails"]["legalFormLabel"] = "Foundation"
    signals = assess_amla("zefix", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == TRUST_OR_ARRANGEMENT)
    match = sig.evidence["matches"][0]["match"]
    assert match == "legalFormLabel contains 'foundation'"


def test_trust_signal_fires_on_entity_type_subtype() -> None:
    # Exercise the entityType.subtype field path (not the arrangement shortcut).
    bods = [_entity("E1", name="Holdings Ltd")]
    bods[0]["recordDetails"]["entityType"] = {
        "type": "registeredEntity",
        "subtype": "trust",
    }
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == TRUST_OR_ARRANGEMENT)
    assert sig.evidence["matches"][0]["match"] == "entityType.subtype contains 'trust'"


# ---------------------------------------------------------------------
# FATF grey list — June 2026 plenary
# ---------------------------------------------------------------------


def test_fatf_grey_list_june_2026_membership() -> None:
    # Added at the June 2026 plenary.
    assert "BA" in FATF_GREY_LIST_CODES  # Bosnia and Herzegovina
    assert "IQ" in FATF_GREY_LIST_CODES  # Iraq
    # Removed at the June 2026 plenary.
    assert "DZ" not in FATF_GREY_LIST_CODES  # Algeria
    assert "NA" not in FATF_GREY_LIST_CODES  # Namibia
    assert len(FATF_GREY_LIST_CODES) == 22


def test_fatf_grey_signal_fires_for_newly_added_jurisdiction() -> None:
    bods = [_entity("E1", jurisdiction_code="IQ", jurisdiction_name="Iraq")]
    signals = assess_amla("openaleph", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == FATF_GREY_LIST)
    assert sig.confidence == "medium"
    assert "June 2026" in sig.summary
    assert sig.evidence["jurisdictions"][0]["code"] == "IQ"


def test_fatf_grey_signal_does_not_fire_for_removed_jurisdiction() -> None:
    # Algeria came off the grey list in June 2026 — no FATF_GREY_LIST signal.
    bods = [_entity("E1", jurisdiction_code="DZ", jurisdiction_name="Algeria")]
    signals = assess_amla("openaleph", {"entity_id": "X"}, bods)
    assert FATF_GREY_LIST not in {s.code for s in signals}
    # …but Algeria IS still EU-listed. The Commission adopts its updates
    # months after the FATF plenary that prompts them, so the two lists
    # genuinely diverge — which is why these are separate signals and not
    # one widened code set.
    assert EU_HIGH_RISK_THIRD_COUNTRY in {s.code for s in signals}


# ---------------------------------------------------------------------
# EU Article 29 high-risk third countries
# ---------------------------------------------------------------------


def test_eu_hrtc_membership_matches_delegated_regulations_2026_46_and_2026_83() -> None:
    """Delegated Regs (EU) 2026/46 and (EU) 2026/83, both applying from
    29 January 2026 and both published in OJ L of 9 January 2026.

    Two instruments, one application date — which is why a date-only check
    does not distinguish them. Assert membership per instrument.
    """
    # Added by 2026/83, to Section I.
    assert "BO" in EU_HIGH_RISK_THIRD_COUNTRY_CODES  # Bolivia
    assert "VG" in EU_HIGH_RISK_THIRD_COUNTRY_CODES  # British Virgin Islands
    # Removed by 2026/83.
    for code in ("BF", "ML", "MZ", "NG", "ZA", "TZ"):
        assert code not in EU_HIGH_RISK_THIRD_COUNTRY_CODES
    # Added by 2026/46, to the new Section IV.
    assert "RU" in EU_HIGH_RISK_THIRD_COUNTRY_CODES  # Russian Federation
    assert set(EU_HRTC_SECTION_IV_CODES) == {"RU"}
    assert EU_HRTC_SECTION_IV_CODES <= EU_HIGH_RISK_THIRD_COUNTRY_CODES
    # 23 in Section I, plus Iran (II), DPRK (III) and Russia (IV).
    assert len(EU_HIGH_RISK_THIRD_COUNTRY_CODES) == 26


def test_eu_hrtc_instrument_names_both_delegated_regulations() -> None:
    """The prose string is user-facing — it must not credit only 2026/83.

    2026/46 went unnoticed for seven months because "as amended to
    29 January 2026" was already true without it.
    """
    assert "2016/1675" in EU_HRTC_INSTRUMENT
    assert "2026/46" in EU_HRTC_INSTRUMENT
    assert "2026/83" in EU_HRTC_INSTRUMENT


def test_russia_is_eu_listed_but_on_no_fatf_list() -> None:
    """Section IV is the widest case of the FATF/EU divergence.

    The FATF suspended Russia's membership on 24 February 2023; it has never
    been on the black or grey list. The EU listed it on its own analysis in
    (EU) 2026/46. A future refresh must not "tidy" this by adding RU to a
    FATF set — no plenary has ever put it there.
    """
    assert "RU" not in FATF_BLACK_LIST_CODES
    assert "RU" not in FATF_GREY_LIST_CODES

    bods = [_entity("E1", jurisdiction_code="RU",
                    jurisdiction_name="Russian Federation")]
    signals = assess_amla("gleif", {"entity_id": "X"}, bods)
    codes = {s.code for s in signals}
    assert EU_HIGH_RISK_THIRD_COUNTRY in codes
    assert FATF_BLACK_LIST not in codes
    assert FATF_GREY_LIST not in codes


def test_eu_hrtc_section_iv_summary_says_suspended_not_deficient() -> None:
    """Russia's listing must not be reported as a FATF identification.

    One signal code for the whole Annex — Article 29 attaches the same EDD
    obligation to every section — but the sentence has to distinguish them.
    """
    bods = [_entity("E1", jurisdiction_code="RU",
                    jurisdiction_name="Russian Federation")]
    signals = assess_amla("gleif", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == EU_HIGH_RISK_THIRD_COUNTRY)
    assert sig.confidence == "high"
    assert sig.kind == "risk"
    assert "Section IV" in sig.summary
    assert "suspended from FATF membership" in sig.summary
    assert sig.evidence["jurisdictions"][0]["annex_section"] == "IV"


def test_eu_hrtc_sections_one_to_three_carry_no_section_iv_clause() -> None:
    """A Section I hit gets the plain sentence — no suspension wording."""
    bods = [_entity("E1", jurisdiction_code="VG",
                    jurisdiction_name="British Virgin Islands")]
    signals = assess_amla("gleif", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == EU_HIGH_RISK_THIRD_COUNTRY)
    assert "Section IV" not in sig.summary
    assert "suspended" not in sig.summary
    assert sig.evidence["jurisdictions"][0]["annex_section"] == "I-III"


def test_eu_hrtc_signal_fires_and_names_the_instrument() -> None:
    bods = [_entity("E1", jurisdiction_code="VG",
                    jurisdiction_name="British Virgin Islands")]
    signals = assess_amla("gleif", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == EU_HIGH_RISK_THIRD_COUNTRY)
    assert sig.confidence == "high"
    assert sig.kind == "risk"
    assert "2016/1675" in sig.summary
    assert sig.evidence["jurisdictions"][0]["code"] == "VG"


def test_eu_hrtc_does_not_fire_for_unlisted_jurisdiction() -> None:
    """The UK is not on the EU list — and must not be."""
    bods = [_entity("E1", jurisdiction_code="GB", jurisdiction_name="United Kingdom")]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    codes = {s.code for s in signals}
    assert EU_HIGH_RISK_THIRD_COUNTRY not in codes
    assert FATF_GREY_LIST not in codes
    assert FATF_BLACK_LIST not in codes
    # A UK entity on its own raises nothing — not even structural context.
    # Its own jurisdiction is where it *is*, not somewhere its ownership
    # chain *reaches* (Phase 153).
    assert codes == set()


# ---------------------------------------------------------------------
# (b) Non-EU / EEA jurisdiction
# ---------------------------------------------------------------------


def test_non_eu_jurisdiction_is_context_not_risk() -> None:
    """Non-EU status is a structural observation, not an adverse finding.

    Neither the AMLA CDD RTS nor AMLR Annex III(3) treats being outside
    the EU as a risk factor in itself, so the signal reports at low
    confidence and is classified kind="context" — which is what keeps it
    out of the risk chip strip and out of the "N risk signals" count on
    the share card and share-page meta description.
    """
    bods = [
        _entity("E1", jurisdiction_code="DE"),
        _entity("E2", jurisdiction_code="PA", jurisdiction_name="Panama"),
        _rel("R1", "E1", "E2"),
    ]
    signals = assess_amla("openaleph", {"entity_id": "E1"}, bods)
    sig = next(s for s in signals if s.code == NON_EU_JURISDICTION)
    assert sig.kind == "context"
    assert sig.confidence == "low"
    assert "not a risk finding" in sig.summary
    assert "PA" in sig.summary
    assert sig.evidence["jurisdictions"][0]["code"] == "PA"
    # And it must survive serialisation — every surface reads this field.
    assert sig.to_dict()["kind"] == "context"


def test_non_eu_never_counts_the_subjects_own_jurisdiction() -> None:
    """Phase 153. Shell plc's report carried "Ownership chain reaches
    jurisdictions outside the EU/EEA: GB" on seven source cards, each from a
    bundle holding nothing above the subject but the subject. Where a company
    *is* is not somewhere its chain *reaches*."""
    bods = [
        _entity("E1", jurisdiction_code="GB"),
        _entity("E2", jurisdiction_code="GB"),
        _rel("R1", "E1", "E2"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    sig = next(s for s in signals if s.code == NON_EU_JURISDICTION)
    # The GB *parent* is a chain fact; the subject's own GB is not listed as
    # a separate statement.
    assert [j["statement_id"] for j in sig.evidence["jurisdictions"]] == ["E2"]
    # And a lone entity, or a subject whose only owners share its own EU
    # jurisdiction, raises nothing.
    assert NON_EU_JURISDICTION not in {
        s.code for s in assess_amla("companies_house", {"entity_id": "E1"},
                                    [_entity("E1", jurisdiction_code="GB")])
    }


def test_non_eu_never_counts_subsidiaries_below_the_subject() -> None:
    """A GLEIF bundle lists a parent's direct subsidiaries. They are below
    the subject, not on its ownership chain, and must not be reported as
    somewhere the chain reaches."""
    bods = [
        _entity("E1", jurisdiction_code="DE"),                # subject
        _entity("E2", jurisdiction_code="KY"),                # subsidiary
        _entity("E3", jurisdiction_code="BM"),                # subsidiary
        _rel("R1", "E2", "E1"),                               # E1 owns E2
        _rel("R2", "E3", "E1"),                               # E1 owns E3
    ]
    signals = assess_amla("gleif", {"entity_id": "E1"}, bods)
    assert NON_EU_JURISDICTION not in {s.code for s in signals}


def test_non_eu_walks_the_whole_chain_above_the_subject() -> None:
    bods = [
        _entity("E1", jurisdiction_code="DE"),
        _entity("E2", jurisdiction_code="FR"),
        _entity("E3", jurisdiction_code="US"),
        _rel("R1", "E1", "E2"),
        _rel("R2", "E2", "E3"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    sig = next(s for s in signals if s.code == NON_EU_JURISDICTION)
    assert [j["code"] for j in sig.evidence["jurisdictions"]] == ["US"]


def test_non_eu_finds_the_subject_by_its_identifier_not_its_position() -> None:
    """Mappers put the subject first, but the rule must not depend on it:
    the entity carrying the adapter-local hit id is the subject."""
    owner = _entity("E9", jurisdiction_code="PA", name="Owner SA")
    subject = _entity("E1", jurisdiction_code="DE", name="Subject GmbH")
    subject["recordDetails"]["identifiers"] = [
        {"scheme": "DE-HRB", "id": "HRB 12345"},
    ]
    bods = [owner, subject, _rel("R1", "E1", "E9")]
    signals = assess_amla("firmenbuch", {}, bods, hit_id="HRB 12345")
    sig = next(s for s in signals if s.code == NON_EU_JURISDICTION)
    assert [j["code"] for j in sig.evidence["jurisdictions"]] == ["PA"]
    # Same bundle, hit id naming the owner: the owner is now the subject and
    # has nothing above it, so nothing is reported.
    owner["recordDetails"]["identifiers"] = [{"scheme": "PA-REG", "id": "PA-1"}]
    signals = assess_amla("firmenbuch", {}, bods, hit_id="PA-1")
    assert NON_EU_JURISDICTION not in {s.code for s in signals}


def test_fatf_and_eu_jurisdiction_signals_stay_risk() -> None:
    """The two list-based jurisdiction signals remain risk findings."""
    bods = [_entity("E1", jurisdiction_code="IR", jurisdiction_name="Iran")]
    signals = assess_amla("openaleph", {"entity_id": "X"}, bods)
    by_code = {s.code: s for s in signals}
    assert by_code[FATF_BLACK_LIST].kind == "risk"
    assert by_code[EU_HIGH_RISK_THIRD_COUNTRY].kind == "risk"


def test_eu_member_states_do_not_fire_non_eu_signal() -> None:
    bods = [
        _entity("E1", jurisdiction_code="DE"),
        _entity("E2", jurisdiction_code="FR"),
        _entity("E3", jurisdiction_code="IE"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert NON_EU_JURISDICTION not in {s.code for s in signals}


def test_eea_non_eu_countries_treated_as_eu_equivalent() -> None:
    """Norway / Iceland / Liechtenstein share EU AML supervision."""
    for code in ("NO", "IS", "LI"):
        assert code in EU_EEA_COUNTRY_CODES
    bods = [_entity("E1", jurisdiction_code="NO")]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert NON_EU_JURISDICTION not in {s.code for s in signals}


def test_non_eu_aggregates_codes_in_summary() -> None:
    bods = [
        _entity("E0", jurisdiction_code="DE"),  # the subject
        _entity("E1", jurisdiction_code="VG", jurisdiction_name="British Virgin Islands"),
        _entity("E2", jurisdiction_code="KY", jurisdiction_name="Cayman Islands"),
        _entity("E3", jurisdiction_code="DE"),  # EU — ignored in summary
        _rel("R1", "E0", "E1"),
        _rel("R2", "E0", "E2"),
        _rel("R3", "E0", "E3"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "E0"}, bods)
    sig = next(s for s in signals if s.code == NON_EU_JURISDICTION)
    assert "KY" in sig.summary and "VG" in sig.summary and "DE" not in sig.summary


# ---------------------------------------------------------------------
# (c) Nominee
# ---------------------------------------------------------------------


def test_nominee_fires_on_interest_details() -> None:
    bods = [
        _entity("E1"),
        _person("P1", full_name="John Doe"),
        _rel(
            "R1", "E1", "P1", ip_kind="person",
            interests=[
                {
                    "type": "shareholding",
                    "details": "Held by John Doe acting as nominee shareholder.",
                }
            ],
        ),
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == NOMINEE)
    # Text-only evidence is real but weaker than a filed code — "Nominee
    # Services Ltd" is a company name, not a declaration — so it reports
    # medium and says outright what it matched on.
    assert sig.confidence == "medium"
    assert sig.evidence["basis"] == "textual"
    assert "descriptive text" in sig.summary
    assert "AMLA" not in sig.summary  # Phase 272: regime-neutral wording


def test_nominee_fires_on_interest_type_string() -> None:
    bods = [
        _entity("E1"),
        _person("P1"),
        _rel(
            "R1", "E1", "P1", ip_kind="person",
            interests=[{"type": "nomineeShareholder"}],
        ),
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert NOMINEE in {s.code for s in signals}


def test_nominee_fires_on_person_statement_name() -> None:
    bods = [
        _person("P1", full_name="ABC Nominees Ltd Trustee"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert NOMINEE in {s.code for s in signals}


def test_no_nominee_signal_for_plain_relationship() -> None:
    bods = [
        _entity("E1"),
        _person("P1"),
        _rel("R1", "E1", "P1", ip_kind="person"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert NOMINEE not in {s.code for s in signals}


# ---------------------------------------------------------------------
# Layered ownership + complexity elements (Phase 272)
# ---------------------------------------------------------------------


def _three_layer_chain() -> list[dict]:
    """Subject E1 owned by E2 owned by E3 (three corporate layers).

    All entities default to DE so the chain on its own does NOT trigger
    NON_EU_JURISDICTION — individual tests then mutate one layer to add
    a specific aggravator (non-EU, trust, nominee).
    """
    return [
        _entity("E1", name="Subject GmbH", jurisdiction_code="DE"),
        _entity("E2", name="Holding 1 GmbH", jurisdiction_code="DE"),
        _entity("E3", name="Holding 2 GmbH", jurisdiction_code="DE"),
        _rel("R1", "E1", "E2"),
        _rel("R2", "E2", "E3"),
    ]


def test_layers_signal_fires_at_three() -> None:
    signals = assess_amla(
        "companies_house", {"entity_id": "E1"}, _three_layer_chain()
    )
    layers = next(s for s in signals if s.code == COMPLEX_OWNERSHIP_LAYERS)
    assert layers.evidence["layers"] == 3
    assert layers.confidence == "medium"


def test_layers_signal_does_not_fire_at_two() -> None:
    bods = [
        _entity("E1"),
        _entity("E2"),
        _rel("R1", "E1", "E2"),
    ]
    codes = {s.code for s in assess_amla("companies_house", {"entity_id": "E1"}, bods)}
    assert COMPLEX_OWNERSHIP_LAYERS not in codes


def test_layers_handles_cycles_safely() -> None:
    """A → B → C → A (cycle) shouldn't infinite-loop and shouldn't
    inflate the layer count beyond the distinct nodes in the cycle."""
    bods = [
        _entity("E1"),
        _entity("E2"),
        _entity("E3"),
        _rel("R1", "E1", "E2"),
        _rel("R2", "E2", "E3"),
        _rel("R3", "E3", "E1"),  # cycle
    ]
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    layers = next(s for s in signals if s.code == COMPLEX_OWNERSHIP_LAYERS)
    assert layers.evidence["layers"] == 3


def test_layers_never_counts_subsidiaries_below_the_subject() -> None:
    """The layers are between the customer and the BO, and the BO is ABOVE.

    BODS edges run ``interestedParty --(owns)--> subject``, so following them
    forwards walks *down* into subsidiaries. Until Phase 170 that is what the
    DFS did, and a GLEIF bundle listing a parent's children reported the depth
    of the group below the subject as the depth of the chain above it. The
    subject here owns a three-deep chain of subsidiaries and is owned by
    nobody: there are no layers between it and a beneficial owner.
    """
    bods = [
        _entity("E1", name="Subject GmbH", identifier="SUBJ", jurisdiction_code="DE"),
        _entity("S1", name="Sub 1 GmbH", jurisdiction_code="DE"),
        _entity("S2", name="Sub 2 GmbH", jurisdiction_code="DE"),
        _entity("S3", name="Sub 3 GmbH", jurisdiction_code="DE"),
        _rel("R1", "S1", "E1"),  # E1 owns S1
        _rel("R2", "S2", "S1"),
        _rel("R3", "S3", "S2"),
    ]
    codes = {s.code for s in assess_amla("gleif", {"entity_id": "SUBJ"}, bods)}
    assert COMPLEX_OWNERSHIP_LAYERS not in codes


def test_layers_never_counts_a_chain_the_subject_is_not_on() -> None:
    """Production regression, Shell plc, 2026-09-05.

    The DFS started from every entity node and kept the longest path found
    anywhere in the bundle, so a chain among parties that never touch the
    subject satisfied Article 12(1) on the subject's behalf. Shell plc's chip
    — and its verdict sentence, "over an ownership chain 3 layers deep" — rested
    on ``BlackRock -> Royal Dutch Shell plc -> Shell Midstream Operating LLC``,
    three nodes none of which is the looked-up Shell plc.

    The shape is reproduced here: the subject has one owner, and a separate
    three-node chain sits elsewhere in the same bundle.
    """
    bods = [
        _entity("E1", name="Shell plc", identifier="SUBJ", jurisdiction_code="GB"),
        _entity("E2", name="Direct owner Ltd", jurisdiction_code="GB"),
        _entity("X1", name="BlackRock", jurisdiction_code="US"),
        _entity("X2", name="Royal Dutch Shell plc", jurisdiction_code="NL"),
        _entity("X3", name="Shell Midstream Operating LLC", jurisdiction_code="US"),
        _rel("R1", "E1", "E2"),   # subject's own chain: two layers
        _rel("R2", "X2", "X1"),   # side chain, three nodes, no subject on it
        _rel("R3", "X3", "X2"),
    ]
    codes = {s.code for s in assess_amla("opensanctions", {"entity_id": "SUBJ"}, bods)}
    assert COMPLEX_OWNERSHIP_LAYERS not in codes
    # And because conditions (a)/(b) are scoped to that path, the composite
    # cannot be built out of side-branch entities either — the US entities
    # above are exactly what fed condition (b) on the live Shell report.
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


def test_layers_path_starts_at_the_subject_and_runs_upwards() -> None:
    signals = assess_amla(
        "companies_house", {"entity_id": "E1"}, _three_layer_chain()
    )
    layers = next(s for s in signals if s.code == COMPLEX_OWNERSHIP_LAYERS)
    assert layers.evidence["longest_path"] == ["E1", "E2", "E3"]
    assert layers.evidence["subject_statement_id"] == "E1"


def test_layers_reports_whether_the_chain_reached_a_beneficial_owner() -> None:
    """The count is a lower bound, so no person terminus is required — but
    whether one was found is recorded, so a truncated chain and a completed
    one are distinguishable. GLEIF and OpenCorporates bundles carry no person
    statements at all; requiring a BO would silence the signal on both."""
    truncated = assess_amla(
        "gleif", {"entity_id": "E1"}, _three_layer_chain()
    )
    sig = next(s for s in truncated if s.code == COMPLEX_OWNERSHIP_LAYERS)
    assert sig.evidence["reaches_beneficial_owner"] is False

    completed = _three_layer_chain() + [
        _person("P1"),
        _rel("R3", "E3", "P1", ip_kind="person"),
    ]
    sig2 = next(
        s
        for s in assess_amla("companies_house", {"entity_id": "E1"}, completed)
        if s.code == COMPLEX_OWNERSHIP_LAYERS
    )
    assert sig2.evidence["reaches_beneficial_owner"] is True


def _layers(signals):
    return next(s for s in signals if s.code == COMPLEX_OWNERSHIP_LAYERS)


def _elements(sig) -> dict[str, dict]:
    return {e["element"]: e for e in sig.evidence["complexity_elements"]}


def test_retired_composite_is_never_emitted() -> None:
    """The consultation-draft composite is gone: three layers plus a trust
    and a high-risk jurisdiction — every old trigger at once — still raises
    no COMPLEX_CORPORATE_STRUCTURE. The final draft RTS sets no threshold,
    so no chip may say a structure "meets" one."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG", "name": "BVI"}
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods.append(_entity("E9", name="Nominee Holdings"))
    bods.append(_rel("R9", "E9", "E1", interests=[
        {"type": "otherInfluenceOrControl", "directOrIndirect": "direct",
         "details": "registered owner as nominee"},
    ]))
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert COMPLEX_CORPORATE_STRUCTURE not in codes
    assert COMPLEX_CORPORATE_STRUCTURE in RETIRED_SIGNAL_CODES
    assert not any("meets" in s.summary.lower() for s in signals)
    assert not any("threshold" in s.summary.lower() for s in signals)
    assert set(_elements(_layers(signals))) == {
        "arrangement", "high_risk_jurisdiction", "nominee"
    }


def test_assess_amla_is_an_alias_for_assess_structure() -> None:
    assert assess_amla is assess_structure


def test_layers_reports_intermediate_layers_in_its_sentence() -> None:
    """The final draft counts *intermediate* layers; every entity above the
    subject is one. ``layers`` keeps counting the subject (the depth
    resolver, graph_shape and the verdict read it)."""
    sig = _layers(assess_amla("companies_house", {"entity_id": "E1"}, _three_layer_chain()))
    assert sig.evidence["layers"] == 3
    assert sig.evidence["intermediate_layers"] == 2
    assert "2 intermediate corporate layers" in sig.summary
    assert "AMLA" not in sig.summary
    assert "not a finding in itself" in sig.summary
    assert sig.evidence["complexity_elements"] == []


def test_non_eu_layer_is_not_a_complexity_element() -> None:
    """Panama is outside the EU but on neither FATF list nor the EU list.
    The consultation draft's "outside the EU" condition is gone: the
    context note still fires, but no element is recorded."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "PA", "name": "Panama"}
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert NON_EU_JURISDICTION in {s.code for s in signals}
    assert _layers(signals).evidence["complexity_elements"] == []


def test_high_risk_jurisdiction_element_names_its_lists() -> None:
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG", "name": "British Virgin Islands"}
    sig = _layers(assess_amla("companies_house", {"entity_id": "E1"}, bods))
    el = _elements(sig)["high_risk_jurisdiction"]
    assert el["jurisdictions"] == [
        {"statement_id": "E3", "code": "VG", "name": "British Virgin Islands",
         "lists": ["eu", "fatf_grey"]}
    ]
    assert "high-risk jurisdiction (VG)" in sig.summary


def test_high_risk_element_counts_the_subject_itself() -> None:
    """Art 11(4)(c): "the customer OR any legal entities present at any of
    these layers". Unlike NON_EU_JURISDICTION, the subject's own
    jurisdiction counts here."""
    bods = _three_layer_chain()
    bods[0]["recordDetails"]["jurisdiction"] = {"code": "IR", "name": "Iran"}
    sig = _layers(assess_amla("companies_house", {"entity_id": "E1"}, bods))
    el = _elements(sig)["high_risk_jurisdiction"]
    assert [j["statement_id"] for j in el["jurisdictions"]] == ["E1"]
    assert el["jurisdictions"][0]["lists"] == ["eu", "fatf_black"]


def test_high_risk_element_is_scoped_to_the_layered_path() -> None:
    """A Russian entity with no edges is on no layer. The standalone EU
    signal still reports it (bundle-wide); the element does not."""
    bods = _three_layer_chain()
    bods.append(_entity("E9", name="Unrelated OOO", jurisdiction_code="RU"))
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert EU_HIGH_RISK_THIRD_COUNTRY in {s.code for s in signals}
    assert "high_risk_jurisdiction" not in _elements(_layers(signals))


def test_high_risk_lists_are_configurable(monkeypatch) -> None:
    """OPENCHECK_HIGH_RISK_JURISDICTION_LISTS=eu narrows the element to the
    EU list: Bulgaria (FATF grey, not EU-listed) stops counting, the BVI
    (on both) still counts, and the standalone FATF signal is untouched."""
    monkeypatch.setenv("OPENCHECK_HIGH_RISK_JURISDICTION_LISTS", "eu")
    get_settings.cache_clear()
    assert high_risk_lists_for("BG") == []
    assert high_risk_lists_for("VG") == ["eu"]
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "BG", "name": "Bulgaria"}
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert FATF_GREY_LIST in {s.code for s in signals}
    assert "high_risk_jurisdiction" not in _elements(_layers(signals))


def test_high_risk_lists_default_to_all_three() -> None:
    assert high_risk_lists_for("BG") == ["fatf_grey"]
    assert high_risk_lists_for("RU") == ["eu"]
    assert high_risk_lists_for("KP") == ["eu", "fatf_black"]
    assert high_risk_lists_for("DE") == []


def test_trust_element_is_scoped_to_the_layered_path() -> None:
    """Art 11(4)(b) says "in any of those layers". A trust on no layer
    raises the standalone signal but is not an element of the chain."""
    bods = _three_layer_chain()
    bods.append(_entity("E9", entity_type="arrangement", name="Unrelated Family Trust"))
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert TRUST_OR_ARRANGEMENT in {s.code for s in signals}
    assert "arrangement" not in _elements(_layers(signals))


def test_trust_on_the_path_is_an_element() -> None:
    bods = _three_layer_chain()
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    sig = _layers(assess_amla("companies_house", {"entity_id": "E1"}, bods))
    assert _elements(sig)["arrangement"]["statement_ids"] == ["E2"]
    assert "a trust or arrangement on the chain" in sig.summary


def test_nominee_element_stays_bundle_wide() -> None:
    """Art 11(4)(d) reads "involvement … in the structure", looser than
    "in any of those layers" — deliberately NOT path-scoped. Pins the
    asymmetry so nobody "tidies" it into the path-scoped pair."""
    bods = _three_layer_chain()
    bods.append(_entity("E9", name="Nominee Holdings"))
    bods.append(
        _rel("R9", "E9", "E1", interests=[
            {"type": "otherInfluenceOrControl",
             "directOrIndirect": "direct",
             "details": "registered owner as nominee"},
        ])
    )
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert NOMINEE in {s.code for s in signals}
    el = _elements(_layers(signals))["nominee"]
    assert el["scope"] == "structure"
    assert "nominee arrangements in the structure" in _layers(signals).summary


def test_elements_do_not_change_confidence() -> None:
    """No threshold, no escalation: the layers chip stays medium however
    many elements sit on the chain."""
    bods = _three_layer_chain()
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "KP"}
    assert _layers(assess_amla("companies_house", {"entity_id": "E1"}, bods)).confidence == "medium"


# ---------------------------------------------------------------------
# POSSIBLE_OBFUSCATION advisory — opacity with layering (Phase 272)
# ---------------------------------------------------------------------


def _anonymous_person() -> dict:
    return {
        "statementId": "P1",
        "recordType": "person",
        "recordDetails": {
            "personType": "anonymousPerson",
            "names": [{"type": "individual", "fullName": "Withheld"}],
        },
    }


def test_possible_obfuscation_fires_on_opacity_with_layering() -> None:
    """Opacity (a deliberately withheld party) on a layered chain — no
    non-EU entity, no nominee needed any more. Reframed as an EDD prompt."""
    bods = _three_layer_chain() + [_anonymous_person()]
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert OPAQUE_OWNERSHIP in codes
    assert POSSIBLE_OBFUSCATION in codes
    advisory = next(s for s in signals if s.code == POSSIBLE_OBFUSCATION)
    assert advisory.confidence == "low"
    assert "legitimate economic, legal or other rationale" in advisory.summary
    assert "enhanced due diligence" in advisory.summary
    assert "AMLA" not in advisory.summary
    assert advisory.evidence["triggered_by"] == [
        COMPLEX_OWNERSHIP_LAYERS, OPAQUE_OWNERSHIP
    ]


def test_possible_obfuscation_does_not_fire_without_opacity() -> None:
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "PA", "name": "Panama"}
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    assert POSSIBLE_OBFUSCATION not in {s.code for s in signals}


def test_possible_obfuscation_does_not_fire_without_layers() -> None:
    """Opacity plus a non-EU owner used to be enough when paired with
    layers; without layers it never was, and non-EU no longer counts."""
    bods = _three_layer_chain()[:4] + [_anonymous_person()]  # two entities only
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    assert COMPLEX_OWNERSHIP_LAYERS not in {s.code for s in signals}
    assert POSSIBLE_OBFUSCATION not in {s.code for s in signals}


# ---------------------------------------------------------------------
# Empty / non-BODS inputs
# ---------------------------------------------------------------------


def test_assess_amla_returns_empty_for_empty_bundle() -> None:
    assert assess_amla("companies_house", {"entity_id": "X"}, []) == []


def test_assess_bundle_returns_structural_signals_inline() -> None:
    """End-to-end: assess_bundle exposes the structural signals too."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG"}
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[1]["recordDetails"]["name"] = "The Doe Family Trust"
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert {
        COMPLEX_OWNERSHIP_LAYERS,
        NON_EU_JURISDICTION,
        TRUST_OR_ARRANGEMENT,
        EU_HIGH_RISK_THIRD_COUNTRY,
    }.issubset(codes)
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


# ---------------------------------------------------------------------
# Env-var overrides for the EU+EEA jurisdiction set
# ---------------------------------------------------------------------


def test_eu_eea_codes_defaults_match_constant() -> None:
    """No env vars set → resolver returns the documented default."""
    assert _eu_eea_codes() == DEFAULT_EU_EEA_COUNTRY_CODES
    # Back-compat alias still points at the defaults.
    assert EU_EEA_COUNTRY_CODES == DEFAULT_EU_EEA_COUNTRY_CODES


def test_equivalent_jurisdictions_env_adds_codes(monkeypatch) -> None:
    """OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS=GB,CH should additively
    suppress the non-EU signal for those codes without losing the EU+EEA
    defaults."""
    monkeypatch.setenv("OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS", "GB, CH")
    get_settings.cache_clear()

    codes = _eu_eea_codes()
    assert "GB" in codes
    assert "CH" in codes
    assert "DE" in codes  # default EU still present
    assert "NO" in codes  # default EEA still present

    # And the rule honours it: a UK-only chain no longer fires non-EU.
    bods = [_entity("E1", jurisdiction_code="GB")]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
    assert NON_EU_JURISDICTION not in {s.code for s in signals}


def test_eu_eea_override_env_replaces_default(monkeypatch) -> None:
    """OPENCHECK_AMLA_EU_EEA_OVERRIDE replaces the entire set — useful
    for strict AMLA EU-only mode (no EEA)."""
    monkeypatch.setenv("OPENCHECK_AMLA_EU_EEA_OVERRIDE", "DE, FR, IT")
    get_settings.cache_clear()

    codes = _eu_eea_codes()
    assert codes == frozenset({"DE", "FR", "IT"})

    # NO (Norway) is in the EEA default but excluded under the override
    # → should now fire the non-EU signal.
    bods = [
        _entity("E1", jurisdiction_code="DE"),
        _entity("E2", jurisdiction_code="NO", jurisdiction_name="Norway"),
        _rel("R1", "E1", "E2"),
    ]
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert NON_EU_JURISDICTION in {s.code for s in signals}


def test_eu_eea_override_takes_precedence_over_extras(monkeypatch) -> None:
    """If both vars are set, override wins and extras are ignored."""
    monkeypatch.setenv("OPENCHECK_AMLA_EU_EEA_OVERRIDE", "DE")
    monkeypatch.setenv("OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS", "GB,CH")
    get_settings.cache_clear()

    codes = _eu_eea_codes()
    assert codes == frozenset({"DE"})


def test_equivalent_jurisdictions_handles_lower_case_and_whitespace(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS", " gb ,  ch ")
    get_settings.cache_clear()
    codes = _eu_eea_codes()
    assert "GB" in codes
    assert "CH" in codes
