"""AMLA CDD RTS risk-signal tests.

These mirror the objective conditions of AMLA's draft CDD RTS for
"complex corporate structures":

  (a) trust or legal arrangement in any layer
  (b) jurisdictions outside the EU/EEA
  (c) nominee shareholders/directors anywhere

Plus the threshold rule: ≥3 layers + **≥2** of (a)/(b)/(c) → complex
corporate structure. Article 12(1) says "more than one of the following
conditions", i.e. at least two; condition (b) is scoped to the layered
path.

Plus the two list-based jurisdiction RISK signals (FATF, EU Article 29)
and the demotion of NON_EU_JURISDICTION to kind="context".

Plus the subjective ``POSSIBLE_OBFUSCATION`` advisory signal.

Plus operator-tunable jurisdiction list via ``OPENCHECK_AMLA_*`` env vars.
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
    TRUST_OR_ARRANGEMENT,
    _eu_eea_codes,
    assess_amla,
    assess_bundle,
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
    assert "AMLA" in sig.summary
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
    assert "AMLA" in sig.summary


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
# Layered ownership + composite COMPLEX_CORPORATE_STRUCTURE
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


def test_complex_corporate_structure_needs_two_conditions_not_one() -> None:
    """Article 12(1) requires "MORE THAN ONE" condition — i.e. ≥2.

    Three layers plus a single non-EU layer is one condition, so the
    composite must NOT fire. This is the central proportionality fix: a
    layered group that merely reaches outside the EU is not, on the RTS's
    own terms, a complex corporate structure.
    """
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {
        "code": "VG",
        "name": "British Virgin Islands",
    }
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert COMPLEX_OWNERSHIP_LAYERS in codes
    assert NON_EU_JURISDICTION in codes  # standalone signal still reports it
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


def test_complex_corporate_structure_fires_on_two_conditions() -> None:
    """Non-EU layer (b) PLUS a trust layer (a) = two conditions → fires."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {
        "code": "VG",
        "name": "British Virgin Islands",
    }
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[1]["recordDetails"]["name"] = "The Doe Family Trust"
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert COMPLEX_CORPORATE_STRUCTURE in codes
    composite = next(s for s in signals if s.code == COMPLEX_CORPORATE_STRUCTURE)
    assert set(composite.evidence["triggers"]) == {
        "trust/arrangement",
        "non-EU jurisdiction",
    }
    assert composite.evidence["layers"] == 3


def test_complex_corporate_structure_does_not_fire_without_aggravator() -> None:
    """Three layers, all EU, no trust, no nominee — not "complex" per AMLA."""
    signals = assess_amla(
        "companies_house", {"entity_id": "E1"}, _three_layer_chain()
    )
    codes = {s.code for s in signals}
    assert COMPLEX_OWNERSHIP_LAYERS in codes
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


def test_complex_corporate_structure_does_not_fire_on_trust_alone() -> None:
    """A trust layer on its own is also only one condition."""
    bods = _three_layer_chain()
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[1]["recordDetails"]["name"] = "The Doe Family Trust"
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    assert COMPLEX_CORPORATE_STRUCTURE not in {s.code for s in signals}


def test_trust_condition_is_scoped_to_the_layered_path() -> None:
    """Article 12(1)(a) says "in any of the LAYERS", like point (b).

    A trust on a side branch still raises the standalone
    TRUST_OR_ARRANGEMENT signal, but must not count towards the Article 12
    composite — even alongside a genuine second condition on the path.
    """
    bods = _three_layer_chain()
    # Condition (b) genuinely met ON the path.
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG", "name": "BVI"}
    # A trust with NO relationship edges — not on any layer.
    bods.append(
        _entity("E9", entity_type="arrangement", name="Unrelated Family Trust")
    )
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert TRUST_OR_ARRANGEMENT in codes  # bundle-wide signal still fires
    # …but only condition (b) is on the path, so one condition, no composite.
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


def test_trust_on_the_path_does_count_towards_the_composite() -> None:
    """Control: the same trust, this time on a layer, tips it to two."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG", "name": "BVI"}
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    composite = next(
        s for s in signals if s.code == COMPLEX_CORPORATE_STRUCTURE
    )
    assert set(composite.evidence["triggers"]) == {
        "trust/arrangement",
        "non-EU jurisdiction",
    }


def test_nominee_condition_stays_bundle_wide() -> None:
    """Point (c) reads "involved in the structure", not "in any of these
    layers" — deliberately looser, so it is NOT path-scoped."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG", "name": "BVI"}
    # A nominee relationship hanging off the bundle, not on the main chain.
    bods.append(_entity("E9", name="Nominee Holdings"))
    bods.append(
        _rel("R9", "E9", "E1", interests=[
            {"type": "otherInfluenceOrControl",
             "directOrIndirect": "direct",
             "details": "registered owner as nominee"},
        ])
    )
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert NOMINEE in codes
    assert COMPLEX_CORPORATE_STRUCTURE in codes


def test_non_eu_condition_is_scoped_to_the_layered_path() -> None:
    """Article 12(1)(b) says "present at any of THESE LAYERS".

    An off-path non-EU entity that is not part of the ownership chain
    must not count towards the Article 12 composite — even alongside a
    genuine second condition. Since Phase 153 the standalone
    NON_EU_JURISDICTION signal walks the chain above the subject too, so
    an entity with no edges at all raises nothing anywhere.
    """
    bods = _three_layer_chain()
    # A trust ON the path — condition (a) is genuinely met.
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[1]["recordDetails"]["name"] = "The Doe Family Trust"
    # A non-EU entity with NO relationship edges — not on any layer.
    bods.append(
        _entity("E9", name="Unrelated Panama SA", jurisdiction_code="PA")
    )
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    # The Panama entity is on no chain, so neither the standalone signal…
    assert NON_EU_JURISDICTION not in codes
    # …nor the composite sees it: only condition (a) is met.
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


# ---------------------------------------------------------------------
# Subjective POSSIBLE_OBFUSCATION advisory
# ---------------------------------------------------------------------


def test_possible_obfuscation_fires_with_opacity_and_layered_concern() -> None:
    """The advisory still catches what the hard composite now declines to.

    Layers + a single non-EU layer is only ONE Article 12 condition, so
    COMPLEX_CORPORATE_STRUCTURE must not fire — but combined with an
    anonymousPerson (identity deliberately withheld, e.g. a super-secure
    PSC) this is still worth surfacing for human review, which is exactly
    what the low-confidence advisory is for.
    """
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {
        "code": "PA",
        "name": "Panama",
    }
    # …and an anonymousPerson at the bottom of the chain so opacity fires
    # (unknownPerson no longer does — unknown-to-this-source is not the
    # same claim as deliberately-withheld).
    bods.append(
        {
            "statementId": "P1",
            "recordType": "person",
            "recordDetails": {
                "personType": "anonymousPerson",
                "names": [{"type": "individual", "fullName": "Withheld"}],
            },
        }
    )
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert OPAQUE_OWNERSHIP in codes
    assert COMPLEX_CORPORATE_STRUCTURE not in codes
    assert POSSIBLE_OBFUSCATION in codes
    advisory = next(s for s in signals if s.code == POSSIBLE_OBFUSCATION)
    assert advisory.confidence == "low"
    assert "legitimate economic rationale" in advisory.summary


def test_possible_obfuscation_does_not_fire_without_opacity() -> None:
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {
        "code": "PA",
        "name": "Panama",
    }
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    assert POSSIBLE_OBFUSCATION not in {s.code for s in signals}


# ---------------------------------------------------------------------
# Empty / non-BODS inputs
# ---------------------------------------------------------------------


def test_assess_amla_returns_empty_for_empty_bundle() -> None:
    assert assess_amla("companies_house", {"entity_id": "X"}, []) == []


def test_assess_bundle_returns_amla_signals_inline() -> None:
    """End-to-end: assess_bundle should expose the AMLA signals too."""
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["jurisdiction"] = {"code": "VG"}
    # Second condition so the composite genuinely fires end-to-end.
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[1]["recordDetails"]["name"] = "The Doe Family Trust"
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert {
        COMPLEX_OWNERSHIP_LAYERS,
        NON_EU_JURISDICTION,
        COMPLEX_CORPORATE_STRUCTURE,
    }.issubset(codes)


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
