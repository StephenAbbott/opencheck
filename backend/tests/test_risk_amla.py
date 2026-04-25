"""AMLA CDD RTS risk-signal tests.

These mirror the objective conditions of AMLA's draft CDD RTS for
"complex corporate structures":

  (a) trust or legal arrangement in any layer
  (b) jurisdictions outside the EU/EEA
  (c) nominee shareholders/directors anywhere

Plus the threshold rule: ≥3 layers + ≥1 of (a)/(b)/(c) → complex
corporate structure.

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
            name: str = "Acme") -> dict:
    rd: dict = {
        "entityType": {"type": entity_type},
        "name": name,
    }
    if jurisdiction_code:
        rd["incorporatedInJurisdiction"] = {
            "code": jurisdiction_code,
            "name": jurisdiction_name or jurisdiction_code,
        }
    if legal_form:
        rd["legalForm"] = legal_form
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
    ip_key = "describedByEntityStatement" if ip_kind == "entity" else "describedByPersonStatement"
    return {
        "statementId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "subject": {"describedByEntityStatement": subject},
            "interestedParty": {ip_key: ip},
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


# ---------------------------------------------------------------------
# (b) Non-EU / EEA jurisdiction
# ---------------------------------------------------------------------


def test_non_eu_jurisdiction_fires_for_panama() -> None:
    bods = [_entity("E1", jurisdiction_code="PA", jurisdiction_name="Panama")]
    signals = assess_amla("openaleph", {"entity_id": "X"}, bods)
    sig = next(s for s in signals if s.code == NON_EU_JURISDICTION)
    assert sig.confidence == "high"
    assert "PA" in sig.summary
    assert sig.evidence["jurisdictions"][0]["code"] == "PA"


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
        _entity("E1", jurisdiction_code="VG", jurisdiction_name="British Virgin Islands"),
        _entity("E2", jurisdiction_code="KY", jurisdiction_name="Cayman Islands"),
        _entity("E3", jurisdiction_code="DE"),  # EU — ignored in summary
    ]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
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
    assert sig.confidence == "high"
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


def test_complex_corporate_structure_fires_when_layered_plus_non_eu() -> None:
    bods = _three_layer_chain()
    # Tag the topmost holding with a non-EU jurisdiction.
    bods[2]["recordDetails"]["incorporatedInJurisdiction"] = {
        "code": "VG",
        "name": "British Virgin Islands",
    }
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert COMPLEX_OWNERSHIP_LAYERS in codes
    assert NON_EU_JURISDICTION in codes
    assert COMPLEX_CORPORATE_STRUCTURE in codes
    composite = next(s for s in signals if s.code == COMPLEX_CORPORATE_STRUCTURE)
    assert "non-EU jurisdiction" in composite.evidence["triggers"]
    assert composite.evidence["layers"] == 3


def test_complex_corporate_structure_does_not_fire_without_aggravator() -> None:
    """Three layers, all UK, no trust, no nominee — not "complex" per AMLA."""
    signals = assess_amla(
        "companies_house", {"entity_id": "E1"}, _three_layer_chain()
    )
    codes = {s.code for s in signals}
    assert COMPLEX_OWNERSHIP_LAYERS in codes
    assert COMPLEX_CORPORATE_STRUCTURE not in codes


def test_complex_corporate_structure_fires_with_trust_layer() -> None:
    bods = _three_layer_chain()
    bods[1]["recordDetails"]["entityType"] = {"type": "arrangement"}
    bods[1]["recordDetails"]["name"] = "The Doe Family Trust"
    signals = assess_amla("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert COMPLEX_CORPORATE_STRUCTURE in codes
    composite = next(s for s in signals if s.code == COMPLEX_CORPORATE_STRUCTURE)
    assert "trust/arrangement" in composite.evidence["triggers"]


# ---------------------------------------------------------------------
# Subjective POSSIBLE_OBFUSCATION advisory
# ---------------------------------------------------------------------


def test_possible_obfuscation_fires_with_opacity_and_layered_concern() -> None:
    bods = _three_layer_chain()
    # Add a non-EU layer so the composite signal fires…
    bods[2]["recordDetails"]["incorporatedInJurisdiction"] = {
        "code": "PA",
        "name": "Panama",
    }
    # …and an unknownPerson at the bottom of the chain so opacity fires.
    bods.append(
        {
            "statementId": "P1",
            "recordType": "person",
            "recordDetails": {
                "personType": "unknownPerson",
                "names": [{"type": "individual", "fullName": "Unknown"}],
            },
        }
    )
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert OPAQUE_OWNERSHIP in codes
    assert COMPLEX_CORPORATE_STRUCTURE in codes
    assert POSSIBLE_OBFUSCATION in codes
    advisory = next(s for s in signals if s.code == POSSIBLE_OBFUSCATION)
    assert advisory.confidence == "low"
    assert "legitimate economic rationale" in advisory.summary


def test_possible_obfuscation_does_not_fire_without_opacity() -> None:
    bods = _three_layer_chain()
    bods[2]["recordDetails"]["incorporatedInJurisdiction"] = {
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
    bods[2]["recordDetails"]["incorporatedInJurisdiction"] = {"code": "VG"}
    signals = assess_bundle("companies_house", {"entity_id": "E1"}, bods)
    codes = {s.code for s in signals}
    assert {COMPLEX_OWNERSHIP_LAYERS, NON_EU_JURISDICTION, COMPLEX_CORPORATE_STRUCTURE}.issubset(codes)


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
    bods = [_entity("E1", jurisdiction_code="NO", jurisdiction_name="Norway")]
    signals = assess_amla("companies_house", {"entity_id": "X"}, bods)
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
