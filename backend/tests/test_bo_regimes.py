"""Phase B (BOC audit) — the per-jurisdiction BO regimes registry is coherent.

These tests pin the SHAPE of the registry, not the legal content: the legal
content is draft until manually verified against current legislation, and the
`review_status` field says so. What must always hold:

* every source allowed to assert beneficialOwnershipOrControl by the Phase 102
  policy has a regime entry explaining under WHICH definition it asserts;
* no policy value strays outside the closed vocabulary;
* threshold operator/value travel together and match the quoted wording;
* the conservative default for anything unknown is "omit".
"""

from __future__ import annotations

from opencheck.bods.bo_regimes import REGIMES, BORegime, boc_policy, get_regime
from opencheck.bods.mapper import _BO_ASSERTING_SOURCES

_ALLOWED_POLICIES = {
    "assert_true", "assert_false", "omit", "copy_verbatim", "per_statement_code",
}


def test_every_bo_asserting_source_has_a_regime():
    # gleif's asserting path is the bods_gleif parquet source; the native
    # gleif adapter never asserts. Everything else maps 1:1.
    missing = {s for s in _BO_ASSERTING_SOURCES if s not in REGIMES}
    assert not missing, f"BO-asserting sources without a documented regime: {missing}"


def test_policies_use_closed_vocabulary():
    for regime in REGIMES.values():
        for kind, policy in regime.record_kinds.items():
            assert policy in _ALLOWED_POLICIES, (regime.source_id, kind, policy)


def test_threshold_fields_travel_together():
    for regime in REGIMES.values():
        assert (regime.threshold_operator is None) == (regime.threshold_value is None)
        if regime.threshold_operator is not None:
            assert regime.threshold_operator in (">", ">=")
            assert regime.threshold_wording, regime.source_id


def test_threshold_operator_matches_wording():
    """'more than' must be '>', 'at least' must be '>='. Guards transcription."""
    for regime in REGIMES.values():
        wording = (regime.threshold_wording or "").lower()
        if "more than" in wording:
            assert regime.threshold_operator == ">", regime.source_id
        if "at least" in wording or "najmenej" in wording:
            assert regime.threshold_operator == ">=", regime.source_id


def test_assert_true_only_under_a_stated_definition():
    """A regime may assert true only if it states the definition it applies."""
    for regime in REGIMES.values():
        if any(p == "assert_true" for p in regime.record_kinds.values()):
            assert regime.legal_basis or regime.regime_kind == "securities_disclosure", (
                f"{regime.source_id} asserts true without a documented legal basis"
            )
            assert regime.bo_definition


def test_unknown_source_and_kind_default_to_omit():
    assert boc_policy("some_new_adapter", "anything") == "omit"
    assert boc_policy("ur_latvia", "unmapped_kind") == "omit"
    assert get_regime("opencorporates") is None


def test_entries_carry_review_metadata():
    for regime in REGIMES.values():
        assert regime.last_verified >= "2026-08-28"
        assert regime.review_status in ("draft", "verified")


def test_record_kind_routing_through_the_helper():
    """Phase C: set_beneficial_ownership(record_kind=...) consults the regime
    registry, fixing the source-level/record-level conflation (finding 7):
    an Estonian SHAREHOLDER record from a BO-asserting source must not get
    true just because the source also publishes BO declarations."""
    from opencheck.bods.mapper import set_beneficial_ownership

    assert "beneficialOwnershipOrControl" not in set_beneficial_ownership(
        {}, "ariregister", record_kind="shareholder"
    )
    assert set_beneficial_ownership(
        {}, "ariregister", record_kind="kasusaaja_bo"
    )["beneficialOwnershipOrControl"] is True
    assert "beneficialOwnershipOrControl" not in set_beneficial_ownership(
        {}, "ur_latvia", record_kind="member_shareholder"
    )
    assert set_beneficial_ownership(
        {}, "sec_edgar", record_kind="filer_entity"
    )["beneficialOwnershipOrControl"] is False
    # Unknown kinds and sources stay conservative.
    assert "beneficialOwnershipOrControl" not in set_beneficial_ownership(
        {}, "ur_latvia", record_kind="unmapped_kind"
    )
    assert "beneficialOwnershipOrControl" not in set_beneficial_ownership(
        {}, "some_new_adapter", record_kind="shareholder"
    )
    # An explicit source assertion still outranks the registry.
    assert set_beneficial_ownership(
        {}, "ariregister", asserted=False, record_kind="kasusaaja_bo"
    )["beneficialOwnershipOrControl"] is False


def test_decided_policy_changes_are_encoded():
    """The 2026-08-28 decisions: omit-not-false for LV holdings/officers and
    the Wikidata person leak; CH directors omit."""
    lv = REGIMES["ur_latvia"]
    assert lv.record_kinds["member_shareholder"] == "omit"
    assert lv.record_kinds["officer"] == "omit"
    assert REGIMES["wikidata"].record_kinds["owner_natural_person"] == "omit"
    assert REGIMES["companies_house"].record_kinds["officer_director"] == "omit"
