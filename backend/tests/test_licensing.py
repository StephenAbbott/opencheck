"""Tests for the licensing compatibility matrix (opencheck.licensing)."""

from __future__ import annotations

from opencheck.licensing import assess, classify, full_matrix, source_licensing


# --- classify ---------------------------------------------------------------


def test_classify_public_domain_is_commercial_no_attribution() -> None:
    t = classify("CC0-1.0")
    assert t.commercial_use == "yes"
    assert t.attribution_required is False
    assert t.share_alike is False
    assert t.color == "green"


def test_classify_ogl_is_commercial_with_attribution() -> None:
    t = classify("OGL-3.0")
    assert t.commercial_use == "yes"
    assert t.attribution_required is True
    assert t.color == "green"


def test_classify_non_commercial() -> None:
    t = classify("CC-BY-NC-4.0")
    assert t.commercial_use == "no"
    assert t.share_alike is False
    assert t.color == "amber"  # per-source colour; the bundle verdict goes red


def test_classify_non_commercial_share_alike() -> None:
    t = classify("CC-BY-NC-SA-4.0")
    assert t.commercial_use == "no"
    assert t.share_alike is True


def test_classify_descriptive_open_gov_string_is_permissive() -> None:
    # Adapters declare some licences as prose, not codes.
    assert classify("Danish Open Government Data (CVR brugervilkår)").commercial_use == "yes"
    assert classify("Open Government Data (PSI Directive)").commercial_use == "yes"
    assert classify("SE-PSI").commercial_use == "yes"


def test_classify_bespoke_and_unknown_are_conditional() -> None:
    assert classify("OC-Terms").commercial_use == "conditional"
    assert classify("per-collection").commercial_use == "conditional"
    assert classify("Totally Made Up Licence").commercial_use == "conditional"
    assert classify(None).commercial_use == "conditional"


# --- assess (uses the live REGISTRY) ----------------------------------------


def test_assess_all_permissive_is_green() -> None:
    a = assess(["gleif", "companies_house"])
    assert a.commercial_use == "yes"
    assert a.color == "green"
    assert a.attribution_required is True  # companies_house OGL needs attribution
    assert {s.source_id for s in a.per_source} == {"gleif", "companies_house"}


def test_assess_with_non_commercial_source_is_red() -> None:
    a = assess(["gleif", "opensanctions"])
    assert a.commercial_use == "no"
    assert a.color == "red"
    assert any("commercial" in w.lower() for w in a.warnings)


def test_assess_share_alike_source() -> None:
    a = assess(["opencorporates"])  # OC-Terms: conditional + share-alike (registered)
    assert a.share_alike is True
    assert a.color == "amber"
    assert any("share-alike" in w.lower() for w in a.warnings)


def test_classify_cc_by_nc_sa_is_share_alike_non_commercial() -> None:
    t = classify("CC-BY-NC-SA-4.0")
    assert t.commercial_use == "no"
    assert t.share_alike is True


def test_assess_conditional_source_is_amber() -> None:
    a = assess(["opencorporates"])  # OC-Terms
    assert a.commercial_use == "conditional"
    assert a.color == "amber"


def test_assess_ignores_unknown_source_ids() -> None:
    a = assess(["gleif", "not_a_real_source"])
    assert {s.source_id for s in a.per_source} == {"gleif"}


# --- full_matrix ------------------------------------------------------------


def test_full_matrix_covers_all_registered_sources() -> None:
    from opencheck.sources import REGISTRY

    matrix = full_matrix()
    assert matrix["disclaimer"]
    assert len(matrix["sources"]) == len(REGISTRY)
    # Every source resolves to terms with a colour and a commercial verdict.
    for s in matrix["sources"]:
        assert s["terms"]["color"] in {"green", "amber", "red"}
        assert s["terms"]["commercial_use"] in {"yes", "no", "conditional"}
    assert matrix["licenses"]  # distinct licence catalogue is non-empty


def test_source_licensing_unknown_returns_none() -> None:
    assert source_licensing("not_a_real_source") is None
