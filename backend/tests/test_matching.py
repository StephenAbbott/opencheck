"""Tests for the shared cross-source matching guards (ftmg-derived)."""

from __future__ import annotations

from opencheck.matching import (
    MIN_IDENTIFIER_LEN,
    canonical_identifier,
    canonical_url,
    is_matchable_name,
)


def test_canonical_identifier_strips_separators_and_upcases() -> None:
    # Same registration number written three ways → one canonical form.
    forms = ["556056-6258", "5560566258", "556 056 6258"]
    canon = {canonical_identifier(f) for f in forms}
    assert canon == {"5560566258"}


def test_canonical_identifier_drops_short_values() -> None:
    # Shorter than MIN_IDENTIFIER_LEN after normalisation → not corroboratable.
    assert canonical_identifier("1234") is None
    assert canonical_identifier("AB-12") is None
    assert MIN_IDENTIFIER_LEN == 7


def test_canonical_identifier_min_len_override_keeps_short_canonical_ids() -> None:
    # Wikidata QIDs are legitimately short and globally unique.
    assert canonical_identifier("Q9545", min_len=0) == "Q9545"


def test_canonical_identifier_handles_empty() -> None:
    assert canonical_identifier(None) is None
    assert canonical_identifier("") is None
    assert canonical_identifier("   ") is None


def test_canonical_url_equates_trailing_slash_and_scheme() -> None:
    a = canonical_url("https://OpenCorporates.com/companies/gb/12345678/")
    b = canonical_url("http://opencorporates.com/companies/gb/12345678")
    assert a is not None and a == b


def test_canonical_url_handles_empty() -> None:
    assert canonical_url(None) is None
    assert canonical_url("") is None


def test_is_matchable_name_rejects_single_token() -> None:
    assert is_matchable_name("vladimir putin") is True
    assert is_matchable_name("ivanov") is False
    assert is_matchable_name("") is False
    assert is_matchable_name(None) is False
    assert is_matchable_name("   ") is False


# --- Fallback path (base install without the ftm extra / rigour) -----------


def test_fallback_identifier_matches_rigour_for_registry_codes(monkeypatch) -> None:
    import opencheck.matching as m

    monkeypatch.setattr(m, "_HAS_RIGOUR", False)
    assert m.canonical_identifier("556056-6258") == "5560566258"
    assert m.canonical_identifier("GB-12 345 678") == "GB12345678"
    assert m.canonical_identifier("1234") is None
    assert m.canonical_identifier("Q9545", min_len=0) == "Q9545"


def test_fallback_url_equates_scheme_and_trailing_slash(monkeypatch) -> None:
    import opencheck.matching as m

    monkeypatch.setattr(m, "_HAS_RIGOUR", False)
    a = m.canonical_url("https://opencorporates.com/companies/gb/12345678")
    b = m.canonical_url("http://opencorporates.com/companies/gb/12345678/")
    assert a is not None and a == b
