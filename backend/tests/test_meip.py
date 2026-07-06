"""Tests for the OECD-UNSD MEIP signpost lookup."""

from __future__ import annotations

from opencheck.meip import (
    MEIP_MNE_HEADS,
    MEIP_SUBSIDIARIES,
    MEIP_URL,
    meip_lookup,
)

# Worked example: Apple (UK) Limited — a subsidiary in the MEIP register.
_APPLE_UK = "549300QKDHYRRQH2MB86"
# 3M Company — one of the 500 MNE heads.
_3M_HEAD = "LUZQVYP4VS22CLWDAR65"


def test_data_tables_load_and_are_disjoint() -> None:
    assert len(MEIP_SUBSIDIARIES) > 20000
    assert len(MEIP_MNE_HEADS) >= 400
    # A subsidiary LEI is never also an MNE-head LEI.
    assert not (set(MEIP_SUBSIDIARIES) & set(MEIP_MNE_HEADS))


def test_subsidiary_match_shape() -> None:
    m = meip_lookup(_APPLE_UK)
    assert m is not None
    assert m.mode == "subsidiary"
    assert m.name == "APPLE (UK) LIMITED"
    assert m.parent_mne == "Apple Inc"
    assert m.immediate_parent == "APPLE OPERATIONS INTERNATIONAL LIMITED"
    assert "APPLE UK LIMITED" in m.alt_names
    assert m.address and "LONDON" in m.address
    assert m.source_url == MEIP_URL


def test_subsidiary_match_is_case_insensitive() -> None:
    assert meip_lookup(_APPLE_UK.lower()) == meip_lookup(_APPLE_UK)


def test_lei_identifier_always_corroborated() -> None:
    m = meip_lookup(_APPLE_UK)
    lei_id = next(i for i in m.identifiers if i.scheme == "lei")
    assert lei_id.value == _APPLE_UK
    assert lei_id.corroborated is True


def test_corroboration_flags_matching_gleif_ids() -> None:
    # GLEIF publishes the same OpenCorporates + Capital IQ ids for this LEI.
    m = meip_lookup(
        _APPLE_UK, {"opencorporates": "gb/01591116", "capiq": "46365431"}
    )
    by_scheme = {i.scheme: i for i in m.identifiers}
    assert by_scheme["opencorporates"].corroborated is True
    assert by_scheme["capiq"].corroborated is True
    # GLEIF does not publish PermID, so it stays informational.
    assert by_scheme["permid"].corroborated is False


def test_no_corroboration_without_known_ids() -> None:
    m = meip_lookup(_APPLE_UK)
    for ident in m.identifiers:
        if ident.scheme != "lei":
            assert ident.corroborated is False


def test_corroboration_ignores_mismatched_ids() -> None:
    m = meip_lookup(_APPLE_UK, {"opencorporates": "gb/99999999", "capiq": "0"})
    by_scheme = {i.scheme: i for i in m.identifiers}
    assert by_scheme["opencorporates"].corroborated is False
    assert by_scheme["capiq"].corroborated is False


def test_mne_head_match_shape() -> None:
    m = meip_lookup(_3M_HEAD)
    assert m is not None
    assert m.mode == "mne_head"
    assert m.parent_mne == "3M Co"
    assert m.immediate_parent is None
    assert m.subsidiaries_total and m.subsidiaries_total > 0
    assert m.subsidiaries_with_lei is not None


def test_no_match_returns_none() -> None:
    assert meip_lookup("00000000000000000000") is None
    assert meip_lookup(None) is None
    assert meip_lookup("") is None
