"""Tests for GLEIF ELF (ISO 20275) legal-form code resolution."""

from __future__ import annotations

from typing import Any

from opencheck.bods.mapper import map_gleif
from opencheck.elf import ELF_CODES, resolve_elf
from opencheck.risk import TRUST_OR_ARRANGEMENT, assess_amla


def test_resolve_known_code() -> None:
    # 2JZ4 = Swiss "Foundation"/"Stiftung" (GLEIF's own legal form). Ships in the
    # supplement even though the auto-generated snapshot is partial.
    assert resolve_elf("2JZ4") == "Foundation"
    assert resolve_elf("2jz4") == "Foundation"  # case-insensitive


def test_resolve_unknown_and_empty() -> None:
    assert resolve_elf(None) is None
    assert resolve_elf("") is None
    assert resolve_elf("ZZZZ") is None
    # Placeholder "no separate legal form" code is intentionally not vendored.
    assert resolve_elf("9999") is None


def test_codes_are_uppercase_four_char() -> None:
    assert ELF_CODES, "expected a non-empty vendored map"
    for code in ELF_CODES:
        assert code == code.upper()
        assert len(code) == 4


def _gleif_bundle(legal_form_id: str | None) -> dict[str, Any]:
    entity: dict[str, Any] = {
        "legalName": {"name": "Global Legal Entity Identifier Foundation"},
        "jurisdiction": "CH",
    }
    if legal_form_id is not None:
        entity["legalForm"] = {"id": legal_form_id, "other": None}
    return {
        "lei": "506700GE1G29325QX363",
        "record": {
            "attributes": {
                "lei": "506700GE1G29325QX363",
                "entity": entity,
                "registration": {"lastUpdateDate": "2026-01-01T00:00:00Z"},
            }
        },
    }


def _entity(stmts: Any) -> dict[str, Any]:
    return next(s for s in stmts if s["recordType"] == "entity")


def test_map_gleif_attaches_legal_form_label() -> None:
    ent = _entity(map_gleif(_gleif_bundle("2JZ4")).statements)
    assert ent["recordDetails"]["legalFormLabel"] == "Foundation"


def test_map_gleif_no_label_for_unknown_code() -> None:
    ent = _entity(map_gleif(_gleif_bundle("ZZZZ")).statements)
    assert "legalFormLabel" not in ent["recordDetails"]


def test_map_gleif_no_label_when_no_legal_form() -> None:
    ent = _entity(map_gleif(_gleif_bundle(None)).statements)
    assert "legalFormLabel" not in ent["recordDetails"]


def test_gleif_only_foundation_fires_trust_signal() -> None:
    # The reported case: GLEIF's own LEI, resolved via ELF code (no national
    # register), must trip the trust/arrangement signal on the legal form —
    # not the "…Foundation" in its name.
    bods = map_gleif(_gleif_bundle("2JZ4")).statements
    signals = assess_amla("gleif", {"entity_id": "506700GE1G29325QX363"}, bods)
    sig = next(s for s in signals if s.code == TRUST_OR_ARRANGEMENT)
    assert sig.evidence["matches"][0]["match"] == "legalFormLabel contains 'foundation'"


# The ELF-code ticket (13 Nov 2026): the code itself, beside its label.


def test_elf_code_keeps_a_real_code_and_drops_placeholders() -> None:
    from opencheck.elf import elf_code

    assert elf_code("b6es") == "B6ES"
    assert elf_code(" H0PO ") == "H0PO"
    # A well-formed code the vendored list lacks is still GLEIF's assertion.
    assert elf_code("ZZZZ") == "ZZZZ"
    for value in (None, "", "8888", "9999", "ABC", "AB-CD", "ABCDE"):
        assert elf_code(value) is None


def test_map_gleif_carries_the_elf_code_beside_the_label() -> None:
    ent = _entity(map_gleif(_gleif_bundle("2jz4")).statements)
    assert ent["recordDetails"]["legalFormCode"] == "2JZ4"
    assert ent["recordDetails"]["legalFormLabel"] == "Foundation"


def test_map_gleif_never_puts_the_elf_code_in_identifiers() -> None:
    """An ELF code names a form many entities share; an identifier merges
    entities in the reconciler. It must never reach ``identifiers``."""
    ent = _entity(map_gleif(_gleif_bundle("2JZ4")).statements)
    ids = ent["recordDetails"].get("identifiers") or []
    assert all(i.get("id") != "2JZ4" for i in ids)
    assert all("20275" not in str(i.get("scheme") or "") for i in ids)


def test_map_gleif_no_code_for_a_placeholder_or_no_legal_form() -> None:
    for value in ("8888", "9999", None):
        ent = _entity(map_gleif(_gleif_bundle(value)).statements)
        assert "legalFormCode" not in ent["recordDetails"]


def test_two_companies_sharing_a_legal_form_are_not_identifier_linked() -> None:
    from opencheck.reconcile import _identifier_keys

    a = _entity(map_gleif(_gleif_bundle("2JZ4")).statements)
    other = _gleif_bundle("2JZ4")
    other["lei"] = other["record"]["attributes"]["lei"] = "5299000J2N45DDNE4Y28"
    b = _entity(map_gleif(other).statements)
    assert not (_identifier_keys(a) & _identifier_keys(b))
