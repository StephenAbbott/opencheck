"""Phase B of the rigour adoption plan: the shared name normaliser.

Two contracts are pinned here:

1. **Latin-script names normalise exactly as before** — the deleted
   ``cross_check``/``icij_check`` normaliser is re-implemented below as a
   reference, and every name in the committed demo corpus must produce the
   same output unless it contains a character the shared module newly folds
   (extended Latin non-decomposables, Cyrillic, Greek). Match scores and
   thresholds therefore do not move for the corpus that exists today.

2. **The new fold layers behave as designed** — Cyrillic/Greek names gain a
   Latin comparable form, ``reconcile``/``openaleph``/``nz_associations``
   gain the diacritic handling their local normalisers lacked, and Greek
   homoglyph identifiers (Cyprus ``ΗΕ``) canonicalise to the Latin key.
"""

from __future__ import annotations

import difflib
import json
import pathlib
import re
import unicodedata

from opencheck import names
from opencheck.matching import canonical_identifier

# --- Reference: the deleted cross_check/icij_check normaliser --------------

_OLD_FOLDS = {
    "ł": "l", "Ł": "L",
    "ø": "o", "Ø": "O",
    "æ": "ae", "Æ": "Ae",
    "œ": "oe", "Œ": "Oe",
    "ð": "d", "Ð": "D",
    "þ": "th", "Þ": "Th",
    "ß": "ss",
}


def _old_normalise(name: str) -> str:
    if not name:
        return ""
    folded = "".join(_OLD_FOLDS.get(c, c) for c in name)
    decomposed = unicodedata.normalize("NFKD", folded)
    ascii_only = "".join(c for c in decomposed if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", " ", ascii_only.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _newly_folded(name: str) -> bool:
    """True when the name contains a character the shared module folds and
    the old normaliser did not (extended Latin, Cyrillic, Greek, or a
    casefold-only expansion)."""
    lowered = name.casefold()
    extended = set(names._LATIN_FOLDS) - {k.casefold() for k in _OLD_FOLDS}
    if any(ch in names._SCRIPT_FOLDS for ch in lowered):
        return True
    if any(ch in extended for ch in lowered):
        return True
    # casefold vs lower differ (e.g. dotted İ)
    return lowered != name.lower()


# --- Contract 1: corpus parity ---------------------------------------------


def test_demo_corpus_latin_names_unchanged():
    demo = pathlib.Path(__file__).resolve().parents[2] / "data" / "demo"
    checked = 0
    changed: list[tuple[str, str, str]] = []
    for path in sorted(demo.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            stmt = json.loads(line)
            rd = stmt.get("recordDetails") or {}
            raws = [
                (block.get("fullName") or "").strip()
                for block in rd.get("names") or []
            ]
            raws += [str(n).strip() for n in rd.get("alternateNames") or []]
            for raw in raws:
                if not raw:
                    continue
                checked += 1
                old, new = _old_normalise(raw), names.normalise_name(raw)
                if old != new:
                    changed.append((raw, old, new))
    assert checked > 30, "demo corpus should contain a real number of names"
    unexplained = [c for c in changed if not _newly_folded(c[0])]
    assert not unexplained, f"unexplained normaliser drift: {unexplained[:5]}"


def test_latin_names_identical_to_old_normaliser():
    for name in [
        "Ørsted Wind Power A/S",
        "HORNSEA 1 LIMITED",
        "Unilever PLC",
        "Anne-Marie O'Brien",
        "Müller & Söhne GmbH",
        "Łukasz Kowalski",
        "Kraków Development S.A.",
        "ÆGIR INSURANCE",
    ]:
        assert names.normalise_name(name) == _old_normalise(name), name


# --- Contract 2: the new fold layers ---------------------------------------


def test_cyrillic_names_gain_latin_comparable_form():
    assert names.normalise_name("Газпром") == "gazprom"
    assert names.normalise_name("ЛУКОЙЛ") == "lukoil"
    assert names.normalise_name("ООО Газпром Экспорт") == "ooo gazprom eksport"
    # native vs transliterated forms of the same name now score high
    a = names.normalise_name("Газпром Экспорт")
    b = names.normalise_name("Gazprom Export")
    assert difflib.SequenceMatcher(a=a, b=b).ratio() > 0.85


def test_greek_names_gain_latin_comparable_form():
    assert names.normalise_name("Ελλάς") == "ellas"
    assert names.normalise_name("ΑΛΦΑ ΤΡΑΠΕΖΑ") == "alfa trapeza"


def test_cjk_passes_through_unchanged():
    # Lossy CJK romanisation would manufacture noise — deliberately untouched.
    assert names.normalise_name("株式会社日立製作所") == "株式会社日立製作所"


def test_extended_latin_folds():
    assert names.normalise_name("Đorđe Straße") == "dorde strasse"
    assert names.normalise_name("İstanbul Holding") == "istanbul holding"


def test_empty_and_none():
    assert names.normalise_name(None) == ""
    assert names.normalise_name("") == ""
    assert names.normalise_name("  ") == ""


# --- Delegation: all five call sites share this normaliser ------------------


def test_all_call_sites_delegate():
    from opencheck import cross_check, icij_check, nz_associations, reconcile
    from opencheck.sources import openaleph

    probe = "Łukasz Ørsted-Экспорт"
    expected = names.normalise_name(probe)
    assert cross_check._normalise(probe) == expected
    assert icij_check._normalise(probe) == expected
    assert reconcile._normalise_name(probe) == expected
    assert openaleph._normalise_name(probe) == expected
    assert nz_associations._norm_name(probe) == expected


def test_fold_tables_deleted():
    import opencheck.cross_check as cc
    import opencheck.icij_check as ic

    assert not hasattr(cc, "_NON_DECOMPOSABLE_FOLDS")
    assert not hasattr(ic, "_NON_DECOMPOSABLE_FOLDS")


# --- Homoglyph identifiers --------------------------------------------------


def test_homoglyph_fold_for_identifiers():
    assert names.fold_homoglyphs("ΗΕ 489243") == "HE 489243"  # Greek Eta/Epsilon
    assert names.fold_homoglyphs("АВ123") == "AB123"  # Cyrillic А/В
    assert names.fold_homoglyphs("HE 489243") == "HE 489243"  # Latin unchanged


def test_canonical_identifier_folds_cyprus_greek_numbers():
    # Greek-script and Latin-script forms of the same Cyprus HE number must
    # produce the same canonical key (previously: fallback stripped the Greek
    # letters entirely; the rigour path produced a different key).
    greek = canonical_identifier("ΗΕ 489243")
    latin = canonical_identifier("HE 489243")
    assert greek is not None
    assert greek == latin


def test_canonical_identifier_latin_behaviour_unchanged():
    assert canonical_identifier("GB-12 345 678") == "GB12345678"
    assert canonical_identifier("short", min_len=7) is None
