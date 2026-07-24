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


# --- Phase C: org-type-aware merge keys -------------------------------------

import pytest  # noqa: E402

from opencheck.reconcile import possibly_same_entities  # noqa: E402


def _ent(sid: str, name: str, jur: str = "DK", founding: str | None = None):
    rd = {"name": name, "jurisdiction": {"code": jur}}
    if founding:
        rd["foundingDate"] = founding
    return {
        "recordType": "entity",
        "statementId": sid,
        "recordDetails": rd,
        "source": {"description": f"src-{sid}"},
    }


@pytest.mark.skipif(
    not names._HAS_RIGOUR_NAMES, reason="rigour not installed (ftm extra)"
)
def test_org_comparable_name_equivalences():
    assert names.org_comparable_name("Unilever Public Limited Company") == (
        names.org_comparable_name("Unilever PLC")
    )
    assert names.org_comparable_name("Tesco Stores Limited") == (
        names.org_comparable_name("TESCO STORES LTD")
    )
    # cross-language generic class: Russian ооо ≡ German GmbH ≡ Ltd
    assert names.org_comparable_name("Acme GmbH") == names.org_comparable_name(
        "Acme Limited"
    )


def test_despace():
    assert names.despace("orsted wind power a s") == "orstedwindpoweras"
    assert names.despace("") == ""


def test_possibly_same_merges_org_type_variants():
    if not names._HAS_RIGOUR_NAMES:
        pytest.skip("rigour not installed (ftm extra)")
    pairs = possibly_same_entities(
        [_ent("s1", "Tesco Stores Limited", "GB"), _ent("s2", "TESCO STORES LTD", "GB")]
    )
    assert len(pairs) == 1
    assert {pairs[0].a, pairs[0].b} == {"s1", "s2"}


def test_possibly_same_merges_danish_as_variants():
    # A/S vs AS collide via the despaced plain-form key — no rigour needed.
    pairs = possibly_same_entities(
        [_ent("s1", "Ørsted Wind Power A/S"), _ent("s2", "ORSTED WIND POWER AS")]
    )
    assert len(pairs) == 1


def test_possibly_same_still_requires_jurisdiction_match():
    pairs = possibly_same_entities(
        [_ent("s1", "Tesco Stores Limited", "GB"), _ent("s2", "Tesco Stores Limited", "IE")]
    )
    assert pairs == []


def test_possibly_same_exact_name_pairs_unchanged():
    pairs = possibly_same_entities(
        [_ent("s1", "Acme Widgets", "GB"), _ent("s2", "Acme Widgets", "GB")]
    )
    assert len(pairs) == 1


# --- Phase D: shared scorer + dense-script matchability ---------------------

from opencheck.matching import is_matchable_name  # noqa: E402


def test_name_similarity_order_invariant():
    assert names.name_similarity("Doe, John", "John Doe") >= 0.99
    # NZ Companies Office order: "LastName First Middle"
    assert names.name_similarity("SMITH John Andrew", "John Andrew Smith") >= 0.99


def test_name_similarity_never_below_old_scorer():
    import difflib as _d

    cases = [
        ("Vladimir Putin", "Vladimir Putln"),
        ("Gazprom Export", "Газпром Экспорт"),
        ("Jane O'Brien", "Jane OBrien"),
        ("Acme Widgets Ltd", "Acme Widgets Limited"),
    ]
    for a, b in cases:
        na, nb = names.normalise_name(a), names.normalise_name(b)
        old = 1.0 if na == nb else _d.SequenceMatcher(a=na, b=nb).ratio()
        assert names.name_similarity(a, b) >= old, (a, b)


@pytest.mark.skipif(
    not names._HAS_RIGOUR_NAMES, reason="rigour not installed (ftm extra)"
)
def test_name_similarity_rigour_typo_component():
    # Edit-budgeted Levenshtein: one substitution in a long name scores high.
    assert names.name_similarity("Konstantin Ernstov", "Konstantin Ernstev") >= 0.88


def test_name_similarity_distinct_names_stay_low():
    assert names.name_similarity("HORNSEA 1 LIMITED", "Unilever PLC") < 0.5
    assert names.name_similarity("", "anything") == 0.0


def test_has_dense_script():
    assert names.has_dense_script("田中太郎")
    assert names.has_dense_script("株式会社日立")
    assert names.has_dense_script("김민준")
    assert not names.has_dense_script("John Smith")
    assert not names.has_dense_script("Газпром")


def test_is_matchable_name_dense_script_fix():
    # Previously blocked: CJK names have no internal spaces.
    assert is_matchable_name("田中太郎")
    assert is_matchable_name("김민준")
    # Still blocked: single Latin/Cyrillic tokens are too generic.
    assert not is_matchable_name("Ivanov")
    assert not is_matchable_name("fernandez")
    # Unchanged: multi-token names match, empty doesn't.
    assert is_matchable_name("john smith")
    assert not is_matchable_name("")
    assert not is_matchable_name(None)
    # A single dense character is still too generic.
    assert not is_matchable_name("金")


# --- Phase E: transliterated alternates + language codes --------------------

from opencheck.bods.mapper import make_entity_statement, make_person_statement  # noqa: E402
from opencheck.sources.wikidata import _summarise_bindings  # noqa: E402


def test_transliterate_display():
    assert names.transliterate_display("Газпром") == "Gazprom"
    assert names.transliterate_display("ЛУКОЙЛ") == "LUKOIL"
    assert names.transliterate_display("ΑΛΦΑ Τράπεζα") == "ALFA Trapeza"
    # No Cyrillic/Greek → nothing to add
    assert names.transliterate_display("Plain Latin Ltd") is None
    assert names.transliterate_display("Café Sté") is None
    assert names.transliterate_display("") is None
    assert names.transliterate_display(None) is None


def test_normalise_language_code():
    assert names.normalise_language_code("en") == "eng"
    assert names.normalise_language_code("el") == "ell"
    assert names.normalise_language_code("zh-Hans") == "zho"
    assert names.normalise_language_code("xx") is None
    assert names.normalise_language_code("") is None
    assert names.normalise_language_code(None) is None


def test_entity_statement_gains_transliterated_alternate():
    stmt = make_entity_statement(
        source_id="wikidata", local_id="Q102673", name="Аэрофлот"
    )
    assert "Aeroflot" in stmt["recordDetails"]["alternateNames"]
    # Latin names: no alternateNames key manufactured
    plain = make_entity_statement(
        source_id="wikidata", local_id="Q1", name="Plain Ltd"
    )
    assert "alternateNames" not in plain["recordDetails"]


def test_person_statement_gains_typed_transliteration():
    stmt = make_person_statement(
        source_id="wikidata", local_id="Q000", full_name="Алексей Миллер"
    )
    entries = stmt["recordDetails"]["names"]
    assert entries[0] == {"type": "legal", "fullName": "Алексей Миллер"}
    assert {"type": "transliteration", "fullName": "Aleksei Miller"} in entries
    # Latin person: only the legal entry
    plain = make_person_statement(
        source_id="wikidata", local_id="Q001", full_name="Jane Smith"
    )
    assert plain["recordDetails"]["names"] == [
        {"type": "legal", "fullName": "Jane Smith"}
    ]


def _label_row(label: str, lang: str) -> dict:
    return {
        "label": {"value": label},
        "labelLang": {"value": lang},
        "instance": {"value": "http://www.wikidata.org/entity/Q4830453"},
        "instanceLabel": {"value": "business"},
    }


def test_wikidata_summary_captures_multilingual_labels():
    rows = [
        _label_row("Газпром", "ru"),
        _label_row("Gazprom", "en"),
        _label_row("Γκαζπρόμ", "el"),
    ]
    summary = _summarise_bindings("Q102180", rows)
    assert summary["label"] == "Gazprom"  # English preferred for display
    langs = {e["language"]: e["label"] for e in summary["labels"]}
    assert langs == {"ru": "Газпром", "en": "Gazprom", "el": "Γκαζπρόμ"}


def test_wikidata_summary_single_language_unchanged():
    # Old-shape fixtures (label rows without labelLang) still work.
    rows = [{
        "label": {"value": "Acme Corp"},
        "instance": {"value": "http://www.wikidata.org/entity/Q4830453"},
        "instanceLabel": {"value": "business"},
    }]
    summary = _summarise_bindings("Q1", rows)
    assert summary["label"] == "Acme Corp"
    assert summary["labels"] == []


def test_map_wikidata_multilingual_alternate_names():
    from opencheck.bods import map_wikidata

    bundle = {
        "source_id": "wikidata",
        "qid": "Q102180",
        "summary": {
            "qid": "Q102180",
            "label": "Gazprom",
            "labels": [
                {"language": "el", "label": "Γκαζπρόμ"},
                {"language": "en", "label": "Gazprom"},
                {"language": "ru", "label": "Газпром"},
            ],
            "is_person": False,
            "is_entity": True,
            "instance_of": [{"qid": "Q4830453", "label": "business"}],
            "identifiers": {},
            "positions": [],
            "citizenships": [],
            "parent_orgs": [],
        },
    }
    result = map_wikidata(bundle)
    ent = next(s for s in result.statements if s["recordType"] == "entity")
    alts = ent["recordDetails"]["alternateNames"]
    assert "Γκαζπρόμ" in alts and "Газпром" in alts
    assert "Gazprom" not in alts  # the display label is not its own alternate
