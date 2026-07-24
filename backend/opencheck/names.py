"""Shared name normalisation (Phase B of the rigour adoption plan).

Before this module, five hand-rolled name normalisers lived across the
codebase with subtly different behaviour — ``cross_check._normalise`` and a
verbatim ``icij_check`` duplicate (each with its own copy of a
non-decomposable fold table), ``reconcile._normalise_name`` (no folds, so the
same name could bridge in one module and not another), ``sources/openaleph``
(the only one using casefold) and ``nz_associations`` (lowercase+split only).
Non-Latin scripts fell through all of them: NFKD leaves Cyrillic/Greek intact
and the downstream regexes either kept them as unmatched opaque tokens or
stripped them entirely.

This module is the one place name normalisation happens. Design constraints:

* **Deterministic across environments.** Production builds pyicu (Docker
  installs the ICU toolchain for followthemoney), dev usually doesn't — and
  ``normality.ascii_text`` output differs between the two (ICU-less fallback
  renders ``Ø`` as ``O/``). Every transform here is a plain table or stdlib
  Unicode operation, so prod, CI and ICU-less dev produce identical strings.
  rigour's own transliteration helpers (``maybe_ascii``) only exist in the
  Rust-cored 2.x line, which followthemoney 3.8.x caps us below — revisit
  when bods-ftm upgrades (see the rigour-adoption plan on Notion).
* **Comparable forms, not display forms.** Output feeds matching, merge keys
  and screening comparisons; it is never shown to users. Original names are
  always preserved in hits/statements.

Layers (compose in this order):

1. ``fold_non_decomposable`` — stand-alone non-ASCII Latin letters NFKD
   leaves in place (``ø``, ``ł``, ``æ``, ``ß``…). Superset of the two
   deleted ``_NON_DECOMPOSABLE_FOLDS`` tables.
2. NFKD + combining-mark strip (``é`` → ``e``).
3. ``fold_script`` — bounded per-character Cyrillic/Greek → Latin tables
   (BGN/PCGN-flavoured, matching the Latin forms OpenSanctions publishes:
   ``Газпром`` → ``gazprom``, ``ЛУКОЙЛ`` → ``lukoil``), so native and
   transliterated forms of the same name finally score as similar instead
   of ~0. Other scripts (CJK, Arabic…) pass through unchanged — matching
   them via lossy romanisation would manufacture noise.
4. Punctuation → space, lowercase, squash.

``fold_homoglyphs`` is separate and serves *identifiers*: uppercase Greek and
Cyrillic letters that are visual homoglyphs of Latin capitals (Cyprus company
numbers arrive as Greek ``ΗΕ 489243``; the same number from GLEIF is Latin
``HE 489243`` — without the fold they canonicalise to different keys in every
environment, ICU or not).
"""

from __future__ import annotations

import difflib
import re
import unicodedata

try:  # pragma: no cover - exercised via the ftm extra in CI/prod
    from rigour.names import replace_org_types_compare as _rigour_org_compare
    from rigour.text import levenshtein_similarity as _rigour_lev_sim

    _HAS_RIGOUR_NAMES = True
except ImportError:  # pragma: no cover - base install without the ftm extra
    _rigour_org_compare = None  # type: ignore[assignment]
    _rigour_lev_sim = None  # type: ignore[assignment]
    _HAS_RIGOUR_NAMES = False

# --- Layer 1: non-decomposable Latin letters --------------------------------
# NFKD does not decompose these; both deleted _NON_DECOMPOSABLE_FOLDS tables
# (cross_check, icij_check) are strict subsets. Lowercase only — callers fold
# case first (casefold maps ẞ→ß, İ→i̇ etc. before we get here).
_LATIN_FOLDS = {
    "ø": "o",
    "æ": "ae",
    "œ": "oe",
    "ł": "l",
    "ð": "d",
    "đ": "d",
    "þ": "th",
    "ß": "ss",
    "ħ": "h",
    "ı": "i",
    "ŋ": "n",
    "ƒ": "f",
    "ĸ": "k",
}

# --- Layer 3a: Cyrillic → Latin (BGN/PCGN-flavoured, lowercase) -------------
# Covers Russian plus the Ukrainian/Belarusian/Serbian letters that appear in
# the registries and screening lists OpenCheck touches.
_CYRILLIC = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "i", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    # Ukrainian / Belarusian
    "і": "i", "ї": "yi", "є": "ye", "ґ": "g", "ў": "u",
    # South Slavic
    "ј": "j", "љ": "lj", "њ": "nj", "ђ": "dj", "ћ": "c", "џ": "dz",
}

# --- Layer 3b: Greek → Latin (lowercase) ------------------------------------
_GREEK = {
    "α": "a", "β": "v", "γ": "g", "δ": "d", "ε": "e", "ζ": "z", "η": "i",
    "θ": "th", "ι": "i", "κ": "k", "λ": "l", "μ": "m", "ν": "n", "ξ": "x",
    "ο": "o", "π": "p", "ρ": "r", "σ": "s", "ς": "s", "τ": "t", "υ": "y",
    "φ": "f", "χ": "ch", "ψ": "ps", "ω": "o",
}

_SCRIPT_FOLDS = {**_CYRILLIC, **_GREEK}

# --- Homoglyphs (uppercase, for identifiers) --------------------------------
# Greek and Cyrillic capitals that are visual homoglyphs of Latin capitals.
# Deliberately NOT phonetic: Greek Η romanises as "i" in names, but in an
# identifier the registry's own Latin form uses the lookalike letter
# (Cyprus ΗΕ ↔ HE).
_HOMOGLYPHS = {
    # Greek
    "Α": "A", "Β": "B", "Ε": "E", "Ζ": "Z", "Η": "H", "Ι": "I", "Κ": "K",
    "Μ": "M", "Ν": "N", "Ο": "O", "Ρ": "P", "Τ": "T", "Υ": "Y", "Χ": "X",
    # Cyrillic
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O",
    "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X", "І": "I",
}

_PUNCT_TO_SPACE = re.compile(r"[^\w\s]")
_SQUASH = re.compile(r"\s+")


def fold_homoglyphs(text: str) -> str:
    """Map Greek/Cyrillic lookalike capitals to their Latin twins.

    For identifier canonicalisation only (see ``matching.canonical_identifier``)
    — apply BEFORE any case folding, since the table is keyed on capitals.
    """
    if not text:
        return text
    return "".join(_HOMOGLYPHS.get(ch, ch) for ch in text)


def fold_ascii(text: str) -> str:
    """Casefold + fold to a deterministic lowercase quasi-ASCII form.

    Latin diacritics stripped, non-decomposables folded, Cyrillic/Greek
    transliterated; other scripts pass through unchanged. No punctuation or
    whitespace handling — compose via ``normalise_name``.
    """
    if not text:
        return ""
    folded = text.casefold()
    folded = "".join(_LATIN_FOLDS.get(ch, ch) for ch in folded)
    decomposed = unicodedata.normalize("NFKD", folded)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return "".join(_SCRIPT_FOLDS.get(ch, ch) for ch in stripped)


def normalise_name(name: str | None) -> str:
    """THE shared comparable form for names, replacing all five old
    normalisers: ``fold_ascii`` then punctuation → space, squash, strip.

    For any Latin-script name the output is identical to what
    ``cross_check._normalise`` / ``icij_check._normalise`` produced (their
    fold tables are subsets of ours), so match scores there are unchanged.
    ``reconcile`` / ``openaleph`` / ``nz_associations`` gain the fold layers
    their local normalisers lacked — that is the Phase B fix, pinned by
    tests/test_names.py.
    """
    if not name:
        return ""
    cleaned = _PUNCT_TO_SPACE.sub(" ", fold_ascii(name))
    return _SQUASH.sub(" ", cleaned).strip()


def org_comparable_name(name: str | None, *, generic: bool = True) -> str:
    """Comparable form for ORGANISATION names (Phase C).

    Runs rigour's curated org-type normalisation over the casefolded raw name
    BEFORE the shared fold pipeline (org types must be recognised before
    punctuation-stripping mangles them): spelled-out legal forms collapse to
    their abbreviation and, with ``generic=True``, to a cross-language class —
    "Unilever Public Limited Company" ≡ "Unilever PLC", "ооо газпром" ≡
    "gazprom llc"-class. Without rigour (base install) this degrades to plain
    ``normalise_name`` — dev-only divergence, same caveat as ``matching.py``.

    Note: "A/S" is NOT in rigour's alias data, so Danish suffixes are handled
    by the despaced secondary key (``despace``), not org-type replacement.
    """
    if not name:
        return ""
    text = name
    if _HAS_RIGOUR_NAMES:
        text = _rigour_org_compare(text.casefold(), generic=generic)
    return normalise_name(text)


def despace(comparable: str) -> str:
    """Space-stripped variant of an already-comparable form, used as a
    secondary merge key so tokenisation artefacts still collide
    ("ørsted … a/s" → "…a s" vs "… AS" → "…as" ⇒ both "…as")."""
    return comparable.replace(" ", "")


# --- Phase D: the shared name-similarity scorer -----------------------------

def name_similarity(a: str | None, b: str | None) -> float:
    """Similarity in [0.0, 1.0] between two raw names — THE scorer behind
    RELATED_PEP / RELATED_SANCTIONED and BackgroundCheck person screening
    (threshold 0.88, one concept product-wide).

    Composition (max of):

    * ``difflib.SequenceMatcher`` on the shared comparable forms — the
      historical scorer, unchanged, so every pair that matched before still
      matches with at least its old score;
    * the same ratio on TOKEN-SORTED forms — name-order invariance
      ("Doe, John" ↔ "John Doe", NZ Companies Office "LastName First"),
      deterministic in every environment;
    * rigour's ``levenshtein_similarity`` when installed (edit-budgeted:
      ≤4 edits and ≤20% of length — strict, so it only ever adds
      near-typo matches like "Jóhn Smíth" spelled slightly differently).

    Scores can only rise relative to the old scorer — a deliberate
    recall-first choice for a "possibly related, human reviews it" surface;
    scripts/eval_name_matching.py quantifies the movement on the demo corpus.
    """
    na, nb = normalise_name(a or ""), normalise_name(b or "")
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    score = difflib.SequenceMatcher(a=na, b=nb).ratio()
    sa, sb = " ".join(sorted(na.split())), " ".join(sorted(nb.split()))
    if (sa, sb) != (na, nb):
        score = max(score, difflib.SequenceMatcher(a=sa, b=sb).ratio())
    if _HAS_RIGOUR_NAMES:
        score = max(score, _rigour_lev_sim(na, nb))
    return score


# Unicode blocks whose scripts write names without spaces — a "single token"
# guard calibrated for space-separated scripts must not apply to them.
_DENSE_RANGES = (
    (0x3040, 0x30FF),   # Hiragana + Katakana
    (0x4E00, 0x9FFF),   # CJK Unified Ideographs
    (0x3400, 0x4DBF),   # CJK Extension A
    (0xAC00, 0xD7AF),   # Hangul syllables
    (0xF900, 0xFAFF),   # CJK Compatibility Ideographs
)


def has_dense_script(text: str) -> bool:
    """True when the text contains characters from a script that does not
    separate name parts with spaces (CJK, kana, Hangul)."""
    return any(
        lo <= ord(ch) <= hi for ch in text or "" for lo, hi in _DENSE_RANGES
    )


# --- Phase E: transliterated alternates + language codes --------------------

def transliterate_display(text: str | None) -> str | None:
    """A case-preserving Latin form of a Cyrillic/Greek name, or ``None``
    when the text contains no Cyrillic/Greek (nothing worth adding).

    Unlike the lowercase comparable forms above this is emitted into BODS
    output (entity ``alternateNames`` strings, person ``names`` entries with
    ``type: transliteration``), so casing is preserved character-wise:
    ``Газпром`` → ``Gazprom``, ``ЛУКОЙЛ`` → ``LUKOIL``. Same deterministic
    tables as the comparable pipeline — no ICU dependence.
    """
    if not text:
        return None
    if not any(ch.casefold() in _SCRIPT_FOLDS for ch in text):
        return None
    out: list[str] = []
    for ch in text:
        low = ch.casefold()
        mapped = _SCRIPT_FOLDS.get(low)
        if mapped is None and low not in _SCRIPT_FOLDS:
            # Accented Greek/Cyrillic (ά, ё́…) arrive precomposed — map the
            # decomposed base letter and drop the accent; anything genuinely
            # non-Greek/Cyrillic (Latin é, CJK…) passes through unchanged.
            base = unicodedata.normalize("NFD", low)[:1]
            if base in _SCRIPT_FOLDS:
                mapped = _SCRIPT_FOLDS[base]
        if mapped is None:
            out.append(ch)
        elif ch != low:  # source char was uppercase
            out.append(mapped[:1].upper() + mapped[1:])
        else:
            out.append(mapped)
    return "".join(out)


def normalise_language_code(code: str | None) -> str | None:
    """Normalise a language identifier to an ISO 639-2/3 alpha-3 code.

    Prefers rigour's ``iso_639_alpha3`` (accepts 2/3-letter codes, some
    names, BCP-47 tags like ``zh-Hans``); falls back to pycountry — a hard
    dependency already — for base installs. ``None`` when unrecognised.
    """
    if not code or not str(code).strip():
        return None
    raw = str(code).strip()
    try:  # pragma: no cover - exercised via the ftm extra in CI/prod
        from rigour.langs import iso_639_alpha3

        # rigour 0.x does not parse BCP-47 subtags (2.x does) — retry with
        # the primary subtag before giving up.
        resolved = iso_639_alpha3(raw) or iso_639_alpha3(
            raw.lower().split("-")[0].split("_")[0]
        )
        if resolved:
            return resolved
    except ImportError:  # pragma: no cover - base install
        pass
    import pycountry

    key = raw.lower().split("-")[0].split("_")[0]
    try:
        lang = (
            pycountry.languages.get(alpha_2=key)
            or pycountry.languages.get(alpha_3=key)
        )
    except LookupError:
        return None
    return getattr(lang, "alpha_3", None) if lang else None
