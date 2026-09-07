"""The FollowTheMoney "names" group OpenCheck reads (Phase 174).

``names.FTM_NAME_PROPS`` is a *gate*, not a display list: a hit that does not
bear the searched name in one of these properties is dropped, so a property
missing from the group costs a real match and the symptom — no hit — is
indistinguishable from a company that is genuinely not listed. It fails closed
and silently, which is why the group is defined once and pinned here.

What these tests fix in place:

1. ``abbreviation`` is read. FtM added it at the ``LegalEntity`` level to hold
   acronyms apart from full names (OpenSanctions changelog #39); until
   2026-09-15 its values were also copied into ``weakAlias``, and OpenCheck
   read neither, so an acronym-only surface form could never clear the gate.
2. ``weakAlias`` is *not* read, and that is a decision rather than an
   oversight — upstream files a name there precisely when it should not be
   trusted alone.
3. The gate and the scorer read the same constant. Two copies of this tuple
   already existed; the pair drifting means a hit admitted by one and scored
   against nothing by the other.

The model itself is checked separately, against the installed
``followthemoney``, by ``scripts/check_ftm_names.py`` in CI — a new upstream
name property fails the build there rather than being quietly unread.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from opencheck import names
from opencheck.openaleph_check import _best_name_score
from opencheck.sources.openaleph import _bears_name, _normalise_name

_BACKEND = pathlib.Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# The group itself
# ---------------------------------------------------------------------------


def test_abbreviation_is_read_and_weak_alias_is_not() -> None:
    assert "abbreviation" in names.FTM_NAME_PROPS
    # The three that were always read stay read.
    for prop in ("name", "alias", "previousName"):
        assert prop in names.FTM_NAME_PROPS
    # Declined on purpose, and recorded as declined so the drift check can
    # tell that apart from never having noticed it.
    assert "weakAlias" not in names.FTM_NAME_PROPS
    assert "weakAlias" in names.FTM_NAME_PROPS_EXCLUDED
    assert not set(names.FTM_NAME_PROPS) & names.FTM_NAME_PROPS_EXCLUDED


# ---------------------------------------------------------------------------
# The gate: sources/openaleph._bears_name
# ---------------------------------------------------------------------------


def test_bears_name_accepts_an_abbreviation_only_hit() -> None:
    """The acronym case the whole change is for: a record whose only match on
    the searched form is its ``abbreviation``."""
    item = {
        "caption": "African National Congress",
        "properties": {
            "name": ["African National Congress"],
            "abbreviation": ["ANC"],
        },
    }
    assert _bears_name(item, _normalise_name("ANC")) is True
    # And the full name still matches, from a different property.
    assert _bears_name(item, _normalise_name("African National Congress")) is True


def test_bears_name_still_rejects_a_weak_alias_only_hit() -> None:
    """``weakAlias`` is weak by construction. Admitting a hit on one would
    defeat the gate — this is the boundary the exclusion draws."""
    item = {
        "caption": "Some Unrelated Holding BV",
        "properties": {
            "name": ["Some Unrelated Holding BV"],
            "weakAlias": ["ANC"],
        },
    }
    assert _bears_name(item, _normalise_name("ANC")) is False


def test_bears_name_tolerates_a_bare_string_abbreviation() -> None:
    """FtM values are arrays, but collections have been seen to emit a bare
    string; the existing loop handles it and must keep doing so."""
    item = {"properties": {"abbreviation": "IKEA"}}
    assert _bears_name(item, _normalise_name("IKEA")) is True


# ---------------------------------------------------------------------------
# The scorer: openaleph_check._best_name_score
# ---------------------------------------------------------------------------


def test_best_name_score_scores_against_an_abbreviation() -> None:
    item = {
        "caption": "African National Congress",
        "properties": {
            "name": ["African National Congress"],
            "abbreviation": ["ANC"],
        },
    }
    best, score = _best_name_score("ANC", item)
    assert best == "ANC"
    assert score == pytest.approx(1.0)


def test_best_name_score_ignores_a_weak_alias() -> None:
    """Same boundary as the gate, on the other side of the pair: a name good
    enough to score against is a name good enough to admit."""
    item = {
        "caption": "Some Unrelated Holding BV",
        "properties": {
            "name": ["Some Unrelated Holding BV"],
            "weakAlias": ["ANC"],
        },
    }
    best, _score = _best_name_score("ANC", item)
    assert best != "ANC"


# ---------------------------------------------------------------------------
# One definition, two call sites
# ---------------------------------------------------------------------------


def _string_tuples(path: pathlib.Path) -> list[tuple[str, ...]]:
    """Every literal tuple of plain strings in a module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[tuple[str, ...]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Tuple):
            continue
        values = [
            el.value
            for el in node.elts
            if isinstance(el, ast.Constant) and isinstance(el.value, str)
        ]
        if values and len(values) == len(node.elts):
            found.append(tuple(values))
    return found


@pytest.mark.parametrize(
    "relpath",
    ["opencheck/sources/openaleph.py", "opencheck/openaleph_check.py"],
)
def test_no_second_copy_of_the_names_group(relpath: str) -> None:
    """Neither call site may re-declare the group inline.

    A second copy is how the wrong Companies House RA map survived in
    ``sources/gleif.py``: the one that mattered got fixed, the other kept
    being right-looking and wrong.
    """
    path = _BACKEND / relpath
    source = path.read_text(encoding="utf-8")
    assert "FTM_NAME_PROPS" in source, f"{relpath} should read the shared group"

    group = set(names.FTM_NAME_PROPS)
    for literal in _string_tuples(path):
        overlap = group & set(literal)
        assert len(overlap) < 2, (
            f"{relpath} declares its own names-group tuple {literal!r} — "
            "read names.FTM_NAME_PROPS instead."
        )


def test_drift_check_script_agrees_with_the_installed_model() -> None:
    """The CI drift check, run in-process so a local suite catches it too.

    Skipped without the ``ftm`` extra; CI's test job and the ``ftm-edges``
    drift job both install it.
    """
    pytest.importorskip("followthemoney")

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "check_ftm_names", _BACKEND / "scripts" / "check_ftm_names.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.main() == 0
