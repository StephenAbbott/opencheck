"""The name-match gate is one value, defined once (issue #302).

``names.NAME_MATCH_THRESHOLD`` is the product-wide gate. Before #302 its
value was restated as a literal in eight modules; this scans every module
under ``opencheck/`` for a numeric ``0.88`` so a ninth copy — under any
name, such as an adapter's ``_NAME_SIMILARITY_THRESHOLD`` — fails here.
Prose (docstrings, comments, user-facing text) is not a number and is not
checked.
"""

from __future__ import annotations

import ast
from pathlib import Path

import opencheck
from opencheck import cross_check, names, openaleph_check, watchlist
from opencheck.routers import person_check
from opencheck.sources import acra_singapore, cr_hongkong, dlcp_dc, ny_dos

_PACKAGE = Path(opencheck.__file__).parent


def test_the_threshold_has_its_value():
    assert names.NAME_MATCH_THRESHOLD == 0.88


def test_no_other_module_restates_the_value():
    offenders = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        if path == _PACKAGE / "names.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, float)
                and node.value == names.NAME_MATCH_THRESHOLD
            ):
                offenders.append(f"{path.relative_to(_PACKAGE)}:{node.lineno}")
    assert not offenders, (
        "import names.NAME_MATCH_THRESHOLD instead of restating it: "
        + ", ".join(offenders)
    )


def test_no_other_module_defines_its_own_name_match_threshold():
    for path in sorted(_PACKAGE.rglob("*.py")):
        if path == _PACKAGE / "names.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "NAME_MATCH_THRESHOLD":
                        # watchlist keeps an alias for its callers; it must
                        # point at names', not hold a number of its own.
                        assert not isinstance(node.value, ast.Constant), path


def test_every_former_copy_reads_the_shared_value():
    t = names.NAME_MATCH_THRESHOLD
    assert t == watchlist.NAME_MATCH_THRESHOLD
    assert t == person_check.STRONG_MATCH_THRESHOLD
    for adapter in (cr_hongkong, ny_dos, acra_singapore, dlcp_dc):
        assert t == adapter._NAME_SIMILARITY_THRESHOLD, adapter.__name__
    import inspect

    for fn in (cross_check.assess_cross_source_names, openaleph_check.assess_openaleph_names):
        assert inspect.signature(fn).parameters["min_score"].default == t, fn.__name__
