"""Generated modules must stay generated.

`bods/psc_natures.py` and `bods/ch_constants.py` are re-vendored from upstream
Companies House data by `scripts/revendor_*.py`, and both say "Do NOT edit by
hand" in their docstrings. The `Drift checks / enums` CI job enforces it by
regenerating and diffing.

That job needs the network, so it only runs in CI — which means a hand-edit
passes `make test` locally and fails after the PR is open. That happened in
Phase 104: a derived constant (`NOMINEE_NATURE_CODES`) was added straight into
`psc_natures.py`, worked perfectly, and broke the drift check. It now lives in
`bods/nominees.py`, which imports the generated dict instead.

This test is the cheap local half of the same guard: it fails the moment a
symbol appears in a generated module that the generator would not produce, and
says where to put it instead.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_BODS = Path(__file__).resolve().parents[1] / "opencheck" / "bods"

# module filename -> the module-level names its generator actually emits
GENERATED: dict[str, set[str]] = {
    "psc_natures.py": {
        "annotations",  # from __future__ import annotations
        "_DEFAULT_SUPER_SECURE",  # emitted by render_module
        "PSC_NATURE_DESCRIPTIONS",
        "SUPER_SECURE_DESCRIPTIONS",
        "PSC_STATEMENT_DESCRIPTIONS",
        "describe_nature",
        "describe_super_secure",
        "describe_statement",
    },
}


def _module_level_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
    return names


@pytest.mark.parametrize("filename", sorted(GENERATED))
def test_no_hand_added_symbols(filename: str) -> None:
    path = _BODS / filename
    actual = _module_level_names(path)
    unexpected = actual - GENERATED[filename]
    assert not unexpected, (
        f"{filename} is AUTO-GENERATED (see its docstring) and the vendored-enum "
        f"drift check regenerates it from upstream, so these hand-added names "
        f"will be wiped and will fail CI: {sorted(unexpected)}. "
        f"Put anything derived from the enumeration in a hand-maintained module "
        f"that imports it — bods/nominees.py is the worked example."
    )


@pytest.mark.parametrize("filename", sorted(GENERATED))
def test_docstring_still_warns(filename: str) -> None:
    """If the warning is ever removed, this test's premise is gone too."""
    text = (_BODS / filename).read_text(encoding="utf-8")
    assert "Do NOT edit by hand" in text
