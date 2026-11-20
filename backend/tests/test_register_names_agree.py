"""One name gate for the register adapters (codebase review, 20 Nov 2026).

Eight adapters kept their own copy of the same four-step check
(comparable form, despaced form, distinctive tokens, similarity). They now
call ``names.register_names_agree``; this pins what it accepts and fails if
an adapter grows a private copy again.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import opencheck
from opencheck import names

_SOURCES = Path(opencheck.__file__).parent / "sources"


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("Moody's Corporation", "MOODY'S CORP"),  # legal-form words set aside
        ("Tech Data Co Ltd", "TECHDATA CO LTD"),  # spaces removed
        ("Acme Widgets Holdings Inc", "ACME WIDGETS INC"),  # distinctive tokens
    ],
)
def test_the_same_company_agrees(a, b):
    assert names.register_names_agree(a, b)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("American Foreign Policy Council", "CONNIE-19 STREET LLC"),
        ("38 Town Line Road LP", "River Ridge Apartments LP"),
        ("", "ACME INC"),
        ("ACME INC", None),
    ],
)
def test_different_or_missing_names_do_not_agree(a, b):
    assert not names.register_names_agree(a, b)


def test_the_threshold_is_the_product_wide_one():
    a, b = "Northwind Trading Limited", "Northwind Tradeing Ltd"
    score = names.name_similarity(a, b)
    assert names.register_names_agree(a, b, threshold=score)
    assert not names.register_names_agree(
        "Alpha Beta Gamma", "Delta Epsilon Zeta", threshold=1.0
    )


def _private_copies(tree: ast.AST) -> bool:
    """A function that calls all of org_comparable_name, despace and
    name_similarity is a re-implementation of the shared gate."""
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):
            continue
        called = {
            n.func.attr
            for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
        }
        if {"org_comparable_name", "despace", "name_similarity"} <= called:
            return True
    return False


def test_no_adapter_keeps_its_own_copy_of_the_gate():
    offenders = [
        p.name
        for p in sorted(_SOURCES.glob("*.py"))
        if _private_copies(ast.parse(p.read_text(encoding="utf-8")))
    ]
    assert not offenders, (
        "call names.register_names_agree instead of re-implementing it: "
        + ", ".join(offenders)
    )
