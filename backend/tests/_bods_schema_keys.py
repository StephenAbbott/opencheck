"""Every key in a BODS statement that the v0.4 schema does not define.

The data-dictionary ticket's first guard caught only keys starting
``opencheck``. The post-deploy check of 4 Nov 2026 found production carrying
three more that nothing documented — ``legalFormLabel`` on ``recordDetails``,
``formerName`` and ``identityVerification`` on annotations — because none of
them has the prefix. BODS v0.4's objects are open, so the only honest test of
"is this ours?" is the schema itself: a key the schema does not define at that
place is an extension, whatever it is called.

The schema is the copy vendored in ``libcovebods`` (a dev dependency, which is
why this lives with the tests and not in ``opencheck/``: the drift job installs
``--no-dev``). Its documents reference each other by ``$id`` URN
(``urn:entity``, ``urn:components#/$defs/Address``). At each place in a
statement the walker takes the union of every schema node that can describe
it — ``$ref`` resolved, ``oneOf``/``anyOf``/``allOf`` and ``then``/``else``
flattened — so the record-type conditionals on ``recordDetails`` allow the
union of the three record kinds. That is permissive on purpose: the question
is "does BODS define this key anywhere here", not "is the statement valid",
which libcovebods already answers.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any

import libcovebods

SCHEMA_DIR = Path(libcovebods.__file__).parent / "data" / "schema-0-4-0"
_STATEMENT = ({"$ref": "urn:statement#/$defs/Statement"}, "urn:statement")

#: (schema node, ``$id`` of the document it sits in)
_Node = tuple[dict[str, Any], str]


@lru_cache(maxsize=None)
def _documents() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in SCHEMA_DIR.glob("*.json"):
        doc = json.loads(path.read_text(encoding="utf-8"))
        out[doc.get("$id") or path.name] = doc
    return out


def _resolve(node: dict[str, Any], here: str) -> _Node:
    while "$ref" in node:
        document, _, fragment = node["$ref"].partition("#")
        here = document or here
        node = _documents()[here]
        for part in (p for p in fragment.split("/") if p):
            node = node[part]
    return node, here


def _expand(nodes: list[_Node]) -> list[_Node]:
    out: list[_Node] = []
    stack = list(nodes)
    while stack:
        node, here = _resolve(*stack.pop())
        out.append((node, here))
        for key in ("oneOf", "anyOf", "allOf"):
            stack.extend((sub, here) for sub in node.get(key, []))
        for key in ("then", "else"):
            if isinstance(node.get(key), dict):
                stack.append((node[key], here))
    return out


def _walk(value: Any, nodes: list[_Node], path: str) -> Iterator[tuple[str, str]]:
    alternatives = _expand(nodes)
    if isinstance(value, dict):
        properties: dict[str, list[_Node]] = {}
        for node, here in alternatives:
            for key, sub in (node.get("properties") or {}).items():
                properties.setdefault(key, []).append((sub, here))
        for key, child in value.items():
            if key not in properties:
                yield f"{path}/{key}", key
            else:
                yield from _walk(child, properties[key], f"{path}/{key}")
    elif isinstance(value, list):
        items = [
            (node["items"], here) for node, here in alternatives
            if isinstance(node.get("items"), dict)
        ]
        for index, child in enumerate(value):
            yield from _walk(child, items, f"{path}/{index}")


def non_schema_keys(statement: dict[str, Any]) -> list[tuple[str, str]]:
    """``(JSON pointer, key)`` for every key BODS v0.4 does not define where it sits."""
    return list(_walk(statement, [_STATEMENT], ""))
