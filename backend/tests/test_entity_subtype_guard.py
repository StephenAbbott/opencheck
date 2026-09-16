"""Phase 214 — ``entityType.subtype`` is a closed codelist in BODS v0.4.

Four mappers (``cyprus_drcor``, ``abr_australia``, ``mca_india`` and, until
Phase 204, ``acra_singapore``) wrote the register's own entity-type wording
into ``entityType.subtype``, so every statement they emitted with a type
failed JSON Schema validation. These tests pin:

* the codelist table in ``bods/validator.py`` to the schema vendored in
  ``libcovebods``, so it cannot drift;
* ``validate_shape`` reporting a bad subtype;
* the registry-wide guard in ``tests/_entity_subtype_guard.py`` — installed on
  every source mapper, with no module left holding an unwrapped reference,
  and catching a bad statement from every return shape a mapper uses.
"""

from __future__ import annotations

import json
import pathlib
import sys
import types
from typing import Any

import pytest

from opencheck.bods import BODSBundle, make_entity_statement, validate_shape
from opencheck.bods.validator import (
    VALID_ENTITY_SUBTYPES,
    VALID_ENTITY_SUBTYPES_BY_TYPE,
    _VALID_ENTITY_TYPES,
    entity_subtype_issue,
)
from tests import _entity_subtype_guard as guard


def _entity(**entity_type: Any) -> dict[str, Any]:
    stmt = make_entity_statement(source_id="test", local_id="1", name="ACME")
    stmt["recordDetails"]["entityType"] = {"type": "registeredEntity", **entity_type}
    return stmt


# ---------------------------------------------------------------------------
# The codelist table matches the v0.4 schema
# ---------------------------------------------------------------------------


def test_subtype_table_matches_the_vendored_v04_schema() -> None:
    libcovebods = pytest.importorskip("libcovebods")
    path = pathlib.Path(libcovebods.__file__).parent / "data/schema-0-4-0/entity-record.json"
    entity_type = json.loads(path.read_text(encoding="utf-8"))["properties"]["entityType"]

    subtype = entity_type["properties"]["subtype"]
    assert subtype["openCodelist"] is False
    assert frozenset(subtype["enum"]) == VALID_ENTITY_SUBTYPES

    from_schema: dict[str, frozenset[str]] = {}
    for rule in entity_type["allOf"]:
        allowed = frozenset(rule["then"]["properties"]["subtype"]["enum"])
        for t in rule["if"]["properties"]["type"]["enum"]:
            from_schema[t] = allowed
    assert from_schema == VALID_ENTITY_SUBTYPES_BY_TYPE
    assert set(from_schema) == _VALID_ENTITY_TYPES


# ---------------------------------------------------------------------------
# entity_subtype_issue / validate_shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "entity_type",
    [
        {"type": "registeredEntity"},
        {"type": "registeredEntity", "details": "Local Company"},
        {"type": "registeredEntity", "subtype": "other"},
        {"type": "arrangement", "subtype": "nomination"},
        {"type": "arrangement", "subtype": "trust"},
        {"type": "legalEntity", "subtype": "trust"},
        {"type": "stateBody", "subtype": "stateAgency"},
    ],
)
def test_valid_or_absent_subtype_has_no_issue(entity_type: dict[str, str]) -> None:
    assert entity_subtype_issue({"entityType": entity_type}) is None


@pytest.mark.parametrize(
    "entity_type,fragment",
    [
        ({"type": "registeredEntity", "subtype": "Public"}, "not in the closed v0.4 codelist"),
        (
            {"type": "registeredEntity", "subtype": "Commonwealth Government Entity"},
            "not in the closed v0.4 codelist",
        ),
        ({"type": "registeredEntity", "subtype": ""}, "not in the closed v0.4 codelist"),
        ({"type": "registeredEntity", "subtype": "stateAgency"}, "does not align"),
        ({"type": "legalEntity", "subtype": "nomination"}, "does not align"),
    ],
)
def test_invalid_subtype_is_reported(entity_type: dict[str, str], fragment: str) -> None:
    issue = entity_subtype_issue({"entityType": entity_type})
    assert issue and fragment in issue


def test_validate_shape_reports_a_free_text_subtype() -> None:
    issues = validate_shape([_entity(subtype="Limited Company")])
    assert len(issues) == 1
    assert "entityType.subtype 'Limited Company'" in issues[0]
    assert validate_shape([_entity(details="Limited Company")]) == []


# ---------------------------------------------------------------------------
# The registry-wide guard
# ---------------------------------------------------------------------------


def test_guard_is_installed_on_every_source_mapper() -> None:
    import opencheck.bods.mapper as mapper_mod

    names = [n for n, v in vars(mapper_mod).items() if guard.is_source_mapper(n, v)]
    # A floor, not an exact count: new mappers must not need this test edited.
    assert len(names) >= 50
    unguarded = [n for n in names if not getattr(getattr(mapper_mod, n), guard._MARK, False)]
    assert unguarded == []
    for name in ("map_cyprus_drcor", "map_abr_australia", "map_mca_india", "map_acra_singapore"):
        assert name in names


def test_no_loaded_module_holds_an_unwrapped_mapper() -> None:
    """A module that bound ``map_x`` before the guard was installed would
    call the original and bypass the check. Scan every loaded opencheck and
    test module for a reference to an original function."""
    originals = {id(fn): fn.__name__ for fn in guard.WRAPPED}
    leaks = [
        f"{mod_name}.{attr}"
        for mod_name, mod in list(sys.modules.items())
        if isinstance(mod, types.ModuleType)
        and (mod_name.startswith("opencheck") or mod_name.startswith("tests"))
        for attr, value in list(vars(mod).items())
        if id(value) in originals and not attr.startswith("_")
    ]
    assert leaks == []


def _bad() -> dict[str, Any]:
    return _entity(subtype="Private")


def test_guard_catches_a_generator_mapper() -> None:
    def map_fake(bundle: dict[str, Any]):
        yield _bad()

    list(guard.wrap(map_fake)({}))
    (violation,) = guard.drain()
    assert violation[0] == "map_fake"
    assert "'Private'" in violation[2]


def test_guard_catches_a_bundle_mapper() -> None:
    def map_fake(bundle: dict[str, Any]) -> BODSBundle:
        return BODSBundle(statements=[_bad()])

    guard.wrap(map_fake)({})
    assert len(guard.drain()) == 1


def test_guard_catches_a_list_mapper() -> None:
    def map_fake(bundle: dict[str, Any]) -> list[dict[str, Any]]:
        return [_bad(), _entity(details="Private")]

    guard.wrap(map_fake)({})
    assert len(guard.drain()) == 1


def test_guard_preserves_laziness_and_output() -> None:
    seen: list[int] = []

    def map_fake(bundle: dict[str, Any]):
        for i in range(3):
            seen.append(i)
            yield _entity(details=str(i))

    gen = guard.wrap(map_fake)({})
    assert seen == []
    out = list(gen)
    assert [s["recordDetails"]["entityType"]["details"] for s in out] == ["0", "1", "2"]
    assert guard.drain() == []


def test_wrapping_is_idempotent() -> None:
    def map_fake(bundle: dict[str, Any]):
        yield from ()

    once = guard.wrap(map_fake)
    assert guard.wrap(once) is once
    assert guard.wrap(map_fake) is once
