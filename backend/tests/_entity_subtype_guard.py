"""Registry-wide guard: no mapper may emit an invalid ``entityType.subtype``.

Phase 214. ``entityType.subtype`` is a closed codelist in BODS v0.4, and four
mappers wrote the register's own entity-type wording into it for months
without a test noticing: the one libcovebods case that existed validated a
fixture the mapper never read, and three of the mappers had no case at all.
A per-mapper test only protects the mappers someone remembered to test.

So instead of relying on per-mapper coverage, ``tests/conftest.py`` calls
:func:`install` at import time, before any test module is imported. It
replaces every source mapper (``map_*``, but not the ``map_to_*`` exporters)
in the ``opencheck.bods`` namespaces with a wrapper that inspects each
statement the mapper yields or returns. Every existing mapper test fixture
in the suite therefore runs through the check, and so will the next mapper's.

A violation is recorded rather than raised, because many callers (the lookup
router, the batch runner) catch mapper exceptions and degrade the source —
a raise could be swallowed and the test would still pass. The autouse
fixture in ``conftest.py`` fails the test that produced the violation, at
teardown, with the mapper name and the offending value.

``tests/test_entity_subtype_guard.py`` proves the guard is installed on every
mapper, that no module holds an unwrapped reference, and that it catches a
bad statement from each return shape.
"""

from __future__ import annotations

import collections.abc
import functools
import importlib
import pkgutil
import types
from typing import Any, Callable, Iterator

from opencheck.bods.validator import entity_subtype_issue, jurisdiction_input_issues

#: Violations recorded since the last :func:`drain`, as
#: ``(mapper name, statementId, issue)``.
VIOLATIONS: list[tuple[str, str, str]] = []

#: Original (unwrapped) mapper function → its wrapper.
WRAPPED: dict[Callable[..., Any], Callable[..., Any]] = {}

_MARK = "__opencheck_subtype_guard__"


def _namespaces() -> list[types.ModuleType]:
    import opencheck.bods as bods_pkg
    import opencheck.bods.mapper as mapper_mod
    import opencheck.bods.mappers as mappers_pkg

    mods = [bods_pkg, mapper_mod, mappers_pkg]
    for info in pkgutil.iter_modules(mappers_pkg.__path__):
        mods.append(importlib.import_module(f"opencheck.bods.mappers.{info.name}"))
    return mods


def is_source_mapper(name: str, value: Any) -> bool:
    return (
        name.startswith("map_")
        and not name.startswith("map_to_")
        and isinstance(value, types.FunctionType)
    )


#: Mappers that hand on a publisher's own BODS verbatim, whose identifiers
#: are the publisher's to fix, not OpenCheck's (Phase 208: "do not improve"
#: the OECD's statements). Exempt from the identifier-scheme rule only.
PUBLISHER_VERBATIM: frozenset[str] = frozenset({"map_meip"})

#: Mappers that yielded at least one statement since the session began —
#: read by ``test_mapper_contract.py`` to prove every registered source's
#: mapper went through the checks, not just the ones someone tested.
EXERCISED: set[str] = set()


def check_statement(mapper: str, stmt: Any) -> None:
    if not isinstance(stmt, dict):
        return
    EXERCISED.add(mapper)
    sid = str(stmt.get("statementId") or "?")
    # Phase 239: the risk engine's jurisdiction / identifier input contract.
    for issue in jurisdiction_input_issues(
        stmt, identifier_schemes=mapper not in PUBLISHER_VERBATIM
    ):
        VIOLATIONS.append((mapper, sid, issue))
    if stmt.get("recordType") != "entity":
        return
    issue = entity_subtype_issue(stmt.get("recordDetails") or {})
    if issue:
        VIOLATIONS.append((mapper, sid, issue))


def _checked(mapper: str, gen: Iterator[Any]) -> Iterator[Any]:
    for stmt in gen:
        check_statement(mapper, stmt)
        yield stmt


def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
    if getattr(fn, _MARK, False):
        return fn
    if fn in WRAPPED:
        return WRAPPED[fn]

    @functools.wraps(fn)
    def guarded(*args: Any, **kwargs: Any) -> Any:
        result = fn(*args, **kwargs)
        # Any iterator, not only a generator: the passthrough mappers
        # (``map_bods_gleif``, ``map_bods_uk_psc``, ``map_meip``) return
        # ``iter(list)``, which went through this guard unchecked until
        # Phase 239's coverage test noticed.
        if isinstance(result, collections.abc.Iterator):
            return _checked(fn.__name__, result)
        statements = getattr(result, "statements", None)
        if isinstance(statements, list):
            for stmt in statements:
                check_statement(fn.__name__, stmt)
        elif isinstance(result, list):
            for stmt in result:
                check_statement(fn.__name__, stmt)
        return result

    setattr(guarded, _MARK, True)
    WRAPPED[fn] = guarded
    return guarded


def install() -> None:
    """Wrap every source mapper in every ``opencheck.bods`` namespace."""
    for mod in _namespaces():
        for name, value in list(vars(mod).items()):
            if is_source_mapper(name, value):
                setattr(mod, name, wrap(value))


def drain() -> list[tuple[str, str, str]]:
    found = list(VIOLATIONS)
    VIOLATIONS.clear()
    return found
