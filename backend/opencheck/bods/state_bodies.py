"""Type a government owner as a BODS ``stateBody`` from its GLEIF category.

Phase 240. Most sources that name a company's owner do not say whether the
owner is part of a state. OpenSanctions files Norway's
``FINANSDEPARTEMENTET`` — 67% of Equinor, and alternately named "Government of
Norway" — as a ``Company`` tagged ``gov.soe``, so its mapper typed it a
``registeredEntity`` and ``STATE_CONTROLLED`` never fired. The record does carry
its LEI, ``549300L0BT3FJTN9MX24``, and GLEIF files that LEI under the entity
category ``RESIDENT_GOVERNMENT_ENTITY`` (sub-category ``STATE_GOVERNMENT``).

GLEIF's category describes a government body **itself**, never a company the
government owns — which is exactly the question for a party that holds shares.
(It is useless for recognising an SOE: Pertamina is ``GENERAL``; see the
``opencheck-soe-state-ownership`` notes.) So any entity statement, from any
source, that carries an ``XI-LEI`` identifier whose Golden Copy row says
``RESIDENT_GOVERNMENT_ENTITY`` is typed ``stateBody`` here.

* **A local read, never a request.** The category comes from the Golden Copy
  mirror (``entity_pages.get_store()``); without the file — local development,
  the test suite — nothing changes. No GLEIF API call is added to a lookup.
* **The source's own word is kept.** The retyped statement carries an
  ``entityType.details`` naming the basis and a ``transformation`` annotation
  on ``/recordDetails/entityType/type`` recording what the source typed it,
  so the "as filed" reading is recoverable.
* Only ``registeredEntity`` / ``legalEntity`` / ``unknownEntity`` parties are
  retyped: a ``state`` stays a state, and an ``arrangement`` or an anonymous
  party is not a government because of an identifier attached to it.
* Statements are copied before they are changed — a stored bundle is cached
  and shared between requests.
* **A publisher's own BODS is left as published** (``VERBATIM_SOURCES``): the
  OECD's MEIP statements are shown beside GLEIF precisely because they are
  the OECD's (Phase 208), so OpenCheck does not retype them.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

from . import annotations as _ann

log = logging.getLogger(__name__)

GOVERNMENT_CATEGORY = "RESIDENT_GOVERNMENT_ENTITY"

#: Sources whose statements are a publisher's own BODS, passed through verbatim.
VERBATIM_SOURCES: frozenset[str] = frozenset({"meip"})

#: Entity types a GLEIF government category may replace.
_RETYPABLE = frozenset({"registeredEntity", "legalEntity", "unknownEntity"})

#: GLEIF sub-categories, in words.
_SUBCATEGORY_WORDS = {
    "CENTRAL_GOVERNMENT": "central government",
    "STATE_GOVERNMENT": "state government",
    "LOCAL_GOVERNMENT": "local government",
    "SOCIAL_SECURITY": "social security",
}

#: Kept in step with ``risk.GLEIF_GOVERNMENT_DETAILS_PREFIX``, which the
#: STATE_CONTROLLED caveat keys on (a test pins the two together).
DETAILS_PREFIX = "Resident government entity in GLEIF"


def _lei_of(stmt: dict[str, Any]) -> str | None:
    rd = stmt.get("recordDetails") or {}
    for ident in rd.get("identifiers") or ():
        if isinstance(ident, dict) and ident.get("scheme") == "XI-LEI":
            lei = str(ident.get("id") or "").strip().upper()
            if len(lei) == 20:
                return lei
    return None


def _default_lookup(leis: list[str]) -> dict[str, dict[str, Any]]:
    """``{lei: {category, subCategory}}`` from the Golden Copy mirror."""
    try:
        from .. import entity_pages
    except Exception:  # noqa: BLE001 — never fail a mapping over this
        return {}
    store = entity_pages.get_store()
    if store is None:
        return {}
    try:
        rows = store.get_many(leis)
    except Exception as exc:  # noqa: BLE001
        log.warning("state_bodies: mirror read failed: %s", exc)
        return {}
    out: dict[str, dict[str, Any]] = {}
    for lei, row in rows.items():
        detail = row.detail or {}
        if detail.get("category"):
            out[lei] = {
                "category": detail.get("category"),
                "subCategory": detail.get("subCategory"),
            }
    return out


def classify_government_entities(
    statements: list[dict[str, Any]],
    *,
    source_id: str | None = None,
    lookup: Any = None,
    creation_date: str | None = None,
) -> list[dict[str, Any]]:
    """Return ``statements`` with GLEIF-government parties typed ``stateBody``.

    ``lookup(leis) -> {lei: {"category", "subCategory"}}`` defaults to the
    Golden Copy mirror; tests pass their own. The list is returned unchanged
    (the same object) when nothing is retyped.
    """
    if source_id in VERBATIM_SOURCES:
        return statements
    candidates: dict[int, str] = {}
    for i, stmt in enumerate(statements):
        if stmt.get("recordType") != "entity":
            continue
        etype = ((stmt.get("recordDetails") or {}).get("entityType") or {}).get("type")
        if etype not in _RETYPABLE:
            continue
        lei = _lei_of(stmt)
        if lei:
            candidates[i] = lei
    if not candidates:
        return statements
    categories = (lookup or _default_lookup)(sorted(set(candidates.values())))
    if not categories:
        return statements

    out = list(statements)
    for i, lei in candidates.items():
        info = categories.get(lei) or {}
        if info.get("category") != GOVERNMENT_CATEGORY:
            continue
        stmt = copy.deepcopy(out[i])
        rd = stmt.setdefault("recordDetails", {})
        before = dict(rd.get("entityType") or {})
        sub = _SUBCATEGORY_WORDS.get(str(info.get("subCategory") or ""))
        details = f"{DETAILS_PREFIX} ({sub})" if sub else DETAILS_PREFIX
        rd["entityType"] = {"type": "stateBody", "details": details}
        filed = before.get("type") or "registeredEntity"
        if before.get("details"):
            filed += f" ({before['details']})"
        _ann.annotate(
            stmt,
            _ann.transformation(
                _ann.pointer("recordDetails", "entityType", "type"),
                f"The source typed this party {filed}. OpenCheck types it a "
                f"state body because GLEIF files its LEI {lei} under the "
                f"entity category {GOVERNMENT_CATEGORY}"
                + (f" ({info.get('subCategory')})" if info.get("subCategory") else "")
                + ".",
                transformed_content="stateBody",
                creation_date=creation_date,
            ),
        )
        out[i] = stmt
    return out
