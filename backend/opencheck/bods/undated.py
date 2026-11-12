"""Ownership and control relationships that no source dates.

BODS v0.4 leaves an interest's ``startDate`` and ``endDate`` optional, and
a missing date means "not stated", not "since forever" or "still true".
OpenCheck reads an undated relationship as current (``bods.lifecycle``),
which is the only reading open to it, but a reader cannot tell from the
graph whether the holding was filed last week or copied from a list of
2021. Rosneft's FullCheck of 11 Nov 2026 showed BP Russian Investments
holding about 20% four years after BP announced its exit: every one of the
120 ownership relationships in that lookup was undated.

Stephen's decision on the ticket "How to deal with data sources where
relationships have no start date or end date" (11 Nov 2026): keep undated
relationships, never drop them, and say so where they are read. This module
is the one rule for "undated":

* a relationship statement,
* not closed (a record closed without a date is ended, the CAC rule),
* with at least one ownership or control interest — a role (director,
  board member, an officer marked with ``opencheckOfficerPosition``) is an
  appointment, not a holding, and is never called undated here,
* a GLEIF reporting-exception bridge (interest ``details`` opening "GLEIF
  reporting exception", what ``mapper._gleif_exception_statements`` writes
  and ``mapper.gleif_reporting_exception`` reads back) is a filing that
  there is no parent to report, not a holding, and is never called undated
  either (LUKOIL's NO_KNOWN_PERSON bridges carried the note, 12 Nov 2026),
* and no interest carrying a ``startDate`` or an ``endDate``.

``note_undated`` keeps one ``commenting`` annotation (``UNDATED_NOTE``) on
such a statement and removes it from one that has since gained a date. It
runs as each statement leaves a ``@content_addressed`` mapper, so no mapper
calls it; annotations are outside the content digest, so no id moves.

Mirrored in ``frontend/src/lib/undated.ts`` (``isUndatedRelationship``,
``UNDATED_LABEL``); ``tests/test_undated_relationships.py`` parses the
TypeScript.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .annotations import commenting
from .officer_roles import officer_position

#: Interest types that name an appointment, not a holding.
ROLE_INTEREST_TYPES: frozenset[str] = frozenset(
    {"seniorManagingOfficial", "boardMember", "boardChair"}
)

#: How a GLEIF reporting-exception bridge's interest ``details`` open — the
#: same prefix ``mapper._EXCEPTION_DETAILS`` reads (not imported: ``mapper``
#: imports ``statements``, which calls this module).
EXCEPTION_DETAILS_PREFIX = "GLEIF reporting exception"

#: Where the note points.
UNDATED_TARGET = "/recordDetails/interests"

#: The note's sentence. Describes; never judges the holding true or false.
UNDATED_NOTE = (
    "The source gives no start or end date for this relationship. OpenCheck "
    "reads it as current, but it may describe a holding that has since ended."
)

#: The short form the graph, the tree and the reports print beside the edge.
UNDATED_LABEL = "no date given"


def is_reporting_exception(interest: Mapping[str, Any]) -> bool:
    """True for the interest of a GLEIF reporting-exception bridge."""
    return str(interest.get("details") or "").startswith(EXCEPTION_DETAILS_PREFIX)


def _holding_interests(interests: Any) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for interest in interests or []:
        if not isinstance(interest, Mapping):
            continue
        if interest.get("type") in ROLE_INTEREST_TYPES:
            continue
        if officer_position(interest) is not None:
            continue
        if is_reporting_exception(interest):
            continue
        out.append(interest)
    return out


def is_undated(stmt: Mapping[str, Any]) -> bool:
    """True for a current ownership or control relationship with no date."""
    if stmt.get("recordType") != "relationship":
        return False
    if stmt.get("recordStatus") == "closed":
        return False
    interests = (stmt.get("recordDetails") or {}).get("interests") or []
    if not _holding_interests(interests):
        return False
    for interest in interests:
        if isinstance(interest, Mapping) and (
            interest.get("startDate") or interest.get("endDate")
        ):
            return False
    return True


def _is_note(annotation: Any) -> bool:
    return (
        isinstance(annotation, Mapping)
        and annotation.get("motivation") == "commenting"
        and annotation.get("description") == UNDATED_NOTE
    )


def note_undated(stmt: dict[str, Any]) -> dict[str, Any]:
    """Add or remove the undated note on an OpenCheck statement, in place.

    Another publisher's statements (no ``opencheck-`` recordId) are left
    alone — they are published verbatim (Phase 317's rule).
    """
    rid = stmt.get("recordId")
    if not isinstance(rid, str) or not rid.startswith("opencheck-"):
        return stmt
    if stmt.get("recordType") != "relationship":
        return stmt
    annotations = stmt.get("annotations")
    has_note = isinstance(annotations, list) and any(_is_note(a) for a in annotations)
    if is_undated(stmt):
        if not has_note:
            stmt.setdefault("annotations", []).append(
                commenting(UNDATED_TARGET, UNDATED_NOTE)
            )
    elif has_note and isinstance(annotations, list):
        kept = [a for a in annotations if not _is_note(a)]
        if kept:
            stmt["annotations"] = kept
        else:
            stmt.pop("annotations", None)
    return stmt
