"""An officer appointment, said in the interest itself.

Some registers name officers whose position has no role code in BODS v0.4 —
a company secretary, an agent, a representative, an incorporator. The
mappers give those ``otherInfluenceOrControl`` so the person stays visible
without the claim that they run the company (Phase 197). The type alone
cannot say that, though: every reader that classifies an edge by its interest
type (the canvas, the PDF diagram, the MCP ownership map) drew a company
secretary as a controller.

So an officer appointment carries its position in a BODS extension field on
the interest, ``opencheckOfficerPosition`` — the ``opencheckSourceId``
precedent (Phase 267): v0.4's objects are open and libcovebods lists an
unknown key as an additional field, not an error. A reader that knows the key
treats the interest as a role; one that does not still reads the type it
always read.

Mirrored in ``frontend/src/lib/bodsGraph.ts`` (``OFFICER_POSITION_KEY``,
``officerLabel``); ``tests/test_reporting_diagram_parity.py`` pins the pair.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: The extension key on a BODS interest object.
OFFICER_POSITION_KEY = "opencheckOfficerPosition"

#: What an unnamed officer position is called.
UNNAMED_POSITION = "officer"


def mark_officer(interest: dict[str, Any], position: str | None) -> dict[str, Any]:
    """Set the officer marker on ``interest`` (in place) and return it."""
    interest[OFFICER_POSITION_KEY] = (position or "").strip() or UNNAMED_POSITION
    return interest


def officer_position(interest: Mapping[str, Any]) -> str | None:
    """The position an officer interest names, or None for any other interest."""
    value = interest.get(OFFICER_POSITION_KEY)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def officer_label(position: str) -> str:
    """The position as a label: first letter capitalised, the rest as filed
    ("secretary" → "Secretary", "Company Secretary" unchanged)."""
    text = position.strip()
    return text[:1].upper() + text[1:]
