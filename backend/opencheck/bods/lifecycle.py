"""Has this relationship ended? (Phase 219)

BODS v0.4 says a relationship has ended in two places, and a publisher may use
either or both:

* the **record** — ``recordStatus: "closed"`` on the relationship statement;
* each **interest** — an ``endDate``.

Open Ownership's UK PSC extract (OpenCheck's demo graph) has both on 78
relationships and six closed relationships with no ``endDate`` at all, so
reading only one of the two misses cases. CAC Nigeria's INACTIVE rows are the
live example of the second shape: closed, no cessation date published.

This is the rule for the exported PDF / HTML / Markdown diagram. The on-screen
graph states the same rule in ``frontend/src/lib/relationshipStatus.ts``; the
two are pinned by parallel tests over the same cases
(``tests/test_bods_lifecycle.py`` and ``relationshipStatus.test.ts``), not by a
shared file — change one and change the other.

Pure, side-effect-free.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

_ISO_DAY = re.compile(r"^(\d{4}-\d{2}-\d{2})")

_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


def today_iso() -> str:
    """Today as ``YYYY-MM-DD`` in UTC — the form BODS dates compare in."""
    return datetime.now(UTC).date().isoformat()


def _iso_day(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    m = _ISO_DAY.match(value)
    return m.group(1) if m else None


def record_closed(stmt: dict[str, Any] | None) -> bool:
    """Is this statement's record closed?"""
    return bool(stmt) and stmt.get("recordStatus") == "closed"


def interest_ended(interest: dict[str, Any], closed: bool, as_of: str | None = None) -> bool:
    """An interest has ended when its record is closed, or its ``endDate`` is on
    or before ``as_of``. A future ``endDate`` is a scheduled end, not an ended
    one; an unreadable one is ignored rather than guessed at."""
    if closed:
        return True
    end = _iso_day(interest.get("endDate"))
    return end is not None and end <= (as_of or today_iso())


@dataclass(frozen=True)
class Lifecycle:
    ended: bool
    #: The latest published ``endDate`` on an ended relationship; ``None`` for a
    #: closed record that names no date — never invented.
    ended_on: str | None = None


def relationship_lifecycle(
    interests: list[dict[str, Any]], closed: bool, as_of: str | None = None
) -> Lifecycle:
    """Ended when the record is closed or every interest has ended. A
    relationship with no interests has ended only when its record is closed."""
    as_of = as_of or today_iso()
    ended = closed or (bool(interests) and all(interest_ended(i, False, as_of) for i in interests))
    if not ended:
        return Lifecycle(False)
    dated = [d for d in (_iso_day(i.get("endDate")) for i in interests) if d and d <= as_of]
    return Lifecycle(True, max(dated) if dated else None)


def statement_lifecycle(stmt: dict[str, Any], as_of: str | None = None) -> Lifecycle:
    """:func:`relationship_lifecycle` for one relationship statement."""
    rd = stmt.get("recordDetails") or {}
    return relationship_lifecycle(list(rd.get("interests") or []), record_closed(stmt), as_of)


def ended_phrase(ended_on: str | None) -> str:
    """``"ended 30 November 2024"``, or ``"ended"`` when no usable date."""
    if not ended_on:
        return "ended"
    try:
        y, m, d = (int(p) for p in ended_on.split("-"))
    except ValueError:
        return "ended"
    if not (1 <= m <= 12) or not y or not d:
        return "ended"
    return f"ended {d} {_MONTHS[m - 1]} {y}"
