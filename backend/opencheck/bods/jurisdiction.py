"""An entity statement's jurisdiction, under either spelling (Phase 239).

BODS v0.4 puts an entity's place of registration at
``recordDetails.jurisdiction``. v0.3 called it ``incorporatedInJurisdiction``.
Two adapters (``bods_gleif``, ``bods_uk_psc``) and the script that builds
``data/cache/bods_data/`` wrote the v0.3 key under ``bodsVersion: "0.4"``
until Phase 239, and the risk engine looked for the old key at the top of the
statement rather than inside ``recordDetails`` — so it never found it. On BP
that hid 225 entity statements from the FATF, EU high-risk-third-country and
non-EU checks.

The writers are fixed. This module is for everything written before them: a
replay-cache entry, a saved report, a pre-extracted bundle. ``read`` accepts
every place the jurisdiction has been found; ``upgrade`` renames the v0.3 key
in place so a statement OpenCheck serves under 0.4 says 0.4.
"""

from __future__ import annotations

from typing import Any

LEGACY_KEY = "incorporatedInJurisdiction"


def read(stmt: dict[str, Any]) -> dict[str, Any] | None:
    """The jurisdiction object an entity statement carries, or ``None``."""
    rd = stmt.get("recordDetails")
    rd = rd if isinstance(rd, dict) else {}
    for value in (rd.get("jurisdiction"), rd.get(LEGACY_KEY), stmt.get(LEGACY_KEY)):
        if isinstance(value, dict) and value:
            return value
    return None


def upgrade(stmt: dict[str, Any]) -> dict[str, Any]:
    """Rename a v0.3 ``incorporatedInJurisdiction`` to ``jurisdiction``, in
    place, and return the statement. A statement that already carries
    ``jurisdiction`` keeps it; the legacy key is dropped either way."""
    rd = stmt.get("recordDetails")
    if not isinstance(rd, dict) or LEGACY_KEY not in rd:
        return stmt
    legacy = rd.pop(LEGACY_KEY)
    if isinstance(legacy, dict) and legacy and not rd.get("jurisdiction"):
        rd["jurisdiction"] = {k: v for k, v in legacy.items() if v}
    return stmt
