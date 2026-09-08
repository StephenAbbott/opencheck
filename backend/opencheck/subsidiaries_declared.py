"""Declared subsidiaries — every non-GLEIF list OpenCheck holds, in one shape.

Phase 185. The Subsidiaries tab brings together what each source says a
company owns. GLEIF Level 2 stays where it is (``subsidiaries.py``,
``GET /subsidiaries``) because it is the one list detailed enough to reuse —
every child carries an LEI, the relation is typed, and it maps to BODS. The
lists here are weaker in different ways and are gathered for *display*:

* **OECD-UNSD MEIP** — the register of the 500 largest MNEs' subsidiaries.
  LEI-keyed, so every row links to its own OpenCheck page, but the committed
  data holds only the LEI-carrying subset of what MEIP publishes.
* **EITI Company Assessment** — names a supporting company declared to EITI,
  with a country and the report years. No identifier of any kind.
* **GEM (Climate TRACE ownership)** — the entities GEM records the subject as
  directly owning, with a percentage where GEM has one, and an LEI where the
  GLEIF-certified GEM↔LEI mapping (or GEM's own LEI column) supplies it.

Sources disagree, and are meant to: no two of them measure the same thing.
The response keeps them apart — one block per source, each saying what it
measures — rather than merging them into a single "the subsidiaries" list
that would assert a completeness nobody publishes. The one thing the tab does
try to settle is *which rows are companies OpenCheck can open*, which is why
an LEI is attached wherever the data itself supplies one and never guessed.

Not part of the OpenCheck API surface: the API page, the export and the MCP
server keep serving GLEIF only. This endpoint exists for the tab.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .meip import MEIP_URL, meip_declared
from .sources.climatetrace import gem_direct_subsidiaries
from .sources.eiti_assessment import declared_subsidiaries as eiti_declared

__all__ = ["assemble_declared", "DECLARED_SOURCES"]

#: Display metadata, in the order the tab shows them. ``measures`` is the
#: sentence that explains why this list differs from the others — it is the
#: advocacy point of the tab, so it travels with the data rather than living
#: in the component.
DECLARED_SOURCES: dict[str, dict[str, str]] = {
    "meip": {
        "label": "OECD-UNSD MEIP",
        "measures": (
            "the OECD-UNSD Multinational Enterprise Information Platform's "
            "annual register of the subsidiaries of the world's 500 largest "
            "multinational enterprises; only the subsidiaries that carry an "
            "LEI are held here"
        ),
        "homepage": MEIP_URL,
    },
    "eiti_assessment": {
        "label": "EITI Company Assessment",
        "measures": (
            "the controlled subsidiaries a supporting company declared to "
            "EITI, covering its extractive operations in EITI implementing "
            "countries; names and countries only, no identifiers"
        ),
        "homepage": "https://eiti-database.eiti.org/",
    },
    "climatetrace": {
        "label": "Global Energy Monitor (Climate TRACE)",
        "measures": (
            "the entities Global Energy Monitor records as directly owned, "
            "built from asset ownership in the energy sector; a percentage "
            "where GEM has one"
        ),
        "homepage": "https://climatetrace.org/",
    },
}


def _block(source_id: str, **fields: Any) -> dict[str, Any]:
    meta = DECLARED_SOURCES[source_id]
    base: dict[str, Any] = {
        "id": source_id,
        "label": meta["label"],
        "measures": meta["measures"],
        "homepage": meta["homepage"],
        # ``available`` is "the data could be read", ``covered`` is "the
        # subject is in this source's universe". False on either is not a
        # finding about the company, and the two must never render alike.
        "available": True,
        "covered": False,
        "reason": None,
        "total": None,
        "context": None,
        "rows": [],
    }
    base.update(fields)
    base["listed"] = len(base["rows"])
    base["with_lei"] = sum(1 for r in base["rows"] if r.get("lei"))
    return base


def _row(
    name: str,
    *,
    lei: str | None = None,
    country: str | None = None,
    relation: str | None = None,
    percent: float | None = None,
    years: list[str] | None = None,
    via: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "lei": lei,
        "country": country,
        "relation": relation,
        "percent": percent,
        "years": years or [],
        "via": via,
    }


def _meip_block(lei: str) -> dict[str, Any]:
    decl = meip_declared(lei)
    if decl is None:
        return _block("meip", reason="not in the MEIP register")
    rows = [
        _row(
            r["name"],
            lei=r["lei"],
            country=r["country"],
            relation="direct" if r["direct"] else "in group",
            via=r["immediate_parent"],
        )
        for r in decl["rows"]
    ]
    return _block(
        "meip",
        covered=True,
        total=decl["total"],
        context={
            "mode": decl["mode"],
            "name": decl["name"],
            "parent_mne": decl["parent_mne"],
            "immediate_parent": decl["immediate_parent"],
            "with_lei": decl["with_lei"],
        },
        rows=rows,
    )


def _eiti_block(lei: str) -> dict[str, Any]:
    decl = eiti_declared(lei)
    if decl is None:
        return _block("eiti_assessment", reason="not an EITI supporting company")
    rows = [
        _row(r["name"], country=r["country"], relation="declared", years=r["years"])
        for r in decl["rows"]
    ]
    return _block(
        "eiti_assessment",
        covered=True,
        total=len(rows),
        context={"name": decl["name"], "assessment_year": decl["assessment_year"]},
        rows=rows,
    )


def _gem_block(lei: str) -> dict[str, Any]:
    try:
        decl = gem_direct_subsidiaries(lei)
    except Exception as exc:  # pragma: no cover - defensive; GEM parsing is I/O
        return _block("climatetrace", available=False, reason=f"GEM data could not be read ({exc})")
    if decl is None:
        return _block("climatetrace", reason="not a GEM entity")
    if not decl.get("available", True):
        return _block("climatetrace", available=False, reason="GEM ownership data is not loaded")
    rows = [
        _row(
            r["name"],
            lei=r["lei"],
            country=r["country"],
            relation="direct",
            percent=r["percent"],
            via=r["gem_id"],
        )
        for r in decl["rows"]
    ]
    return _block(
        "climatetrace",
        covered=True,
        total=len(rows),
        context={"gem_id": decl["gem_id"], "name": decl["name"]},
        rows=rows,
    )


async def assemble_declared(lei: str) -> dict[str, Any]:
    """Every declared-subsidiary list OpenCheck holds for ``lei``.

    The MEIP and EITI reads are dictionary lookups over committed data; GEM
    may parse (or on a cold host, download) several MB of CSV, so it runs in
    a thread. A source that cannot be read reports ``available: false`` and
    still returns, so one missing file never blanks the tab.
    """
    norm = lei.strip().upper()
    meip_b, eiti_b, gem_b = await asyncio.gather(
        asyncio.to_thread(_meip_block, norm),
        asyncio.to_thread(_eiti_block, norm),
        asyncio.to_thread(_gem_block, norm),
    )
    sources = [meip_b, eiti_b, gem_b]
    return {
        "lei": norm,
        "sources": sources,
        "covered": sum(1 for s in sources if s["covered"]),
        "listed": sum(s["listed"] for s in sources),
        "with_lei": sum(s["with_lei"] for s in sources),
    }
