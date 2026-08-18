"""Override layer that serves canonical BODS bundles from
``data/cache/bods_data/`` for GLEIF and UK Companies House.

Why this exists
---------------

OpenCheck's live mappers (``bods.map_gleif`` / ``bods.map_companies_house``)
produce a thin slice of BODS — the GLEIF Level 2 endpoints don't
walk parent chains beyond direct + ultimate, and the UK Companies
House public JSON doesn't expose the multi-layer PSC chain in a
form that the dagre visualiser can connect into a single graph.

Open Ownership publish *processed* BODS v0.4 datasets at
``bods-data.openownership.org`` with proper interconnected
subject ↔ interestedParty relationships. We pre-extract per-subject
subgraphs from those (see ``scripts/extract_bods_subgraphs.py``)
and ship them as JSON-Lines files under ``data/cache/bods_data/``.

This module is the runtime loader. ``/lookup`` consults it before
falling back to the live mapper:

* ``bods_data/gleif/<LEI>.jsonl`` — GLEIF subgraph for an LEI.
* ``bods_data/uk/<GB-COH>.jsonl`` — UK PSC subgraph for a company
  number.

When a bundle exists, those statements are returned verbatim as the
canonical BODS for that source. The live mapper is skipped — we trust
Open Ownership's processed output more than our own thin slice.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .cache import data_root

#: Display names stamped onto bundle statements whose ``source`` block lacks a
#: ``description``. ``extract_bods_subgraphs.py`` carries only ``source.type``
#: and ``source.url`` through from Open Ownership's Parquet — the assertedBy
#: names live in sub-tables it doesn't read — so without this every fact built
#: from a bundle statement can only be labelled "an OpenCheck source" in the
#: narrative evidence packet (Phase 111 curated regeneration surfaced this on
#: the TAQA/BP/Rosneft summaries). The names must match the corresponding
#: adapters' ``SourceInfo.name`` EXACTLY: the packet builder reverse-maps
#: display name → adapter id to make citation chips link to source cards.
_SOURCE_DESCRIPTIONS: dict[str, str] = {
    "gleif": "GLEIF",
    "uk": "UK Companies House",
}


def _bundle_path(source: str, key: str) -> Path:
    """Return the on-disk JSON-Lines path for a source / key pair.

    Layout: ``data/cache/bods_data/<source>/<key>.jsonl``.
    """
    return data_root() / "cache" / "bods_data" / source / f"{key}.jsonl"


def has_bundle(source: str, key: str) -> bool:
    """Cheap presence check — used by adapters before the network /
    live-transform path."""
    return _bundle_path(source, key).is_file()


def load_bundle(source: str, key: str) -> list[dict[str, Any]] | None:
    """Load a per-subject BODS bundle as a list of statements.

    Returns ``None`` when no bundle exists for the (source, key) pair
    so callers can fall back to the live mapper.
    """
    path = _bundle_path(source, key)
    if not path.is_file():
        return None
    description = _SOURCE_DESCRIPTIONS.get(source)
    statements: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                statement = json.loads(raw)
            except json.JSONDecodeError as exc:
                # Defensive: skip malformed lines rather than crash the
                # whole lookup. The script that writes these is well-
                # tested but a manual edit could break a line.
                raise ValueError(
                    f"{path}:{line_no}: invalid JSON in BODS bundle"
                ) from exc
            # The statements stay verbatim-Open-Ownership except for one
            # display annotation: a missing ``source.description`` gets the
            # canonical source name so downstream provenance labels (the
            # narrative packet's citation chips) never fall back to the
            # anonymous "an OpenCheck source". An existing description is
            # never overwritten.
            if description and isinstance(statement, dict):
                src = statement.get("source")
                if src is None:
                    statement["source"] = {"description": description}
                elif isinstance(src, dict) and not src.get("description"):
                    src["description"] = description
            statements.append(statement)
    return statements


def gleif_bundle_for_lei(lei: str) -> list[dict[str, Any]] | None:
    """Canonical GLEIF subgraph for a given LEI, or ``None`` if not
    pre-extracted."""
    return load_bundle("gleif", lei.upper())


def uk_bundle_for_company_number(number: str) -> list[dict[str, Any]] | None:
    """Canonical UK PSC subgraph for a given Companies House number, or
    ``None`` if not pre-extracted."""
    return load_bundle("uk", number.strip())
