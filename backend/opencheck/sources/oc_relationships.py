"""OpenCorporates Relationships bulk-file lookup.

The OC Relationships dataset is distributed as a bulk CSV file (not
available via the standard REST API). This module provides a lookup
class that indexes the file in memory and returns matching relationships
for a given company, normalised to the same internal format used by the
OpenCorporates adapter's network mapper so that ``map_opencorporates``
works unchanged.

CSV schema (as of the OC Relationships data dictionary):
  relationship_type            — branch | control_statement | subsidiary | share_parcel
  oc_relationship_identifier   — unique integer ID for the relationship
  subject_entity_name          — name of the controlling / parent entity
  subject_entity_company_number
  subject_entity_jurisdiction_code
  object_entity_name           — name of the controlled / subsidiary entity
  object_entity_company_number
  object_entity_jurisdiction_code
  percentage_min_share_ownership
  percentage_max_share_ownership
  percentage_min_voting_rights
  percentage_max_voting_rights
  number_of_shares
  start_date_type              — at | before | after (qualifier on start_date)
  start_date
  end_date_type                — at | before | after (qualifier on end_date)
  end_date
  created_at
  updated_at

BODS direction convention (same as the live network mapper):
  subject   = the entity being owned / controlled  (object_entity in OC CSV)
  intParty  = the entity that owns / controls       (subject_entity in OC CSV)

Usage
-----
    lookup = OCRelationshipsBulkLookup.from_file("/path/to/relationships.csv")
    network_payload = lookup.get_network("gb", "00445790")
    # network_payload is ready to pass into map_opencorporates as bundle["network"]

The lookup is NOT activated by default. The OpenCorporates adapter only
uses it when ``OPENCORPORATES_RELATIONSHIPS_FILE`` is configured in the
environment. See ``opencheck/config.py``.
"""

from __future__ import annotations

import csv
import logging
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _normalise_ocid(jurisdiction_code: str, company_number: str) -> str:
    """Return a canonical lookup key: ``{jurisdiction}/{number}``."""
    return f"{jurisdiction_code.lower().strip()}/{company_number.strip()}"


def _float_or_none(value: str) -> float | None:
    if not value or not value.strip():
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _date_or_none(value: str) -> str | None:
    return value.strip() or None


def _row_to_network_relationship(row: dict[str, str]) -> dict[str, Any]:
    """Convert a CSV row to the normalised relationship dict understood by
    ``_oc_parse_network_relationships`` (Style A, unwrapped)."""
    start_date = _date_or_none(row.get("start_date", ""))
    end_date = _date_or_none(row.get("end_date", ""))
    start_type = (row.get("start_date_type") or "").strip()  # at | before | after

    # Build a human-readable details note when the date qualifier is imprecise.
    details_parts: list[str] = []
    if start_date and start_type and start_type != "at":
        details_parts.append(f"Start date is {start_type} {start_date}")
    end_type = (row.get("end_date_type") or "").strip()
    if end_date and end_type and end_type != "at":
        details_parts.append(f"End date is {end_type} {end_date}")
    oc_id = row.get("oc_relationship_identifier", "")
    if oc_id:
        details_parts.append(f"OC relationship ID: {oc_id}")

    rel: dict[str, Any] = {
        "relationship_type": row.get("relationship_type", "").strip(),
        # "source" = the controlling/parent entity (subject in OC CSV)
        "source": {
            "company": {
                "name": row.get("subject_entity_name", "").strip(),
                "jurisdiction_code": row.get("subject_entity_jurisdiction_code", "").strip(),
                "company_number": row.get("subject_entity_company_number", "").strip(),
            }
        },
        # "target" = the controlled/subsidiary entity (object in OC CSV)
        "target": {
            "company": {
                "name": row.get("object_entity_name", "").strip(),
                "jurisdiction_code": row.get("object_entity_jurisdiction_code", "").strip(),
                "company_number": row.get("object_entity_company_number", "").strip(),
            }
        },
        "percentage_min_share_ownership": _float_or_none(
            row.get("percentage_min_share_ownership", "")
        ),
        "percentage_max_share_ownership": _float_or_none(
            row.get("percentage_max_share_ownership", "")
        ),
        "percentage_min_voting_rights": _float_or_none(
            row.get("percentage_min_voting_rights", "")
        ),
        "percentage_max_voting_rights": _float_or_none(
            row.get("percentage_max_voting_rights", "")
        ),
        "start_date": start_date,
        "end_date": end_date,
    }

    if details_parts:
        rel["details"] = "; ".join(details_parts)

    return rel


class OCRelationshipsBulkLookup:
    """In-memory index of an OpenCorporates Relationships CSV file.

    Indexes every row by the OC identifier of both the subject (parent)
    and object (subsidiary) entity so lookups by either side are O(1).

    The full OC Relationships file can be very large (tens of millions of
    rows). For a demo or development scenario, supply a filtered excerpt
    instead of the full file.
    """

    def __init__(self) -> None:
        # Maps normalised ocid → list of raw CSV rows
        self._by_subject: dict[str, list[dict[str, str]]] = defaultdict(list)
        self._by_object: dict[str, list[dict[str, str]]] = defaultdict(list)
        self._row_count: int = 0

    @classmethod
    def from_file(cls, path: str | Path) -> OCRelationshipsBulkLookup:
        """Load and index a relationships CSV file.

        Args:
            path: Absolute or relative path to the CSV file.

        Returns:
            An indexed ``OCRelationshipsBulkLookup`` instance.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file is missing required columns.
        """
        lookup = cls()
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"OC relationships file not found: {path}")

        required = {
            "relationship_type",
            "subject_entity_jurisdiction_code",
            "subject_entity_company_number",
            "object_entity_jurisdiction_code",
            "object_entity_company_number",
        }

        with path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                raise ValueError(f"OC relationships file appears empty: {path}")
            missing = required - set(reader.fieldnames)
            if missing:
                raise ValueError(
                    f"OC relationships file missing columns: {missing}. "
                    f"Got: {list(reader.fieldnames)}"
                )

            for row in reader:
                lookup._row_count += 1
                subj_key = _normalise_ocid(
                    row.get("subject_entity_jurisdiction_code", ""),
                    row.get("subject_entity_company_number", ""),
                )
                obj_key = _normalise_ocid(
                    row.get("object_entity_jurisdiction_code", ""),
                    row.get("object_entity_company_number", ""),
                )
                if subj_key != "/":
                    lookup._by_subject[subj_key].append(row)
                if obj_key != "/" and obj_key != subj_key:
                    lookup._by_object[obj_key].append(row)

        logger.info(
            "Loaded OC relationships bulk file: %d rows, %d subject keys, %d object keys",
            lookup._row_count,
            len(lookup._by_subject),
            len(lookup._by_object),
        )
        return lookup

    def get_network(
        self,
        jurisdiction_code: str,
        company_number: str,
    ) -> dict[str, Any]:
        """Return a network payload for the given company, ready to be passed
        into ``map_opencorporates`` as ``bundle["network"]``.

        Collects all relationships where the company appears as either the
        subject (parent/controller) or the object (subsidiary/controlled).
        Skips relationships with an ``end_date`` set (historical).

        Returns:
            A dict in the "Style A" format: ``{"relationships": [{"relationship": {...}}, …]}``
            Compatible with ``_oc_parse_network_relationships`` in mapper.py.
        """
        key = _normalise_ocid(jurisdiction_code, company_number)
        rows = self._by_subject.get(key, []) + self._by_object.get(key, [])

        # Deduplicate by oc_relationship_identifier in case of index overlap
        seen_ids: set[str] = set()
        relationships: list[dict[str, Any]] = []
        for row in rows:
            rel_id = row.get("oc_relationship_identifier", "")
            if rel_id and rel_id in seen_ids:
                continue
            if rel_id:
                seen_ids.add(rel_id)
            # Skip historical relationships — mapper also does this but
            # filtering early keeps the payload lean.
            if _date_or_none(row.get("end_date", "")):
                continue
            relationships.append({"relationship": _row_to_network_relationship(row)})

        logger.debug(
            "OC bulk lookup %s: %d active relationships found",
            key,
            len(relationships),
        )
        return {"relationships": relationships}

    @property
    def row_count(self) -> int:
        return self._row_count


@lru_cache(maxsize=1)
def _get_bulk_lookup(file_path: str) -> OCRelationshipsBulkLookup:
    """Module-level cached loader — file is only read once per process."""
    return OCRelationshipsBulkLookup.from_file(file_path)


def get_bulk_lookup_for_settings() -> OCRelationshipsBulkLookup | None:
    """Return the configured bulk lookup, or None if not configured.

    Reads ``OPENCORPORATES_RELATIONSHIPS_FILE`` from settings. Safe to
    call on every request — the file is only loaded once thanks to
    ``lru_cache``.
    """
    from ..config import get_settings

    settings = get_settings()
    file_path = settings.opencorporates_relationships_file
    if not file_path:
        return None
    try:
        return _get_bulk_lookup(file_path)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Cannot load OC relationships bulk file: %s", exc)
        return None
