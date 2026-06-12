"""Climate TRACE / Global Energy Monitor adapter.

This adapter bridges two complementary open datasets:

* **Global Energy Monitor (GEM)** — tracks fossil-fuel infrastructure
  worldwide. GEM publishes an ownership tracker (``ownership.zip``) under
  CC BY 4.0 that maps facility ownership to named entities with LEI codes
  where known. OpenCheck downloads and indexes this file so that any LEI
  resolved during a ``/lookup`` call can be cross-referenced to a GEM
  entity ID.

* **Climate TRACE** — provides per-asset and aggregate emissions estimates
  derived from satellite and sensor data. Entities in Climate TRACE share
  the same ``ownerIds`` namespace as GEM entity IDs, so the bridge is
  transparent: look up a LEI → GEM entity ID → Climate TRACE emissions.

Category: **ESG** (environmental, social and governance) — this is not a
customer due diligence source. Data is surfaced separately from CDD sources
in the OpenCheck UI.

Data licensing
--------------
GEM ownership tracker: CC BY 4.0 — use directly, no NC restriction.
Climate TRACE: CC BY 4.0.
GLEIF GEM-to-LEI mapping: CC BY 4.0.
(Do **not** use the OpenSanctions GEM dataset — it adds a NC restriction.)

LEI → GEM entity ID resolution — two sources, merged
------------------------------------------------------
1. **GLEIF certified mapping** (primary): monthly-updated file at
   ``https://mapping.gleif.org/api/v2/gem-lei/latest/download`` published
   jointly by GLEIF and GEM from June 2026. Contains ~5,400 GLEIF-certified
   LEI ↔ GEM entity ID pairs in a two-column CSV (``LEI,GEM``).
   Cached at ``{data_root}/gem/gleif-gem-lei.zip``.

2. **GEM self-reported LEIs** (supplementary fallback): the "Global Legal
   Entity Identifier Index" column in GEM's ownership.zip. Less reliable
   than the certified mapping (unvalidated, may have stale or missing LEIs),
   but covers more entities.  Takes effect only where the GLEIF file has no
   entry.

GLEIF-certified entries override GEM self-reported entries for the same
entity when both are present and differ.

GEM ownership data download — GCS bucket first, GitHub zip fallback
-------------------------------------------------------------------
GEM refreshes its ownership data in the Climate TRACE GCS bucket every two
months (dated ``DDMMYY`` filenames, e.g. ``ownership_all_entities_050826.csv``).
The GitHub ``ownership.zip`` mirror lags behind the bucket (observed: zip on
the Feb 2026 release while the bucket served May 2026), so the bucket is the
preferred source:

1. List ``gs://climate_trace/ownership/`` via the public JSON API and download
   the latest ``ownership_all_entities_*.csv``,
   ``ownership_all_entity_relationships_*.csv`` and
   ``ownership_all_entity_asset_relationships_*.csv``. Cached at
   ``{data_root}/gem/ownership_all_<kind>.csv`` and refreshed when older than
   ``_GEM_MAX_AGE_DAYS``.
2. Fallback: the GitHub zip at
   ``https://github.com/climatetracecoalition/climate-trace-tools/raw/main/
   climate_trace_tools/data/ownership/ownership.zip``, cached at
   ``{data_root}/gem/ownership.zip``. A pre-seeded zip with no cached CSVs is
   used as-is without any network access (offline/test friendly).

The entity-relationship CSV (entity → entity edges with ``percent_of_ownership``)
and the entity-asset CSV (Climate TRACE ``source_id`` → immediate owner) feed an
ownership summary per entity: direct asset count, group (transitive) asset
count, subsidiary count and a sector breakdown.

Climate TRACE API v7 endpoints used
-------------------------------------
``GET /v7/owners?name=<name>``  — owner/entity name search.
``GET /v7/sources/emissions?ownerIds=<id>&year=<year>&gas=co2e_100yr``
    — aggregate emissions for an owner across all direct assets.
``GET /v7/sources?ownerIds=<id>``
    — list of assets owned.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import zipfile
from pathlib import Path
from typing import Any

import httpx

from ..cache import Cache, data_root
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

log = logging.getLogger(__name__)

_GEM_ZIP_URL = (
    "https://github.com/climatetracecoalition/climate-trace-tools/raw/main/"
    "climate_trace_tools/data/ownership/ownership.zip"
)
# Public listing of the Climate TRACE GCS bucket folder GEM refreshes
# every two months. Preferred over the GitHub zip, which lags behind.
_GCS_LIST_URL = (
    "https://storage.googleapis.com/storage/v1/b/climate_trace/o"
    "?prefix=ownership/&fields=items(name,updated)"
)
_GCS_FILE_URL = "https://storage.googleapis.com/climate_trace/{name}"
# The three CSV kinds GEM publishes. Order matters for substring matching:
# "entity_relationships" is NOT a substring of "entity_asset_relationships".
_GEM_CSV_KINDS = ("entities", "entity_relationships", "entity_asset_relationships")
_GEM_MAX_AGE_DAYS = 70  # bucket refreshes roughly every 2 months
# GLEIF-certified GEM Entity ID ↔ LEI mapping.  Published monthly by GLEIF
# and GEM from June 2026 under CC BY 4.0.  Two-column CSV: ``LEI,GEM``.
# The ``/latest/download`` path always serves the most recent release.
# https://www.gleif.org/en/lei-data/lei-mapping/download-gem-to-lei-relationship-files
_GLEIF_GEM_URL = "https://mapping.gleif.org/api/v2/gem-lei/latest/download"

_CT_API = "https://api.climatetrace.org"
_CACHE_NS = "climatetrace"

# Column names in the GEM ownership CSVs.
_LEI_COL = "Global Legal Entity Identifier Index"
_ENTITY_ID_COL = "Entity ID"
_ENTITY_NAME_COL = "Full Name"          # was "Entity Name" in earlier schema
_PARENT_IDS_COL = "Gem parents IDs"
_PARENT_NAMES_COL = "Gem parents"
_COUNTRY_COL = "Headquarters Country"  # ISO 3166-1 alpha-3

# Module-level singletons — built lazily on first access so import is cheap.
_lei_index: dict[str, str] | None = None          # LEI → GEM entity ID
_entity_index: dict[str, dict[str, str]] | None = None  # GEM entity ID → row
_rel_children: dict[str, list[dict[str, Any]]] | None = None  # owner → owned entities
_asset_index: dict[str, list[dict[str, str]]] | None = None   # entity → CT assets


def _gem_zip_path() -> Path:
    return data_root() / "gem" / "ownership.zip"


def _gem_csv_path(kind: str) -> Path:
    """Local cache path for a GCS-sourced GEM CSV (kind in _GEM_CSV_KINDS)."""
    return data_root() / "gem" / f"ownership_all_{kind}.csv"


def _gleif_gem_zip_path() -> Path:
    return data_root() / "gem" / "gleif-gem-lei.zip"


def _csv_age_days(path: Path) -> float:
    import time

    return (time.time() - path.stat().st_mtime) / 86_400


def _download_gem_csvs_from_gcs() -> bool:
    """Download the latest dated GEM CSVs from the Climate TRACE GCS bucket.

    Returns True if at least the entities CSV was downloaded successfully.
    """
    try:
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            listing = client.get(_GCS_LIST_URL)
            listing.raise_for_status()
            items = listing.json().get("items") or []
            ok = False
            for kind in _GEM_CSV_KINDS:
                # Dated filenames, e.g. ownership_all_entities_050826.csv.
                # Sort matching names descending so the lexicographically
                # greatest (not necessarily newest — DDMMYY) doesn't mislead:
                # pick by the GCS "updated" timestamp instead.
                matches = [
                    it for it in items
                    if f"all_{kind}_" in (it.get("name") or "")
                    and it["name"].endswith(".csv")
                ]
                if not matches:
                    continue
                latest = max(matches, key=lambda it: it.get("updated") or "")
                url = _GCS_FILE_URL.format(name=latest["name"])
                r = client.get(url)
                r.raise_for_status()
                path = _gem_csv_path(kind)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(r.content)
                log.info("GEM %s CSV saved from GCS: %s (%d bytes)",
                         kind, latest["name"], len(r.content))
                if kind == "entities":
                    ok = True
            return ok
    except Exception as exc:
        log.warning("Could not download GEM CSVs from GCS: %s", exc)
        return False


def _ensure_gem_data() -> None:
    """Make GEM ownership data available locally.

    Preference order:
    1. Cached GCS CSVs younger than ``_GEM_MAX_AGE_DAYS`` — use as-is.
    2. Stale GCS CSVs — try to refresh from the bucket; keep stale copy on failure.
    3. No CSVs but a pre-seeded ownership.zip — use the zip without any
       network access (keeps offline/test environments untouched).
    4. Nothing on disk — try the GCS bucket first, then fall back to the
       GitHub ownership.zip.
    """
    entities_csv = _gem_csv_path("entities")
    if entities_csv.exists():
        if _csv_age_days(entities_csv) < _GEM_MAX_AGE_DAYS:
            return
        log.info("GEM CSVs are stale — refreshing from GCS bucket")
        _download_gem_csvs_from_gcs()  # keep stale copy if refresh fails
        return

    zip_path = _gem_zip_path()
    if zip_path.exists():
        return

    if _download_gem_csvs_from_gcs():
        return

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading GEM ownership.zip from %s", _GEM_ZIP_URL)
    try:
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            r = client.get(_GEM_ZIP_URL)
            r.raise_for_status()
        zip_path.write_bytes(r.content)
        log.info("GEM ownership.zip saved to %s (%d bytes)", zip_path, len(r.content))
    except Exception as exc:
        log.warning("Could not download GEM ownership.zip: %s", exc)


def _read_gem_csv_text(kind: str) -> str | None:
    """Return the text of a GEM CSV by kind, from cached GCS CSV or the zip.

    Matches zip members by the ``all_<kind>_`` / ``all_<kind>.`` substring so
    both dated (``ownership_all_entities_050826.csv``) and undated
    (``all_entities.csv``) release filenames work.
    """
    path = _gem_csv_path(kind)
    if path.exists():
        try:
            return path.read_text(encoding="utf-8-sig", errors="replace")
        except Exception as exc:
            log.warning("Error reading GEM %s CSV: %s", kind, exc)

    zip_path = _gem_zip_path()
    if not zip_path.exists():
        return None
    try:
        with zipfile.ZipFile(zip_path) as zf:
            candidates = [
                n for n in zf.namelist()
                if (f"all_{kind}_" in n or n.endswith(f"all_{kind}.csv"))
                and not n.startswith("__MACOSX")
                and n.endswith(".csv")
            ]
            if not candidates:
                return None
            with zf.open(candidates[0]) as raw:
                return raw.read().decode("utf-8-sig", errors="replace")
    except Exception as exc:
        log.warning("Error reading %s from GEM zip: %s", kind, exc)
        return None


_GLEIF_GEM_MAX_AGE_DAYS = 32  # re-download after ~one month


def _ensure_gleif_gem_data() -> None:
    """Download the GLEIF-certified GEM Entity ID-to-LEI mapping if absent or stale.

    The mapping is published monthly by GLEIF and GEM.  A cached copy older than
    ``_GLEIF_GEM_MAX_AGE_DAYS`` is treated as stale and re-downloaded automatically
    so the Render instance always uses a reasonably current version.
    """
    import time

    path = _gleif_gem_zip_path()
    if path.exists():
        age_days = (time.time() - path.stat().st_mtime) / 86_400
        if age_days < _GLEIF_GEM_MAX_AGE_DAYS:
            return
        log.info(
            "GLEIF GEM mapping is %.0f days old (max %d) — refreshing",
            age_days,
            _GLEIF_GEM_MAX_AGE_DAYS,
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading GLEIF GEM-to-LEI mapping from %s", _GLEIF_GEM_URL)
    try:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            r = client.get(_GLEIF_GEM_URL)
            r.raise_for_status()
        path.write_bytes(r.content)
        log.info(
            "GLEIF GEM-to-LEI mapping saved to %s (%d bytes)", path, len(r.content)
        )
    except Exception as exc:
        log.warning("Could not download GLEIF GEM-to-LEI mapping: %s", exc)


def _load_gleif_gem_mapping() -> dict[str, str]:
    """Parse the GLEIF-certified GEM-to-LEI zip and return LEI → GEM entity ID.

    The zip contains a single CSV with two columns: ``LEI`` and ``GEM``.
    Returns an empty dict if the file cannot be read.
    """
    _ensure_gleif_gem_data()
    lei_to_gem: dict[str, str] = {}
    path = _gleif_gem_zip_path()
    if not path.exists():
        return lei_to_gem
    try:
        with zipfile.ZipFile(path) as zf:
            candidates = [
                n for n in zf.namelist()
                if n.endswith(".csv") and not n.startswith("__MACOSX")
            ]
            if not candidates:
                return lei_to_gem
            with zf.open(candidates[0]) as raw:
                text = raw.read().decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            lei = (row.get("LEI") or "").strip().upper()
            gem_id = (row.get("GEM") or "").strip()
            if len(lei) == 20 and gem_id:
                lei_to_gem[lei] = gem_id
    except Exception as exc:
        log.warning("Error parsing GLEIF GEM-to-LEI mapping: %s", exc)
    log.info("GLEIF GEM mapping loaded: %d certified LEI → GEM pairs", len(lei_to_gem))
    return lei_to_gem


def _load_gem_indexes() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Parse the GEM entities CSV and build LEI and entity indexes.

    Downloads the GEM data (GCS CSVs preferred, GitHub ownership.zip fallback)
    if it is not already on disk (Render and other ephemeral-filesystem hosts
    start with a clean slate on every deploy, so the download must happen at
    runtime, not build time).

    Returns ``(lei_index, entity_index)`` where:
    * ``lei_index`` maps a normalised LEI string → GEM entity ID.
    * ``entity_index`` maps a GEM entity ID → the full CSV row dict.
    """
    _ensure_gem_data()  # no-op if data already exists

    lei_idx: dict[str, str] = {}
    ent_idx: dict[str, dict[str, str]] = {}

    text = _read_gem_csv_text("entities")
    if text is None:
        log.warning("GEM entities CSV not available — index will be empty")
        return lei_idx, ent_idx

    try:
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            entity_id = (row.get(_ENTITY_ID_COL) or "").strip()
            if not entity_id:
                continue
            ent_idx[entity_id] = dict(row)

            lei_raw = (row.get(_LEI_COL) or "").strip()
            if lei_raw and lei_raw.lower() not in ("", "not found", "n/a"):
                # GEM sometimes stores multiple LEIs in one cell (semicolon-delimited).
                for lei in lei_raw.split(";"):
                    lei = lei.strip().upper()
                    if len(lei) == 20 and lei not in lei_idx:
                        lei_idx[lei] = entity_id

    except Exception as exc:
        log.warning("Error parsing GEM entities CSV: %s", exc)

    log.info(
        "GEM index built: %d entities, %d self-reported LEI → entity mappings",
        len(ent_idx),
        len(lei_idx),
    )

    # Merge GLEIF-certified mappings on top — they take precedence over GEM's
    # self-reported LEI column, which can be unvalidated or stale.
    gleif_mapping = _load_gleif_gem_mapping()
    overrides = 0
    additions = 0
    for lei, gem_id in gleif_mapping.items():
        if lei in lei_idx:
            if lei_idx[lei] != gem_id:
                lei_idx[lei] = gem_id
                overrides += 1
        else:
            lei_idx[lei] = gem_id
            additions += 1
    if gleif_mapping:
        log.info(
            "GLEIF certified mapping applied: %d overrides, %d new LEI → entity entries",
            overrides,
            additions,
        )

    return lei_idx, ent_idx


def _get_indexes() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    global _lei_index, _entity_index
    if _lei_index is None or _entity_index is None:
        _lei_index, _entity_index = _load_gem_indexes()
    return _lei_index, _entity_index


def _load_relationship_indexes() -> tuple[
    dict[str, list[dict[str, Any]]], dict[str, list[dict[str, str]]]
]:
    """Build the entity→subsidiaries and entity→assets indexes.

    * ``rel_children`` maps an owner GEM entity ID → list of
      ``{"entity_id", "name", "percent"}`` for entities it directly owns
      (from ``ownership_all_entity_relationships``; ``percent`` is a float
      or None when GEM doesn't publish a share).
    * ``asset_index`` maps a GEM entity ID → list of
      ``{"source_id", "name", "sector", "subsector"}`` Climate TRACE assets
      it immediately owns (from ``ownership_all_entity_asset_relationships``).

    Both are empty when the CSVs are unavailable (e.g. pre-May-2026 zips
    without them, or offline test environments) — callers must tolerate that.
    """
    rel_children: dict[str, list[dict[str, Any]]] = {}
    asset_index: dict[str, list[dict[str, str]]] = {}

    text = _read_gem_csv_text("entity_relationships")
    if text:
        try:
            for row in csv.DictReader(io.StringIO(text)):
                owner = (row.get("owner_entity_id") or "").strip()
                subject = (row.get("subject_entity_id") or "").strip()
                if not owner or not subject:
                    continue
                pct_raw = (row.get("percent_of_ownership") or "").strip()
                try:
                    pct: float | None = float(pct_raw) if pct_raw else None
                except ValueError:
                    pct = None
                rel_children.setdefault(owner, []).append(
                    {
                        "entity_id": subject,
                        "name": (row.get("subject_name") or "").strip(),
                        "percent": pct,
                    }
                )
        except Exception as exc:
            log.warning("Error parsing GEM entity relationships CSV: %s", exc)

    text = _read_gem_csv_text("entity_asset_relationships")
    if text:
        try:
            for row in csv.DictReader(io.StringIO(text)):
                owner = (row.get("immediate_source_owner_entity_id") or "").strip()
                source_id = (row.get("source_id") or "").strip()
                if not owner or not source_id:
                    continue
                asset_index.setdefault(owner, []).append(
                    {
                        "source_id": source_id,
                        "name": (row.get("source_name") or "").strip(),
                        "sector": (row.get("source_sector") or "").strip(),
                        "subsector": (row.get("source_subsector") or "").strip(),
                    }
                )
        except Exception as exc:
            log.warning("Error parsing GEM entity-asset relationships CSV: %s", exc)

    log.info(
        "GEM relationship indexes built: %d owners with subsidiaries, "
        "%d entities with assets",
        len(rel_children),
        len(asset_index),
    )
    return rel_children, asset_index


def _get_relationship_indexes() -> tuple[
    dict[str, list[dict[str, Any]]], dict[str, list[dict[str, str]]]
]:
    global _rel_children, _asset_index
    if _rel_children is None or _asset_index is None:
        _rel_children, _asset_index = _load_relationship_indexes()
    return _rel_children, _asset_index


_MAX_OWNERSHIP_DEPTH = 25  # GEOT's longest observed chain is 17 hops


def _ownership_summary(entity_id: str) -> dict[str, Any]:
    """Summarise GEM ownership reach for an entity.

    Walks the entity→entity ownership graph downwards (breadth-first, cycle
    safe) and counts Climate TRACE assets owned directly and across the whole
    group. Counts are of distinct assets — GEM ownership chains routinely
    overlap, and shares along a chain must never be summed naively.
    """
    rel_children, asset_index = _get_relationship_indexes()

    direct_assets = asset_index.get(entity_id, [])

    # BFS over owned entities.
    seen: set[str] = {entity_id}
    frontier = [entity_id]
    depth = 0
    while frontier and depth < _MAX_OWNERSHIP_DEPTH:
        next_frontier: list[str] = []
        for eid in frontier:
            for child in rel_children.get(eid, []):
                cid = child["entity_id"]
                if cid not in seen:
                    seen.add(cid)
                    next_frontier.append(cid)
        frontier = next_frontier
        depth += 1

    group_asset_ids: set[str] = set()
    by_sector: dict[str, int] = {}
    for eid in seen:
        for asset in asset_index.get(eid, []):
            sid = asset["source_id"]
            if sid in group_asset_ids:
                continue
            group_asset_ids.add(sid)
            sector = asset.get("sector") or "unknown"
            by_sector[sector] = by_sector.get(sector, 0) + 1

    return {
        "direct_asset_count": len({a["source_id"] for a in direct_assets}),
        "group_asset_count": len(group_asset_ids),
        "subsidiary_count": len(seen) - 1,
        "group_assets_by_sector": by_sector,
        "direct_subsidiaries": rel_children.get(entity_id, []),
    }


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ClimateTRACEAdapter(SourceAdapter):
    """GEM + Climate TRACE adapter — ESG category."""

    id = "climatetrace"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Global Energy Monitor / Climate TRACE",
            homepage="https://globalenergymonitor.org/",
            description=(
                "Global fossil-fuel asset ownership data (GEM) combined with "
                "satellite-derived emissions estimates (Climate TRACE). "
                "LEI resolution uses the GLEIF-certified GEM Entity ID mapping "
                "(June 2026). Enables ESG and climate risk screening by LEI."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Global Energy Monitor, CC BY 4.0. "
                "Climate TRACE, CC BY 4.0. "
                "GLEIF GEM Entity ID-to-LEI mapping, CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            category="esg",
        )

    # ------------------------------------------------------------------
    # Search — Climate TRACE owner name search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind == SearchKind.PERSON:
            return []

        cache_key = f"{_CACHE_NS}/search/{query.lower().strip()}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        try:
            payload = await self._ct_get(
                "/v7/owners",
                params={"name": query},
                cache_key=cache_key,
            )
        except Exception as exc:
            log.warning("Climate TRACE search failed: %s", exc)
            return []

        owners = payload if isinstance(payload, list) else payload.get("owners") or []
        hits: list[SourceHit] = []
        for owner in owners[:10]:
            owner_id = str(owner.get("id") or "")
            name = owner.get("name") or owner_id
            if not owner_id:
                continue
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=owner_id,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"GEM/Climate TRACE entity · ID {owner_id}",
                    identifiers={"gem_entity_id": owner_id},
                    raw=owner,
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch by GEM entity ID
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        entity_id = hit_id.strip()
        cache_key = f"{_CACHE_NS}/fetch/{entity_id}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "entity_id": entity_id, "is_stub": True}

        return await self._fetch_entity_data(entity_id, cache_key)

    # ------------------------------------------------------------------
    # LEI-based lookup (called by /lookup endpoint)
    # ------------------------------------------------------------------

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Map a LEI to a GEM entity ID, then fetch full emissions data.

        Returns ``None`` if the LEI is not present in the GEM ownership index.
        """
        lei_norm = lei.strip().upper()
        lei_idx, ent_idx = _get_indexes()
        entity_id = lei_idx.get(lei_norm)
        if not entity_id:
            return None

        cache_key = f"{_CACHE_NS}/by_lei/{lei_norm}"
        if not self.info.live_available and not self._cache.has(cache_key):
            # Return a stub with just the GEM CSV row — no live emissions.
            row = ent_idx.get(entity_id) or {}
            return _stub_bundle(entity_id, row, lei_norm)

        return await self._fetch_entity_data(entity_id, cache_key)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_entity_data(
        self, entity_id: str, cache_key: str
    ) -> dict[str, Any]:
        """Fetch full GEM + Climate TRACE data for a GEM entity ID."""
        _, ent_idx = _get_indexes()
        gem_row = ent_idx.get(entity_id) or {}

        # Aggregate emissions (2024, CO2e 100-year GWP)
        emissions_payload: dict[str, Any] = {}
        assets_payload: list[dict[str, Any]] = []

        try:
            emissions_payload = await self._ct_get(
                "/v7/sources/emissions",
                params={
                    "ownerIds": entity_id,
                    "year": "2024",
                    "gas": "co2e_100yr",
                },
                cache_key=f"{cache_key}/emissions",
            )
        except Exception as exc:
            log.warning("Climate TRACE emissions fetch failed for %s: %s", entity_id, exc)

        try:
            assets_raw = await self._ct_get(
                "/v7/sources",
                params={"ownerIds": entity_id},
                cache_key=f"{cache_key}/assets",
            )
            assets_payload = (
                assets_raw if isinstance(assets_raw, list)
                else assets_raw.get("sources") or []
            )
        except Exception as exc:
            log.warning("Climate TRACE sources fetch failed for %s: %s", entity_id, exc)

        bundle = {
            "source_id": self.id,
            "entity_id": entity_id,
            "entity_name": gem_row.get(_ENTITY_NAME_COL) or entity_id,
            "lei": (gem_row.get(_LEI_COL) or "").strip(),
            "gem_row": gem_row,
            "emissions": _parse_emissions(emissions_payload),
            "assets": assets_payload,
            "parents": _parse_parents(gem_row),
            "ownership": _ownership_summary(entity_id),
            "is_stub": False,
        }
        self._cache.put(cache_key, bundle)
        return bundle

    async def _ct_get(
        self,
        path: str,
        params: dict[str, str],
        cache_key: str,
    ) -> Any:
        """GET from the Climate TRACE API with caching."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        url = _CT_API + path
        async with build_client() as client:
            response = await client.get(url, params=params)
            if not response.is_success:
                log.warning(
                    "Climate TRACE API %s returned %s", url, response.status_code
                )
                return {}
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="E0",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub GEM/Climate TRACE result — set OPENCHECK_ALLOW_LIVE=true "
                    "to query live data."
                ),
                identifiers={"gem_entity_id": "E0"},
                raw={},
                is_stub=True,
            )
        ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_emissions(payload: Any) -> dict[str, Any]:
    """Normalise the Climate TRACE v7 emissions response to a flat summary.

    Climate TRACE API v7 ``/v7/sources/emissions`` response shape::

        {
            "totals": {
                "summaries": [{"gas": "co2e_100yr", "emissionsQuantity": 345183438.27, ...}],
                ...
            },
            "sectors": {
                "summaries": [{"sector": "fossil-fuel-operations", "gas": "co2e_100yr",
                               "emissionsQuantity": 333224762.65, ...}],
                ...
            },
            ...
        }

    Falls back to the older flat-list shape (``emissions_quantity`` / ``co2e_100yr``
    fields per row) in case of cached pre-v7 responses.
    """
    total_co2e: float = 0.0
    by_sector: dict[str, float] = {}

    if isinstance(payload, dict) and "totals" in payload:
        # ── v7 structured response ──────────────────────────────────────────
        totals = payload.get("totals") or {}
        for summary in totals.get("summaries") or []:
            if summary.get("gas") == "co2e_100yr":
                try:
                    total_co2e = float(summary.get("emissionsQuantity") or 0)
                except (TypeError, ValueError):
                    pass
                break

        sectors = payload.get("sectors") or {}
        for sec in sectors.get("summaries") or []:
            if sec.get("gas") != "co2e_100yr":
                continue
            sector_name = sec.get("sector") or "unknown"
            try:
                value = float(sec.get("emissionsQuantity") or 0)
            except (TypeError, ValueError):
                value = 0.0
            by_sector[sector_name] = by_sector.get(sector_name, 0.0) + value

    else:
        # ── legacy / flat-list fallback ─────────────────────────────────────
        rows: list[dict[str, Any]] = []
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows = payload.get("emissions") or payload.get("sources") or []

        for row in rows:
            value_raw = (
                row.get("emissions_quantity")
                or row.get("emissionsQuantity")
                or row.get("co2e_100yr")
                or row.get("emissions")
                or 0
            )
            try:
                value = float(value_raw)
            except (TypeError, ValueError):
                value = 0.0

            total_co2e += value
            sector = row.get("sector") or row.get("subsector") or "unknown"
            by_sector[sector] = by_sector.get(sector, 0.0) + value

    return {
        "total_co2e_tonnes": total_co2e,
        "unit": "tonnes CO2e (GWP100)",
        "year": 2024,
        "by_sector": by_sector,
    }


# GEM appends the ownership share in square brackets to both the parent ID
# and parent name columns, e.g. "E100000000817 [55.0%]" / "Vivant Corp [55.0%]".
# ~12,000 of ~13,600 populated rows carry the suffix (May 2026 release).
_PARENT_SHARE_RE = re.compile(r"\s*\[\s*([\d.]+)\s*%?\s*\]\s*$")


def _split_parent_token(token: str) -> tuple[str, float | None]:
    """Split "E100000000817 [55.0%]" into ("E100000000817", 55.0)."""
    token = token.strip()
    m = _PARENT_SHARE_RE.search(token)
    if not m:
        return token, None
    try:
        share: float | None = float(m.group(1))
    except ValueError:
        share = None
    return token[: m.start()].strip(), share


def _parse_parents(gem_row: dict[str, str]) -> list[dict[str, Any]]:
    """Extract parent entities declared in the GEM CSV row.

    Each parent dict has ``entity_id``, ``name`` and ``share`` (float
    percentage, or None when GEM doesn't publish one). The share suffix that
    GEM embeds in both columns ("… [55.0%]") is stripped from the values.
    """
    parent_ids_raw = (gem_row.get(_PARENT_IDS_COL) or "").strip()
    parent_names_raw = (gem_row.get(_PARENT_NAMES_COL) or "").strip()

    if not parent_ids_raw:
        return []

    ids = [p for p in (t.strip() for t in parent_ids_raw.split(";")) if p]
    names = [p for p in (t.strip() for t in parent_names_raw.split(";")) if p]

    parents = []
    for i, raw_id in enumerate(ids):
        pid, share = _split_parent_token(raw_id)
        if not pid:
            continue
        if i < len(names):
            pname, name_share = _split_parent_token(names[i])
            if share is None:
                share = name_share
        else:
            pname = pid
        parents.append({"entity_id": pid, "name": pname or pid, "share": share})
    return parents


def _stub_bundle(
    entity_id: str,
    gem_row: dict[str, str],
    lei: str,
) -> dict[str, Any]:
    """Return a partial bundle from GEM CSV data alone (no live API call)."""
    return {
        "source_id": "climatetrace",
        "entity_id": entity_id,
        "entity_name": gem_row.get(_ENTITY_NAME_COL) or entity_id,
        "lei": lei,
        "gem_row": gem_row,
        "emissions": {},
        "assets": [],
        "parents": _parse_parents(gem_row),
        "ownership": _ownership_summary(entity_id),
        "is_stub": True,
    }
