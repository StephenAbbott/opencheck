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
(Do **not** use the OpenSanctions GEM dataset — it adds a NC restriction.)

GEM ownership.zip download
--------------------------
``https://github.com/climatetracecoalition/climate-trace-tools/raw/main/
  climate_trace_tools/data/ownership/ownership.zip``

Cached locally at ``{data_root}/gem/ownership.zip`` once downloaded.

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
_CT_API = "https://api.climatetrace.org"
_CACHE_NS = "climatetrace"

# Column names in the GEM ownership CSVs (as of 2024 release).
_ENTITY_CSV = "all_entities.csv"
_LEI_COL = "Global Legal Entity Identifier Index"
_ENTITY_ID_COL = "Entity ID"
_ENTITY_NAME_COL = "Entity Name"
_PARENT_IDS_COL = "Gem parents IDs"
_PARENT_NAMES_COL = "Gem parents"

# Module-level singletons — built lazily on first access so import is cheap.
_lei_index: dict[str, str] | None = None          # LEI → GEM entity ID
_entity_index: dict[str, dict[str, str]] | None = None  # GEM entity ID → row


def _gem_zip_path() -> Path:
    return data_root() / "gem" / "ownership.zip"


def _ensure_gem_data() -> None:
    """Download GEM ownership.zip if not already on disk."""
    path = _gem_zip_path()
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading GEM ownership.zip from %s", _GEM_ZIP_URL)
    try:
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            r = client.get(_GEM_ZIP_URL)
            r.raise_for_status()
        path.write_bytes(r.content)
        log.info("GEM ownership.zip saved to %s (%d bytes)", path, len(r.content))
    except Exception as exc:
        log.warning("Could not download GEM ownership.zip: %s", exc)


def _load_gem_indexes() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Parse GEM ownership.zip and build LEI and entity indexes.

    Returns ``(lei_index, entity_index)`` where:
    * ``lei_index`` maps a normalised LEI string → GEM entity ID.
    * ``entity_index`` maps a GEM entity ID → the full CSV row dict.
    """
    lei_idx: dict[str, str] = {}
    ent_idx: dict[str, dict[str, str]] = {}

    path = _gem_zip_path()
    if not path.exists():
        log.warning("GEM ownership.zip not found at %s — index will be empty", path)
        return lei_idx, ent_idx

    try:
        with zipfile.ZipFile(path) as zf:
            # The ZIP may contain the CSV at the root or inside a subdirectory.
            candidates = [
                n for n in zf.namelist() if n.endswith(_ENTITY_CSV)
            ]
            if not candidates:
                log.warning("Could not find %s inside GEM zip", _ENTITY_CSV)
                return lei_idx, ent_idx
            with zf.open(candidates[0]) as raw:
                text = raw.read().decode("utf-8", errors="replace")

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
        log.warning("Error parsing GEM ownership.zip: %s", exc)

    log.info(
        "GEM index built: %d entities, %d LEI → entity mappings",
        len(ent_idx),
        len(lei_idx),
    )
    return lei_idx, ent_idx


def _get_indexes() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    global _lei_index, _entity_index
    if _lei_index is None or _entity_index is None:
        _lei_index, _entity_index = _load_gem_indexes()
    return _lei_index, _entity_index


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
                "Enables ESG and climate risk screening by LEI."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Global Energy Monitor, CC BY 4.0. "
                "Climate TRACE, CC BY 4.0."
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
    """Normalise the Climate TRACE emissions response to a flat summary.

    The ``/v7/sources/emissions`` response can be a list of sector
    rows or a dict with a ``sources`` or ``emissions`` key.  We sum
    across all rows to produce a total CO2e figure plus a per-sector
    breakdown.
    """
    rows: list[dict[str, Any]] = []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = payload.get("emissions") or payload.get("sources") or []

    total_co2e: float = 0.0
    by_sector: dict[str, float] = {}

    for row in rows:
        # Normalise: different API versions use different field names.
        value_raw = (
            row.get("emissions_quantity")
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


def _parse_parents(gem_row: dict[str, str]) -> list[dict[str, str]]:
    """Extract parent entities declared in the GEM CSV row."""
    parent_ids_raw = (gem_row.get(_PARENT_IDS_COL) or "").strip()
    parent_names_raw = (gem_row.get(_PARENT_NAMES_COL) or "").strip()

    if not parent_ids_raw:
        return []

    ids = [p.strip() for p in parent_ids_raw.split(";") if p.strip()]
    names = [p.strip() for p in parent_names_raw.split(";") if p.strip()]

    parents = []
    for i, pid in enumerate(ids):
        pname = names[i] if i < len(names) else pid
        parents.append({"entity_id": pid, "name": pname})
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
        "is_stub": True,
    }
