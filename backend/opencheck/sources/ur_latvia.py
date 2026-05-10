"""Latvia Register of Enterprises (UR) adapter.

The Uzņēmumu reģistrs (UR) is Latvia's statutory business register, operating
under the Ministry of Justice.  Open data is published on Latvia's national
open-data portal (data.gov.lv) via a CKAN instance.  Every table is loaded
into the CKAN Datastore, so records can be queried row-by-row through the API
without downloading the full CSV bulk files.

Five datasets are used
----------------------
business_register   (128 MB CSV) — entity profiles
beneficial_owners   ( 17 MB CSV) — UBO declarations
historical_names    (  7 MB CSV) — former names
officers            ( 40 MB CSV) — board members / representatives
members_sia         ( 24 MB CSV) — LLC (SIA) shareholders

Resource IDs on data.gov.lv
----------------------------
``25e80bf3-f107-4ab4-89ef-251b5b9374e9``  business register
``20a9b26d-d056-4dbb-ae18-9ff23c87bdee``  beneficial owners
``ad772b8b-76e4-4334-83d9-3beadf513aa6``  historical names
``e665114a-73c2-4375-9470-55874b4cfa6b``  officers
``837b451a-4833-4fd1-bfdd-b45b35a994fd``  members (SIA shares)

The flow with GLEIF
-------------------
1. GLEIF returns ``registeredAt.id == "RA000423"`` and ``registeredAs``
   holding the 11-digit Latvian registration number (regcode).
2. app.py extracts ``derived["lv_regcode"]`` and calls ``fetch()`` here.
3. ``fetch()`` joins all five datasets for the given regcode, returning a
   single payload that the BODS mapper converts into statements.

Authentication: none — all endpoints are public.
License: Open data (PSI Directive / Public Information Act).
Attribution: "Contains data from the Latvian Register of Enterprises (UR),
  open data published on data.gov.lv."
CKAN portal: https://data.gov.lv/dati/lv/organization/ur
UR website: https://www.ur.gov.lv/en/
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

# CKAN base URL for Latvia's open-data portal.
_CKAN_BASE = "https://data.gov.lv/dati/api/3/action"

# Resource IDs — taken from the five UR datasets on data.gov.lv.
_RES_BUSINESS = "25e80bf3-f107-4ab4-89ef-251b5b9374e9"
_RES_BOWNERS = "20a9b26d-d056-4dbb-ae18-9ff23c87bdee"
_RES_HIST_NAMES = "ad772b8b-76e4-4334-83d9-3beadf513aa6"
_RES_OFFICERS = "e665114a-73c2-4375-9470-55874b4cfa6b"
_RES_MEMBERS = "837b451a-4833-4fd1-bfdd-b45b35a994fd"

_CACHE_NS = "ur_latvia"

# GLEIF Registration Authority code for Latvia's UR.
LV_RA_CODE: str = "RA000423"

# CKAN datastore_search_sql limit — keep generous for officers/members.
_PAGE = 100

# Latvian entity-type codes → English labels.
_ENTITY_TYPES: dict[str, str] = {
    "SIA": "Sabiedrība ar ierobežotu atbildību (LLC)",
    "AS": "Akciju sabiedrība (Joint-stock company)",
    "IK": "Individuālais komersants (Sole trader)",
    "IND": "Individuālais uzņēmums (Private enterprise)",
    "ZEM": "Zemnieku saimniecība (Farm enterprise)",
    "PS": "Pilnsabiedrība (General partnership)",
    "KS": "Komandītsabiedrība (Limited partnership)",
    "KB": "Kooperatīvā sabiedrība (Cooperative)",
    "BDR": "Biedrība (Association)",
    "NOD": "Nodibinājums (Foundation)",
    "VU": "Valsts uzņēmums (State enterprise)",
    "PSV": "Pašvaldības uzņēmums (Municipal enterprise)",
    "FIL": "Filiāle (Branch)",
    "AKF": "Ārvalsts komersanta filiāle (Foreign branch)",
    "PAR": "Ārvalsts komersanta pārstāvniecība (Foreign representative office)",
    "DRZ": "Draudze (Religious congregation)",
    "MIL": "Masu informācijas līdzeklis (Mass media entity)",
    "SPO": "Sporta organizācija (Sports organisation)",
    "SAB": "Sabiedriskā organizācija (Public organisation)",
    "PAJ": "Paju sabiedrība (Share company)",
    "ASF": "AS filiāle (JSC branch)",
}


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_regcode(rc: str | int) -> str:
    """Return the plain 11-digit registration number string.

    GLEIF stores Latvian regcodes as plain digit strings.  The CKAN
    ``regcode`` column is typed as integer, so we strip any whitespace and
    non-digit characters then return the numeric string.
    """
    return str(rc).strip().lstrip("0").rjust(11, "0") if str(rc).strip().isdigit() else str(rc).strip()


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode()).hexdigest()[:16]


def _entity_url(regcode: str) -> str:
    return f"https://www.latvija.lv/lv/bizness/uznemumu-registrs/{regcode}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class UrLatviaAdapter(SourceAdapter):
    """Source adapter for the Latvian Register of Enterprises (UR)."""

    id = "ur_latvia"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        live = get_settings().allow_live
        return SourceInfo(
            id=self.id,
            name="UR — Latvian Register of Enterprises",
            homepage="https://www.ur.gov.lv/en/",
            description=(
                "Latvian company data from the Register of Enterprises (UR), "
                "sourced via Latvia's open-data portal (data.gov.lv). "
                "Provides entity profiles, beneficial owners, officers, and "
                "shareholders for companies registered in Latvia."
            ),
            license="Open Government Data (PSI Directive)",
            attribution=(
                "Contains data from the Latvian Register of Enterprises (UR), "
                "open data published on data.gov.lv."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # Search — SQL ILIKE on the business register name column
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            records = cached[0]
        else:
            # Use SQL ILIKE for reliable Latvian-text matching.
            safe_q = query.replace("'", "''")
            sql = (
                f"SELECT regcode, name, type, type_text, registered, terminated, closed, address "
                f"FROM \"{_RES_BUSINESS}\" "
                f"WHERE name ILIKE '%{safe_q}%' "
                f"LIMIT 10"
            )
            url = f"{_CKAN_BASE}/datastore_search_sql?sql={quote(sql)}"
            async with build_client() as client:
                resp = await client.get(url)
            if not resp.is_success:
                _log.warning("UR Latvia search failed: %s", resp.status_code)
                return self._stub_search(query)
            data = resp.json()
            if not data.get("success"):
                _log.warning("UR Latvia search CKAN error: %s", data.get("error"))
                return self._stub_search(query)
            records = data.get("result", {}).get("records", [])
            self._cache.put(cache_key, records)

        return [self._entity_hit(rec) for rec in records]

    # ------------------------------------------------------------------
    # Fetch — join all five datasets for a given regcode
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full UR payload for a given Latvian registration number.

        ``hit_id`` is the 11-digit regcode (may arrive as a string from GLEIF).
        """
        regcode = normalise_regcode(hit_id)
        cache_key = f"{_CACHE_NS}/entity/{regcode}"

        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_payload(regcode, legal_name)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        # ----------------------------------------------------------
        # Fetch all five datasets in parallel-ish (sequential for now
        # since httpx async context is per-call; refactor if needed).
        # ----------------------------------------------------------
        async with build_client() as client:
            entity = await self._fetch_entity(client, regcode)
            hist_names = await self._fetch_resource(
                client, _RES_HIST_NAMES, "regcode", regcode
            )
            bowners = await self._fetch_resource(
                client, _RES_BOWNERS, "legal_entity_registration_number", regcode
            )
            officers = await self._fetch_resource(
                client, _RES_OFFICERS, "at_legal_entity_registration_number", regcode
            )
            members = await self._fetch_resource(
                client, _RES_MEMBERS, "at_legal_entity_registration_number", regcode
            )

        payload: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": regcode,
            "lv_regcode": regcode,
            "legal_name": legal_name,
            "entity": entity,
            "historical_names": hist_names,
            "beneficial_owners": bowners,
            "officers": officers,
            "members": members,
            "is_stub": False,
        }
        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_entity(
        self, client: Any, regcode: str
    ) -> dict[str, Any]:
        """Fetch a single entity record from the business register."""
        filters = json.dumps({"regcode": int(regcode)})
        url = (
            f"{_CKAN_BASE}/datastore_search"
            f"?resource_id={_RES_BUSINESS}"
            f"&filters={quote(filters)}&limit=1"
        )
        resp = await client.get(url)
        if not resp.is_success:
            _log.warning("UR Latvia entity fetch failed: %s (regcode=%s)", resp.status_code, regcode)
            return {}
        data = resp.json()
        records = (data.get("result") or {}).get("records") or []
        return records[0] if records else {}

    async def _fetch_resource(
        self,
        client: Any,
        resource_id: str,
        filter_field: str,
        regcode: str,
    ) -> list[dict[str, Any]]:
        """Fetch all rows from a resource matching the regcode filter field.

        The filter value may be stored as integer or string in the CKAN
        datastore, so we try integer first then fall back to string.
        """
        try:
            filter_val: int | str = int(regcode)
        except (ValueError, TypeError):
            filter_val = regcode

        filters = json.dumps({filter_field: filter_val})
        url = (
            f"{_CKAN_BASE}/datastore_search"
            f"?resource_id={resource_id}"
            f"&filters={quote(filters)}&limit={_PAGE}"
        )
        resp = await client.get(url)
        if not resp.is_success:
            _log.warning(
                "UR Latvia resource fetch failed: %s (resource=%s, regcode=%s)",
                resp.status_code, resource_id, regcode,
            )
            return []
        data = resp.json()
        records = (data.get("result") or {}).get("records") or []

        # If integer filter returned nothing, try string (some tables differ).
        if not records and isinstance(filter_val, int):
            filters_str = json.dumps({filter_field: str(regcode)})
            url2 = (
                f"{_CKAN_BASE}/datastore_search"
                f"?resource_id={resource_id}"
                f"&filters={quote(filters_str)}&limit={_PAGE}"
            )
            resp2 = await client.get(url2)
            if resp2.is_success:
                records = (resp2.json().get("result") or {}).get("records") or []

        return records

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(rec: dict[str, Any]) -> SourceHit:
        regcode = str(rec.get("regcode") or "")
        name = (rec.get("name") or regcode or "Unknown").strip()
        entity_type = (rec.get("type") or "").strip()
        type_label = _ENTITY_TYPES.get(entity_type, entity_type)
        closed = (rec.get("closed") or "").strip()
        terminated = rec.get("terminated")

        status_parts: list[str] = []
        if terminated or closed:
            status_parts.append("inactive")
        else:
            status_parts.append("active")
        if type_label:
            status_parts.append(type_label)

        return SourceHit(
            source_id="ur_latvia",
            hit_id=regcode,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(filter(None, [f"LV {regcode}"] + status_parts)),
            identifiers={"lv_regcode": regcode},
            raw=rec,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub helpers
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="40003567907",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub UR Latvia record — set OPENCHECK_ALLOW_LIVE=true to "
                    "query the live data.gov.lv CKAN portal."
                ),
                identifiers={"lv_regcode": "40003567907"},
                raw={"regcode": 40003567907, "name": f"{query} (stub)"},
            )
        ]

    def _stub_payload(self, regcode: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": regcode,
            "lv_regcode": regcode,
            "legal_name": legal_name,
            "entity": {},
            "historical_names": [],
            "beneficial_owners": [],
            "officers": [],
            "members": [],
            "is_stub": True,
        }
