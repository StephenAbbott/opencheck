"""New Zealand — Companies Register / NZBN adapter.

Live, key-gated adapter over the New Zealand Business Number (NZBN) API
(Ministry of Business, Innovation and Employment — Companies Office),
``https://api.business.govt.nz/gateway/nzbn/v5``. The NZBN ``FullEntity``
response is unusually rich: alongside core entity data it carries the
company's directors (``roles``), its shareholders with share allocations
(``company-details.shareholding``), and the ultimate holding company — so
New Zealand is one of the few OpenCheck sources with a real ownership graph
including shareholder percentages.

The flow with GLEIF:

  1. GLEIF carries ``registeredAt.id == "RA000466"`` (NZ Companies Register)
     and ``registeredAs == "<company number>"`` (e.g. Fonterra Co-op
     ``1166320``) — *not* the NZBN.
  2. ``routers/lookup.py`` derives ``derived["nz_company_number"]`` and calls
     ``fetch()`` here with the GLEIF legal name.
  3. The NZBN ``EntitiesGet`` directory search resolves the company number →
     NZBN (its ``search-term`` explicitly covers "legacy numbers (eg company
     number)"), then ``EntitiesByNzbnGet`` returns the full entity by NZBN.

Authentication: ``Ocp-Apim-Subscription-Key`` header (a free NZBN API
subscription key — ``NZBN_API_KEY``). No OAuth is required for the GET
operations used here.

GLEIF RA code: RA000466 (Companies Register — Companies Office, MBIE).
License: New Zealand Companies Register / NZBN open data (MBIE), CC BY 4.0.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo

_LOG = logging.getLogger(__name__)

_API_BASE = "https://api.business.govt.nz/gateway/nzbn/v5"
_CACHE_NS = "nz_companies"

# GLEIF Registration Authority code for the NZ Companies Register.
NZ_RA_CODE: str = "RA000466"


def normalise_nz_company_number(value: str) -> str:
    """NZ company numbers are short numeric strings; strip whitespace."""
    return re.sub(r"\s+", "", str(value or "")).strip()


def _entity_url(nzbn: str) -> str:
    """Public NZBN entity page."""
    return f"https://www.nzbn.govt.nz/mynzbn/nzbndetails/{nzbn}/"


def _date(value: Any) -> str | None:
    """Trim an ISO date-time to YYYY-MM-DD."""
    s = str(value or "").strip()
    return s[:10] if s else None


def _person_name(p: Any) -> str:
    """Build a full name from an NZBN person block (role or shareholder)."""
    if not isinstance(p, dict):
        return ""
    full = str(p.get("fullName") or "").strip()
    if full:
        return full
    parts = [p.get("firstName"), p.get("middleNames"), p.get("lastName")]
    return " ".join(str(x).strip() for x in parts if x and str(x).strip()).strip()


def _address_str(a: Any) -> str | None:
    """Join an NZBN address block into a single line."""
    if not isinstance(a, dict):
        return None
    parts = [
        a.get("address1"), a.get("address2"), a.get("address3"), a.get("address4"),
        a.get("postCode"),
    ]
    line = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
    return line or None


def _registered_address(full: dict[str, Any]) -> str | None:
    """Prefer the REGISTERED address, else the first address on file."""
    block = full.get("addresses") or {}
    addresses = block.get("addressList") if isinstance(block, dict) else None
    addresses = addresses or []
    for a in addresses:
        if str((a or {}).get("addressType") or "").upper() == "REGISTERED":
            return _address_str(a)
    return _address_str(addresses[0]) if addresses else None


def _paf(a: Any) -> str | None:
    """Extract the PAF delivery-point id (pafId) from an address block."""
    if not isinstance(a, dict):
        return None
    return str(a.get("pafId") or "").strip() or None


def _role_address(r: dict[str, Any]) -> dict[str, Any]:
    """The first usable address block for a role (roleAddress[] or ASIC)."""
    block = r.get("roleAddress")
    if isinstance(block, list) and block and isinstance(block[0], dict):
        return block[0]
    asic = r.get("roleAsicAddress")
    return asic if isinstance(asic, dict) else {}


def _norm_roles(roles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalise governance roles (directors etc.) → person/entity role-holders."""
    out: list[dict[str, Any]] = []
    for r in roles or []:
        if not isinstance(r, dict):
            continue
        role_type = str(r.get("roleType") or "").strip() or None
        start, end = _date(r.get("startDate")), _date(r.get("endDate"))
        status = str(r.get("roleStatus") or "").strip() or None
        addr = _role_address(r)
        addr_str, paf = _address_str(addr), _paf(addr)
        ent = r.get("roleEntity") or {}
        if isinstance(ent, dict) and str(ent.get("entityName") or "").strip():
            out.append({
                "kind": "entity",
                "name": str(ent["entityName"]).strip(),
                "nzbn": str(ent.get("nzbn") or "").strip() or None,
                "role_type": role_type, "status": status, "start": start, "end": end,
                "address": addr_str, "paf_id": paf,
            })
            continue
        name = _person_name(r.get("rolePerson"))
        if name:
            out.append({
                "kind": "person", "name": name, "nzbn": None,
                "role_type": role_type, "status": status, "start": start, "end": end,
                "address": addr_str, "paf_id": paf,
            })
    return out


def _norm_shareholders(company_details: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise the share allocations into per-shareholder rows with percent.

    Each ``shareAllocation`` records a number of shares (``allocation``) against
    the company total (``numberOfShares``); the ``shareholder`` list within an
    allocation is jointly held. We attribute the allocation's percentage to each
    shareholder in the group and flag joint holdings.
    """
    sh = (company_details or {}).get("shareholding") or {}
    try:
        total = float(sh.get("numberOfShares") or 0)
    except (TypeError, ValueError):
        total = 0.0
    out: list[dict[str, Any]] = []
    for alloc in sh.get("shareAllocation") or []:
        if not isinstance(alloc, dict):
            continue
        try:
            shares = float(alloc.get("allocation") or 0)
        except (TypeError, ValueError):
            shares = 0.0
        percent = round(shares / total * 100, 4) if total else None
        holders = alloc.get("shareholder") or []
        joint = isinstance(holders, list) and len(holders) > 1
        for h in holders:
            if not isinstance(h, dict):
                continue
            other = h.get("otherShareholder") or {}
            start = _date(h.get("appointmentDate"))
            addr = h.get("shareholderAddress") or {}
            addr_str, paf = _address_str(addr), _paf(addr)
            if isinstance(other, dict) and str(other.get("currentEntityName") or "").strip():
                out.append({
                    "kind": "entity",
                    "name": str(other["currentEntityName"]).strip(),
                    "nzbn": str(other.get("nzbn") or "").strip() or None,
                    "company_number": str(other.get("companyNumber") or "").strip() or None,
                    "shares": shares, "percent": percent, "jointly_held": joint, "start": start,
                    "address": addr_str, "paf_id": paf,
                })
                continue
            name = _person_name(h.get("individualShareholder"))
            if name:
                out.append({
                    "kind": "person", "name": name, "nzbn": None, "company_number": None,
                    "shares": shares, "percent": percent, "jointly_held": joint, "start": start,
                    "address": addr_str, "paf_id": paf,
                })
    return out


def _norm_uhc(uhc: Any) -> dict[str, Any] | None:
    """Normalise the ultimate holding company block, if present and named."""
    if not isinstance(uhc, dict):
        return None
    name = str(uhc.get("name") or "").strip()
    if not name:
        return None
    return {
        "name": name,
        "nzbn": str(uhc.get("nzbn") or "").strip() or None,
        "number": str(uhc.get("number") or "").strip() or None,
        "country": str(uhc.get("country") or "").strip() or None,
    }


class NzCompaniesAdapter(SourceAdapter):
    """Source adapter for the New Zealand Companies Register (NZBN API)."""

    id = "nz_companies"

    lookup_derivers = (
        LookupDeriver(frozenset({NZ_RA_CODE}), "nz_company_number", normalise_nz_company_number),
    )
    lookup_pass_legal_name = True
    # The lookup does two sequential HTTP calls (search → entity detail).
    lookup_timeout_s = 45.0

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="New Zealand Companies Register (NZBN)",
            homepage="https://companies-register.companiesoffice.govt.nz/",
            description=(
                "New Zealand company data from the NZBN API (Companies Office / "
                "MBIE): entity details, directors, shareholders with share "
                "allocations, and the ultimate holding company."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the New Zealand Companies Register / NZBN "
                "(Ministry of Business, Innovation and Employment) via the NZBN "
                "API, licensed CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=settings.allow_live and bool(settings.nzbn_api_key),
            is_national_register=True,
        )

    # Identifier-keyed: entered via the LEI flow, not free-text name search.
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — company number → NZBN → full entity
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        number = normalise_nz_company_number(hit_id)

        def _bundle(company, roles, shareholders, uhc, nzbn, is_stub) -> dict[str, Any]:
            return {
                "source_id": self.id,
                "nz_company_number": number,
                "nzbn": nzbn,
                "company": company,
                "roles": roles or [],
                "shareholders": shareholders or [],
                "ultimate_holding_company": uhc,
                "legal_name": legal_name,
                "link": _entity_url(nzbn) if nzbn else None,
                "is_stub": is_stub,
            }

        if not number:
            return _bundle(None, [], [], None, "", True)

        cache_key = f"{_CACHE_NS}/company/{number}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return _bundle(None, [], [], None, "", True)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        key = get_settings().nzbn_api_key
        headers = {"Ocp-Apim-Subscription-Key": key} if key else {}

        # GLEIF stores either the company number *or* the 13-digit NZBN in
        # registeredAs. A 13-digit all-numeric value is already an NZBN, so skip
        # the directory-search resolution step.
        if len(number) == 13 and number.isdigit():
            nzbn = number
        else:
            nzbn = await self._resolve_nzbn(number, headers)
        if not nzbn:
            # GLEIF confirmed the entity is registered (RA000466) but we could
            # not resolve it — surface a non-stub card with the GLEIF name
            # rather than hiding it (mirrors the CNPJ / CRO / JAR adapters).
            bundle = _bundle({"name": legal_name} if legal_name else None,
                             [], [], None, "", False)
            self._cache.put(cache_key, bundle)
            return bundle

        full = await self._get_entity(nzbn, headers)
        bundle = self._normalise(number, nzbn, full, legal_name)
        self._cache.put(cache_key, bundle)
        return bundle

    async def _resolve_nzbn(self, number: str, headers: dict[str, str]) -> str:
        """Resolve a company number to its NZBN via the directory search."""
        url = f"{_API_BASE}/entities?search-term={quote(number)}&page-size=10"
        try:
            async with build_client() as client:
                resp = await client.get(url, headers=headers)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("nz_companies: search HTTP error: %s", exc)
            return ""
        if not resp.is_success:
            _LOG.warning("nz_companies: search HTTP %s", resp.status_code)
            return ""
        try:
            items = (resp.json() or {}).get("items") or []
        except ValueError:
            return ""
        # Prefer an exact match on the legacy company number.
        for it in items:
            if str((it or {}).get("sourceRegisterUniqueId") or "") == number and it.get("nzbn"):
                return str(it["nzbn"])
        if len(items) == 1 and items[0].get("nzbn"):
            return str(items[0]["nzbn"])
        return ""

    async def _get_entity(self, nzbn: str, headers: dict[str, str]) -> dict[str, Any]:
        url = f"{_API_BASE}/entities/{quote(nzbn)}"
        try:
            async with build_client() as client:
                resp = await client.get(url, headers=headers)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("nz_companies: entity HTTP error: %s", exc)
            return {}
        if not resp.is_success:
            _LOG.warning("nz_companies: entity HTTP %s", resp.status_code)
            return {}
        try:
            data = resp.json()
        except ValueError:
            return {}
        return data if isinstance(data, dict) else {}

    async def _get_json(self, url: str, headers: dict[str, str]) -> Any:
        try:
            async with build_client() as client:
                resp = await client.get(url, headers=headers)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("nz_companies: HTTP error %s: %s", url, exc)
            return None
        if not resp.is_success:
            return None
        try:
            return resp.json()
        except ValueError:
            return None

    async def fetch_timeline_data(self, company_number: str) -> dict[str, Any] | None:
        """Raw FullEntity + dated name/status/address history for the Time Machine.

        Returns ``None`` when not live, no NZBN key, or the entity can't be
        resolved. Reuses the company-number → NZBN resolution."""
        settings = get_settings()
        key = settings.nzbn_api_key
        if not settings.allow_live or not key:
            return None
        number = normalise_nz_company_number(company_number)
        if not number:
            return None
        headers = {"Ocp-Apim-Subscription-Key": key}
        if len(number) == 13 and number.isdigit():
            nzbn = number
        else:
            nzbn = await self._resolve_nzbn(number, headers)
        if not nzbn:
            return None
        full = await self._get_entity(nzbn, headers)
        base = f"{_API_BASE}/entities/{quote(nzbn)}/history"
        name_h = await self._get_json(f"{base}/entity-names", headers)
        status_h = await self._get_json(f"{base}/entity-statuses", headers)
        addr_h = await self._get_json(f"{base}/addresses", headers)
        addr_list = (addr_h or {}).get("addressList") if isinstance(addr_h, dict) else addr_h
        return {
            "company_number": number,
            "nzbn": nzbn,
            "full": full or {},
            "name_history": name_h if isinstance(name_h, list) else [],
            "status_history": status_h if isinstance(status_h, list) else [],
            "address_history": addr_list if isinstance(addr_list, list) else [],
        }

    def _normalise(
        self, number: str, nzbn: str, full: dict[str, Any], legal_name: str
    ) -> dict[str, Any]:
        company = {
            "name": (str(full.get("entityName") or "").strip() or legal_name or None),
            "status": (
                str(full.get("entityStatusDescription") or "").strip()
                or str(full.get("entityStatusCode") or "").strip()
                or None
            ),
            "entity_type": str(full.get("entityTypeDescription") or "").strip() or None,
            "founding_date": _date(full.get("registrationDate")),
            "address": _registered_address(full),
            "trading_names": [
                str(t.get("name")).strip()
                for t in (full.get("tradingNames") or [])
                if isinstance(t, dict) and t.get("name")
            ],
            "previous_names": [
                str(n).strip() for n in (full.get("previousEntityNames") or []) if n
            ],
        }
        company_details = full.get("company-details") or {}
        return {
            "source_id": self.id,
            "nz_company_number": number,
            "nzbn": nzbn,
            "company": company,
            "roles": _norm_roles(full.get("roles") or []),
            "shareholders": _norm_shareholders(company_details),
            "ultimate_holding_company": _norm_uhc(company_details.get("ultimateHoldingCompany")),
            "legal_name": legal_name,
            "link": _entity_url(nzbn),
            "is_stub": False,
        }
