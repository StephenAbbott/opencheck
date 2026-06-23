"""Time Machine fetch service — pull raw change data and assemble a timeline.

Lazy and never on the main lookup (same posture as ``/securities``). Fetches:

- **GLEIF** (key-free): the LEI record (to derive the Companies House number) and
  the field-modification change log, partitioned into LEI vs RR records.
- **Companies House** (needs ``COMPANIES_HOUSE_API_KEY``): filing history for the
  derived company number. Degrades to GLEIF-only when no key is set or the
  company is not GB / has no CH number.

Failures of either source are swallowed so the endpoint always returns a
(possibly empty) timeline rather than erroring.
"""

from __future__ import annotations

import asyncio
from urllib.parse import quote

import httpx

from ..config import get_settings
from ..http import build_client
from .ariregister import ariregister_change_events
from .assemble import Timeline, assemble_timeline
from .nz_companies import nz_change_events

_GLEIF_RECORD_URL = "https://api.gleif.org/api/v1/lei-records/{lei}"
_GLEIF_MODS_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/field-modifications"
_CH_API_BASE = "https://api.company-information.service.gov.uk"

# GLEIF Registration Authority codes.
_CH_RA_CODE = "RA000585"   # UK Companies House
_NZ_RA_CODE = "RA000466"   # NZ Companies Register
_EE_RA_CODE = "RA000181"   # Estonian e-Business Register

_MODS_PAGE_SIZE = 200
_MODS_PAGE_CAP = 5  # ≤ 1000 modifications — plenty for a per-entity timeline
_CH_PAGE_SIZE = 100
_CH_PAGE_CAP = 10  # ≤ 1000 filings


async def _gleif_registration(
    client: httpx.AsyncClient, lei: str
) -> tuple[str | None, str | None, str | None]:
    """Return ``(ch_number, nz_number, ee_registry_code)`` from the GLEIF record."""
    resp = await client.get(_GLEIF_RECORD_URL.format(lei=quote(lei)))
    if resp.status_code == 404:
        return None, None, None
    resp.raise_for_status()
    entity = (
        (((resp.json() or {}).get("data") or {}).get("attributes") or {}).get("entity")
        or {}
    )
    registered_as = (entity.get("registeredAs") or "").strip()
    registered_at = (entity.get("registeredAt") or {}).get("id")
    jurisdiction = entity.get("jurisdiction")
    ch = registered_as if (
        registered_as and (registered_at == _CH_RA_CODE or jurisdiction == "GB")
    ) else None
    nz = registered_as if (registered_as and registered_at == _NZ_RA_CODE) else None
    ee = registered_as if (registered_as and registered_at == _EE_RA_CODE) else None
    return ch, nz, ee


async def _gleif_modifications(
    client: httpx.AsyncClient, lei: str
) -> tuple[list[dict], list[dict]]:
    """Return ``(lei_mods, rr_mods)`` — field-modifications split by record type."""
    lei_mods: list[dict] = []
    rr_mods: list[dict] = []
    for page in range(1, _MODS_PAGE_CAP + 1):
        resp = await client.get(
            _GLEIF_MODS_URL.format(lei=quote(lei)),
            params={"page[size]": _MODS_PAGE_SIZE, "page[number]": page},
        )
        if resp.status_code == 404:
            break
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") or []
        for node in data:
            attrs = node.get("attributes") or {}
            if (attrs.get("recordType") or "").upper() == "RR":
                rr_mods.append(attrs)
            else:
                lei_mods.append(attrs)
        last_page = ((payload.get("meta") or {}).get("pagination") or {}).get("lastPage")
        if not data or (last_page and page >= last_page):
            break
    return lei_mods, rr_mods


async def _ch_filings(
    client: httpx.AsyncClient, number: str, api_key: str
) -> list[dict]:
    """Fetch Companies House filing history for ``number`` (Basic auth: key, '')."""
    filings: list[dict] = []
    auth = httpx.BasicAuth(api_key, "")
    for page in range(_CH_PAGE_CAP):
        resp = await client.get(
            f"{_CH_API_BASE}/company/{quote(number)}/filing-history",
            params={"items_per_page": _CH_PAGE_SIZE, "start_index": page * _CH_PAGE_SIZE},
            auth=auth,
        )
        if resp.status_code in (401, 403, 404):
            break
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("items") or []
        filings.extend(items)
        total = payload.get("total_count") or 0
        if not items or (page + 1) * _CH_PAGE_SIZE >= total:
            break
    return filings


async def fetch_timeline(lei: str) -> Timeline:
    """Fetch GLEIF (+ Companies House where possible) history and assemble it."""
    settings = get_settings()
    if not settings.allow_live:
        return Timeline(subject_lei=lei, company_number=None, events=[], notable=[])

    company_number: str | None = None
    nz_number: str | None = None
    ee_code: str | None = None
    lei_mods: list[dict] = []
    rr_mods: list[dict] = []
    ch_filings: list[dict] = []

    async with build_client() as client:
        # GLEIF record (for the CH/NZ/EE numbers) and the change log run concurrently.
        results = await asyncio.gather(
            _gleif_registration(client, lei),
            _gleif_modifications(client, lei),
            return_exceptions=True,
        )
        reg_res, mods_res = results
        if not isinstance(reg_res, BaseException):
            company_number, nz_number, ee_code = reg_res
        if not isinstance(mods_res, BaseException):
            lei_mods, rr_mods = mods_res

        # Prefer the dedicated history key; fall back to the lookup adapter's key.
        api_key = (
            settings.companies_house_history_api_key
            or settings.companies_house_api_key
        )
        if api_key and company_number:
            try:
                ch_filings = await _ch_filings(client, company_number, api_key)
            except httpx.HTTPError:
                ch_filings = []

    # New Zealand — reconstruct events from the NZBN dated records (manages its
    # own client + key). Best-effort; never sinks the timeline.
    nz_events = []
    if nz_number and settings.nzbn_api_key:
        try:
            from ..sources import REGISTRY
            data = await REGISTRY["nz_companies"].fetch_timeline_data(nz_number)
        except Exception:  # noqa: BLE001
            data = None
        if data:
            nz_events = nz_change_events(data)

    # Estonia — registry-card + beneficial-owner history via the credentialed
    # RIK SOAP API (read-only). Best-effort; never sinks the timeline.
    ee_events = []
    if ee_code and settings.ariregister_username and settings.ariregister_password:
        try:
            from ..sources import REGISTRY
            data = await REGISTRY["ariregister"].fetch_timeline_data(ee_code)
        except Exception:  # noqa: BLE001
            data = None
        if data:
            ee_events = ariregister_change_events(data)

    return assemble_timeline(
        lei=lei,
        company_number=company_number,
        gleif_lei_mods=lei_mods,
        gleif_rr_mods=rr_mods,
        ch_filings=ch_filings,
        extra_events=nz_events + ee_events,
    )


__all__ = ["fetch_timeline"]
