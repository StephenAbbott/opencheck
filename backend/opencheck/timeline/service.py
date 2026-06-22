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
from .assemble import Timeline, assemble_timeline

_GLEIF_RECORD_URL = "https://api.gleif.org/api/v1/lei-records/{lei}"
_GLEIF_MODS_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/field-modifications"
_CH_API_BASE = "https://api.company-information.service.gov.uk"

# GLEIF Registration Authority code for UK Companies House.
_CH_RA_CODE = "RA000585"

_MODS_PAGE_SIZE = 200
_MODS_PAGE_CAP = 5  # ≤ 1000 modifications — plenty for a per-entity timeline
_CH_PAGE_SIZE = 100
_CH_PAGE_CAP = 10  # ≤ 1000 filings


async def _gleif_company_number(client: httpx.AsyncClient, lei: str) -> str | None:
    """Derive the Companies House number from the GLEIF record (GB + RA000585)."""
    resp = await client.get(_GLEIF_RECORD_URL.format(lei=quote(lei)))
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    entity = (
        (((resp.json() or {}).get("data") or {}).get("attributes") or {}).get("entity")
        or {}
    )
    registered_as = (entity.get("registeredAs") or "").strip()
    registered_at = (entity.get("registeredAt") or {}).get("id")
    jurisdiction = entity.get("jurisdiction")
    if registered_as and (registered_at == _CH_RA_CODE or jurisdiction == "GB"):
        return registered_as
    return None


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
    lei_mods: list[dict] = []
    rr_mods: list[dict] = []
    ch_filings: list[dict] = []

    async with build_client() as client:
        # GLEIF record (for the CH number) and the change log run concurrently.
        results = await asyncio.gather(
            _gleif_company_number(client, lei),
            _gleif_modifications(client, lei),
            return_exceptions=True,
        )
        num_res, mods_res = results
        if not isinstance(num_res, BaseException):
            company_number = num_res
        if not isinstance(mods_res, BaseException):
            lei_mods, rr_mods = mods_res

        api_key = settings.companies_house_api_key
        if api_key and company_number:
            try:
                ch_filings = await _ch_filings(client, company_number, api_key)
            except httpx.HTTPError:
                ch_filings = []

    return assemble_timeline(
        lei=lei,
        company_number=company_number,
        gleif_lei_mods=lei_mods,
        gleif_rr_mods=rr_mods,
        ch_filings=ch_filings,
    )


__all__ = ["fetch_timeline"]
