"""Time Machine fetch service — pull raw change data and assemble a timeline.

Lazy and never on the main lookup (same posture as ``/securities``). Fetches:

- **GLEIF** (key-free): the LEI record (to derive the Companies House number) and
  the field-modification change log, partitioned into LEI vs RR records.
- **Companies House** (needs ``COMPANIES_HOUSE_API_KEY``): filing history for the
  derived company number. Degrades to GLEIF-only when no key is set or the
  company is not GB / has no CH number.

Failures of either source are swallowed so the endpoint always returns a
(possibly empty) timeline rather than erroring — but **swallowed is not the
same as unreported** (Phase 146). Until then a GLEIF 429 (the Phase 143
transport hands the last one back to its caller) produced an empty change log
that read as "checked — no history", and, because the CH / NZ / Estonia /
Denmark branches gate on registry numbers taken from that same swallowed
record, silently skipped every other timeline source too. The timeline now
carries what did and did not run:

* ``gleif_record_available`` / ``gleif_events_available`` — false when GLEIF
  refused that call, so the frontend can say the history could not be checked
  rather than showing an empty one;
* ``registry_sources_blocked`` — the record failed, so the registry-history
  sources could not even be *attempted* (no company number to attempt them
  with) — a different statement from "attempted, no events";
* ``company_number_basis`` — ``"cached"`` when the number came from the GLEIF
  adapter's on-disk record rather than a live call. The Golden Copy snapshot
  cannot help here: it holds no ``registeredAs``/``registeredAt``.
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote

import httpx

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .ariregister import ariregister_change_events
from .assemble import Timeline, assemble_timeline
from .cvr_denmark import cvr_change_events
from .nz_companies import nz_change_events

log = logging.getLogger(__name__)

_GLEIF_RECORD_URL = "https://api.gleif.org/api/v1/lei-records/{lei}"
_GLEIF_MODS_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/field-modifications"
_CH_API_BASE = "https://api.company-information.service.gov.uk"

# GLEIF Registration Authority codes.
_CH_RA_CODE = "RA000585"   # UK Companies House
_NZ_RA_CODE = "RA000466"   # NZ Companies Register
_EE_RA_CODE = "RA000181"   # Estonian e-Business Register
_DK_RA_CODE = "RA000170"   # Danish CVR / Erhvervsstyrelsen

#: The GLEIF adapter's own on-disk record for this LEI — the only local copy
#: of ``registeredAs``/``registeredAt`` there is. Read with no age bound: a
#: stale registry number is still the right registry number far more often
#: than ``None`` is, and the response says the number came from cache.
_GLEIF_RECORD_CACHE_KEY = "gleif/lei/{lei}"

_cache = Cache()

_MODS_PAGE_SIZE = 200
_MODS_PAGE_CAP = 5  # ≤ 1000 modifications — plenty for a per-entity timeline
_CH_PAGE_SIZE = 100
_CH_PAGE_CAP = 10  # ≤ 1000 filings


async def _gleif_registration(
    client: httpx.AsyncClient, lei: str
) -> tuple[str | None, str | None, str | None, str | None]:
    """Return ``(ch_number, nz_number, ee_registry_code, dk_cvr)`` from GLEIF."""
    resp = await client.get(_GLEIF_RECORD_URL.format(lei=quote(lei)))
    if resp.status_code == 404:
        return None, None, None, None
    resp.raise_for_status()
    return _registration_numbers(resp.json())


def _cached_registration(lei: str) -> tuple[str | None, str | None, str | None, str | None] | None:
    """Registry numbers from the GLEIF adapter's cached record, or ``None``.

    No network, no age bound. This is the fallback for a rate-limited live
    record: without it the CH / NZ / EE / DK branches do not merely fail, they
    are never attempted, and the timeline loses every source at once.
    """
    hit = _cache.get_payload(_GLEIF_RECORD_CACHE_KEY.format(lei=lei))
    if hit is None:
        return None
    numbers = _registration_numbers(hit[0])
    return numbers if any(numbers) else None


def _registration_numbers(
    payload: dict | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    """``(ch, nz, ee, dk)`` from a GLEIF Level-1 record payload."""
    entity = (
        (((payload or {}).get("data") or {}).get("attributes") or {}).get("entity")
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
    dk = registered_as if (registered_as and registered_at == _DK_RA_CODE) else None
    return ch, nz, ee, dk


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
        # Live mode off is not a GLEIF refusal — nothing was asked, and the
        # endpoint's `available: false` already says the history is not there.
        return Timeline(subject_lei=lei, company_number=None, events=[], notable=[])

    company_number: str | None = None
    nz_number: str | None = None
    ee_code: str | None = None
    dk_cvr: str | None = None
    lei_mods: list[dict] = []
    rr_mods: list[dict] = []
    ch_filings: list[dict] = []

    gleif_record_available = True
    gleif_events_available = True
    company_number_basis: str | None = None

    async with build_client() as client:
        # GLEIF record (for the CH/NZ/EE numbers) and the change log run concurrently.
        results = await asyncio.gather(
            _gleif_registration(client, lei),
            _gleif_modifications(client, lei),
            return_exceptions=True,
        )
        reg_res, mods_res = results
        if not isinstance(reg_res, BaseException):
            company_number, nz_number, ee_code, dk_cvr = reg_res
            company_number_basis = "live"
        else:
            # A 429 handed back by the Phase 143 transport, the throttle
            # refusing to send, a timeout. Until Phase 146 this exception was
            # dropped on the floor and took every registry source with it.
            log.warning("timeline: GLEIF record unavailable for %s: %s", lei, reg_res)
            gleif_record_available = False
            cached = _cached_registration(lei)
            if cached is not None:
                company_number, nz_number, ee_code, dk_cvr = cached
                company_number_basis = "cached"
        if not isinstance(mods_res, BaseException):
            lei_mods, rr_mods = mods_res
        else:
            log.warning("timeline: GLEIF change log unavailable for %s: %s", lei, mods_res)
            gleif_events_available = False

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

    # Denmark — reconstruct events from CVR's bitemporal records. The normal CVR
    # adapter fetch already returns the full virkning history in the bundle, so no
    # dedicated history call is needed. Best-effort; never sinks the timeline.
    dk_events = []
    if dk_cvr and settings.cvr_denmark_api_key:
        try:
            from ..sources import REGISTRY
            bundle = await REGISTRY["cvr_denmark"].fetch(dk_cvr)
        except Exception:  # noqa: BLE001
            bundle = None
        if bundle:
            dk_events = cvr_change_events(bundle)

    timeline = assemble_timeline(
        lei=lei,
        company_number=company_number,
        gleif_lei_mods=lei_mods,
        gleif_rr_mods=rr_mods,
        ch_filings=ch_filings,
        extra_events=nz_events + ee_events + dk_events,
    )
    timeline.gleif_record_available = gleif_record_available
    timeline.gleif_events_available = gleif_events_available
    # "Blocked" only when the record failed AND nothing local stood in: with a
    # cached number the registry sources really were attempted.
    timeline.registry_sources_blocked = (
        not gleif_record_available and company_number_basis is None
    )
    timeline.company_number_basis = company_number_basis
    return timeline


__all__ = ["fetch_timeline"]
