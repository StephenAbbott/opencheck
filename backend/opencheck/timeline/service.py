"""Time Machine fetch service — pull raw change data and assemble a timeline.

Lazy and never on the main lookup (same posture as ``/securities``). Fetches
from every register OpenCheck holds a change log for — five, not the two this
docstring claimed until Phase 190:

- **GLEIF** (key-free): the LEI record (to derive the national registry numbers
  below) and the field-modification change log, partitioned into LEI vs RR
  records.
- **Companies House** (needs ``COMPANIES_HOUSE_API_KEY``): filing history for the
  derived company number. Degrades to GLEIF-only when no key is set or the
  company is not GB / has no CH number.
- **New Zealand Companies Office** (needs ``NZBN_API_KEY``): events
  reconstructed from the NZBN dated records.
- **Estonian e-Äriregister** (needs the RIK credentials): registry-card and
  beneficial-owner history over the read-only SOAP API.
- **Danish CVR** (needs ``CVR_DENMARK_API_KEY``): events reconstructed from the
  bitemporal ``virkning`` records the ordinary adapter fetch already returns.

Every one of them lands in the same ``ChangeEvent`` model and is merged onto one
axis by :mod:`.assemble` — a national register that publishes history is not a
separate timeline, it is more of this one.

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
from .companies_house import officer_change_events
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
_CH_PAGE_SIZE = 100  # the register's maximum
# Phase 194: was 10, so a company with a long life had its history cut at
# 1,000 filings — Lloyds Bank PLC has 2,404 — and Companies House returns
# them newest first, so what went missing was the oldest, which is exactly
# what a history tab is for. Unmarked, the way the officers list was cut
# before Phase 192. 50 pages covers every company measured; a list cut by the
# cap now says so (``filings_truncated``) rather than ending quietly.
_CH_PAGE_CAP = 50  # ≤ 5,000 filings
# Officers are far fewer than filings — Lloyds Bank PLC, the deepest board
# measured, has 232 across its whole life. 20 pages is 2,000.
_CH_OFFICERS_PAGE_CAP = 20


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
) -> tuple[list[dict], bool]:
    """Fetch Companies House filing history for ``number`` (Basic auth: key, '').

    Returns the filings and whether the register holds more than the page cap
    allowed — a truncated history is a fact about the fetch, not about the
    company, and the tab has to be able to say which it is showing.
    """
    filings: list[dict] = []
    truncated = False
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
    else:
        # The loop ran out of pages rather than out of filings.
        truncated = True
    return filings, truncated


async def _ch_officers(
    client: httpx.AsyncClient, number: str, api_key: str
) -> dict | None:
    """Fetch the Companies House officers list for ``number``, all pages.

    Phase 198. The board stream used to be read out of filing history, which
    was free — the fetch above already had it — but a filing publishes a
    *name* and no officer id, so a board row could say who joined and never
    point at them. The officers list publishes the id, which is the same key
    the BODS mapper builds a person statement on, so a row assembled from it
    can address the person the graph draws.

    ``None`` (not ``{}``) when the register refused or was not asked, so the
    caller can tell "no officers" from "did not read the officers": the
    difference between an empty board stream and an unchecked one.

    Paginated the way Phase 192 paginated the lookup adapter's copy of this
    call: the register's default page is 35 officers and Lloyds Bank PLC has
    more than 200 once the resigned ones are counted.
    """
    items: list[dict] = []
    auth = httpx.BasicAuth(api_key, "")
    resigned_count = active_count = 0
    for page in range(_CH_OFFICERS_PAGE_CAP):
        resp = await client.get(
            f"{_CH_API_BASE}/company/{quote(number)}/officers",
            params={
                "items_per_page": _CH_PAGE_SIZE,
                "start_index": page * _CH_PAGE_SIZE,
                # The default omits resigned officers, and a board *history*
                # is mostly resigned officers.
                "register_view": "false",
            },
            auth=auth,
        )
        if resp.status_code in (401, 403, 404):
            return None if page == 0 else {"items": items}
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("items") or []
        items.extend(batch)
        resigned_count = payload.get("resigned_count") or resigned_count
        active_count = payload.get("active_count") or active_count
        total = payload.get("total_results") or 0
        if not batch or (page + 1) * _CH_PAGE_SIZE >= total:
            break
    return {
        "items": items,
        "active_count": active_count,
        "resigned_count": resigned_count,
    }


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
    ch_filings_truncated = False
    ch_officers: dict | None = None

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
            # Two reads of the same register, independent of each other: the
            # filing history behind the administrative stream, and (Phase 198)
            # the officers list behind the board stream. Concurrent, and each
            # allowed to fail without taking the other with it.
            filings_res, officers_res = await asyncio.gather(
                _ch_filings(client, company_number, api_key),
                _ch_officers(client, company_number, api_key),
                return_exceptions=True,
            )
            if isinstance(filings_res, BaseException):
                log.warning(
                    "timeline: CH filing history unavailable for %s: %s",
                    company_number,
                    filings_res,
                )
            else:
                ch_filings, ch_filings_truncated = filings_res
            if isinstance(officers_res, BaseException):
                log.warning(
                    "timeline: CH officers unavailable for %s: %s",
                    company_number,
                    officers_res,
                )
            else:
                ch_officers = officers_res

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

    # The board stream. Emitted here rather than inside ``assemble_timeline``
    # because it comes from a second Companies House call, not from the filing
    # stream that function already classifies.
    officer_events = (
        officer_change_events(ch_officers, company_id=company_number or "")
        if ch_officers
        else []
    )

    timeline = assemble_timeline(
        lei=lei,
        company_number=company_number,
        gleif_lei_mods=lei_mods,
        gleif_rr_mods=rr_mods,
        ch_filings=ch_filings,
        extra_events=nz_events + ee_events + dk_events + officer_events,
    )
    # Phase 190: the numbers above are how each register addresses this
    # company, and until now they were derived, used to decide which history
    # calls to make, and dropped. The History tab links every dated row back to
    # the record it came from, and a link to the Danish or Estonian register
    # cannot be built from an LEI. Present here means "this register knows the
    # company by this number" — not that its history was fetched, which the
    # events themselves say.
    timeline.registry_numbers = {
        source_id: number
        for source_id, number in (
            ("companies_house", company_number),
            ("nz_companies", nz_number),
            ("ariregister", ee_code),
            ("cvr_denmark", dk_cvr),
        )
        if number
    }
    timeline.filings_truncated = ch_filings_truncated
    # Phase 198: whether the officers list was READ, not whether it had rows.
    # An empty board stream means one of two very different things and the tab
    # has to be able to say which.
    timeline.officers_available = ch_officers is not None
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
