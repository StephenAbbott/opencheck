"""Securities (ISIN) assembly for an entity's LEI.

Combines three open datasets, each in the role it is actually good at:

* **GLEIF** ``/lei-records/{lei}/isins`` — the authoritative LEI→ISIN list and
  the *total count*. Major issuers carry tens of thousands of ISINs (Deutsche
  Bank ≈ 22,500), so we only ever pull a *count + one page* — never the lot.
  CC0, no key.
* **OpenFIGI** ``/v3/mapping`` — types the handful of ISINs we actually display
  (security type, name, ticker, exchange). Called only on the shown subset, so
  cost stays O(dozens). Optional key (``X-OPENFIGI-APIKEY``) raises the limit.
* **OpenSanctions** ``/search/securities`` — the sanction overlay. Surfaces the
  sanctioned ISINs tied to the LEI (and fills GLEIF's blind spot for sanctioned
  Russian securities, which GLEIF/ANNA omits). CC-BY-NC — the sanctioned section
  carries the non-commercial notice.

The design rule: **never enumerate every ISIN**. Sanctioned securities (small,
pre-filtered, high value) come from OpenSanctions independently of GLEIF paging;
the long tail is a count behind a page.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import quote

from .config import get_settings
from .http import build_client

log = logging.getLogger(__name__)

_GLEIF_ISINS_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/isins"
_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
_OPENSANCTIONS_URL = "https://api.opensanctions.org/search/securities"

# ISINs shown per drawer page. Kept small so OpenFIGI enrichment stays cheap.
PAGE_SIZE = 20
# OpenFIGI batch limits: 100 jobs/request with a key, 10 without.
_OPENFIGI_BATCH_KEYED = 100
_OPENFIGI_BATCH_ANON = 10
# How many OpenSanctions security results to scan for sanctioned ISINs.
_SANCTIONED_SCAN_LIMIT = 50

_OS_NC_NOTICE = (
    "OpenSanctions is licensed under CC-BY-NC-4.0. Commercial re-use of this "
    "data is not permitted under the source license."
)

# OpenSanctions dataset id → human regime label (best-effort; unknown ids fall
# back to the raw id so nothing is hidden).
_REGIME_LABELS: dict[str, str] = {
    "us_ofac_sdn": "US OFAC SDN",
    "us_ofac_cons": "US OFAC (non-SDN)",
    "eu_fsf": "EU",
    "eu_journal_sanctions": "EU",
    "gb_hmt_sanctions": "UK HMT",
    "gb_hmt_invbans": "UK investment ban",
    "ch_seco_sanctions": "Swiss SECO",
    "ca_dfatd_sema_sanctions": "Canada",
    "au_dfat_sanctions": "Australia",
    "jp_mof_sanctions": "Japan",
    "ru_nsd_isin": "Russia NSD (EO 14071)",
}


def _regimes(datasets: list[str], topics: list[str]) -> list[str]:
    labels = [_REGIME_LABELS.get(d, d) for d in datasets if d in _REGIME_LABELS]
    # De-duplicate, preserve order.
    seen: set[str] = set()
    out: list[str] = []
    for label in labels:
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out


def _row(
    isin: str,
    figi: dict[str, Any] | None,
    sanctioned: bool,
    meta: dict[str, Any] | None,
) -> dict[str, Any]:
    figi = figi or {}
    row: dict[str, Any] = {
        "isin": isin,
        "type": figi.get("type"),
        "name": figi.get("name"),
        "ticker": figi.get("ticker"),
        "exchange": figi.get("exchange"),
        "sanctioned": bool(sanctioned),
    }
    if sanctioned and meta:
        row["regimes"] = meta.get("regimes") or []
        if meta.get("opensanctions_id"):
            row["opensanctions_id"] = meta["opensanctions_id"]
    return row


async def _gleif_isins(client, lei: str, page: int, page_size: int) -> tuple[int, list[str]]:
    """Return ``(total, isins)`` for one page of the LEI's ISINs (GLEIF, CC0)."""
    url = _GLEIF_ISINS_URL.format(lei=quote(lei))
    resp = await client.get(
        url, params={"page[size]": page_size, "page[number]": page}
    )
    if resp.status_code == 404:
        return 0, []
    resp.raise_for_status()
    payload = resp.json()
    total = (
        (payload.get("meta") or {}).get("pagination", {}).get("total", 0)
    )
    isins: list[str] = []
    for node in payload.get("data") or []:
        isin = (node.get("attributes") or {}).get("isin") or node.get("id")
        if isin:
            isins.append(isin)
    return int(total or 0), isins


async def _openfigi_map(client, isins: list[str], api_key: str | None) -> dict[str, dict[str, Any]]:
    """Map ISIN → security metadata via OpenFIGI. Best-effort; missing/erroring
    ISINs are simply left untyped."""
    if not isins:
        return {}
    batch = _OPENFIGI_BATCH_KEYED if api_key else _OPENFIGI_BATCH_ANON
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(isins), batch):
        chunk = isins[start : start + batch]
        body = [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]
        try:
            resp = await client.post(_OPENFIGI_URL, json=body, headers=headers)
            resp.raise_for_status()
            results = resp.json()
        except Exception as exc:  # noqa: BLE001 — enrichment is non-fatal
            log.warning("OpenFIGI mapping failed (%d ISINs): %s", len(chunk), exc)
            continue
        for isin, result in zip(chunk, results or []):
            data = (result or {}).get("data") or []
            if not data:
                continue
            d = data[0]
            out[isin] = {
                "type": d.get("securityType2") or d.get("securityType"),
                "name": d.get("name"),
                "ticker": d.get("ticker"),
                "exchange": d.get("exchCode"),
                "marketSector": d.get("marketSector"),
            }
    return out


async def _sanctioned_isins(client, lei: str, api_key: str | None) -> dict[str, dict[str, Any]]:
    """Return ``{isin: meta}`` for sanctioned securities tied to this LEI.

    Best-effort against the OpenSanctions ``securities`` scope. We collect ISINs
    from any result that is a sanctions *target* or carries a ``sanction`` topic,
    handling both ``Security`` entities (``isin`` property) and issuer entities
    that list their securities. Returns ``{}`` on any error or unexpected shape
    so the feature degrades to "GLEIF + OpenFIGI only" rather than breaking.
    """
    if not api_key:
        return {}
    headers = {"Authorization": f"ApiKey {api_key}"}
    try:
        resp = await client.get(
            _OPENSANCTIONS_URL,
            params={"q": lei, "limit": _SANCTIONED_SCAN_LIMIT},
            headers=headers,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001 — overlay is non-fatal
        log.warning("OpenSanctions securities lookup failed for %s: %s", lei, exc)
        return {}

    out: dict[str, dict[str, Any]] = {}
    for res in payload.get("results") or []:
        props = res.get("properties") or {}
        topics = res.get("topics") or props.get("topics") or []
        is_sanctioned = res.get("target") is True or any("sanction" in str(t) for t in topics)
        if not is_sanctioned:
            continue
        candidates = props.get("isin") or props.get("isinCode") or props.get("securities") or []
        if isinstance(candidates, str):
            candidates = [candidates]
        meta = {
            "opensanctions_id": res.get("id"),
            "name": res.get("caption"),
            "regimes": _regimes(res.get("datasets") or [], topics),
        }
        for isin in candidates:
            if isinstance(isin, str) and isin:
                out.setdefault(isin, meta)
    return out


async def assemble_securities(
    lei: str, *, page: int = 1, page_size: int = PAGE_SIZE
) -> dict[str, Any]:
    """Assemble the securities view for an LEI from GLEIF + OpenFIGI + OpenSanctions."""
    lei = lei.strip().upper()
    settings = get_settings()
    empty = {
        "lei": lei,
        "available": False,
        "total": 0,
        "page": page,
        "page_size": page_size,
        "securities": [],
        "sanctioned": [],
        "sources": [],
        "license_notices": [],
    }
    if not settings.allow_live:
        return empty

    async with build_client() as client:
        total, isins = await _gleif_isins(client, lei, page, page_size)
        sanctioned_map, figi_map = await asyncio.gather(
            _sanctioned_isins(client, lei, settings.opensanctions_api_key),
            _openfigi_map(client, isins, settings.openfigi_api_key),
        )
        # Sanctioned ISINs may not be in the current GLEIF page (or in GLEIF at
        # all — e.g. Rosneft). Enrich those too so the banner shows their type.
        missing = [i for i in sanctioned_map if i not in figi_map]
        if missing:
            figi_map.update(await _openfigi_map(client, missing, settings.openfigi_api_key))

    securities = [
        _row(isin, figi_map.get(isin), isin in sanctioned_map, sanctioned_map.get(isin))
        for isin in isins
    ]
    sanctioned = [
        _row(isin, figi_map.get(isin), True, meta) for isin, meta in sanctioned_map.items()
    ]

    sources = ["gleif"]
    if figi_map or settings.openfigi_api_key:
        sources.append("openfigi")
    if settings.opensanctions_api_key:
        sources.append("opensanctions")

    return {
        "lei": lei,
        "available": True,
        "total": total,
        "page": page,
        "page_size": page_size,
        "securities": securities,
        "sanctioned": sanctioned,
        "sources": sources,
        "license_notices": (
            [{"source_id": "opensanctions", "notice": _OS_NC_NOTICE}] if sanctioned else []
        ),
    }
