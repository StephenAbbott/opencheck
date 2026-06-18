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

import json
import logging
import os
import urllib.request
from typing import Any
from urllib.parse import quote

from .config import get_settings
from .http import build_client

log = logging.getLogger(__name__)

_GLEIF_ISINS_URL = "https://api.gleif.org/api/v1/lei-records/{lei}/isins"
_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# ISINs shown per drawer page. Kept small so OpenFIGI enrichment stays cheap.
PAGE_SIZE = 20
# OpenFIGI batch limits: 100 jobs/request with a key, 10 without.
_OPENFIGI_BATCH_KEYED = 100
_OPENFIGI_BATCH_ANON = 10

_OS_NC_NOTICE = (
    "OpenSanctions is licensed under CC-BY-NC-4.0. Commercial re-use of this "
    "data is not permitted under the source license."
)


# ---------------------------------------------------------------------------
# Sanctioned-securities index (built from OpenSanctions securities.csv)
# ---------------------------------------------------------------------------
#
# OpenSanctions has no live "sanctioned securities by LEI" API — that collection
# is a packaging of a bulk CSV export. ``scripts/extract_securities.py`` turns
# it into a compact JSON index keyed by LEI; we load that file here.


# Loaded once (from a local file or a URL) and cached. Reset in tests via
# ``reset_index_cache``; warmed at startup via ``warm_index``.
_INDEX_CACHE: dict[str, Any] | None = None


def reset_index_cache() -> None:
    global _INDEX_CACHE
    _INDEX_CACHE = None


def _load_index() -> dict[str, Any]:
    """Load the sanctioned-securities index from the configured file or URL."""
    settings = get_settings()
    path = settings.securities_index_file
    if path and os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:  # noqa: BLE001 — overlay is non-fatal
            log.warning("Failed to load securities index file %s: %s", path, exc)
            return {}
    url = settings.securities_index_url
    if url:
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 — operator-set URL
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 — overlay is non-fatal
            log.warning("Failed to download securities index %s: %s", url, exc)
            return {}
    return {}


def _index() -> dict[str, Any]:
    global _INDEX_CACHE
    if _INDEX_CACHE is None:
        _INDEX_CACHE = _load_index()
    return _INDEX_CACHE


def warm_index() -> None:
    """Force-load the index (run off the event loop at startup)."""
    _index()


def _sanctioned_for_lei(lei: str) -> dict[str, dict[str, Any]]:
    """``{isin: meta}`` for sanctioned securities tied to this LEI (from the index)."""
    entry = _index().get(lei.upper())
    if not entry:
        return {}
    meta = {
        "regimes": entry.get("regimes") or [],
        "opensanctions_id": entry.get("id"),
        "name": entry.get("name"),
    }
    return {isin: meta for isin in entry.get("isins") or [] if isin}


def sanctioned_securities_signal(lei: str) -> dict[str, Any] | None:
    """A ``SANCTIONED_SECURITY`` risk-signal dict if the LEI has sanctioned
    securities in the index, else ``None``. Shaped like ``RiskSignal.to_dict()``
    so the lookup pipeline can merge it with the other signals."""
    entry = _index().get(lei.strip().upper())
    if not entry:
        return None
    isins = [i for i in (entry.get("isins") or []) if i]
    if not isins:
        return None
    regimes = entry.get("regimes") or []
    n = len(isins)
    summary = (
        f"{n} sanctioned secur{'ity' if n == 1 else 'ities'} mapped to this entity"
        + (f" ({', '.join(regimes)})" if regimes else "")
        + " — its securities are subject to sanctions / investment bans."
    )
    return {
        "code": "SANCTIONED_SECURITY",
        "confidence": "high",
        "summary": summary,
        "source_id": "opensanctions",
        "hit_id": entry.get("id") or lei.strip().upper(),
        "evidence": {
            "isin_count": n,
            "regimes": regimes,
            "sample_isins": isins[:5],
            "eo_14071": bool(entry.get("eo_14071")),
        },
    }


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


async def assemble_securities(
    lei: str, *, page: int = 1, page_size: int = PAGE_SIZE
) -> dict[str, Any]:
    """Assemble the securities view for an LEI from GLEIF + OpenFIGI + the
    OpenSanctions sanctioned-securities index."""
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

    # Sanctioned overlay comes from the local index (no network); on only when
    # an index file is configured and loads.
    overlay_on = bool(settings.securities_index_file or settings.securities_index_url) and bool(_index())
    sanctioned_map = _sanctioned_for_lei(lei) if overlay_on else {}

    async with build_client() as client:
        total, isins = await _gleif_isins(client, lei, page, page_size)
        figi_map = await _openfigi_map(client, isins, settings.openfigi_api_key)
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
    if overlay_on:
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
