"""Malta Business Registry (MBR) adapter.

The MBR is Malta's statutory register of companies and commercial
partnerships. It publishes a free, key-less **Open Data API** (an EU
High-Value Dataset under the Open Data Directive) at
``openapi.baros.mbr.mt``.

Data access:

* ``GET /api/v1/companies/{registration_number}`` — a single company's
  core record: name, type (legal form), state (status), registered
  office address, area of activity, registration number and date.
* ``GET /api/v1/companies?limit=&after=`` — cursor-paginated listing
  (bulk sync only; there is **no** name/``q`` filter, so this adapter does
  not support free-text search — it is entered via the LEI lookup flow).
* ``GET /api/v1/companies/download/{file_name}`` — bulk file download
  (not used here).

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000443"`` (MBR RA code) and
     ``registeredAs == "C 113927"`` (letter prefix + space + digits) for
     Maltese entities.
  2. routers/lookup.py derives ``derived["mt_crn"]`` and calls ``fetch()``.

The API exposes **entity data only** — no directors, shareholders or
beneficial owners — so the BODS mapping yields a single entity statement.
The MBR's separate beneficial-ownership register is not part of this API.

Authentication: none (no API key, confirmed against the OpenAPI spec —
no security schemes; an unauthenticated request returns 200).
License: CC BY 4.0. https://creativecommons.org/licenses/by/4.0/
Attribution: Contains data from the Malta Business Registry, CC BY 4.0.
Open Data API: https://openapi.baros.mbr.mt/swaggerui
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

# MBR Open Data API base.
_API_BASE = "https://openapi.baros.mbr.mt/api/v1"

_CACHE_NS = "malta_mbr"

# GLEIF Registration Authority code for the Malta Business Registry
# (Registry of Companies, mbr.mt) — confirmed live against GLEIF.
MT_RA_CODE: str = "RA000443"

# Maltese registration number: a short letter prefix (commonly "C" for a
# company) + the number, e.g. "C 113927". GLEIF stores it with a space; the
# MBR API path parameter expects the same canonical form.
_MT_ID_RE = re.compile(r"^([A-Za-z]{1,3})\s*(\d+)$")


def normalise_mt_crn(crn: str) -> str:
    """Canonicalise a Maltese registration number to ``"<PREFIX> <digits>"``.

    Collapses whitespace, upper-cases the prefix and ensures a single space
    between the letter prefix and the number (so ``"c113927"`` and
    ``"C  113927"`` both become ``"C 113927"``, matching GLEIF's
    ``registeredAs`` form and the MBR API path parameter). Inputs that do not
    match the prefix+digits shape are returned trimmed/upper-cased unchanged.
    """
    s = " ".join(str(crn).strip().split()).upper()
    m = _MT_ID_RE.match(s)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return s


def _company_url(reg: str) -> str:
    """A stable, dereferenceable URL for the company's MBR record."""
    return f"{_API_BASE}/companies/{quote(reg)}"


class MaltaMbrAdapter(SourceAdapter):
    """Source adapter for the Malta Business Registry Open Data API."""

    id = "malta_mbr"

    lookup_derivers = (
        LookupDeriver(frozenset({MT_RA_CODE}), "mt_crn", normalise_mt_crn),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Malta Business Registry (MBR)",
            homepage="https://mbr.mt/",
            description=(
                "Maltese company data from the Malta Business Registry (MBR) "
                "Open Data API (CC BY 4.0). Provides core entity details — "
                "name, status, legal form, registered office and registration "
                "date — for companies on the Maltese register."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Malta Business Registry, available "
                "under CC BY 4.0 via the MBR Open Data API "
                "(openapi.baros.mbr.mt)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # Search — unsupported (the API has no name/query filter). The adapter
    # is entered via the LEI lookup flow (fetch by registration number), so
    # search intentionally returns [] (see tests/_IDENTIFIER_KEYED).
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — single company by registration number
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        reg = normalise_mt_crn(hit_id)

        def _bundle(company: dict[str, Any] | None, is_stub: bool) -> dict[str, Any]:
            return {
                "source_id": self.id,
                "mt_crn": reg,
                "company": company,
                "legal_name": legal_name,
                "is_stub": is_stub,
            }

        if not reg:
            return _bundle(None, True)

        cache_key = f"{_CACHE_NS}/company/{reg}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return _bundle(None, True)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return _bundle(cached[0] or {}, False)

        url = _company_url(reg)
        async with build_client() as client:
            response = await client.get(url)
            if not response.is_success:
                _LOG.warning(
                    "Malta MBR returned %s for %s — skipping",
                    response.status_code,
                    reg,
                )
                return _bundle(None, False)
            data = response.json()

        # Detail endpoint wraps the record in a top-level ``data`` object.
        company = (data.get("data") if isinstance(data, dict) else None) or {}
        self._cache.put(cache_key, company)
        return _bundle(company, False)
