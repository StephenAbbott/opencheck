"""India — Ministry of Corporate Affairs (MCA) Company Master Data adapter.

The MCA Company Master Data is India's national company register extract,
published as open data on the Open Government Data (OGD) Platform India
(data.gov.in) under the Government Open Data License – India (GODL). The
"Registrars of Companies (RoC)-wise Company Master Data" resource carries
~3.67M company records (live-verified 2026-08-08; resource ``updated_date``
2026-07-22): CIN, name, RoC, class/category/sub-category, authorised and
paid-up capital, registration date, registered office address, listing
status, company status, and NIC industrial classification.

API (OGD platform, key required)::

    GET https://api.data.gov.in/resource/{RESOURCE_UUID}
        ?api-key=<key>&format=json&limit=N&filters[<Field>]=<value>

All fields are Elasticsearch ``keyword`` type — **exact match only**, with
names stored UPPERCASE. There is no fuzzy/full-text search, so the primary
flow is the GLEIF bridge (India has the world's largest active-LEI count):

  1. GLEIF returns ``registeredAt.id == "RA000394"`` (Ministry of Corporate
     Affairs) and ``registeredAs`` = the 21-character CIN for Indian entities.
  2. routers/lookup.py derives ``derived["in_cin"]`` and calls ``fetch()``,
     which queries ``filters[CIN]`` — one exact hit or nothing.

``search()`` is exact-name-only (uppercased) and is offered on that basis.

Entity-level only — the master data has no officers or beneficial owners —
so the BODS mapper (``map_mca_india``) produces a single entity statement.

Activation: set ``DATA_GOV_IN_API_KEY`` in .env (a data.gov.in account key;
registration currently requires a JanParichay/MeriPehchaan login). Live only
when ``OPENCHECK_ALLOW_LIVE`` is also true. data.gov.in's *documented sample
key* works for smoke tests but is globally shared and heavily rate-limited
(HTTP 429) — do not ship it as configuration.

GLEIF RA code: RA000394 (Ministry of Corporate Affairs → CIN).
License: Government Open Data License – India (GODL).
  https://data.gov.in/government-open-data-license-india
Attribution: Contains Ministry of Corporate Affairs Company Master Data,
  published by the Open Government Data (OGD) Platform India and used under
  the Government Open Data License – India. MCA and the OGD Platform do not
  endorse OpenCheck or this use.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.mca_india import MCABundle

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Ministry of Corporate Affairs.
MCA_RA_CODE: str = "RA000394"

# OGD Platform resource: "Registrars of Companies (RoC)-wise Company Master
# Data" (catalog "Company Master Data", Ministry of Corporate Affairs).
_RESOURCE_UUID = "4dbe5667-7b6b-41d7-82af-211562424d9a"
_API_URL = f"https://api.data.gov.in/resource/{_RESOURCE_UUID}"
_RESOURCE_PAGE = (
    "https://www.data.gov.in/resource/registrars-companies-roc-wise-company-master-data"
)

# 21-char CIN: listing letter (L/U/F) + 5-digit NIC code + 2-letter state +
# 4-digit year + 3-letter ownership type + 6-digit RoC serial,
# e.g. L85110KA1981PLC013115. Kept advisory: a non-matching identifier still
# round-trips the API (exact filter → no record → stub) rather than erroring.
_CIN_RE = re.compile(r"^[A-Z]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")


def normalise_cin(raw: str) -> str:
    """Return the canonical CIN — uppercase, all separators stripped.

    GLEIF stores the CIN verbatim; occasional records carry stray spaces or
    hyphens. Raises ValueError when nothing identifier-like remains (the
    lookup pipeline then skips this adapter).
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", str(raw or "")).upper()
    if not cleaned:
        raise ValueError("empty CIN")
    return cleaned


def looks_like_cin(value: str) -> bool:
    """Shape check for a company CIN (LLPINs/FCRNs are shorter, no match)."""
    return bool(_CIN_RE.match(value))


class McaIndiaAdapter(SourceAdapter):
    """Source adapter for the MCA Company Master Data (data.gov.in)."""

    id = "mca_india"

    lookup_derivers = (
        LookupDeriver(frozenset({MCA_RA_CODE}), "in_cin", normalise_cin),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        key = getattr(settings, "data_gov_in_api_key", None)
        return SourceInfo(
            id=self.id,
            name="Ministry of Corporate Affairs — Company Master Data (India)",
            homepage="https://www.data.gov.in/catalog/company-master-data",
            description=(
                "India's national company register extract — CIN, company "
                "name, status, class/category, authorised and paid-up "
                "capital, registration date, Registrar of Companies, "
                "registered office address and NIC industrial classification "
                "— from the Ministry of Corporate Affairs Company Master "
                "Data on the Open Government Data Platform. Entity-level "
                "only; no officer or ownership data. Exact-match search."
            ),
            license="GODL-India",
            attribution=(
                "Contains Ministry of Corporate Affairs Company Master Data, "
                "published by the Open Government Data (OGD) Platform India "
                "and used under the Government Open Data License – India. "
                "MCA and the OGD Platform do not endorse this use."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(settings.allow_live and key),
            is_national_register=True,
            country="IN",
        )

    def _key(self) -> str:
        return getattr(get_settings(), "data_gov_in_api_key", None) or ""

    async def _query(self, filters: dict[str, str], limit: int) -> list[dict[str, Any]]:
        """Run one exact-filter query; return the ``records`` list.

        Returns ``[]`` on any HTTP/parse failure — including the OGD
        platform's HTTP 429 rate-limit response — after logging.
        """
        params: dict[str, str] = {
            "api-key": self._key(),
            "format": "json",
            "limit": str(limit),
        }
        for field, value in filters.items():
            params[f"filters[{field}]"] = value
        try:
            async with build_client() as client:
                resp = await client.get(_API_URL, params=params)
        except Exception as exc:  # noqa: BLE001
            logger.warning("mca_india: request failed: %s", exc)
            return []
        if resp.status_code == 429:
            logger.warning("mca_india: OGD API rate limit exceeded (HTTP 429)")
            return []
        if not resp.is_success:
            logger.warning("mca_india: HTTP %s from OGD API", resp.status_code)
            return []
        try:
            data = resp.json()
        except ValueError as exc:
            logger.warning("mca_india: could not parse OGD response: %s", exc)
            return []
        records = data.get("records")
        return records if isinstance(records, list) else []

    # ------------------------------------------------------------------
    # Search (exact company name) — used by the standalone /search path
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        if not self.info.live_available:
            return self._stub_search(query)
        # Names are stored UPPERCASE and the field is exact-match keyword.
        name = " ".join((query or "").upper().split())
        if not name:
            return []
        records = await self._query({"CompanyName": name}, limit=10)
        hits: list[SourceHit] = []
        for r in records:
            if not isinstance(r, dict):
                continue
            cin = (r.get("CIN") or "").strip().upper()
            rec_name = (r.get("CompanyName") or "").strip()
            if not cin or not rec_name:
                continue
            summary_bits = [f"IN-CIN {cin}"]
            roc = (r.get("CompanyROCcode") or "").strip()
            status = (r.get("CompanyStatus") or "").strip()
            if roc:
                summary_bits.append(roc)
            if status:
                summary_bits.append(status)
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=cin,
                    kind=SearchKind.ENTITY,
                    name=rec_name,
                    summary=" · ".join(summary_bits),
                    identifiers={"in_cin": cin},
                    raw=r,
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch (by CIN, exact)
    # ------------------------------------------------------------------

    def _stub(self, identifier: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "cin": identifier or "U00000KA2000PLC000000",
            "name": legal_name or "",
            "status": None,
            "category": None,
            "sub_category": None,
            "company_class": None,
            "listing_status": None,
            "authorized_capital": None,
            "paidup_capital": None,
            "registration_date": None,
            "address": None,
            "state_code": None,
            "roc_code": None,
            "indian_foreign": None,
            "nic_code": None,
            "industrial_classification": None,
            "link": _RESOURCE_PAGE,
            "is_stub": True,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return MCA master data for a CIN (21-char, exact match).

        ``legal_name`` is a GLEIF fallback for the stub.
        """
        try:
            cin = normalise_cin(hit_id)
        except ValueError:
            return self._stub("", legal_name)

        if not self.info.live_available:
            return self._stub(cin, legal_name)

        records = await self._query({"CIN": cin}, limit=1)
        record = records[0] if records and isinstance(records[0], dict) else None
        if not record:
            return self._stub(cin, legal_name)

        def _s(field: str) -> str | None:
            value = (record.get(field) or "").strip()
            return value or None

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "cin": (_s("CIN") or cin).upper(),
            "name": _s("CompanyName") or legal_name or "",
            "status": _s("CompanyStatus"),
            "category": _s("CompanyCategory"),
            "sub_category": _s("CompanySubCategory"),
            "company_class": _s("CompanyClass"),
            "listing_status": _s("Listingstatus"),
            "authorized_capital": _s("AuthorizedCapital"),
            "paidup_capital": _s("PaidupCapital"),
            "registration_date": _s("CompanyRegistrationdate_date"),
            "address": _s("Registered_Office_Address"),
            "state_code": _s("CompanyStateCode"),
            "roc_code": _s("CompanyROCcode"),
            "indian_foreign": _s("CompanyIndian/Foreign Company"),
            "nic_code": _s("nic_code"),
            "industrial_classification": _s("CompanyIndustrialClassification"),
            "link": _RESOURCE_PAGE,
            "is_stub": False,
        }
        validate_raw("mca_india", MCABundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Stub
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="U00000KA2000PLC000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub MCA record — set DATA_GOV_IN_API_KEY and enable live mode.",
                identifiers={"in_cin": "U00000KA2000PLC000000"},
                raw={"cin": "U00000KA2000PLC000000"},
                is_stub=True,
            )
        ]
