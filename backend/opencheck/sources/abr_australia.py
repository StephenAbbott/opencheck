"""Australian Business Register (ABR) — ABN Lookup adapter.

ABN Lookup is the public view of the Australian Business Register, operated by
the Australian Taxation Office (ATO). It exposes free web services (registered
GUID required) over the public ABN/ACN/business-name data, updated hourly from
the ABR.

This adapter uses the lightweight **JSON (JSONP) endpoints** rather than the
SOAP/WSDL service:

  * ABN lookup:  GET /json/AbnDetails.aspx?abn=<abn>&callback=callback&guid=<guid>
  * ACN lookup:  GET /json/AcnDetails.aspx?acn=<acn>&callback=callback&guid=<guid>
  * Name search: GET /json/MatchingNames.aspx?name=<q>&maxResults=N&callback=callback&guid=<guid>

All three return a JSONP body — ``callback({...})`` — which this adapter
unwraps to JSON. The AbnDetails/AcnDetails payload shape (observed live) is::

    {"Abn","AbnStatus","AbnStatusEffectiveFrom","Acn","AddressDate",
     "AddressPostcode","AddressState","BusinessName":[],"EntityName",
     "EntityTypeCode","EntityTypeName","Gst","Message"}

``Message`` is populated on errors / not-found (e.g. an unrecognised GUID).
ABN Lookup is **entity-level only** — there is no officer or beneficial-owner
data — so the BODS mapper produces a single entity statement.

GLEIF bridge (two RA codes — Australian entities use either):
  * ``registeredAt.id == "RA000014"`` (ASIC) → ``registeredAs`` is the **ACN**
    (9 digits, e.g. "676 964 677") → looked up via AcnDetails.
  * ``registeredAt.id == "RA000013"`` (ABR/ATO) → ``registeredAs`` is the
    **ABN** (11 digits, e.g. "31 976 733 718") → looked up via AbnDetails.

``routers/lookup.py`` extracts ``derived["au_acn"]`` / ``derived["au_abn"]``
and calls ``fetch()``; the adapter routes by digit length.

Activation: set ``ABN_GUID=<your-guid>`` in .env (free registration at
abr.business.gov.au). Live only when ``OPENCHECK_ALLOW_LIVE`` is also true.

License: Creative Commons Attribution 3.0 Australia (CC BY 3.0 AU).
  https://creativecommons.org/licenses/by/3.0/au/
Attribution: Contains data sourced from the Australian Business Register (ABR),
  used under CC BY 3.0 AU. The ATO does not endorse OpenCheck or this use.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import quote

from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.abr_australia import ABRBundle

logger = logging.getLogger(__name__)

# GLEIF Registration Authority codes for Australia.
ABR_ASIC_RA_CODE: str = "RA000014"  # ASIC Register of Companies → ACN
ABR_ABR_RA_CODE: str = "RA000013"   # Australian Business Register → ABN

_JSON_BASE = "https://abr.business.gov.au/json"
_VIEW_URL = "https://abr.business.gov.au/ABN/View?abn={abn}"
_SEARCH_URL = "https://abr.business.gov.au/"

_JSONP_RE = re.compile(r"^[A-Za-z_$][\w$]*\((.*)\)\s*;?\s*$", re.DOTALL)


def _digits(raw: str) -> str:
    return re.sub(r"\D", "", str(raw or "").strip())


def normalise_acn(raw: str) -> str:
    """Return a 9-digit ACN (digits only). GLEIF formats it as '676 964 677'."""
    return _digits(raw)


def normalise_abn(raw: str) -> str:
    """Return an 11-digit ABN (digits only). GLEIF formats it as '31 976 733 718'."""
    return _digits(raw)


def _unwrap_jsonp(text: str) -> dict[str, Any]:
    """Strip a ``callback(...)`` JSONP wrapper and parse the inner JSON."""
    stripped = (text or "").strip()
    m = _JSONP_RE.match(stripped)
    payload = m.group(1) if m else stripped
    try:
        data = json.loads(payload)
        return data if isinstance(data, dict) else {}
    except (TypeError, ValueError) as exc:
        logger.warning("abr_australia: could not parse JSONP response: %s", exc)
        return {}


class AbrAustraliaAdapter(SourceAdapter):
    """Source adapter for the Australian Business Register (ABN Lookup)."""

    id = "abr_australia"

    lookup_derivers = (
        LookupDeriver(frozenset({ABR_ASIC_RA_CODE}), "au_acn", normalise_acn),
        LookupDeriver(frozenset({ABR_ABR_RA_CODE}), "au_abn", normalise_abn),
    )
    lookup_pass_legal_name = True


    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        guid = getattr(settings, "abn_guid", None)
        return SourceInfo(
            id=self.id,
            name="Australian Business Register (ABN Lookup)",
            homepage="https://abr.business.gov.au/",
            description=(
                "Australian company and business data — ABN, ACN, entity name "
                "and type, ABN/GST status, registered state and postcode, and "
                "business (trading) names — from the Australian Business "
                "Register's free ABN Lookup web services. Entity-level only; "
                "no officer or ownership data."
            ),
            license="CC-BY-3.0-AU",
            attribution=(
                "Contains data sourced from the Australian Business Register "
                "(ABR), used under CC BY 3.0 AU. The Australian Taxation Office "
                "does not endorse this use."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(settings.allow_live and guid),
            is_national_register=True,
            country="AU",
        )

    def _guid(self) -> str:
        return getattr(get_settings(), "abn_guid", None) or ""

    # ------------------------------------------------------------------
    # Search (name) — used by the standalone /search path
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        if not self.info.live_available:
            return self._stub_search(query)
        url = (
            f"{_JSON_BASE}/MatchingNames.aspx?name={quote(query)}"
            f"&maxResults=10&callback=callback&guid={self._guid()}"
        )
        try:
            async with build_client() as client:
                resp = await client.get(url)
            data = _unwrap_jsonp(resp.text) if resp.is_success else {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("abr_australia: name search failed: %s", exc)
            return []

        names = data.get("Names") or []
        hits: list[SourceHit] = []
        for n in names:
            if not isinstance(n, dict):
                continue
            abn = _digits(n.get("Abn") or "")
            name = (n.get("Name") or "").strip()
            if not name:
                continue
            state = (n.get("State") or "").strip()
            postcode = (n.get("Postcode") or "").strip()
            summary = "AU-ABN " + abn if abn else "AU"
            if state or postcode:
                summary += f" · {state} {postcode}".rstrip()
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=abn or name,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=summary.strip(" ·"),
                    identifiers={"au_abn": abn} if abn else {},
                    raw=n,
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch (by ABN or ACN — routed on digit length)
    # ------------------------------------------------------------------

    def _stub(self, identifier: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "abn": identifier if len(identifier) == 11 else "",
            "acn": identifier if len(identifier) == 9 else "",
            "name": legal_name or "",
            "entity_type_code": None,
            "entity_type_name": None,
            "abn_status": None,
            "abn_status_from": None,
            "state": None,
            "postcode": None,
            "gst": None,
            "business_names": [],
            "link": _SEARCH_URL,
            "is_stub": True,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return ABR data for an ABN (11 digits) or ACN (9 digits).

        ``hit_id`` is normalised to digits and routed: 11 → AbnDetails,
        9 → AcnDetails. ``legal_name`` is a GLEIF fallback for the stub.
        """
        ident = _digits(hit_id)
        if len(ident) == 11:
            url = (
                f"{_JSON_BASE}/AbnDetails.aspx?abn={ident}"
                f"&callback=callback&guid={self._guid()}"
            )
        elif len(ident) == 9:
            url = (
                f"{_JSON_BASE}/AcnDetails.aspx?acn={ident}"
                f"&callback=callback&guid={self._guid()}"
            )
        else:
            return self._stub(ident, legal_name)

        if not self.info.live_available:
            return self._stub(ident, legal_name)

        try:
            async with build_client() as client:
                resp = await client.get(url)
            if not resp.is_success:
                logger.warning("abr_australia: HTTP %s for %s", resp.status_code, ident)
                return self._stub(ident, legal_name)
            data = _unwrap_jsonp(resp.text)
        except Exception as exc:  # noqa: BLE001
            logger.warning("abr_australia: fetch failed (%s): %s", ident, exc)
            return self._stub(ident, legal_name)

        abn = _digits(data.get("Abn") or "")
        name = (data.get("EntityName") or "").strip()
        # No ABN/name in payload → not found or error (Message populated).
        if not abn and not name:
            return self._stub(ident, legal_name)

        business_names = [
            b.strip() for b in (data.get("BusinessName") or []) if isinstance(b, str) and b.strip()
        ]
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "abn": abn,
            "acn": _digits(data.get("Acn") or "") or (ident if len(ident) == 9 else ""),
            "name": name or legal_name or "",
            "entity_type_code": (data.get("EntityTypeCode") or "").strip() or None,
            "entity_type_name": (data.get("EntityTypeName") or "").strip() or None,
            "abn_status": (data.get("AbnStatus") or "").strip() or None,
            "abn_status_from": (data.get("AbnStatusEffectiveFrom") or "").strip() or None,
            "state": (data.get("AddressState") or "").strip() or None,
            "postcode": (data.get("AddressPostcode") or "").strip() or None,
            "gst": data.get("Gst"),
            "business_names": business_names,
            "link": _VIEW_URL.format(abn=abn) if abn else _SEARCH_URL,
            "is_stub": False,
        }
        validate_raw("abr_australia", ABRBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Stub
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="00000000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub ABR record — set ABN_GUID and enable live mode.",
                identifiers={"au_abn": "00000000000"},
                raw={"abn": "00000000000"},
                is_stub=True,
            )
        ]
