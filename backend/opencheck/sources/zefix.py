"""Zefix (Swiss Federal Commercial Registry) adapter.

Zefix is the publicly accessible service of the Federal Commercial Registry
Office (FCRO / EHRA), operated under the Federal Department of Justice and
Police (FDJP).  It surfaces data from the central database of legal entities
(Zentralregister) for all companies entered in the Swiss commercial register.

Live endpoints used:

* ``POST /api/v1/company/search`` — entity search by name
* ``GET  /api/v1/company/uid/{uid}`` — full company record by Swiss UID

Authentication: HTTP Basic (username + password supplied by FCRO).
Request credentials via zefix@bj.admin.ch.

The flow with GLEIF:

  1. User searches for a Swiss company.
  2. The GLEIF adapter returns hits with ``registeredAs = "CHE..."`` from
     ``entity.registeredAt`` RA codes RA000548 / RA000549 (FSO linkage).
  3. That CHE identifier is used as the ``hit_id`` here, or the user may
     search Zefix directly by name.

Data is available under CC BY 4.0 (opendata.swiss terms_by).
API reference: https://www.zefix.admin.ch/ZefixPublicREST/swagger-ui/index.html
"""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_API_BASE = "https://www.zefix.admin.ch/ZefixPublicREST/api/v1"
_CACHE_NS = "zefix"

# GLEIF Registration Authority codes that identify the Swiss UID register.
# Both RA000548 (FSO primary) and RA000549 (FSO cantonal/secondary) are used
# in practice.  These are checked in gleif.py to expose ``che_uid`` on hits.
CH_RA_CODES: frozenset[str] = frozenset({"RA000548", "RA000549"})

_UID_DIGITS_RE = re.compile(r"CHE[-.]?(\d{3})[-.]?(\d{3})[-.]?(\d{3})", re.IGNORECASE)


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def normalise_uid(uid: str) -> str:
    """Strip separators so ``CHE-313.550.547`` → ``CHE313550547`` for API calls."""
    m = _UID_DIGITS_RE.match(uid.strip())
    if m:
        return f"CHE{m.group(1)}{m.group(2)}{m.group(3)}"
    # Already clean or unrecognised format — return as-is.
    return uid.replace("-", "").replace(".", "").upper()


def format_uid(uid: str) -> str:
    """Format ``CHE313550547`` → ``CHE-313.550.547`` for display and BODS identifiers."""
    clean = normalise_uid(uid)
    if clean.upper().startswith("CHE") and len(clean) == 12:
        digits = clean[3:]
        return f"CHE-{digits[:3]}.{digits[3:6]}.{digits[6:]}"
    return uid


class ZefixAdapter(SourceAdapter):
    """Source adapter for Zefix — Swiss Federal Commercial Registry."""

    id = "zefix"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        live = (
            settings.allow_live
            and bool(settings.zefix_username)
            and bool(settings.zefix_password)
        )
        return SourceInfo(
            id=self.id,
            name="Zefix — Swiss Commercial Registry",
            homepage="https://www.zefix.ch/",
            description=(
                "Swiss company data from the Federal Commercial Registry "
                "(Zefix / FCRO), sourced via the Swiss UID."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Swiss Federal Commercial Registry "
                "Office (FCRO / EHRA) via Zefix, available under CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        if not self.info.live_available:
            return self._stub_search(query)

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            results = cached[0]
        else:
            settings = get_settings()
            async with build_client() as client:
                response = await client.post(
                    f"{_API_BASE}/company/search",
                    json={"name": query, "activeOnly": True},
                    auth=(settings.zefix_username or "", settings.zefix_password or ""),
                )
                response.raise_for_status()
                results = response.json()
            self._cache.put(cache_key, results)

        return [self._company_short_hit(item) for item in (results or [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Return full Zefix record for a Swiss UID (CHE number).

        ``hit_id`` may be in any CHE format — ``CHE313550547``,
        ``CHE-313.550.547``, or ``CHE313.550.547``.  It is normalised
        before the API call.
        """
        uid = normalise_uid(hit_id)
        cache_key = f"{_CACHE_NS}/uid/{uid}"
        cached = self._cache.get_payload(cache_key)

        if cached is not None:
            data = cached[0]
        elif not self.info.live_available:
            return {"source_id": self.id, "uid": uid, "company": None, "is_stub": True}
        else:
            settings = get_settings()
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}/company/uid/{quote(uid)}",
                    auth=(settings.zefix_username or "", settings.zefix_password or ""),
                )
                response.raise_for_status()
                data = response.json()
            self._cache.put(cache_key, data)

        # The UID endpoint returns a list (a UID may appear in multiple cantons
        # for branch offices, but the first entry is the head office record).
        company = data[0] if isinstance(data, list) and data else (data or {})
        return {
            "source_id": self.id,
            "uid": uid,
            "company": company,
            "is_stub": False,
        }

    # ------------------------------------------------------------------
    # Hit factories
    # ------------------------------------------------------------------

    @staticmethod
    def _company_short_hit(item: dict[str, Any]) -> SourceHit:
        uid = item.get("uid") or ""
        name = item.get("name") or ""
        legal_seat = item.get("legalSeat") or ""
        status = item.get("status") or ""
        lf = (item.get("legalForm") or {}).get("name") or {}
        legal_form_en = lf.get("en") or lf.get("de") or ""

        summary_parts = [f"UID {format_uid(uid)}"]
        if legal_seat:
            summary_parts.append(legal_seat)
        if legal_form_en:
            summary_parts.append(legal_form_en)
        if status and status != "ACTIVE":
            summary_parts.append(status.lower())

        return SourceHit(
            source_id="zefix",
            hit_id=uid,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_parts),
            identifiers={"che_uid": format_uid(uid)},
            raw=item,
            is_stub=False,
        )

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="CHE123456789",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub Zefix record — set OPENCHECK_ALLOW_LIVE=true "
                    "and configure ZEFIX_USERNAME / ZEFIX_PASSWORD."
                ),
                identifiers={"che_uid": "CHE-123.456.789"},
                raw={"uid": "CHE123456789", "name": f"{query} (stub)"},
            )
        ]
