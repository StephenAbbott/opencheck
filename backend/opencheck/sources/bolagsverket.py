"""Bolagsverket (Swedish Companies Registration Office) adapter.

Bolagsverket operates the Swedish Handelsregistret (Companies Register),
Föreningsregistret (Cooperative Register), and other registers. This
adapter uses the "API for valuable datasets" (Öppna data / PSI-data),
which is free to use but requires a registered API key via the developer
portal at https://portal.api.bolagsverket.se/

The register data includes:
* Entity information: company name, organisation number, status, legal form
* Registered address
* Officers: board members (styrelseledamöter), CEO (VD), signatories
  (firmatecknare), auditors (revisorer), and similar roles.

Unlike INPI, officer data in the Swedish commercial register is entirely
public and PSI-compliant — it is safe to republish and map to BODS
person statements.

The flow with GLEIF:
  1. GLEIF returns ``registeredAt.id == "RA000544"`` (Bolagsverket RA code)
     and ``registeredAs = "<10-digit org number>"`` for Swedish entities.
  2. app.py extracts ``derived["se_org_number"]`` and calls ``fetch()``
     here.
  3. We hit Bolagsverket's API and map the result to BODS entity +
     person statements.

Authentication: Bearer token using the API key from the developer portal.
Portal: https://portal.api.bolagsverket.se/
API docs: https://portal.api.bolagsverket.se/ (requires portal login)

GLEIF RA code: RA000544

Organisation number format: 10-digit number, canonically displayed as
``NNNNNN-NNNN`` (e.g. ``556016-0680`` for Telefonaktiebolaget LM Ericsson).
The API accepts either the hyphenated or raw 10-digit form.

License: PSI-compliant open data. Bolagsverket publishes this data under
Swedish public sector information legislation (PSI-lagen), consistent with
the EU Open Data Directive. Attribution required.

NOTE: The exact endpoint paths and response shape below are based on the
documented API design from the Bolagsverket developer portal. The
``_API_BASE`` constant and the JSON field names in ``fetch()`` /
``map_bolagsverket()`` will need to be verified and adjusted once an API
key has been granted and live responses can be inspected.
"""

from __future__ import annotations

import re
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

# Base URL for the Bolagsverket API gateway (WSO2 APIM).
# This will be confirmed once the API key is granted and the portal
# can be navigated to find the exact path. The pattern below mirrors
# what is documented on the Bolagsverket developer portal landing page.
_API_BASE = "https://portal.api.bolagsverket.se/foretagsinformation/v1"
_CACHE_NS = "bolagsverket"

# GLEIF Registration Authority code for Bolagsverket.
# Confirmed via GLEIF fulltext search for Swedish entities (Ericsson,
# Volvo, etc.) — all carry registeredAt.id == "RA000544".
BV_RA_CODE: str = "RA000544"

# Swedish organisation number: 10 digits, optionally with a hyphen after
# the sixth digit (e.g. ``556016-0680`` or ``5560160680``).
_ORG_NUMBER_RE = re.compile(r"^(\d{6})-?(\d{4})$")


def normalise_org_number(org_number: str) -> str:
    """Normalise a Swedish organisation number to raw 10-digit form.

    Accepts:
    * ``556016-0680`` → ``5560160680``
    * ``5560160680``  → ``5560160680``
    * ``  556016-0680  `` (strips whitespace)

    Raises ``ValueError`` for values that don't match the expected 10-digit
    (with optional hyphen) format.
    """
    m = _ORG_NUMBER_RE.match(org_number.strip())
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # Already a clean 10-digit string?
    stripped = org_number.strip().replace("-", "")
    if len(stripped) == 10 and stripped.isdigit():
        return stripped
    raise ValueError(f"Unrecognised Swedish organisation number format: {org_number!r}")


def format_org_number(org_number: str) -> str:
    """Format a 10-digit org number as the canonical hyphenated display form.

    ``5560160680`` → ``556016-0680``
    """
    raw = normalise_org_number(org_number)
    return f"{raw[:6]}-{raw[6:]}"


class BolagsverketAdapter(SourceAdapter):
    """Source adapter for Bolagsverket — Swedish Companies Registration Office."""

    id = "bolagsverket"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        live = settings.allow_live and bool(settings.bolagsverket_api_key)
        return SourceInfo(
            id=self.id,
            name="Bolagsverket — Swedish Companies Registration Office",
            homepage="https://www.bolagsverket.se/",
            description=(
                "Swedish company data from Bolagsverket's open data API, "
                "including entity details and board-level officer information."
            ),
            license="SE-PSI",
            attribution=(
                "Contains data from Bolagsverket (Swedish Companies Registration "
                "Office), published as open PSI data."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed; reached via GLEIF org number, not by name.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search is not supported — returns an empty list.

        Swedish entities are reached via their organisation number derived
        from the GLEIF ``registeredAs`` field, not via free-text search.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the Bolagsverket record for a Swedish organisation number.

        ``hit_id`` may be a 10-digit raw number or the hyphenated form
        (``556016-0680``). It is normalised before the API call.

        Pass ``legal_name`` (from GLEIF) as a fallback when the API does
        not return the company name (shouldn't happen but guards against
        it gracefully).
        """
        try:
            org_number = normalise_org_number(hit_id)
        except ValueError:
            return {
                "source_id": self.id,
                "org_number": hit_id,
                "company": None,
                "legal_name": legal_name,
                "is_stub": True,
            }

        cache_key = f"{_CACHE_NS}/org/{org_number}"
        cached = self._cache.get_payload(cache_key)

        if cached is not None:
            data = cached[0]
        elif not self.info.live_available:
            return {
                "source_id": self.id,
                "org_number": org_number,
                "company": None,
                "legal_name": legal_name,
                "is_stub": True,
            }
        else:
            settings = get_settings()
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}/{format_org_number(org_number)}",
                    headers={"Authorization": f"Bearer {settings.bolagsverket_api_key}"},
                )
                response.raise_for_status()
                data = response.json()
            self._cache.put(cache_key, data)

        return {
            "source_id": self.id,
            "org_number": org_number,
            "company": data,
            "legal_name": legal_name,
            "is_stub": False,
        }
