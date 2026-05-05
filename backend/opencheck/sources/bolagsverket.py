"""Bolagsverket (Swedish Companies Registration Office) adapter.

Bolagsverket operates the Swedish Handelsregistret (Companies Register),
Föreningsregistret (Cooperative Register), and other registers. This
adapter uses the "API for valuable datasets" (Öppna data / PSI-data /
värdefulla datamängder), which is free to use but requires OAuth2
client credentials issued via the developer portal.

The register data includes:
* Entity information: company name, organisation number, legal form
* Registered address
* Company status (active / deregistered) and deregistration reason

Note: Officer/board member data (foretradare) is NOT returned by the
/organisationer endpoint. This endpoint covers the EU high-value company
dataset (värdefulla datamängder) only.

The flow with GLEIF:
  1. GLEIF returns ``registeredAt.id == "RA000544"`` (Bolagsverket RA code)
     and ``registeredAs = "<10-digit org number>"`` for Swedish entities.
  2. app.py extracts ``derived["se_org_number"]`` and calls ``fetch()``
     here.
  3. We POST the org number to Bolagsverket's /organisationer endpoint
     and map the result to a BODS entity statement.

Authentication: OAuth2 Client Credentials Grant.
  Token endpoint: https://portal.api.bolagsverket.se/oauth2/token
  BOLAGSVERKET_API_KEY       → OAuth2 client_id  (Consumer Key)
  BOLAGSVERKET_CLIENT_SECRET → OAuth2 client_secret (Consumer Secret)

Both values are issued via the Bolagsverket developer portal:
  https://portal.api.bolagsverket.se/

GLEIF RA code: RA000544

Organisation number format: 10-digit number, canonically displayed as
``NNNNNN-NNNN`` (e.g. ``556016-0680`` for Telefonaktiebolaget LM Ericsson).
The API accepts the raw 10-digit form in the POST body.

API endpoint: POST /vardefulla-datamangder/v1/organisationer
  Request body: {"identitetsbeteckning": "<10-digit-org-number>"}
  Required OAuth2 scope: vardefulla-datamangder:read
Gateway host: gw.api.bolagsverket.se

License: PSI-compliant open data. Bolagsverket publishes this data under
Swedish public sector information legislation (PSI-lagen), consistent with
the EU Open Data Directive. Attribution required.
"""

from __future__ import annotations

import base64
import json
import re
import time
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

# Gateway and token endpoints — production environment (confirmed).
# Token endpoint confirmed: OAuth2 Client Credentials Grant returns a
# valid JWT with scope="default". The gateway returns 403 scope error
# until Bolagsverket grants the application access to /organisationer
# in the WSO2 developer portal (subscription scope configuration).
_GATEWAY_BASE = "https://gw.api.bolagsverket.se/vardefulla-datamangder/v1"
_ORGANISATIONER_URL = f"{_GATEWAY_BASE}/organisationer"
_TOKEN_URL = "https://portal.api.bolagsverket.se/oauth2/token"

_CACHE_NS = "bolagsverket"

# GLEIF Registration Authority code for Bolagsverket.
# All Swedish entities in GLEIF carry registeredAt.id == "RA000544".
BV_RA_CODE: str = "RA000544"

# Swedish organisation number: 10 digits, optionally with a hyphen after
# the sixth digit (e.g. ``556016-0680`` or ``5560160680``).
_ORG_NUMBER_RE = re.compile(r"^(\d{6})-?(\d{4})$")

# In-memory token cache: (access_token, expires_at_epoch)
_token_cache: tuple[str, float] | None = None


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


async def _get_access_token(client_id: str, client_secret: str) -> str:
    """Obtain a Bearer token via OAuth2 Client Credentials Grant.

    The token is cached in-process until 60 seconds before expiry.
    WSO2 tokens typically have a 3600-second TTL.
    """
    global _token_cache
    now = time.monotonic()
    if _token_cache is not None:
        token, expires_at = _token_cache
        if now < expires_at:
            return token

    credentials = base64.b64encode(
        f"{client_id}:{client_secret}".encode()
    ).decode()

    async with build_client() as client:
        response = await client.post(
            _TOKEN_URL,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            content="grant_type=client_credentials&scope=vardefulla-datamangder%3Aread",
        )
        response.raise_for_status()
        data = response.json()

    token = data["access_token"]
    expires_in = int(data.get("expires_in", 3600))
    _token_cache = (token, now + expires_in - 60)  # 60s safety margin
    return token


class BolagsverketAdapter(SourceAdapter):
    """Source adapter for Bolagsverket — Swedish Companies Registration Office."""

    id = "bolagsverket"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        live = (
            settings.allow_live
            and bool(settings.bolagsverket_api_key)
            and bool(settings.bolagsverket_client_secret)
        )
        return SourceInfo(
            id=self.id,
            name="Bolagsverket — Swedish Companies Registration Office",
            homepage="https://www.bolagsverket.se/",
            description=(
                "Swedish company data from Bolagsverket's open data API "
                "(värdefulla datamängder), including entity details and "
                "registered address."
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
        not return the company name.
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
            token = await _get_access_token(
                settings.bolagsverket_api_key,
                settings.bolagsverket_client_secret,
            )
            async with build_client() as client:
                # POST /organisationer with a JSON object containing the
                # organisation number as identitetsbeteckning.
                # Response: {"organisationer": [{...}, ...]}
                response = await client.post(
                    _ORGANISATIONER_URL,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    content=json.dumps({"identitetsbeteckning": org_number}),
                )
                response.raise_for_status()
                raw = response.json()

            # We posted a single org number so take the first element.
            organisationer = raw.get("organisationer") or []
            data = organisationer[0] if organisationer else {}
            self._cache.put(cache_key, data)

        return {
            "source_id": self.id,
            "org_number": org_number,
            "company": data,
            "legal_name": legal_name,
            "is_stub": False,
        }
