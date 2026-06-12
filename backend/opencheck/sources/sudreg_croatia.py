"""Croatian Court Register (Sudski registar) adapter.

The Sudski registar is Croatia's central court-administered business
register, operated by the Ministry of Justice and Public Administration
(Ministarstvo pravosuđa i uprave). Open data is published via an OAuth2-
protected JSON API ("sudreg_javni" — the public service, v3).

Register data exposed by the public API includes:
* Entity identity: legal name (tvrtka), short name (skraćena tvrtka)
* Identifiers: MBS (matični broj subjekta — court register number) and
  OIB (osobni identifikacijski broj — the 11-digit personal/company id)
* Legal form (pravni oblik), status, founding date (datum osnivanja)
* Registered seat (sjedište) and share capital (temeljni kapital)

Note: officer/board-member and beneficial-ownership data are NOT exposed
by the public API (personal data was withdrawn from the open service), so
this adapter emits a single BODS entity statement — no person or
relationship statements. Croatian beneficial ownership is held in a
separate register (Registar stvarnih vlasnika) administered by FINA.

The flow with GLEIF:
  1. GLEIF returns ``registeredAt.id == "RA000156"`` (Croatian Court
     Registry RA code) and ``registeredAs = "<9-digit MBS>"`` (the
     zero-padded ``potpuni_mbs``, e.g. ``080000604``) for Croatian
     entities.
  2. lookup.py extracts ``derived["hr_mbs"]`` and calls ``fetch()`` here.
  3. We GET /detalji_subjekta?tip_identifikatora=mbs&identifikator=<mbs>
     and map the result to a BODS entity statement.

Authentication: OAuth2 Client Credentials Grant.
  Token endpoint: https://sudreg-data.gov.hr/api/oauth/token
  SUDREG_CLIENT_ID     → OAuth2 client_id
  SUDREG_CLIENT_SECRET → OAuth2 client_secret
Both values are issued via the user-registration application at
https://sudreg-data.gov.hr/ (note: the Client ID and Client Secret end
in literal dots ``..`` — these are an integral part of the credential).

GLEIF RA code: RA000156

MBS format: the court register number. GLEIF publishes the 9-digit
zero-padded form (``potpuni_mbs``). The API accepts both the padded and
unpadded numeric form for ``tip_identifikatora=mbs``.

API base: https://sudreg-data.gov.hr/api/javni
OpenAPI:  https://sudreg-data.gov.hr/api/javni/dokumentacija/open_api

License: Open data published by the Ministry of Justice and Public
Administration of the Republic of Croatia via data.gov.hr (Otvorena
dozvola / Open Licence). Attribution required.
"""

from __future__ import annotations

import re
import time
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.sudreg_croatia import SudregBundle

# Production API host (a separate ``-test`` host exists for sandbox use).
_API_BASE = "https://sudreg-data.gov.hr/api/javni"
_DETALJI_URL = f"{_API_BASE}/detalji_subjekta"
_TOKEN_URL = "https://sudreg-data.gov.hr/api/oauth/token"

_CACHE_NS = "sudreg_croatia"

# Public search portal — no per-entity deep link is published, so we use
# the register's search homepage as the source URL.
_PORTAL_URL = "https://sudreg.pravosudje.hr"

# GLEIF Registration Authority code for the Croatian Court Registry.
# All Croatian entities in GLEIF carry registeredAt.id == "RA000156".
SUDREG_RA_CODE: str = "RA000156"

# In-memory token cache: (access_token, expires_at_epoch_monotonic)
_token_cache: tuple[str, float] | None = None


def normalise_mbs(mbs: str) -> str:
    """Normalise a Croatian MBS (court register number) to its canonical
    9-digit zero-padded form.

    Accepts the raw numeric form (``80000604``) or the already-padded
    ``potpuni_mbs`` (``080000604``). Returns the digits zero-padded to a
    minimum width of 9, matching the GLEIF ``registeredAs`` representation.
    Returns an empty string when the input contains no digits.
    """
    digits = re.sub(r"[^0-9]", "", (mbs or "").strip())
    if not digits:
        return ""
    return digits.zfill(9)


async def _get_access_token(client_id: str, client_secret: str) -> str:
    """Obtain a Bearer token via OAuth2 Client Credentials Grant.

    The token is cached in-process until 60 seconds before expiry. The
    sudreg token TTL is 21600 seconds (6 hours).
    """
    global _token_cache
    now = time.monotonic()
    if _token_cache is not None:
        token, expires_at = _token_cache
        if now < expires_at:
            return token

    async with build_client() as client:
        # HTTP Basic auth (client_id:client_secret) + grant_type form body.
        response = await client.post(
            _TOKEN_URL,
            auth=(client_id, client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            content="grant_type=client_credentials",
        )
        response.raise_for_status()
        data = response.json()

    token = data["access_token"]
    expires_in = int(data.get("expires_in", 21600))
    _token_cache = (token, now + expires_in - 60)  # 60s safety margin
    return token


class SudregCroatiaAdapter(SourceAdapter):
    """Source adapter for the Croatian Court Register (Sudski registar)."""

    id = "sudreg_croatia"

    lookup_derivers = (
        LookupDeriver(frozenset({SUDREG_RA_CODE}), "hr_mbs", normalise_mbs),
    )
    lookup_pass_legal_name = True


    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        live = (
            settings.allow_live
            and bool(settings.sudreg_client_id)
            and bool(settings.sudreg_client_secret)
        )
        return SourceInfo(
            id=self.id,
            name="Sudski registar — Croatian Court Register",
            homepage="https://sudreg.pravosudje.hr",
            description=(
                "Croatian company data from the Sudski registar (Court "
                "Register) public API, including legal name, MBS and OIB "
                "identifiers, legal form, status, founding date, registered "
                "seat and share capital."
            ),
            license="HR-OpenData",
            attribution=(
                "Contains data from the Sudski registar (Court Register), "
                "Ministry of Justice and Public Administration of the "
                "Republic of Croatia, published as open data via data.gov.hr."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=live,
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed; reached via the GLEIF MBS, not by name.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search is not supported — returns an empty list.

        Croatian entities are reached via their MBS, derived from the GLEIF
        ``registeredAs`` field, not via free-text search.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the Sudski registar record for a Croatian MBS.

        ``hit_id`` is the court register number (MBS); it is normalised to
        the 9-digit zero-padded form before the API call. Pass
        ``legal_name`` (from GLEIF) as a fallback display name.
        """
        mbs = normalise_mbs(hit_id)

        stub_bundle: dict[str, Any] = {
            "source_id": self.id,
            "mbs": mbs or hit_id,
            "oib": "",
            "subject": None,
            "legal_name": legal_name,
            "is_stub": True,
        }

        if not mbs:
            return stub_bundle

        cache_key = f"{_CACHE_NS}/mbs/{mbs}"
        cached = self._cache.get_payload(cache_key)

        if cached is not None:
            data = cached[0]
        elif not self.info.live_available:
            return stub_bundle
        else:
            settings = get_settings()
            token = await _get_access_token(
                settings.sudreg_client_id,
                settings.sudreg_client_secret,
            )
            async with build_client() as client:
                response = await client.get(
                    _DETALJI_URL,
                    params={
                        "tip_identifikatora": "mbs",
                        "identifikator": mbs,
                        "expand_relations": "true",
                    },
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                )
            # Not-found / invalid identifiers return HTTP 400 with an
            # ``error_code`` body rather than a 404. Treat any non-2xx or
            # error-shaped payload as "no record" → stub.
            if response.status_code != 200:
                return stub_bundle
            data = response.json()
            if not isinstance(data, dict) or "error_code" in data or not data:
                return stub_bundle
            self._cache.put(cache_key, data)

        if not isinstance(data, dict) or not data or "error_code" in data:
            return stub_bundle

        oib_raw = data.get("oib")
        bundle = {
            "source_id": self.id,
            "mbs": mbs,
            "oib": str(oib_raw) if oib_raw is not None else "",
            "subject": data,
            "legal_name": legal_name,
            "is_stub": False,
        }
        validate_raw("sudreg_croatia", SudregBundle, bundle)
        return bundle
