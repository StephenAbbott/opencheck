"""INPI (Institut National de la Propriété Industrielle) adapter.

INPI operates the Registre National des Entreprises (RNE) — France's
national company register, incorporating data from SIRENE (INSEE) and the
Greffe network.  This adapter fetches entity data for French companies
whose SIREN number can be derived from a GLEIF record where
``registeredAt.id == "RA000189"`` (INSEE/SIRENE RA code) and
``registeredAs`` carries a 9-digit SIREN.

Live endpoints used:

* ``POST https://registre-national-entreprises.inpi.fr/api/sso/login``
  — exchange username + password for a Bearer token.
* ``GET  https://registre-national-entreprises.inpi.fr/api/companies/{siren}``
  — full company record for a SIREN.

Auth: The token is held in-process for the lifetime of the server; a 401
response triggers a single re-authentication before raising.

⚠️  French BO restriction
French law (Loi Sapin II / décret 2017-1094) restricts public redistribution
of beneficial-ownership data from the RNE.  This adapter therefore:

1. Returns ``is_stub=True`` when ``diffusionINSEE == "N"`` (non-diffusable
   company — no data may be redistributed).
2. Emits entity statements ONLY; person statements are never produced,
   regardless of what ``composition.pouvoirs`` carries.  Any entry in that
   array that has ``beneficiaireEffectif: true`` is a BO record and MUST
   NOT be republished without legitimate-interest authorisation.

Identifier scheme: ``FR-SIREN`` (follows GB-COH / CH-UID / NL-KVK pattern)
API documentation: https://registre-national-entreprises.inpi.fr/
GLEIF RA code: RA000189 (Register of Companies — Sirene)
"""

from __future__ import annotations

import asyncio
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_AUTH_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
_API_BASE = "https://registre-national-entreprises.inpi.fr/api"
_CACHE_NS = "inpi"

# GLEIF Registration Authority code for INSEE/SIRENE (France).
INPI_RA_CODE: str = "RA000189"

# In-process lock to prevent concurrent token-refresh races.
_TOKEN_LOCK = asyncio.Lock()


def normalise_siren(siren: str) -> str:
    """Normalise a SIREN number: strip whitespace, zero-pad to 9 digits."""
    return siren.strip().zfill(9)


class InpiAdapter(SourceAdapter):
    """Source adapter for INPI — French national company register (RNE)."""

    id = "inpi"

    def __init__(self) -> None:
        self._cache = Cache()
        self._token: str | None = None

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        # live_available requires both allow_live AND credentials
        live = (
            settings.allow_live
            and bool(settings.inpi_username)
            and bool(settings.inpi_password)
        )
        return SourceInfo(
            id=self.id,
            name="INPI — Registre National des Entreprises",
            homepage="https://registre-national-entreprises.inpi.fr/",
            description=(
                "French company data from the Registre National des Entreprises "
                "(RNE), operated by INPI, sourced via the SIREN number."
            ),
            license="Licence Ouverte / Open Licence 2.0",
            attribution=(
                "Contains data from the Registre National des Entreprises (RNE), "
                "INPI — Licence Ouverte / Open Licence 2.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=live,
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed, not name-searchable.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search is not supported — returns an empty list.

        INPI entities are reached via their SIREN number (from GLEIF
        ``registeredAs``), not by free-text name.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Return the RNE company record for a SIREN number.

        ``hit_id`` must be a 9-digit SIREN (string, with or without leading
        zeros).  Returns a stub bundle when live mode is disabled or when
        the company's ``diffusionINSEE`` field is ``"N"`` (non-diffusable).
        """
        siren = normalise_siren(hit_id)
        cache_key = f"{_CACHE_NS}/company/{siren}"

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            data = cached[0]
            return self._make_bundle(siren, data)

        if not self.info.live_available:
            return {
                "source_id": self.id,
                "siren": siren,
                "company": None,
                "is_stub": True,
            }

        data = await self._get_company(siren)
        self._cache.put(cache_key, data)
        return self._make_bundle(siren, data)

    def _make_bundle(self, siren: str, data: dict[str, Any]) -> dict[str, Any]:
        """Build the bundle dict from a raw RNE API response.

        Companies with ``diffusionINSEE == "N"`` are non-diffusable:
        their data must not be redistributed, so we return a stub.
        """
        if (data.get("diffusionINSEE") or "").upper() == "N":
            return {
                "source_id": self.id,
                "siren": siren,
                "company": None,
                "is_stub": True,
                "non_diffusable": True,
            }
        return {
            "source_id": self.id,
            "siren": siren,
            "company": data,
            "is_stub": False,
        }

    # ------------------------------------------------------------------
    # HTTP with token auth
    # ------------------------------------------------------------------

    async def _get_company(self, siren: str) -> dict[str, Any]:
        """GET /api/companies/{siren}, refreshing the Bearer token on 401."""
        token = await self._ensure_token()
        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}/companies/{siren}",
                headers={"Authorization": f"Bearer {token}"},
            )
            if response.status_code == 401:
                # Token expired — refresh once and retry.
                token = await self._refresh_token()
                response = await client.get(
                    f"{_API_BASE}/companies/{siren}",
                    headers={"Authorization": f"Bearer {token}"},
                )
            response.raise_for_status()
            return response.json()

    async def _ensure_token(self) -> str:
        """Return the cached token, obtaining one first if needed."""
        if self._token:
            return self._token
        return await self._refresh_token()

    async def _refresh_token(self) -> str:
        """Authenticate and store a fresh Bearer token in-process."""
        async with _TOKEN_LOCK:
            # Re-check after acquiring the lock — another coroutine may have
            # refreshed already while we were waiting.
            if self._token:
                return self._token

            settings = get_settings()
            async with build_client() as client:
                response = await client.post(
                    _AUTH_URL,
                    json={
                        "username": settings.inpi_username,
                        "password": settings.inpi_password,
                    },
                )
                response.raise_for_status()
                payload = response.json()

            token: str = payload["token"]
            self._token = token
            return token
