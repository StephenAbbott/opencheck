"""Estonian e-Business Register (e-Äriregister) adapter — live SOAP API.

The Centre of Registers and Information Systems (RIK) provides a SOAP/XML
API for querying the e-Business Register.  Access requires a free contract
with RIK (see https://www.rik.ee/en/e-business-register/api).

Operations used
  arireg.detailandmed_v2     — full company record including persons on card
  arireg.tegelikudKasusaajad_v2 — declared beneficial owners

JSON output is requested via ``ariregister_valjundi_formaat=json`` so no
XML parsing is needed for the payload; only a small regex is used to extract
the JSON string from the SOAP envelope.

Authentication: HTTP Basic credentials issued under the free contract.
  Set ``ARIREGISTER_USERNAME`` and ``ARIREGISTER_PASSWORD`` in ``.env``.

Rate limits (free tier): 50,000 requests/day, 1 simultaneous request.

GLEIF RA code: RA000181

The flow with GLEIF:
  1. GLEIF returns ``registeredAt.id == "RA000181"`` for Estonian entities
     and ``registeredAs = "<registry_code>"``.
  2. lookup.py extracts ``derived["ee_registry_code"]`` and calls
     ``fetch()`` here with the 8-digit registry code.
  3. We call ``detailandmed_v2`` for entity + person data, then
     ``tegelikudKasusaajad_v2`` for beneficial owners.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Estonian e-Business Register.
EE_RA_CODE: str = "RA000181"

# SOAP endpoint.
_SOAP_URL = "https://ariregxmlv6.rik.ee/"

# Register URL template for a company page.
_COMPANY_URL = "https://ariregister.rik.ee/eng/company/{registry_code}"

_TIMEOUT = 30.0

# Role codes that indicate a shareholder (vs. an officer/director).
_SHAREHOLDER_ROLES: frozenset[str] = frozenset({"O", "OSAN", "HUL", "HUL_UL"})

# Regex to extract the JSON payload from inside the SOAP <keha> element.
# The response wraps the JSON value in <keha>…</keha> (possibly namespace-prefixed).
_KEHA_RE = re.compile(
    r"<(?:[^:>]+:)?keha\b[^>]*>(.*?)</(?:[^:>]+:)?keha>",
    re.DOTALL,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _soap_envelope(query_name: str, params: dict[str, str]) -> str:
    """Build a minimal SOAP 1.1 envelope for an Ariregister operation."""
    inner = "\n".join(
        f"         <{k}>{v}</{k}>" for k, v in params.items()
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"'
        ' xmlns:nik="http://arireg.x-road.eu/producer/">\n'
        "   <soapenv:Header/>\n"
        "   <soapenv:Body>\n"
        f"      <nik:{query_name}>\n"
        "         <keha>\n"
        f"{inner}\n"
        "         </keha>\n"
        f"      </nik:{query_name}>\n"
        "   </soapenv:Body>\n"
        "</soapenv:Envelope>"
    )


def _extract_json_from_soap(text: str) -> Any:
    """Extract and parse the JSON payload from a SOAP response body."""
    m = _KEHA_RE.search(text)
    if not m:
        raise ValueError("No <keha> element found in SOAP response")
    raw = m.group(1).strip()
    return json.loads(raw)


def _norm_date(raw: str | None) -> str | None:
    """Normalise an API date value to YYYY-MM-DD.

    The live API returns dates as ``YYYY-MM-DDZ`` or
    ``YYYY-MM-DDTHH:MM:SS.sssZ``.  Strip the time/zone component so the
    BODS mapper receives a clean ISO date string.

    Also accepts the bulk-data ``DD.MM.YYYY`` format so this function can
    be used for any field regardless of source.
    """
    if not raw:
        return None
    s = str(raw).strip()
    # YYYY-MM-DD[T…] or YYYY-MM-DDZ
    if len(s) >= 10 and s[4] == "-":
        return s[:10].rstrip("Z")
    # DD.MM.YYYY
    parts = s.split(".")
    if len(parts) == 3:
        d, m, y = parts
        if len(y) == 4 and d.isdigit() and m.isdigit():
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None


def _as_list(val: Any) -> list:
    """Ensure a value that may be a dict (single item) or list is a list."""
    if val is None:
        return []
    if isinstance(val, dict):
        return [val]
    if isinstance(val, list):
        return val
    return []


def _parse_persons(company: dict[str, Any]) -> tuple[list[dict], list[dict]]:
    """Split kaardile_kantud_isikud into (shareholders, officers).

    Returns two lists.  Shareholder roles are O, OSAN, HUL, HUL_UL;
    everything else is treated as an officer.
    """
    persons_raw = company.get("kaardile_kantud_isikud") or {}
    # The field is an object with an "isik" key (single or list).
    persons = _as_list(persons_raw.get("isik") if isinstance(persons_raw, dict) else persons_raw)

    shareholders: list[dict] = []
    officers: list[dict] = []

    for person in persons:
        if not isinstance(person, dict):
            continue
        role = (person.get("isiku_roll") or "").strip().upper()
        # Normalise dates in-place so the mapper sees YYYY-MM-DD.
        for date_field in ("algus_kpv", "lopp_kpv", "synniaeg"):
            person[date_field] = _norm_date(person.get(date_field))
        if role in _SHAREHOLDER_ROLES:
            shareholders.append(person)
        else:
            officers.append(person)

    return shareholders, officers


def _parse_beneficial_owners(bo_data: Any) -> list[dict]:
    """Extract the kasusaaja list from a tegelikudKasusaajad_v2 response."""
    if not isinstance(bo_data, dict):
        return []
    kasusaajad = bo_data.get("kasusaajad") or {}
    items = _as_list(
        kasusaajad.get("kasusaaja") if isinstance(kasusaajad, dict) else kasusaajad
    )
    result = []
    for bo in items:
        if not isinstance(bo, dict):
            continue
        for date_field in ("algus_kpv", "lopp_kpv", "synniaeg"):
            bo[date_field] = _norm_date(bo.get(date_field))
        result.append(bo)
    return result


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class AriregisterAdapter(SourceAdapter):
    """Source adapter for the Estonian e-Business Register (e-Äriregister)."""

    id = "ariregister"

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        live = bool(settings.ariregister_username and settings.ariregister_password)
        return SourceInfo(
            id=self.id,
            name="Estonian e-Business Register (e-Äriregister)",
            homepage="https://ariregister.rik.ee/eng",
            description=(
                "Estonian company data including entity details, shareholders "
                "(with ownership percentages), board members, and beneficial "
                "owners, from the e-Business Register live API (RIK)."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Data from the Estonian e-Business Register (e-Äriregister), "
                "published by the Centre of Registers and Information Systems "
                "(RIK), CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=live,
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _credentials(self) -> tuple[str, str] | None:
        s = get_settings()
        u = s.ariregister_username
        p = s.ariregister_password
        if u and p:
            return (u, p)
        return None

    async def _post_soap(
        self,
        client: httpx.AsyncClient,
        query_name: str,
        params: dict[str, str],
    ) -> Any:
        """Post a SOAP request and return the parsed JSON payload."""
        body = _soap_envelope(query_name, params)
        resp = await client.post(
            _SOAP_URL,
            content=body.encode(),
            headers={
                "Content-Type": "text/xml; charset=UTF-8",
                "SOAPAction": f'"{query_name}"',
            },
        )
        resp.raise_for_status()
        return _extract_json_from_soap(resp.text)

    async def _fetch_company_data(
        self,
        client: httpx.AsyncClient,
        registry_code: str,
        username: str,
        password: str,
    ) -> dict[str, Any] | None:
        """Call detailandmed_v2 and return the first ettevotja dict."""
        params = {
            "Isikukood_Registrikood": registry_code,
            "Keel": "eng",
            "Yandmed": "1",
            "Iandmed": "1",
            "ariregister_kasutajanimi": username,
            "ariregister_parool": password,
            "ariregister_valjundi_formaat": "json",
        }
        try:
            data = await self._post_soap(client, "arireg.detailandmed_v2", params)
        except Exception as exc:
            logger.error("ariregister: detailandmed_v2 error for %s: %s", registry_code, exc)
            return None

        ettevotjad = _as_list(data.get("ettevotjad", {}).get("ettevotja") if isinstance(data, dict) else None)
        if not ettevotjad:
            logger.info("ariregister: no company found for %s", registry_code)
            return None
        return ettevotjad[0] if isinstance(ettevotjad[0], dict) else None

    async def _fetch_bo_data(
        self,
        client: httpx.AsyncClient,
        registry_code: str,
        username: str,
        password: str,
    ) -> list[dict]:
        """Call tegelikudKasusaajad_v2 and return the kasusaaja list."""
        params = {
            "Isikukood_Registrikood": registry_code,
            "Ainult_kehtivad": "1",
            "ariregister_kasutajanimi": username,
            "ariregister_parool": password,
            "ariregister_valjundi_formaat": "json",
        }
        try:
            data = await self._post_soap(
                client, "arireg.tegelikudKasusaajad_v2", params
            )
        except Exception as exc:
            logger.warning(
                "ariregister: tegelikudKasusaajad_v2 error for %s: %s",
                registry_code, exc,
            )
            return []
        return _parse_beneficial_owners(data)

    # ------------------------------------------------------------------
    # Search — identifier-keyed via GLEIF, not name-based.
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name-based search is not supported — returns an empty list.

        Estonian entities are reached via their registry code derived from
        the GLEIF ``registeredAs`` field, not via free-text search.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any]:
        """Return the e-Äriregister data for an Estonian registry code.

        ``hit_id`` is the 8-digit Estonian registry code (e.g. ``"14064835"``).
        ``legal_name`` is used as a fallback display name when the API has no
        record for this code or credentials are not yet configured.
        """
        # Normalise to canonical 8-digit zero-padded form.
        code = hit_id.strip().lstrip("0") or hit_id.strip()
        if code.isdigit():
            code = code.zfill(8)
        registry_code = code

        stub_bundle: dict[str, Any] = {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": legal_name,
            "legal_form": None,
            "vat_number": None,
            "status": None,
            "registration_date": None,
            "address": None,
            "link": _COMPANY_URL.format(registry_code=registry_code),
            "shareholders": [],
            "officers": [],
            "beneficial_owners": [],
            "is_stub": True,
        }

        creds = self._credentials()
        if creds is None:
            logger.info(
                "ariregister: no credentials configured — returning stub for %s",
                registry_code,
            )
            return stub_bundle

        username, password = creds

        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            company = await self._fetch_company_data(
                client, registry_code, username, password
            )
            if company is None:
                return stub_bundle

            shareholders, officers = _parse_persons(company)
            beneficial_owners = await self._fetch_bo_data(
                client, registry_code, username, password
            )

        # Build the address string from component fields.
        aadress = company.get("aadress") or {}
        if isinstance(aadress, dict):
            parts = [
                aadress.get("taisaadress") or "",
                aadress.get("postiindeks") or "",
                aadress.get("ehak_nimetus") or "",
                aadress.get("asukohamaa") or "",
            ]
            address = ", ".join(p for p in parts if p)
        else:
            address = str(aadress) if aadress else None

        return {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": (company.get("arinimi") or legal_name).strip(),
            "legal_form": company.get("oiguslik_vorm") or None,
            "vat_number": company.get("kmkr_nr") or None,
            "status": company.get("staatus") or None,
            "registration_date": _norm_date(company.get("esmaregistreerimise_kpv")),
            "address": address or None,
            "link": _COMPANY_URL.format(registry_code=registry_code),
            "shareholders": shareholders,
            "officers": officers,
            "beneficial_owners": beneficial_owners,
            "is_stub": False,
        }
