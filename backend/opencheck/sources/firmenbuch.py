"""Austrian Firmenbuch (commercial register) adapter.

The Firmenbuch is Austria's commercial register, operated by the Federal
Ministry of Justice (BMJ).  Since March 2025 it has been published as a
High Value Dataset under EU Implementing Regulation 2023/138, Annex 5,
licensed under CC BY 4.0 with daily updates.

API characteristics:
  - SOAP 1.2 over HTTPS (not REST/JSON)
  - X-API-KEY header authentication
  - WSDL: https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws/fbw.wsdl
  - Key registration: https://justizonline.gv.at/jop/web/iwg/register

Operations used by this adapter:
  * SUCHEFIRMAREQUEST  — name search → list of matches with FIRMA_ID
  * AUSZUGREQUEST      — full extract for a FIRMA_ID (entity, officers,
                         shareholders, address)
  * VERAENDERUNGENFIRMAREQUEST — change history (used for temporal metadata)

Data scope:
  - Entity details (name, FN number, legal form, address, founding date)
  - Officers: Geschäftsführer, Vorstand, Prokuristen (managing directors,
    board members, authorised signatories)
  - Shareholders (Gesellschafter) for GmbH, KG, OG with capital amounts
    (from which percentage can be computed)

Intentional exclusion:
  - WiEReG beneficial ownership data (restricted since ECJ 2022 ruling;
    requires Austrian/EU digital ID and legitimate interest registration)
  - AG share ownership (not in the commercial register; tracked via OEKB)

GLEIF Registration Authority code for Firmenbuch: RA000017
(observed on Austrian LEI records via the GLEIF API)

License: CC BY 4.0
Attribution: Contains data from the Austrian Firmenbuch via the BMJ HVD API
  (CC BY 4.0), © Bundesministerium für Justiz.
API documentation:
  https://justizonline.gv.at/jop/web/assets/iwg/WebService(HVD)_20250116.zip
Open-source reference implementation:
  https://github.com/Lukhers-dev/firmenbuch-HVD
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOAP_ENDPOINT = "https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws/fbw"
_WSDL_URL = "https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws/fbw.wsdl"
_CACHE_NS = "firmenbuch"

# GLEIF Registration Authority code for the Austrian Firmenbuch.
AT_FB_RA_CODE: str = "RA000017"

# Firmenbuchnummer format: digits followed by a letter (e.g. "473888w", "366715m").
_FN_RE = re.compile(r"^\d+[a-z]$", re.IGNORECASE)

# SOAP 1.2 namespace
_SOAP_NS = "http://www.w3.org/2003/05/soap-envelope"
_FBW_NS = "http://at.gv.justiz.fbw.webservice/"


def normalise_fn(fn: str) -> str:
    """Normalise a Firmenbuchnummer: strip whitespace, lowercase suffix letter.

    Examples:
        "473888 W" → "473888w"
        " FN 366715m " → "366715m"  (strips "FN " prefix if present)
    """
    cleaned = fn.strip()
    # Remove optional "FN " prefix (common in Austrian documents)
    if cleaned.upper().startswith("FN "):
        cleaned = cleaned[3:].strip()
    return cleaned.lower()


def is_valid_fn(fn: str) -> bool:
    """Return True if *fn* looks like a valid Firmenbuchnummer."""
    return bool(_FN_RE.match(normalise_fn(fn)))


def _company_url(fn: str) -> str:
    """Return the Firmenbuch public-facing URL for a Firmenbuchnummer."""
    return f"https://justizonline.gv.at/jop/web/firmenbuchabfrage?firmennummer={quote(fn)}"


# ---------------------------------------------------------------------------
# SOAP request builders
# ---------------------------------------------------------------------------

def _soap_envelope(body_xml: str) -> str:
    """Wrap *body_xml* in a SOAP 1.2 envelope."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<soap:Envelope xmlns:soap="{_SOAP_NS}" '
        f'xmlns:fbw="{_FBW_NS}">'
        "<soap:Body>"
        f"{body_xml}"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def _search_request_xml(query: str) -> str:
    """Build a SUCHEFIRMAREQUEST SOAP body for a name query."""
    esc = _xml_escape(query)
    return _soap_envelope(
        f"<fbw:SUCHEFIRMAREQUEST>"
        f"<fbw:SUCHBEGRIFF>{esc}</fbw:SUCHBEGRIFF>"
        f"<fbw:SUCHE_NACH>NAME</fbw:SUCHE_NACH>"
        f"<fbw:ERGEBNIS_ANZAHL>10</fbw:ERGEBNIS_ANZAHL>"
        f"</fbw:SUCHEFIRMAREQUEST>"
    )


def _extract_request_xml(firma_id: str, variante: str = "VOLLZUG") -> str:
    """Build an AUSZUGREQUEST SOAP body for a FIRMA_ID."""
    esc = _xml_escape(firma_id)
    return _soap_envelope(
        f"<fbw:AUSZUGREQUEST>"
        f"<fbw:FIRMA_ID>{esc}</fbw:FIRMA_ID>"
        f"<fbw:VARIANTE>{variante}</fbw:VARIANTE>"
        f"</fbw:AUSZUGREQUEST>"
    )


def _changes_request_xml(firma_id: str) -> str:
    """Build a VERAENDERUNGENFIRMAREQUEST SOAP body."""
    esc = _xml_escape(firma_id)
    return _soap_envelope(
        f"<fbw:VERAENDERUNGENFIRMAREQUEST>"
        f"<fbw:FIRMA_ID>{esc}</fbw:FIRMA_ID>"
        f"</fbw:VERAENDERUNGENFIRMAREQUEST>"
    )


def _xml_escape(s: str) -> str:
    """Escape XML special characters."""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&apos;")
    )


# ---------------------------------------------------------------------------
# SOAP response parsers
# ---------------------------------------------------------------------------

def _parse_search_response(xml_text: str) -> list[dict[str, Any]]:
    """Parse a SUCHEFIRMAREQUEST response into a list of hit dicts."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    hits: list[dict[str, Any]] = []
    # The response contains FIRMA elements with FIRMA_ID, FIRMENWORTLAUT, FN, STATUS
    for firma in root.iter("FIRMA"):
        firma_id = _text(firma, "FIRMA_ID")
        name = _text(firma, "FIRMENWORTLAUT")
        fn = _text(firma, "FN")
        status = _text(firma, "STATUS")
        rechtsform = _text(firma, "RECHTSFORM")
        if firma_id and name:
            hits.append({
                "firma_id": firma_id,
                "name": name,
                "fn": fn,
                "status": status,
                "rechtsform": rechtsform,
            })
    return hits


def _parse_extract_response(xml_text: str) -> dict[str, Any]:
    """Parse an AUSZUGREQUEST response into a structured dict.

    Returns a dict with keys:
      name, fn, uid, rechtsform, status, address, founding_date,
      officers (list), shareholders (list), stamm_kapital
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}

    # Top-level entity fields
    name = _text(root, ".//FIRMENWORTLAUT")
    fn = _text(root, ".//FN")
    uid = _text(root, ".//UID")
    rechtsform = _text(root, ".//RECHTSFORM")
    status = _text(root, ".//STATUS")
    founding_date = _text(root, ".//GRUENDUNGSDATUM") or _text(root, ".//EINTRAGEDATUM")

    # Address — prefer Geschäftsanschrift (business address)
    address = _parse_address(root)

    # Stammkapital (registered capital) — for GmbH, used to compute share %
    stamm_kapital_str = _text(root, ".//STAMMKAPITAL") or ""
    try:
        stamm_kapital = float(stamm_kapital_str.replace(",", ".")) if stamm_kapital_str else None
    except ValueError:
        stamm_kapital = None

    # Officers — FUN elements
    officers = _parse_officers(root)

    # Shareholders — GESELLSCHAFTER or KOMMANDITISTEN / KOMPLEMENTAERE elements
    shareholders = _parse_shareholders(root)

    return {
        "name": name,
        "fn": fn,
        "uid": uid,
        "rechtsform": rechtsform,
        "status": status,
        "address": address,
        "founding_date": founding_date,
        "stamm_kapital": stamm_kapital,
        "officers": officers,
        "shareholders": shareholders,
    }


def _parse_address(root: ET.Element) -> str:
    """Extract a formatted address string from the XML tree."""
    # Try GESCHAEFTSANSCHRIFT first, then SITZ
    for tag in ("GESCHAEFTSANSCHRIFT", "SITZ", "ANSCHRIFT"):
        addr_el = root.find(f".//{tag}")
        if addr_el is not None:
            parts = [
                _text(addr_el, "STRASSE"),
                _text(addr_el, "HAUSNUMMER"),
                _text(addr_el, "STIEGE"),
                _text(addr_el, "ORT"),
                _text(addr_el, "PLZ"),
            ]
            non_empty = [p for p in parts if p]
            if non_empty:
                return " ".join(non_empty)
    return ""


def _parse_officers(root: ET.Element) -> list[dict[str, Any]]:
    """Extract officer records (FUN = Funktion elements)."""
    officers: list[dict[str, Any]] = []
    for fun in root.iter("FUN"):
        role_code = _text(fun, "FUNKTION_CODE") or _text(fun, "FUNKTION")
        role_name = _text(fun, "FUNKTION_TEXT") or role_code or ""
        start_date = _text(fun, "EINTRITTSDATUM") or _text(fun, "EINTRAGUNGSDATUM")
        end_date = _text(fun, "AUSTRITTSDATUM") or _text(fun, "LOESCHDATUM")

        # Skip terminated roles
        if end_date:
            continue

        # Person details — may be under PERSON or directly on FUN
        person_el = fun.find("PERSON") or fun
        given_name = _text(person_el, "VORNAME") or ""
        family_name = _text(person_el, "NACHNAME") or _text(person_el, "NAME") or ""
        full_name = " ".join(filter(None, [given_name, family_name])).strip()
        dob = _text(person_el, "GEBURTSDATUM")

        if not full_name:
            continue

        officers.append({
            "full_name": full_name,
            "given_name": given_name,
            "family_name": family_name,
            "role_code": role_code or "",
            "role_name": role_name,
            "start_date": start_date,
            "dob": dob,
        })
    return officers


def _parse_shareholders(root: ET.Element) -> list[dict[str, Any]]:
    """Extract shareholder/partner records.

    Covers:
      - GESELLSCHAFTER (GmbH shareholders)
      - KOMMANDITISTEN (KG limited partners)
      - KOMPLEMENTAERE (KG general partners)
    """
    shareholders: list[dict[str, Any]] = []

    def _extract(tag: str, kind: str) -> None:
        for el in root.iter(tag):
            end_date = _text(el, "AUSTRITTSDATUM") or _text(el, "LOESCHDATUM")
            if end_date:
                continue

            person_el = el.find("PERSON") or el
            entity_el = el.find("GESELLSCHAFT") or el.find("FIRMA")

            # Determine if shareholder is a person or entity
            given_name = _text(person_el, "VORNAME") or ""
            family_name = _text(person_el, "NACHNAME") or _text(person_el, "NAME") or ""
            company_name = (
                _text(entity_el, "FIRMENWORTLAUT") or _text(el, "FIRMENWORTLAUT")
                if entity_el is not None else ""
            ) or ""

            full_name = " ".join(filter(None, [given_name, family_name])).strip()
            is_person = bool(full_name)
            display_name = full_name if is_person else company_name

            if not display_name:
                continue

            # Capital contribution → use to compute share %
            einlage_str = _text(el, "STAMMEINLAGE") or _text(el, "EINLAGE") or ""
            try:
                einlage = float(einlage_str.replace(",", ".")) if einlage_str else None
            except ValueError:
                einlage = None

            dob = _text(person_el, "GEBURTSDATUM") if is_person else None
            fn_shareholder = _text(el, "FN") or ""  # If the shareholder is a registered entity

            shareholders.append({
                "display_name": display_name,
                "is_person": is_person,
                "given_name": given_name,
                "family_name": family_name,
                "dob": dob,
                "einlage": einlage,
                "fn": fn_shareholder,
                "kind": kind,
            })

    _extract("GESELLSCHAFTER", "gesellschafter")
    _extract("KOMMANDITIST", "kommanditist")
    _extract("KOMPLEMENTAER", "komplementaer")
    return shareholders


def _text(el: ET.Element, path: str) -> str:
    """Return stripped text of the first matching element, or empty string."""
    found = el.find(path)
    if found is not None and found.text:
        return found.text.strip()
    return ""


# ---------------------------------------------------------------------------
# Role code → BODS interest type mapping
# ---------------------------------------------------------------------------

# Austrian Firmenbuch Funktion codes → BODS interest type + label
_AT_ROLE_MAP: dict[str, tuple[str, str]] = {
    "GF":   ("otherInfluenceOrControl", "Geschäftsführer (Managing Director)"),
    "GFI":  ("otherInfluenceOrControl", "Geschäftsführerin (Managing Director)"),
    "VW":   ("boardMember",             "Vorstandsmitglied (Board Member)"),
    "VWV":  ("boardMember",             "Vorstandsvorsitzender (Chair of Board)"),
    "VWI":  ("boardMember",             "Vorstandsmitglied (Board Member)"),
    "AR":   ("boardMember",             "Aufsichtsratsmitglied (Supervisory Board)"),
    "ARV":  ("boardMember",             "Aufsichtsratsvorsitzender (Supervisory Board Chair)"),
    "PK":   ("otherInfluenceOrControl", "Prokurist (Authorised Signatory)"),
    "PKI":  ("otherInfluenceOrControl", "Prokuristin (Authorised Signatory)"),
    "LI":   ("otherInfluenceOrControl", "Liquidator"),
    "LII":  ("otherInfluenceOrControl", "Liquidatorin"),
    "GES":  ("otherInfluenceOrControl", "Gesellschafter (Partner/Shareholder)"),
    "KOMPL": ("otherInfluenceOrControl", "Komplementär (General Partner)"),
    "KOMM": ("otherInfluenceOrControl", "Kommanditist (Limited Partner)"),
}


def _role_to_interest(role_code: str, role_name: str) -> tuple[str, str]:
    """Return (bods_interest_type, display_label) for an Austrian role code."""
    upper = role_code.upper()
    if upper in _AT_ROLE_MAP:
        return _AT_ROLE_MAP[upper]
    # Fallback: infer from role name
    name_lower = role_name.lower()
    if "geschäftsführ" in name_lower:
        return ("otherInfluenceOrControl", role_name)
    if "vorstand" in name_lower:
        return ("boardMember", role_name)
    if "aufsichtsrat" in name_lower:
        return ("boardMember", role_name)
    if "prokurist" in name_lower:
        return ("otherInfluenceOrControl", role_name)
    if "liquidator" in name_lower:
        return ("otherInfluenceOrControl", role_name)
    return ("otherInfluenceOrControl", role_name or role_code)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class FirmenbuchAdapter(SourceAdapter):
    """Source adapter for the Austrian Firmenbuch (commercial register).

    Uses the Firmenbuch HVD SOAP API (CC BY 4.0) to retrieve entity,
    officer, and shareholder data for Austrian companies.

    Requires FIRMENBUCH_API_KEY to be set for live mode.
    """

    id = "firmenbuch"

    def __init__(self) -> None:
        self._cache = Cache()
        # In-process name store: fn → legal_name (from GLEIF when API unavailable)
        self._names: dict[str, str] = {}

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Firmenbuch — Austrian Commercial Register",
            homepage="https://justizonline.gv.at/jop/web/firmenbuchabfrage",
            description=(
                "Austrian company data from the Firmenbuch (commercial register), "
                "including entity details, officers, and shareholders for GmbH, KG "
                "and OG entities, via the BMJ High Value Dataset API."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Austrian Firmenbuch via the BMJ HVD API "
                "(CC BY 4.0), © Bundesministerium für Justiz."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=settings.allow_live and bool(settings.firmenbuch_api_key),
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Search by company name.

        Firmenbuch also routes entity lookups via the GLEIF → RA000017 path
        (identifier-keyed), but name search is supported here for direct use.
        """
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{query.lower().strip()}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        xml_response = await self._soap_call(
            _search_request_xml(query),
            cache_key=cache_key,
            action="SUCHEFIRMAREQUEST",
        )
        if not xml_response:
            return []

        hits_raw = _parse_search_response(xml_response)
        return [self._entity_hit(h) for h in hits_raw if h.get("firma_id")]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any]:
        """Fetch full Firmenbuch record for a Firmenbuchnummer (FN).

        ``hit_id`` must be a Firmenbuchnummer string (e.g. "473888w").
        ``legal_name`` is an optional fallback name from GLEIF.
        """
        fn = normalise_fn(hit_id)
        if legal_name:
            self._names[fn] = legal_name

        cache_key = f"{_CACHE_NS}/extract/{fn}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {
                "source_id": self.id,
                "fn": fn,
                "extract": None,
                "legal_name": self._names.get(fn, legal_name),
                "is_stub": True,
            }

        # The SOAP API accepts the FN directly as the FIRMA_ID.
        xml_response = await self._soap_call(
            _extract_request_xml(fn),
            cache_key=cache_key,
            action="AUSZUGREQUEST",
        )
        if not xml_response:
            return {
                "source_id": self.id,
                "fn": fn,
                "extract": None,
                "legal_name": self._names.get(fn, legal_name),
                "is_stub": False,
            }

        extract = _parse_extract_response(xml_response)
        # If no name from the extract, fall back to GLEIF-provided name
        if not extract.get("name"):
            extract["name"] = self._names.get(fn, legal_name)

        return {
            "source_id": self.id,
            "fn": fn,
            "extract": extract,
            "legal_name": self._names.get(fn, legal_name),
            "is_stub": False,
        }

    # ------------------------------------------------------------------
    # HTTP — SOAP over httpx (no zeep dependency)
    # ------------------------------------------------------------------

    async def _soap_call(
        self,
        body: str,
        *,
        cache_key: str,
        action: str,
    ) -> str | None:
        """Send a SOAP 1.2 request and return the raw XML response string.

        Results are cached by *cache_key*. Returns None on error.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        settings = get_settings()
        api_key = settings.firmenbuch_api_key or ""

        try:
            async with build_client() as client:
                response = await client.post(
                    _SOAP_ENDPOINT,
                    content=body.encode("utf-8"),
                    headers={
                        "Content-Type": "application/soap+xml; charset=utf-8",
                        "X-API-KEY": api_key,
                        "SOAPAction": action,
                    },
                )
                response.raise_for_status()
                xml_text = response.text
        except Exception:
            import logging
            logging.getLogger(__name__).warning(
                "Firmenbuch SOAP call failed (action=%s, cache_key=%s)",
                action,
                cache_key,
                exc_info=True,
            )
            return None

        self._cache.put(cache_key, xml_text)
        return xml_text

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(item: dict[str, Any]) -> SourceHit:
        fn = item.get("fn") or item.get("firma_id") or ""
        name = item.get("name") or fn or "Unknown"
        rechtsform = item.get("rechtsform") or ""
        status = item.get("status") or ""

        summary_parts = [f"AT-FB {fn}"]
        if rechtsform:
            summary_parts.append(rechtsform)
        if status and status.lower() not in ("aktiv", "active"):
            summary_parts.append(status)

        return SourceHit(
            source_id="firmenbuch",
            hit_id=fn,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_parts),
            identifiers={"at_fn": fn},
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="473888w",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub Firmenbuch record — set OPENCHECK_ALLOW_LIVE=true "
                    "and FIRMENBUCH_API_KEY to query the live HVD API."
                ),
                identifiers={"at_fn": "473888w"},
                raw={"fn": "473888w", "name": f"{query} (stub)"},
            )
        ]
