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
  * SUCHEFIRMAREQUEST   — name search → list of matches
  * AUSZUG_V2_REQUEST   — entity extract (UMFANG=Kurzinformation)
  * VERAENDERUNGENFIRMAREQUEST — change history (future use)

Data scope (HVD free tier — UMFANG=Kurzinformation):
  - Company name (FI_DKZ02 / BEZEICHNUNG)
  - Business address (FI_DKZ03, STELLE free-text form)
  - Entity status (AUFRECHT attribute on FI_DKZ02)
  - FN number (FNR attribute on the response element)
  - Officers: managing directors, authorised signatories, supervisory board
    members, liquidators — via top-level FUN/PER elements in the response
    (confirmed by the Firmenbuch team, May 2026)

NOT available on the free HVD API key:
  - Shareholders (Gesellschafter, Komplementäre, Kommanditisten)
  - Registered capital / Stammkapital
  - Founding date
  These require UMFANG=aktueller Auszug / historischer Auszug, which needs
  a paid Justiz Online subscription.

Officer data structure in Kurzinformation responses:
  - FUN elements (siblings of FIRMA at AUSZUG_V2_RESPONSE level):
      FKEN attribute — role code (GF, PK, AR, VW, LI, …)
      FKENTEXT attribute — role label ("GESCHÄFTSFÜHRER/IN (handelsrechtlich)")
      PNR attribute — person reference key
      FU_DKZ10 child — carries AUFRECHT ("true"/"false") and VNR
  - PER elements (also siblings of FIRMA):
      PNR attribute — matches FUN.PNR
      PE_DKZ02 child — person data: VORNAME, NACHNAME, GEBURTSDATUM, …

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

import datetime
import logging
import re
import httpx
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOAP_ENDPOINT = "https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws"
_CACHE_NS = "firmenbuch"

# GLEIF Registration Authority code for the Austrian Firmenbuch.
AT_FB_RA_CODE: str = "RA000017"

# Firmenbuchnummer format: digits followed by a letter (e.g. "473888w", "366715m").
_FN_RE = re.compile(r"^\d+[a-z]$", re.IGNORECASE)

# SOAP 1.2 envelope namespace + per-operation namespaces (from Postman reference collection).
_SOAP_NS = "http://www.w3.org/2003/05/soap-envelope"
_NS_SUCHE_FIRMA = "ns://firmenbuch.justiz.gv.at/Abfrage/SucheFirmaRequest"
_NS_AUSZUG = "ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugRequest"
_NS_VERAEND_FIRMA = "ns://firmenbuch.justiz.gv.at/Abfrage/VeraenderungenFirmaRequest"


def normalise_fn(fn: str) -> str:
    """Normalise a Firmenbuchnummer: strip whitespace, lowercase suffix letter.

    The Firmenbuch API returns FNR with a space before the suffix letter
    (e.g. ``"229831 m"``).  We collapse that to the canonical form without
    space (``"229831m"``).

    Examples:
        "473888 W"  → "473888w"
        "229831 m"  → "229831m"
        " FN 366715m " → "366715m"  (strips "FN " prefix if present)
    """
    cleaned = fn.strip()
    # Remove optional "FN " prefix (common in Austrian documents)
    if cleaned.upper().startswith("FN "):
        cleaned = cleaned[3:].strip()
    # Collapse space between digits and trailing letter (API format: "12345 a")
    cleaned = re.sub(r"(\d+)\s+([a-zA-Z])$", r"\1\2", cleaned)
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

def _soap_envelope(ns_prefix: str, ns_uri: str, body_xml: str) -> str:
    """Wrap *body_xml* in a SOAP 1.2 envelope with a per-operation namespace.

    The Firmenbuch API requires a ``<soap:Header/>`` element and a blank
    ``SOAPAction`` header (``""``).  Each operation has its own namespace URI.
    """
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<soap:Envelope xmlns:soap="{_SOAP_NS}" '
        f'xmlns:{ns_prefix}="{ns_uri}">'
        "<soap:Header/>"
        "<soap:Body>"
        f"{body_xml}"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def _search_request_xml(query: str) -> str:
    """Build a SUCHEFIRMAREQUEST SOAP body for a name search.

    Uses the correct namespace and field structure from the reference Postman
    collection (Lukhers-dev/firmenbuch-HVD).
    """
    esc = _xml_escape(query)
    return _soap_envelope(
        "suc",
        _NS_SUCHE_FIRMA,
        f"<suc:SUCHEFIRMAREQUEST>"
        f"<suc:FIRMENWORTLAUT>{esc}</suc:FIRMENWORTLAUT>"
        f"<suc:EXAKTESUCHE>false</suc:EXAKTESUCHE>"
        f"<suc:SUCHBEREICH>1</suc:SUCHBEREICH>"
        f"<suc:GERICHT></suc:GERICHT>"
        f"<suc:RECHTSFORM></suc:RECHTSFORM>"
        f"<suc:RECHTSEIGENSCHAFT></suc:RECHTSEIGENSCHAFT>"
        f"<suc:ORTNR></suc:ORTNR>"
        f"</suc:SUCHEFIRMAREQUEST>",
    )


def _extract_request_xml(fn: str) -> str:
    """Build an AUSZUG_V2_REQUEST SOAP body for a Firmenbuchnummer.

    ``fn`` is passed directly as ``<aus:FNR>`` — the v2 operation accepts the
    FN (e.g. "659195f") directly, no internal FIRMA_ID resolution needed.

    ``UMFANG=Kurzinformation`` is the only value supported by the free HVD
    API key.  It returns name (FI_DKZ02) and business address (FI_DKZ03) only.
    The richer modes (``Auszug``, ``Vollzug``, ``Vollinhalt``) cause HTTP 500
    on the free tier — they require a paid Justiz Online subscription.

    ``STICHTAG`` (today's date in ISO format YYYY-MM-DD) is required by the
    API; omitting it causes HTTP 400.
    """
    esc = _xml_escape(fn)
    today = datetime.date.today().isoformat()  # YYYY-MM-DD, required by the API
    return _soap_envelope(
        "aus",
        _NS_AUSZUG,
        f"<aus:AUSZUG_V2_REQUEST>"
        f"<aus:FNR>{esc}</aus:FNR>"
        f"<aus:STICHTAG>{today}</aus:STICHTAG>"
        f"<aus:UMFANG>Kurzinformation</aus:UMFANG>"
        f"</aus:AUSZUG_V2_REQUEST>",
    )


def _changes_request_xml(von: str, bis: str) -> str:
    """Build a VERAENDERUNGENFIRMAREQUEST SOAP body for a date range."""
    return _soap_envelope(
        "ver",
        _NS_VERAEND_FIRMA,
        f"<ver:VERAENDERUNGENFIRMAREQUEST>"
        f"<ver:VON>{_xml_escape(von)}</ver:VON>"
        f"<ver:BIS>{_xml_escape(bis)}</ver:BIS>"
        f"</ver:VERAENDERUNGENFIRMAREQUEST>",
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

def _strip_namespaces(xml_text: str) -> str:
    """Remove XML namespace prefixes and declarations so ElementTree can use
    simple local-name paths.

    The Firmenbuch API response prefixes every element and attribute with a
    namespace (e.g. ``ns6:FIRMA``, ``ns6:FNR="..."``) and embeds 19 namespace
    declarations on the root element.  Stripping them lets the rest of the
    parser use plain ``find``/``iter`` calls.
    """
    import re as _re
    # Remove xmlns:prefix="uri" declarations
    text = _re.sub(r'\s+xmlns(?::\w+)?="[^"]*"', "", xml_text)
    # Remove namespace prefix from element names: <ns6:FOO → <FOO, </ns6:FOO → </FOO
    text = _re.sub(r"<(/?)\w+:(\w)", r"<\1\2", text)
    # Remove namespace prefix from attribute names: ns6:FOO= → FOO=
    text = _re.sub(r"\s\w+:(\w+=)", r" \1", text)
    return text


def _parse_search_response(xml_text: str) -> list[dict[str, Any]]:
    """Parse a SUCHEFIRMAREQUEST response into a list of hit dicts.

    The real search response wraps results in ERGEBNIS elements (confirmed
    from the official HVD interface description v1.3, page 11), each with:
      FNR           child element (not attribute) — Firmenbuchnummer
      NAME          child element — company name
      STATUS        child element — may be empty
      SITZ          child element — registered seat / city
      RECHTSFORM/TEXT child subtree — legal form description
    """
    try:
        root = ET.fromstring(_strip_namespaces(xml_text))
    except ET.ParseError:
        return []

    hits: list[dict[str, Any]] = []
    for ergebnis in root.iter("ERGEBNIS"):
        fn = _text(ergebnis, "FNR").strip()
        name = _text(ergebnis, "NAME").strip()
        rechtsform_el = ergebnis.find("RECHTSFORM")
        rechtsform = _text(rechtsform_el, "TEXT") if rechtsform_el is not None else ""
        status = _text(ergebnis, "STATUS")
        sitz = _text(ergebnis, "SITZ")
        if fn or name:
            hits.append({
                "fn": normalise_fn(fn) if fn else "",
                "name": name,
                "rechtsform": rechtsform,
                "status": status,
                "sitz": sitz,
            })
    return hits


def _parse_extract_response(xml_text: str) -> dict[str, Any]:
    """Parse an AUSZUG_V2_RESPONSE into a structured dict.

    The response uses DKZ (Datenkennzeichen) codes to identify sections:
      FI_DKZ02 — company name (one or more BEZEICHNUNG children, join them)
      FI_DKZ03 — business address (STRASSE, HAUSNUMMER, PLZ, ORT)
      FI_DKZ06 — registered capital / Stammkapital (BETRAG attribute or child)
      FI_DKZ07 — GmbH shareholders / Gesellschafter
      FI_DKZ08 — managing directors / Geschäftsführer
      FI_DKZ09 — authorised signatories / Prokuristen
      FI_DKZ10 — supervisory board / Aufsichtsrat
      FI_DKZ12 — general partners / Komplementäre (KG)
      FI_DKZ13 — limited partners / Kommanditisten (KG)
      FI_DKZ14 — board members / Vorstand (AG/SE)
      FI_DKZ16 — liquidators / Liquidatoren

    The FN and status are read from attributes on the AUSZUG_V2_RESPONSE
    element itself: FNR="229831 m", and from FI_DKZ02 AUFRECHT="true".
    """
    try:
        root = ET.fromstring(_strip_namespaces(xml_text))
    except ET.ParseError:
        return {}

    # The response element sits inside Envelope/Body
    resp_el = root.find(".//AUSZUG_V2_RESPONSE") or root
    fn_raw = (resp_el.get("FNR") or "").strip()
    fn = normalise_fn(fn_raw) if fn_raw else ""

    firma_el = resp_el.find(".//FIRMA") or resp_el

    # ── Name ──────────────────────────────────────────────────────────────
    # FI_DKZ02: one entry per active name variant; join BEZEICHNUNG children.
    name_parts: list[str] = []
    for dkz02 in firma_el.iter("FI_DKZ02"):
        if dkz02.get("AUFRECHT", "true").lower() == "false":
            continue
        parts = [b.text.strip() for b in dkz02.iter("BEZEICHNUNG") if b.text]
        if parts:
            name_parts = parts  # take the last / most recent active block
    name = " ".join(name_parts).strip()

    # Status from the first FI_DKZ02 AUFRECHT attribute
    first_dkz02 = firma_el.find(".//FI_DKZ02")
    is_active = (first_dkz02 is not None and
                 first_dkz02.get("AUFRECHT", "true").lower() != "false")
    status = "aktiv" if is_active else "gelöscht"

    # ── Address (FI_DKZ03) ────────────────────────────────────────────────
    address = _parse_address(firma_el)

    # ── Registered capital (FI_DKZ06) ─────────────────────────────────────
    stamm_kapital: float | None = None
    dkz06 = firma_el.find(".//FI_DKZ06")
    if dkz06 is not None:
        raw_cap = (dkz06.get("BETRAG") or _text(dkz06, "BETRAG") or
                   _text(dkz06, "STAMMKAPITAL") or "").replace(",", ".")
        try:
            stamm_kapital = float(raw_cap) if raw_cap else None
        except ValueError:
            pass

    # ── Officers ───────────────────────────────────────────────────────────
    # Kurzinformation responses carry officer data in top-level FUN/PER
    # elements (siblings of FIRMA).  Fall back to the FI_DKZ structure inside
    # FIRMA if FUN/PER yields nothing (e.g. a paid-tier Auszug response).
    officers = _parse_fun_per_officers(resp_el)
    if not officers:
        officers = _parse_officers(firma_el)

    # ── Shareholders ───────────────────────────────────────────────────────
    shareholders = _parse_shareholders(firma_el, stamm_kapital)

    return {
        "name": name,
        "fn": fn,
        "uid": _text(firma_el, ".//UID"),
        "rechtsform": "",  # encoded in name suffix for Austrian entities
        "status": status,
        "address": address,
        "founding_date": None,  # not in Auszug response; available via Vollzug
        "stamm_kapital": stamm_kapital,
        "officers": officers,
        "shareholders": shareholders,
    }


def _parse_address(firma_el: ET.Element) -> str:
    """Extract a formatted address string from a FIRMA element.

    Prefers FI_DKZ03 (Geschäftsanschrift / business address); falls back
    to FI_DKZ04 (Sitz / registered office).

    DKZ03 address type is a ``xs:choice`` (per the official XSD):
      STELLE        — free-text combined address line (used in Kurzinformation)
      STRASSE + HAUSNUMMER + STIEGE — structured (may appear in paid Auszug)

    The official HVD interface description v1.3 (page 6) shows STELLE format
    in the Kurzinformation example response; structured fields may appear when
    a paid subscription key is configured.
    """
    for tag in ("FI_DKZ03", "FI_DKZ04"):
        addr_el = firma_el.find(f".//{tag}")
        if addr_el is None:
            continue
        # Primary path: STELLE (free-text, Kurzinformation responses)
        stellen = [s.text.strip() for s in addr_el.findall("STELLE") if s.text]
        if stellen:
            street_parts = stellen
        else:
            # Fallback: structured address fields (paid Auszug responses)
            street_parts = [p for p in [
                _text(addr_el, "STRASSE"),
                _text(addr_el, "HAUSNUMMER"),
                _text(addr_el, "STIEGE"),
            ] if p]
        location_parts = [p for p in [_text(addr_el, "PLZ"), _text(addr_el, "ORT")] if p]
        non_empty = street_parts + location_parts
        if non_empty:
            return " ".join(non_empty)
    return ""


def _parse_person(el: ET.Element) -> tuple[str, str, str, str]:
    """Extract (given_name, family_name, full_name, dob) from a DKZ element.

    Firmenbuch person records store names in VORNAME / NACHNAME children.
    The date of birth (Geburtsdatum) appears as GEBURTSDATUM.
    """
    given = _text(el, "VORNAME") or ""
    family = _text(el, "NACHNAME") or _text(el, "NAME") or ""
    full = " ".join(filter(None, [given, family])).strip()
    dob = _text(el, "GEBURTSDATUM") or el.get("GEBURTSDATUM") or ""
    return given, family, full, dob


def _parse_fun_per_officers(resp_el: ET.Element) -> list[dict[str, Any]]:
    """Parse officers from the FUN/PER structure in a Kurzinformation response.

    Kurzinformation responses place officer data in top-level FUN and PER
    elements that are siblings of FIRMA inside AUSZUG_V2_RESPONSE, NOT inside
    the FIRMA element itself (confirmed by the Firmenbuch team, May 2026).

    FUN element — one per role appointment:
        FKEN attribute    — role code (GF, PK, AR, VW, LI, …)
        FKENTEXT attribute — human-readable role label
        PNR attribute     — person reference key linking to a PER element
        FU_DKZ10 child    — carries AUFRECHT="true"|"false" and VNR

    PER element — one per person:
        PNR attribute     — matches FUN.PNR
        PE_DKZ02 child    — current personal data (VORNAME, NACHNAME,
                             GEBURTSDATUM, …)

    Terminated appointments (FU_DKZ child AUFRECHT="false") are skipped.
    """
    # Index PER elements by PNR so we can resolve FUN → person in O(1).
    per_map: dict[str, ET.Element] = {}
    for per_el in resp_el.iter("PER"):
        pnr = (per_el.get("PNR") or "").strip()
        if pnr:
            per_map[pnr] = per_el

    officers: list[dict[str, Any]] = []
    for fun_el in resp_el.iter("FUN"):
        fken = (fun_el.get("FKEN") or "").strip()
        fkentext = (fun_el.get("FKENTEXT") or "").strip()
        pnr = (fun_el.get("PNR") or "").strip()

        # Active check: look at the AUFRECHT attribute on the FU_DKZ child.
        # If any child explicitly says AUFRECHT="false" the appointment is
        # terminated — skip it.
        is_active = True
        for child in fun_el:
            if child.get("AUFRECHT", "true").lower() == "false":
                is_active = False
                break
        if not is_active:
            continue

        # Resolve to the linked PER element.
        per_el = per_map.get(pnr)
        given = family = full = dob = ""
        if per_el is not None:
            pe_dkz02 = per_el.find(".//PE_DKZ02")
            target = pe_dkz02 if pe_dkz02 is not None else per_el
            given, family, full, dob = _parse_person(target)

        if not full:
            continue

        officers.append({
            "full_name": full,
            "given_name": given,
            "family_name": family,
            "role_code": fken,
            "role_name": fkentext,
            "start_date": "",  # not in FU_DKZ10 at Kurzinformation scope
            "dob": dob,
        })
    return officers


def _parse_officers(firma_el: ET.Element) -> list[dict[str, Any]]:
    """Fallback: extract officers from FI_DKZ sections inside FIRMA.

    This path was written for the paid Auszug tier where officers appear as
    FI_DKZ08 / FI_DKZ09 / FI_DKZ10 / FI_DKZ14 / FI_DKZ16 children of
    FIRMA.  In practice the free Kurzinformation tier uses the FUN/PER
    structure parsed by ``_parse_fun_per_officers`` instead.  This function
    is kept as a fallback in case the response structure varies.
    """
    _DKZ_ROLES: dict[str, tuple[str, str]] = {
        "FI_DKZ08": ("GF",    "Geschäftsführer"),
        "FI_DKZ09": ("PK",    "Prokurist"),
        "FI_DKZ10": ("AR",    "Aufsichtsratsmitglied"),
        "FI_DKZ12": ("KOMPL", "Komplementär"),
        "FI_DKZ14": ("VW",    "Vorstandsmitglied"),
        "FI_DKZ16": ("LI",    "Liquidator"),
    }
    officers: list[dict[str, Any]] = []
    for dkz_tag, (role_code, role_name) in _DKZ_ROLES.items():
        for dkz_el in firma_el.iter(dkz_tag):
            if dkz_el.get("AUFRECHT", "true").lower() == "false":
                continue
            given, family, full, dob = _parse_person(dkz_el)
            if not full:
                continue
            officers.append({
                "full_name": full,
                "given_name": given,
                "family_name": family,
                "role_code": role_code,
                "role_name": role_name,
                "start_date": dkz_el.get("EINTRAGUNGSDATUM") or dkz_el.get("EINTRITTSDATUM") or "",
                "dob": dob,
            })
    return officers


def _parse_shareholders(
    firma_el: ET.Element,
    stamm_kapital: float | None,
) -> list[dict[str, Any]]:
    """Extract shareholder / partner records.

    DKZ → role mapping:
      FI_DKZ07  GmbH Gesellschafter / shareholders
      FI_DKZ13  KG Kommanditisten / limited partners

    Share percentage is computed from STAMMEINLAGE / stamm_kapital when both
    are available.  Corporate shareholders may carry a FNR attribute that
    links to another Firmenbuch entry.
    """
    shareholders: list[dict[str, Any]] = []

    def _extract(dkz_tag: str, kind: str) -> None:
        for dkz_el in firma_el.iter(dkz_tag):
            if dkz_el.get("AUFRECHT", "true").lower() == "false":
                continue
            given, family, full, dob = _parse_person(dkz_el)
            # Corporate shareholder: name in BEZEICHNUNG children
            if not full:
                bez = [b.text.strip() for b in dkz_el.iter("BEZEICHNUNG") if b.text]
                full = " ".join(bez).strip()
            if not full:
                continue
            is_person = bool(given or family)

            einlage_raw = (_text(dkz_el, "STAMMEINLAGE") or _text(dkz_el, "EINLAGE") or
                           dkz_el.get("STAMMEINLAGE") or "").replace(",", ".")
            try:
                einlage: float | None = float(einlage_raw) if einlage_raw else None
            except ValueError:
                einlage = None

            pct: float | None = None
            if einlage is not None and stamm_kapital:
                try:
                    pct = round(einlage / stamm_kapital * 100, 4)
                except ZeroDivisionError:
                    pass

            shareholders.append({
                "display_name": full,
                "is_person": is_person,
                "given_name": given,
                "family_name": family,
                "dob": dob if is_person else None,
                "einlage": einlage,
                "share_pct": pct,
                "fn": dkz_el.get("FNR") or "",
                "kind": kind,
            })

    _extract("FI_DKZ07", "gesellschafter")
    _extract("FI_DKZ13", "kommanditist")
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
    # Live API returns FKEN="PR"/"PRI" for Prokurist/Prokuristin — the "PK"/"PKI"
    # codes match the DKZ fallback path; both must be mapped.
    "PR":   ("otherInfluenceOrControl", "Prokurist (Authorised Signatory)"),
    "PRI":  ("otherInfluenceOrControl", "Prokuristin (Authorised Signatory)"),
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
                "Austrian company name, address, status, and officers "
                "(managing directors, authorised signatories, supervisory board) "
                "from the Firmenbuch (commercial register), via the BMJ High "
                "Value Dataset API. Shareholder data requires a paid subscription."
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

        xml_response, _ = await self._soap_call(
            _search_request_xml(query),
            cache_key=cache_key,
        )
        if not xml_response:
            return []

        hits_raw = _parse_search_response(xml_response)
        return [self._entity_hit(h) for h in hits_raw if h.get("fn")]

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

        xml_response, soap_error = await self._soap_call(
            _extract_request_xml(fn),
            cache_key=cache_key,
        )
        if not xml_response:
            return {
                "source_id": self.id,
                "fn": fn,
                "extract": None,
                "legal_name": self._names.get(fn, legal_name),
                "is_stub": False,
                "soap_error": soap_error,
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
    ) -> tuple[str | None, str | None]:
        """Send a SOAP 1.2 request and return ``(xml_text, error)`` pair.

        On success: ``(xml_text, None)``.
        On failure: ``(None, error_description)`` — the error string is
        surfaced in the fetch bundle under ``"soap_error"`` so callers can
        diagnose failures without needing debug-level logging.

        Results are cached by *cache_key*; cached calls always return no error.

        The Firmenbuch API uses a blank SOAPAction (``""``), not the operation
        name.  The ``X-Api-Key`` header carries the HVD API key.
        The API is SOAP/XML only, so we override the ``Accept`` header that
        ``build_client()`` sets to ``application/json``.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0], None

        settings = get_settings()
        api_key = settings.firmenbuch_api_key or ""

        # The Firmenbuch SOAP endpoint is an Austrian government server that can
        # take 5–10 s for the full TCP + TLS handshake from non-Austrian networks.
        # Use a generous connect timeout to accommodate the slow TLS negotiation;
        # the read timeout is tighter since the API responds quickly once connected.
        _fb_timeout = httpx.Timeout(connect=15.0, read=10.0, write=5.0, pool=5.0)

        try:
            async with build_client() as client:
                response = await client.post(
                    _SOAP_ENDPOINT,
                    content=body.encode("utf-8"),
                    headers={
                        "Content-Type": "application/soap+xml;charset=UTF-8",
                        # Override the default Accept: application/json set by
                        # build_client() — the Firmenbuch API is SOAP/XML only.
                        "Accept": "application/soap+xml;charset=UTF-8",
                        "SOAPAction": '""',
                        "X-Api-Key": api_key,
                    },
                    timeout=_fb_timeout,
                )
                response.raise_for_status()
                xml_text = response.text
        except Exception as exc:
            error_str = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Firmenbuch SOAP call failed (cache_key=%s): %s",
                cache_key,
                error_str,
                exc_info=True,
            )
            return None, error_str

        logger.debug(
            "Firmenbuch raw XML response (cache_key=%s):\n%s",
            cache_key,
            xml_text,
        )
        self._cache.put(cache_key, xml_text)
        return xml_text, None

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
