"""Estonian e-Business Register (e-Äriregister) adapter — public web scraper.

Fetches data from the public printable page at ariregister.rik.ee without
any authentication.  The printable-page endpoint is a server-rendered HTML
page that contains all the data the website shows: general company info,
board members, shareholders, and beneficial owners.

Endpoints used:
  /eng/company/{reg_code}/company_print_json — full company printable page (HTML)
  /eng/api/autocomplete?q={query}            — JSON search (name / reg code)

No API key or contract required.  RIK confirmed (May 2026) that the
ariregister.rik.ee public portal is freely accessible.

GLEIF RA code: RA000181

The flow with GLEIF:
  1. GLEIF returns registeredAt.id == "RA000181" for Estonian entities
     and registeredAs = "<registry_code>".
  2. lookup.py extracts derived["ee_registry_code"] and calls fetch().
  3. We GET the printable HTML page and parse the Bootstrap label/value
     pairs plus the officers, shareholders and beneficial-owners tables.

Output bundle keys (same as the previous SOAP adapter so map_ariregister
in bods/mapper.py needs no changes):
  registry_code, name, legal_form, vat_number, status, registration_date,
  address, link, shareholders, officers, beneficial_owners, is_stub
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Estonian e-Business Register.
EE_RA_CODE: str = "RA000181"

_BASE_URL = "https://ariregister.rik.ee"
_PRINT_URL = _BASE_URL + "/eng/company/{reg_code}/company_print_json"
_AUTO_URL  = _BASE_URL + "/eng/api/autocomplete"
_TIMEOUT   = 20.0

_HEADERS = {
    "User-Agent": "OpenCheck/1.0 (https://opencheck.onrender.com; beneficialownership.co.uk)",
    "Accept": "text/html,application/json,*/*",
}

# Maps English role labels from the HTML page → internal Estonian role code
# used by the existing map_ariregister() mapper in bods/mapper.py.
_ROLE_LABEL_MAP: dict[str, str] = {
    "management board member":   "JUHL",
    "board member":              "JUHL",
    "procurist":                 "PROK",
    "liquidator (board member)": "LIKVJ",
    "liquidator":                "LIKV",
    "general partner":           "TOSAN",
    "limited partner":           "UOSAN",
    "authorised representative": "ASES",
    "legal representative":      "SJESI",
    "branch manager":            "VFILJ",
    "fund manager":              "FV",
}

# Maps English manner-of-control text → Estonian BO control code
# used by map_ariregister().
_BO_MANNER_MAP: dict[str, str] = {
    "direct ownership":        "O",
    "direct participation":    "O",
    "indirect ownership":      "K",
    "indirect participation":  "K",
    "through voting rights":   "H",
    "via voting rights":       "H",
    "voting rights":           "H",
}


# ---------------------------------------------------------------------------
# HTML parsing helpers
# ---------------------------------------------------------------------------

def _clean(html_fragment: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", html_fragment)
    return re.sub(r"\s+", " ", text).strip()


def _parse_info(html: str) -> dict[str, str]:
    """Extract label → value pairs from Bootstrap row structure.

    The page uses:
        <div class="col-md-4 text-muted">Label</div>
        <div class="col font-weight-bold">Value … </div>
    """
    pattern = (
        r'class="col-md-4 text-muted"[^>]*>(.*?)</div>'
        r'\s*<div[^>]*class="col[^"]*">(.*?)</div>'
    )
    result: dict[str, str] = {}
    for label_html, val_html in re.findall(pattern, html, re.DOTALL):
        label = _clean(label_html)
        value = _clean(val_html)
        # Drop "Open map" suffix from address values
        value = re.sub(r"\s*Open map.*$", "", value, flags=re.DOTALL).strip()
        if label and value:
            result[label] = value
    return result


def _find_table(html: str, header_keywords: list[str]) -> list[list[str]]:
    """Find the first <table> whose header row contains all given keywords.

    Returns a list of data rows (each row is a list of cell text values).
    """
    for table_html in re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL):
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
        if not rows:
            continue
        header_cells = [
            _clean(c)
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rows[0], re.DOTALL)
        ]
        header_text = " | ".join(header_cells).lower()
        if all(kw.lower() in header_text for kw in header_keywords):
            data_rows: list[list[str]] = []
            for row in rows[1:]:
                cells = [
                    _clean(c)
                    for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
                ]
                cells = [c for c in cells if c]  # drop blank cells
                if cells:
                    data_rows.append(cells)
            return data_rows
    return []


def _is_estonian_personal_code(code: str) -> bool:
    """Return True if code looks like an Estonian personal identification code.

    Estonian personal codes are exactly 11 digits and the first digit encodes
    gender + birth century (valid values: 1-6 for 20th/21st century).
    """
    return bool(re.fullmatch(r"[1-6]\d{10}", code))


def _is_registry_code(code: str) -> bool:
    """Return True if code looks like an Estonian commercial registry code.

    Estonian registry codes are 8 digits.
    """
    return bool(re.fullmatch(r"\d{8}", code))


def _parse_date(raw: str) -> str | None:
    """Convert DD.MM.YYYY date string to ISO YYYY-MM-DD, or return None."""
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", raw.strip())
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    return None


def _parse_contribution(contribution_str: str) -> tuple[str | None, str | None]:
    """Extract (amount, currency) from strings like '834.00 EUR' or '1515363.00 EUR Sole ownership'."""
    m = re.match(r"([\d\s,\.]+)\s+([A-Z]{3})", contribution_str.strip())
    if m:
        amount = m.group(1).replace(" ", "").replace(",", "")
        currency = m.group(2)
        return amount, currency
    return None, None


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'Firstname Lastname' → (first, last). Last word is the surname."""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return " ".join(parts[:-1]), parts[-1]
    return "", full_name.strip()


# ---------------------------------------------------------------------------
# Page-level parsers
# ---------------------------------------------------------------------------

def _parse_officers(html: str) -> list[dict[str, Any]]:
    """Parse the officers/board members table.

    Expected headers: Name | Personal identification code | Role | Start - end
    """
    rows = _find_table(html, ["Name", "Personal identification code", "Role"])
    officers: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if len(row) < 3:
            continue
        name = row[0]
        id_code = row[1] if len(row) > 1 else ""
        role_label = row[2].lower() if len(row) > 2 else ""
        start_raw = row[3] if len(row) > 3 else ""
        end_raw = row[4] if len(row) > 4 else ""

        role_code = _ROLE_LABEL_MAP.get(role_label)
        if not role_code:
            # Try partial match
            for label, code in _ROLE_LABEL_MAP.items():
                if label in role_label:
                    role_code = code
                    break
        if not role_code:
            logger.debug("ariregister: unknown officer role %r — skipping", row[2])
            continue

        first, last = _split_name(name)
        officers.append({
            "kirje_id": str(idx + 1),
            "eesnimi": first,
            "nimi_arinimi": last,
            "isiku_roll": role_code,
            "isiku_tyyp": "F",
            "isikukood_registrikood": id_code if _is_estonian_personal_code(id_code) else "",
            "algus_kpv": _parse_date(start_raw) or start_raw or None,
            "lopp_kpv": _parse_date(end_raw) or end_raw or None,
            "synniaeg": None,
            "valis_kood_riik": None,
            "isikukood_hash": None,
        })
    return officers


def _parse_shareholders(html: str) -> list[dict[str, Any]]:
    """Parse the shareholders/members table.

    Expected headers: Participation | Contribution | Name | Code | Start - End
    """
    rows = _find_table(html, ["Participation", "Contribution", "Name", "Code"])
    shareholders: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if len(row) < 4:
            continue
        pct_str = re.sub(r"[^\d\.]", "", row[0])  # strip "%" etc.
        contribution_str = row[1]
        name = row[2]
        code = row[3].strip()
        start_raw = row[4] if len(row) > 4 else ""
        end_raw = row[5] if len(row) > 5 else ""

        amount, currency = _parse_contribution(contribution_str)

        # Determine if legal entity or natural person
        if _is_estonian_personal_code(code):
            isiku_tyyp = "F"
            reg_code_field = ""
            personal_code = code
            first, last = _split_name(name)
        else:
            isiku_tyyp = "J"
            reg_code_field = code if _is_registry_code(code) else ""
            personal_code = ""
            first = ""
            last = name.strip()

        shareholders.append({
            "kirje_id": str(idx + 1),
            "isiku_tyyp": isiku_tyyp,
            "eesnimi": first,
            "nimi_arinimi": last,
            "isikukood_registrikood": reg_code_field or personal_code,
            "osaluse_protsent": pct_str or None,
            "osaluse_suurus": amount,
            "osaluse_valuuta": currency,
            "algus_kpv": _parse_date(start_raw) or start_raw or None,
            "lopp_kpv": _parse_date(end_raw) or end_raw or None,
            "synniaeg": None,
            "valis_kood_riik": None,
        })
    return shareholders


def _parse_beneficial_owners(html: str) -> list[dict[str, Any]]:
    """Parse the beneficial owners table.

    Expected headers: Name | Personal identification code … | Manner … | Start - end
    """
    rows = _find_table(html, ["Name", "Manner"])
    bos: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if len(row) < 3:
            continue
        full_name = row[0]
        id_code = row[1] if len(row) > 1 else ""
        manner_label = (row[2] if len(row) > 2 else "").lower()
        start_raw = row[3] if len(row) > 3 else ""
        end_raw = row[4] if len(row) > 4 else ""

        control_code = _BO_MANNER_MAP.get(manner_label)
        if not control_code:
            for label, code in _BO_MANNER_MAP.items():
                if label in manner_label:
                    control_code = code
                    break
        control_code = control_code or "M"

        first, last = _split_name(full_name)
        personal_code = id_code if _is_estonian_personal_code(id_code) else ""

        bos.append({
            "kirje_id": str(idx + 1),
            "eesnimi": first,
            "nimi": last,
            "isikukood": personal_code,
            "kontrolli_teostamise_viis": control_code,
            "algus_kpv": _parse_date(start_raw) or start_raw or None,
            "lopp_kpv": _parse_date(end_raw) or end_raw or None,
            "synniaeg": None,
            "aadress_riik": None,
            "valis_kood_riik": None,
            "isikukood_hash": None,
        })
    return bos


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class AriregisterAdapter(SourceAdapter):
    """Source adapter for the Estonian e-Business Register (e-Äriregister).

    Uses the public printable-page endpoint — no authentication required.
    """

    id = "ariregister"

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Estonian e-Business Register (e-Äriregister)",
            homepage="https://ariregister.rik.ee/eng",
            description=(
                "Estonian company data including entity details, shareholders "
                "(with ownership percentages), board members, and beneficial "
                "owners, from the public e-Business Register portal (RIK)."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Data from the Estonian e-Business Register (e-Äriregister), "
                "published by the Centre of Registers and Information Systems "
                "(RIK), CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=True,
            is_national_register=True,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name or registry-code search via the autocomplete JSON endpoint."""
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            try:
                r = await client.get(
                    _AUTO_URL,
                    params={"q": query},
                    headers=_HEADERS,
                )
                r.raise_for_status()
            except Exception as exc:
                logger.warning("ariregister: search error for %r: %s", query, exc)
                return []

        data = r.json()
        hits: list[SourceHit] = []
        for item in (data.get("data") or []):
            reg = str(item.get("reg_code") or "")
            name = item.get("name") or ""
            status = item.get("status") or ""
            address = item.get("legal_address") or ""
            url = item.get("url") or f"{_BASE_URL}/eng/company/{reg}"
            if not reg or not name:
                continue
            hits.append(SourceHit(
                source_id=self.id,
                hit_id=reg,
                kind=SearchKind.ENTITY,
                name=name,
                summary=f"EE-ARIREGISTER {reg}",
                identifiers={"ee_registry_code": reg},
                raw={"reg_code": reg, "name": name, "status": status,
                     "address": address, "url": url},
                is_stub=False,
            ))
        return hits

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any]:
        """Fetch company data from the public printable page.

        ``hit_id`` is the 8-digit Estonian registry code.
        """
        # Normalise registry code (strip leading zeros for display; keep 8 digits)
        code = hit_id.strip()
        if code.isdigit():
            code = code.lstrip("0") or code
        registry_code = code

        stub: dict[str, Any] = {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": legal_name,
            "legal_form": None,
            "vat_number": None,
            "status": None,
            "registration_date": None,
            "address": None,
            "link": f"{_BASE_URL}/eng/company/{registry_code}",
            "shareholders": [],
            "officers": [],
            "beneficial_owners": [],
            "is_stub": True,
        }

        url = _PRINT_URL.format(reg_code=registry_code)
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                r = await client.get(url, headers=_HEADERS, follow_redirects=True)
        except Exception as exc:
            logger.error("ariregister: fetch error for %s: %s", registry_code, exc)
            return stub

        if r.status_code != 200:
            logger.info("ariregister: HTTP %s for %s", r.status_code, registry_code)
            return stub

        # If the page redirected to a "not found" search page, bail out.
        final_url = str(r.url)
        if "/eng/company/" not in final_url:
            logger.info("ariregister: no company found for %s", registry_code)
            return stub

        html = r.text

        # ── Extract label/value info ─────────────────────────────────────────
        info = _parse_info(html)
        name = info.get("Name") or legal_name
        legal_form = info.get("Legal form") or None
        status_raw = info.get("Status") or ""
        status = status_raw if status_raw else None
        reg_date = _parse_date(info.get("Registered") or "") or info.get("Registered")
        address_raw = info.get("Address") or ""
        # Address sometimes ends with postal code — keep as-is.
        address = address_raw if address_raw else None

        # ── Parse the company name from H2 heading if label lookup missed ────
        if not name:
            m = re.search(r"<h2[^>]*>([^<]+?)\s*\(\d+\)\s*</h2>", html)
            if m:
                name = m.group(1).strip()

        if not name:
            logger.info("ariregister: could not determine company name for %s", registry_code)
            return stub

        officers           = _parse_officers(html)
        shareholders       = _parse_shareholders(html)
        beneficial_owners  = _parse_beneficial_owners(html)

        return {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": name,
            "legal_form": legal_form,
            "vat_number": None,
            "status": status,
            "registration_date": reg_date,
            "address": address,
            "link": f"{_BASE_URL}/eng/company/{registry_code}",
            "shareholders": shareholders,
            "officers": officers,
            "beneficial_owners": beneficial_owners,
            "is_stub": False,
        }
