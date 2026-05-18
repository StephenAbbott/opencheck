"""Lithuanian Register of Legal Entities (JAR) adapter.

The Registrų centras (Centre of Registers) maintains the Juridinių Asmenų
Registras (JAR) — Lithuania's statutory register of all legal entities,
operating under the Ministry of Justice.

This adapter uses the JAR public search interface at:
  https://www.registrucentras.lt/jar/p/

Two live endpoints are used (both return HTML tables, parsed inline):
  Name search:  GET /jar/p/index.php?pav=<query>     — up to ~20 results
  Code lookup:  GET /jar/p/index.php?kod=<9-digit>   — single entity

Rate limit: Registrų centras applies a soft cap of 100 public queries per
IP address per day.  Search results are cached to stay within this limit.

Open data bulk download (CC BY 4.0, updated daily):
  https://www.registrucentras.lt/aduomenys/?byla=JAR_IREGISTRUOTI.csv
  Key fields: jaAsm_Kodas (9-digit code), jaAsm_Pavadinimas (name),
  jaAsm_Adresas (address), jaAsm_FormKodas / jaAsm_FormPav (legal form),
  jaAsm_StatusKodas / jaAsm_StatusPav (status), jaAsm_Reg (reg. date),
  jaAsm_IzReg (deregistration date).

BO / participant data: Formerly available via JADIS open data, participant
(shareholder) records are now being migrated to JANGIS, a restricted
sub-system accessible only to those with legitimate interest.  This adapter
covers entity data only; BO data is intentionally excluded.

The flow with GLEIF:
  1. GLEIF returns ``registeredAt.id == "RA000430"`` (JAR RA code) and
     ``registeredAs = "<9-digit-code>"`` for Lithuanian entities.
  2. app.py extracts ``derived["lt_code"]`` and calls ``fetch()`` here.
  3. We scrape the public JAR page and return entity details.

Authentication: none — public interface, no API key required.
GLEIF RA code: RA000430 (Register of Legal Entities, Registrų centras)
License: CC BY 4.0 (Creative Commons Attribution 4.0 International).
  https://creativecommons.org/licenses/by/4.0/
Attribution: "Contains data from the Lithuanian Register of Legal Entities
  (JAR), published by Registrų centras (registrucentras.lt), CC BY 4.0."
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

# Base URL for the public JAR search interface.
_SEARCH_URL = "https://www.registrucentras.lt/jar/p/index.php"

_CACHE_NS = "jar_lithuania"

# GLEIF Registration Authority code for Lithuania's JAR.
LT_RA_CODE: str = "RA000430"

# Lithuanian entity code: exactly 9 digits.
_CODE_RE = re.compile(r"^\d{9}$")

# Lithuanian legal form short codes → English descriptions.
# Full form names appear verbatim in HTML; this dict maps the most common
# abbreviations seen in the TEISINĖ FORMA column.
_LEGAL_FORMS: dict[str, str] = {
    "UAB": "Uždaroji akcinė bendrovė (Private limited company)",
    "AB": "Akcinė bendrovė (Public limited company)",
    "MB": "Mažoji bendrija (Small partnership)",
    "IĮ": "Individuali įmonė (Sole proprietorship)",
    "TŪB": "Tikroji ūkinė bendrija (General partnership)",
    "KŪB": "Komanditinė ūkinė bendrija (Limited partnership)",
    "VšĮ": "Viešoji įstaiga (Public institution)",
    "Asociacija": "Asociacija (Association)",
    "Valstybės įmonė": "Valstybės įmonė (State enterprise)",
    "Savivaldybės įmonė": "Savivaldybės įmonė (Municipal enterprise)",
    "Biudžetinė įstaiga": "Biudžetinė įstaiga (Budget institution)",
    "Labdaros ir paramos fondas": "Labdaros ir paramos fondas (Charitable foundation)",
    "Kooperatinė bendrovė": "Kooperatinė bendrovė (Cooperative)",
    "Europos ekonominių interesų grupė": "Europos ekonominių interesų grupė (EEIG)",
}

# Status values that indicate an inactive entity.
_INACTIVE: frozenset[str] = frozenset(
    s.lower()
    for s in [
        "Išregistruotas",
        "Likviduojamas",
        "Bankrutuojantis",
        "Reorganizuojamas",
        "Sustabdyta",
        "Bankrotas",
    ]
)

# Regex to extract entity rows from the JAR HTML results table.
# The table structure is:
#   <td>CODE [img link]</td><td>NAME <br> ADDRESS</td><td>FORM <br> STATUS</td>
_ROW_RE = re.compile(
    r"<td[^>]*>\s*(\d{9}).*?</td>"   # code cell (9-digit code first)
    r"\s*<td[^>]*>(.*?)</td>"         # name + address cell
    r"\s*<td[^>]*>(.*?)</td>",        # legal form + status cell
    re.DOTALL | re.IGNORECASE,
)

# Count of results line e.g. "Rasta įrašų: 27"
_COUNT_RE = re.compile(r"Rasta\s+įrašų:\s*(\d+)", re.IGNORECASE)

# No results / error indicators
_NO_RESULTS_PATTERNS = (
    "nerasta",  # "not found"
    "Nieko nerasta",
    "0 įrašų",
)


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_code(code: str | int) -> str:
    """Return the canonical 9-digit Lithuanian legal entity code string."""
    return str(code).strip().zfill(9)


def is_valid_lt_code(code: str) -> bool:
    """True when ``code`` is a valid 9-digit Lithuanian entity code."""
    return bool(_CODE_RE.match(normalise_code(code)))


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _entity_url(code: str) -> str:
    """Return the public JAR entity page URL for a given code."""
    return f"{_SEARCH_URL}?kod={code}"


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _strip_tags(html_fragment: str) -> str:
    """Remove all HTML tags from a fragment, treating <br> as a newline."""
    html_fragment = re.sub(r"<br\s*/?>", "\n", html_fragment, flags=re.IGNORECASE)
    return re.sub(r"<[^>]+>", " ", html_fragment)


def _clean(text: str) -> str:
    """Collapse whitespace and strip, preserving single newlines."""
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def _parse_jar_html(html: str) -> list[dict[str, str]]:
    """Extract entity records from JAR public-search result HTML.

    Returns a list of dicts with keys:
      code, name, address, legal_form, status
    """
    results: list[dict[str, str]] = []
    for m in _ROW_RE.finditer(html):
        code = m.group(1)
        name_addr = _clean(_strip_tags(m.group(2)))
        form_status = _clean(_strip_tags(m.group(3)))

        parts = name_addr.split("\n", 1)
        name = parts[0].strip()
        address = parts[1].strip() if len(parts) > 1 else ""

        fs_parts = form_status.split("\n", 1)
        legal_form = fs_parts[0].strip()
        status = fs_parts[1].strip() if len(fs_parts) > 1 else ""

        if code and name:
            results.append(
                {
                    "code": code,
                    "name": name,
                    "address": address,
                    "legal_form": legal_form,
                    "status": status,
                }
            )
    return results


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class JarLithuaniaAdapter(SourceAdapter):
    """Source adapter for the Lithuanian Register of Legal Entities (JAR)."""

    id = "jar_lithuania"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="JAR — Lithuanian Register of Legal Entities",
            homepage="https://www.registrucentras.lt/jar/",
            description=(
                "Lithuanian company data from the Register of Legal Entities "
                "(JAR), maintained by Registrų centras. Provides entity name, "
                "code, address, legal form, and registration status for all "
                "entities registered in Lithuania."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Lithuanian Register of Legal Entities "
                "(JAR), published by Registrų centras, available under CC BY 4.0. "
                "Source: registrucentras.lt."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search — name-based HTML scrape of the JAR public search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        html = await self._fetch_html(
            f"{_SEARCH_URL}?pav={quote(query)}",
            cache_key=cache_key,
        )
        if html is None:
            return self._stub_search(query)

        records = _parse_jar_html(html)
        return [self._entity_hit(rec) for rec in records]

    # ------------------------------------------------------------------
    # Fetch — code-based lookup of a single entity
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full JAR record for a Lithuanian entity code.

        ``hit_id`` is a 9-digit entity code (may arrive zero-padded or not).
        ``legal_name`` is an optional fallback from GLEIF.
        """
        code = normalise_code(hit_id)

        stub_bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": code,
            "lt_code": code,
            "name": legal_name,
            "address": None,
            "legal_form": None,
            "status": None,
            "link": _entity_url(code),
            "is_stub": True,
        }

        if not is_valid_lt_code(code):
            _log.warning("jar_lithuania: invalid code %r — returning stub", hit_id)
            return stub_bundle

        cache_key = f"{_CACHE_NS}/entity/{code}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return stub_bundle

        html = await self._fetch_html(
            f"{_SEARCH_URL}?kod={quote(code)}",
            cache_key=cache_key,
        )
        if html is None:
            return stub_bundle

        records = _parse_jar_html(html)
        if not records:
            return stub_bundle

        rec = records[0]
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": code,
            "lt_code": code,
            "name": rec.get("name") or legal_name,
            "address": rec.get("address") or None,
            "legal_form": rec.get("legal_form") or None,
            "status": rec.get("status") or None,
            "link": _entity_url(code),
            "is_stub": False,
        }
        return bundle

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _fetch_html(self, url: str, *, cache_key: str) -> str | None:
        """GET ``url``, cache the HTML, and return it.

        Returns ``None`` on HTTP error; logs a warning but does not raise.
        The cached value is the raw HTML string (stored as a single-element
        list to match the Cache.put/get_payload contract).
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        headers = {
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "lt,en;q=0.8",
        }
        try:
            async with build_client() as client:
                resp = await client.get(url, headers=headers)
        except Exception as exc:  # noqa: BLE001
            _log.warning("jar_lithuania: HTTP error fetching %s: %s", url, exc)
            return None

        if not resp.is_success:
            _log.warning(
                "jar_lithuania: HTTP %s fetching %s", resp.status_code, url
            )
            return None

        html = resp.text
        self._cache.put(cache_key, html)
        return html

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(rec: dict[str, str]) -> SourceHit:
        code = rec.get("code", "")
        name = (rec.get("name") or code or "Unknown").strip()
        legal_form = (rec.get("legal_form") or "").strip()
        status = (rec.get("status") or "").strip()
        address = (rec.get("address") or "").strip()

        # Determine active/inactive flag from status.
        is_active = status.lower() not in _INACTIVE if status else True
        status_label = "active" if is_active else status or "inactive"

        summary_parts: list[str] = [f"LT {code}"]
        if legal_form:
            summary_parts.append(legal_form)
        summary_parts.append(status_label)

        return SourceHit(
            source_id="jar_lithuania",
            hit_id=code,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_parts),
            identifiers={"lt_code": code},
            raw=rec,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub helpers
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="111950694",  # AB Lietuvos energija (state energy group)
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub JAR Lithuania record — set OPENCHECK_ALLOW_LIVE=true "
                    "to query the live Registrų centras public search."
                ),
                identifiers={"lt_code": "111950694"},
                raw={
                    "code": "111950694",
                    "name": f"{query} (stub)",
                    "address": "Žvejų g. 14, LT-09310 Vilnius",
                    "legal_form": "AB",
                    "status": "Veikiantis",
                },
            )
        ]
