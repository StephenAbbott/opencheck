"""SEC EDGAR adapter — Schedule 13D/13G beneficial ownership filings.

Surfaces major shareholders (>5 % beneficial owners) of US-listed companies
from the mandatory structured XML filings introduced on December 18 2024.

Search strategy
    EDGAR company-search atom feed (browse-edgar?company=<name>&output=atom)
    → one hit per matching subject company (the issuer), keyed by CIK.
    In practice the CIK is usually sourced directly from OpenCorporates data
    so the name-search fallback is rarely used.

Fetch strategy
    data.sec.gov/submissions/CIK{padded}.json lists all 13D/13G filings
    associated with the company (as issuer or as filer).  For each eligible
    accession (filed ≥ 2024-12-18), primary_doc.xml is fetched from:
        /Archives/edgar/data/{issuer_cik}/{accession_nodashes}/primary_doc.xml
    Files are archived under the subject company's CIK, not the filer's CIK.
    Results are deduplicated per reporter, retaining the most recent filing.

No API key is required — EDGAR is publicly accessible.  The User-Agent header
must identify the application and include a contact e-mail (set via the
OPENCHECK_EDGAR_CONTACT_EMAIL env var) or cloud-hosted requests will be
silently blocked with 403.  See https://www.sec.gov/os/webmaster-faq#developers

Coverage is limited to publicly-traded US companies with shareholders holding
>5 % of a registered equity class who have filed since December 18 2024.
"""

from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.sec_edgar import EDGARBundle

_EDGAR_BASE = "https://www.sec.gov"
_BROWSE_BASE = f"{_EDGAR_BASE}/cgi-bin/browse-edgar"
_SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
# Authoritative ticker→CIK→title map for all exchange-listed US issuers —
# the same universe that files Schedule 13D/13G.  Used to resolve a GLEIF
# legal name to a CIK without relying on EDGAR's fragile prefix name-search.
_TICKERS_URL = f"{_EDGAR_BASE}/files/company_tickers.json"
_CACHE_NS = "sec_edgar"
_NS_13D = "http://www.sec.gov/edgar/schedule13D"
_NS_13G = "http://www.sec.gov/edgar/schedule13g"  # lowercase g — different schema
_NS_ATOM = "http://www.w3.org/2005/Atom"

# Maximum filings retrieved per form type per subject company.
_MAX_FILINGS = 20

# SEC mandate date for machine-readable (structured XML) Schedule 13D/13G
# filings. Filings before this date have no primary_doc.xml, so no
# beneficial-owner data can be extracted from them.
# See https://www.sec.gov/rules/final/2024/33-11253.pdf
_STRUCTURED_FROM = "2024-12-18"

# EDGAR citizenship/organisation codes → ISO 3166-1 alpha-2.
_US_STATES: frozenset[str] = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
})

_EDGAR_CITIZENSHIP_TO_ISO: dict[str, str] = {
    "X1": "US",   # US Person
    "X2": "CA",   # Canadian person
    **{state: "US" for state in _US_STATES},
}

# typeOfReportingPerson codes that indicate a natural person.
_INDIVIDUAL_CODES: frozenset[str] = frozenset({"IN"})

# Trailing legal-form tokens stripped when normalising a company name, so a
# GLEIF legal name ("THE WALT DISNEY COMPANY", "Netflix, Inc.") matches an
# EDGAR conformed name ("Walt Disney Co", "NETFLIX INC").
_LEGAL_FORM_SUFFIXES: frozenset[str] = frozenset({
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY",
    "PLC", "LTD", "LIMITED", "LLC", "LLP", "LP", "NV", "SA", "AG",
    "SE", "AB", "AS", "OYJ", "SPA", "GMBH", "KG", "BV",
})


def _normalise_company_name(name: str) -> str:
    """Normalise a company name for cross-source matching.

    Uppercases, replaces punctuation with spaces, strips a leading ``THE``,
    and repeatedly strips trailing legal-form tokens (``INC``, ``CO``,
    ``COMPANY``, ``CORP`` …).  Returns the distinctive name tokens joined by
    single spaces (empty string if nothing remains).

    Examples::

        "THE WALT DISNEY COMPANY" -> "WALT DISNEY"
        "Netflix, Inc."           -> "NETFLIX"
        "Walt Disney Co"          -> "WALT DISNEY"
    """
    s = re.sub(r"[^A-Z0-9 ]+", " ", (name or "").upper())
    tokens = s.split()
    while tokens and tokens[0] == "THE":
        tokens = tokens[1:]
    while tokens and tokens[-1] in _LEGAL_FORM_SUFFIXES:
        tokens = tokens[:-1]
    return " ".join(tokens)


# ----------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _xml_text(elem: ET.Element | None) -> str:
    """Return stripped text content or empty string for a possibly-None element."""
    if elem is None:
        return ""
    return (elem.text or "").strip()


def _ns(tag: str) -> str:
    """Qualify a tag name with the SCHEDULE 13D namespace."""
    return f"{{{_NS_13D}}}{tag}"


def _parse_company_cik(entry_id: str) -> str:
    """Extract bare CIK from EDGAR atom entry id.

    Example id: ``urn:tag:sec.gov,2008:company=0001234567``
    Returns ``1234567`` (leading zeros stripped, empty string on failure).
    """
    if "company=" in entry_id:
        raw = entry_id.split("company=")[-1]
        return raw.lstrip("0") or "0"
    return ""


# ----------------------------------------------------------------------
# Atom parsing helpers
# ----------------------------------------------------------------------


def _parse_company_hits_from_atom(atom_xml: str) -> list[SourceHit]:
    """Parse EDGAR company-search atom → SourceHit list."""
    if not atom_xml:
        return []
    try:
        root = ET.fromstring(atom_xml)
    except ET.ParseError:
        return []

    ns = {"atom": _NS_ATOM}
    hits: list[SourceHit] = []
    for entry in root.findall("atom:entry", ns):
        title_el = entry.find("atom:title", ns)
        id_el = entry.find("atom:id", ns)
        if title_el is None or id_el is None:
            continue
        name = _xml_text(title_el)
        entry_id = _xml_text(id_el)
        cik = _parse_company_cik(entry_id)
        if not cik:
            continue

        summary_el = entry.find("atom:summary", ns)
        summary_text = _xml_text(summary_el) if summary_el is not None else ""

        hits.append(
            SourceHit(
                source_id="sec_edgar",
                hit_id=cik,
                kind=SearchKind.ENTITY,
                name=name,
                summary=f"CIK {cik} · {summary_text}".rstrip(" ·") if summary_text else f"CIK {cik} · US listed company",
                identifiers={"edgar_cik": cik},
                raw={"cik": cik, "name": name, "summary": summary_text},
                is_stub=False,
            )
        )
    return hits


def _parse_filing_refs_from_atom(atom_xml: str) -> list[dict[str, str]]:
    """Parse an EDGAR filing-search atom feed → list of filing reference dicts.

    EDGAR exposes a per-company filing-search atom at:
        /cgi-bin/browse-edgar?action=getcompany&CIK=<cik>&type=SC+13&output=atom

    Each entry in the feed corresponds to one filing.  This helper extracts
    the metadata needed to locate the primary XML document:

    - ``filer_cik``  — EDGAR CIK of the filer (from the link href)
    - ``accession``  — 18-digit accession number, dashes removed
    - ``form_type``  — e.g. ``"SCHEDULE 13D"`` (from the ``<category>`` term)
    - ``filed``      — ISO date string e.g. ``"2026-04-15"``

    Only entries whose ``form_type`` contains ``"13D"`` or ``"13G"`` are
    returned; an empty feed (no entries) returns ``[]``.
    """
    if not atom_xml:
        return []
    try:
        root = ET.fromstring(atom_xml)
    except ET.ParseError:
        return []

    ns = {"atom": _NS_ATOM}
    refs: list[dict[str, str]] = []
    for entry in root.findall("atom:entry", ns):
        # The EDGAR getcompany atom puts the clean filing metadata in
        # <content> child elements; prefer those and fall back to the
        # <category>/<id>/<link> elements for other feed variants.
        content_el = entry.find("atom:content", ns)

        def _ctext(tag: str) -> str:
            if content_el is None:
                return ""
            el = content_el.find(f"atom:{tag}", ns)
            return _xml_text(el)

        # Form type — <content><filing-type> or <category term=…>.
        form_type = _ctext("filing-type")
        if not form_type:
            cat_el = entry.find("atom:category", ns)
            form_type = (cat_el.get("term") or "").strip() if cat_el is not None else ""
        if not ("13D" in form_type or "13G" in form_type):
            continue

        # Accession — <content><accession-number> or the <id> urn tag.
        raw_accession = _ctext("accession-number")
        if not raw_accession:
            id_text = _xml_text(entry.find("atom:id", ns))
            if "accession-number=" in id_text:
                raw_accession = id_text.split("accession-number=")[-1].strip()
        if not raw_accession:
            continue
        accession = raw_accession.replace("-", "")

        # Archive CIK — from <content><filing-href> or the <link> href:
        #   /Archives/edgar/data/{cik}/{accession_nodashes}/…-index.htm
        href = _ctext("filing-href")
        if not href:
            link_el = entry.find("atom:link", ns)
            href = (link_el.get("href") or "") if link_el is not None else ""
        filer_cik = ""
        if "/Archives/edgar/data/" in href:
            tail = href.split("/Archives/edgar/data/")[-1]
            cik_candidate = tail.split("/")[0]
            filer_cik = cik_candidate.lstrip("0") or cik_candidate

        # Filed date — <content><filing-date> is a clean YYYY-MM-DD; fall back
        # to a date found in <summary> ("Filed: …") or the <updated> prefix.
        filed = _ctext("filing-date")
        if not filed:
            summary_text = _xml_text(entry.find("atom:summary", ns))
            m = re.search(r"\d{4}-\d{2}-\d{2}", summary_text)
            if m:
                filed = m.group(0)
        if not filed:
            filed = _xml_text(entry.find("atom:updated", ns))[:10]

        refs.append(
            {
                "filer_cik": filer_cik,
                "accession": accession,
                "form_type": form_type,
                "filed": filed,
            }
        )
    return refs


# ----------------------------------------------------------------------
# Filing XML parser
# ----------------------------------------------------------------------


def _parse_filing_xml(xml_text: str, source_url: str = "") -> dict[str, Any] | None:
    """Parse a SCHEDULE 13D/G XML document → normalised dict.

    Handles both the 13D namespace (``http://www.sec.gov/edgar/schedule13D``)
    and the 13G namespace (``http://www.sec.gov/edgar/schedule13g``) which
    differ in casing and element names.

    Returns ``None`` if the document is empty, unparseable, or missing
    the required structural elements.
    """
    if not xml_text:
        return None
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    # Detect namespace from the root element tag.
    ns = _NS_13D
    if "{" in root.tag:
        ns = root.tag.split("}")[0][1:]
    is_13g = "schedule13g" in ns.lower()

    def ntag(tag: str) -> str:
        return f"{{{ns}}}{tag}"

    cover_header = root.find(f".//{ntag('coverPageHeader')}")
    if cover_header is None:
        return None

    issuer_info = cover_header.find(ntag("issuerInfo"))
    if issuer_info is None:
        return None

    # 13D uses issuerCIK (uppercase K); 13G uses issuerCik (lowercase k).
    issuer_cik_el = issuer_info.find(ntag("issuerCIK"))
    if issuer_cik_el is None:
        issuer_cik_el = issuer_info.find(ntag("issuerCik"))
    issuer_cik = _xml_text(issuer_cik_el).lstrip("0") or ""
    issuer_name = _xml_text(issuer_info.find(ntag("issuerName")))

    # CUSIP is a flat child of issuerInfo in both schemas:
    #   13D: issuerCUSIP  (per SEC XML spec / John Friedman's columnar mapping)
    #   13G: issuerCusip
    # The old nested path issuerCusips/issuerCusipNumber was wrong.
    cusip_el = issuer_info.find(ntag("issuerCUSIP"))
    if cusip_el is None:
        cusip_el = issuer_info.find(ntag("issuerCusip"))
    issuer_cusip = _xml_text(cusip_el)

    # filerCik — the entity that submitted this document
    # (headerData/filerInfo/filer/filerCredentials/cik).
    # When filerCik == issuerCik the subject company filed the 13D itself
    # (e.g. GameStop reporting its own stake in eBay); otherwise a third
    # party is reporting ownership of the subject company.
    filer_cik_el = root.find(f".//{ntag('filerCredentials')}/{ntag('cik')}")
    filer_cik = _xml_text(filer_cik_el).lstrip("0") if filer_cik_el is not None else ""

    # Address block (optional — present in most 13D filings).
    addr_el = issuer_info.find(ntag("address"))
    issuer_address: dict[str, str] = {}
    if addr_el is not None:
        for field in ("street1", "street2", "city", "stateOrCountry", "zipCode"):
            val = _xml_text(addr_el.find(ntag(field)))
            if val:
                issuer_address[field] = val

    issuer: dict[str, Any] = {
        "cik": issuer_cik,
        "name": issuer_name,
        "cusip": issuer_cusip,
        "address": issuer_address,
    }

    reporters: list[dict[str, Any]] = []
    if is_13g:
        # 13G: each reporting person is in a coverPageHeaderReportingPersonDetails
        # element (may appear multiple times under formData).
        for details_el in root.findall(f".//{ntag('coverPageHeaderReportingPersonDetails')}"):
            reporter = _parse_13g_reporter_element(details_el, ns)
            if reporter:
                reporters.append(reporter)
    else:
        # 13D: reporters nested under reportingPersons/reportingPersonInfo.
        reporting_el = root.find(f".//{ntag('reportingPersons')}")
        if reporting_el is not None:
            for person_el in reporting_el.findall(ntag("reportingPersonInfo")):
                reporter = _parse_reporter_element(person_el)
                if reporter:
                    reporters.append(reporter)

    return {
        "issuer": issuer,
        "reporters": reporters,
        "filer_cik": filer_cik,
        "source_url": source_url,
    }


def _parse_reporter_element(elem: ET.Element) -> dict[str, Any] | None:
    """Parse a ``reportingPersonInfo`` XML element into a normalised dict."""
    name = _xml_text(elem.find(_ns("reportingPersonName")))
    if not name:
        return None

    reporter_cik = _xml_text(elem.find(_ns("reportingPersonCIK"))).lstrip("0") or ""
    type_code = _xml_text(elem.find(_ns("typeOfReportingPerson")))
    citizenship_raw = _xml_text(elem.find(_ns("citizenshipOrOrganization")))
    citizenship_iso = _EDGAR_CITIZENSHIP_TO_ISO.get(citizenship_raw.upper(), "")

    def _float(tag_name: str) -> float | None:
        raw = _xml_text(elem.find(_ns(tag_name)))
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    return {
        "reporter_cik": reporter_cik,
        "name": name,
        "type_code": type_code,
        "citizenship_raw": citizenship_raw,
        "citizenship_iso": citizenship_iso,
        "is_individual": type_code in _INDIVIDUAL_CODES,
        "percent_of_class": _float("percentOfClass"),
        "sole_voting_power": _float("soleVotingPower"),
        "shared_voting_power": _float("sharedVotingPower"),
        "aggregate_amount_owned": _float("aggregateAmountOwned"),
    }


def _parse_13g_reporter_element(
    elem: ET.Element, ns: str
) -> dict[str, Any] | None:
    """Parse a 13G ``coverPageHeaderReportingPersonDetails`` element.

    The 13G schema differs from 13D: share counts are nested under
    ``reportingPersonBeneficiallyOwnedNumberOfShares``, the ownership
    percentage field is ``classPercent``, and the aggregate is
    ``reportingPersonBeneficiallyOwnedAggregateNumberOfShares``.
    """

    def ntag(tag: str) -> str:
        return f"{{{ns}}}{tag}"

    name = _xml_text(elem.find(ntag("reportingPersonName")))
    if not name:
        return None

    reporter_cik = _xml_text(elem.find(ntag("reportingPersonCIK"))).lstrip("0") or ""
    type_code = _xml_text(elem.find(ntag("typeOfReportingPerson")))
    citizenship_raw = _xml_text(elem.find(ntag("citizenshipOrOrganization")))
    citizenship_iso = _EDGAR_CITIZENSHIP_TO_ISO.get(citizenship_raw.upper(), "")

    def _float_direct(tag_name: str) -> float | None:
        el = elem.find(ntag(tag_name))
        raw = _xml_text(el)
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    # Voting / dispositive power is nested inside reportingPersonBeneficiallyOwnedNumberOfShares.
    shares_el = elem.find(ntag("reportingPersonBeneficiallyOwnedNumberOfShares"))

    def _float_nested(tag_name: str) -> float | None:
        parent = shares_el
        if parent is None:
            return None
        el = parent.find(ntag(tag_name))
        raw = _xml_text(el)
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    return {
        "reporter_cik": reporter_cik,
        "name": name,
        "type_code": type_code,
        "citizenship_raw": citizenship_raw,
        "citizenship_iso": citizenship_iso,
        "is_individual": type_code in _INDIVIDUAL_CODES,
        "percent_of_class": _float_direct("classPercent"),
        "sole_voting_power": _float_nested("soleVotingPower"),
        "shared_voting_power": _float_nested("sharedVotingPower"),
        "aggregate_amount_owned": _float_direct(
            "reportingPersonBeneficiallyOwnedAggregateNumberOfShares"
        ),
    }


# ----------------------------------------------------------------------
# Adapter
# ----------------------------------------------------------------------


class SecEdgarAdapter(SourceAdapter):
    """SEC EDGAR adapter for Schedule 13D/13G beneficial ownership filings."""

    id = "sec_edgar"

    def __init__(self) -> None:
        self._cache = Cache()
        # In-memory {normalised_title: cik} index built from company_tickers.json,
        # lazily populated on first resolve_cik() call.
        self._ticker_index: dict[str, str] | None = None

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="SEC EDGAR (Schedule 13D/13G)",
            homepage="https://www.sec.gov/search-filings",
            description=(
                "Major shareholders (>5 % beneficial owners) of US-listed companies "
                "from mandatory SEC Schedule 13D and 13G filings. Coverage is limited "
                "to XML filings submitted from December 2024 onward."
            ),
            license="Public Domain",
            attribution=(
                "SEC EDGAR — public domain, courtesy of the "
                "U.S. Securities and Exchange Commission."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        url = (
            f"{_BROWSE_BASE}?company={quote(query)}&CIK=&type="
            f"&dateb=&owner=include&count=20&search_text=&action=getcompany&output=atom"
        )
        # _get_text raises RuntimeError on 403/429/network failure so that
        # the /lookup endpoint captures it in errors["sec_edgar"] rather
        # than silently producing an empty hit list.
        atom_xml = await self._get_text(url, cache_key=cache_key)
        return _parse_company_hits_from_atom(atom_xml)

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="0000000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary="Stub result — set OPENCHECK_ALLOW_LIVE=true to search SEC EDGAR.",
                identifiers={"edgar_cik": "0000000000"},
                raw={"cik": "0000000000", "name": f"{query} (stub)"},
                is_stub=True,
            )
        ]

    # ------------------------------------------------------------------
    # CIK resolution (legal name → CIK)
    # ------------------------------------------------------------------

    async def _load_ticker_index(self) -> dict[str, str]:
        """Build (and cache) a {normalised_title: cik} index from
        ``company_tickers.json`` — the authoritative SEC map of every
        exchange-listed US issuer.

        Returns an empty dict if the file can't be retrieved (offline, no
        contact e-mail set, etc.).  When two titles normalise to the same
        key, the first wins.
        """
        if self._ticker_index is not None:
            return self._ticker_index

        cache_key = f"{_CACHE_NS}/company_tickers"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {}

        raw = await self._get_text(_TICKERS_URL, cache_key=cache_key)
        index: dict[str, str] = {}
        if raw:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = {}
            # Shape: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, …}
            rows = data.values() if isinstance(data, dict) else data
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = row.get("title") or ""
                cik_raw = row.get("cik_str")
                if title == "" or cik_raw is None:
                    continue
                key = _normalise_company_name(title)
                if key and key not in index:
                    index[key] = str(cik_raw).lstrip("0") or "0"
        self._ticker_index = index
        return index

    async def resolve_cik(self, legal_name: str) -> str | None:
        """Resolve a company legal name to its EDGAR CIK.

        Strategy:
        1. Exact normalised-name match against ``company_tickers.json``
           (authoritative for exchange-listed issuers — the 13D/13G universe).
        2. Fallback to the EDGAR company-search atom feed using the normalised
           name, selecting the candidate whose conformed name normalises to
           the same value (never a blind first-row pick).

        Returns the CIK (leading zeros stripped) or ``None`` if no confident
        match is found.
        """
        target = _normalise_company_name(legal_name)
        if not target:
            return None

        index = await self._load_ticker_index()
        if target in index:
            return index[target]

        # Fallback: normalised company-search, pick an exact normalised match.
        candidates = await self.search(target, SearchKind.ENTITY)
        for hit in candidates:
            if hit.is_stub:
                continue
            if _normalise_company_name(hit.name) == target:
                return hit.hit_id
        return None

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch 13D/13G filings for the subject company identified by CIK.

        ``hit_id`` is the EDGAR CIK for the subject (issuer) company.
        Returns a bundle dict with ``issuer_cik``, ``filings`` (list), and
        ``source_id``.  Each filing entry contains ``reporter``, ``issuer``,
        ``filing_url``, ``form_type``, and ``filed``.
        """
        cik = hit_id.strip().lstrip("0") or hit_id.strip()
        cache_key = f"{_CACHE_NS}/company/{cik}"

        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": cik, "is_stub": True}

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        # _fetch_filings_for_subject → _get_text raises RuntimeError on
        # HTTP errors so the caller sees a real error, not empty filings.
        filings, meta = await self._fetch_filings_for_subject(cik)
        result: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": cik,
            "issuer_cik": cik,
            "filings": filings,
            "legacy_filing_count": meta["legacy_filing_count"],
            "structured_filing_count": meta["structured_filing_count"],
            "latest_filing_date": meta["latest_filing_date"],
        }
        # When the issuer has 13D/13G filings but none in the machine-readable
        # era, explain the empty result instead of leaving a blank card.
        if not filings and meta["legacy_filing_count"]:
            result["coverage_note"] = (
                f"{meta['legacy_filing_count']} Schedule 13D/13G filing(s) found "
                f"for this issuer (most recent {meta['latest_filing_date']}), but all "
                f"predate the SEC's {_STRUCTURED_FROM} structured-data mandate, so no "
                f"machine-readable beneficial owners are available."
            )
        elif not filings:
            result["coverage_note"] = (
                "No Schedule 13D/13G filings found for this issuer since the SEC's "
                f"{_STRUCTURED_FROM} structured-data mandate."
            )
        validate_raw("sec_edgar", EDGARBundle, result)
        self._cache.put(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # Core filing retrieval logic
    # ------------------------------------------------------------------

    async def _fetch_filings_for_subject(
        self, subject_cik: str
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Retrieve and parse recent 13D/13G filings for a subject company.

        Uses the EDGAR filing-search atom feed (browse-edgar?action=getcompany&
        CIK=<cik>&type=SC+13D/13G&output=atom) to list filings where the given
        company is the issuer (subject).  One atom request is made per form type.

        Only filings on/after ``_STRUCTURED_FROM`` (the SEC structured-XML
        mandate) carry a ``primary_doc.xml`` from which beneficial owners can
        be parsed; older filings are counted but skipped (no HTTP fetch).

        Primary XML documents are archived under the filer's CIK (extracted from
        the atom entry link href), at the root of each accession directory:
            /Archives/edgar/data/{filer_cik}/{accession_nodashes}/primary_doc.xml

        Returns ``(records, meta)`` where ``meta`` carries filing counts and the
        latest filing date so the caller can explain an empty result.
        """
        raw_records: list[dict[str, Any]] = []
        legacy_count = 0
        structured_count = 0
        latest_filing_date = ""

        for form_type_param in ("SC+13D", "SC+13G"):
            atom_url = (
                f"{_BROWSE_BASE}?action=getcompany&CIK={subject_cik}"
                f"&type={form_type_param}&dateb=&owner=include"
                f"&count={_MAX_FILINGS}&search_text=&output=atom"
            )
            atom_cache_key = f"{_CACHE_NS}/filings/{subject_cik}/{form_type_param}"
            atom_text = await self._get_text(atom_url, cache_key=atom_cache_key)
            if not atom_text:
                continue

            refs = _parse_filing_refs_from_atom(atom_text)
            for ref in refs:
                filed = ref.get("filed") or ""
                if filed > latest_filing_date:
                    latest_filing_date = filed

                # Filings before the structured-XML mandate have no
                # primary_doc.xml — count them but don't waste a fetch.
                if filed and filed < _STRUCTURED_FROM:
                    legacy_count += 1
                    continue
                structured_count += 1

                filer_cik = ref.get("filer_cik") or subject_cik
                accession = ref["accession"]
                xml_url = (
                    f"{_EDGAR_BASE}/Archives/edgar/data/{filer_cik}"
                    f"/{accession}/primary_doc.xml"
                )
                xml_cache_key = f"{_CACHE_NS}/filing/{filer_cik}/{accession}"
                xml_text = await self._get_text(xml_url, cache_key=xml_cache_key)
                parsed = _parse_filing_xml(xml_text, source_url=xml_url)
                if not parsed:
                    continue

                for reporter in parsed.get("reporters") or []:
                    raw_records.append(
                        {
                            "reporter": reporter,
                            "issuer": parsed.get("issuer", {}),
                            "filer_cik": parsed.get("filer_cik", ""),
                            "filing_url": xml_url,
                            "form_type": ref["form_type"],
                            "filed": ref["filed"],
                        }
                    )

        # Deduplicate: per reporter CIK, keep the most-recently-dated filing.
        # Fall back to filer_cik (from headerData) for 13G filings that omit
        # reportingPersonCIK inside the reporter details element.
        best: dict[str, dict[str, Any]] = {}
        no_cik: list[dict[str, Any]] = []
        for rec in raw_records:
            reporter_cik = (
                (rec["reporter"] or {}).get("reporter_cik", "")
                or rec.get("filer_cik", "")
            )
            if not reporter_cik:
                no_cik.append(rec)
                continue
            prev = best.get(reporter_cik)
            if prev is None or rec["filed"] > prev["filed"]:
                best[reporter_cik] = rec

        records = list(best.values()) + no_cik
        meta = {
            "legacy_filing_count": legacy_count,
            "structured_filing_count": structured_count,
            "latest_filing_date": latest_filing_date,
        }
        return records, meta

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    def _edgar_headers(self) -> dict[str, str]:
        """Return HTTP headers that satisfy SEC EDGAR's fair-use policy.

        EDGAR requires a User-Agent that identifies the application and
        includes a contact e-mail so SEC staff can reach the operator if
        automated access causes problems.  Requests from cloud hosting IPs
        (such as Render) that omit a contact e-mail are silently blocked
        with 403.  See https://www.sec.gov/os/webmaster-faq#developers.
        """
        email = get_settings().edgar_contact_email
        return {
            "User-Agent": f"OpenCheck {email}",
            # The base httpx client sets Accept: application/json which causes
            # EDGAR to respond with HTML instead of atom+xml for company-search
            # endpoints.  Broadening the Accept header fixes this and works for
            # JSON (submissions API) and raw XML (primary_doc.xml) too.
            # Note: no Host header — we use both www.sec.gov and data.sec.gov.
            "Accept": (
                "application/json, application/atom+xml, "
                "text/xml, application/xml, */*"
            ),
            "Accept-Encoding": "gzip, deflate",
        }

    async def _get_text(self, url: str, *, cache_key: str) -> str:
        """Fetch any URL and return raw text; cache the result.

        Returns ``""`` on 404 or for optional resources (individual filing
        XMLs) that may not exist.  Raises ``RuntimeError`` on 403/429/5xx
        so callers can propagate the failure rather than silently producing
        empty results.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        try:
            async with build_client() as client:
                resp = await client.get(url, headers=self._edgar_headers())
                if resp.status_code == 404:
                    self._cache.put(cache_key, "")
                    return ""
                if resp.status_code == 403:
                    raise RuntimeError(
                        "SEC EDGAR returned 403 — check OPENCHECK_EDGAR_CONTACT_EMAIL "
                        "is set to a valid address in your environment"
                    )
                if resp.status_code == 429:
                    raise RuntimeError("SEC EDGAR rate-limited this request (429)")
                resp.raise_for_status()
                text = resp.text
        except RuntimeError:
            raise
        except Exception as exc:
            # Network-level failure (timeout, DNS, SSL) — treat as transient.
            raise RuntimeError(f"SEC EDGAR request failed: {exc}") from exc

        self._cache.put(cache_key, text)
        return text
