"""SEC EDGAR adapter — Schedule 13D/13G beneficial ownership filings.

Surfaces major shareholders (>5 % beneficial owners) of US-listed companies
from the mandatory structured XML filings that became the standard from
December 18 2024 onward (form types "SCHEDULE 13D" / "SCHEDULE 13G").

Search strategy
    EDGAR company-search atom feed (browse-edgar?company=<name>&output=atom)
    → one hit per matching subject company (the issuer), keyed by CIK.

Fetch strategy
    browse-edgar filings atom for both SCHEDULE 13D and SCHEDULE 13G
    → parse primary_doc.xml for each filing
    → deduplicate per reporting-person CIK, retaining the most recent filing.

No API key is required — EDGAR is publicly accessible; the User-Agent header
already set by ``http.build_client()`` satisfies EDGAR's fair-access policy.

Coverage is limited to publicly-traded US companies whose shareholders hold
>5 % of a registered equity class and have filed since December 18 2024.
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

_EDGAR_BASE = "https://www.sec.gov"
_BROWSE_BASE = f"{_EDGAR_BASE}/cgi-bin/browse-edgar"
_SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
_CACHE_NS = "sec_edgar"
_NS_13D = "http://www.sec.gov/edgar/schedule13D"
_NS_13G = "http://www.sec.gov/edgar/schedule13g"  # lowercase g — different schema
_NS_ATOM = "http://www.w3.org/2005/Atom"

# Maximum filings retrieved per form type per subject company.
_MAX_FILINGS = 20

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

# Extracts (filer_cik, accession_nodashes) from EDGAR archive hrefs.
# Matches: /Archives/edgar/data/{cik}/{accession}/
_ARCHIVE_URL_RE = re.compile(r"/Archives/edgar/data/(\d+)/(\d+)/")


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


def _parse_accession_link(href: str) -> tuple[str, str]:
    """Extract (filer_cik, accession_nodashes) from an EDGAR archive href.

    Returns ("", "") if the URL does not match the expected pattern.
    """
    m = _ARCHIVE_URL_RE.search(href)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


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
    """Parse EDGAR filings atom → list of filing references.

    Each reference is a dict with keys:
    ``filer_cik``, ``accession`` (no dashes), ``form_type``, ``filed``.
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
        link_el = entry.find("atom:link", ns)
        if link_el is None:
            continue
        href = link_el.get("href", "")
        filer_cik, accession = _parse_accession_link(href)
        if not filer_cik or not accession:
            continue

        category_el = entry.find("atom:category", ns)
        form_type = category_el.get("term", "") if category_el is not None else ""

        updated_el = entry.find("atom:updated", ns)
        filed = _xml_text(updated_el)[:10] if updated_el is not None else ""

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

    cusip_el = issuer_info.find(f".//{ntag('issuerCusipNumber')}")
    issuer_cusip = _xml_text(cusip_el) if cusip_el is not None else ""

    issuer: dict[str, Any] = {
        "cik": issuer_cik,
        "name": issuer_name,
        "cusip": issuer_cusip,
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

    return {"issuer": issuer, "reporters": reporters, "source_url": source_url}


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
        filings = await self._fetch_filings_for_subject(cik)
        result: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": cik,
            "issuer_cik": cik,
            "filings": filings,
        }
        self._cache.put(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # Core filing retrieval logic
    # ------------------------------------------------------------------

    async def _fetch_filings_for_subject(
        self, subject_cik: str
    ) -> list[dict[str, Any]]:
        """Retrieve and parse recent 13D/13G filings for a subject company.

        Uses the EDGAR submissions index (data.sec.gov/submissions/CIK{n}.json)
        to list all 13D/13G filings associated with the company — both where it
        is the issuer (subject) and where it is the reporting person (filer).
        Only filings from 18 December 2024 onward are fetched (mandatory XML era).

        Primary XML documents are archived under the subject company's own CIK,
        not under the filer's CIK, at the root of each accession directory.

        After parsing, results are deduplicated per reporter CIK, retaining the
        most-recently-dated filing per reporter.
        """
        padded_cik = subject_cik.zfill(10)
        submissions_url = f"{_SUBMISSIONS_BASE}/CIK{padded_cik}.json"
        sub_cache_key = f"{_CACHE_NS}/submissions/{subject_cik}"
        submissions_text = await self._get_text(submissions_url, cache_key=sub_cache_key)
        if not submissions_text:
            return []

        try:
            submissions_data = json.loads(submissions_text)
        except (json.JSONDecodeError, ValueError):
            return []

        recent = submissions_data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])

        # Collect 13D/13G entries filed since XML became mandatory.
        eligible: list[dict[str, str]] = []
        for i, form in enumerate(forms):
            if ("13D" in form or "13G" in form) and dates[i] >= "2024-12-18":
                eligible.append(
                    {
                        "accession": accessions[i],
                        "form_type": form,
                        "filed": dates[i],
                    }
                )

        raw_records: list[dict[str, Any]] = []
        for ref in eligible[:_MAX_FILINGS]:
            accession_nodashes = ref["accession"].replace("-", "")
            # EDGAR archives primary_doc.xml under the subject (issuer) company's
            # CIK, regardless of which entity filed the document.
            xml_url = (
                f"{_EDGAR_BASE}/Archives/edgar/data/{subject_cik}"
                f"/{accession_nodashes}/primary_doc.xml"
            )
            xml_cache_key = f"{_CACHE_NS}/filing/{subject_cik}/{accession_nodashes}"
            xml_text = await self._get_text(xml_url, cache_key=xml_cache_key)
            parsed = _parse_filing_xml(xml_text, source_url=xml_url)
            if not parsed:
                continue

            for reporter in parsed.get("reporters") or []:
                raw_records.append(
                    {
                        "reporter": reporter,
                        "issuer": parsed.get("issuer", {}),
                        "filing_url": xml_url,
                        "form_type": ref["form_type"],
                        "filed": ref["filed"],
                    }
                )

        # Deduplicate: per reporter CIK, keep the most-recently-dated filing.
        best: dict[str, dict[str, Any]] = {}
        no_cik: list[dict[str, Any]] = []
        for rec in raw_records:
            reporter_cik = (rec["reporter"] or {}).get("reporter_cik", "")
            if not reporter_cik:
                no_cik.append(rec)
                continue
            prev = best.get(reporter_cik)
            if prev is None or rec["filed"] > prev["filed"]:
                best[reporter_cik] = rec

        return list(best.values()) + no_cik

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
