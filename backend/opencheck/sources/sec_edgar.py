"""SEC EDGAR adapter — Schedule 13D/13G beneficial ownership filings.

Surfaces major shareholders (>5 % beneficial owners) of US-listed companies
from the mandatory structured XML filings introduced on December 18 2024.

Search strategy
    EDGAR company-search atom feed (browse-edgar?company=<name>&output=atom)
    → one hit per matching subject company (the issuer), keyed by CIK.
    In practice the CIK is usually sourced directly from OpenCorporates data
    so the name-search fallback is rarely used.

Fetch strategy
    The browse-edgar filing feed for the company's CIK, once per structured
    form type — ``SCHEDULE 13D`` and ``SCHEDULE 13G`` (Phase 252: the feed's
    ``type=`` is a prefix match and the mandate renamed the forms, so the
    legacy ``SC 13D`` / ``SC 13G`` names never return a structured filing).
    For each accession, primary_doc.xml is fetched from the archive path in
    the feed entry:
        /Archives/edgar/data/{cik}/{accession_nodashes}/primary_doc.xml
    Results are deduplicated per reporter, retaining the most recent filing.

No API key is required — EDGAR is publicly accessible.  The User-Agent header
must identify the application and include a contact e-mail (set via the
OPENCHECK_EDGAR_CONTACT_EMAIL env var) or cloud-hosted requests will be
silently blocked with 403.  See https://www.sec.gov/os/webmaster-faq#developers

Coverage is limited to publicly-traded US companies with shareholders holding
>5 % of a registered equity class who have filed since December 18 2024.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote

from .. import names
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .edgar_codes import EDGAR_CODES, edgar_country
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
# Address fields (street1, city …) in both schedules are in this namespace.
_NS_COMMON = "http://www.sec.gov/edgar/common"

# Maximum filings retrieved per form type per subject company.
# Set to 40 so that even when a company has many self-filed 13G entries
# (the company as institutional investor) in the atom page, we still have
# capacity to reach third-party filings about the company after the
# directional filter discards the self-filed ones.
_MAX_FILINGS = 40

# SEC mandate date for machine-readable (structured XML) Schedule 13D/13G
# filings. Filings before this date have no primary_doc.xml, so no
# beneficial-owner data can be extracted from them.
# See https://www.sec.gov/rules/final/2024/33-11253.pdf
_STRUCTURED_FROM = "2024-12-18"

# EDGAR form types, as the ``type=`` parameter of the browse-edgar filing feed
# spells them. The filter is a PREFIX match on the form name, and the XML
# mandate RENAMED the forms: every structured filing is ``SCHEDULE 13D`` /
# ``SCHEDULE 13G`` (``/A`` for amendments), every legacy one ``SC 13D`` /
# ``SC 13G``. ``type=SC+13G`` therefore never returns a structured filing.
# Until Phase 252 the adapter asked only for the ``SC`` forms, so it found the
# legacy filings, skipped each one for having no XML, and answered "no
# record" for every US issuer — Moody's has three structured 13G filings
# (TCI / Christopher Hohn, Vanguard Capital Management, The Vanguard Group)
# and OpenCheck showed none. Verified against live EDGAR, 28 Sept 2026.
_STRUCTURED_FORM_TYPES: tuple[str, ...] = ("SCHEDULE+13D", "SCHEDULE+13G")
# Queried only to explain an empty result (the coverage note): legacy filings
# carry no primary_doc.xml, so nothing is ever parsed from them.
_LEGACY_FORM_TYPES: tuple[str, ...] = ("SC+13D", "SC+13G")

# How many days before a live-tier EDGAR cache entry is treated as stale.
# Institutional investors file SC 13G annual updates in January/February;
# a 7-day TTL ensures the first post-mandate filing season is picked up
# without hammering EDGAR on every request.
_CACHE_TTL_DAYS = 7

# Retry configuration for transient EDGAR errors (429 rate-limiting, 5xx
# outages — e.g. the 503s the fragile browse-edgar cgi-bin search endpoint
# occasionally returns). Kept short: a single lookup can make many
# sequential EDGAR requests (ticker index, per-form-type atom feeds, one
# primary_doc.xml per filing) inside one ~30s per-source time budget (see
# routers/lookup.py:_source_budget), so retries must not eat that budget on
# their own. Mirrors the established retry pattern in sources/kvk.py.
_MAX_RETRIES = 2
_RETRY_BACKOFF_BASE = 1.0  # seconds; doubled on each attempt
_RETRY_BACKOFF_MAX = 8.0  # cap, regardless of Retry-After or backoff
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# EDGAR citizenship/organisation codes → ISO 3166-1 alpha-2: the SEC's full
# state-and-country table since Phase 259 (``edgar_codes.py``). The table
# this replaced knew only X1, the states and X2, which it read as Canada —
# X2 is Burkina Faso — so TCI Fund Management and Christopher Hohn (X0,
# United Kingdom) carried no nationality at all.
_EDGAR_NAME_TO_CODE: dict[str, str] = {
    name.upper(): code for code, (_iso, name) in EDGAR_CODES.items()
}


def _edgar_citizenship(raw: str) -> tuple[str, str]:
    """``(iso_alpha2, name)`` for a ``citizenshipOrOrganization`` value.

    An EDGAR code first ("X0", "DE"); some filers write the SEC's name
    instead ("UNITED KINGDOM", "Delaware"), which is looked up in the same
    table. Anything else is ``("", "")`` — never a guess.
    """
    raw = (raw or "").strip()
    iso, name = edgar_country(raw)
    if iso or name:
        return iso, name
    code = _EDGAR_NAME_TO_CODE.get(raw.upper())
    return edgar_country(code) if code else ("", "")


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

# Spelled-out equivalent of "&" left dangling once a trailing legal-form
# suffix is stripped from names like "Eli Lilly and Company" (GLEIF spells
# the conjunction out; EDGAR's conformed name uses "&", which the shared
# punctuation-to-space fold already discards before this point, e.g.
# "ELI LILLY & Co" -> "ELI LILLY CO" -> "ELI LILLY"). Only stripped when it
# ends up trailing (see the loop below), so a mid-name conjunction like
# "Barnes and Noble" is untouched.
_TRAILING_CONNECTOR_TOKENS: frozenset[str] = frozenset({"AND"})


# Straight, curly and modifier apostrophes, and the backtick some filers use.
_APOSTROPHES = re.compile("['\u2018\u2019\u02bc`]")

# EDGAR's conformed names carry a trailing state / country / series tag —
# "MOODYS CORP /DE/", "ICU MEDICAL INC/DE", "COSTCO WHOLESALE CORP /NEW",
# "Gores Holdings X, Inc. / CI", "Spirax-Sarco Engineering PLC/ADR". 552 of
# the 8,004 titles in company_tickers.json (28 Sept 2026). Normalised, the tag
# became a trailing token that also stopped the legal-form strip, so none of
# those companies could be matched by name (Phase 254).
_EDGAR_TITLE_TAG = re.compile(r"^(?P<head>.*?)(?P<sep>\s*/\s*)(?P<tag>[A-Za-z]{2,8})\s*/?\s*$")


def _strip_edgar_title_tag(title: str) -> str:
    """Remove EDGAR's trailing ``/XX/`` tag from a conformed company name.

    Applied to EDGAR's names only, never to GLEIF's. The tag is removed only
    where the slash is plainly a separator — whitespace before it, or it
    follows punctuation or a legal-form token ("INC/DE", "Corp./CI",
    "PLC/ADR") — so a name whose slash is part of the name keeps it:
    "Cadeler A/S" (one-letter tag, never matched) and "DATA I/O CORP"
    (nothing trails) are untouched.
    """
    title = (title or "").strip()
    m = _EDGAR_TITLE_TAG.match(title)
    if not m:
        return title
    head = m.group("head")
    if not head.strip():
        return title
    separated = bool(m.group("sep")[:1].isspace()) or head[-1:] in ".,)"
    last = re.sub(r"[^A-Za-z]", "", head.split()[-1]).upper() if head.split() else ""
    if separated or last in _LEGAL_FORM_SUFFIXES:
        return head.rstrip(" ,")
    return title


def _edgar_title_key(title: str) -> str:
    """The match key for an EDGAR conformed name: tag stripped, then normalised."""
    return _normalise_company_name(_strip_edgar_title_tag(title))


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
        "Eli Lilly and Company"   -> "ELI LILLY"
        "ELI LILLY & Co"          -> "ELI LILLY"
    """
    # Phase C (rigour adoption): the shared fold pipeline supplies the base
    # form (diacritics, ø/æ/ß folds, Cyrillic/Greek transliteration) so a
    # GLEIF "MÜLLER" matches an EDGAR "MULLER". The trailing legal-form token
    # strip stays local: rigour's org-type data does not cover bare
    # "COMPANY"/"CO" (it would regress "THE WALT DISNEY COMPANY" ↔
    # "Walt Disney Co"). SWITCH POINT re-verified and KEPT 2026-08-01:
    # rigour 2.3.1 still leaves both "the walt disney company" and
    # "walt disney co" untouched — tests/test_names.py pins the gap as a
    # canary that fails when a future rigour covers it (then delete the
    # local strip; tests/test_sec_edgar_resolve.py pins the Disney match).
    # Apostrophes are dropped, never turned into a space (Phase 254): GLEIF
    # writes "MOODY'S CORPORATION" and EDGAR "MOODYS CORP", and the shared
    # fold's punctuation-to-space made the first "MOODY S" — no match.
    s = names.normalise_name(_APOSTROPHES.sub("", name or "")).upper()
    tokens = s.split()
    while tokens and tokens[0] == "THE":
        tokens = tokens[1:]
    while tokens and (
        tokens[-1] in _LEGAL_FORM_SUFFIXES
        or tokens[-1] in _TRAILING_CONNECTOR_TOKENS
    ):
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


def _us_date_to_iso(raw: str) -> str:
    """``MM/DD/YYYY`` (EDGAR's schedule dates) → ``YYYY-MM-DD``; else ``""``.

    An ISO date passes through unchanged. Anything else is dropped rather
    than guessed at.
    """
    raw = (raw or "").strip()
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        month, day, year = (int(g) for g in m.groups())
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    return ""


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

    # CUSIP. Every live filing read in Phase 252 (13D and 13G, X0202 schema)
    # nests it: issuerCusips/issuerCusipNumber — the path an earlier comment
    # here called wrong. The flat issuerCUSIP / issuerCusip spellings are kept
    # as fallbacks. No real filing had reached this parser before Phase 252
    # (see _STRUCTURED_FORM_TYPES), which is how the claim went unchecked.
    cusip_el = issuer_info.find(f"{ntag('issuerCusips')}/{ntag('issuerCusipNumber')}")
    if cusip_el is None:
        cusip_el = issuer_info.find(ntag("issuerCUSIP"))
    if cusip_el is None:
        cusip_el = issuer_info.find(ntag("issuerCusip"))
    issuer_cusip = _xml_text(cusip_el)

    # The date of the event that required this filing — 13D ``dateOfEvent``,
    # 13G ``eventDateRequiresFilingThisStatement``, both MM/DD/YYYY. For an
    # exit filing (nothing left held) it dates the end of the holding.
    event_date = _us_date_to_iso(
        _xml_text(cover_header.find(ntag("dateOfEvent")))
        or _xml_text(cover_header.find(ntag("eventDateRequiresFilingThisStatement")))
    )

    # filerCik — the entity that submitted this document
    # (headerData/filerInfo/filer/filerCredentials/cik).
    # When filerCik == issuerCik the subject company filed the 13D itself
    # (e.g. GameStop reporting its own stake in eBay); otherwise a third
    # party is reporting ownership of the subject company.
    filer_cik_el = root.find(f".//{ntag('filerCredentials')}/{ntag('cik')}")
    filer_cik = _xml_text(filer_cik_el).lstrip("0") if filer_cik_el is not None else ""

    # Address block — 13D ``address``, 13G
    # ``issuerPrincipalExecutiveOfficeAddress``. The fields inside are in the
    # EDGAR *common* namespace (``com:street1``), not the schedule's own; the
    # schedule namespace is kept as a fallback.
    addr_el = issuer_info.find(ntag("address"))
    if addr_el is None:
        addr_el = issuer_info.find(ntag("issuerPrincipalExecutiveOfficeAddress"))
    issuer_address: dict[str, str] = {}
    if addr_el is not None:
        for field in ("street1", "street2", "city", "stateOrCountry", "zipCode"):
            field_el = addr_el.find(f"{{{_NS_COMMON}}}{field}")
            if field_el is None:
                field_el = addr_el.find(ntag(field))
            val = _xml_text(field_el)
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
        "event_date": event_date,
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
    citizenship_iso, citizenship_name = _edgar_citizenship(citizenship_raw)

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
        "citizenship_name": citizenship_name,
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
    citizenship_iso, citizenship_name = _edgar_citizenship(citizenship_raw)

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
        "citizenship_name": citizenship_name,
        "is_individual": type_code in _INDIVIDUAL_CODES,
        "percent_of_class": _float_direct("classPercent"),
        "sole_voting_power": _float_nested("soleVotingPower"),
        "shared_voting_power": _float_nested("sharedVotingPower"),
        "aggregate_amount_owned": _float_direct(
            "reportingPersonBeneficiallyOwnedAggregateNumberOfShares"
        ),
    }


def _reporter_key(rec: dict[str, Any]) -> str:
    """Who a filing record is about, for keeping one filing per reporter.

    The reporter's own CIK when the filing gives one. Schedule 13G usually
    does not: it names each reporting person without a CIK. Before Phase 252
    the fallback was the *filer's* CIK alone, so a joint filing collapsed onto
    one reporter — TCI Fund Management and Christopher Hohn file together on
    Moody's, and Hohn was dropped. The fallback is now the filer's CIK plus the
    reporter's name, which keeps joint reporters apart and still lets a later
    amendment by the same filer replace an earlier one.
    """
    reporter = rec.get("reporter") or {}
    if reporter.get("reporter_cik"):
        return f"cik:{reporter['reporter_cik']}"
    # The name through the company-name normaliser (Phase 254): the same
    # filer spells itself "JPMORGAN CHASE & CO" in one 13G and
    # "JPMORGAN CHASE & CO." in its amendment (McDonald's, 2026), and a
    # punctuation difference kept the superseded 5.1% beside the current 4.3%.
    name = _normalise_company_name(reporter.get("name") or "")
    return f"filer:{rec.get('filer_cik') or ''}:{name}"


def _latest_per_reporter(raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep each reporter's most recently filed record, in first-seen order."""
    best: dict[str, dict[str, Any]] = {}
    for rec in raw_records:
        key = _reporter_key(rec)
        prev = best.get(key)
        if prev is None or (rec.get("filed") or "") > (prev.get("filed") or ""):
            best[key] = rec
    return list(best.values())


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
        contact e-mail set, etc.).  A key that two different CIKs share is
        left out (Phase 254) rather than given to whichever came first.
        """
        if self._ticker_index is not None:
            return self._ticker_index

        cache_key = f"{_CACHE_NS}/company_tickers"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {}

        raw = await self._get_text(_TICKERS_URL, cache_key=cache_key)
        ciks: dict[str, set[str]] = {}
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
                key = _edgar_title_key(title)
                if key:
                    ciks.setdefault(key, set()).add(str(cik_raw).lstrip("0") or "0")
        # A key two issuers share is not an answer: "FIRST BANCORP /NC/" and
        # "FIRST BANCORP /PR/" both key to "FIRST BANCORP", and "Toro Co" /
        # "Toro Corp" to "TORO". Until Phase 254 the first title in the file
        # won, which could hand a lookup another company's filings. An
        # ambiguous key is left out, and resolution falls through to the
        # company search (which must also agree exactly).
        index = {key: next(iter(found)) for key, found in ciks.items() if len(found) == 1}
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
        matches = {
            hit.hit_id
            for hit in candidates
            if not hit.is_stub and _edgar_title_key(hit.name) == target
        }
        # Exactly one company, or no answer — the same rule as the index.
        return next(iter(matches)) if len(matches) == 1 else None

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
            by_note = (
                f"  ({meta['filing_by_count']} filing(s) made by this company "
                f"as an investor in other companies were excluded.)"
                if meta.get("filing_by_count")
                else ""
            )
            result["coverage_note"] = (
                f"{meta['legacy_filing_count']} Schedule 13D/13G filing(s) found "
                f"for this issuer (most recent {meta['latest_filing_date']}), but all "
                f"predate the SEC's {_STRUCTURED_FROM} structured-data mandate, so no "
                f"machine-readable beneficial owners are available.{by_note}"
            )
        elif not filings:
            by_note = (
                f"  ({meta['filing_by_count']} filing(s) made by this company "
                f"as an investor in other companies were excluded.)"
                if meta.get("filing_by_count")
                else ""
            )
            result["coverage_note"] = (
                "No Schedule 13D/13G filings found for this issuer since the SEC's "
                f"{_STRUCTURED_FROM} structured-data mandate.{by_note}"
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
        """Retrieve and parse the structured 13D/13G filings about a company.

        Uses the EDGAR filing-search atom feed (browse-edgar?action=getcompany&
        CIK=<cik>&type=<form>&output=atom) once per structured form type —
        ``SCHEDULE 13D`` and ``SCHEDULE 13G`` (see ``_STRUCTURED_FORM_TYPES``:
        the ``type=`` filter is a prefix match, and the XML mandate renamed
        the forms, so asking for ``SC 13G`` never returns a structured filing).

        Only when no structured filing about the subject survives are the
        legacy ``SC 13D`` / ``SC 13G`` feeds read, and then only to count them
        for the coverage note: legacy filings have no ``primary_doc.xml``.

        Primary XML documents are fetched from the archive path in the atom
        entry's link, at the root of each accession directory:
            /Archives/edgar/data/{filer_cik}/{accession_nodashes}/primary_doc.xml

        Returns ``(records, meta)`` where ``meta`` carries filing counts and the
        latest filing date so the caller can explain an empty result.
        ``legacy_filing_count`` is ``None`` when the legacy feeds were not read.
        """
        raw_records: list[dict[str, Any]] = []
        structured_count = 0
        filing_by_count = 0   # filings BY the subject (not about it) — discarded
        latest_filing_date = ""

        for form_type_param in _STRUCTURED_FORM_TYPES:
            for ref in await self._filing_refs(subject_cik, form_type_param):
                # A legacy form cannot carry XML, whatever its date.
                if ref.get("form_type", "").upper().startswith("SC "):
                    continue
                structured_count += 1

                filer_cik = ref.get("filer_cik") or subject_cik
                accession = ref["accession"]
                xml_url = (
                    f"{_EDGAR_BASE}/Archives/edgar/data/{filer_cik}"
                    f"/{accession}/primary_doc.xml"
                )
                # Individual filing XMLs are immutable — no TTL needed.
                xml_cache_key = f"{_CACHE_NS}/filing/{filer_cik}/{accession}"
                xml_text = await self._get_text(xml_url, cache_key=xml_cache_key)
                parsed = _parse_filing_xml(xml_text, source_url=xml_url)
                if not parsed:
                    continue

                # --- Directional filter ---
                # The EDGAR getcompany atom for a given CIK includes filings
                # ABOUT that company (where it is the issuer/subject) AND
                # filings BY that company (where it is the reporting investor).
                # We only want filings where the subject company is the issuer,
                # i.e. third parties reporting their >5 % stake in it.
                issuer_cik_in_xml = (parsed.get("issuer") or {}).get("cik", "")
                if issuer_cik_in_xml and issuer_cik_in_xml != subject_cik:
                    filing_by_count += 1
                    continue

                filed = ref.get("filed") or ""
                if filed > latest_filing_date:
                    latest_filing_date = filed

                reporters = parsed.get("reporters") or []
                for reporter in reporters:
                    raw_records.append(
                        {
                            "reporter": reporter,
                            # Phase 259: the other reporting persons on the
                            # same filing. A joint filing's cover pages can
                            # each report the SAME shares (TCI Fund
                            # Management and Christopher Hohn, 8.21% each,
                            # of one Moody's stake), so the mapper says so
                            # rather than let two 8.21% edges read as 16.42%.
                            "joint_with": [
                                other.get("name") or ""
                                for other in reporters
                                if other is not reporter and other.get("name")
                            ],
                            "issuer": parsed.get("issuer", {}),
                            "filer_cik": parsed.get("filer_cik", ""),
                            "filing_url": xml_url,
                            "form_type": ref["form_type"],
                            "filed": filed,
                            "event_date": parsed.get("event_date") or "",
                        }
                    )

        records = _latest_per_reporter(raw_records)

        legacy_count: int | None = None
        if not records:
            legacy_count = 0
            for form_type_param in _LEGACY_FORM_TYPES:
                for ref in await self._filing_refs(subject_cik, form_type_param):
                    if not ref.get("form_type", "").upper().startswith("SC "):
                        continue
                    legacy_count += 1
                    filed = ref.get("filed") or ""
                    if filed > latest_filing_date:
                        latest_filing_date = filed

        meta = {
            "legacy_filing_count": legacy_count,
            "structured_filing_count": structured_count,
            "filing_by_count": filing_by_count,
            "latest_filing_date": latest_filing_date,
        }
        return records, meta

    async def _filing_refs(
        self, subject_cik: str, form_type_param: str
    ) -> list[dict[str, str]]:
        """One browse-edgar filing feed for *subject_cik*, parsed to refs."""
        atom_url = (
            f"{_BROWSE_BASE}?action=getcompany&CIK={subject_cik}"
            f"&type={form_type_param}&dateb=&owner=include"
            f"&count={_MAX_FILINGS}&search_text=&output=atom"
        )
        atom_cache_key = f"{_CACHE_NS}/filings/{subject_cik}/{form_type_param}"
        # Filing-list atom feeds are mutable (new filings arrive); apply TTL.
        atom_text = await self._get_text(
            atom_url, cache_key=atom_cache_key, max_age_days=_CACHE_TTL_DAYS
        )
        return _parse_filing_refs_from_atom(atom_text) if atom_text else []

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

    async def _get_text(
        self, url: str, *, cache_key: str, max_age_days: float | None = None
    ) -> str:
        """Fetch any URL and return raw text; cache the result.

        ``max_age_days`` — when set, passes through to ``Cache.get_payload``
        so that live-tier entries older than this many days are treated as a
        miss and re-fetched.  Pass ``_CACHE_TTL_DAYS`` for mutable resources
        (filing-list atom feeds); leave ``None`` for immutable ones (individual
        filing XMLs which never change after submission).

        Transient failures (429 rate-limiting, 5xx outages) are retried with
        exponential backoff — honouring ``Retry-After`` when EDGAR sends one
        — up to ``_MAX_RETRIES`` times before giving up.

        Returns ``""`` on 404 or for optional resources (individual filing
        XMLs) that may not exist.  Raises ``RuntimeError`` on 403, or on
        429/5xx once retries are exhausted, so callers can propagate the
        failure rather than silently producing empty results.
        """
        cached = self._cache.get_payload(cache_key, max_age_days=max_age_days)
        if cached is not None:
            return cached[0]

        try:
            async with build_client() as client:
                resp = None
                delay = _RETRY_BACKOFF_BASE
                for attempt in range(_MAX_RETRIES + 1):
                    resp = await client.get(url, headers=self._edgar_headers())
                    if resp.status_code not in _RETRYABLE_STATUS_CODES:
                        break
                    if attempt == _MAX_RETRIES:
                        # Exhausted retries — fall through and let the
                        # status-code checks below surface the failure.
                        break
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after is not None:
                        try:
                            wait = min(float(retry_after), _RETRY_BACKOFF_MAX)
                        except ValueError:
                            wait = delay
                    else:
                        wait = min(delay, _RETRY_BACKOFF_MAX)
                    await asyncio.sleep(wait)
                    delay *= 2

                if resp.status_code == 404:
                    self._cache.put(cache_key, "")
                    return ""
                if resp.status_code == 403:
                    raise RuntimeError(
                        "SEC EDGAR returned 403 — check OPENCHECK_EDGAR_CONTACT_EMAIL "
                        "is set to a valid address in your environment"
                    )
                if resp.status_code == 429:
                    raise RuntimeError(
                        "SEC EDGAR rate-limited this request (429) after "
                        f"{_MAX_RETRIES} retries"
                    )
                resp.raise_for_status()
                text = resp.text
        except RuntimeError:
            raise
        except Exception as exc:
            # Network-level failure (timeout, DNS, SSL) or an unretried 5xx —
            # treat as transient.
            raise RuntimeError(f"SEC EDGAR request failed: {exc}") from exc

        self._cache.put(cache_key, text)
        return text
