"""TED — Tenders Electronic Daily (EU public procurement notices) adapter.

TED (https://ted.europa.eu/) is the official EU public procurement journal
(Supplement to the Official Journal, OJ S). Since the eForms rollout
(mandatory 25 Oct 2023; last legacy TED-XML notices Jan 2024), notices carry
structured organisation identifiers (eForms BT-501), which makes an
identifier-keyed "has this entity won EU public contracts?" lookup possible.

This adapter answers that question live, with two calls:

* ``POST https://api.ted.europa.eu/v3/notices/search`` — expert-search query
  ``organisation-identifier-tenderer IN (...)`` over the LEI **and** the
  national registration number(s) OpenCheck derives from the GLEIF anchor.
* ``GET  https://ted.europa.eu/en/notice/{pub}/xml`` — per-notice eForms XML,
  used to confirm *winner* vs *losing tenderer* via the ``efac:NoticeResult``
  chain (LotResult → LotTender → TenderingParty → Tenderer → Organization →
  ``cbc:CompanyID``).

Hard-won constraints (all verified live against production, 2026-08-03 —
see the "EU TED eForms" Notion ticket for the full investigation):

* **The LEI fill rate in BT-501 is currently zero** (0 LEIs in 5,031
  organisation identifiers sampled from July-2026 notices). The LEI is still
  always included in the query — zero cost, and the lookup self-upgrades as
  LEI adoption in eForms grows. Recall comes from GLEIF ``registeredAs``.
* Identifier fields support exact ``=`` / ``IN`` only — the ``~`` (contains)
  operator returns HTTP 400 ``QUERY_UNSUPPORTED_FIELD_OPERATION``. Query the
  GLEIF-verbatim form (Siemens matches ``"HRB 6684"`` *with* the space) plus
  a whitespace/punctuation-stripped variant.
* ``scope`` must be ``ALL`` (the default): ``ACTIVE``/``LATEST`` exclude
  contract-award notices entirely.
* ``SORT BY publication-date DESC`` is valid; there is no ``ASC`` keyword.
* Coverage is the eForms era only (effectively 2024+). "No TED hits" means
  "no eForms-era hits", not "never won anything" — the frontend card must
  keep saying so.
* French notices use SIREN and SIRET inconsistently for the same company
  (disjoint result sets); with SIREN-only keys some FR notices are missed.

Authentication: none — the Search API and notice XML are anonymous/key-free.
Fair-usage quota: 700 requests/min/IP (far above per-lookup needs).
License: procurement notices in OJ S are freely reusable, for commercial or
  non-commercial purposes, under Commission Decision 2011/833/EU (with source
  acknowledgement); TED metadata is CC0.
API reference: https://docs.ted.europa.eu/api/latest/index.html
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.ted_eu import TedEuBundle

log = logging.getLogger(__name__)

_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"
_NOTICE_XML_URL = "https://ted.europa.eu/en/notice/{pub}/xml"
_NOTICE_HTML_URL = "https://ted.europa.eu/en/notice/-/detail/{pub}"
_CACHE_NS = "ted_eu"

#: Max notices returned by the search (newest first).
_SEARCH_LIMIT = 25
#: How many of the newest notices get a per-notice XML winner confirmation.
_MAX_XML_CONFIRM = 10
#: Cap on the identifier IN() set.
_MAX_IDENTIFIERS = 8

#: Jurisdictions whose entities can plausibly appear in TED notices: EU-27 +
#: EEA (IS, LI, NO) + GB and CH (frequent cross-border participants). Entities
#: elsewhere can in principle win EU contracts too, but the identifier the
#: buyer records for them is unpredictable — revisit if evidence shows misses.
TED_JURISDICTIONS: frozenset[str] = frozenset(
    {
        "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
        "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
        "RO", "SK", "SI", "ES", "SE",
        "IS", "LI", "NO",
        "GB", "CH",
    }
)

#: Search-result fields (all verified live 2026-08-03). ``links`` is returned
#: automatically and must not be requested explicitly.
_FIELDS = [
    "publication-number",
    "publication-date",
    "notice-title",
    "notice-type",
    "buyer-name",
    "buyer-country",
    "total-value",
    "total-value-cur",
    "classification-cpv",
    "winner-selection-status",
    "organisation-name-tenderer",
    "organisation-identifier-tenderer",
    "contract-conclusion-date",
]

_STRIP_RE = re.compile(r"[\s.\-]")
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]")


def _clean(value: str) -> str:
    """Sanitise an identifier for embedding in a quoted expert-query term."""
    return (value or "").replace('"', "").strip()


def _norm(value: str) -> str:
    """Comparison form: uppercase, alphanumerics only (``HRB 6684``→``HRB6684``)."""
    return _NON_ALNUM_RE.sub("", (value or "").upper())


def build_identifier_set(
    lei: str, registered_as: str, derived: dict[str, str] | None = None
) -> list[str]:
    """Ordered, deduplicated identifier values for the ``IN ()`` query.

    Always starts with the LEI (future-proofing — see module docstring), then
    the GLEIF ``registeredAs`` verbatim (TED matches the exact stored string,
    spaces included), then a separator-stripped variant, then any derived
    national identifiers whose value differs from what is already queued.
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        value = _clean(value)
        if value and value not in seen and len(out) < _MAX_IDENTIFIERS:
            out.append(value)
            seen.add(value)

    add(lei)
    add(registered_as)
    if registered_as:
        add(_STRIP_RE.sub("", _clean(registered_as)))
    for key, value in (derived or {}).items():
        if key == "lei":
            continue
        add(value)
    return out


def _slug(parts: list[str]) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _lang_pick(value: Any) -> str:
    """Pick a display string from a TED multilingual field.

    Multilingual fields come back as ``{"eng": [...] | "...", "fra": ...}``.
    Prefer English, fall back to the first language present.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and value:
        chosen = value.get("eng")
        if chosen is None:
            chosen = next(iter(value.values()))
        if isinstance(chosen, list):
            chosen = chosen[0] if chosen else ""
        return str(chosen or "")
    if isinstance(value, list) and value:
        return str(value[0])
    return ""


def _date_only(value: Any) -> str:
    """``2025-07-03+02:00`` → ``2025-07-03`` (TED dates carry a UTC offset)."""
    if isinstance(value, list):
        value = value[0] if value else ""
    text = str(value or "")
    return text.split("+")[0].split("T")[0] if text else ""


# ---------------------------------------------------------------------------
# eForms notice-XML winner-chain parsing (namespace-agnostic)
# ---------------------------------------------------------------------------
# The chain: LotResult (TenderResultCode selec-w) → LotTender → TenderingParty
# → Tenderer (ORG-XXXX) → Organization → Company/PartyLegalEntity/CompanyID.
# Parsed by *local* element names so eForms SDK namespace-URI revisions don't
# break it. Reference elements and full definitions of the same id are merged.


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(el: ET.Element, *path: str) -> str:
    """Text of the first descendant matching a chain of local names."""
    current = [el]
    for name in path:
        nxt: list[ET.Element] = []
        for node in current:
            nxt.extend(c for c in node if _local(c.tag) == name)
        if not nxt:
            return ""
        current = nxt
    return (current[0].text or "").strip()


def parse_notice_xml(xml_text: str, targets: list[str]) -> dict[str, Any] | None:
    """Extract the winner chain for the target identifiers from a notice XML.

    ``targets`` are raw identifier values; matching is done on normalised
    forms (:func:`_norm`). Returns ``None`` when the XML cannot be parsed;
    otherwise a dict with ``role`` (``"won"`` / ``"tendered"`` / ``"unknown"``),
    per-lot award details and the CompanyID values that matched.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.warning("TED notice XML parse failed: %s", exc)
        return None

    target_norms = {_norm(t) for t in targets if _norm(t)}
    if not target_norms:
        return None

    matched_orgs: set[str] = set()
    matched_ids: list[str] = []
    tpa_orgs: dict[str, set[str]] = {}
    lot_tenders: dict[str, dict[str, Any]] = {}
    lot_results: list[dict[str, Any]] = []
    contracts: dict[str, dict[str, str]] = {}

    for el in root.iter():
        name = _local(el.tag)
        if name == "Organization":
            org_id = _child_text(el, "Company", "PartyIdentification", "ID")
            if not org_id:
                continue
            for company in (c for c in el if _local(c.tag) == "Company"):
                for legal in (c for c in company if _local(c.tag) == "PartyLegalEntity"):
                    for cid in (c for c in legal if _local(c.tag) == "CompanyID"):
                        value = (cid.text or "").strip()
                        if value and _norm(value) in target_norms:
                            matched_orgs.add(org_id)
                            if value not in matched_ids:
                                matched_ids.append(value)
        elif name == "TenderingParty":
            tpa_id = _child_text(el, "ID")
            tenderers = {
                _child_text(t, "ID")
                for t in el
                if _local(t.tag) == "Tenderer"
            }
            tenderers.discard("")
            if tpa_id and tenderers:
                tpa_orgs.setdefault(tpa_id, set()).update(tenderers)
        elif name == "LotTender":
            ten_id = _child_text(el, "ID")
            if not ten_id:
                continue
            entry = lot_tenders.setdefault(ten_id, {})
            tpa = _child_text(el, "TenderingParty", "ID")
            if tpa:
                entry["tpa"] = tpa
            amount_el = next(
                (
                    a
                    for total in el
                    if _local(total.tag) == "LegalMonetaryTotal"
                    for a in total
                    if _local(a.tag) == "PayableAmount"
                ),
                None,
            )
            if amount_el is not None and (amount_el.text or "").strip():
                entry["amount"] = amount_el.text.strip()
                entry["currency"] = amount_el.get("currencyID", "")
            lot = _child_text(el, "TenderLot", "ID")
            if lot:
                entry["lot"] = lot
        elif name == "LotResult":
            status = _child_text(el, "TenderResultCode")
            ten_id = _child_text(el, "LotTender", "ID")
            lot = _child_text(el, "TenderLot", "ID")
            con_id = _child_text(el, "SettledContract", "ID")
            if status or ten_id:
                lot_results.append(
                    {"status": status, "tender": ten_id, "lot": lot, "contract": con_id}
                )
        elif name == "SettledContract":
            con_id = _child_text(el, "ID")
            if not con_id:
                continue
            entry = contracts.setdefault(con_id, {})
            award_date = _child_text(el, "AwardDate")
            if award_date:
                entry["award_date"] = _date_only(award_date)
            ref = _child_text(el, "ContractReference", "ID")
            if ref:
                entry["contract_reference"] = ref

    lots_won: list[str] = []
    awarded_values: list[dict[str, str]] = []
    award_dates: list[str] = []
    contract_references: list[str] = []
    tendered = False

    def _is_ours(ten_id: str) -> bool:
        tpa = lot_tenders.get(ten_id, {}).get("tpa", "")
        return bool(tpa_orgs.get(tpa, set()) & matched_orgs)

    for ten_id, entry in lot_tenders.items():
        if _is_ours(ten_id):
            tendered = True

    for result in lot_results:
        ten_id = result["tender"]
        if not ten_id or not _is_ours(ten_id):
            continue
        if result["status"] == "selec-w":
            lot = result["lot"] or lot_tenders.get(ten_id, {}).get("lot", "")
            if lot and lot not in lots_won:
                lots_won.append(lot)
            entry = lot_tenders.get(ten_id, {})
            if entry.get("amount"):
                awarded_values.append(
                    {"amount": entry["amount"], "currency": entry.get("currency", "")}
                )
            contract = contracts.get(result["contract"], {})
            if contract.get("award_date"):
                award_dates.append(contract["award_date"])
            if contract.get("contract_reference"):
                contract_references.append(contract["contract_reference"])

    if lots_won:
        role = "won"
    elif tendered:
        role = "tendered"
    else:
        # The flat search index matched, but the XML chain didn't resolve
        # (e.g. structure drift) — keep the notice, mark it unconfirmed.
        role = "unknown"

    return {
        "role": role,
        "lots_won": lots_won,
        "awarded_values": awarded_values,
        "award_dates": award_dates,
        "contract_references": contract_references,
        "matched_company_ids": matched_ids,
    }


class TedEuAdapter(SourceAdapter):
    """Source adapter for TED — Tenders Electronic Daily (EU procurement)."""

    id = "ted_eu"

    # No lookup_derivers: dispatched from _dispatch() like eiti, keyed on the
    # GLEIF anchor's (lei, registeredAs, jurisdiction) rather than an RA code,
    # so it applies to any LEI holder in a TED-relevant jurisdiction.

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="TED — Tenders Electronic Daily (EU procurement)",
            homepage="https://ted.europa.eu/",
            description=(
                "EU public procurement notices from Tenders Electronic Daily "
                "(OJ S): contract awards where the entity appears as a "
                "tenderer or winner, matched on eForms organisation "
                "identifiers (eForms era, ≈2024 onwards)."
            ),
            license=(
                "EU open data — Commission Decision 2011/833/EU "
                "(free reuse incl. commercial, with attribution)"
            ),
            attribution=(
                "Contains procurement notice data from Tenders Electronic "
                "Daily (TED), © European Union, via ted.europa.eu."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed source; free-text search intentionally empty
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Identifier-keyed lookup (called by the lookup pipeline)
    # ------------------------------------------------------------------

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        jurisdiction: str,
        derived: dict[str, str] | None = None,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """Search TED for award notices naming any of the entity's identifiers.

        Returns ``None`` when live mode is off, the jurisdiction is outside
        TED's plausible coverage, or no identifiers are available. Otherwise
        returns the bundle (which may legitimately have ``total_notice_count``
        of 0 — the hit builder drops the card in that case, per the
        "absence isn't a hit" rule).
        """
        if not self.info.live_available:
            return None
        country = (jurisdiction or "").split("-")[0].strip().upper()
        if country and country not in TED_JURISDICTIONS:
            return None
        identifiers = build_identifier_set(lei, registered_as, derived)
        if not identifiers:
            return None
        return await self._build_bundle(identifiers, legal_name=legal_name, lei=lei)

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Re-fetch by hit id (deepen / per-source retry path).

        ``hit_id`` is the ``|``-joined identifier set from the original hit.
        """
        identifiers = [i for i in (hit_id or "").split("|") if _clean(i)]
        if not identifiers or not self.info.live_available:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        lei = next((i for i in identifiers if len(_clean(i)) == 20), "")
        bundle = await self._build_bundle(identifiers, legal_name=legal_name, lei=lei)
        return bundle

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_bundle(
        self, identifiers: list[str], *, legal_name: str = "", lei: str = ""
    ) -> dict[str, Any]:
        payload = await self._search_notices(identifiers)
        total = int(payload.get("totalNoticeCount") or 0)
        raw_notices = payload.get("notices") or []

        confirmations: list[dict[str, Any] | None] = []
        if raw_notices:
            confirmations = await asyncio.gather(
                *[
                    self._confirm_notice(n, identifiers)
                    for n in raw_notices[:_MAX_XML_CONFIRM]
                ]
            )

        notices: list[dict[str, Any]] = []
        for i, raw in enumerate(raw_notices):
            pub = str(raw.get("publication-number") or "")
            confirmation = confirmations[i] if i < len(confirmations) else None
            cpv_list = raw.get("classification-cpv") or []
            if isinstance(cpv_list, str):
                cpv_list = [cpv_list]
            cpv: list[str] = []
            for code in cpv_list:
                if code and code not in cpv:
                    cpv.append(str(code))
            buyer_country = raw.get("buyer-country") or []
            notice: dict[str, Any] = {
                "publication_number": pub,
                "publication_date": _date_only(raw.get("publication-date")),
                "notice_type": str(raw.get("notice-type") or ""),
                "title": _lang_pick(raw.get("notice-title")),
                "buyer_name": _lang_pick(raw.get("buyer-name")),
                "buyer_country": str(
                    buyer_country[0] if isinstance(buyer_country, list) and buyer_country
                    else buyer_country or ""
                ),
                "total_value": raw.get("total-value"),
                "currency": str(
                    (raw.get("total-value-cur") or [""])[0]
                    if isinstance(raw.get("total-value-cur"), list)
                    else raw.get("total-value-cur") or ""
                ),
                "cpv": cpv,
                "contract_conclusion_date": _date_only(
                    raw.get("contract-conclusion-date")
                ),
                "role": "unknown",
                "lots_won": [],
                "awarded_values": [],
                "award_dates": [],
                "contract_references": [],
                "matched_company_ids": [],
                "confirmed": False,
                "url": _NOTICE_HTML_URL.format(pub=pub),
                "xml_url": _NOTICE_XML_URL.format(pub=pub),
            }
            if confirmation is not None:
                notice.update(confirmation)
                notice["confirmed"] = confirmation["role"] in ("won", "tendered")
            notices.append(notice)

        wins = sum(1 for n in notices if n["role"] == "won")
        matched: list[str] = []
        for n in notices:
            for value in n["matched_company_ids"]:
                if value not in matched:
                    matched.append(value)

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "lei": lei,
            "legal_name": legal_name,
            "identifiers_queried": identifiers,
            "total_notice_count": total,
            "confirmed_wins": wins,
            "matched_company_ids": matched,
            "notices": notices,
            "is_stub": False,
        }
        validate_raw("ted_eu", TedEuBundle, bundle)
        return bundle

    async def _search_notices(self, identifiers: list[str]) -> dict[str, Any]:
        """One expert-search POST; HTTP errors propagate (→ source_error)."""
        cache_key = f"{_CACHE_NS}/search/{_slug(identifiers)}"
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        quoted = " ".join(f'"{v}"' for v in identifiers)
        query = (
            f"organisation-identifier-tenderer IN ({quoted}) "
            "SORT BY publication-date DESC"
        )
        body = {
            "query": query,
            "fields": _FIELDS,
            "limit": _SEARCH_LIMIT,
            "page": 1,
            "paginationMode": "PAGE_NUMBER",
            "scope": "ALL",  # ACTIVE/LATEST exclude award notices (verified)
        }
        async with build_client() as client:
            response = await client.post(
                _SEARCH_URL,
                json=body,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    async def _confirm_notice(
        self, raw_notice: dict[str, Any], identifiers: list[str]
    ) -> dict[str, Any] | None:
        """Fetch + parse one notice XML; ``None`` on any failure (soft)."""
        pub = str(raw_notice.get("publication-number") or "")
        if not pub:
            return None
        xml_text = await self._fetch_notice_xml(pub)
        if not xml_text:
            return None
        return parse_notice_xml(xml_text, identifiers)

    async def _fetch_notice_xml(self, pub: str) -> str | None:
        cache_key = f"{_CACHE_NS}/xml/{pub}"
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0].get("xml")
        try:
            async with build_client() as client:
                # build_client() defaults Accept to application/json, which the
                # notice endpoint rejects with 406 (caught by the live smoke
                # tier) — ask for XML explicitly.
                response = await client.get(
                    _NOTICE_XML_URL.format(pub=pub),
                    headers={"Accept": "application/xml, text/xml, */*"},
                )
                if not response.is_success:
                    log.warning(
                        "TED notice XML returned %s for %s", response.status_code, pub
                    )
                    return None
                xml_text = response.text
        except Exception as exc:  # noqa: BLE001 — confirmation is best-effort
            log.warning("TED notice XML fetch failed for %s: %s", pub, exc)
            return None
        self._cache.put(cache_key, {"xml": xml_text})
        return xml_text
