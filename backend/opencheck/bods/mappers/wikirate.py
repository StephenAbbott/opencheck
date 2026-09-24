"""Wikirate — open ESG metric answers → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from dataclasses import field
from typing import Any, Iterable

from ..statements import make_entity_statement


# ----------------------------------------------------------------------
# Wikirate — open ESG metric answers
# ----------------------------------------------------------------------

# Wikirate Company-card identifier fields → BODS identifier schemes.
# Only fields Wikirate itself publishes are asserted (corroboration rule).
_WIKIRATE_IDENTIFIER_SCHEMES: dict[str, tuple[str, str]] = {
    "legal_entity_identifier": ("XI-LEI", "Legal Entity Identifier"),
    "wikidata_id": ("WIKIDATA", "Wikidata"),
    "uk_company_number": ("GB-COH", "Companies House"),
    "sec_central_index_key": ("US-SEC-CIK", "SEC EDGAR CIK"),
    "australian_business_number": ("AU-ABN", "Australian Business Number"),
    # A company number with no jurisdiction — not OpenCorporates' own
    # ``gb/00102498`` id, so not the ``OpenCorporates`` scheme. Named for what
    # it is (Phase 239; it had no scheme at all), and deliberately not
    # jurisdiction-prefixed, so it never bridges to a register's number.
    "open_corporates_id": (
        "OPENCORPORATES-COMPANY-NUMBER", "OpenCorporates company number, no jurisdiction"
    ),
}


def map_wikirate(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Wikirate fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the company, carrying the identifiers
    Wikirate independently publishes on the Company card. Metric answers
    describe ESG performance, not ownership or control, so no person or
    relationship statements are emitted.
    """
    if not bundle or bundle.get("is_stub"):
        return

    card_id = bundle.get("card_id")
    name: str = (bundle.get("name") or "").strip()
    if not card_id or not name:
        return

    identifiers: list[dict[str, str]] = []
    raw_identifiers: dict[str, Any] = bundle.get("identifiers") or {}
    for field, (scheme, scheme_name) in _WIKIRATE_IDENTIFIER_SCHEMES.items():
        value = raw_identifiers.get(field)
        if isinstance(value, list):
            value = value[0] if value else None
        if not value:
            continue
        identifiers.append(
            {"id": str(value), "scheme": scheme, "schemeName": f"{scheme_name} (via Wikirate)"}
        )

    entity = make_entity_statement(
        source_id="wikirate",
        local_id=str(card_id),
        name=name,
        identifiers=identifiers,
        source_url=bundle.get("wikirate_url") or "https://wikirate.org/",
    )
    yield entity
