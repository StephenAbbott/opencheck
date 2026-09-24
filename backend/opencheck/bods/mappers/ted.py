"""TED (Tenders Electronic Daily) award winners → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..statements import make_entity_statement


def map_ted_eu(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a TED (Tenders Electronic Daily) fetch bundle to BODS statements.

    Emits one entity statement for the subject company. Procurement awards
    describe economic activity, not ownership or control, so no person or
    relationship statements are emitted (the EITI precedent). The award
    details ride in the adapter's raw bundle for the frontend card.

    Identifier corroboration: only what TED itself published is asserted —
    the eForms BT-501 values that actually matched on the returned notices.
    Their scheme is not machine-readable in eForms (no scheme attribute on
    ``cbc:CompanyID``), so national numbers get a ``schemeName`` only; the
    LEI gets ``scheme: "XI-LEI"`` when a notice carried the LEI string
    (fill rate is zero as of 2026-08 — this arms automatically as LEI
    adoption in eForms grows).
    """
    if not bundle or bundle.get("is_stub"):
        return

    total = int(bundle.get("total_notice_count") or 0)
    if total <= 0:
        return

    lei: str = (bundle.get("lei") or "").strip().upper()
    name: str = (bundle.get("legal_name") or "").strip()
    local_id = lei or "|".join(bundle.get("identifiers_queried") or [])
    if not local_id:
        return

    identifiers: list[dict[str, str]] = []
    for value in bundle.get("matched_company_ids") or []:
        value = str(value).strip()
        if not value:
            continue
        if lei and value.upper() == lei:
            identifiers.append(
                {
                    "id": lei,
                    "scheme": "XI-LEI",
                    "schemeName": "Legal Entity Identifier (via TED notice)",
                }
            )
        else:
            # eForms says which field the number was filed in, never which
            # register issued it, so the scheme names the field (Phase 239;
            # it had none). Not jurisdiction-prefixed: it must not bridge to
            # a register's number on a coincidence of digits.
            identifiers.append(
                {
                    "id": value,
                    "scheme": "EFORMS-BT-501",
                    "schemeName": (
                        "Organisation identifier — eForms BT-501 (via TED notice)"
                    ),
                }
            )

    notices = bundle.get("notices") or []
    wins = int(bundle.get("confirmed_wins") or 0)
    details_parts = [
        f"Named as tenderer on {total} EU procurement award notice"
        f"{'s' if total != 1 else ''} (TED, eForms era)"
    ]
    if wins:
        details_parts.append(f"{wins} confirmed win{'s' if wins != 1 else ''}")
    source_url = next(
        (n.get("url") for n in notices if n.get("url")), "https://ted.europa.eu/"
    )

    # The most recent notice publication date. TED publishes the notice; that
    # publication is the declaration. Same precedent as SEC issuer details
    # taking the latest filing date (Phase 100). Deliberately not
    # contract_conclusion_date or award_date — those are event dates about the
    # contract, not TED's assertion about this record.
    latest_notice = max(
        (n.get("publication_date") or "" for n in notices), default=""
    ) or None

    entity = make_entity_statement(
        source_id="ted_eu",
        local_id=local_id,
        name=name or local_id,
        identifiers=identifiers,
        entity_details="; ".join(details_parts),
        source_url=source_url,
        statement_date=latest_notice,
    )
    yield entity
