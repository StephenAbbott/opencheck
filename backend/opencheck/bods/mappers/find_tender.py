"""United Kingdom — Find a Tender (public procurement notices) → BODS.

Procurement describes economic activity, not ownership or control, so the
mapper emits one entity statement for the supplier and nothing else — the
TED and ChileCompra precedent. The counts ride in ``entityType.details``; the
adapter's raw bundle feeds the card.

Identifier corroboration: the notices name the supplier by its Companies
House number (``GB-COH``, the org-id.guide scheme), or by a PPON the same
feed pairs with exactly one CH number elsewhere. Either way the CH number is
the publisher's own, so it is asserted, written as Companies House writes it
(eight characters, zero-padded) — the form GLEIF's statement takes too, so
the two corroborate.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..statements import content_addressed, make_entity_statement


@content_addressed
def map_find_tender_uk(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Find a Tender fetch bundle to one BODS entity statement."""
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    supplier = bundle.get("supplier") or {}
    ch = str(bundle.get("company_number") or "").strip()
    if not supplier or not ch:
        return

    awards = int(supplier.get("awards") or 0)
    buyers = int(supplier.get("buyers") or 0)
    parts = ["Supplier to UK public bodies on Find a Tender"]
    if awards:
        parts.append(
            f"{awards:,} contract award{'s' if awards != 1 else ''} from "
            f"{buyers:,} buyer{'s' if buyers != 1 else ''}"
        )
    first, last = supplier.get("first_date"), supplier.get("last_date")
    if first and last:
        parts.append(f"{first} to {last}")
    if int(bundle.get("via_ppon") or 0):
        parts.append("some awards matched through the supplier's PPON")

    name = (supplier.get("name") or bundle.get("legal_name") or "").strip()
    yield make_entity_statement(
        source_id="find_tender_uk",
        local_id=ch,
        name=name or ch,
        identifiers=[
            {
                "id": ch,
                "scheme": "GB-COH",
                "schemeName": "Companies House",
            }
        ],
        entity_details="; ".join(parts),
        source_url=bundle.get("link") or None,
        # The latest dated award the notices record for this supplier.
        statement_date=supplier.get("last_date") or None,
    )
