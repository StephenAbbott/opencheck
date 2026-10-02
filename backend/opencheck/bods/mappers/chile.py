"""Chile — ChileCompra (Mercado Público public procurement) → BODS.

Procurement describes economic activity, not ownership or control, so the
mapper emits one entity statement for the supplier and nothing else — the
TED and EITI precedent. The figures ride in ``entityType.details`` and the
adapter's raw bundle feeds the card.

Identifier corroboration: ChileCompra publishes the supplier's RUT on every
row it files, so the RUT is asserted — it is the source's own key for the
record returned, not the anchor's identifier echoed back. ``CL-RUT`` is the
org-id.guide scheme for the Chilean tax number; the value is written the way
Chilean documents print it (``76.242.192-5``), check digit verified at build
time, and GLEIF's copy is normalised to the same form.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..statements import make_entity_statement


def _chilecompra_clp(value: int) -> str:
    """The filed peso total in full — ``CLP 26,602,180,912``, never rounded."""
    return f"CLP {value:,}"


def map_chilecompra(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a ChileCompra fetch bundle to one BODS entity statement."""
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    supplier = bundle.get("supplier") or {}
    rut = str(bundle.get("rut") or "").strip()
    if not supplier or not rut:
        return

    orders = int(supplier.get("orders") or 0)
    bid = int(supplier.get("tenders_bid") or 0)
    won = int(supplier.get("tenders_won") or 0)
    parts: list[str] = ["Supplier to the Chilean state on Mercado Público"]
    if orders:
        parts.append(
            f"{orders:,} purchase order{'s' if orders != 1 else ''} worth "
            f"{_chilecompra_clp(int(supplier.get('order_value_clp') or 0))}"
        )
    if bid:
        parts.append(f"{won:,} of {bid:,} tender{'s' if bid != 1 else ''} bid won")
    if bundle.get("window"):
        parts.append(str(bundle["window"]))

    name = (supplier.get("name") or bundle.get("legal_name") or "").strip()
    yield make_entity_statement(
        source_id="chilecompra",
        local_id=rut,
        name=name or rut,
        identifiers=[
            {
                # The dotted form GLEIF's statement is normalised to as well
                # (``gleif_registration_scheme``), so the two corroborate.
                "id": str(bundle.get("rut_display") or rut),
                "scheme": "CL-RUT",
                "schemeName": "RUT — Rol Único Tributario (Chile)",
            }
        ],
        entity_details="; ".join(parts),
        source_url=bundle.get("link") or None,
        # The latest dated activity ChileCompra filed for this supplier — its
        # own assertion about the record, the TED latest-notice precedent.
        statement_date=supplier.get("last_date") or None,
    )
