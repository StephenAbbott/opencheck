"""Belgium — BCE / KBO (Crossroads Bank for Enterprises) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ----------------------------------------------------------------------
# BCE Belgium → BODS
# ----------------------------------------------------------------------


def map_bce_belgium(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a BceBelgiumAdapter fetch bundle to BODS v0.4 statements.

    Returns a single entity statement.  No beneficial ownership data is
    available from the BCE/KBO open data publication — the Belgian UBO
    register is not openly accessible.

    Identifiers emitted:
      * BE-BCE_KBO  — 10-digit enterprise number, no dots (e.g. "0433795975")
      * XI-VAT      — Belgian VAT number derived from the enterprise number
                      by prepending "BE0" for numbers beginning with 0 or
                      "BE" for numbers beginning with 1.
    """
    if not bundle or bundle.get("is_stub"):
        return

    enterprise_number: str = bundle.get("enterprise_number") or ""
    dotted: str = bundle.get("dotted") or ""
    name: str = (
        bundle.get("name")
        or bundle.get("name_nl")
        or bundle.get("name_fr")
        or bundle.get("name_de")
        or ""
    )
    if not enterprise_number or not name:
        return

    source_url = bundle.get("link") or (
        f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
        f"?ondernemingsnummer={enterprise_number}"
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": enterprise_number,
            "scheme": "BE-BCE_KBO",
            "schemeName": "Crossroads Bank for Enterprises (Belgium)",
        }
    ]

    # Belgian VAT numbers are the enterprise number prefixed with "BE":
    # enterprises starting with 0 → e.g. BE0433795975
    # enterprises starting with 1 → e.g. BE1234567890 (natural persons — rare)
    # The VAT-registration status is NOT checked here (not all enterprises are
    # VAT-registered); we emit the potential VAT number as an identifier hint.
    if enterprise_number.isdigit() and len(enterprise_number) == 10:
        identifiers.append({
            "id": f"BE{enterprise_number}",
            "scheme": "XI-VAT",
            "schemeName": "VAT number",
        })

    # Founding date from BCE start_date (may be YYYY-MM-DD or blank).
    founding_date: str | None = bundle.get("start_date") or None
    if founding_date and not re.match(r"^\d{4}-\d{2}-\d{2}$", founding_date):
        founding_date = None

    address_str = bundle.get("address") or ""
    addresses = (
        [_addr("registered", address_str, "BE")]
        if address_str
        else []
    )

    entity_stmt = make_entity_statement(
        source_id="bce_belgium",
        local_id=enterprise_number,
        name=name,
        jurisdiction=("Belgium", "BE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # KBO/BCE open-data ``Status``: AC (active) / ST (stopped) (Phase 151).
    be_status = (bundle.get("status") or "").strip()
    _liveness.apply_register_status(
        entity_stmt,
        source_label=SOURCE_NAMES["bce_belgium"],
        liveness=_liveness.classify(be_status, live=("AC", "active"), terminal=("ST", "stopped")),
        raw=be_status or None,
    )
    yield entity_stmt
