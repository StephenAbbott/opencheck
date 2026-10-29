"""Belgium — BCE / KBO (Crossroads Bank for Enterprises) → BODS.

Split out of ``mapper.py`` in Phase 246; rewritten for the index built from
the monthly KBO Open Data file when the source was wired into the lookup
(the Belgium wiring, 29 Oct 2026). ``mapper.py`` re-exports every name here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, content_addressed, make_entity_statement

# ----------------------------------------------------------------------
# BCE Belgium → BODS
# ----------------------------------------------------------------------

_BE_SCHEME = "BE-BCE_KBO"
_BE_SCHEME_NAME = "Crossroads Bank for Enterprises (Belgium)"


@content_addressed
def map_bce_belgium(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a BceBelgiumAdapter fetch bundle to BODS v0.4 statements.

    One entity statement. The open data carries no shareholders, officers or
    beneficial owners — the Belgian UBO register is held separately by FPS
    Finance and is not open — so there is nothing else to draw.

    Identifiers:
      * ``BE-BCE_KBO`` — the enterprise number **dotted** (``0202.239.951``),
        as the register writes it and as GLEIF files it under RA000025, so
        the two statements corroborate (Phase 239: the scheme follows the
        number, spelled as the register spells it).
      * ``XI-VAT`` — the enterprise number prefixed ``BE``; VAT registration
        itself is not checked (not every enterprise is VAT-registered).

    A note card (``not_found``) and a stub yield nothing.
    """
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return

    enterprise_number: str = bundle.get("enterprise_number") or ""
    dotted: str = bundle.get("dotted") or ""
    name: str = (
        bundle.get("name")
        or bundle.get("name_nl")
        or bundle.get("name_fr")
        or bundle.get("name_de")
        or bundle.get("name_en")
        or ""
    )
    if not re.fullmatch(r"\d{10}", enterprise_number) or not name:
        return
    if not dotted:
        dotted = f"{enterprise_number[:4]}.{enterprise_number[4:7]}.{enterprise_number[7:]}"

    source_url = bundle.get("link") or (
        "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
        f"?ondernemingsnummer={enterprise_number}"
    )

    identifiers: list[dict[str, str]] = [
        {"id": dotted, "scheme": _BE_SCHEME, "schemeName": _BE_SCHEME_NAME},
        {"id": f"BE{enterprise_number}", "scheme": "XI-VAT", "schemeName": "VAT number"},
    ]

    founding_date: str | None = bundle.get("start_date") or None
    if founding_date and not re.match(r"^\d{4}-\d{2}-\d{2}$", founding_date):
        founding_date = None

    address_str = bundle.get("address") or ""
    addresses = [_addr("registered", address_str, "BE")] if address_str else []

    # The register's own wording for the legal form and the kind of
    # enterprise, as the details of a closed-codelist entityType (Phase 214).
    form = (bundle.get("juridical_form") or "").strip()
    native = (bundle.get("juridical_form_native") or "").strip()
    kind = (bundle.get("enterprise_type") or "").strip()
    details_parts: list[str] = []
    if form:
        details_parts.append(f"{form} ({native})" if native and native != form else form)
    if kind == "natural person":
        details_parts.append("natural-person enterprise")
    entity_details = "; ".join(details_parts) or None

    entity_stmt = make_entity_statement(
        source_id="bce_belgium",
        local_id=enterprise_number,
        name=name,
        jurisdiction=("Belgium", "BE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        entity_details=entity_details,
        source_url=source_url,
    )
    # KBO ``Status``: AC (active) / ST (stopped). The juridical situation
    # (the register's wording — "Normal situation", "Bankruptcy opened"…)
    # rides as the raw status text where it says more than the status.
    be_status = (bundle.get("status") or "").strip()
    situation = (bundle.get("juridical_situation") or "").strip()
    raw = be_status or None
    if situation and situation.lower() not in ("normal situation", "normale toestand"):
        raw = f"{be_status} — {situation}" if be_status else situation
    _liveness.apply_register_status(
        entity_stmt,
        source_label=SOURCE_NAMES["bce_belgium"],
        liveness=_liveness.classify(be_status, live=("AC", "active"), terminal=("ST", "stopped")),
        raw=raw,
    )
    yield entity_stmt
