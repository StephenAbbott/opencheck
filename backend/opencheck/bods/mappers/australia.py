"""Australia — Australian Business Register (ABN Lookup) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ---------------------------------------------------------------------------
# Australian Business Register — ABN Lookup (data.gov.au, CC BY 3.0 AU)
# ---------------------------------------------------------------------------
#
# ABN Lookup publishes entity-level firmographic data only — no officers or
# beneficial owners — so this mapper produces a single entity statement.


def map_abr_australia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield a BODS v0.4 entity statement for an Australian ABR entity.

    Identifiers: ``AU-ABN`` (always) and ``AU-ACN`` (companies only).
    Business/trading names become ``alternateNames``; the registered state and
    postcode become a partial registered address.
    """
    if not bundle or bundle.get("is_stub"):
        return

    abn = (bundle.get("abn") or "").strip()
    acn = (bundle.get("acn") or "").strip()
    name = (bundle.get("name") or "").strip()
    if not (abn or acn) or not name:
        return

    local_id = abn or acn
    source_url = bundle.get("link") or "https://abr.business.gov.au/"

    # ── Identifiers ───────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = []
    if abn:
        identifiers.append({
            "id": abn,
            "scheme": "AU-ABN",
            "schemeName": "Australian Business Number — Australian Business Register",
        })
    if acn:
        identifiers.append({
            "id": acn,
            "scheme": "AU-ACN",
            "schemeName": "Australian Company Number — Australian Securities and Investments Commission",
        })

    # ── Partial registered address (state + postcode only) ────────────────
    addresses: list[dict[str, Any]] = []
    addr_parts = [p for p in (bundle.get("state"), bundle.get("postcode")) if p]
    if addr_parts:
        addresses.append(_addr("registered", " ".join(addr_parts), "AU"))

    # ── Business / trading names → alternateNames ─────────────────────────
    alternate_names = [
        b for b in (bundle.get("business_names") or []) if isinstance(b, str) and b.strip()
    ]

    stmt = make_entity_statement(
        source_id="abr_australia",
        local_id=local_id,
        name=name,
        jurisdiction=("Australia", "AU"),
        identifiers=identifiers,
        addresses=addresses,
        alternate_names=alternate_names,
        source_url=source_url,
        # ABR's entity type name ("Commonwealth Government Entity") is the
        # register's local wording, so it goes to entityType.details — never
        # entityType.subtype, a closed BODS 0.4 codelist. It is not mapped to
        # an enum value such as stateAgency either: that would be a
        # classification the register did not publish (Phase 214).
        entity_details=(bundle.get("entity_type_name") or "").strip() or None,
    )

    # Cancelled-status annotation.
    abn_status = (bundle.get("abn_status") or "").strip().lower()
    # ABN Lookup publishes the ABN's status ("Active" / "Cancelled") and the
    # date it took effect. A cancelled ABN is the register's terminal state
    # for the registration OpenCheck resolved (Phase 151 — previously written
    # as the literal "unknown" when no date was given, which the schema
    # forbids; now the date is set only when ABR gives one).
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["abr_australia"],
        liveness=_liveness.classify(abn_status, live=("active",), terminal=("cancelled",)),
        raw=(bundle.get("abn_status") or "").strip() or None,
        since=bundle.get("abn_status_from") if abn_status and abn_status != "active" else None,
    )

    yield stmt
