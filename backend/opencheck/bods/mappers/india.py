"""India — MCA Company Master Data (data.gov.in) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ---------------------------------------------------------------------------
# India — Ministry of Corporate Affairs Company Master Data (data.gov.in, GODL)
# ---------------------------------------------------------------------------
#
# The MCA master data is entity-level firmographic data only — no officers or
# beneficial owners — so this mapper produces a single entity statement.

# CompanyStatus values that mean the company has ceased to exist on the
# register. Deliberately conservative: in-progress states ("Under Process of
# Striking Off", "Under Liquidation") are the ``pending`` class and
# inactive-but-registered states ("Dormant") are still ``live`` — the company
# exists.
_MCA_TERMINAL_STATUSES = frozenset({
    "strike off",
    "struck off",
    "dissolved",
    "amalgamated",
    "liquidated",
    "converted to llp",
    "converted to llp and dissolved",
})
_MCA_PENDING_STATUSES = frozenset({
    "under process of striking off",
    "under liquidation",
    "to be struck off",
})
_MCA_LIVE_STATUSES = frozenset({"active", "dormant", "active in progress", "dormant under section 455"})


def map_mca_india(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield a BODS v0.4 entity statement for an Indian MCA company.

    Identifier: ``IN-MCA`` (the 21-character CIN). The registered office
    address becomes a registered address; the registration date becomes
    ``foundingDate``; terminal register statuses set ``dissolutionDate``
    ("unknown" — the master data carries no event date).
    """
    if not bundle or bundle.get("is_stub"):
        return

    cin = (bundle.get("cin") or "").strip().upper()
    name = (bundle.get("name") or "").strip()
    if not cin or not name:
        return

    source_url = bundle.get("link") or (
        "https://www.data.gov.in/resource/registrars-companies-roc-wise-company-master-data"
    )

    identifiers = [{
        "id": cin,
        "scheme": "IN-MCA",
        "schemeName": "Corporate Identification Number (CIN) — Ministry of Corporate Affairs",
    }]

    addresses: list[dict[str, Any]] = []
    address = (bundle.get("address") or "").strip()
    if address:
        addresses.append(_addr("registered", address, "IN"))

    # The register's class ("Public" / "Private" / "One Person Company"),
    # category and sub-category are all local wording, so all three go to
    # entityType.details, class first: "Public — Company limited by shares —
    # Non-government company". The class used to be written to
    # entityType.subtype, a closed BODS 0.4 codelist that never takes free
    # text (Phase 214).
    detail_bits = [
        b for b in (
            (bundle.get("company_class") or "").strip(),
            (bundle.get("category") or "").strip(),
            (bundle.get("sub_category") or "").strip(),
        ) if b
    ]

    stmt = make_entity_statement(
        source_id="mca_india",
        local_id=cin,
        name=name,
        jurisdiction=("India", "IN"),
        identifiers=identifiers,
        founding_date=(bundle.get("registration_date") or None),
        addresses=addresses,
        source_url=source_url,
        entity_details=" — ".join(detail_bits) or None,
    )

    # Register status → liveness annotation (Phase 151). MCA publishes no
    # date, so ``dissolutionDate`` is never set — it used to be the literal
    # "unknown", which the schema forbids.
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["mca_india"],
        liveness=_liveness.classify(
            bundle.get("status"),
            live=_MCA_LIVE_STATUSES,
            pending=_MCA_PENDING_STATUSES,
            terminal=_MCA_TERMINAL_STATUSES,
        ),
        raw=(bundle.get("status") or "").strip() or None,
    )

    yield stmt
