"""Singapore — ACRA (data.gov.sg) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ---------------------------------------------------------------------------
# ACRA Singapore (data.gov.sg datastore_search)
# ---------------------------------------------------------------------------
#
# ACRA's open data carries entity data only — no officers, shareholders or
# beneficial owners — so this mapper produces a single entity statement.
#
# A bundle carries up to two rows for one UEN (see sources/acra_singapore.py):
#   entity — collection 1 ("UEN"): name, status Registered/Deregistered, entity
#            type, UEN issue date, street + postal code.
#   detail — collection 2 ("ACRA Information on Corporate Entities"), when the
#            entity is in it: company type, detailed status, incorporation
#            date, full address, former names 1–15. Missing values are "na".
# The detail row wins field by field; the entity row fills what it lacks.

#: ACRA status labels → register liveness. Exact labels observed live across
#: 90,000 collection-2 rows and both collection-1 datasets on 2026-09-10. The
#: families with open-ended suffixes ("Dissolved - …", "In Liquidation - …",
#: "Struck Off (…)") are matched by prefix in ``_acra_liveness``; anything else
#: unlisted is ``unknown``, never guessed.
_ACRA_LIVE_LABELS = frozenset({"live company", "live", "registered"})
_ACRA_PENDING_LABELS = frozenset(
    {
        "gazetted to be struck off",
        "cancellation in progress",
        "live (receiver or receiver and manager appointed)",
    }
)
_ACRA_TERMINAL_LABELS = frozenset(
    {
        "deregistered",
        "struck off",
        "terminated",
        "cancelled",
        "cancelled (non-renewal)",
        "ceased registration",
        "registration expired and has not been renewed",
        "amalgamated",
        "converted to llp",
    }
)


def _acra_liveness(label: str | None) -> str:
    """Classify an ACRA status label (collection 1 or 2)."""
    text = " ".join(str(label or "").split()).lower()
    if not text:
        return _liveness.UNKNOWN
    if text in _ACRA_PENDING_LABELS or text.startswith("in liquidation"):
        return _liveness.PENDING
    if (
        text in _ACRA_TERMINAL_LABELS
        or text.startswith("dissolved")
        or text.startswith("struck off (")
    ):
        return _liveness.TERMINAL
    if text in _ACRA_LIVE_LABELS:
        return _liveness.LIVE
    return _liveness.UNKNOWN


def _acra_address(entity: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any] | None:
    """The registered address, from the detail row when it has one."""
    from ...sources.acra_singapore import clean_field  # local import avoids a cycle

    if detail:
        if clean_field(detail.get("address_type")).upper() == "FOREIGN":
            text = ", ".join(
                p
                for p in (
                    clean_field(detail.get("other_address_line1")),
                    clean_field(detail.get("other_address_line2")),
                )
                if p
            )
            return _addr("registered", text) if text else None
        block = clean_field(detail.get("block"))
        street = clean_field(detail.get("street_name"))
        level = clean_field(detail.get("level_no"))
        unit = clean_field(detail.get("unit_no"))
        building = clean_field(detail.get("building_name"))
        postal = clean_field(detail.get("postal_code"))
        parts = [
            " ".join(p for p in (block, street) if p),
            f"#{level}-{unit}" if level and unit else "",
            building,
            f"SINGAPORE {postal}" if postal else "",
        ]
        text = ", ".join(p for p in parts if p)
        if text:
            return _addr("registered", text, "SG")

    street = clean_field(entity.get("reg_street_name"))
    postal = clean_field(entity.get("reg_postal_code"))
    parts = [street, f"SINGAPORE {postal}" if postal else ""]
    text = ", ".join(p for p in parts if p)
    return _addr("registered", text, "SG") if text else None


def map_acra_singapore(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AcraSingaporeAdapter fetch bundle to one BODS v0.4 entity statement.

    The UEN is identified as ``SG-ACRA``. Former names go to ``alternateNames``
    (the precedent NZ Companies and the FtM mapper set). The register's own
    type wording — company type, else entity type — goes to
    ``entityType.details``: ``entityType.subtype`` is a closed codelist in
    BODS 0.4 and never takes free text. The status label is classified into a
    register liveness annotation; ACRA publishes no status date, so
    ``dissolutionDate`` is never set.
    """
    from ...sources.acra_singapore import (  # local import avoids a cycle
        SG_UEN_SCHEME,
        SG_UEN_SCHEME_NAME,
        clean_field,
        former_names,
        record_url,
    )

    if not bundle or bundle.get("is_stub"):
        return

    entity: dict[str, Any] = bundle.get("entity") or {}
    detail: dict[str, Any] | None = bundle.get("detail") or None
    uen = (clean_field(entity.get("uen")) or clean_field(bundle.get("uen"))).upper()
    name = (
        clean_field((detail or {}).get("entity_name"))
        or clean_field(entity.get("entity_name"))
        or clean_field(bundle.get("legal_name"))
    )
    if not uen or not name:
        return

    alternate_names = [n for n in former_names(detail) if n != name]

    # Only collection 2's incorporation date is a founding date. Collection 1's
    # ``uen_issue_date`` is when the UEN was issued; the finding says so in
    # those words, and the statement does not promote it to ``foundingDate``.
    founding = clean_field((detail or {}).get("registration_incorporation_date"))
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", founding):
        founding = ""

    details_label = (
        clean_field((detail or {}).get("company_type_description"))
        or clean_field((detail or {}).get("business_constitution_description"))
        or clean_field((detail or {}).get("entity_type_description"))
        or clean_field(entity.get("entity_type_desc"))
    )

    address = _acra_address(entity, detail)
    resource_id = clean_field(bundle.get("record_resource_id"))

    stmt = make_entity_statement(
        source_id="acra_singapore",
        local_id=uen,
        name=name,
        jurisdiction=("Singapore", "SG"),
        identifiers=[{"id": uen, "scheme": SG_UEN_SCHEME, "schemeName": SG_UEN_SCHEME_NAME}],
        founding_date=founding or None,
        addresses=[address] if address else [],
        alternate_names=alternate_names,
        entity_type="registeredEntity",
        entity_details=details_label or None,
        source_url=record_url(resource_id, uen) if resource_id else None,
    )

    raw_status = clean_field((detail or {}).get("entity_status_description")) or clean_field(
        entity.get("uen_status_desc")
    )
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["acra_singapore"],
        liveness=_acra_liveness(raw_status),
        raw=raw_status or None,
    )

    yield stmt
