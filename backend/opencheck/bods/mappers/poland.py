"""Poland — KRS (Krajowy Rejestr Sądowy) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..statements import _addr, make_entity_statement


# ----------------------------------------------------------------------
# KRS Poland (Krajowy Rejestr Sądowy) → BODS
# ----------------------------------------------------------------------


def map_krs_poland(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a KrsPolandAdapter fetch bundle to BODS v0.4 statements.

    Yields a single entity statement.  No person or ownership-or-control
    statements are emitted because the KRS public API masks personal data
    (names appear as "Ł*******", PESEL as "7**********").  The CRBR adapter
    (Phase 32) provides the unmasked beneficial ownership data.

    Identifiers emitted:
      • KRS number  — scheme "PL-KRS"
      • NIP         — scheme "PL-NIP"  (if present)
      • REGON       — scheme "PL-REGON" (if present; 9-digit entity-level)
    """
    if not bundle or bundle.get("is_stub"):
        return

    krs: str = (bundle.get("pl_krs") or bundle.get("hit_id") or "").strip()
    if not krs:
        return

    name: str = (bundle.get("name") or "").strip() or f"PL-KRS-{krs}"

    # ----------------------------------------------------------------
    # Entity type
    # ----------------------------------------------------------------
    legal_form: str = (bundle.get("legal_form") or "").upper()
    _BODS_ENTITY_TYPE: dict[str, str] = {
        "FUNDACJA": "arrangement",
        "STOWARZYSZENIE": "arrangement",
        "PRZEDSIĘBIORSTWO PAŃSTWOWE": "stateBody",
        "AGENCJA": "stateBody",
    }
    entity_type = "registeredEntity"
    for kw, bods_type in _BODS_ENTITY_TYPE.items():
        if kw in legal_form:
            entity_type = bods_type
            break

    # ----------------------------------------------------------------
    # Address
    # ----------------------------------------------------------------
    raw_address: str = (bundle.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "PL")] if raw_address else []
    )

    # ----------------------------------------------------------------
    # Identifiers
    # ----------------------------------------------------------------
    identifiers: list[dict[str, str]] = [
        {
            "id": krs,
            "scheme": "PL-KRS",
            "schemeName": "Polish National Court Register (Krajowy Rejestr Sądowy)",
        }
    ]
    nip: str = (bundle.get("nip") or "").strip()
    if nip:
        identifiers.append({
            "id": nip,
            "scheme": "PL-NIP",
            "schemeName": "Polish Tax Identification Number (Numer Identyfikacji Podatkowej)",
        })
    regon: str = (bundle.get("regon") or "").strip()
    if regon:
        identifiers.append({
            "id": regon,
            "scheme": "PL-REGON",
            "schemeName": "Polish Statistical Number (Rejestr Gospodarki Narodowej)",
        })

    founding_date: str | None = bundle.get("registration_date")
    source_url: str = bundle.get("link") or f"https://ekrs.ms.gov.pl/"

    entity_stmt = make_entity_statement(
        source_id="krs_poland",
        local_id=krs,
        name=name,
        jurisdiction=("Poland", "PL"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        entity_type=entity_type,
        source_url=source_url,
        # dataOstatniegoWpisu — "date of the last entry" in the KRS. When the
        # court register last revised this record, i.e. its declaration date.
        # Not dataRejestracjiWKRS, which is the original registration.
        statement_date=bundle.get("last_change_date"),
    )
    yield entity_stmt
