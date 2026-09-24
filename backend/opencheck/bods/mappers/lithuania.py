"""Lithuania — Register of Legal Entities (JAR) → BODS v0.4.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ----------------------------------------------------------------------
# Lithuanian Register of Legal Entities (JAR) → BODS v0.4
# ----------------------------------------------------------------------

# Map common Lithuanian legal form abbreviations to BODS entity types.
_LT_ENTITY_TYPES: dict[str, str] = {
    "UAB": "registeredEntity",          # Private limited company
    "AB": "registeredEntity",           # Public limited company
    "MB": "registeredEntity",           # Small partnership
    "IĮ": "registeredEntity",           # Sole proprietorship
    "TŪB": "registeredEntity",          # General partnership
    "KŪB": "registeredEntity",          # Limited partnership
    "VšĮ": "legalEntity",               # Public institution (non-profit)
    "Asociacija": "legalEntity",        # Association
    "Valstybės įmonė": "stateBody",     # State enterprise
    "Savivaldybės įmonė": "stateBody",  # Municipal enterprise
    "Biudžetinė įstaiga": "stateBody",  # Budget institution
    "Labdaros ir paramos fondas": "legalEntity",  # Charitable foundation
    "Kooperatinė bendrovė": "registeredEntity",   # Cooperative
}


def map_jar_lithuania(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a JarLithuaniaAdapter fetch bundle to a BODS v0.4 entity statement.

    Only entity-level data (name, code, address, legal form, status) is
    available from the JAR public search interface.  Participant / beneficial
    ownership data from the former JADIS system has been migrated to JANGIS,
    which is restricted to legitimate-interest access; it is intentionally
    excluded from this adapter.
    """
    if not bundle or bundle.get("is_stub"):
        return

    lt_code: str = (bundle.get("lt_code") or bundle.get("hit_id") or "").strip()
    name: str = (bundle.get("name") or "").strip() or f"LT-{lt_code}"
    if not lt_code or not name:
        return

    legal_form: str = (bundle.get("legal_form") or "").strip()
    entity_type: str = _LT_ENTITY_TYPES.get(legal_form, "registeredEntity")

    # Address — pre-formatted string from the JAR HTML.
    raw_address: str = (bundle.get("address") or "").strip()
    addresses: list[dict[str, Any]] = (
        [_addr("registered", raw_address, "LT")]
        if raw_address
        else []
    )

    identifiers: list[dict[str, str]] = [
        {
            "id": lt_code,
            "scheme": "LT-JAR",
            "schemeName": "Juridinių Asmenų Registras (Lithuanian Register of Legal Entities)",
        }
    ]

    source_url = f"https://www.registrucentras.lt/jar/p/index.php?kod={lt_code}"

    jar_entity = make_entity_statement(
        source_id="jar_lithuania",
        local_id=lt_code,
        name=name,
        jurisdiction=("Lithuania", "LT"),
        identifiers=identifiers,
        addresses=addresses,
        entity_type=entity_type,
        source_url=source_url,
    )
    # JAR status (Phase 151), in the register's Lithuanian: Veikiantis =
    # operating; Likviduojamas / Bankrutuojantis / Bankrotas /
    # Reorganizuojamas = a process under way; Išregistruotas = deregistered.
    # Sustabdyta (suspended) is left unclassified.
    _liveness.apply_register_status(
        jar_entity,
        source_label=SOURCE_NAMES["jar_lithuania"],
        liveness=_liveness.classify(
            bundle.get("status"),
            live=("Veikiantis", "Veikianti", "Veikiantys"),
            pending=("Likviduojamas", "Likviduojama", "Bankrutuojantis", "Bankrutuojanti", "Bankrotas", "Reorganizuojamas", "Reorganizuojama"),
            terminal=("Išregistruotas", "Išregistruota", "Išregistruotas iš registro"),
        ),
        raw=(bundle.get("status") or "").strip() or None,
    )
    yield jar_entity
