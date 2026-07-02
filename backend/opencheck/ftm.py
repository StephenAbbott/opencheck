"""FollowTheMoney conversion for the OpenAleph /match flow.

SPIKE (spike/bods-ftm-api-match): convert the lookup subject into an FtM
``EntityUpdate`` payload for ``POST /api/2/match`` — identifier-aware,
native-FtM matching as a precision upgrade over the free-text name
fallback.

Two conversion paths, same output shape (``{"schema", "properties"}``):

1. **bods-ftm** (preferred, when installed): the subject is expressed as a
   BODS v0.4 entity statement and converted with
   ``bods_ftm.bods_to_ftm.entity_mapper.entity_statement_to_ftm`` — the
   canonical BODS↔FtM mapping (XI-LEI → ``leiCode``, national register
   schemes → ``registrationNumber``, jurisdiction → country code).
2. **Built-in minimal converter** (fallback): produces the identical
   property set for the subject fields OpenCheck holds. Exists because
   bods-ftm depends on followthemoney → pyicu, which needs ICU headers at
   build time — a deployment-relevant dependency kept optional for now
   (install with the ``ftm`` extra; see pyproject).

De-spike note: once pyicu packaging is settled for Render/CI, collapse to
path 1 only.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

# Jurisdiction (ISO 3166-1 alpha-2, upper) → org-id.guide scheme code for
# the national register number GLEIF carries in ``registeredAs``. Mirrors
# the subset that bods-ftm's identifier_mapper maps to FtM properties.
_JURISDICTION_SCHEME: dict[str, str] = {
    "GB": "GB-COH",
    "NL": "NL-KVK",
    "SE": "SE-BV",
    "DK": "DK-CVR",
    "NO": "NO-BRREG",
    "FI": "FI-PRH",
    "AT": "AT-FB",
    "CZ": "CZ-ARES",
    "PL": "PL-KRS",
    "BE": "BE-BCE_KBO",
    "FR": "FR-RCS",
    "SG": "SG-ACRA",
}


def _subject_bods_statement(
    lei: str,
    legal_name: str,
    jurisdiction: str = "",
    registered_as: str = "",
) -> dict[str, Any]:
    """Express the lookup subject as a minimal BODS v0.4 entity statement."""
    identifiers: list[dict[str, str]] = [{"id": lei, "scheme": "XI-LEI"}]
    scheme = _JURISDICTION_SCHEME.get((jurisdiction or "").upper())
    if registered_as and scheme:
        identifiers.append({"id": registered_as, "scheme": scheme})
    details: dict[str, Any] = {
        "entityType": {"type": "registeredEntity"},
        "name": legal_name,
        "identifiers": identifiers,
    }
    if jurisdiction:
        details["jurisdiction"] = {"code": jurisdiction.upper()}
    return {"recordId": lei, "recordDetails": details}


def _via_bods_ftm(statement: dict[str, Any]) -> dict[str, Any] | None:
    """Convert via the bods-ftm library; None when it isn't installed."""
    try:
        from bods_ftm.bods_to_ftm.entity_mapper import entity_statement_to_ftm
    except ImportError:
        return None
    try:
        proxy = entity_statement_to_ftm(statement)
    except Exception as exc:  # noqa: BLE001
        log.warning("bods-ftm conversion failed: %s", exc)
        return None
    if proxy is None:
        return None
    as_dict = proxy.to_dict()
    # The synthetic bods-ftm id is meaningless to the target instance;
    # /api/2/match only needs schema + properties.
    return {"schema": as_dict.get("schema"), "properties": as_dict.get("properties") or {}}


def _via_builtin(
    lei: str,
    legal_name: str,
    jurisdiction: str = "",
    registered_as: str = "",
) -> dict[str, Any]:
    """Minimal FtM Company payload — same shape bods-ftm produces."""
    properties: dict[str, list[str]] = {
        "name": [legal_name],
        "leiCode": [lei],
    }
    if jurisdiction:
        # FtM country values are lowercase alpha-2 (e.g. "gb").
        properties["jurisdiction"] = [jurisdiction.lower()]
    if registered_as:
        properties["registrationNumber"] = [registered_as]
    return {"schema": "Company", "properties": properties}


def subject_to_ftm_entity(
    lei: str,
    legal_name: str,
    jurisdiction: str = "",
    registered_as: str = "",
) -> dict[str, Any] | None:
    """Build the FtM ``EntityUpdate`` payload for ``POST /api/2/match``.

    Returns None only when there is not enough data to match on
    (no legal name).
    """
    lei = (lei or "").strip().upper()
    legal_name = (legal_name or "").strip()
    if not legal_name or not lei:
        return None

    converted = _via_bods_ftm(
        _subject_bods_statement(lei, legal_name, jurisdiction, registered_as)
    )
    if converted is not None:
        return converted
    return _via_builtin(lei, legal_name, jurisdiction, registered_as)
