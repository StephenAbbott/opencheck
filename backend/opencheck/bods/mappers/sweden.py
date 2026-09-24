"""Sweden — Bolagsverket (Companies Registration Office) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ----------------------------------------------------------------------
# Bolagsverket (Swedish Companies Registration Office) → BODS
# ----------------------------------------------------------------------
#
# Bolagsverket publishes company information via a WSO2 API gateway.
# The register is fully public for officer/board data — unlike INPI,
# there is no BO restriction; board members, CEO, and signatories are
# safe to republish as BODS person statements.
#
# Identifier scheme: "SE-BLV"  (follows GB-COH / CH-FDJP / NL-KVK / FR-INSEE)
# Jurisdiction: Sweden ("SE")
# Source: https://www.bolagsverket.se/
#
# Response shape confirmed from Bolagsverket API documentation.
# POST /organisationer → {"organisationer": [{...}]}
# The mapper receives the first element of that array as bundle["company"].
#
# Confirmed bundle shape:
#
#   {
#     "source_id": "bolagsverket",
#     "org_number": "5299999994",
#     "company": {
#       "organisationsidentitet": {"identitetsbeteckning": "5299999994"},
#       "organisationsnamn": {
#         "organisationsnamnLista": [
#           {"namn": "Cykelbolaget AB", "registreringsdatum": "2024-03-15"}
#         ]
#       },
#       "organisationsdatum": {"registreringsdatum": "2000-01-23"},
#       "organisationsform": {"kod": "AB", "klartext": "Aktiebolag"},
#       "juridiskForm": {"kod": "49", "klartext": "Övriga aktiebolag"},
#       "postadressOrganisation": {
#         "postadress": {
#           "utdelningsadress": "Jobbstigen 2",
#           "postnummer": "12345",
#           "postort": "Grönköping",
#           "land": "Sverige",
#           "coAdress": "C/o Annat företag"
#         }
#       },
#       "verksamhetsbeskrivning": {"beskrivning": "Handel med skor"},
#       "verksamOrganisation": {"kod": "JA"},   # JA = active
#       "avregistreradOrganisation": {"avregistreringsdatum": "2023-05-05T..."},
#       "avregistreringsorsak": {"klartext": "Likvidation"},
#       "pagaendeAvvecklingsEllerOmstruktureringsforfarande": {
#         "pagaendeAvvecklingsEllerOmstruktureringsforfarandeLista": [
#           {"kod": "KK", "klartext": "Konkurs", "fromDatum": "..."}
#         ]
#       }
#     },
#     "legal_name": "Cykelbolaget AB",
#     "is_stub": False,
#   }
#
# Note: Officer/board member data is NOT returned by /organisationer.
# This endpoint covers the EU high-value company dataset only.


def map_bolagsverket(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Bolagsverket fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the registered company. Officer data
    is not available from the /organisationer endpoint so no person or
    relationship statements are emitted.

    Returns an empty iterable for stub bundles or missing company data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    org_number: str = bundle.get("org_number") or ""
    if not org_number:
        return

    # Company name: organisationsnamn.organisationsnamnLista[0].namn
    # Fall back to the GLEIF-supplied legal_name if missing.
    namn_lista: list[dict[str, Any]] = (
        (company.get("organisationsnamn") or {}).get("organisationsnamnLista") or []
    )
    name: str = ""
    if namn_lista:
        # The list may contain multiple names (trading names, historical).
        # Take the first entry — the API returns the current registered name first.
        name = (namn_lista[0].get("namn") or "").strip()
    if not name:
        name = (bundle.get("legal_name") or "").strip()
    if not name:
        return

    # Format org number for display: NNNNNN-NNNN
    org_display = f"{org_number[:6]}-{org_number[6:]}" if len(org_number) == 10 else org_number

    # Founding / registration date: organisationsdatum.registreringsdatum (YYYY-MM-DD)
    founding_date: str | None = (
        (company.get("organisationsdatum") or {}).get("registreringsdatum") or None
    )
    # Guard against non-ISO or timestamp strings
    if founding_date and len(founding_date) != 10:
        founding_date = None

    identifiers: list[dict[str, str]] = [
        {
            "id": org_display,
            "scheme": "SE-BLV",
            "schemeName": "Bolagsverket — Swedish Companies Registration Office",
        }
    ]

    # Address: postadressOrganisation.postadress
    addr_block: dict[str, Any] = (
        (company.get("postadressOrganisation") or {}).get("postadress") or {}
    )
    addresses = _bv_address(addr_block)

    source_url = "https://www.bolagsverket.se/"

    entity = make_entity_statement(
        source_id="bolagsverket",
        local_id=org_number,
        name=name,
        jurisdiction=("Sweden", "SE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        source_url=source_url,
    )
    # Register status (Phase 151): ``avregistreradOrganisation`` carries the
    # deregistration date and ``avregistreringsorsak`` the reason; a pending
    # winding-up / restructuring list (konkurs, likvidation, ...) is the
    # pending class; ``verksamOrganisation.kod`` JA is live.
    dereg = (company.get("avregistreradOrganisation") or {}).get("avregistreringsdatum") or ""
    dereg_reason = (company.get("avregistreringsorsak") or {}).get("klartext") or ""
    pending_list = (
        (company.get("pagaendeAvvecklingsEllerOmstruktureringsforfarande") or {}).get(
            "pagaendeAvvecklingsEllerOmstruktureringsforfarandeLista"
        )
        or []
    )
    active_code = str((company.get("verksamOrganisation") or {}).get("kod") or "").upper()
    if dereg:
        se_liveness, se_raw, se_since = _liveness.TERMINAL, dereg_reason or "avregistrerad", str(dereg)[:10]
    elif pending_list:
        first = pending_list[0] if isinstance(pending_list[0], dict) else {}
        se_liveness = _liveness.PENDING
        se_raw = str(first.get("klartext") or first.get("kod") or "pågående avveckling")
        se_since = str(first.get("fromDatum") or "")[:10] or None
    elif active_code == "JA":
        se_liveness, se_raw, se_since = _liveness.LIVE, "verksam", None
    else:
        se_liveness, se_raw, se_since = _liveness.UNKNOWN, None, None
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["bolagsverket"],
        liveness=se_liveness,
        raw=se_raw,
        since=se_since,
    )
    yield entity


def _bv_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a Bolagsverket postadress block.

    Field names confirmed from API documentation:
    utdelningsadress (street), postnummer, postort (city), land (country),
    coAdress (c/o line).
    """
    if not block:
        return []
    parts = [
        block.get("coAdress"),
        block.get("utdelningsadress"),
        block.get("postnummer"),
        block.get("postort"),
        block.get("land"),
    ]
    joined = ", ".join(p for p in parts if p)
    if not joined:
        return []
    country = block.get("land") or "SE"
    return [_addr("registered", joined, country)]
