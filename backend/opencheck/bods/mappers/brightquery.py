"""United States — BrightQuery / OpenData.org → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any

from ..statements import (
    BODSBundle,
    _addr,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# BrightQuery / OpenData.org → BODS
# ----------------------------------------------------------------------
#
# BrightQuery's COMPANY dataset provides US entities; PEOPLE_BUSINESS
# provides their executives / contacts.  Because BQ records executive
# affiliations rather than beneficial ownership, all people relationships
# are mapped to ``otherInfluenceOrControl`` with
# ``beneficialOwnershipOrControl = false`` — mirroring the approach taken
# by the reference bods-brightquery adapter.
#
# Identifier mapping (OTHER_ID_TYPE → BODS scheme):
#   CIK          → US-SEC
#   PERMID       → PERMID
#   SAM_UEI      → US-SAM-UEI
#   SAM_CAGE     → US-SAM-CAGE
#   CAPIQ        → CAPIQ
#   PITCHBOOK_ID → PITCHBOOK
#   NPI          → US-NPI
#   OPEN_FIGI    → OPENFIGI
#   ISIN         → ISIN
#   TICKER       → TICKER

_BQ_IDENTIFIER_MAP: list[tuple[str, str, str]] = [
    # (OTHER_ID_TYPE, BODS scheme code, human name)
    ("CIK",          "US-SEC",    "US SEC Central Index Key"),
    ("PERMID",       "PERMID",    "Refinitiv PermID"),
    ("SAM_UEI",      "US-SAM-UEI","US SAM Unique Entity Identifier"),
    ("SAM_CAGE",     "US-SAM-CAGE","US SAM CAGE Code"),
    ("CAPIQ",        "CAPIQ",     "S&P Capital IQ"),
    ("PITCHBOOK_ID", "PITCHBOOK", "PitchBook"),
    ("NPI",          "US-NPI",    "US National Provider Identifier"),
    ("OPEN_FIGI",    "OPENFIGI",  "OpenFIGI"),
    ("ISIN",         "ISIN",      "International Securities Identification Number"),
    ("TICKER",       "TICKER",    "Stock Ticker"),
]

_BQ_SOURCE_URL = "https://opendata.org/"


def _bq_features(record: dict[str, Any]) -> list[dict[str, Any]]:
    return record.get("FEATURES") or []


def _bq_get_feature(feats: list[dict], key: str) -> dict | None:
    """Return the first feature dict that contains *key*."""
    for f in feats:
        if key in f:
            return f
    return None


def _bq_get_value(feats: list[dict], key: str, default: str = "") -> str:
    f = _bq_get_feature(feats, key)
    return str(f[key]).strip() if f and f.get(key) is not None else default


def _bq_other_ids(feats: list[dict]) -> dict[str, str]:
    """Return all OTHER_ID_TYPE → OTHER_ID_NUMBER pairs from FEATURES."""
    result: dict[str, str] = {}
    for f in feats:
        id_type = f.get("OTHER_ID_TYPE")
        id_number = f.get("OTHER_ID_NUMBER")
        if id_type and id_number:
            result[str(id_type)] = str(id_number).strip()
    return result


def map_brightquery(bundle: dict[str, Any]) -> BODSBundle:
    """Map a BrightQuery bundle to BODS v0.4 statements.

    ``bundle`` shape (as returned by ``BrightQueryAdapter.fetch()``):

    .. code-block:: python

        {
            "source_id": "brightquery",
            "hit_id": "<LEI>",
            "lei": "<LEI>",
            "bq_id": "<RECORD_ID>",
            "name": "<primary name string>",
            "company": {<Senzing COMPANY record>},
            "people":  [{<Senzing PEOPLE_BUSINESS record>}, ...],
        }

    Produces:
    * One ``entity`` statement for the company.
    * One ``person`` + one ``relationship`` statement per named executive.
    """
    result = BODSBundle()

    company = bundle.get("company") or {}
    people = bundle.get("people") or []
    lei = bundle.get("lei") or bundle.get("hit_id") or ""
    bq_id = bundle.get("bq_id") or str(company.get("RECORD_ID") or "")

    if not company or not bq_id:
        return result

    feats = _bq_features(company)
    name = _bq_get_value(feats, "NAME_ORG") or bundle.get("name") or f"BrightQuery {bq_id}"
    other_ids = _bq_other_ids(feats)

    # --- Entity identifiers ---
    identifiers: list[dict[str, str]] = [
        {"id": bq_id, "scheme": "BRIGHTQUERY", "schemeName": "BrightQuery"},
    ]
    if lei:
        identifiers.append(
            {"id": lei, "scheme": "XI-LEI", "schemeName": "Legal Entity Identifier"}
        )
    for id_type, scheme, scheme_name in _BQ_IDENTIFIER_MAP:
        val = other_ids.get(id_type)
        if val:
            identifiers.append({"id": val, "scheme": scheme, "schemeName": scheme_name})

    # --- Business address ---
    addresses: list[dict[str, str]] = []
    addr_feat = _bq_get_feature(feats, "ADDR_LINE1") or _bq_get_feature(feats, "ADDR_CITY")
    if addr_feat:
        parts = [
            addr_feat.get("ADDR_LINE1"),
            addr_feat.get("ADDR_CITY"),
            addr_feat.get("ADDR_STATE"),
            addr_feat.get("ADDR_POSTAL_CODE"),
            addr_feat.get("ADDR_COUNTRY"),
        ]
        joined = ", ".join(p for p in parts if p)
        if joined:
            country = addr_feat.get("ADDR_COUNTRY", "")
            # Normalise "USA" → "US" for BODS country field.
            if country.upper() == "USA":
                country = "US"
            addresses.append(_addr("registered", joined, country))

    entity = make_entity_statement(
        source_id="brightquery",
        local_id=bq_id,
        name=name,
        jurisdiction=("United States", "US"),
        identifiers=identifiers,
        addresses=addresses,
        source_url=_BQ_SOURCE_URL,
    )
    result.statements.append(entity)
    entity_sid = entity["statementId"]

    # --- Executives (PEOPLE_BUSINESS records) ---
    for person_record in people:
        pfeats = _bq_features(person_record)
        person_id = str(person_record.get("RECORD_ID") or "").strip()
        if not person_id:
            continue

        # Build a display name; skip truly nameless records.
        full_name = _bq_get_value(pfeats, "NAME_FULL")
        if not full_name:
            first = _bq_get_value(pfeats, "NAME_FIRST")
            last = _bq_get_value(pfeats, "NAME_LAST")
            full_name = f"{first} {last}".strip()
        if not full_name:
            continue

        # Role from REL_POINTER_ROLE (e.g. "Executive", "Director").
        role = ""
        for f in pfeats:
            if "REL_POINTER_ROLE" in f:
                role = str(f["REL_POINTER_ROLE"]).strip()
                break

        local_person_id = f"{bq_id}:person:{person_id}"

        person = make_person_statement(
            source_id="brightquery",
            local_id=local_person_id,
            full_name=full_name.title(),
            source_url=_BQ_SOURCE_URL,
        )
        result.statements.append(person)
        person_sid = person["statementId"]

        interest: dict[str, Any] = {
            "type": "otherInfluenceOrControl",
            "directOrIndirect": "unknown",
            "beneficialOwnershipOrControl": False,
        }
        if role:
            interest["details"] = role

        rel = make_relationship_statement(
            source_id="brightquery",
            local_id=f"{bq_id}:rel:{person_id}",
            subject_statement_id=entity_sid,
            interested_party_statement_id=person_sid,
            interested_party_type="person",
            interests=[interest],
            source_url=_BQ_SOURCE_URL,
        )
        result.statements.append(rel)

    return result
