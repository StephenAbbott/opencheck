"""BODS v0.4 → Senzing JSON entity specification mapper.

OpenCheck's internal spine is BODS v0.4. This module projects an assembled BODS
bundle into the **Senzing JSON entity specification** so a user can load an
OpenCheck ownership graph straight into Senzing for entity resolution.

Output shape — the *modern* Senzing spec
(https://www.senzing.com/docs/entity_specification/):

    {
      "DATA_SOURCE": "OPENCHECK",
      "RECORD_ID": "<BODS statementId>",
      "FEATURES": [
        {"RECORD_TYPE": "ORGANIZATION"},
        {"NAME_ORG": "...", "NAME_TYPE": "PRIMARY"},
        {"LEI_NUMBER": "..."},
        {"REGISTRATION_DATE": "..."}, {"REGISTRATION_COUNTRY": "GB"},
        {"ADDR_FULL": "...", "ADDR_TYPE": "BUSINESS", "ADDR_COUNTRY": "GB"},
        {"REL_ANCHOR_DOMAIN": "OPENCHECK", "REL_ANCHOR_KEY": "<statementId>"}
      ]
    }

Modelling decisions
-------------------
* **One Senzing record per BODS entity/person statement.** OpenCheck sets
  ``statementId == recordId`` for entity/person statements, and relationship
  statements reference those ids, so the BODS statementId is a perfect stable
  Senzing ``RECORD_ID``.
* **Every entity/person record carries exactly one ``REL_ANCHOR``** keyed by its
  own statementId, so other records can point at it (Senzing allows at most one
  anchor per record).
* **A BODS relationship statement is folded into a ``REL_POINTER``** placed on
  the *interested party's* record, pointing at the *subject's* anchor. Read as
  "interested party →(role)→ subject", e.g. an owner ``OWNER_OF`` a company. The
  ``REL_POINTER_ROLE`` is derived from the BODS interest type and share band.
  A relationship is dropped (and counted) only when its interested party has no
  resolvable statement (an ``unspecified`` / unknown party) — there is no record
  to anchor the pointer to.

This is a pure, side-effect-free function over the BODS list; no network, no I/O.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

DATA_SOURCE = "OPENCHECK"
DOMAIN = "OPENCHECK"

# BODS entity/person address ``type`` → Senzing ``ADDR_TYPE``.
_ENTITY_ADDR_TYPE = {
    "registered": "BUSINESS",
    "business": "BUSINESS",
    "headquarters": "BUSINESS",
    "service": "MAILING",
    "alternative": "MAILING",
}
_PERSON_ADDR_TYPE = {
    "residence": "HOME",
    "service": "MAILING",
    "alternative": "MAILING",
    "registered": "HOME",
}

# BODS interest ``type`` → a readable, Senzing-friendly relationship role verb.
# Anything not listed falls back to the upper-snake-cased BODS type, so the
# mapping is never lossy — a new interest type still produces a sensible role.
_INTEREST_ROLE = {
    "shareholding": "OWNER_OF",
    "votingRights": "VOTING_RIGHTS_IN",
    "appointmentOfBoard": "APPOINTS_BOARD_OF",
    "seniorManagingOfficial": "PRINCIPAL_OF",
    "boardMember": "DIRECTOR_OF",
    "boardChair": "CHAIR_OF",
    "settlor": "SETTLOR_OF",
    "trustee": "TRUSTEE_OF",
    "protector": "PROTECTOR_OF",
    "beneficiaryOfLegalArrangement": "BENEFICIARY_OF",
    "rightToProfitOrSurplus": "RIGHT_TO_SURPLUS_OF",
    "rightToSurplusAssetsOnDissolution": "RIGHT_TO_ASSETS_OF",
    "otherInfluenceOrControl": "CONTROLS",
    "unknownInterest": "INTERESTED_PARTY_OF",
}

_LEI_RE = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")


def _camel_to_upper(value: str) -> str:
    """``seniorManagingOfficial`` → ``SENIOR_MANAGING_OFFICIAL``."""
    spaced = re.sub(r"(?<!^)(?=[A-Z])", "_", value)
    return re.sub(r"[^0-9A-Za-z]+", "_", spaced).upper().strip("_")


def _share_suffix(share: dict[str, Any] | None) -> str:
    """Render a BODS ``share`` object as a short human percentage label."""
    if not share:
        return ""

    def _num(key: str) -> float | None:
        val = share.get(key)
        return val if isinstance(val, (int, float)) else None

    exact = _num("exact")
    if exact is not None:
        return f"{exact:g}%"

    lo = _num("minimum")
    excl_lo = _num("exclusiveMinimum")
    hi = _num("maximum")
    excl_hi = _num("exclusiveMaximum")

    low = lo if lo is not None else excl_lo
    high = hi if hi is not None else excl_hi
    low_op = ">" if (excl_lo is not None and lo is None) else ""

    if low is not None and high is not None:
        if low == high:
            return f"{low:g}%"
        return f"{low_op}{low:g}-{high:g}%"
    if low is not None:
        return f"{low_op or '>='}{low:g}%"
    if high is not None:
        return f"<={high:g}%"
    return ""


def _interest_role(interest: dict[str, Any]) -> str:
    """A single ``REL_POINTER_ROLE`` for one BODS interest entry."""
    itype = interest.get("type") or "unknownInterest"
    role = _INTEREST_ROLE.get(itype) or _camel_to_upper(itype)
    suffix = _share_suffix(interest.get("share"))
    return f"{role} {suffix}".strip()


def _addr_features(
    addresses: list[dict[str, Any]], *, person: bool
) -> list[dict[str, Any]]:
    table = _PERSON_ADDR_TYPE if person else _ENTITY_ADDR_TYPE
    default = "HOME" if person else "BUSINESS"
    out: list[dict[str, Any]] = []
    for addr in addresses or []:
        full = (addr.get("address") or "").strip()
        if not full:
            continue
        feat: dict[str, Any] = {
            "ADDR_TYPE": table.get((addr.get("type") or "").lower(), default),
            "ADDR_FULL": full,
        }
        country = addr.get("country")
        if country:
            feat["ADDR_COUNTRY"] = country
        out.append(feat)
    return out


def _identifier_features(
    identifiers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Map BODS identifier objects to Senzing identity features.

    LEIs become ``LEI_NUMBER``; everything else becomes ``NATIONAL_ID`` with the
    issuing country inferred from a ``GB-COH`` style scheme prefix when present.
    Identifiers that only carry a ``uri`` (a link, no value) are skipped.
    """
    out: list[dict[str, Any]] = []
    for ident in identifiers or []:
        value = (ident.get("id") or "").strip()
        if not value:
            continue
        scheme = (ident.get("scheme") or "").strip()
        scheme_name = (ident.get("schemeName") or "").strip()
        haystack = f"{scheme} {scheme_name}".upper()

        if "LEI" in haystack or _LEI_RE.match(value):
            out.append({"LEI_NUMBER": value})
            continue

        feat: dict[str, Any] = {
            "NATIONAL_ID_NUMBER": value,
            "NATIONAL_ID_TYPE": scheme or scheme_name,
        }
        # Schemes are commonly "<ISO2>-<REGISTER>" (e.g. GB-COH, NO-ORG).
        prefix = scheme.split("-", 1)[0] if "-" in scheme else ""
        if len(prefix) == 2 and prefix.isalpha():
            feat["NATIONAL_ID_COUNTRY"] = prefix.upper()
        out.append(feat)
    return out


def _entity_record(stmt: dict[str, Any]) -> dict[str, Any]:
    rd = stmt.get("recordDetails") or {}
    features: list[dict[str, Any]] = [{"RECORD_TYPE": "ORGANIZATION"}]

    name = rd.get("name")
    if name:
        features.append({"NAME_ORG": name, "NAME_TYPE": "PRIMARY"})
    for alt in rd.get("alternateNames") or []:
        if alt:
            features.append({"NAME_ORG": alt, "NAME_TYPE": "ALTERNATE"})

    features.extend(_identifier_features(rd.get("identifiers") or []))

    founding = rd.get("foundingDate")
    if founding:
        features.append({"REGISTRATION_DATE": founding})
    jurisdiction = rd.get("jurisdiction") or {}
    if jurisdiction.get("code"):
        features.append({"REGISTRATION_COUNTRY": jurisdiction["code"]})

    features.extend(_addr_features(rd.get("addresses") or [], person=False))

    sid = stmt["statementId"]
    features.append({"REL_ANCHOR_DOMAIN": DOMAIN, "REL_ANCHOR_KEY": sid})
    return {"DATA_SOURCE": DATA_SOURCE, "RECORD_ID": sid, "FEATURES": features}


def _person_record(stmt: dict[str, Any]) -> dict[str, Any]:
    rd = stmt.get("recordDetails") or {}
    features: list[dict[str, Any]] = [{"RECORD_TYPE": "PERSON"}]

    names = rd.get("names") or []
    primary_done = False
    for name in names:
        full = (name.get("fullName") or "").strip()
        if not full:
            continue
        features.append(
            {"NAME_FULL": full, "NAME_TYPE": "PRIMARY" if not primary_done else "ALTERNATE"}
        )
        primary_done = True

    features.extend(_identifier_features(rd.get("identifiers") or []))

    if rd.get("birthDate"):
        features.append({"DATE_OF_BIRTH": rd["birthDate"]})
    for nat in rd.get("nationalities") or []:
        if nat.get("code"):
            features.append({"NATIONALITY": nat["code"]})

    features.extend(_addr_features(rd.get("addresses") or [], person=True))

    sid = stmt["statementId"]
    features.append({"REL_ANCHOR_DOMAIN": DOMAIN, "REL_ANCHOR_KEY": sid})
    return {"DATA_SOURCE": DATA_SOURCE, "RECORD_ID": sid, "FEATURES": features}


def _pointer_features(stmt: dict[str, Any]) -> list[dict[str, Any]]:
    """REL_POINTER feature(s) for one BODS relationship statement.

    One pointer per interest entry (each disclosed interest is its own Senzing
    relationship), all pointing from the interested party at the subject's
    anchor. Falls back to a single generic pointer when no interests are listed.
    """
    rd = stmt.get("recordDetails") or {}
    subject = rd.get("subject")
    if not subject:
        return []

    base = {"REL_POINTER_DOMAIN": DOMAIN, "REL_POINTER_KEY": subject}
    pointers: list[dict[str, Any]] = []
    for interest in rd.get("interests") or []:
        pointer = dict(base)
        pointer["REL_POINTER_ROLE"] = _interest_role(interest)
        if interest.get("startDate"):
            pointer["REL_POINTER_FROM_DATE"] = interest["startDate"]
        if interest.get("endDate"):
            pointer["REL_POINTER_THRU_DATE"] = interest["endDate"]
        pointers.append(pointer)

    if not pointers:
        pointers.append({**base, "REL_POINTER_ROLE": "INTERESTED_PARTY_OF"})
    return pointers


@lru_cache(maxsize=1)
def _desc_to_source_id() -> dict[str, str]:
    """Reverse map: BODS ``source.description`` → opencheck source_id, derived from
    the registry so it matches exactly how statements are stamped by
    ``mapper._source_block``. Lazy + cached to avoid a circular import at load."""
    from ..sources import REGISTRY
    from .mapper import _source_block

    out: dict[str, str] = {}
    for sid in REGISTRY:
        desc = (_source_block(sid, None).get("description") or "").strip()
        if desc:
            out[desc] = sid
    return out


def _source_ids_of(stmt: dict[str, Any]) -> set[str]:
    """The registered source_id(s) behind a BODS statement, via its source block.
    Empty when the source isn't a registered adapter (no licence info to attach)."""
    desc = ((stmt.get("source") or {}).get("description") or "").strip()
    if not desc:
        return set()
    sid = _desc_to_source_id().get(desc)
    return {sid} if sid else set()


def _attach_licensing(record: dict[str, Any], source_ids: set[str]) -> None:
    """Stamp ``DATA_LICENSE`` (the most-restrictive contributing licence) and
    ``ATTRIBUTION`` payload attributes onto a record — the Senzing-spec home for
    non-resolving record metadata. No-op when no source resolves (e.g. unit tests
    with hand-built statements)."""
    if not source_ids:
        return
    from ..licensing import attribution_for, most_restrictive

    lic = most_restrictive(source_ids)
    attr = attribution_for(source_ids)
    if lic is None and not attr:
        return
    # Insert the payload attributes ahead of FEATURES for readability.
    features = record.pop("FEATURES")
    if lic is not None:
        record["DATA_LICENSE"] = lic.terms.license
    if attr:
        record["ATTRIBUTION"] = attr
    record["FEATURES"] = features


def map_to_senzing(bods_statements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project a BODS v0.4 bundle into Senzing JSON entity records.

    Returns one record per entity/person statement (insertion order preserved),
    each with its disclosed ownership/control relationships folded in as
    ``REL_POINTER`` features, plus ``DATA_LICENSE`` / ``ATTRIBUTION`` payload
    attributes computed from the record's contributing sources (the most-
    restrictive licence wins — a record combining a permissive and a
    non-commercial source carries the non-commercial licence). Deterministic; it
    reads the source registry for licensing but does no network/IO.
    """
    records: dict[str, dict[str, Any]] = {}
    # Per-record set of contributing source_ids (its own statement + any folded
    # relationship statements) → drives the DATA_LICENSE / ATTRIBUTION payload.
    contributors: dict[str, set[str]] = {}
    relationships: list[dict[str, Any]] = []

    for stmt in bods_statements or []:
        rtype = stmt.get("recordType")
        sid = stmt.get("statementId")
        if rtype == "entity" and sid:
            records[sid] = _entity_record(stmt)
            contributors[sid] = _source_ids_of(stmt)
        elif rtype == "person" and sid:
            records[sid] = _person_record(stmt)
            contributors[sid] = _source_ids_of(stmt)
        elif rtype == "relationship":
            relationships.append(stmt)

    for stmt in relationships:
        rd = stmt.get("recordDetails") or {}
        party = rd.get("interestedParty")
        # Only a plain statementId reference resolves to a record we can anchor
        # the pointer on; an "unspecified" (unknown owner) party object cannot.
        if not isinstance(party, str):
            continue
        target = records.get(party)
        if target is None:
            continue
        target["FEATURES"].extend(_pointer_features(stmt))
        # The folded relationship's source also contributed to this record.
        contributors.setdefault(party, set()).update(_source_ids_of(stmt))

    for sid, record in records.items():
        _attach_licensing(record, contributors.get(sid) or set())

    return list(records.values())


def to_senzing_jsonl(bods_statements: list[dict[str, Any]]) -> str:
    """Senzing records as newline-delimited JSON (the loader's ingest format)."""
    records = map_to_senzing(bods_statements)
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n"
