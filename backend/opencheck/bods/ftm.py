"""BODS v0.4 → FollowTheMoney (FtM) entity mapper.

OpenCheck's internal spine is BODS v0.4. This module projects an assembled BODS
bundle into **FtM entities** — the data model of OpenSanctions, OpenAleph/Aleph
and the wider followthemoney tooling — so a user can take an OpenCheck
ownership graph straight into those investigative workflows
(``ftm`` CLI, ``alephclient write-entities``, OpenSanctions matching, …).

Output shape — one JSON object per entity, the standard FtM serialisation::

    {"id": "<BODS statementId>", "schema": "Company",
     "properties": {"name": ["…"], "leiCode": ["…"], …}}

Modelling decisions
-------------------
* **One FtM entity per BODS entity/person statement.** OpenCheck sets
  ``statementId == recordId`` for entity/person statements and relationship
  statements reference those ids, so the BODS statementId is a stable FtM id
  and references resolve with no extra entity resolution.
* **BODS entityType → FtM schema:** ``registeredEntity`` → ``Company``;
  ``state`` / ``stateBody`` → ``PublicBody``; everything else (legalEntity,
  arrangement, anonymousEntity, unknownEntity) → ``LegalEntity``.
* **A BODS relationship statement becomes FtM interval entities** — one per
  disclosed interest, read as "interested party →(interest)→ subject":
  management interests (``seniorManagingOfficial``, ``boardMember``,
  ``boardChair``) → ``Directorship`` (director / organization); every other
  ownership/control interest → ``Ownership`` (owner / asset, with
  ``percentage`` and ``ownershipType`` direct/indirect); a relationship with
  no interests listed → a single ``UnknownLink``. Multi-interest relationships
  get deterministic ids ``<statementId>-2``, ``-3``, … for the extra entities.
* **Dropped (never fabricated):** relationships whose interested party is an
  ``unspecified``/unknown object rather than a statement reference — there is
  no FtM node to link.

Relation to ``opencheck/ftm.py`` (package root): that module converts only the
*lookup subject* for OpenAleph's ``POST /api/2/match`` (via the bods-ftm
library when installed). This one is the export path: a pure, dependency-free
function over the whole BODS bundle, mirroring ``bods/senzing.py``. The
canonical bidirectional converter remains
`bods-ftm <https://github.com/StephenAbbott/bods-ftm>`_ — this covers the
subset OpenCheck emits, with no ICU/followthemoney toolchain needed at runtime.
Licensing is not stamped per entity (FtM has no licence slot); the ZIP bundle's
``LICENSES.md`` carries it.
"""

from __future__ import annotations

import json
import re
from typing import Any

# BODS entityType.type → FtM schema.
_ENTITY_SCHEMA = {
    "registeredEntity": "Company",
    "legalEntity": "LegalEntity",
    "arrangement": "LegalEntity",
    "anonymousEntity": "LegalEntity",
    "unknownEntity": "LegalEntity",
    "state": "PublicBody",
    "stateBody": "PublicBody",
}

# BODS interest types that describe management rather than ownership/control —
# these become FtM ``Directorship`` links; everything else becomes ``Ownership``.
_DIRECTORSHIP_INTERESTS = {
    "seniorManagingOfficial",
    "boardMember",
    "boardChair",
}

# Human-readable role labels for the FtM ``role`` property.
_INTEREST_LABEL = {
    "shareholding": "shareholding",
    "votingRights": "voting rights",
    "appointmentOfBoard": "right to appoint or remove the board",
    "seniorManagingOfficial": "senior managing official",
    "boardMember": "board member",
    "boardChair": "board chair",
    "settlor": "settlor",
    "trustee": "trustee",
    "protector": "protector",
    "beneficiaryOfLegalArrangement": "beneficiary",
    "rightToProfitOrSurplus": "right to profit or surplus",
    "rightToSurplusAssetsOnDissolution": "right to surplus assets on dissolution",
    "otherInfluenceOrControl": "other influence or control",
    "unknownInterest": "unknown interest",
}

_LEI_RE = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")


def _camel_to_words(value: str) -> str:
    """``rightToProfitOrSurplus`` → ``right to profit or surplus``."""
    return re.sub(r"(?<!^)(?=[A-Z])", " ", value).lower()


def _interest_label(interest: dict[str, Any]) -> str:
    itype = interest.get("type") or "unknownInterest"
    label = _INTEREST_LABEL.get(itype) or _camel_to_words(itype)
    details = (interest.get("details") or "").strip()
    return f"{label} — {details}" if details else label


def _percentage(share: dict[str, Any] | None) -> str:
    """A BODS ``share`` object as an FtM ``percentage`` string (no % sign)."""
    if not share:
        return ""

    def _num(key: str) -> float | None:
        val = share.get(key)
        return val if isinstance(val, (int, float)) else None

    exact = _num("exact")
    if exact is not None:
        return f"{exact:g}"

    lo = _num("minimum")
    excl_lo = _num("exclusiveMinimum")
    hi = _num("maximum")
    excl_hi = _num("exclusiveMaximum")
    low = lo if lo is not None else excl_lo
    high = hi if hi is not None else excl_hi
    low_op = ">" if (excl_lo is not None and lo is None) else ""

    if low is not None and high is not None:
        if low == high:
            return f"{low:g}"
        return f"{low_op}{low:g}-{high:g}"
    if low is not None:
        return f"{low_op or '>='}{low:g}"
    if high is not None:
        return f"<={high:g}"
    return ""


class _Props:
    """Multi-valued FtM property accumulator (dedupes, drops empties)."""

    def __init__(self) -> None:
        self._data: dict[str, list[str]] = {}

    def add(self, prop: str, value: Any) -> None:
        text = str(value).strip() if value is not None else ""
        if not text:
            return
        values = self._data.setdefault(prop, [])
        if text not in values:
            values.append(text)

    def as_dict(self) -> dict[str, list[str]]:
        return self._data


def _add_identifiers(props: _Props, identifiers: list[dict[str, Any]], *, person: bool) -> None:
    """LEIs → ``leiCode``; Wikidata QIDs → ``wikidataId``; other register/ID
    values → ``registrationNumber`` (entities) / ``idNumber`` (persons).
    Identifiers that only carry a ``uri`` (a link, no value) are skipped."""
    for ident in identifiers or []:
        value = (ident.get("id") or "").strip()
        if not value:
            continue
        scheme = (ident.get("scheme") or "").strip()
        haystack = f"{scheme} {ident.get('schemeName') or ''}".upper()
        if "LEI" in haystack or _LEI_RE.match(value):
            props.add("leiCode", value)
        elif "WIKIDATA" in haystack:
            props.add("wikidataId", value)
        else:
            props.add("idNumber" if person else "registrationNumber", value)


def _entity_to_ftm(stmt: dict[str, Any]) -> dict[str, Any]:
    rd = stmt.get("recordDetails") or {}
    etype = ((rd.get("entityType") or {}).get("type")) or "registeredEntity"
    props = _Props()

    props.add("name", rd.get("name"))
    for alt in rd.get("alternateNames") or []:
        props.add("alias", alt)

    _add_identifiers(props, rd.get("identifiers") or [], person=False)

    jurisdiction = (rd.get("jurisdiction") or {}).get("code") or ""
    if jurisdiction:
        # FtM country values are lowercase (e.g. "gb"); BODS GB sub-region
        # codes were already normalised to ISO 3166-1 by the mapper.
        props.add("jurisdiction", jurisdiction.lower())
    props.add("incorporationDate", rd.get("foundingDate"))
    props.add("dissolutionDate", rd.get("dissolutionDate"))
    for addr in rd.get("addresses") or []:
        props.add("address", addr.get("address"))

    return {
        "id": stmt["statementId"],
        "schema": _ENTITY_SCHEMA.get(etype, "LegalEntity"),
        "properties": props.as_dict(),
    }


def _person_to_ftm(stmt: dict[str, Any]) -> dict[str, Any]:
    rd = stmt.get("recordDetails") or {}
    props = _Props()

    primary_done = False
    for name in rd.get("names") or []:
        full = (name.get("fullName") or "").strip()
        if not full:
            continue
        props.add("name" if not primary_done else "alias", full)
        primary_done = True

    _add_identifiers(props, rd.get("identifiers") or [], person=True)

    props.add("birthDate", rd.get("birthDate"))
    for nat in rd.get("nationalities") or []:
        code = (nat.get("code") or "").strip()
        if code:
            props.add("nationality", code.lower())
    for addr in rd.get("addresses") or []:
        props.add("address", addr.get("address"))

    return {"id": stmt["statementId"], "schema": "Person", "properties": props.as_dict()}


def _interest_to_ftm(
    link_id: str, party: str, subject: str, interest: dict[str, Any]
) -> dict[str, Any]:
    """One FtM interval entity for one BODS interest entry."""
    itype = interest.get("type") or "unknownInterest"
    props = _Props()
    props.add("role", _interest_label(interest))
    props.add("startDate", interest.get("startDate"))
    props.add("endDate", interest.get("endDate"))

    if itype in _DIRECTORSHIP_INTERESTS:
        props.add("director", party)
        props.add("organization", subject)
        return {"id": link_id, "schema": "Directorship", "properties": props.as_dict()}

    props.add("owner", party)
    props.add("asset", subject)
    props.add("percentage", _percentage(interest.get("share")))
    doi = (interest.get("directOrIndirect") or "").strip()
    if doi and doi != "unknown":
        props.add("ownershipType", doi)
    return {"id": link_id, "schema": "Ownership", "properties": props.as_dict()}


def _relationship_to_ftm(
    stmt: dict[str, Any], known_ids: set[str]
) -> list[dict[str, Any]]:
    """FtM link entities for one BODS relationship statement.

    One entity per interest (extra entities get ``<statementId>-2``, ``-3``, …
    ids, deterministically); a relationship with no interests becomes a single
    ``UnknownLink``. Dropped when the interested party is not a statement
    reference (``unspecified`` party — nothing to link) or either end is
    missing from the bundle.
    """
    rd = stmt.get("recordDetails") or {}
    subject = rd.get("subject")
    party = rd.get("interestedParty")
    sid = stmt.get("statementId")
    if not sid or not isinstance(subject, str) or not isinstance(party, str):
        return []
    if subject not in known_ids or party not in known_ids:
        return []

    interests = [i for i in rd.get("interests") or [] if isinstance(i, dict)]
    if not interests:
        props = _Props()
        props.add("subject", party)
        props.add("object", subject)
        props.add("role", "interested party (no interest details disclosed)")
        return [{"id": sid, "schema": "UnknownLink", "properties": props.as_dict()}]

    out: list[dict[str, Any]] = []
    for n, interest in enumerate(interests, start=1):
        link_id = sid if n == 1 else f"{sid}-{n}"
        out.append(_interest_to_ftm(link_id, party, subject, interest))
    return out


def map_to_ftm(bods_statements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project a BODS v0.4 bundle into FtM entities.

    Returns entity/person nodes first (insertion order preserved), then the
    interval entities derived from relationship statements — so a streaming
    loader always sees a link's endpoints before the link. Pure and
    deterministic; no network, no I/O.
    """
    nodes: list[dict[str, Any]] = []
    known_ids: set[str] = set()
    relationships: list[dict[str, Any]] = []

    for stmt in bods_statements or []:
        rtype = stmt.get("recordType")
        sid = stmt.get("statementId")
        if rtype == "entity" and sid:
            nodes.append(_entity_to_ftm(stmt))
            known_ids.add(sid)
        elif rtype == "person" and sid:
            nodes.append(_person_to_ftm(stmt))
            known_ids.add(sid)
        elif rtype == "relationship":
            relationships.append(stmt)

    links: list[dict[str, Any]] = []
    for stmt in relationships:
        links.extend(_relationship_to_ftm(stmt, known_ids))
    return nodes + links


def to_ftm_jsonl(bods_statements: list[dict[str, Any]]) -> str:
    """FtM entities as newline-delimited JSON — the format ``ftm`` CLI tools
    and ``alephclient write-entities`` ingest directly."""
    entities = map_to_ftm(bods_statements)
    return "\n".join(json.dumps(e, ensure_ascii=False) for e in entities) + "\n"
