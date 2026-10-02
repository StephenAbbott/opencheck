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
* **A party BODS cannot name** — an ``UnspecifiedRecord`` (``{reason,
  description}``) in ``subject`` or ``interestedParty`` — becomes a
  **placeholder entity**, one per relationship statement and side, so the
  ``Ownership`` / ``Directorship`` / ``UnknownLink`` survives and the chain
  visibly ends in an undisclosed party (Phase 276; until then the link was
  dropped). The shape follows `bods-ftm`'s own converter — placeholder plus
  link, with the reason and description on both — except that it is keyed per
  statement (``<statementId>-unspecified-<side>``), never per reason, so two
  companies' undisclosed owners are two nodes, not one hub. (Cypher's
  ``#<side>`` suffix is not used: FtM's entity-reference grammar rejects
  ``#``, which would silently empty the link's endpoint.)
  FtM forces one trade-off: ``Ownership.owner`` and ``Directorship.director``
  only accept a ``LegalEntity``, and every LegalEntity schema is *matchable*,
  so the interested-party placeholder is a ``LegalEntity`` named after the
  company it is unspecified for (``Unspecified interested party in Acme
  Ltd``) — a name that will not collide across companies in Aleph xref or
  yente. The subject-side placeholder is an ``Asset``, which is not matchable
  (``Organization`` only when a directorship interest needs one).
* **Dropped:** relationships with a party that is absent or references a
  statement outside the bundle — there is no FtM node to link.

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
from collections.abc import Callable
from typing import Any

from .. import identifiers
from .annotations import person_identifiers_from_annotations
from .refs import resolver, unspecified_party

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

# LEI classification (shared; see opencheck/identifiers.py): strict ISO 17442
# shape, plus the check digits when OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED is
# on — so a coincidentally LEI-shaped registration number is not mislabelled
# ``leiCode``. Source-asserted LEI schemes ("LEI" in the haystack) are trusted
# as before.
_classify_lei = identifiers.classify_lei


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
        # ``LEI`` as a word, not a substring: "GLEIF" contains it, and the
        # GLEIF mapper names every registration number's scheme after GLEIF's
        # Registration Authorities list — so until Phase 239 an unmapped
        # register number (Equinor's organisation number) went out as a
        # ``leiCode``.
        if re.search(r"\bLEI\b", haystack) or _classify_lei(value):
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

    # Phase 255: non-document person identifiers ride in annotations.
    _add_identifiers(
        props,
        [*(rd.get("identifiers") or []), *person_identifiers_from_annotations(stmt)],
        person=True,
    )

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


#: The two relationship fields that name a party.
_PARTY_SIDES: tuple[str, ...] = ("subject", "interestedParty")


def _display_name(stmt: dict[str, Any] | None) -> str:
    """The name an entity or person statement goes by, or ``""``."""
    rd = (stmt or {}).get("recordDetails") or {}
    if (stmt or {}).get("recordType") == "person":
        for name in rd.get("names") or []:
            full = (name.get("fullName") or "").strip() if isinstance(name, dict) else ""
            if full:
                return full
        return ""
    return (rd.get("name") or "").strip() if isinstance(rd.get("name"), str) else ""


def _unspecified_note(side: str, party: dict[str, str]) -> str:
    """``Unspecified interestedParty (reason: …) — description``."""
    text = f"Unspecified {side}"
    if party.get("reason"):
        text += f" (reason: {party['reason']})"
    if party.get("description"):
        text += f" — {party['description']}"
    return text


def _placeholder_to_ftm(
    placeholder_id: str,
    side: str,
    party: dict[str, str],
    other_name: str,
    interests: list[dict[str, Any]],
) -> dict[str, Any]:
    """The FtM entity standing in for one unspecified party.

    ``other_name`` is the known party at the other end, so the placeholder's
    name is specific to its company rather than a string every placeholder
    shares — the thing that would make Aleph xref or yente pair them up.
    """
    props = _Props()
    if side == "interestedParty":
        # The only schemas Ownership.owner / Directorship.director accept.
        schema = "LegalEntity"
        props.add(
            "name",
            f"Unspecified interested party in {other_name}"
            if other_name
            else "Unspecified interested party",
        )
    else:
        # Asset is not matchable and is Ownership.asset's (and fits
        # UnknownLink.object's) range; Directorship.organization needs an
        # Organization.
        directorship = any(
            (i.get("type") or "") in _DIRECTORSHIP_INTERESTS for i in interests
        )
        schema = "Organization" if directorship else "Asset"
        props.add(
            "name",
            f"Unspecified subject of an interest held by {other_name}"
            if other_name
            else "Unspecified subject",
        )
    props.add("notes", _unspecified_note(side, party))
    return {"id": placeholder_id, "schema": schema, "properties": props.as_dict()}


def _relationship_to_ftm(
    stmt: dict[str, Any],
    known_ids: set[str],
    resolve: Callable[[Any], str] = lambda ref: ref,
    by_id: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(placeholders, links)`` for one BODS relationship statement.

    One link per interest (extra links get ``<statementId>-2``, ``-3``, … ids,
    deterministically); a relationship with no interests becomes a single
    ``UnknownLink``. An unspecified party becomes a placeholder entity (Phase
    276) and every link carries its reason in ``description``. Dropped when a
    party is absent or names a statement outside the bundle.
    """
    rd = stmt.get("recordDetails") or {}
    sid = stmt.get("statementId")
    if not sid:
        return [], []

    interests = [i for i in rd.get("interests") or [] if isinstance(i, dict)]
    ends: dict[str, str] = {}
    unspecified: dict[str, dict[str, str]] = {}
    for side in _PARTY_SIDES:
        raw = rd.get(side)
        record = unspecified_party(raw)
        if record is not None:
            ends[side] = f"{sid}-unspecified-{side}"
            unspecified[side] = record
            continue
        # A v0.4 reference is a recordId; the FtM ids are statementIds.
        ref = resolve(raw)
        if ref and ref in known_ids:
            ends[side] = ref
    if len(ends) != len(_PARTY_SIDES):
        return [], []
    subject = ends["subject"]
    party = ends["interestedParty"]

    placeholders: list[dict[str, Any]] = []
    for side, record in unspecified.items():
        other = party if side == "subject" else subject
        # A placeholder at the other end too has no name to borrow.
        other_name = _display_name((by_id or {}).get(other))
        placeholders.append(_placeholder_to_ftm(ends[side], side, record, other_name, interests))
    notes = [_unspecified_note(side, record) for side, record in unspecified.items()]

    if not interests:
        props = _Props()
        props.add("subject", party)
        props.add("object", subject)
        props.add("role", "interested party (no interest details disclosed)")
        for note in notes:
            props.add("description", note)
        return placeholders, [{"id": sid, "schema": "UnknownLink", "properties": props.as_dict()}]

    links: list[dict[str, Any]] = []
    for n, interest in enumerate(interests, start=1):
        link_id = sid if n == 1 else f"{sid}-{n}"
        link = _interest_to_ftm(link_id, party, subject, interest)
        for note in notes:
            link["properties"].setdefault("description", []).append(note)
        links.append(link)
    return placeholders, links


def map_to_ftm(bods_statements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project a BODS v0.4 bundle into FtM entities.

    Returns entity/person nodes first (insertion order preserved), then any
    placeholders for unspecified parties, then the interval entities derived
    from relationship statements — so a streaming loader always sees a link's
    endpoints before the link. Pure and
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

    by_id = {
        s["statementId"]: s
        for s in bods_statements or []
        if s.get("recordType") in ("entity", "person") and s.get("statementId")
    }
    placeholders: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    resolve = resolver(bods_statements or [])
    for stmt in relationships:
        made, linked = _relationship_to_ftm(stmt, known_ids, resolve, by_id)
        placeholders.extend(made)
        links.extend(linked)
    return nodes + placeholders + links


def to_ftm_jsonl(bods_statements: list[dict[str, Any]]) -> str:
    """FtM entities as newline-delimited JSON — the format ``ftm`` CLI tools
    and ``alephclient write-entities`` ingest directly."""
    entities = map_to_ftm(bods_statements)
    return "\n".join(json.dumps(e, ensure_ascii=False) for e in entities) + "\n"
