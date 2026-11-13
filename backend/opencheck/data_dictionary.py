"""The output data dictionary — what every field OpenCheck writes means.

OpenCheck documents its *inputs* and its concepts (``docs/dates.md``,
``docs/identifiers.md``, ``docs/risk-signals.md``) and OpenAPI covers request
parameters, but nothing described the **output** field by field: the BODS
extension keys, the annotation motivations, the signal and degradation
objects, the export columns, the MCP responses. Open Ownership's usable-data
guidance (feature 8, *clear field and object descriptions*) says
documentation should cover codes and optional fields, not just names.

A hand-written dictionary would go stale the way the hardcoded source-count
references did, so this module **generates** ``docs/data-dictionary.md`` from
the code that produces the output:

* the Pydantic response models — field, type, default and the ``#:`` comment
  or ``Field(description=)`` beside it, read from the source with ``ast``;
* the ``RiskSignal`` / ``DegradedSource`` dataclasses and their closed
  vocabularies (reasons, checks, confidence, kind);
* the BODS extension registry (:data:`EXTENSIONS`) and the annotation
  motivations and helper kinds in ``bods/annotations.py``;
* the export tables — bods-gql's node/edge dataclasses for CSV / Excel /
  BigQuery, the batch ``rows.csv`` columns, and the Senzing, FollowTheMoney
  and AML AI row keys **observed by running the real writers** over
  :func:`sample_bundle`;
* the MCP tools — each tool's docstring and the top-level keys of the dict
  it returns, read from ``mcp/server.py`` and ``mcp/shaping.py`` with ``ast``.

Two rules keep it honest. **A key the generator meets without a description
is an error, not a blank row** — a new bods-gql column, Senzing feature or
``opencheck*`` key fails the build until :data:`COLUMN_DESCRIPTIONS` or
:data:`EXTENSIONS` names it. And **the committed file must equal the
rendering** (``scripts/generate_data_dictionary.py --check``, run in CI), so
the dictionary cannot drift from the code.

``render_export_notes(family)`` renders the one family's tables for the
``DATA-DICTIONARY.md`` shipped inside the CSV, BigQuery and AML AI zips, from
the same descriptions.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

DOC_PATH = "docs/data-dictionary.md"
DOC_URL = "https://github.com/StephenAbbott/opencheck/blob/main/docs/data-dictionary.md"

# ---------------------------------------------------------------------------
# BODS extension keys
# ---------------------------------------------------------------------------

#: Every key OpenCheck writes into a BODS statement that BODS v0.4 does not
#: define. v0.4's objects are open, and libcovebods lists an unknown key as an
#: *additional field*, not an error — but a reader has to be told what it
#: means. A key here is documented. **Any** key a mapper writes that the v0.4
#: schema does not define at that place, and that is not here, fails the
#: mapper guard (``tests/_entity_subtype_guard.py``, judged against the schema
#: by ``tests/_bods_schema_keys.py``) — whatever the key is called. The first
#: version of this guard caught only keys starting ``opencheck``, and the
#: post-deploy check of 4 Nov 2026 found five registered nowhere
#: (``legalFormLabel``, ``primaryIndustryCode``, ``formerName``,
#: ``identityVerification``, ``companiesHouseFiling``).
#:
#: ``where`` is the JSON path of the object the key sits on; ``example`` is
#: a value as written; ``since`` the phase or ticket that introduced it.
EXTENSIONS: tuple[dict[str, str], ...] = (
    {
        "key": "opencheckSourceId",
        "where": "/source (every statement OpenCheck publishes)",
        "type": "string",
        "description": (
            "The adapter id of the source the statement was mapped from — "
            "`companies_house`, `gleif`, `meip` … — the key of the REGISTRY and "
            "of `/sources`. Beside `source.description` (a display name) so "
            "licence decisions never have to recover the adapter from a label: "
            "RDF `bods:license`, Senzing `DATA_LICENSE`, the FullCheck network's "
            "`contributing_source_ids` and `LICENSES.md` all read it. On the "
            "OECD's MEIP statements it is the one key OpenCheck adds."
        ),
        "example": '"companies_house"',
        "since": "Phase 267",
    },
    {
        "key": "opencheckOfficerPosition",
        "where": "/recordDetails/interests[] (relationship statements)",
        "type": "string",
        "description": (
            "The officer position behind an interest typed "
            "`otherInfluenceOrControl` or `seniorManagingOfficial` that is an "
            "appointment, not ownership or control — \"Secretary\", "
            "\"Supervisor\", \"Authorised person\". BODS has no role code for a "
            "secretary or an agent, so the mappers type the interest and mark "
            "it; the graph, the exported diagram and the ownership map read the "
            "mark and draw a *role* edge labelled with the position. A reader "
            "that does not know the key still reads the interest type."
        ),
        "example": '"Secretary"',
        "since": "Phase 340",
    },
    {
        "key": "gleifLegalEntityEvent",
        "where": "/annotations[] (a `commenting` annotation on a GLEIF entity statement)",
        "type": "object",
        "description": (
            "GLEIF's Legal Entity Event as published — `type`, `status`, "
            "`effectiveDate`, `recordedDate`, plus `effectiveDay`, the calendar "
            "day OpenCheck read from the filer's local-midnight timestamp. The "
            "annotation's sentence states the same event in words; the object "
            "lets a consumer key on GLEIF's codes."
        ),
        "example": (
            '{"type": "MERGERS_AND_ACQUISITIONS", "status": "COMPLETED", '
            '"effectiveDate": "2024-10-24T22:00:00Z", "effectiveDay": "2024-10-25"}'
        ),
        "since": "Phase 305",
    },
    {
        "key": "legalFormLabel",
        "where": "/recordDetails (entity statements)",
        "type": "string",
        "description": (
            "The entity's legal form in words, as the source names it — GLEIF's "
            "ISO 20275 Entity Legal Form code resolved to its label "
            "(\"Foundation\", \"Aktieselskab\"), or the register's own wording "
            "(Danish CVR, Swiss Zefix). BODS `entityType.subtype` is a closed "
            "codelist that cannot hold it. The AMLA trust-or-arrangement rule "
            "reads it, so a foundation is caught by its legal form and never by "
            "its name; the subject profile shows it and the RDF export carries it."
        ),
        "example": '"Aktieselskab"',
        "since": "the GLEIF, CVR Denmark and Zefix mappers (before Phase 100)",
    },
    {
        "key": "primaryIndustryCode",
        "where": "/recordDetails (Danish CVR entity statements)",
        "type": "string",
        "description": (
            "The company's main industry code as the Danish Central Business "
            "Register files it (Dansk Branchekode DB07, the Danish version of "
            "NACE Rev. 2). Carried as filed; nothing in OpenCheck reads it."
        ),
        "example": '"702200"',
        "since": "the CVR Denmark mapper (before Phase 100)",
    },
    {
        "key": "formerName",
        "where": "/annotations[] (a `commenting` annotation on `/recordDetails/alternateNames/<i>`)",
        "type": "object",
        "description": (
            "Says the `alternateNames` entry the annotation points at is a "
            "former legal name, not a trading name or transliteration: `name`, "
            "and `from` / `until` where the register dates it (Companies House "
            "does; GLEIF dates only the change). Only names a source types as "
            "former get one. A reader that does not know the key still sees the "
            "name in `alternateNames`."
        ),
        "example": '{"name": "BARRICK GOLD CORPORATION", "until": "2024-05-06"}',
        "since": "Phase 309",
    },
    {
        "key": "identityVerification",
        "where": "/annotations[] (a `commenting` annotation on a Companies House officer or PSC)",
        "type": "object",
        "description": (
            "Companies House's record that an identity verification statement "
            "is in place: `status` (`verified`), `recordedBy`, `route` "
            "(`authorisedCorporateServiceProvider` or `companiesHouse`), and "
            "where the register says so `verifiedBy` (the provider, with its "
            "`antiMoneyLaunderingSupervisoryBodies`) and `identityVerifiedOn`. On "
            "a role, `verificationStatementSuppliedOn`; on a person, "
            "`firstVerificationStatementOn` — a statement date, never a "
            "verification date. Absent where nothing is verified."
        ),
        "example": (
            '{"status": "verified", "route": "companiesHouse", '
            '"verificationStatementSuppliedOn": "2026-03-18"}'
        ),
        "since": "Phase 203",
    },
    {
        "key": "companiesHouseFiling",
        "where": "/annotations[] (a `commenting` annotation on a Companies House PSC relationship)",
        "type": "object",
        "description": (
            "The filing that notified, or ceased, a person with significant "
            "control: `event` (`notification` / `cessation`), `match` (how it "
            "was found — the PSC form, the incorporation filing, the next "
            "confirmation statement, or none), `declarant` (always the company: "
            "Companies House publishes no presenter), and where matched the "
            "form `type`, `transactionId`, filing `date` and `documentUrl`. The "
            "annotation's sentence says the same in words."
        ),
        "example": (
            '{"event": "notification", "match": "filing", "declarant": "company", '
            '"type": "PSC02", "date": "2016-06-30"}'
        ),
        "since": "the filing-provenance ticket (Nov 2026)",
    },
)

#: Keys that are documented extensions, by name.
EXTENSION_KEYS: frozenset[str] = frozenset(e["key"] for e in EXTENSIONS)

#: The prefix every OpenCheck-coined key carries. ``gleifLegalEntityEvent``
#: is the one documented key outside it (it names GLEIF's object, not ours).
EXTENSION_PREFIX = "opencheck"


def extension_keys_in(value: Any, _path: str = "") -> Iterator[tuple[str, str]]:
    """Yield ``(path, key)`` for every ``opencheck*`` key anywhere in *value*."""
    if isinstance(value, dict):
        for key, child in value.items():
            here = f"{_path}/{key}"
            if isinstance(key, str) and key.startswith(EXTENSION_PREFIX):
                yield here, key
            yield from extension_keys_in(child, here)
    elif isinstance(value, list):
        for i, child in enumerate(value):
            yield from extension_keys_in(child, f"{_path}/{i}")


def unregistered_extension_keys(statement: Any) -> list[str]:
    """Paths of ``opencheck*`` keys in *statement* that :data:`EXTENSIONS` lacks."""
    return [
        path for path, key in extension_keys_in(statement) if key not in EXTENSION_KEYS
    ]


#: What each BODS annotation motivation means in OpenCheck's output. A
#: motivation the package writes must not read :data:`NOT_WRITTEN`, and one
#: it does not write must — :func:`motivations_written` reads the code and
#: ``tests/test_data_dictionary.py`` holds the two together. Until the
#: post-deploy check of 4 Nov 2026 this table said OpenCheck never writes
#: ``linking``, while ``listing.py`` wrote it on every PermID listing.
NOT_WRITTEN = "Defined by BODS; OpenCheck does not write it."
MOTIVATION_WORDS: dict[str, str] = {
    "transformation": "OpenCheck changed the source's value on the way to BODS — a role label became an interest type, a date changed format, a partial date was dropped, a Nasdaq CSD account prefix was stripped from a name. `description` states what the source said; `transformedContent` mirrors the emitted value.",
    "commenting": "The value is exactly what the source published, but a reader could misread it — an imprecise `birthDate`, a reporting exception, a succession partner, a GLEIF event, a former name, an identity verification, the filing that notified a PSC, a founder's historic role, a discrepancy report the register flags.",
    "identifying": "Which real-world thing the statement is about, where BODS gives the key no home: a register's own person key (`EE-ARIREGISTER-HASH`), a Wikidata Q-id, an OpenSanctions record id, an SEC CIK — written by `person_identifier_note` in one fixed sentence shape so exports can read the identifier back.",
    "linking": "A value OpenCheck added from a source other than the statement's own, with a link to it: the subject's primary stock-exchange listing (`/recordDetails/publicListing`) on OpenCheck's GLEIF statement, from LSEG PermID, with the quote's page as `url` — so the listing is never read as something GLEIF published.",
    "correcting": NOT_WRITTEN,
}

#: The annotation constructors in ``bods/annotations.py`` and the motivation
#: each writes.
_MOTIVATION_HELPERS = {
    "transformation": "transformation",
    "commenting": "commenting",
    "identifying": "identifying",
}


def motivations_written() -> dict[str, list[str]]:
    """Every motivation the package writes, with the modules that write it.

    Read from the source: a dict literal with a constant ``"motivation"``
    value, or a call to one of the constructors in ``bods/annotations.py``.
    """
    found: dict[str, set[str]] = {}
    for path in _PACKAGE_ROOT.rglob("*.py"):
        rel = str(path.relative_to(_PACKAGE_ROOT))
        if rel in ("data_dictionary.py", "bods/annotations.py"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                for k, v in zip(node.keys, node.values, strict=True):
                    if (
                        isinstance(k, ast.Constant) and k.value == "motivation"
                        and isinstance(v, ast.Constant) and isinstance(v.value, str)
                    ):
                        found.setdefault(v.value, set()).add(rel)
            elif isinstance(node, ast.Call):
                func = node.func
                name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
                if name in _MOTIVATION_HELPERS:
                    found.setdefault(_MOTIVATION_HELPERS[name], set()).add(rel)
    return {m: sorted(mods) for m, mods in sorted(found.items())}


# ---------------------------------------------------------------------------
# Column and key descriptions for the export tables
# ---------------------------------------------------------------------------

_GQL_COMMON = {
    "record_id": "The BODS `recordId` — the party's or relationship's stable identity across versions. Relationships reference parties by it.",
    "statement_id": "The BODS `statementId` — this statement's own id, a digest of its content under the record id, so an edited record is a new statement.",
    "statement_date": "`statementDate`: the day the source's record was true (the register's own date where it publishes one, else the snapshot cut or retrieval).",
    "record_status": "`recordStatus`: `new`, `updated` or `closed`. A closed relationship has ended.",
    "declaration_subject": "`declarationSubject`: the `recordId` of the subject of the network this statement belongs to — per source, each bundle's own subject.",
    "is_component": "`isComponent`: true for a hop inside an indirect ownership chain whose primary relationship lists it in `component_records_json`.",
    "publisher_name": "`publicationDetails.publisher.name` — \"OpenCheck\" for statements OpenCheck mapped, the OECD / Open Ownership for statements passed through verbatim.",
    "publication_date": "`publicationDetails.publicationDate` — when the statement was published.",
    "bods_version": "`publicationDetails.bodsVersion` — always `0.4`.",
    "source_json": "The statement's `source` object as JSON: `type`, `description`, `url`, `retrievedAt` and `opencheckSourceId`.",
    "annotations_json": "The statement's `annotations` array as JSON — see *Annotations*. Empty when none.",
    "identifiers_json": "Every `identifiers[]` entry as JSON (`scheme`, `id`, `schemeName`, `uri`).",
    "addresses_json": "Every `addresses[]` entry as JSON (`type`, `address`, `postCode`, `country`).",
}

#: Column → description, per export family. The column *sets* come from the
#: code (bods-gql's dataclasses, ``batch.CSV_COLUMNS``, the Senzing / FtM /
#: AML AI writers run over the sample bundle); only the words live here. A
#: column the code produces that this table lacks fails the render.
COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "entity_nodes.csv": {
        **_GQL_COMMON,
        "name": "`recordDetails.name` — the legal name as the source publishes it.",
        "entity_type": "`entityType.type`: `registeredEntity`, `legalEntity`, `arrangement`, `anonymousEntity`, `unknownEntity`, `state`, `stateBody`.",
        "entity_subtype": "`entityType.subtype` from the closed v0.4 codelist (`trust`, `nomination`, `stateBody` …), or empty.",
        "entity_type_details": "`entityType.details` — the register's own wording for the entity's form (\"Private limited company\", \"Local Company\").",
        "jurisdiction_name": "`jurisdiction.name` — the country or subdivision in words.",
        "jurisdiction_code": "`jurisdiction.code` — ISO 3166-1 alpha-2 or 3166-2 (`US-DE`), or empty when the source gives only a name.",
        "founding_date": "`foundingDate` as a full ISO day; a partial date the source gave is dropped and said in an annotation.",
        "dissolution_date": "`dissolutionDate` as a full ISO day, where the register or GLEIF's completed terminal event dates it.",
        "uri": "`uri` — the record's own page on the source, where the source has one.",
        "primary_identifier_id": "The first `identifiers[].id` — usually the LEI (`XI-LEI`) or the register number.",
        "primary_identifier_scheme": "The scheme of the first identifier (`XI-LEI`, `GB-COH`, an RA code for a register OpenCheck has no adapter for).",
        "registered_address": "The `registered` address, as one line.",
        "registered_country": "The registered address's country code.",
        "alternate_names_json": "`alternateNames[]` as JSON — former names, trading names, GLEIF's transliterated names.",
        "node_label": "The property-graph label: `Entity`.",
    },
    "person_nodes.csv": {
        **_GQL_COMMON,
        "name": "The person's full name (`names[].fullName` of the first name).",
        "family_name": "`names[].familyName` of the first name, where the source splits names.",
        "given_name": "`names[].givenName` of the first name, where the source splits names.",
        "person_type": "`personType`: `knownPerson`, `anonymousPerson` or `unknownPerson`.",
        "birth_date": "`birthDate` at the precision the register publishes — `YYYY`, `YYYY-MM` or a full day.",
        "death_date": "`deathDate`, rarely published.",
        "nationality_code": "The first `nationalities[].code` (ISO 3166-1 alpha-2).",
        "pep_status": "`politicalExposure.status`: `isPep`, `isNotPep` or `unknown` — as the source states it, never OpenCheck's screening result.",
        "names_json": "Every `names[]` entry as JSON (`type`, `fullName`, `familyName`, `givenName`).",
        "nationalities_json": "Every `nationalities[]` entry as JSON (`name`, `code`).",
        "political_exposure_json": "The `politicalExposure` object as JSON — status and the source's `details[]`.",
        "node_label": "The property-graph label: `Person`.",
    },
    "ownership_edges.csv": {
        **_GQL_COMMON,
        "interested_party": "The `recordId` of the party holding the interest (`interestedParty`) — the edge's source node.",
        "subject": "The `recordId` of the party the interest is in (`subject`) — the edge's target node.",
        "interest_types": "Every `interests[].type`, joined — `shareholding`, `votingRights`, `appointmentOfBoard`, `seniorManagingOfficial`, `otherInfluenceOrControl` ….",
        "is_beneficial_ownership": "True when any interest carries `beneficialOwnershipOrControl: true` — a declared beneficial owner under the register's regime; empty where the regime makes no such claim.",
        "direct_or_indirect": "`interests[].directOrIndirect` of the first interest: `direct`, `indirect` or `unknown`.",
        "share_exact": "`share.exact` of the first interest with a share, as a percentage.",
        "share_minimum": "`share.minimum` of a banded share (inclusive).",
        "share_maximum": "`share.maximum` of a banded share (inclusive).",
        "share_exclusive_minimum": "`share.exclusiveMinimum` — the band's lower end, exclusive (\"more than 25%\").",
        "share_exclusive_maximum": "`share.exclusiveMaximum` — the band's upper end, exclusive.",
        "interest_start_date": "`interests[].startDate` of the first interest.",
        "interest_end_date": "`interests[].endDate` of the first interest — set on an ended interest; the record is then `closed`.",
        "interests_json": "Every `interests[]` object as JSON, including `details` (the register's words), `share` and `opencheckOfficerPosition`.",
        "component_records_json": "`componentRecords[]` as JSON — the hops a primary indirect relationship is assembled from.",
        "subject_unspecified_json": "`subject` when it names no record: an `unspecified` object with `reason` and `description`.",
        "interested_party_unspecified_json": "`interestedParty` when it names no record — a withheld or unknown owner, `reason` from the v0.4 codelist and the register's `description`.",
        "edge_label": "The property-graph label: `HAS_INTEREST`.",
    },
    "rows.csv": {
        "lei": "The LEI screened.",
        "legal_name": "GLEIF's legal name.",
        "jurisdiction": "GLEIF's jurisdiction code (ISO 3166-1 alpha-2, or 3166-2 for US states).",
        "register_status": "The register status from the subject profile (`live`, `dissolved`, `liquidation`, `unknown`).",
        "lei_registration": "The LEI record's own registration status (`ISSUED`, `LAPSED`, `RETIRED` …) — the LEI's status, never the company's.",
        "lei_registration_since": "The day a non-ISSUED status dates from (a lapse is dated by the missed renewal).",
        "verdict": "The one-sentence template verdict.",
        "risk_count": "Number of signals with `kind: risk` after merging.",
        "risk_codes": "Those signals' codes, `;`-joined.",
        "context_count": "Number of signals with `kind: context` — structural context, never a finding.",
        "context_codes": "Those signals' codes, `;`-joined.",
        "sources_applicable": "How many sources applied to this company (the GLEIF anchor included).",
        "sources_answered": "How many applicable sources replied — with a record or with none.",
        "sources_with_data": "How many returned a record.",
        "degraded": "`true` when any screen or source did not fully run — never read the row as clean while set.",
        "degraded_sources": "The degraded `source_id`s, `;`-joined.",
        "state": "`done` for a screened row, `failed` for one that was not.",
        "reason": "Why a row failed (an unknown LEI, a rate limit), empty when done.",
        "report_url": "The report page for the LEI on opencheck.world.",
        "disclosure": "Each source's disclosure status, as space-separated `source/covers=value` tokens (`companies_house/beneficial_owners=exempt gleif/parent=exempt`); empty when no source stated one.",
    },
    "senzing": {
        "DATA_SOURCE": "Always `OPENCHECK` — the Senzing data source every record is loaded under.",
        "RECORD_ID": "The BODS `statementId` of the party; for a placeholder record, a synthetic id for the unspecified side of a relationship.",
        "RECORD_TYPE": "`ORGANIZATION` for an entity statement, `PERSON` for a person statement.",
        "NAME_ORG": "An organisation name; `NAME_TYPE` says whether it is the `PRIMARY` name or an `ALTERNATE` one.",
        "NAME_FULL": "A person's full name; `NAME_TYPE` as above.",
        "NAME_TYPE": "`PRIMARY` or `ALTERNATE`, beside each name feature.",
        "LEI_NUMBER": "The LEI, from an `XI-LEI` identifier or any value that passes the LEI check digits.",
        "NATIONAL_ID_NUMBER": "A register number (`GB-COH`, `NO-BRC`, a GLEIF RA code, `REG-<country>` …), with `NATIONAL_ID_TYPE` naming the scheme. Which schemes are register numbers is a fixed table, `opencheck.bods.identifier_kinds`.",
        "NATIONAL_ID_TYPE": "The identifier's scheme (`GB-COH`, `NO-BRC` …), or its scheme name when it has none.",
        "NATIONAL_ID_COUNTRY": "The ISO 3166-1 country that issued the number, from the scheme's two-letter prefix or `REG-<country>`; absent when the scheme names none (a GLEIF RA code, `XI-`). Never read from the scheme name.",
        "TAX_ID_NUMBER": "A tax or VAT number (`XI-VAT`, `PL-NIP`, `CA-BN`, `<ISO3>-TAXID` …), with `TAX_ID_TYPE` naming the scheme.",
        "TAX_ID_TYPE": "The tax identifier's scheme.",
        "TAX_ID_COUNTRY": "The country that issued the tax number, read from the scheme as for `NATIONAL_ID_COUNTRY`; absent for `XI-VAT`, whose country is in the number.",
        "PASSPORT_NUMBER": "A person's passport number, from an `<ISO3>-PASSPORT` identifier.",
        "PASSPORT_COUNTRY": "The passport's issuing country, from the scheme's alpha-3 prefix.",
        "OTHER_ID_NUMBER": "Another database's or register's own key — Wikidata, OpenSanctions, OpenCorporates, an SEC CIK, a BIC, a register's person key — with `OTHER_ID_TYPE` naming the scheme. Supporting evidence for Senzing, never a reason to keep two records apart. Industry, securities and market codes (NACE, ISIN, MIC …) are left out.",
        "OTHER_ID_TYPE": "The other identifier's scheme.",
        "OTHER_ID_COUNTRY": "The country in the scheme's prefix (`US-SEC-CIK` → `US`), where it has one.",
        "REGISTRATION_DATE": "`foundingDate`.",
        "REGISTRATION_COUNTRY": "`jurisdiction.code`.",
        "DATE_OF_BIRTH": "`birthDate` at the register's precision.",
        "NATIONALITY": "A `nationalities[].code`, one feature per nationality.",
        "ADDR_FULL": "An address as one line, with `ADDR_TYPE` and `ADDR_COUNTRY` beside it.",
        "ADDR_TYPE": "Senzing's `BUSINESS` / `MAILING` / `HOME` from the BODS address type.",
        "ADDR_COUNTRY": "The address's country code.",
        "REL_ANCHOR_DOMAIN": "`OPENCHECK` — every party is a relationship anchor in that domain.",
        "REL_ANCHOR_KEY": "The party's `statementId`, which pointers refer to.",
        "REL_POINTER_DOMAIN": "`OPENCHECK` — a relationship pointer from a party to another party.",
        "REL_POINTER_KEY": "The `statementId` of the party pointed at.",
        "REL_POINTER_ROLE": "The relationship's role, one pointer per interest: the interest type upper-cased (`SHAREHOLDING`, `SENIOR_MANAGING_OFFICIAL` …) with a share suffix where one is stated, or `INTERESTED_PARTY_OF` when no interest is listed.",
        "REL_POINTER_FROM_DATE": "The interest's `startDate`.",
        "REL_POINTER_THRU_DATE": "The interest's `endDate` — set on an ended interest.",
        "UNSPECIFIED_PARTY": "On a placeholder record: which side (`subject` / `interestedParty`) of the relationship names no record.",
        "BODS_RELATIONSHIP_ID": "On a placeholder record: the relationship statement it stands in for.",
        "UNSPECIFIED_REASON": "On a placeholder record: the BODS `unspecified.reason` code (`informationUnknownToPublisher`, `interestedPartyExemptFromDisclosure` …).",
        "UNSPECIFIED_DESCRIPTION": "On a placeholder record: the register's own words for why the party is not named.",
        "DATA_LICENSE": "The licence of the statement's source, from `opencheckSourceId`.",
        "ATTRIBUTION": "The source's attribution line.",
        "FEATURES": "The list of feature objects; every key below `RECORD_TYPE` is a feature.",
    },
    "ftm": {
        "id": "The FollowTheMoney entity id — the BODS `statementId` for a party, a derived id for an interest.",
        "schema": "The FtM schema: `Company` or `Organization` (entity), `Person`, `LegalEntity` (an unspecified party), `Ownership` or `Directorship` (an interest).",
        "properties": "The FtM property map; every key below is a property.",
        "name": "The party's names.",
        "alias": "Alternate names.",
        "leiCode": "The LEI.",
        "registrationNumber": "Register numbers from non-LEI identifier schemes.",
        "wikidataId": "The Wikidata Q-id, from a `WIKIDATA` identifier (on a person, read back from its `identifying` annotation).",
        "idNumber": "A person's other identifier — a tax or identity-document number from `identifiers`, or a register's person key read back from its `identifying` annotation.",
        "jurisdiction": "`jurisdiction.code`.",
        "incorporationDate": "`foundingDate`.",
        "dissolutionDate": "`dissolutionDate`.",
        "birthDate": "`birthDate`.",
        "nationality": "`nationalities[].code`.",
        "address": "Addresses, one line each.",
        "owner": "On an `Ownership`: the interested party's entity id.",
        "asset": "On an `Ownership`: the subject's entity id.",
        "director": "On a `Directorship`: the interested party's entity id.",
        "organization": "On a `Directorship`: the subject's entity id.",
        "role": "The interest type in words (\"shareholding\", \"senior managing official\", \"other influence or control\").",
        "ownershipType": "On an `Ownership`: the interest types in words.",
        "percentage": "On an `Ownership`: the share, exact or as a band (\"25–50%\").",
        "startDate": "The interest's `startDate`.",
        "endDate": "The interest's `endDate`.",
        "description": "On an interest with an unspecified party: the note saying which side is unspecified, the reason code and the register's words.",
        "notes": "On a `LegalEntity` placeholder: the same note — which side, the reason code and the register's words.",
    },
    "party.ndjson": {
        "party_id": "The AML AI party id — the BODS `recordId`.",
        "validity_start_time": "The row's validity start: the statement's `publicationDate` as a timestamp.",
        "type": "`COMPANY` for an entity statement, `CONSUMER` for a person.",
        "source_system": "Always `BODS`.",
        "name": "The party's name.",
        "birth_date": "A person's `birthDate` as a BigQuery date (a partial date is rounded).",
        "establishment_date": "An entity's `foundingDate` as a BigQuery date.",
        "addresses": "Repeated address records (`type`, `address_line`, `city`, `postal_code`, `region_code`).",
        "nationalities": "Repeated nationality records (`region_code`).",
        "residencies": "Repeated residency records (`region_code`) from an entity's jurisdiction.",
    },
    "party_supplementary_data.ndjson": {
        "party_supplementary_data_id": "Which supplementary field the row carries (`ownership_share_pct`, `pep_flag` …).",
        "party_id": "The party the row describes.",
        "validity_start_time": "The row's validity start.",
        "supplementary_data_payload": "`{\"value\": <number>}` — the field's value.",
        "source_system": "Always `BODS`.",
    },
    "account_party_link.ndjson": {
        "account_id": "A synthetic account standing for the subject company (`bods-acct-<recordId>`): AML AI models relationships through accounts, and OpenCheck holds none.",
        "party_id": "The party linked to the account.",
        "validity_start_time": "The earliest relationship date, else the subject's publication date.",
        "role": "`PRIMARY_HOLDER` for the subject, `SUPPLEMENTARY_HOLDER` for each interested party.",
        "source_system": "Always `BODS`.",
    },
}

#: The export families, in the order the document lists them, with a sentence
#: each and the ``?format=`` values that produce them.
EXPORT_FAMILIES: tuple[tuple[str, str, str], ...] = (
    (
        "csv",
        "CSV and Excel — `format=csv` (zip of three tables), `format=xlsx` (one workbook, a sheet per table) and `format=gql` (the same tables plus BigQuery DDL and GQL queries)",
        "Three tables projected by [bods-gql](https://github.com/StephenAbbott/bods-gql): one row per entity statement, per person statement and per relationship statement. Every cell is text; empty tables are omitted (a company with no natural persons ships no `person_nodes.csv`). Every column name below is a dataclass field of the library, read at generation time.",
    ),
    (
        "batch",
        "Batch screening — `rows.csv` in the batch zip and the `rows[]` of `opencheck_batch_lookup`",
        "One row per LEI screened, in paste order. The same columns, in the same order, as the page's client-side download (`frontend/src/lib/batch.ts`).",
    ),
    (
        "senzing",
        "Senzing — `format=senzing`",
        "One JSON record per party in [Senzing's entity specification](https://www.senzing.com/docs/entity_specification/), features as a list of single-key objects. Relationships are `REL_ANCHOR` / `REL_POINTER` features; an unspecified party becomes a placeholder record so the pointer resolves. Keys are observed by running the writer over the sample bundle.",
    ),
    (
        "ftm",
        "FollowTheMoney — `format=ftm`",
        "One [FollowTheMoney](https://followthemoney.tech/) entity per party and per interest, as JSON lines with `id`, `schema` and `properties`. Property names below are FtM's; the list is what the writer produced for the sample bundle.",
    ),
    (
        "amlai",
        "Google AML AI — `format=amlai`",
        "The three input tables of the [AML AI data model](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model), one NDJSON file each, produced by [bods-aml-ai](https://github.com/StephenAbbott/bods-aml-ai). Keys are observed by running the transform over the sample bundle; `nationalities`, `addresses` and `supplementary_data_payload` are nested records.",
    ),
)


# ---------------------------------------------------------------------------
# A sample bundle every writer is run over
# ---------------------------------------------------------------------------


def sample_bundle() -> list[dict[str, Any]]:
    """A small synthetic BODS bundle exercising every shape the writers read.

    Two companies, two people, a shareholding with a share band, an officer
    appointment marked with ``opencheckOfficerPosition``, an ended interest,
    an unspecified owner, alternate names, a nationality, an address of each
    kind, a PEP declaration, Wikidata and tax identifiers — and **every key in**
    :data:`EXTENSIONS` and every motivation OpenCheck writes, each built by
    the helper that writes it in production, so the export writers are run
    over every shape they can meet (``tests/test_data_dictionary.py`` holds
    the sample to that). Invented names, no real company.
    """
    from .bods.annotations import commenting, entry_date_as_start
    from .bods.ch_filings import FILING_PROPERTY, describe
    from .bods.former_names import former_name_annotation
    from .bods.gleif_events import EVENT_PROPERTY
    from .bods.identity_verification import ROUTE_COMPANIES_HOUSE, Verification, role_annotation
    from .bods.officer_roles import mark_officer
    from .bods.statements import (
        _addr,
        make_entity_statement,
        make_person_statement,
        make_relationship_statement,
        refresh_statement_id,
    )
    from .listing import apply_to_bods

    holdco = make_entity_statement(
        source_id="gleif",
        local_id="DD-HOLDCO",
        name="Example Holdings Limited",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[
            {"scheme": "XI-LEI", "id": "52990000EXAMPLE00001", "schemeName": "LEI"},
            {"scheme": "GB-COH", "id": "01234567", "schemeName": "Companies House"},
        ],
        founding_date="1998-03-12",
        addresses=[
            {**_addr("registered", "1 Example Street, London", "GB"), "postCode": "EC1A 1AA"},
            _addr("business", "2 Trading Row, Leeds", "GB"),
        ],
        alternate_names=["Example Holdings PLC", "Example Group Holdings Limited"],
        entity_details="Private limited company",
        source_url="https://example.invalid/holdco",
    )
    holdco["recordDetails"]["legalFormLabel"] = "Private Limited Company"
    refresh_statement_id(holdco)
    event = commenting(
        "/recordDetails",
        "GLEIF records a completed merger, effective 25 October 2024.",
    )
    event[EVENT_PROPERTY] = {
        "type": "MERGERS_AND_ACQUISITIONS",
        "status": "COMPLETED",
        "effectiveDate": "2024-10-24T22:00:00Z",
        "effectiveDay": "2024-10-25",
    }
    holdco.setdefault("annotations", []).append(event)
    holdco["annotations"].append(
        former_name_annotation(1, "Example Group Holdings Limited", until="2019-06-30")
    )
    opco = make_entity_statement(
        source_id="companies_house",
        local_id="DD-OPCO",
        name="Example Trading Limited",
        jurisdiction=("United Kingdom", "GB"),
        identifiers=[
            {"scheme": "GB-COH", "id": "07654321", "schemeName": "Companies House"},
            {"scheme": "WIKIDATA", "id": "Q4115189", "schemeName": "Wikidata"},
        ],
        founding_date="2005-06-01",
        dissolution_date="2024-01-31",
        source_url="https://example.invalid/opco",
    )
    opco["recordDetails"]["primaryIndustryCode"] = "702200"
    refresh_statement_id(opco)
    owner = make_person_statement(
        source_id="companies_house",
        local_id="DD-OWNER",
        full_name="Alex Example",
        nationalities=[{"name": "United Kingdom", "code": "GB"}],
        birth_date="1970-04",
        addresses=[_addr("service", "3 Service Lane, Bristol", "GB")],
        identifiers=[
            {"scheme": "GBR-TAXID", "id": "AB123456C", "schemeName": "National Insurance number"},
            {"scheme": "WIKIDATA", "id": "Q4115189", "schemeName": "Wikidata"},
        ],
        other_names=[("alternative", "A. Example")],
        political_exposure={
            "status": "isPep",
            "details": [{
                "reason": "Declared a public official by the register.",
                "jurisdiction": {"name": "United Kingdom", "code": "GB"},
            }],
        },
        source_url="https://example.invalid/owner",
    )
    officer = make_person_statement(
        source_id="companies_house",
        local_id="DD-OFFICER",
        full_name="Sam Sample",
        source_url="https://example.invalid/officer",
    )
    shareholding = make_relationship_statement(
        source_id="companies_house",
        local_id="DD-OWNER-OPCO",
        subject_statement_id=opco["recordId"],
        interested_party_statement_id=owner["recordId"],
        interested_party_type="person",
        interests=[
            {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
                "share": {"minimum": 25, "maximum": 50, "exclusiveMinimum": True},
                "startDate": "2016-04-06",
                "details": "Ownership of shares – More than 25% but not more than 50%",
            },
            {"type": "votingRights", "directOrIndirect": "direct", "startDate": "2016-04-06"},
        ],
        source_url="https://example.invalid/psc",
    )
    filing = {
        "event": "notification", "match": "filing", "declarant": "company",
        "type": "PSC01", "transactionId": "MzAwMDAwMDAwMGV4YW1wbGU",
        "date": "2016-06-30", "documentUrl": "https://example.invalid/psc01.pdf",
    }
    notified = commenting(
        "/recordDetails/interestedParty",
        describe(filing, {"notified_on": "2016-04-06"}),
    )
    notified["url"] = filing["documentUrl"]
    notified[FILING_PROPERTY] = filing
    shareholding.setdefault("annotations", []).append(notified)
    consolidation = make_relationship_statement(
        source_id="gleif",
        local_id="DD-HOLDCO-OPCO",
        subject_statement_id=opco["recordId"],
        interested_party_statement_id=holdco["recordId"],
        interested_party_type="entity",
        interests=[
            {
                "type": "shareholding",
                "directOrIndirect": "indirect",
                "share": {"exact": 100},
                "startDate": "2010-01-01",
                "endDate": "2023-12-31",
            }
        ],
    )
    appointment = make_relationship_statement(
        source_id="companies_house",
        local_id="DD-OFFICER-OPCO",
        subject_statement_id=opco["recordId"],
        interested_party_statement_id=officer["recordId"],
        interested_party_type="person",
        interests=[
            mark_officer(
                {"type": "otherInfluenceOrControl", "startDate": "2019-09-01"},
                "Secretary",
            )
        ],
    )
    appointment.setdefault("annotations", []).append(role_annotation(
        "/recordDetails/interestedParty",
        Verification(statement_on="2026-03-18", route=ROUTE_COMPANIES_HOUSE),
    ))
    directorship = make_relationship_statement(
        source_id="companies_house",
        local_id="DD-OWNER-DIRECTOR",
        subject_statement_id=opco["recordId"],
        interested_party_statement_id=owner["recordId"],
        interested_party_type="person",
        interests=[{"type": "seniorManagingOfficial", "startDate": "2005-06-01", "details": "Director"}],
    )
    directorship.setdefault("annotations", []).append(
        entry_date_as_start("The example register", "registered_on")
    )
    withheld = make_relationship_statement(
        source_id="companies_house",
        local_id="DD-WITHHELD-OPCO",
        subject_statement_id=opco["recordId"],
        interested_party_unspecified={
            "reason": "informationUnknownToPublisher",
            "description": "Corporate owner's name not published by the register.",
        },
        interests=[{"type": "shareholding", "directOrIndirect": "unknown"}],
    )
    bundle = [holdco, opco, owner, officer, shareholding, consolidation, appointment, directorship, withheld]
    # The PermID listing, through the code that adds it in production — a
    # ``linking`` annotation and BODS ``publicListing`` on the GLEIF statement.
    return apply_to_bods(bundle, "52990000EXAMPLE00001", {
        "status": "listed",
        "quote": {"ticker": "EXH", "mic": "XLON", "url": "https://example.invalid/quote"},
        "exchange": {"name": "London Stock Exchange", "country": "GB"},
        "as_of": "2026-11-04",
    })


# ---------------------------------------------------------------------------
# Source harvesting (ast)
# ---------------------------------------------------------------------------

_PACKAGE_ROOT = Path(__file__).resolve().parent


def _module_source(dotted: str) -> tuple[str, list[str]]:
    path = _PACKAGE_ROOT.joinpath(*dotted.split(".")).with_suffix(".py")
    text = path.read_text(encoding="utf-8")
    return text, text.splitlines()


def _leading_comment(lines: list[str], lineno: int) -> str:
    """The contiguous ``#:`` / ``#`` comment lines directly above line *lineno* (1-based)."""
    out: list[str] = []
    i = lineno - 2
    while i >= 0:
        stripped = lines[i].strip()
        if stripped.startswith("#:"):
            out.append(stripped[2:].strip())
        elif stripped.startswith("#"):
            out.append(stripped[1:].strip())
        else:
            break
        i -= 1
    return " ".join(reversed(out)).strip()


def _trailing_comment(line: str) -> str:
    marker = "  #:"
    if marker in line:
        return line.split(marker, 1)[1].strip()
    return ""


def _field_description_kw(node: ast.AST | None) -> str:
    """``Field(description=...)`` on an annotated assignment's value, if any."""
    if not isinstance(node, ast.Call):
        return ""
    func = node.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
    if name != "Field":
        return ""
    for kw in node.keywords:
        if kw.arg == "description":
            try:
                value = ast.literal_eval(kw.value)
            except ValueError:
                return ""
            return str(value)
    return ""


def _default_text(node: ast.AST | None) -> str:
    if node is None:
        return "required"
    if isinstance(node, ast.Call):
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        if name == "Field":
            for kw in node.keywords:
                if kw.arg == "default":
                    return ast.unparse(kw.value)
                if kw.arg == "default_factory":
                    return f"{ast.unparse(kw.value)}()"
            return "required"
        if name == "field":
            for kw in node.keywords:
                if kw.arg == "default_factory":
                    return f"{ast.unparse(kw.value)}()"
                if kw.arg == "default":
                    return ast.unparse(kw.value)
            return "required"
    return ast.unparse(node)


@dataclasses.dataclass
class FieldDoc:
    name: str
    type: str
    default: str
    description: str


@dataclasses.dataclass
class ModelDoc:
    module: str
    name: str
    doc: str
    fields: list[FieldDoc]
    bases: list[str]


def harvest_models(dotted: str, names: Iterable[str]) -> list[ModelDoc]:
    """Field docs for the named classes in module *dotted*, from its source.

    A field's description is, in order: ``Field(description=)``, the ``#:`` /
    ``#`` comment block above it, a trailing ``  #:`` comment. Inherited
    fields are not repeated — the base is named instead.
    """
    text, lines = _module_source(dotted)
    tree = ast.parse(text)
    wanted = set(names)
    found: dict[str, ModelDoc] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name not in wanted:
            continue
        fields: list[FieldDoc] = []
        for item in node.body:
            if not isinstance(item, ast.AnnAssign) or not isinstance(item.target, ast.Name):
                continue
            if item.target.id.startswith("_"):
                continue
            description = (
                _field_description_kw(item.value)
                or _leading_comment(lines, item.lineno)
                or _trailing_comment(lines[item.lineno - 1])
            )
            fields.append(
                FieldDoc(
                    name=item.target.id,
                    type=ast.unparse(item.annotation),
                    default=_default_text(item.value),
                    description=description,
                )
            )
        found[node.name] = ModelDoc(
            module=dotted,
            name=node.name,
            doc=(ast.get_docstring(node) or "").strip(),
            fields=fields,
            bases=[ast.unparse(b) for b in node.bases],
        )
    missing = wanted - found.keys()
    if missing:
        raise LookupError(f"{dotted}: no class named {sorted(missing)}")
    return [found[n] for n in names]


#: The response models the API section documents, in reading order:
#: (module, class names, section heading, one-line context).
API_MODELS: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    (
        "routers.lookup",
        ("LookupResponse", "ReportResponse", "RegisterLookupResponse"),
        "`GET /lookup` and `GET /lookup-register`",
        "The folded form of a run. `/lookup-stream` and `/lookup-register-stream` carry the same payload as server-sent events (one event per field group); `LookupResponse` is the fold of those events, and a saved report replays exactly this.",
    ),
    (
        "sources.base",
        ("SourceHit", "SourceInfo"),
        "`hits[]` and `GET /sources`",
        "Every source that answered contributes one `SourceHit` to `hits[]`; `SourceInfo` is the static metadata `/sources` lists per adapter.",
    ),
    (
        "routers.lookup",
        ("DeepenResponse", "LookupSourceResponse"),
        "`GET /deepen` and `GET /lookup-source`",
        "One source's full record (the Data drawer) and the per-source retry.",
    ),
    (
        "routers.search",
        ("SearchResponse",),
        "`GET /search`",
        "A ranked name search across sources.",
    ),
    (
        "routers.subsidiaries",
        ("SubsidiariesResponse", "SubChild", "SubJurisdiction", "DeclaredSubsidiariesResponse", "DeclaredSource", "DeclaredRow"),
        "`GET /subsidiaries` and `GET /subsidiaries/declared`",
        "What the company consolidates (GLEIF Level 2) and the declared lists other sources hold.",
    ),
    (
        "routers.history",
        ("HistoryResponse", "HistoryEntry", "RawChange"),
        "`GET /history`",
        "The Time Machine: how the company's records changed, merged across every register that keeps a change log.",
    ),
    (
        "routers.securities",
        ("SecuritiesResponse", "Security"),
        "`GET /securities`",
        "The ISINs GLEIF maps to the LEI.",
    ),
    (
        "routers.knowability",
        ("KnowabilityResponse",),
        "`GET /knowability`",
        "What is knowable about beneficial owners in a jurisdiction — a dated statement, never a signal.",
    ),
    (
        "routers.person_check",
        ("PersonCheckResponse", "CheckedSource", "PersonMatch", "PersonAppointmentsResponse", "AppointmentItem", "PersonPositionsResponse", "PositionItem", "LegalPersonRelationsResponse", "LegalPersonCompany", "LegalPersonRelation"),
        "`GET /person-check`, `/person-appointments`, `/person-positions`, `/legal-person-relations`",
        "BackgroundCheck: a person screened by name across the watchlist sources, and the other companies a register names them on.",
    ),
    (
        "routers.national_id",
        ("ResolveNationalIdResponse", "NationalIdMatch"),
        "`GET /resolve-national-id`",
        "A register number resolved to LEIs through GLEIF's reverse lookup.",
    ),
    (
        "routers.narrative",
        ("NarrativeResponse",),
        "`GET /narrative`",
        "The AI summary written from the run's evidence packet.",
    ),
    (
        "routers.nz_associations",
        ("PersonAssociationsResponse", "PersonAssociations", "AssociatedCompany"),
        "`GET /nz-associations/person`",
        "A New Zealand role holder's other companies, graded by address match.",
    ),
    (
        "routers.leads",
        ("LeadsResponse", "LeadCandidate", "LeadParent"),
        "`GET /leads`",
        "Candidate parents and owners a lookup did not confirm.",
    ),
)


#: Models whose every field must carry a description — the rendering of one
#: of these with an undescribed field is a test failure
#: (``tests/test_data_dictionary.py``). Widen it as models are documented.
STRICT_MODELS: frozenset[str] = frozenset({
    "LookupResponse",
    "ReportResponse",
    "RegisterLookupResponse",
    "SourceHit",
    "SourceInfo",
    "DeepenResponse",
    "LookupSourceResponse",
    "SearchResponse",
    "RiskSignal",
    "DegradedSource",
    "CrossSourceLink",
    "PossiblySame",
})


def undescribed_fields(models: Iterable[ModelDoc]) -> list[str]:
    """``Model.field`` for every field of a strict model with no description."""
    return [
        f"{m.name}.{f.name}"
        for m in models
        if m.name in STRICT_MODELS
        for f in m.fields
        if not f.description
    ]


def _with_descriptions(model: ModelDoc, words: dict[str, str]) -> ModelDoc:
    """*model* with *words* filling any field that has no source comment."""
    return dataclasses.replace(
        model,
        fields=[
            dataclasses.replace(f, description=f.description or words.get(f.name, ""))
            for f in model.fields
        ],
    )


def harvest_dataclass(cls: type) -> ModelDoc:
    """Field docs for a dataclass from its source (``#:`` comments)."""
    source = inspect.getsource(cls)
    lines = source.splitlines()
    tree = ast.parse(source)
    node = tree.body[0]
    assert isinstance(node, ast.ClassDef)
    fields: list[FieldDoc] = []
    for item in node.body:
        if not isinstance(item, ast.AnnAssign) or not isinstance(item.target, ast.Name):
            continue
        fields.append(
            FieldDoc(
                name=item.target.id,
                type=ast.unparse(item.annotation),
                default=_default_text(item.value),
                description=_leading_comment(lines, item.lineno)
                or _trailing_comment(lines[item.lineno - 1]),
            )
        )
    return ModelDoc(
        module=cls.__module__,
        name=cls.__name__,
        doc=(cls.__doc__ or "").strip(),
        fields=fields,
        bases=[],
    )


#: Module aliases the MCP tool bodies call into, so a ``return
#: shaping.shape_lookup(resp)`` or ``_owners.owners_answer(...)`` is followed
#: into the function that builds the dict.
_FOLLOW_MODULES: dict[str, str] = {"shaping": "mcp.shaping", "_owners": "owners"}

#: Tools whose answer is a model dump or a helper the harvester cannot follow —
#: a sentence saying what it is, instead of a key list.
MCP_RESPONSE_NOTES: dict[str, str] = {
    "opencheck_resolve_national_id": "The `ResolveNationalIdResponse` object (API responses above).",
    "opencheck_person_positions": (
        "For a Companies House officer id, the `PersonAppointmentsResponse` object "
        "without `bods`, cut to the first 100 appointments with `truncated: true` when "
        "cut; for an Estonian person key, the `PersonPositionsResponse` object; for an "
        "Estonian legal person, the `LegalPersonRelationsResponse` object (API "
        "responses above). `error` on a refused call."
    ),
    "opencheck_subsidiaries": (
        "The `SubsidiariesResponse` object (API responses above) plus `statement_count` "
        "(for `format=bods`), `measures` (a sentence saying what GLEIF Level 2 "
        "measures), `declared` (the `DeclaredSubsidiariesResponse` object when "
        "`include_declared`) and `license_notices`. `error` on a refused call."
    ),
}

#: Keys that only an error answer carries, listed after the others.
_ERROR_KEYS = ("error", "status", "code")


def _walk_body(fn: ast.AST) -> Iterator[ast.AST]:
    """``ast.walk`` that does not descend into nested functions or lambdas —
    their returns are theirs, not the enclosing function's."""
    stack = list(reversed(getattr(fn, "body", [])))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        yield node
        stack.extend(reversed(list(ast.iter_child_nodes(node))))


class _KeyHarvester:
    """Top-level keys of the dict a function returns, read from its body.

    Follows: a dict literal returned directly; a name assigned a dict literal
    (plus ``name["k"] = …`` and ``name.update({...})``) and then returned; a
    ``{**call()}`` spread or a ``return call()`` into a function in the same
    module or one of :data:`_FOLLOW_MODULES`.
    """

    def __init__(self) -> None:
        self._modules: dict[str, ast.Module] = {}

    def module(self, dotted: str) -> ast.Module:
        if dotted not in self._modules:
            text, _ = _module_source(dotted)
            self._modules[dotted] = ast.parse(text)
        return self._modules[dotted]

    def function(self, dotted: str, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
        for node in self.module(dotted).body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                return node
        return None

    def _call_keys(self, call: ast.Call, dotted: str, depth: int) -> list[str] | None:
        func = call.func
        if isinstance(func, ast.Name):
            target_module, name = dotted, func.id
        elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            alias = func.value.id
            if alias not in _FOLLOW_MODULES:
                return None
            target_module, name = _FOLLOW_MODULES[alias], func.attr
        else:
            return None
        if name == "_err":
            return list(_ERROR_KEYS[:2])
        target = self.function(target_module, name)
        if target is None or depth > 3:
            return None
        return self.keys(target, target_module, depth + 1)

    def _dict_keys(self, node: ast.Dict, dotted: str, depth: int) -> list[str]:
        keys: list[str] = []
        for k, v in zip(node.keys, node.values, strict=True):
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.append(k.value)
            elif k is None:
                spread = self._value_keys(v, {}, dotted, depth)
                keys.extend(spread or ["…"])
            else:
                keys.append(ast.unparse(k))
        return keys

    def _value_keys(
        self, node: ast.AST, env: dict[str, list[str]], dotted: str, depth: int
    ) -> list[str] | None:
        if isinstance(node, ast.Dict):
            return self._dict_keys(node, dotted, depth)
        if isinstance(node, ast.Name):
            return env.get(node.id)
        if isinstance(node, ast.Call):
            return self._call_keys(node, dotted, depth)
        return None

    def keys(
        self, fn: ast.FunctionDef | ast.AsyncFunctionDef, dotted: str, depth: int = 0
    ) -> list[str]:
        env: dict[str, list[str]] = {}
        found: list[str] = []

        def add(keys: list[str] | None) -> None:
            for k in keys or []:
                if k not in found:
                    found.append(k)

        for node in _walk_body(fn):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    value = self._value_keys(node.value, env, dotted, depth)
                    if value is not None:
                        env[target.id] = list(value)
                elif (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and isinstance(target.slice, ast.Constant)
                    and isinstance(target.slice.value, str)
                ):
                    env.setdefault(target.value.id, []).append(target.slice.value)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                value = self._value_keys(node.value, env, dotted, depth)
                if value is not None:
                    env[node.target.id] = list(value)
            elif (
                isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == "update"
                and isinstance(node.value.func.value, ast.Name)
                and node.value.args
            ):
                extra = self._value_keys(node.value.args[0], env, dotted, depth)
                env.setdefault(node.value.func.value.id, []).extend(extra or [])
            elif isinstance(node, ast.Return) and node.value is not None:
                add(self._value_keys(node.value, env, dotted, depth))
        # Error-only keys last.
        ordinary = [k for k in found if k not in _ERROR_KEYS]
        errors = [k for k in _ERROR_KEYS if k in found]
        return ordinary + errors


@dataclasses.dataclass
class ToolDoc:
    name: str
    summary: str
    args: list[str]
    keys: list[str]
    content_blocks: bool


def list_objects() -> list[ModelDoc]:
    """The dataclasses behind ``cross_source_links[]`` and ``possibly_same_entities[]``."""
    from .reconcile import CrossSourceLink, PossiblySame

    link = _with_descriptions(
        harvest_dataclass(CrossSourceLink),
        {
            "key": "Which identifier the hits share — `lei`, `gb_coh`, `wikidata_qid`, `opensanctions_id` … (a `SourceHit.identifiers` key), or `name_dob` for a person match.",
            "key_value": "The shared value.",
            "confidence": "`strong` — a shared structured identifier; `possible` — a name + birth-year match between persons.",
            "hits": "The hits bridged, each reduced to `source_id`, `hit_id` and `name`. The serialised link also carries `independent_source_count`: how many independent origins the hits represent once source lineage is applied — the number to count as corroboration, never `len(hits)`.",
        },
    )
    same = _with_descriptions(
        harvest_dataclass(PossiblySame),
        {
            "a": "The `statementId` of the first entity statement.",
            "b": "The `statementId` of the second.",
            "reason": "Why they are offered as the same — a name match within one jurisdiction with no shared identifier.",
            "a_name": "The first statement's name, so the report can render the pair without the bundle.",
            "b_name": "The second statement's name.",
            "jurisdiction": "The jurisdiction code both carry.",
            "a_source": "The source that asserted the first record.",
            "b_source": "The source that asserted the second — \"GLEIF vs OpenCorporates\" reads very differently from \"OpenAleph vs OpenAleph\".",
        },
    )
    return [link, same]


def signal_models() -> tuple[ModelDoc, ModelDoc]:
    """``RiskSignal`` and ``DegradedSource``, described."""
    from . import risk

    sig = _with_descriptions(
        harvest_dataclass(risk.RiskSignal),
        {
                "code": "The signal code (`SANCTIONED`, `RELATED_PEP`, `COMPLEX_OWNERSHIP_LAYERS` …) — see [Risk signals](risk-signals.md).",
                "confidence": "`high`, `medium` or `low`. For a name match it is the lower of the rule's confidence and the match's; an uncorroborated name match never reads `high`.",
                "summary": "One sentence stating the finding, built from the evidence. The verdict and the AI summary quote it.",
                "source_id": "The source whose data the rule keyed off (`opensanctions`, `icij`, `gleif` …), or `opencheck` for a rule computed over the merged graph.",
                "hit_id": "The hit the signal is attached to — a source's record id, or the subject's LEI for a graph-level rule.",
                "evidence": "The raw bits the rule keyed off, so a reader can see why without re-running it. Keys vary by code; the ones that name BODS statements (`statement_id`, `subject_statement_id(s)`, `matches[].statement_id`, `jurisdictions[].statement_id`, `longest_path[]`, `ended_relationship_statement_ids`) are resolved to `statementId`s and drive the graph badges.",
        },
    )
    deg = harvest_dataclass(risk.DegradedSource)
    deg = dataclasses.replace(
        deg,
        fields=[
            dataclasses.replace(f, description="One of the `reason` values below.")
            if f.name == "reason"
            else f
            for f in deg.fields
        ],
    )
    return sig, deg


def documented_models() -> list[ModelDoc]:
    """Every model the dictionary renders, as rendered — what the strict test reads."""
    models: list[ModelDoc] = []
    for module, names, _heading, _context in API_MODELS:
        models.extend(harvest_models(module, names))
    models.extend(list_objects())
    models.extend(signal_models())
    return models


def harvest_mcp_tools() -> list[ToolDoc]:
    """Every ``@mcp.tool`` in ``mcp/server.py``: its first docstring line,
    its arguments and the top-level keys of the dict it returns."""
    harvester = _KeyHarvester()
    server = harvester.module("mcp.server")
    tools: list[ToolDoc] = []
    for node in server.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        decorated = False
        structured = True
        for dec in node.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            if isinstance(target, ast.Attribute) and target.attr == "tool":
                decorated = True
                if isinstance(dec, ast.Call):
                    for kw in dec.keywords:
                        if kw.arg == "structured_output" and isinstance(kw.value, ast.Constant):
                            structured = bool(kw.value.value)
        if not decorated:
            continue
        doc = ast.get_docstring(node) or ""
        summary = doc.split("\n\n", 1)[0].replace("\n", " ").strip()
        args = [a.arg for a in node.args.args if a.arg != "self"]
        keys = [] if node.name in MCP_RESPONSE_NOTES else harvester.keys(node, "mcp.server")
        tools.append(
            ToolDoc(
                name=node.name,
                summary=summary,
                args=args,
                keys=keys,
                content_blocks=not structured,
            )
        )
    return tools


# ---------------------------------------------------------------------------
# Observed export keys
# ---------------------------------------------------------------------------


def _observed_senzing_keys(bundle: list[dict[str, Any]]) -> list[str]:
    from .bods.senzing import map_to_senzing

    keys: list[str] = []
    for record in map_to_senzing(bundle):
        for k in record:
            if k not in keys:
                keys.append(k)
        for feature in record.get("FEATURES", []):
            for k in feature:
                if k not in keys:
                    keys.append(k)
    return keys


def _observed_ftm_keys(bundle: list[dict[str, Any]]) -> list[str]:
    from .bods.ftm import map_to_ftm

    keys: list[str] = []
    for entity in map_to_ftm(bundle):
        for k in entity:
            if k not in keys:
                keys.append(k)
        for k in entity.get("properties", {}):
            if k not in keys:
                keys.append(k)
    return keys


def _observed_aml_ai_keys(bundle: list[dict[str, Any]]) -> dict[str, list[str]]:
    from .bods.aml_ai import map_to_aml_ai

    out: dict[str, list[str]] = {}
    for table, rows in map_to_aml_ai(bundle).items():
        keys: list[str] = []
        for row in rows:
            for k in row:
                if k not in keys:
                    keys.append(k)
        out[f"{table}.ndjson"] = keys
    return out


def _gql_columns() -> dict[str, list[str]]:
    from bods_gql.converter.mapper import EntityNode, OwnershipEdge, PersonNode

    return {
        "entity_nodes.csv": [f.name for f in dataclasses.fields(EntityNode)],
        "person_nodes.csv": [f.name for f in dataclasses.fields(PersonNode)],
        "ownership_edges.csv": [f.name for f in dataclasses.fields(OwnershipEdge)],
    }


class UndescribedColumnError(LookupError):
    """The code produced a column or key :data:`COLUMN_DESCRIPTIONS` lacks."""


def _describe(table: str, columns: Iterable[str]) -> list[tuple[str, str]]:
    words = COLUMN_DESCRIPTIONS.get(table, {})
    rows: list[tuple[str, str]] = []
    missing: list[str] = []
    for col in columns:
        if col not in words:
            missing.append(col)
        else:
            rows.append((col, words[col]))
    if missing:
        raise UndescribedColumnError(
            f"{table}: no description for {missing} — add it to "
            "opencheck.data_dictionary.COLUMN_DESCRIPTIONS"
        )
    return rows


def export_tables(family: str) -> list[tuple[str, list[tuple[str, str]]]]:
    """``[(table name, [(column, description), …]), …]`` for one export family."""
    bundle = sample_bundle()
    if family == "csv":
        return [(name, _describe(name, cols)) for name, cols in _gql_columns().items()]
    if family == "batch":
        from .batch import CSV_COLUMNS

        return [("rows.csv", _describe("rows.csv", CSV_COLUMNS))]
    if family == "senzing":
        return [("senzing", _describe("senzing", _observed_senzing_keys(bundle)))]
    if family == "ftm":
        return [("ftm", _describe("ftm", _observed_ftm_keys(bundle)))]
    if family == "amlai":
        return [
            (name, _describe(name, keys))
            for name, keys in _observed_aml_ai_keys(bundle).items()
        ]
    raise KeyError(family)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _cell(text: str) -> str:
    # Source comments are reStructuredText (``x``); the page is Markdown.
    return text.replace("``", "`").replace("|", "\\|").replace("\n", " ").strip()


def _code(text: str) -> str:
    return "`" + text.replace("`", "") + "`"


def _table(headers: list[str], rows: Iterable[Iterable[str]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    for row in rows:
        out.append("| " + " | ".join(_cell(c) for c in row) + " |")
    return out


def _model_section(model: ModelDoc, level: str = "####") -> list[str]:
    out = [f"{level} `{model.name}`", ""]
    if model.bases and model.bases != ["BaseModel"]:
        out.append(f"Extends {', '.join(_code(b) for b in model.bases)}; only its own fields are listed.")
        out.append("")
    if model.doc:
        out.append(model.doc.split("\n\n", 1)[0].replace("\n", " "))
        out.append("")
    if model.fields:
        out.extend(
            _table(
                ["Field", "Type", "Default", "Meaning"],
                (
                    (_code(f.name), _code(f.type), _code(f.default), f.description or "—")
                    for f in model.fields
                ),
            )
        )
    else:
        out.append("No fields of its own.")
    out.append("")
    return out


def _vocabulary_rows(mapping: dict[str, str]) -> list[list[str]]:
    return [[_code(k), v] for k, v in mapping.items()]


def render_export_notes(family: str, *, heading_level: int = 2) -> str:
    """The tables of one export family, for the ``DATA-DICTIONARY.md`` in a zip."""
    label, blurb = next((lab, bl) for fam, lab, bl in EXPORT_FAMILIES if fam == family)
    h = "#" * heading_level
    out = [f"{h} {label}", "", blurb, ""]
    for table, rows in export_tables(family):
        out.append(f"{'#' * (heading_level + 1)} `{table}`")
        out.append("")
        out.extend(_table(["Column", "Meaning"], ((_code(c), d) for c, d in rows)))
        out.append("")
    return "\n".join(out)


def render_zip_dictionary(family: str) -> str:
    """The ``DATA-DICTIONARY.md`` shipped inside an export zip."""
    return "\n".join(
        [
            "# OpenCheck export — data dictionary",
            "",
            "What each column in this package means. The full dictionary — the "
            "BODS extension keys, annotations, risk signals and every API "
            f"response — is at {DOC_URL}, generated from the same code.",
            "",
            render_export_notes(family),
        ]
    )


def render_markdown() -> str:
    """The whole of ``docs/data-dictionary.md``."""
    from . import risk
    from .bods import annotations as ann
    from .bods.officer_roles import OFFICER_POSITION_KEY
    from .degradation import CHECK_SOURCE_FETCH, CHECK_SOURCE_READ

    out: list[str] = []
    w = out.append
    w("# Output data dictionary")
    w("")
    w(
        "What every field OpenCheck writes means — the API responses, the BODS "
        "statements and the keys OpenCheck adds to them, the annotations, the "
        "risk-signal and degradation objects, the export tables and the MCP "
        "tool responses. Inputs and concepts have their own pages: "
        "[How it works](how-it-works.md), [Dates](dates.md), "
        "[Identifiers](identifiers.md), [Risk signals](risk-signals.md), "
        "[Record consistency](record-consistency.md), [MCP server](mcp-server.md)."
    )
    w("")
    w(
        "**Generated from the code** by `backend/scripts/generate_data_dictionary.py` "
        "(`opencheck/data_dictionary.py`): the field tables are read from the "
        "Pydantic models and dataclasses, the vocabularies from their constants, "
        "the export columns from the writers, and the MCP keys from the tool "
        "bodies. CI fails when this file is stale (`--check`), when a column or "
        "`opencheck*` key appears in output without a description, and when a "
        "mapper emits an unregistered extension key. Do not edit it by hand."
    )
    w("")
    w("Contents: [API responses](#api-responses) · [BODS statements](#bods-statements) · "
      "[Risk signals and degraded sources](#risk-signals-and-degraded-sources) · "
      "[Exports](#exports) · [MCP tools](#mcp-tools)")
    w("")

    # ---- API ------------------------------------------------------------
    w("## API responses")
    w("")
    w(
        "Every response is JSON. `Type` is the Python annotation; `list[dict[str, Any]]` "
        "fields hold the objects described elsewhere on this page — `risk_signals[]` is a "
        "[RiskSignal](#risksignal), `degraded_sources[]` a [DegradedSource](#degradedsource), "
        "`bods[]` a [BODS statement](#bods-statements). A `Default` of `required` means the "
        "field is always present; any other value is what a payload recorded before the "
        "field existed carries."
    )
    w("")
    for module, names, heading, context in API_MODELS:
        w(f"### {heading}")
        w("")
        w(context)
        w("")
        for model in harvest_models(module, names):
            out.extend(_model_section(model))
    w("### Objects inside lists")
    w("")
    w(
        "The dict shapes the `list[dict[str, Any]]` fields above carry, where they are "
        "not a BODS statement or a signal."
    )
    w("")
    for model in list_objects():
        out.extend(_model_section(model))

    # ---- BODS -----------------------------------------------------------
    w("## BODS statements")
    w("")
    w(
        "`bods[]`, `/export` and the MCP export carry [BODS v0.4](https://standard.openownership.org/en/0.4.0/) "
        "statements — the standard's own schema documents their fields. What this page "
        "adds is what OpenCheck puts in them that the standard does not define, and how "
        "it uses the constructs the standard leaves open."
    )
    w("")
    w("### Identifiers and dates")
    w("")
    w(
        "`statementId` is content-addressed and `recordId` is the stable identity a "
        "relationship references; `declarationSubject` names each source's own subject. "
        "[Identifiers](identifiers.md) explains the ids; [Dates](dates.md) explains "
        "`statementDate`, `publicationDate`, `source.retrievedAt` and the interest dates."
    )
    w("")
    w("### Extension keys")
    w("")
    w(
        "Keys BODS v0.4 does not define. Its objects are open, and libcovebods reports "
        "an unknown key as an additional field, not an error. Every such key OpenCheck "
        "writes is registered in `opencheck.data_dictionary.EXTENSIONS`: the test suite "
        "walks every statement every mapper produces against the v0.4 schema, and a key "
        "the schema does not define at that place that is not registered here fails it — "
        "whatever it is called. Statements another publisher made (the OECD's MEIP, Open "
        "Ownership's bundles) are carried as published and documented by them."
    )
    w("")
    out.extend(
        _table(
            ["Key", "Where", "Type", "Meaning", "Example", "Since"],
            (
                (_code(e["key"]), e["where"], e["type"], e["description"], _code(e["example"]), e["since"])
                for e in EXTENSIONS
            ),
        )
    )
    w("")
    assert OFFICER_POSITION_KEY in EXTENSION_KEYS
    w("### Annotations")
    w("")
    w(ann.__doc__.strip().split("\n\n", 2)[1].replace("\n", " ") if ann.__doc__ else "")
    w("")
    w(
        "One rule applies to every annotation: **the statement carries the usable value; "
        "the annotation carries the register's words.** An annotation object has "
        "`statementPointerTarget` (an RFC 6901 pointer into its own statement — never a "
        "prefix, never `/`), `motivation`, `description`, optionally `transformedContent`, "
        "`creationDate` and `createdBy` (`{\"name\": \"OpenCheck\", \"uri\": \"https://opencheck.world\"}`)."
    )
    w("")
    w("#### Motivations")
    w("")
    missing = sorted(ann.MOTIVATIONS - MOTIVATION_WORDS.keys())
    assert not missing, missing
    out.extend(_table(["Motivation", "When OpenCheck writes it"], _vocabulary_rows(MOTIVATION_WORDS)))
    w("")
    w("#### Annotation helpers")
    w("")
    w("The functions in `opencheck/bods/annotations.py` that build the annotations OpenCheck writes, with what each records.")
    w("")
    helper_rows = []
    for name in (
        "transformation",
        "identifying",
        "commenting",
        "entry_date_as_start",
        "date_rounding_annotation",
        "person_identifier_note",
    ):
        fn = getattr(ann, name)
        doc = (fn.__doc__ or "").strip().split("\n\n", 1)[0].replace("\n", " ")
        helper_rows.append([_code(name + "()"), doc])
    out.extend(_table(["Helper", "Records"], helper_rows))
    w("")

    # ---- Signals ------------------------------------------------------------
    w("## Risk signals and degraded sources")
    w("")
    w(
        "`risk_signals[]` on a lookup, a report, a deepen and the `risk_signals` stream "
        "event. The codes and their rules are in [Risk signals](risk-signals.md); this "
        "is the object each one is."
    )
    w("")
    sig, deg = signal_models()
    out.extend(_model_section(sig, level="###"))
    w("Closed vocabularies on a signal:")
    w("")
    out.extend(
        _table(
            ["Field", "Values"],
            [
                [_code("kind"), "`risk` — an adverse finding, counted in \"N risk signals\"; `context` — a structural observation worth showing, never counted as a finding."],
                [_code("confidence"), "`high`, `medium`, `low`."],
            ],
        )
    )
    w("")
    out.extend(_model_section(deg, level="###"))
    w(
        "An empty `risk_signals` beside a non-empty `degraded_sources` is **not** a clean "
        "screen: a degradation says a screen or source did not fully run, and every "
        "reader — the verdict, the MCP summary, the batch row — treats it as a check that "
        "did not happen."
    )
    w("")
    w("#### `reason`")
    w("")
    out.extend(_table(["Value", "Meaning"], _vocabulary_rows(risk.DEGRADATION_REASON_LABELS)))
    w("")
    w("#### `check`")
    w("")
    check_rows = {
        CHECK_SOURCE_FETCH: "The source itself did not answer, so none of its records were consulted.",
        CHECK_SOURCE_READ: "The source returned a record and then failed while it was being read in full — a partial answer.",
        **{k: f"{v} did not fully run." for k, v in risk.DEGRADED_CHECK_LABELS.items()},
    }
    out.extend(_table(["Value", "Meaning"], _vocabulary_rows(check_rows)))
    w("")

    # ---- Exports --------------------------------------------------------
    w("## Exports")
    w("")
    w(
        "`GET /export?format=` and the MCP `opencheck_export_bods` serve the same bundle in "
        "every format. `json`, `jsonl`, `xml` and `zip` are the BODS statements themselves "
        "(the zip adds a manifest and `LICENSES.md`); `rdf` is BODS as TriG under the "
        "[BODS RDF vocabulary](https://vocab.openownership.org/); `cypher` is a Neo4j "
        "script with one node per party and one relationship per interest. The tabular "
        "families below are documented column by column, and each zip carries this "
        "section as `DATA-DICTIONARY.md`."
    )
    w("")
    for family, _label, _blurb in EXPORT_FAMILIES:
        w(render_export_notes(family, heading_level=3))

    # ---- MCP ------------------------------------------------------------
    w("## MCP tools")
    w("")
    w(
        "Each tool, what it does and the top-level keys of the object it returns, read "
        "from `opencheck/mcp/server.py` and the shaping functions it delegates to. Keys "
        "shared with the API carry the meaning given above; `hint` is a sentence for the "
        "calling agent. [MCP server](mcp-server.md) covers transport and tiers."
    )
    w("")
    for tool in harvest_mcp_tools():
        w(f"### `{tool.name}`")
        w("")
        w(tool.summary)
        w("")
        w(f"Arguments: {', '.join(_code(a) for a in tool.args) or 'none'}.")
        w("")
        if tool.content_blocks:
            w("Returns content blocks — a JSON text block and an SVG image — not a structured object.")
        elif tool.name in MCP_RESPONSE_NOTES:
            w(MCP_RESPONSE_NOTES[tool.name])
        if tool.keys:
            ordinary = [k for k in tool.keys if k not in _ERROR_KEYS]
            errors = [k for k in tool.keys if k in _ERROR_KEYS]
            w(f"Keys: {', '.join(_code(k) for k in ordinary)}.")
            if errors:
                w("")
                w(f"On a refused call: {', '.join(_code(k) for k in errors)}.")
        w("")
    return "\n".join(out).rstrip() + "\n"


def repo_doc_path() -> Path:
    return _PACKAGE_ROOT.parent.parent / DOC_PATH


def is_stale() -> bool:
    """True when the committed ``docs/data-dictionary.md`` differs from the rendering."""
    path = repo_doc_path()
    if not path.exists():
        return True
    return path.read_text(encoding="utf-8") != render_markdown()


__all__ = [
    "COLUMN_DESCRIPTIONS",
    "DOC_PATH",
    "DOC_URL",
    "EXPORT_FAMILIES",
    "EXTENSIONS",
    "EXTENSION_KEYS",
    "UndescribedColumnError",
    "export_tables",
    "extension_keys_in",
    "harvest_dataclass",
    "harvest_mcp_tools",
    "MCP_RESPONSE_NOTES",
    "MOTIVATION_WORDS",
    "NOT_WRITTEN",
    "motivations_written",
    "documented_models",
    "harvest_models",
    "is_stale",
    "list_objects",
    "undescribed_fields",
    "render_export_notes",
    "render_markdown",
    "render_zip_dictionary",
    "repo_doc_path",
    "sample_bundle",
    "signal_models",
    "STRICT_MODELS",
    "unregistered_extension_keys",
]
