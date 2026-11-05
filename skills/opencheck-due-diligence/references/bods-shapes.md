# BODS v0.4 as OpenCheck writes it

`opencheck_export_bods` (format `json` or `jsonl`), the `bods` part of
`opencheck_subsidiaries(format="bods")` and the `*.bods.jsonl.gz` /
`*.jsonl.gz` files in the bulk datasets are lists of BODS v0.4 statements.
The standard: https://standard.openownership.org/en/0.4.0/. Field-by-field
reference for OpenCheck's additions: `docs/data-dictionary.md` in the repo.

## Contents
1. The three statement types
2. Ids: statementId, recordId, declarationSubject
3. Relationships and interests
4. Dates and ended relationships
5. Unknown and withheld parties
6. Annotations and OpenCheck's extension keys
7. Common mistakes when parsing

## 1. The three statement types

Every statement has `statementId`, `recordId`, `recordType`, `recordStatus`,
`statementDate`, `declarationSubject`, `source`, `publicationDetails` and
`recordDetails`.

| `recordType` | `recordDetails` holds |
|---|---|
| `entity` | `entityType.type` (`registeredEntity`, `legalEntity`, `arrangement`, `anonymousEntity`, `unknownEntity`, `state`, `stateBody`), `name`, `jurisdiction` (`{name, code}`, ISO 3166), `identifiers[]` (`{id, scheme, schemeName}`), `foundingDate`, `dissolutionDate`, `addresses[]`, `publicListing` |
| `person` | `personType` (`knownPerson`, `anonymousPerson`, `unknownPerson`), `names[]`, `nationalities[]`, `birthDate` (may be `YYYY` or `YYYY-MM`), `addresses[]` |
| `relationship` | `subject` (the company owned or controlled), `interestedParty` (the owner or controller), `interests[]`, `isComponent` |

## 2. Ids

- **Relationships reference parties by `recordId`.** `subject` and
  `interestedParty` hold a `recordId`, not a `statementId`. Build your index on
  `recordId` when you join edges to nodes.
- `statementId` is content-addressed: it changes when the record's content
  changes. `recordId` is stable across versions. In a bulk file the same
  `recordId` can therefore appear more than once; keep the latest
  `statementDate`.
- Statements from another publisher (the OECD's MEIP statements, Open
  Ownership's stored bundles) keep their own ids unchanged.
- `declarationSubject` is the `recordId` of the subject of the network a
  statement belongs to — per source in OpenCheck: each source's statements name
  that source's own record of the company. It is not a link to follow.
- Several sources describe the same company. They are separate entity
  statements (different `source.opencheckSourceId`), joined by shared
  identifiers (`GB-COH`, `XI-LEI`, …), never merged into one statement.
  Do not count entity statements as companies.

## 3. Relationships and interests

Each interest: `type`, `directOrIndirect`, `beneficialOwnershipOrControl`
(bool), `share` (`{exact}` or `{minimum, maximum, exclusiveMinimum,
exclusiveMaximum}` — PSC bands such as 25–50% are ranges, not 25 or 50),
`startDate`, `endDate`, `details`.

Common `type`s: `shareholding`, `votingRights`, `appointmentOfBoard`,
`otherInfluenceOrControl`, `seniorManagingOfficial` (directors, managing
officials), `boardMember`, `boardChair`, `unknownInterest`.

- **`beneficialOwnershipOrControl: true`** is the register declaring a
  beneficial owner. Absent or false is not "not an owner"; it means the source
  does not say.
- **GLEIF parent/child edges** (`source.opencheckSourceId: "gleif"`) describe
  accounting consolidation. Their `details` say "consolidat…"; there is no
  share. They are not ownership.
- **MEIP** edges are `unknownInterest`: group membership by statistical
  method, not ownership.
- An interest with `opencheckOfficerPosition` ("Secretary", "Authorised
  person") is an appointment typed `otherInfluenceOrControl` because BODS has no
  role code for it. It is a role, not control.

## 4. Dates

- `statementDate` is the date the source's record describes; `source.retrievedAt`
  is when OpenCheck fetched it. A bulk snapshot is dated by the register's cut.
- An ended relationship is kept, not dropped: `recordStatus: "closed"` and/or
  every interest has an `endDate` on or before today. Treat it as former. A
  closed record with no `endDate` is ended at an unknown date.

## 5. Unknown and withheld parties

`interestedParty` may be `{"unspecified": {"reason": …, "description": …}}`
instead of a `recordId`, or point at an `anonymousEntity` / `unknownPerson`.
Reasons include `subjectExemptFromDisclosure`, `interestedPartyExemptFromDisclosure`,
`informationUnknownToPublisher`, `noBeneficialOwners`. Read the reason before
describing it: an exemption is a legal status, not concealment.

## 6. Annotations and extension keys

`annotations[]` on a statement: `{statementPointerTarget, motivation,
description, …}`. The rule: the statement carries the usable value; the
annotation carries the register's own words, or a caveat. Read them before
asserting anything about the field they point at — e.g. a `commenting`
annotation on a GLEIF statement saying the LEI has lapsed, or that a
dissolution date came from a GLEIF event.

Extension keys you will meet:

| Key | Where | Meaning |
|---|---|---|
| `opencheckSourceId` | `source` | Which OpenCheck source the statement came from; use it for licence and provenance, not the display name |
| `opencheckOfficerPosition` | an interest | The appointment behind an `otherInfluenceOrControl` interest — a role, not control |
| `legalFormLabel` | entity `recordDetails` | The legal form in words |
| `gleifLegalEntityEvent` | an annotation | GLEIF's Legal Entity Event (merger, dissolution…) as published |
| `formerName` | an annotation | The `alternateNames` entry it points at is a former legal name |
| `identityVerification`, `companiesHouseFiling` | annotations on Companies House statements | ID verification in place; the filing that notified or ceased a PSC |

Person identifiers that are not identity documents (Wikidata Q-ids,
OpenSanctions ids, register person keys) are in `identifying` annotations, not
in `identifiers[]`.

## 7. Common mistakes

- Joining `relationship.subject` to `statementId` → most edges dangle.
- Treating every relationship as ownership → officers, GLEIF consolidation and
  MEIP group membership become "owners".
- Adding `share` across sources or across PSC bands → figures over 100%.
- Ignoring `recordStatus: closed` / `endDate` → former owners reported as current.
- Counting statements as companies → each source's record is counted again.
- Dropping `source` → losing the licence: OpenSanctions and EveryPolitician
  statements are CC-BY-NC.
