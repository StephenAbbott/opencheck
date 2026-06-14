---
type: Data Standard
title: Beneficial Ownership Data Standard (BODS) v0.4
description: The open standard OpenCheck emits — entity, person and relationship statements that describe who owns and controls companies.
resource: https://standard.openownership.org/en/0.4.0/
tags: [BODS, data-standard, beneficial-ownership, open-ownership]
timestamp: 2026-06-14
---

# Overview

BODS, maintained by [Open Ownership](https://www.openownership.org/en/), is a
structured, open standard for publishing **who owns and controls companies**.
OpenCheck targets **version 0.4**. Every source OpenCheck queries is mapped into
BODS so heterogeneous registers become directly comparable and linkable.

BODS represents *declarations* (statements) about ownership, not just a static
graph — which lets it carry provenance, dates, and the lifecycle of a record.

# Statement types

| Record type | Describes |
|---|---|
| **entity** | A legal entity — `entityType.type` is `registeredEntity`, `arrangement`, `anonymousEntity`, etc. Carries name, jurisdiction, identifiers. |
| **person** | A natural person — `personType` is `knownPerson`, `anonymousPerson`, or `unknownPerson`. |
| **relationship** | An ownership-or-control link: a `subject`, an `interestedParty`, and one or more `interests`. |

Each statement has a `statementId`, a `recordId` (stable across a record's
lifecycle), a `recordStatus` (`new` / `updated` / `closed`), a `statementDate`,
`publicationDetails`, and a `source` block.

# Interest types OpenCheck emits

`shareholding`, `votingRights`, `appointmentOfBoard`, `seniorManagingOfficial`,
`otherInfluenceOrControl`, `rightsToSurplusAssetsOnDissolution`, `boardMember`,
`nominee`, `nominator`, `unknownInterest`, `unpublishedInterest`.

# Modelling conventions used in OpenCheck

- **Directors / managing officials** → `seniorManagingOfficial` interest.
- **Nominee arrangements** (UK ROE land-nominee codes) → a synthetic
  `arrangement` / `nomination` entity with `nominator` and `nominee`
  relationships, not a bare `nominee` interest. (See the nominee modelling
  guidance in the citations.)
- **Super-secure PSCs** (details withheld by court order) → an
  `anonymousPerson` whose relationship carries an `unpublishedInterest` with the
  official Companies House explanatory text.
- **Missing information** (e.g. "company believes there is no PSC") → an
  ownership-or-control statement with an *unspecified* `interestedParty` carrying
  a `reason` from the BODS `unspecifiedReason` codelist (`noBeneficialOwners`,
  `subjectUnableToConfirmOrIdentifyBeneficialOwner`, …).
- **Lifecycle:** a ceased relationship becomes a `closed` record (stable
  `recordId`, `replacesStatements` → the original `new`).

# Exporting BODS from OpenCheck

The [export API](/api/export.md) returns BODS as JSON, JSONL, canonical XML, or a
ZIP bundle (BODS + manifest + `LICENSES.md`). Output passes a shape validator and
can be checked against `lib-cove-bods`.

# Citations

- https://standard.openownership.org/en/0.4.0/
- https://standard.openownership.org/en/0.4.0/standard/concepts.html
- https://standard.openownership.org/en/0.4.0/standard/modelling/repr-nominations.html
- https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/
