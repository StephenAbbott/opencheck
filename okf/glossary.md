---
type: Reference
title: Beneficial-ownership glossary
description: Key terms used across OpenCheck — beneficial owner, UBO, PSC, RLE, LEI, nominee, and the BODS statement types.
tags: [glossary, terminology, beneficial-ownership]
timestamp: 2026-06-14
---

# Terms

| Term | Meaning |
|---|---|
| **Beneficial owner (BO)** | The natural person(s) who ultimately own or control a legal entity, directly or indirectly. |
| **UBO** | Ultimate Beneficial Owner — the BO at the top of an ownership chain. |
| **PSC** | Person with Significant Control — the UK's term for a beneficial owner of a company, filed at Companies House. |
| **RLE** | Relevant Legal Entity — a corporate PSC (an entity, not a person, that holds significant control). |
| **LEI** | Legal Entity Identifier — a 20-character ISO 17442 global identifier issued via GLEIF. OpenCheck uses it as the anchor for lookups (see [lei-anchoring](/standards/lei-anchoring.md)). |
| **GLEIF** | Global Legal Entity Identifier Foundation — issues and publishes LEI data, including Level 2 ownership relationships. |
| **RA code** | GLEIF Registration Authority code identifying the national register a company is registered at (e.g. `RA000585` = UK Companies House). |
| **Nominee arrangement** | An agreement where one party (the nominee) holds an interest on behalf of another (the nominator). Modelled in BODS as a synthetic `arrangement`/`nomination` entity. |
| **ROE** | UK Register of Overseas Entities — overseas entities owning UK land must declare their beneficial owners. |
| **BODS** | Beneficial Ownership Data Standard — the open standard OpenCheck emits. See [standards/bods.md](/standards/bods.md). |
| **BOVS** | Beneficial Ownership Visualisation System — Open Ownership's conventions for drawing ownership graphs. |
| **Entity statement** | A BODS record describing a legal entity (company, arrangement). |
| **Person statement** | A BODS record describing a natural person. |
| **Relationship statement** | A BODS record describing an ownership-or-control link between an interested party and a subject. |
| **Interest type** | The kind of control in a relationship — e.g. `shareholding`, `votingRights`, `seniorManagingOfficial`, `nominee`, `unknownInterest`. |

# Citations

- https://standard.openownership.org/en/0.4.0/
- https://www.openownership.org/en/
