---
type: Reference
title: LEI / GLEIF anchoring
description: How OpenCheck uses the Legal Entity Identifier and GLEIF Registration Authority codes to resolve a company across national registers.
resource: https://www.gleif.org/
tags: [LEI, GLEIF, identifiers, lookup]
timestamp: 2026-06-14
---

# Why the LEI is the anchor

A company appears under different identifiers in every register (a UK company
number, a Norwegian organisasjonsnummer, a French SIREN, …). OpenCheck uses the
**Legal Entity Identifier (LEI)** — a single global ISO 17442 id issued via
[GLEIF](https://www.gleif.org/) — as the anchor that ties these together.

# How a lookup resolves

1. The user supplies an LEI (or OpenCheck resolves one from a name / national id
   via GLEIF reverse lookup).
2. OpenCheck fetches the **GLEIF anchor record**, which carries:
   - `entity.legalName`, `entity.jurisdiction`,
   - `entity.registeredAs` — the company's id in its home register,
   - `entity.registeredAt.id` — the **RA code** of that register.
3. A per-adapter `LookupDeriver` maps the RA code to a local identifier (e.g. RA
   code `RA000585` → the UK Companies House number) and dispatches that adapter.
4. GLEIF **Level 2** relationships (direct/ultimate parent, reporting
   exceptions) feed corporate ownership edges.

# Registration Authority (RA) codes

Each national-register adapter declares the RA code(s) that derive its
identifier. Examples:

| Register | RA code |
|---|---|
| UK Companies House | `RA000585` |
| Norway (Brønnøysundregistrene) | `RA000472` |
| France (INPI) | `RA000580` |
| Netherlands (KvK) | `RA000463` |
| Estonia (e-Äriregister) | `RA000181` |

A local id can appear under `entity.registeredAs`,
`registration.validatedAs`, or `registration.otherValidationAuthorities`, so
reverse lookups query all three and always pair the id with the RA code to avoid
collisions across registers.

# Citations

- https://www.gleif.org/
- https://www.gleif.org/en/lei-data/gleif-golden-copy
