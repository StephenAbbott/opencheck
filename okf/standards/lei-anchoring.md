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
| France (Sirene / INSEE; read by the INPI adapter) | `RA000189` |
| Netherlands (KvK) | `RA000463` |
| Estonia (e-Äriregister) | `RA000181` |

France is the case worth knowing: GLEIF files most French companies under
`RA000189` (Sirene), some under `RA000192` (Infogreffe), and the INPI adapter
derives the SIREN from all three of those and `RA001129` (the RNE). `RA000580`,
which this table once gave for France, belongs to an authority in the United
Arab Emirates.

# The subject's register record

The same RA code and `registeredAs` also give the subject's own page on its
home register — the `register_record` on the `gleif_done` event and the lookup
response (Phase 296). It comes from one table, `opencheck.register_links`
(mirrored in `frontend/src/lib/registerLinks.ts`), keyed by the derived key the
adapter's `LookupDeriver` already stores the number under, so it is present
whether or not that register's adapter answered. A register with no
per-company page gets no link rather than a search page.

A local id can appear under `entity.registeredAs`,
`registration.validatedAs`, or `registration.otherValidationAuthorities`, so
reverse lookups query all three and always pair the id with the RA code to avoid
collisions across registers.

# Citations

- https://www.gleif.org/
- https://www.gleif.org/en/lei-data/gleif-golden-copy
