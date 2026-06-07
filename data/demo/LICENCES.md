# Data Licences — OpenCheck Demo Graph

The demo graph in this directory (`data/demo/`) is assembled from two
published BODS v0.4 datasets produced by Open Ownership. Both are
**freely shareable**, including in conference talks, blog posts, and
derivative works.

---

## UK PSC data — Open Government Licence v3.0

Source: [UK Companies House — Persons with Significant Control (PSC) data](https://find-and-update.company-information.service.gov.uk/)
Distributed as BODS v0.4 by Open Ownership: [bods-data.openownership.org/source/uk_version_0_4](https://bods-data.openownership.org/source/uk_version_0_4/)

Licence: **[Open Government Licence v3.0 (OGL v3.0)](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)**

OGL v3.0 permits use, adaptation, and redistribution for any purpose
(commercial or non-commercial) provided you acknowledge the source:

> Contains public sector information licensed under the Open Government
> Licence v3.0. Source: Companies House / Open Ownership.

---

## GLEIF Level 1 & Level 2 data — Creative Commons Zero (CC0 1.0)

Source: [Global Legal Entity Identifier Foundation (GLEIF)](https://www.gleif.org/)
Distributed as BODS v0.4 by Open Ownership: [bods-data.openownership.org/source/gleif_version_0_4](https://bods-data.openownership.org/source/gleif_version_0_4/)

Licence: **[Creative Commons Zero v1.0 Universal (CC0 1.0)](https://creativecommons.org/publicdomain/zero/1.0/)**

CC0 dedicates the data to the public domain. No attribution is legally
required, though acknowledging GLEIF and Open Ownership is good practice.

---

## Combined demo graph

The `data/demo/*.jsonl` files merge GLEIF and UK PSC BODS statements per
entity. The combined output inherits both licences: OGL v3.0 (for the PSC
portion) and CC0 (for the GLEIF portion). Both are permissive and
compatible — the combined graph is freely shareable.

**Suggested attribution for talks / slides:**

> Ownership data: Companies House (OGL v3.0) + GLEIF (CC0),
> distributed in BODS v0.4 format by Open Ownership.
> Graph assembled by OpenCheck (github.com/StephenAbbott/opencheck).

---

## Pipeline code — NOT included here

The Open Ownership pipelines that originally produced the BODS bulk output
([bods-uk-psc-pipeline](https://github.com/openownership/bods-uk-psc-pipeline),
[bods-gleif-pipeline](https://github.com/openownership/bods-gleif-pipeline))
are licensed under **AGPL-3.0**. That code is **not** incorporated into
OpenCheck. OpenCheck only consumes their published BODS output as data,
which is licensed under OGL v3.0 / CC0 as above. No AGPL obligations
apply to OpenCheck or to the demo graph.

---

## AuraDB / hosted Neo4j — decision deferred

See `CLAUDE.md` § "Phase 8 — AuraDB deferral" for the recorded decision
and named revisit trigger.
