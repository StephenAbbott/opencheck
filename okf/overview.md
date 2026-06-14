---
type: Project
title: OpenCheck
description: An open beneficial-ownership and corporate-data aggregator that queries ~30 sources in parallel and synthesises a single Beneficial Ownership Data Standard (BODS) v0.4 view of a company and its owners.
resource: https://github.com/StephenAbbott/opencheck
tags: [beneficial-ownership, BODS, due-diligence, open-data, corporate-transparency]
timestamp: 2026-06-14
---

# What OpenCheck is

OpenCheck is an open tool for **beneficial-ownership and corporate due diligence**.
Given a company (by name, by national registration number, or by Legal Entity
Identifier), it queries around **30 open data sources in parallel** — national
company registers, beneficial-ownership registers, cross-border aggregators,
sanctions/PEP data and ESG sources — reconciles them by shared identifiers, and
maps the results into a single, standardised
[BODS v0.4](/standards/bods.md) view of the entity, its people, and the
ownership/control relationships between them.

The goal is to make scattered, heterogeneous corporate data **comparable,
linkable and exportable** as an open standard, with the provenance and licence
of every datum preserved.

# What it produces

- A **unified subject view**: entity + person + relationship statements in BODS
  v0.4, visualised as an ownership graph (Open Ownership's BOVS conventions).
- **Structural risk signals** — non-EU jurisdiction, FATF black/grey-list,
  trust/arrangement, nominee, complex ownership layers, sanctioned/PEP
  cross-matches — surfaced on the relevant statements.
- **Reproducible exports** (BODS JSON / JSONL / XML, plus a manifest and a
  per-source `LICENSES.md`) via the [export API](/api/export.md).
- A **licensing compatibility verdict** so users know whether a combined export
  is safe for commercial use (see the [licensing matrix](/licensing/matrix.md)).

# How an agent should use this bundle

1. Read [architecture](/architecture.md) for the request → BODS pipeline.
2. Read [standards/bods.md](/standards/bods.md) to understand the output shape,
   and [standards/lei-anchoring.md](/standards/lei-anchoring.md) for how lookups
   resolve a company across registers.
3. Browse [sources/](/sources/) for what each data source provides and its
   licence; consult [licensing/matrix.md](/licensing/matrix.md) before reusing
   exported data.
4. Use the [api/](/api/) concepts to call OpenCheck programmatically.

# Related projects

- **[BODS stream](https://github.com/StephenAbbott/bods-stream)** — a live
  visualiser of UK Companies House PSC changes mapped to BODS v0.4 in real time.
- **[bods-mapper](https://github.com/StephenAbbott/bods-mapper)** — the shared
  Companies House PSC → BODS v0.4 mapping core, used by both OpenCheck and BODS
  stream so they cannot drift.

# Citations

- https://github.com/StephenAbbott/opencheck
- https://standard.openownership.org/en/0.4.0/
