---
type: API Endpoint
title: Export
description: Download a reproducible BODS v0.4 bundle for a subject, with manifest and per-source licence notes.
tags: [api, export, bods, licensing]
method: GET
path: /export
timestamp: 2026-07-20
---

# Overview

`GET /export?lei=<LEI>&format=<zip|json|jsonl|xml|csv|cypher|senzing|ftm|gql|amlai|rdf>` (or
`?q=<query>`) downloads a reproducible [BODS v0.4](/standards/bods.md) bundle
for the subject, or a projection of it into a partner ecosystem's format.

# Formats

| Format | Contents |
|---|---|
| `json` | Pretty-printed array of BODS statements. |
| `jsonl` | Newline-delimited BODS statements. |
| `xml` | Canonical BODS XML. |
| `csv` | Zip: the entity / person / ownership-edge tables (the same tables `gql` builds) + `LICENSES.md`, without the property-graph DDL and queries. |
| `cypher` | Neo4j Cypher script that builds the graph — the same writer `POST /export-network` uses. |
| `senzing` | Newline-delimited Senzing JSON entity records (entity resolution). |
| `ftm` | Newline-delimited FollowTheMoney entities (OpenSanctions / OpenAleph). |
| `gql` | Zip: BigQuery property-graph CSV tables + `CREATE PROPERTY GRAPH` DDL + 14 GQL (ISO/IEC 39075) queries + README + `LICENSES.md`, via [bods-gql](https://github.com/StephenAbbott/bods-gql). |
| `rdf` | BODS RDF as TriG — one named graph per statement, a canonical licence URI on every statement, and `bods:Annotation` in two separated layers (the register's own words in each statement's graph, OpenCheck's risk signals and entity-resolution links in a separate analysis graph). |
| `amlai` | Zip: Google AML AI input tables (`party` / `party_supplementary_data` / `account_party_link` NDJSON) + README + `LICENSES.md`, via [bods-aml-ai](https://github.com/StephenAbbott/bods-aml-ai). |
| `zip` | `bods.json` + `bods.jsonl` + `bods.xml` + `senzing.jsonl` + `ftm.jsonl` + `manifest.json` + `LICENSES.md`. |

# Licensing in the bundle

The ZIP's `manifest.json` carries a `licensing` block and `LICENSES.md` leads
with a **compatibility verdict** (commercial use / attribution / share-alike)
plus a per-source traffic-light table — the same data as the
[licensing matrix](/licensing/matrix.md). The most-restrictive source licence
governs the combined bundle.

# Citations

- /standards/bods.md
- /licensing/matrix.md
