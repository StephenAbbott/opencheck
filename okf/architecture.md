---
type: Architecture
title: OpenCheck architecture
description: The request-to-BODS pipeline — source adapters, the LEI-anchored lookup, the BODS mapper, the reconciler, the risk engine, and the React frontend.
resource: https://github.com/StephenAbbott/opencheck
tags: [architecture, fastapi, pipeline, bods]
timestamp: 2026-06-14
---

# Shape

- **Backend:** Python / FastAPI, deployed on Render. Routers live in
  `backend/opencheck/routers/` (search, lookup, export, deepen, licensing).
- **Frontend:** React + Vite + TypeScript, with a Cytoscape.js ownership graph.
- **Source adapters:** one per data source in `backend/opencheck/sources/`,
  registered in a single `REGISTRY`. See [sources/](/sources/).
- **BODS mapper:** `backend/opencheck/bods/mapper.py` — a `map_<source_id>()`
  function per source turns raw payloads into [BODS v0.4](/standards/bods.md).

# The lookup pipeline

A single async generator drives both `/lookup` and `/lookup-stream`, so the
sync and streaming paths cannot diverge:

```
1. Resolve the GLEIF anchor record for the LEI (see lei-anchoring).
2. Derive local identifiers (national company numbers) from the anchor's
   registeredAs + RA code.
3. Dispatch the relevant source adapters in parallel, each with a
   per-source wall-clock timeout.
4. Convert each source result to a neutral SourceHit, then map to BODS v0.4.
5. Reconcile hits across sources by shared identifiers (LEI, company number,
   Wikidata QID).
6. Assess structural + cross-source risk signals.
7. Stream results as Server-Sent Events; the sync endpoint collects them.
```

Completed runs are cached briefly and are addressable by `?lei=`, so a refresh
or shared URL replays instantly.

# Self-describing adapters

Each national-register adapter declares its own lookup wiring (which RA codes
derive its identifier, its dispatch keys, its timeout) as class attributes. The
pipeline builds its dispatch tables from the registry at import time, so adding
a source touches only: the adapter module, its schema, one registry line, one
`map_<source>()` mapper, one hit builder, and its tests.

# Cross-cutting concerns

- **Risk engine** (`risk.py`, `cross_check.py`): structural signals
  (jurisdiction/FATF/trust/nominee/complex-layers) and name-match
  sanctioned/PEP cross-checks, attached to the relevant BODS statements.
- **Reconciler** (`reconcile.py`): asserts cross-source corroboration only when
  a source independently publishes an identifier.
- **Licensing** (`licensing.py`): classifies each source's licence and computes
  a combined commercial-use verdict for exports. See
  [licensing/matrix.md](/licensing/matrix.md).
- **Raw-data policy:** sources whose licence forbids raw re-publication (e.g.
  OpenCorporates) have their raw payload redacted from all API responses and
  exports; the derived BODS output is unaffected.

# Citations

- https://github.com/StephenAbbott/opencheck
