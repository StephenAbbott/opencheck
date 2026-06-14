---
type: API Endpoint
title: Lookup
description: The LEI-anchored synthesis — resolve a company across sources and return a unified BODS v0.4 view.
tags: [api, lookup, bods, sse]
method: GET
path: /lookup, /lookup-stream
timestamp: 2026-06-14
---

# Overview

`GET /lookup?lei=<LEI>&deepen_top=<n>` runs the full
[lookup pipeline](/architecture.md): resolve the GLEIF anchor, derive national
identifiers, dispatch the relevant [sources](/sources/), map each to
[BODS v0.4](/standards/bods.md), reconcile, and assess risk.

`GET /lookup-stream?lei=<LEI>` is the same pipeline served as **Server-Sent
Events** — `gleif_done`, per-source `hit` / `source_error`, and a final `done`
event — so a UI can render progressively. Both paths share one generator and
cannot diverge.

# Parameters

| Param | Description |
|---|---|
| `lei` | ISO 17442 Legal Entity Identifier (20 chars). Required. |
| `deepen_top` | How many top hits to deepen + map + assess (default 3). |
| `refresh` | Bypass the short-lived replay cache. |

# Response

A `LookupResponse`: `lei`, `legal_name`, `jurisdiction`, `derived_identifiers`,
`hits` (per-source results; raw payloads are redacted for sources that forbid
re-publication), `bods` (the merged BODS statements), `cross_source_links`,
`risk_signals`, `license_notices`.

# Citations

- /architecture.md
- /standards/bods.md
