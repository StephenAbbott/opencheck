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
| `deepen_top` | How many top hits to deepen + map + assess (default 5; clamped to 0–10). |
| `refresh` | Bypass the short-lived replay cache. |

# Limits

Every **fresh** run is charged to the caller's one lookup budget (Phase 234,
`OPENCHECK_RATE_LIMIT_LOOKUP`, 10 a minute per address), shared with every
other path that starts a full lookup — `/expand`, each LEI of
`/expand-layer`, a watchlist baseline, a batch row, an export and the MCP
tools. A run replayed from the 15-minute cache, or joined while another
caller's run of the same LEI is in flight, is free. A spent budget answers
`429` with `Retry-After` (on the stream, an `error` event carrying
`retry_after_s`); a process already running its maximum of concurrent
pipelines answers `503` after a bounded queue (60 s). The stream waits
longer (5 min) and sends `queued` events — `position` (1 = next), `running`,
`limit`, `max_wait_s` — while it does; they are about the wait, not the run,
and never appear in a replay or a saved report.

# Response

A `LookupResponse`: `lei`, `legal_name`, `jurisdiction`, `derived_identifiers`,
`hits` (per-source results; raw payloads are redacted for sources that forbid
re-publication), `bods` (the merged BODS statements), `cross_source_links`,
`risk_signals`, `license_notices`, `verdict` (the deterministic one-line
sentence) and `subject_profile` (what the registers say the company *is*:
legal form, register status, founding date and registered address, each with
the sources stating it — facts, never findings; Phase 154), and `listing`
(the primary stock-exchange listing from LSEG PermID — venue, ticker, MIC and a
verified venue link — or null when no PermID key is configured; Phase 236).

# Citations

- /architecture.md
- /standards/bods.md
