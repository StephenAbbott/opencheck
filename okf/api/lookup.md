---
type: API Endpoint
title: Lookup
description: The LEI-anchored synthesis — resolve a company across sources and return a unified BODS v0.4 view.
tags: [api, lookup, bods, sse]
method: GET
path: /lookup, /lookup-stream, /lookup-register, /lookup-register-stream
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

## Without an LEI: `/lookup-register` (Phase 290)

`GET /lookup-register?scheme=<SCHEME>&id=<number>` is the same synthesis for a
company that has no LEI — most UK LLPs, most small companies anywhere —
anchored on its number on a national register. `scheme` is any identifier
scheme `GET /expand-schemes` lists (`GB-COH`, `NL-KVK`, `FR-INSEE`, …, and
the `REG-<country>` aliases); the register that owns it is the one source
read, and its bundle is then screened and assessed as `/lookup`'s is: the
related-party sanctions / PEP screen, Offshore Leaks, OpenAleph, the risk
engine, the verdict, the register profile and the knowability statements.

The response is the `/lookup` shape with `lei` null and `scheme` / `id`
added. What a GLEIF anchor would add is said as absent rather than faked:
`derived_identifiers` carries no `lei`, there is no `listing` line (PermID is
keyed on the LEI), and — with no LEI-keyed OpenSanctions record of the
subject — the company's own name is screened by name, once, with a
subject-level code (`SANCTIONED`, never `RELATED_SANCTIONED`) anchored on
`evidence.statement_id` with `evidence.subject: true`.

`GET /lookup-register-stream` is the SSE form, with `register_done`
(`scheme`, `id`, `legal_name`, `jurisdiction`, `derived_identifiers`) where
`/lookup-stream` says `gleif_done`; it is gated against declared automated
clients exactly as `/lookup-stream` is. Both routes sit on the lookup rate
tier and the shared per-client lookup budget; the replay cache is keyed on
`scheme:id`.

# Parameters

| Param | Description |
|---|---|
| `lei` | ISO 17442 Legal Entity Identifier (20 chars). Required on `/lookup`. |
| `scheme`, `id` | Register scheme and number. Required on `/lookup-register`; `400` names the known schemes when the scheme is unknown or the value is not a number of that register; the register's own "no such number" is `404`. |
| `name` | Optional on `/lookup-register`: the company's name as the caller knows it, handed to registers that search by name behind the number (as the FullCheck hop hands them the PSC filing's). KvK's open data publishes no names at all: without one its record is returned with a `source_read` degradation saying so, and the verdict reads as incomplete rather than clean. A named run and a nameless one are separate replay-cache entries. |
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
the sources stating it — facts, never findings; Phase 154 — plus
`lei_registration`, the LEI record's own GLEIF status and dates, which is not
the company's status; Phase 242), and `listing`
(the primary stock-exchange listing from LSEG PermID — venue, ticker, MIC and a
verified venue link — or null when no PermID key is configured; Phase 236),
and `register_record` (the subject's own page on its home register —
`register`, `identifier`, `url` and `ra_code` — built from the LEI record's
registration authority and number alone, so present even when that register's
adapter did not answer; null where OpenCheck has no per-company address for
the register; Phase 296).

# Citations

- /architecture.md
- /standards/bods.md
