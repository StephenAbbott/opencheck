---
type: API Endpoint
title: Sources & health
description: The source catalogue and a liveness probe.
tags: [api, sources, health]
method: GET
path: /sources, /source-health, /health
timestamp: 2026-09-03
---

# Overview

- `GET /sources` — the catalogue of registered data sources: each source's
  `id`, `name`, `homepage`, `description`, `license`, `attribution`, `category`
  (cdd/esg), `is_national_register`, `supports`, `requires_api_key`,
  `live_available`. Mirrors [sources/](/sources/) in this bundle.
- `GET /source-health` — the last weekly sweep's verdict on every source
  (Phase 161): `generated_at`, `counts` of `ok` / `degraded` / `fail` /
  `skipped`, and per source its `status`, `reason`, `known_gap`, `liveness`,
  `retrieved_at`, `latency_ms`, `statement_total`, any `statement_collapse`
  and a `history` of the last eight sweeps (oldest first). Read from the
  `source-health-latest` release asset the sweep uploads, refreshed at most
  hourly, served stale (`stale: true`) when the asset cannot be re-read, and
  `{"available": false, "reason": …}` when no sweep has published. Nothing
  here contacts a source: *degraded* is the sweep's word for a caveat (a rate
  limit, a snapshot due a refresh, a register that refuses datacentre IPs),
  *skipped* means the sweep held no credential — never "healthy".
- `GET /health` — liveness probe used by the deployment platform.

# Citations

- /sources/
