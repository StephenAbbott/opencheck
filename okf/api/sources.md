---
type: API Endpoint
title: Sources & health
description: The source catalogue and a liveness probe.
tags: [api, sources, health]
method: GET
path: /sources, /health
timestamp: 2026-06-14
---

# Overview

- `GET /sources` — the catalogue of registered data sources: each source's
  `id`, `name`, `homepage`, `description`, `license`, `attribution`, `category`
  (cdd/esg), `is_national_register`, `supports`, `requires_api_key`,
  `live_available`. Mirrors [sources/](/sources/) in this bundle.
- `GET /health` — liveness probe used by the deployment platform.

# Citations

- /sources/
