---
type: API Endpoint
title: License matrix
description: Per-source licence terms and the combined commercial-use verdict for a set of contributing sources.
tags: [api, licensing, compliance]
method: GET
path: /license-matrix
timestamp: 2026-06-14
---

# Overview

`GET /license-matrix` returns the full licensing catalogue: every
[source's](/sources/) licence classified into structured terms (commercial use,
attribution, share-alike, redistribution, traffic-light colour) plus the
distinct licence list and a disclaimer.

`GET /license-matrix?sources=<a,b,c>` additionally returns an `assessment` — the
combined verdict for those contributing sources (the export "licensing
assistant"). The most-restrictive licence wins, so one non-commercial source
makes the whole set non-commercial.

# Response

`{ disclaimer, sources: [{source_id, name, license, terms}], licenses: [terms], assessment? }`
where `terms` has `commercial_use` (yes/no/conditional), `attribution_required`,
`share_alike`, `redistribution`, `color`, `summary`.

# Citations

- /licensing/matrix.md
