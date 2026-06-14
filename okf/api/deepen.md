---
type: API Endpoint
title: Deepen & lookup-source
description: Fetch the full record for a single hit and map it to BODS; re-run one source for an existing lookup.
tags: [api, deepen, bods]
method: GET
path: /deepen, /lookup-source
timestamp: 2026-06-14
---

# Overview

- `GET /deepen?source=<id>&hit_id=<id>` — fetch the full record for one hit from
  one [source](/sources/), map it to [BODS v0.4](/standards/bods.md), and assess
  risk. Returns `raw`, `bods`, `bods_issues`, `license`, `license_notice`,
  `risk_signals`.
- `GET /lookup-source?lei=<LEI>&source_id=<id>` — re-run a single source for an
  existing LEI lookup (the UI's per-source "retry source"), invalidating that
  LEI's replay cache.

# Raw-data policy

For sources whose licence forbids re-publication of raw records (e.g.
OpenCorporates), `/deepen` returns a **redaction notice** in the `raw` field
instead of the source payload; the mapped `bods` is unaffected.

# Citations

- /standards/bods.md
- /licensing/matrix.md
