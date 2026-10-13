---
type: API Endpoint
title: Deepen & lookup-source
description: Fetch the full record for a single hit and map it to BODS; re-run one source for an existing lookup.
tags: [api, deepen, bods]
method: GET
path: /deepen, /lookup-source
timestamp: 2026-10-13
---

# Overview

- `GET /deepen?source=<id>&hit_id=<id>` — fetch the full record for one hit from
  one [source](/sources/), map it to [BODS v0.4](/standards/bods.md), and assess
  risk. Returns `raw`, `bods`, `bods_issues`, `license`, `license_notice`,
  `risk_signals`. Optional `legal_name` (≤ 300 characters) is handed to the
  registers that take a name in a lookup, as GLEIF's is: KvK's open data has
  no company name and maps nothing without one, and other registers check it
  against their own. It applies to that response only — a register that
  remembers names (KvK) is told not to keep it.
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
