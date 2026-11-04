---
type: API Endpoint
title: Search & report
description: Free-text company/person search across sources, and a one-shot synthesis report.
tags: [api, search, report]
method: GET
path: /search, /report, /elf-forms
timestamp: 2026-06-14
---

# Overview

- `GET /search?q=<query>&kind=<entity|person>` — fan out the query to every
  source adapter and return reconciled `hits`. Also available as an SSE stream
  (`/stream`) that yields hits as each source responds.
- Optional filters on `/search` and `/stream`: `jurisdiction`, `legal_form`,
  `entity_status`, `registration_status`, `category` (entities) and
  `nationality`, `birth_year` (people). Only sources that can apply every
  active filter are searched; the response's `filters` block names them
  (`applied_by`) and the sources left out (`left_out`, with a reason). The
  jurisdiction filter is exact.
- `GET /elf-forms?jurisdiction=<code>` — the ISO 20275 legal forms of one
  jurisdiction for the legal-form filter, most used first where the Golden
  Copy mirror is present.
- `GET /report?q=<query>&kind=&deepen_top=<n>` — the same search, then deepen
  the top N hits, map them to [BODS](/standards/bods.md), reconcile and assess
  risk in one call. Same response shape as [/lookup](/api/lookup.md) minus the
  LEI echo.

# Notes

Free-text search is best-effort name matching; the
[LEI-anchored lookup](/api/lookup.md) is the precise path when an LEI is known.
Raw payloads from sources that forbid re-publication (e.g. OpenCorporates) are
redacted from `hits`.

# Citations

- /api/lookup.md
