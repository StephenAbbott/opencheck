---
type: API Endpoint
title: Person check
description: On-demand screening of one named person across every person-capable source, plus officer appointments (BackgroundCheck).
tags: [api, person, screening, pep, sanctions]
method: GET
path: /person-check, /person-appointments, /person-positions
timestamp: 2026-07-22
---

# Overview

- `GET /person-check?name=<name>&birth_year=<yyyy>` — screen one person, on
  demand, across every source adapter that supports person queries (UK
  Companies House officer search, OpenSanctions, EveryPolitician, Wikidata,
  OpenAleph, …). Backs the **BackgroundCheck** view, which lists the people
  connected to an entity (officers, directors and beneficial owners from the
  assembled [BODS](/standards/bods.md) bundle) and runs this endpoint per
  person.
- `GET /person-appointments?officer_id=<id>` — every Companies House
  appointment recorded under one officer identifier, with the BODS
  `personStatement` carrying the `GB-COH-OFFICER` identifier. Unlike the
  name-based screen, this is the register's own same-person assertion.
- `GET /person-positions?entity_id=<id>` — the positions-held history behind
  an EveryPolitician / OpenSanctions PEP record: post, country and start/end
  dates, most recent first, keyed to the record's Wikidata Q-ID, with
  Poliloom maintenance attribution and the PEP coverage caveat.

The person-check response contains every hit **scored against the queried
name** (`name_score`, `birth_year_compatible`, `strong`), deterministic risk
signals derived from **strong matches only** (each carrying an
`evidence.match` block), a `weak_match_count`, `cross_source_links` between
strong matches sharing an identifier (Wikidata Q-ID, OpenSanctions id),
per-source outcome records (attribution, licence, hit count, error), and
explicit caveats.

# Notes

Person screening is name-based — there is no LEI-grade identifier for people —
so the endpoint never asserts identity. A "strong match" requires name
similarity ≥ 0.88 (the same threshold as the
[related-party cross-check](/api/lookup.md)) *and* a compatible birth year
where both sides carry one. A source that errors is reported as unscreened
rather than silently dropped, and an empty result is not presented as a clean
screen.

# Citations

- /api/lookup.md
- /api/search-report.md
- /standards/bods.md
