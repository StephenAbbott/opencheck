---
type: API Endpoint
title: Saved reports
description: Save a completed lookup as a verifiable record of exactly what OpenCheck showed on a given date.
tags: [api, saved-reports, provenance, integrity, audit]
method: POST
path: /saved-reports
timestamp: 2026-09-16
---

# Overview

A [lookup](/api/lookup.md) link re-runs the pipeline; a saved report is the
record. `POST /saved-reports` with `{"lei", "run_completed_at"}` — the value
the run's `done` event carried — copies the run the server still holds (for
15 minutes after it finishes) and returns a `report_id`, a one-time
`manage_token`, the `content_hash`, and the save and expiry dates. A run the
server no longer holds is refused with *run the check again*; nothing is
silently re-run, and no client can post a payload.

`GET /saved-reports/{id}` returns the frozen payload: the lookup's event
stream verbatim, the narrative and its dispositions if the reader generated
them, and the licence assessment at save time. `GET /saved-reports/{id}.json`
serves the exact bytes the SHA-256 `content_hash` covers, so anyone can verify
a report. `POST …/extend` and `DELETE …` need the manage token in
`X-OpenCheck-Manage-Token`.

# Rules

* Kept 90 days from the save; extending resets the clock.
* Never indexed: `noindex`, disallowed in robots.txt, in no sitemap.
* Background check, Subsidiaries, History and ESG are not part of a saved
  report — they fetch live.
* One CC-BY-NC source in the run makes the saved report non-commercial
  ([licensing](/licensing/matrix.md)).

Design: `docs/saved-reports.md`.
