---
type: API Endpoint
title: Watchlist
description: Watch an LEI and be told, through an Atom feed, when GLEIF or OpenSanctions publish a change to it.
tags: [api, watchlist, alerts, atom, gleif, opensanctions]
method: GET
path: /watch
timestamp: 2026-09-16
---

# Overview

`POST /watch/items` with `{"lei": "<LEI>"}` watches a company and returns
a **capability token**; `GET /watch/{token}` is the list — watched
companies with their baseline, the change log, the caps and what each tier
is doing on this instance — and `GET /watch/{token}.atom` is the feed a
reader subscribes to. `DELETE /watch/{token}/items/{lei}` stops watching;
`POST /watch/{token}/recheck` re-runs one company by hand (heavy tier).

There are no accounts. The token is shown once and stored only as a hash.

# How a change is found

Nothing polls a company. A watched LEI is re-run only when one of two
published deltas names it:

* **GLEIF** — the Golden Copy delta the mirror already applies hourly
  ([GLEIF](/sources/gleif.md)). Only a change to a *material* field
  (name, statuses, jurisdiction, legal form, parents, reporting exceptions,
  successor, expiry) counts; renewal churn does not.
* **OpenSanctions** — their entity-level delta per version
  ([OpenSanctions](/sources/opensanctions.md)), matched against the
  watched company's own names or LEI.

The re-run is the ordinary [lookup](/api/lookup.md) with the replay cache
bypassed; the national registers are fetched then, and only then. Each
feed entry says which tier fired, what the re-run found, and how many
sources were actually reached — a signal whose source could not be reached
is reported as *could not re-check*, never as gone.

Design: `docs/watchlist.md`.
