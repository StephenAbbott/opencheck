---
type: API Endpoint
title: Export
description: Download a reproducible BODS v0.4 bundle for a subject, with manifest and per-source licence notes.
tags: [api, export, bods, licensing]
method: GET
path: /export
timestamp: 2026-06-14
---

# Overview

`GET /export?lei=<LEI>&format=<zip|json|jsonl|xml>` (or `?q=<query>`) downloads a
reproducible [BODS v0.4](/standards/bods.md) bundle for the subject.

# Formats

| Format | Contents |
|---|---|
| `json` | Pretty-printed array of BODS statements. |
| `jsonl` | Newline-delimited BODS statements. |
| `xml` | Canonical BODS XML. |
| `zip` | `bods.json` + `bods.jsonl` + `bods.xml` + `manifest.json` + `LICENSES.md`. |

# Licensing in the bundle

The ZIP's `manifest.json` carries a `licensing` block and `LICENSES.md` leads
with a **compatibility verdict** (commercial use / attribution / share-alike)
plus a per-source traffic-light table — the same data as the
[licensing matrix](/licensing/matrix.md). The most-restrictive source licence
governs the combined bundle.

# Citations

- /standards/bods.md
- /licensing/matrix.md
