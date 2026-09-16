# Saved reports

Phase 216 (backend). A `?lei=` link is a *query*, not a *record*: it re-runs
the pipeline, so two people opening it a day apart can see different
findings. A saved report is the record of exactly what OpenCheck showed for
one company on one date — opt-in, addressable, and verifiable.

The page that renders one (`/report/{id}`) is Phase 217; exports, the share
card and the MCP tool are Phase 218.

## What is saved

The **event stream** of a completed lookup, verbatim — the list the replay
cache holds and the React report is built from — plus, when the reader
generated them, the narrative and a frozen copy of its disposition sheet, and
the licence assessment as it stood at save time.

Freezing the events rather than `LookupResponse` is deliberate: the report
page is a fold over the stream, so replaying the stored events through the
same handlers renders the same report. `routers.lookup.fold_lookup_events`
(factored out of `_lookup_impl` for this) folds them into the same
`LookupResponse` the PDF, Markdown and MCP views use, so those cannot drift
from the live ones either.

Payload, schema `opencheck.saved_report/1`:

| Field | What it is |
|---|---|
| `report_id` | The id the report is addressed by |
| `lei`, `legal_name`, `jurisdiction`, `deepen_top` | The subject and the run's parameters |
| `run_completed_at` | When the lookup run finished |
| `saved_at` | When a reader chose to keep it — the **fifth clock** beside the four of Phases 99/100 |
| `events` | `[{"event", "data"}]`, verbatim, including the internal `deepen_result` events the stream skips (the exports need their BODS) |
| `narrative` | The narrative, or `null` |
| `dispositions` | The disposition sheet as it stood at save time, or `null` |
| `licensing` | `licensing.assess` over the sources that returned data, at save time |
| `scope` | What a v1 saved report holds and what it does not (below) |
| `generator` | OpenCheck version and, on Render, the deployed commit |

**Scope (v1, Stephen 16 Sept 2026):** QuickCheck and FullCheck (both are folds
over the lookup events), the narrative and its dispositions. Background check,
Subsidiaries, History, ESG, securities and NZ associations fetch live and are
**not** part of a saved report; the page says so.

## Where it comes from — never from the client

A save *names* a run and the server copies its own copy:

```
POST /saved-reports
{"lei": "213800LH1BZH3DI6G760", "run_completed_at": "2026-09-16T14:02:00+00:00",
 "narrative_run_id": "0123456789abcdef"}          ← optional
```

`run_completed_at` is the value the run's `done` event carries (new in Phase
216, for live and replayed runs alike; `LookupResponse.run_completed_at` mirrors
it, while `fetched_at` stays replay-only). The server reads the held run from
the replay cache (`routers.lookup.replay_entry`) and saves it only if its
completion time matches. The request model forbids extra fields, so there is
no way to post a payload.

- **A run no longer held is not saved.** The replay window is 15 minutes, and a
  per-source retry or a restart clears it. The answer is `409` with
  `X-OpenCheck-Refusal: run_not_held` and "Run the check again, then save it" —
  never a silent re-run whose findings the reader has not seen.
- **A narrative is saved only if this server generated it from that same run.**
  `/narrative` now holds what it generates for the replay window
  (`routers.narrative.held_narrative`), with the run it was built from.
  Otherwise `409 narrative_not_held`. (`/export/pdf` still embeds a posted
  narrative; a saved report makes the stronger claim.)

## Integrity

The payload is serialised once, canonically — sorted keys, no insignificant
whitespace, UTF-8 — and `content_hash` is the SHA-256 of those bytes. The same
bytes are stored (gzipped) and served by `GET /saved-reports/{id}.json`, so

```
curl -s https://api.opencheck.world/saved-reports/<id>.json | shasum -a 256
```

reproduces the hash printed on the page (and, from Phase 218, in the PDF
footer). The hash is re-checked on every read; a mismatch answers `500` with
`X-OpenCheck-Refusal: integrity` and is never rendered.

## Two ids, two capabilities

- `report_id` — `secrets.token_urlsafe(16)`, 22 characters, unguessable — is
  the **read** capability, the link that gets shared. It is not the hash:
  tying the link to the bytes would make extending and deleting awkward.
- `manage_token` — returned once by the save, stored only as its SHA-256 — is
  the **manage** capability, sent as `X-OpenCheck-Manage-Token` to extend or
  delete. The saver's browser keeps it. There are no accounts.

## Retention

`OPENCHECK_SAVED_REPORTS_RETENTION_DAYS` (90) from the save.
`POST /saved-reports/{id}/extend` resets it to that many days from now; it
never changes the hashed record. A background task
(`OPENCHECK_SAVED_REPORTS_PRUNE_INTERVAL_S`, six hours) deletes expired rows,
and a read of an expired row answers `410` and deletes it.
`OPENCHECK_SAVED_REPORTS_MAX_TOTAL` (5,000) caps the store — insurance; a
report measured 5–20 KB gzipped on 16 Sept 2026 (BIRTLEY INVESTMENT LIMITED 19
KB raw, BP 106 KB raw).

## Routes

| Route | Tier | |
|---|---|---|
| `POST /saved-reports` | heavy, bot gate | Save a held run → `201 {report_id, manage_token, content_hash, saved_at, expires_at, report_path, url}` |
| `GET /saved-reports/{id}` | default | Metadata + `payload` |
| `GET /saved-reports/{id}.json` | default | The hashed bytes; `X-OpenCheck-Content-SHA256` |
| `POST /saved-reports/{id}/extend` | default | Manage token |
| `DELETE /saved-reports/{id}` | default | Manage token |

Every success carries `X-Robots-Tag: noindex, nofollow`, `/saved-reports` is in
the robots.txt disallow list, and nothing about a saved report is in any
sitemap. `503` when `OPENCHECK_SAVED_REPORTS_DB_FILE` is unset.

## Licence

A saved report holds whatever the run held — OpenSanctions' CC-BY-NC
statements included — so it carries the composite assessment computed at save
time: one non-commercial source makes the whole report non-commercial. Saving
is opt-in precisely so CC-BY-NC data is never stored at volume for no reader.

## Dispositions moved into the store

`dispositions.py` used to write `data/dispositions/<LEI>/<run_id>.json`, which
Render wipes on every deploy — so an analyst's sign-off did not survive one.
With `OPENCHECK_SAVED_REPORTS_DB_FILE` set, the live sheets live in a
`dispositions` table in the same file (keyed `(lei, run_id)` exactly as
before); a store-backed read still falls back to a legacy file. Unset, the
filesystem path is used as before. A saved report carries a frozen copy; later
edits to the live sheet never change it — save again for a new report.

## Deferred (potential follow-ups)

- "Save every row" on `/batch` — a saved batch (a list of saved report ids and
  one zip hash).
- "Save what this re-run found" on a watchlist entry, and feed entries linking
  to a saved report.
- Freezing the Background check, Subsidiaries, History and ESG tabs.
- A compare view of two saved reports of the same LEI.
