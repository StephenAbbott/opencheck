# Saved reports

Phase 216 (backend). A `?lei=` link is a *query*, not a *record*: it re-runs
the pipeline, so two people opening it a day apart can see different
findings. A saved report is the record of exactly what OpenCheck showed for
one company on one date — opt-in, addressable, and verifiable.

The page that renders one (`/report/{id}`) is Phase 217; exports, the share
card and the MCP tool are Phase 218 (both below).

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
`OPENCHECK_SAVED_REPORTS_PER_IP` (20 a day, Phase 234) keeps one address from
filling that cap on its own: checked before a save (REST or MCP), spent only
when the save succeeds, `429` + `Retry-After` beyond it.
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

## The page — `/report/{id}` (Phase 217)

The saved report renders through **the same React report** as a live check.
`lib/api.ts` has one table, `LOOKUP_EVENT_HANDLERS`, mapping each lookup event
to its handler; `streamLookup` wires it to the live SSE connection and
`replayLookupEvents` feeds a saved report's stored events through it, and
`App.tsx` builds the handlers once (`buildLookupHandlers`) for both. A saved
report therefore cannot set page state differently from the check it keeps. A
fixture with a retired signal code (`lib/savedReport.test.ts`) pins that such a
code is carried, not dropped.

**The banner** (`components/cdd/SavedReportBanner.tsx`) never collapses. It
names the fifth clock beside the check's own ("Saved 16 Sept 2026, 15:18 UTC,
from a check that finished at 15:12 UTC"), says nothing on the page has been
re-checked since, gives the expiry, prints the full SHA-256 with a link to the
saved JSON, and offers *Run a live check*. *Keep for another 90 days* appears
only in the browser that holds the manage token. Wording lives in
`lib/savedReport.ts`; dates are always UTC and use Sept/June/July.

**What reaches for today's data is off, and says so.** Found while building it:
FullCheck is not a fold over the events (`FullCheckPanel` calls `/lookup`, and
its run controls call `/expand`), and every source card's Data drawer calls
`/deepen`. So on a saved report:

| Surface | Live | Saved report |
|---|---|---|
| QuickCheck, verdict, signals, source cards | stream | replayed events |
| Source Data drawer | `/deepen` | the saved `deepen_result` for that result (via `SavedReportContext`); the raw response is not kept, and a result not deepened in the check says so |
| FullCheck | `/lookup` + `/expand` | every saved `deepen_result` statement; Run FullCheck / Go deeper / Add next layer hidden |
| Summary | Generate, sign off | the saved narrative and its frozen dispositions, read-only; "No summary was saved" otherwise |
| Background check, Subsidiaries, History, Climate & ESG | live | a sentence in the tab: not part of a saved report |
| Securities, NZ associations | live | not shown |
| Licence panel | `/license-matrix` | the saved assessment |
| Format downloads, PDF, Markdown | re-run the check | built from the saved report (Phase 218, below) |
| Re-run, Retry source, Resume | live | not offered |

For this, `deepen_result` (internal, never streamed) now also carries
`bods_issues`, `risk_signals`, `license` and `license_notice` — reports saved
before Phase 217 lack them and the drawer fills the gaps.

**Saving a live check.** *Share and export* gains a *Keep* item. It is
`aria-disabled` — never `disabled`, so its reason stays reachable by keyboard —
while the check is streaming, after a per-source retry, or once the run is 15
minutes old, each with its reason (`saveEligibility`). A save posts the LEI and
`run_completed_at` only; if the summary was not written from this run the
report is saved without it and the reader is told. On success the saved link is
copied, the manage token is kept in `localStorage`
(`opencheck.savedReports.manage`, per report id), a one-line confirmation
appears under the subject, and the item becomes *Copy saved-report link*.

**Never indexed, never counted.** `<meta name="robots" content="noindex,
nofollow">` is added while a saved report is on screen (robots.txt already
disallows `/report` on this host), and analytics rolls `/report/{id}` up to
`/report` — the id is a capability.

## Reports, downloads, the share card and MCP (Phase 218)

**Nothing rendered from a saved report runs the check again.**
`routers.saved_reports.open_for_export(report_id, lei=None)` loads the report
(expiry and integrity checked, as every read), folds its events with
`fold_lookup_events` — the same fold `/lookup` uses — and returns that
response with a `saved` block: `report_id`, `content_hash`, `saved_at`,
`run_completed_at`, `report_url`, `json_url` and the frozen `licensing`. A
`lei` naming a different company is a 400.

| Route | With `saved_report_id` |
|---|---|
| `POST /export/pdf`, `POST /export/markdown` | Rendered from the saved events, with the saved narrative and frozen disposition sheet. Posting `narrative` or `dispositions` beside it is a 400 — the report can only say what was saved. |
| `GET /export?saved_report_id=…&format=…` | Every format from the saved events; `lei` optional (must match); `subsidiaries` is a 400 (the network was not saved). |

What changes on the document:

- **The first page says it is a saved report**: a *Saved report* band under
  the cover with both clocks ("Saved 16 September 2026, 15:18 UTC, from a check
  that finished at 15:18 UTC. Nothing in this report has been re-checked since
  it was saved."), the report id, the full SHA-256, the page and JSON links and
  how to verify. The title becomes "OpenCheck saved report — {name}", and *Run
  a live check* says the report is not live.
- **Every PDF page's footer** carries "Saved report {id}" and "SHA-256
  {hash}" (`@page` margin boxes). The id and hash are regex-checked before they
  reach CSS.
- **The licence position is the one assessed at save time**, marked "(as
  assessed when the report was saved)"; LICENSES.md and the xlsx/csv/gql/amlai
  licence sheets use it too.
- **Nothing reads today's clock.** The closing line is "Rendered from saved
  report {id}, saved {date}."; download names are
  `opencheck-{slug}-saved-{YYYYMMDD of the save}`; RDF's `run_date` is the
  run's date; the ZIP manifest's `generated_at` is the save,
  `sources_consulted` is the run's own `sources_applicable` (not today's
  registry), `saved_report` names the record, and zip entries are dated to the
  save — the ZIP is byte-identical across downloads (pinned). The Markdown is
  identical however the engine or the licence table changes afterwards
  (pinned). The PDF's *content* is identical; WeasyPrint's file metadata
  differs between renders, so compare text, not file hashes.

**The share link** of a saved report is `/share/saved/{id}` (API host): Open
Graph tags from the saved events — "Saved report · 16 Sept 2026 · N risk
signals found in that check · not re-checked since" — and a redirect to
`/report/{id}`. Its card, `/og/saved/{id}.png`, replaces *Visit
opencheck.world for more details* with *Saved report · {date} ·
opencheck.world*, and a zero-signal card says "in the check that was saved"
instead of quoting today's source count. Both read the report on every request
(so a deleted or expired report stops previewing), carry `X-Robots-Tag:
noindex`, and the PNG is cached by report id. The page's *Copy saved-report
link* and the confirmation after a save copy this URL.

**On the page**, *Share and export*'s PDF and Markdown items post
`{lei, saved_report_id}` (`reportRequestBody` in `lib/api.ts`), and *Download
data* keeps its format picker — every format with `saved_report_id` — beside
*Download the saved JSON*, without the subsidiary option. On a live check that
panel no longer calls its download "reproducible": a download runs the check
as it stands, and the sentence says so and points to saving
(`LIVE_DOWNLOADS` / `SAVED_DOWNLOADS` in `lib/savedReport.ts`).

**MCP.** `opencheck_save_report(lei, deepen_top=5)` runs `opencheck_lookup`'s
check (reusing a held run) and saves it with `save_from_replay` — the same path
as the page, so the same refusals. It returns `url`, `json_url`,
`content_hash`, `saved_at`, `expires_at`, `manage_token`, `verdict` and the
licence headline, and tells the agent the report is not re-checked. No
summary is saved through MCP (none is generated there). `TOOL_NAMES` is eight;
the server instructions and `opencheck_lookup`'s docstring name the tool.

## Deferred (potential follow-ups)

- "Save every row" on `/batch` — a saved batch (a list of saved report ids and
  one zip hash).
- "Save what this re-run found" on a watchlist entry, and feed entries linking
  to a saved report.
- Freezing the Background check, Subsidiaries, History and ESG tabs.
- A compare view of two saved reports of the same LEI.
