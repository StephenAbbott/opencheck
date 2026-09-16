# Watchlist and change alerts

Phase 215. "Watch this LEI, tell me when something changes." A watchlist is
a saved batch that re-runs on a delta — and the delta is the whole design.

## The constraint first: re-checking must not become a crawl

The obvious design — re-run every watched lookup daily — reproduces the
crawl wave of 29 August 2026: every watched LEI a cold anchor of four to
six GLEIF calls plus ten upstream fetches, several rate-limited (KvK 429s,
ΓΕΜΗ 8/min, Datafordeler 45 s) and one CC-BY-NC at volume. So OpenCheck
never polls a company. It reads the two deltas that are published for it
and re-runs only what they name.

Measured on the 3 September 2026 08:00 UTC Golden Copy publish: 15,920 of
3,420,368 LEI records changed in a day (0.47 %). At 200 watched LEIs that is
an expected 0.9 re-runs a day; at 1,000, 4.7. Re-run cost is proportional to
how much of GLEIF changes, not to how many LEIs are watched.

## Two deltas, one shape

**Tier 1 — GLEIF.** `mirror_refresh.py` already streams and applies the
Golden Copy delta in-process every hour (Phase 180), from
`goldencopy.gleif.org` — a different host from `api.gleif.org`, so it costs
nothing against the throttled window. After each applied delta,
`apply_delta` hands the LEIs its three files named (LEI2, RR, REPEX) to
`watchlist.on_gleif_delta`, which intersects them with the watched set. A
watched LEI in the delta is read back from the mirror and its **material**
fields digested: legal name, entity status, LEI registration status,
jurisdiction, legal form, successor, direct and ultimate parent, the two
reporting exceptions, creation and expiration. `NextRenewalDate` and
`LastUpdateDate` are deliberately not in that set — a row whose only change
is one of those is renewal churn, and it changes no digest and queues
nothing. Only a changed digest queues a re-run.

**Tier 2 — OpenSanctions.** They publish an entity-level delta per version:
`https://data.opensanctions.org/artifacts/default/<version>/entities.delta.json`,
JSON-lines of `{"op": "ADD|MOD|DEL", "entity": {…}}`, about 11 MB, roughly
four a day; `artifacts/default/versions.json` lists the versions. Every
version since the last one seen is streamed, and each organisation entity
(Company, LegalEntity, Organization, PublicBody — never Person) is matched
against the watched entities' own legal names at the 0.88 gate
`names.name_similarity` uses everywhere else, or exactly by `leiCode`. Zero
API calls, no re-screening, and the same public bulk file the licence
already covers; nothing is redistributed. Person screening deltas would put
UBO names into the store — that waits for the GDPR ticket, with email.

**There is no third tier.** National registers are never polled. They are
re-fetched only as a consequence of a Tier 1 or Tier 2 hit: the hit re-runs
the whole `_lookup_pipeline` with `refresh=True`, which fetches them anyway.
Freshness is a consequence of an observed change rather than a clock, and
"OpenCheck does not continuously monitor the registers" describes the
mechanism rather than disclaiming it. The Phase 146 rule is inherited: a
source degraded during the re-run reports "could not check", never "clean".

## What a "change" is

`snapshot_from_response` reduces a `LookupResponse` to the facts a feed
reader compares, using the helpers the report and the batch row already
use — `subject_profile` for register status, founding date and legal form;
`consistency.one_per_entity_identifiers` for identifiers; the risk-signal
codes with the source that produced each; the Phase 156 coverage figures;
the verdict; and `source_liveness`, which is how the entry can say when
each source was actually reached. `diff_snapshots` then names every
difference with a closed vocabulary (`CHANGE_KINDS`):

| Kind | Meaning |
|---|---|
| `gleif_field` | A material GLEIF field (Tier 1's own facts) |
| `legal_name`, `jurisdiction`, `founding_date`, `legal_form`, `dissolution_date` | The profile facts |
| `register_status` | The register's liveness class changed |
| `identifier` | A one-per-entity identifier now has a different value |
| `signal_new` / `context_new` | A code appeared |
| `signal_retired` / `context_retired` | A code is gone **and the source that produced it answered** |
| `signal_unchecked` / `context_unchecked` | A code is gone but its producer is degraded — could not be re-checked |
| `coverage_changed` / `coverage_unchecked` | Sources answered went up or down; `_unchecked` when the fall is degraded sources |
| `verdict` | The sentence changed with none of the above (a degraded source recovering, say) |

The two `_unchecked` kinds are the point. Absence is a finding only when
the source answered.

## Where state lives

One SQLite file on the Render persistent disk beside the GLEIF mirror
(`OPENCHECK_WATCHLIST_DB_FILE=/var/data/watchlist.sqlite`). The ticket's
original plan — a public GitHub release asset — predated Phase 180's disk.
A list is a **capability token**: random, shown once, kept by the browser
(`localStorage`) and stored server-side only as its SHA-256, so holding the
file never yields a feed URL. No accounts, no credentials, no personal data:
LEIs, the legal names GLEIF publishes for them, digests, diffs.

Tables: `lists`, `watches` (per list and LEI: the baseline GLEIF facts and
their digest, the mirror watermark they were read at, the lookup snapshot,
last checked), `entries` (the log), `pending` (queued re-runs, one row per
LEI — a second trigger before the first ran folds into it), `meta` (the
last OpenSanctions version processed).

Caps: `OPENCHECK_WATCHLIST_MAX_TOTAL` (200) and `_MAX_PER_LIST` (10). Cheap
insurance given the arithmetic above, and a bound on the file.

## The worker

`watchlist.watch_loop` runs `tick()` every `OPENCHECK_WATCHLIST_INTERVAL_S`
(300 s): drain queued re-runs, at most `_RERUNS_PER_TICK` (5) per tick, so
even a delta naming every watched LEI at once spreads over the hour; catch up
on OpenSanctions when `_OPENSANCTIONS_INTERVAL_S` (3 h) has elapsed, in a
thread; prune lists that hold nothing and have not been opened for a day.
The first OpenSanctions run only records the current version — it never
scans history. Nothing is downloaded while nothing is watched.

A re-run that fails (GLEIF 429, say) is counted on `/watchstats` and leaves
the baseline where it was; the next delta that names the LEI tries again.

## Routes

| Route | Tier | Does |
|---|---|---|
| `POST /watch/items` `{lei, token?}` | default | Watch an LEI; mints a list and returns the token when none is sent. The baseline is the lookup as it stands (replayed when the reader has just run it) plus the mirror's material fields |
| `GET /watch/{token}` | default | The list: watches with baselines, entries, caps, the feed URL, and what each tier is doing on this instance |
| `DELETE /watch/{token}/items/{lei}` | default | Stop watching |
| `POST /watch/{token}/recheck` `{lei}` | heavy | Re-run now — a deliberate human action, never a schedule. Writes an entry only when something changed; always reports what it found |
| `GET /watch/{token}.atom` | default | The Atom feed: one entry per logged change, `private, no-store`, `noindex` |
| `GET /watchstats` | exempt | Aggregate watcher state; no LEI or name can appear |

`/watch` is disallowed in `robots.txt`. With `OPENCHECK_WATCHLIST_DB_FILE`
unset the routes answer 503 and no background work runs.

## The entry

Every entry opens with the tier that fired, in its own words — "GLEIF
published a change to this record in its 2026-09-15 16:00:00 delta (LEI
registration status)", "OpenSanctions added *X*, matching this company by
name — eu_fsf", "Re-checked by hand" — then what a re-run found, one
sentence per change, then "*N* sources checked as a result on *date*" from
`source_liveness` (the retrieval clock, Phases 99/100), and any source that
could not be checked. A Tier 2 hit writes an entry even when the re-run
found no difference: OpenSanctions naming the entity is itself the news.

## Demo framing

The watchlist page's honesty line reads the mirror: "OpenCheck holds
3,424,074 LEI records as of 2026-09-16 00:00 UTC; the last delta changed
3,903 of them, and only a watched LEI in that delta is re-run." That is the
story — 3.4 million records, thousands changed today, one of them was yours
— and it is true because it is a description of the mechanism.

## Not in v1

- Email (v2): a double opt-in mailing list, not a user database — one row
  of address, confirmed_at, unsubscribe_token, LEIs. Forces the privacy
  notice and a DPA with the sender; it does not force accounts.
- Person watching and related-party screening deltas (with the GDPR
  ticket).
- Tier 1 without a mirror: on an instance with no `entity_pages.sqlite`
  the GLEIF tier cannot fire and the page says so.
