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

## Three deltas, one shape

**Tier 1 — GLEIF.** `mirror_refresh.py` already streams and applies the
Golden Copy delta in-process every hour (Phase 180), from
`goldencopy.gleif.org` — a different host from `api.gleif.org`, so it costs
nothing against the throttled window. After each applied delta,
`apply_delta` hands the LEIs its three files named (LEI2, RR, REPEX) to
`watchlist.on_gleif_delta`, which intersects them with the watched set. A
watched LEI in the delta is read back from the mirror and its **material**
fields digested: legal name, entity status, LEI registration status,
jurisdiction, legal form, successors, direct and ultimate parent, the two
reporting exceptions, creation and expiration — and, since Phase 300, the
register identifier (`registered_at`, `registered_as`, `validated_at`), the
legal and headquarters address *countries*, entity category and
sub-category, and the policy conformity flag. `NextRenewalDate` and
`LastUpdateDate` are deliberately not in that set — a row whose only change
is one of those is renewal churn, and it changes no digest and queues
nothing. Only a changed digest queues a re-run.

**Why those nine (Phase 300).** GLEIF's own "what changed" tooling — the
7 Oct 2026 *Metric in Motion* post and its DataAlerts notebook — compares
every column of a delta row against the Golden Copy, and reading the 7 Oct
LastDay delta (29,665 rows) with that lens showed what the thirteen-field
set was treating as churn. The register number is the key every
identifier-dispatched adapter hangs off, so a change to it changes every
downstream fetch. `successors` carries the names GLEIF publishes without an
LEI (36 of 61 M&A rows that day had a `SuccessorEntityName` and no
`SuccessorLEI`); it reads as a list of `LEI — name` / `name` labels. The
address countries catch a headquarters moving jurisdiction; nothing below
country level joins, because an LOU re-keying a care-of address on renewal
(Birtley's annual Wimborne/Cranleigh ping-pong in GLEIF's field-modification
log) is exactly the noise the filter exists for. Managing LOU, validation
sources, other names and address lines stay out.

**A baseline that predates a field is not a change.** `diff_gleif_facts`
compares only the fields both sides carry, and `on_gleif_delta` digests the
new facts *projected onto the baseline's keys*. A watch stored under the
thirteen-field set that a churn-only delta names is upgraded to the current
shape in place (`WatchlistStore.upgrade_facts`, counted as
`gleif_rebaselined` on `/watchstats`) — no re-run, no entry, and
`last_checked_at` untouched, because the company did not change; OpenCheck's
field set did. The same reasoning as `verdict_template` and `signal_rules`
on the lookup snapshot, applied to Tier 1's own facts.

**Legal Entity Events (Phase 301).** GLEIF records corporate events on the
LEI record itself — a liquidation or dissolution opened or completed, a
merger, an absorption, a legal name or form change with its effective date.
On the 7 Oct 2026 LastDay delta, 19 liquidations, 7 dissolutions and a
merger were recorded as *in progress* on records whose entity status was
still ACTIVE and whose registration status had not moved, so the thirteen
original fields could not see them; across the whole Golden Copy that day,
20,287 material events were in progress. The mirror's `detail_json` now
keeps every event the CSV carries (up to five per record: type, status,
group, effective and recorded dates in UTC, validation documents and
reference; affected fields are not held), `gleif_record_from_row` serves
them in the live API's `entity.eventGroups` shape, and the material fact
`corporate_events` is the record's events as `{type, status, effective,
recorded}`, ordered by recorded date — a new event or a status moving
(IN_PROGRESS → COMPLETED, WITHDRAWN_CANCELLED) changes it. Every type is
material except `CHANGE_LEGAL_ADDRESS`, `CHANGE_HQ_ADDRESS` and
`CHANGE_OTHER_NAMES` (`CORPORATE_EVENT_EXCLUDED`), which are the same
renewal-time re-keying as the address lines — 145,000 of the LastMonth
delta's events. A type GLEIF adds later is material by default.

Three rules keep the change honest:

- **Only a full build may claim events.** A delta applied by this code
  writes events for the rows it touches, but only a full build makes "no
  events" mean "GLEIF published none" for every row. The build script sets
  `meta.detail_events` on a full build; `EntityStore.carries_events` reads
  it, and `gleif_facts` omits `corporate_events` entirely on a file without
  it — absent, not `None` — so the rebuild cannot look like a new event on
  every watched LEI.
- **The since-rule.** A watch taken before the mirror held events has no
  `corporate_events` in its baseline, but it has the mirror watermark it was
  read at (`gleif_watermark`). `diff_gleif_facts(…, since=)` reports the
  events GLEIF *recorded* after that watermark as new — they happened while
  the company was watched — and ignores those recorded earlier, which were
  already true when the watch began. Anything else is upgraded silently, as
  in Phase 300. The decision is now taken on the diff, not on a digest of
  the projected facts, so a key retired from the set (Phase 300's
  `successor_lei`) cannot queue a re-run that names no field.
- **A rebuilt mirror is re-read once.** A new file arrives by asset
  replacement at boot, never through `on_gleif_delta`, so a liquidation
  recorded last week on a watched company would otherwise wait for GLEIF's
  next touch of the record. The worker's tick calls `resync_after_rebuild()`
  before draining: on a file that carries events, once per `meta.built_at`
  (kept in the watchlist's `meta` as `gleif_resync_built_at`), it runs the
  hook over every watched LEI with `resync=True`. Hits queue with
  `trigger.resync`, and the entry opens "OpenCheck's GLEIF mirror was
  rebuilt from the … Golden Copy and now reads legal entity events" rather
  than claiming a delta; `gleif_resyncs` counts runs on `/watchstats`, and
  `gleif_deltas_seen` is not moved. Each monthly rebuild re-reads again,
  which is otherwise renewal churn.

An entry for an event reads "GLEIF recorded a legal entity event:
liquidation (in progress), effective 2026-10-05", a status move on the same
type "liquidation: in progress → completed", and its headline is "legal
entity event recorded" — above a register-status change, since it is the
more specific fact.

**Subsidiaries (Phase 302).** A watched parent now hears about its group.
`direct_children` is the sorted list of LEIs whose RR record says they are
directly consolidated by the watched company, read from the mirror's
`relationships` table (`EntityStore.direct_child_leis`) with the record's
own standing — relationship ACTIVE, registration PUBLISHED or LAPSED — so a
child whose *own* LEI lapses has not left the group. LEIs only: a
subsidiary renaming itself is not a change to its parent. Uncapped, by
decision: a large group re-runs whenever a subsidiary joins or leaves, and
the cost still tracks the deltas.

Two changes to which LEIs a delta names make it reach the parent at all.
The hook's set now includes the RR rows' **end nodes** (the parents), not
only their start nodes (the children) — the parent's own LEI2 record is
usually not in the delta when a child joins. And it includes each delta
child's **previous** direct parent, read from the mirror *before* the RR
file is applied (`mirror_build.previous_direct_parents`): when a subsidiary
moves from A to B the row names only the child and B, so without this A
would never learn it lost one. A renewed relationship record names the
parent too; its children are unchanged, so it is churn.

A baseline from before Phase 302 has no `direct_children`; the both-sides
rule upgrades it silently on its first touch, as Phase 300 does for every
new field. The entry names who moved, with names read from the mirror when
the entry is written (`name_children`, carried as `names` on the change):
"GLEIF lists a new direct subsidiary: Alpha Ltd (LEI)", "GLEIF no longer
lists a direct subsidiary: …", and a subsidiaries-only entry is headlined
"direct subsidiaries changed".

**GLEIF's own log (Phase 303).** `GET /api/v1/lei-records/{lei}/field-modifications`
is GLEIF's record of what changed, field by field: the XPath, `INITIAL` /
`INSERT` / `UPDATE` / `DELETE`, old and new values, and the date of the
Golden Copy publish that carried it (day granularity). Relationship changes
are logged under the *child* LEI with `context.relationshipType` and
`context.endNode`. The watchlist reads it in exactly two places, never on a
schedule and never per watched LEI:

- **On a GLEIF-triggered re-run** (`fetch_gleif_log`), one call fetches the
  log since the *oldest* baseline among the lists watching that LEI; each
  list's entry stores only the lines after its own baseline
  (`gleif_log.after`, keyed on the watch's `gleif_watermark`, so a line
  dated at the baseline's own publish is already in it). The feed and page
  show it under "What a re-run found" as "GLEIF's own modification log
  since …", ten lines then "and N more".
- **When a list first watches an LEI** (`record_prewatch_log`), one call
  fetches the 30 days before, stored on the watch and shown with it ("GLEIF's
  log in the 30 days before you started watching") — not in the feed, since
  it is not news. Re-adding a watched LEI costs no call.

Facts measured on 7 Oct 2026 shape `opencheck/gleif_log.py`: the date
filter accepts only an exact timestamp, so the log is read newest first
(`sort=-date`) in pages of 200 (the ceiling) until a line is no newer than
the cut-off, at most three pages — almost always one request; the XPath
strings are inconsistent (truncated, or missing the leading slash), so
labels come from the trailing element names; and renewal clocks and
re-keyed validation references (`NOISE_ELEMENTS`) are dropped, as the
digest drops them. Every request goes through `build_client()`, so through
the 50/min GLEIF throttle. Nothing raises: a failure is stored as
`{"available": false, "reason": …}` and worded as "could not be read", and
never blocks a watch or an entry. `/watchstats` counts `gleif_log_calls` and
`gleif_log_unavailable`.

The two columns arrive by `watchlist.sqlite` migration 2
(`entries.gleif_log_json`, `watches.prewatch_log_json`, both nullable; NULL
is "not fetched", distinct from a fetched log GLEIF could not serve). As
with every Phase 260 step, a build older than Phase 303 refuses to open a
file this one has migrated.

**Tier 2 — OpenSanctions.** They publish an entity-level delta per version:
`https://data.opensanctions.org/artifacts/default/<version>/entities.delta.json`,
JSON-lines of `{"op": "ADD|MOD|DEL", "entity": {…}}`, about 11 MB, roughly
four a day; `artifacts/default/versions.json` lists the versions. Every
version since the last one seen is streamed, and each organisation entity
(Company, LegalEntity, Organization, PublicBody — never Person) is matched
against the watched entities' own legal names at the 0.88 gate
`names.name_similarity` uses everywhere else, or exactly by `leiCode`. Zero
API calls, no re-screening, and the same public bulk file the licence
already covers; nothing is redistributed.

**A backlog is read oldest first (Phase 260).** At most
`OS_MAX_VERSIONS_PER_TICK` (12) versions are read per tick, and the watermark
(`meta.opensanctions_version`) moves past a version only once its delta was
read. Until Phase 260 the tick took the *newest* twelve and moved the
watermark past everything older, so after an outage of more than three days
a company sanctioned in one of the dropped deltas was never re-run and
nothing said so. Now the rest waits for the next tick — which reads it
straight away rather than after another three hours — and `/watchstats`
(`os_backlog`) and the page's honesty line say how many are still to read. A
failed download stops the tick with the watermark where it was, so that same
version is retried.

`versions.json` lists only the last ~100 versions (some 25 days). When the
watermark is older than the oldest listed version, the versions in between
can no longer be named, let alone read; and a listed version whose delta
answers 404 cannot be read either. Either is recorded as a **gap**
(`meta.opensanctions_gaps`, the last 20; `os_gaps` on `/watchstats`; the
newest three on the page) and answered with a **catch-up**: every watched
company is queued for a re-run on the `catch_up` tier. A catch-up entry is
written only when the re-run finds a difference — unlike an OpenSanctions
hit, the catch-up is not news about the company — and a real trigger for the
same LEI replaces a pending catch-up rather than being folded into it. Person screening deltas would put
UBO names into the store — that waits for the GDPR ticket, with email.

**Tier 3 — a register's own changes (Estonia).** Estonia is the one register
whose whole content OpenCheck rebuilds every day: the Phase 330 index, cut
from RIK's open-data files each morning. The refresh workflow
(`refresh-ariregister-index.yml`) downloads the build it is about to replace,
builds today's, and diffs the two with `scripts/diff_ariregister_index.py`
(about 40 s on the 380,000-company register). Per registry code it hashes
each **people list** — the persons on the registry card, the shareholders
(the shareholders file plus the AS shareholders recorded off the card), the
beneficial owners and the supervisory board — and compares the company's
**name, status and legal form**. An address, VAT number, contact, capital,
card entry, founder or auditor is not a change, and neither is a row
re-registered with the same content (its `kirje_id` is left out). A list's
change is recorded as its row count before and after — never who — so no
person reaches the watchlist store. A company absent from today's files is
`removed` (deleted companies are not published); a new one is `added`. The
day is appended to `ariregister-delta.json`, a rolling fourteen-day file
published beside the index (uploaded after it, so a day the file names is a
day the backend can load). A day touching more than 10 % of the register (and
over 1,000 companies) is kept as a summary with `suspect: true` and no codes:
a regenerated person key would otherwise re-run every watched Estonian
company with nothing to say.

The backend re-checks the index's release asset every hour
(`ARIREGISTER_REFRESH_INTERVAL_S`; before this it picked up a new index only
when it restarted), and the worker's `ariregister_tick` reads the delta only
once the server holds a newer index than the last day it read
(`meta.ariregister_cut`) — so the re-run reads the changed record, and the
file is fetched about once a day. A watch's registry code is GLEIF's
`registeredAs` where `registeredAt` is RA000181 (the mirror facts), else the
`EE-ARIREGISTER` identifier the baseline lookup read. Days are read oldest
first; several days for one company merge into one trigger (each list from
its first count to its last). The first run marks the current day and starts
from the next. A day the file cannot account for — a build that was not
diffed, a broken chain of `previous_cut`s, or a suspect day — is a **gap**:
recorded (`meta.ariregister_gaps`) and answered with a catch-up re-run of
every watched Estonian company (`reason: register_gap`). A file older than
the index (published a minute after it) is waited for. The entry on the
`register` tier is written even when the re-run finds nothing else, as a
Tier 2 hit is: a shareholder change moves no signal or status, so the
register's own record of it is the news. It reads "The Estonian e-Business
Register recorded a change to this company in its open data of 2026-10-17
(registry code 10421629)", then "Shareholders: 3 → 4 entries."

**No other register is polled.** The rest are re-fetched only as a
consequence of a hit on one of the tiers: the hit re-runs the whole
`_lookup_pipeline` with `refresh=True`, which fetches them anyway.
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
| `register_status` | The register's liveness class changed (`live` ↔ `declared` is not a change: the entity status is the same and the LEI registration move behind it is already a `gleif_field` change — Phase 291) |
| `identifier` | A one-per-entity identifier now has a different value |
| `signal_new` / `context_new` | A code appeared |
| `signal_retired` / `context_retired` | A code is gone **and the source that produced it answered** |
| `signal_unchecked` / `context_unchecked` | A code is gone but its producer is degraded — could not be re-checked |
| `coverage_changed` / `coverage_unchecked` | Sources answered, or sources with a record, went up or down; `_unchecked` when the fall is degraded sources. A baseline stored before Phase 241 (whose `answered` meant "with a record") is compared on `with_data` only |
| `verdict` | The sentence changed with none of the above (a degraded source recovering, say) |

The two `_unchecked` kinds are the point. Absence is a finding only when
the source answered.

**A rule change is not a company change.** Three stamps on every snapshot
keep OpenCheck's own changes out of the feed. `verdict_template` (Phase 245)
— verdicts from different templates are not compared. `RETIRED_SIGNAL_CODES`
(Phase 272) — a code the engine stopped emitting vanishing is not
`signal_retired`. `signal_rules` (Phase 273) — the risk-rules version a
snapshot was taken under; `SIGNAL_RULES_CHANGED` lists, per version, the codes
that version moved. Comparing a baseline from an older version, appearances
and disappearances of those codes — and the verdict, which is built from them
— are not reported; the baseline then moves on, so the next re-run compares
like with like, and every other code (a new sanctions listing, say) still
reports on the same re-run. Version 2 is Phase 273: the FATF / EU list signals
stopped reading subsidiaries, which moved to `SUBSIDIARY_LISTED_JURISDICTION`.
Version 3 is Phase 319: `EU_TAX_NON_COOPERATIVE` is new, and the subsidiary
note reads Annex I of the EU tax list too.
Version 4 is Phase 342: the sanctions 50% rule is computed, so
`SANCTIONS_OWNERSHIP_TEST` (or the context `SANCTIONS_OWNERSHIP_TEST_POSSIBLE`)
can appear with nothing in a company's records changed.
Bump `SIGNAL_RULES` and add an entry whenever a rule change moves which codes
fire for an unchanged company.

## Where state lives

One SQLite file on the Render persistent disk beside the GLEIF mirror
(`OPENCHECK_WATCHLIST_DB_FILE=/var/data/watchlist.sqlite`). The ticket's
original plan — a public GitHub release asset — predated Phase 180's disk.
A list is a **capability token**: random, shown once, kept by the browser
(`localStorage`) and stored server-side only as its SHA-256, so holding the
file never yields a feed URL. No accounts, no credentials, no personal data:
LEIs, the legal names GLEIF publishes for them, digests, diffs.

A held token the instance does not know — the list was pruned, or the token
was minted against another instance (a dev server, a rebuilt disk) — is
dropped the moment the backend answers 404 to it. On a report, *Watch for
changes* then mints a fresh list in the same click and says the old one is
gone; on `/watchlist` the page forgets it and says so in the empty state. A
`?token=` from a link is different: an unknown one is shown as an error with
a retry, since the link may simply be wrong. Only a 404 does this — a
network or server fault keeps the token, because the list may still exist.
Before this (three days after Phase 215 shipped) a browser carrying a dead
token failed every Watch with "No watchlist with that token." and could not
recover without clearing storage by hand.

**Schema-versioned and backed up (Phase 260).** The file carries `PRAGMA
user_version` (`watchlist.MIGRATIONS`, applied by `opencheck/sqlite_schema.py`
on open); a file written by a newer build is refused, and the routes answer
503 saying so. It is backed up daily, encrypted, to a private GitHub
repository — see [`backups.md`](backups.md). And every route and worker call
into it runs on a thread: the store waits up to 30 s on a write lock the
mirror-refresh hook may hold, and on the event loop one `/watch` request
doing that froze every request on the server.

Tables: `lists`, `watches` (per list and LEI: the baseline GLEIF facts and
their digest, the mirror watermark they were read at, the lookup snapshot,
last checked), `entries` (the log), `pending` (queued re-runs, one row per
LEI — a second trigger before the first ran folds into it), `meta` (the
last OpenSanctions version processed).

Caps: `OPENCHECK_WATCHLIST_MAX_TOTAL` (200) and `_MAX_PER_LIST` (10). Cheap
insurance given the arithmetic above, and a bound on the file.

Phase 234 stops one address holding the instance cap. `POST /watch/items`
is on the lookup tier, and its baseline — a full lookup when the reader has
not just run one — is charged to the caller's lookup budget. Both caps are
checked **before** the baseline runs, and a list is created only once its
first watch can be added, so a refused add runs nothing and mints nothing.
A tokenless add (a new list) is limited to `OPENCHECK_WATCHLIST_NEW_LISTS_PER_IP`
(5 a day); and a list nobody has opened for `OPENCHECK_WATCHLIST_STALE_DAYS`
(90) — every token-bearing request counts as opening it: the page, the Atom
feed, add, remove, re-check, context — is **warned, then deleted**. Since
pKYC stage C the worker's tick writes one notice entry into the list's log
at 90 days (tier `review`, about the list, not a company) and deletes the
list with its watches and entries `OPENCHECK_WATCHLIST_DELETE_GRACE_DAYS`
(30) later; any visit in between withdraws the notice and the entry stays as
history. The 404 says it: *A list that nobody opens or polls for 90 days is
warned in its feed, and deleted 30 days later.* A feed reader that polls
keeps its list alive indefinitely, so the rule reaches only the owner who
comes back late — which is why the notice has a grace period rather than a
single longer window. Before stage C the deletion was silent at 90 days; the
rule still stops an abandoned list keeping its share of the cap or being
re-run on every delta forever.

## The worker

`watchlist.watch_loop` runs `tick()` every `OPENCHECK_WATCHLIST_INTERVAL_S`
(300 s): drain queued re-runs, at most `_RERUNS_PER_TICK` (5) per tick, so
even a delta naming every watched LEI at once spreads over the hour; catch up
on OpenSanctions when `_OPENSANCTIONS_INTERVAL_S` (3 h) has elapsed, in a
thread; read the Estonian delta when the server's index has moved on (Tier
3, one meta read otherwise); prune lists that hold nothing and have not been opened for a day.
The first OpenSanctions run only records the current version — it never
scans history. Nothing is downloaded while nothing is watched.

A re-run that fails (GLEIF 429, say) is counted on `/watchstats` and leaves
the baseline where it was; the next delta that names the LEI tries again.

## Routes

| Route | Tier | Does |
|---|---|---|
| `POST /watch/items` `{lei, token?, context?}` | default | Watch an LEI; mints a list and returns the token when none is sent. The baseline is the lookup as it stands (replayed when the reader has just run it) plus the mirror's material fields |
| `GET /watch/{token}` | default | The list: watches with baselines, entries, caps, the feed URL, and what each tier is doing on this instance |
| `DELETE /watch/{token}/items/{lei}` | default | Stop watching; the watch's context goes with it |
| `PATCH /watch/{token}/items/{lei}` `{expected_jurisdiction?, expected_parent_lei?, rationale?, onboarding_report_id?}` | default | Say what you expect of this company (see *What the watcher expected*). A field sent as `null` is cleared; one left out is left alone. Writes no entry |
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
found no difference: OpenSanctions naming the entity is itself the news. So
does a register change (Tier 3), which lists the register's counts under the
opening sentence.

## Does it matter? The materiality tier

A change log answers *what changed?*; it did not say whether it mattered. A
`coverage_changed` from a briefly degraded register sat beside
`signal_new: SANCTIONED` with equal weight. Since the pKYC stage A work
every change the diff names, and every entry, carries one of three
**materiality tiers**, graded by a closed table in
`opencheck/watchlist_materiality.py` (`MATERIALITY_RULES` is 1):

| Tier | Key | Meaning |
|---|---|---|
| Note only | `note` | Recorded for the history; nothing to act on by itself. |
| Recheck the source | `recheck` | Something about the company's record or exposure moved; open the report and look. "Source", not "register": OpenSanctions, ICIJ or OpenAleph can prompt a recheck as much as a register. |
| Escalate | `escalate` | A finding against the subject, or the subject ceased or changed hands; route to whoever decides. |

The tier is **data, not workflow**: it starts no re-run and opens no case.
What to do stays the reader's. It is graded **once, at write time, and
kept** — the entry is the record, and "why was A reviewed and B not" needs
the tier A carried on the day. The table's version is stored beside it
(`materiality_rules`); an entry written before grading existed is graded on
read under the current table and says so (`materiality_rules: null`), never
back-filled. An entry's tier is the worst of its changes' tiers; an entry
with no changes is `note`, except a register-tier entry, whose trigger is
graded too (below). A kind, field, code, event type or list this table does
not know is `recheck`, never silent.

Where it shows: `materiality` on every entry and every change in
`GET /watch/{token}`; a second Atom `<category>` on each entry
(`scheme="https://opencheck.world/watchlist/materiality"`, beside the
unchanged tier category) and the tier word prefixed to the title for the two
material tiers ("Escalate — Shell plc: new risk signal"); a chip and a
"Show: All · Material only" filter on `/watchlist` (default All — a stale
list must stay visible); and `entries_by_materiality` on `/watchstats`,
which is how to tell whether the lines below are drawn in the right place.

The signal table is **its own table, not the graph's badge severity**
(`lib/graphStyle.ts`): that ranking is tuned for badge stacking —
`COUNTER_SANCTIONED` is deliberately inverted below `SANCTIONS_LINKED` so a
counter-listing never outranks a real finding on a node — and must not
silently decide triage. The three tables below are the public statement of
the rule; `tests/test_watchlist_materiality.py` fails if they and the module
disagree. Decisions are Stephen's, 17 Oct 2026.

### Change kinds (the snapshot diff)

| Kind | Tier | Why |
|---|---|---|
| `dissolution_date` | escalate | The register says the company has ceased. |
| `jurisdiction` | escalate | Redomiciliation — rare and real. |
| `legal_name` | recheck | A rename, or a different entity under the same number. |
| `legal_form` | recheck | A conversion (Ltd → plc) often accompanies a restructuring. |
| `identifier` | recheck | A re-registration, or a wrong join. |
| `founding_date` | note | A source corrected the incorporation date. |
| `signal_retired` | note | Good news is still news; a retirement never outranks note. |
| `signal_unchecked` | note | The producer did not answer. Absence of evidence is not a finding; the periodic-review line (part C) says which sources were not re-fetched. |
| `context_new` | note | A `kind="context"` observation never counts as a finding. |
| `context_retired` | note | As above. |
| `context_unchecked` | note | As above. |
| `coverage_changed` | note | About OpenCheck, not the company. |
| `coverage_unchecked` | note | As above. |
| `verdict` | note | The sentence moved with no graded kind behind it (a source recovering). |

Two kinds are graded by a rule: `register_status` is **escalate** when the
new class is `terminal` (dissolved, struck off, liquidated) and **recheck**
on any other move; `signal_new` takes the code's tier from the signal table.
`gleif_field` takes the field table.

### The material GLEIF fields (`gleif_field`, Tier 1)

| Field | Tier | Why |
|---|---|---|
| `direct_parent_lei` | escalate | Ownership or control changed — the first-listed pKYC trigger. |
| `ultimate_parent_lei` | escalate | As above. |
| `successors` | escalate | The entity was absorbed or merged away. |
| `expiration_date` | escalate | Entity ceased. |
| `expiration_reason` | escalate | As above. |
| `jurisdiction` | escalate | Redomiciliation. |
| `entity_status` | recheck | By rule: **escalate** when the new value is `INACTIVE`, recheck otherwise. |
| `corporate_events` | recheck | By rule, per event — see below. |
| `legal_name` | recheck | |
| `legal_form` | recheck | |
| `legal_address_country` | recheck | A move across a border (Phase 300 added it for this). |
| `hq_address_country` | recheck | As above. |
| `registered_at` | recheck | Re-registration, or the issuer corrected a wrong number. |
| `registered_as` | recheck | As above. |
| `direct_exception` | recheck | A parent appeared or was withdrawn behind a reporting exception. |
| `ultimate_exception` | recheck | As above. |
| `direct_children` | note | Below the subject: its own corporate activity, not its exposure. |
| `registration_status` | note | The LEI record's status, not the company's (Phase 242). |
| `validated_at` | note | Which authority the LOU checked against. |
| `creation_date` | note | A data correction. |
| `category` | note | A FUND / BRANCH / SOLE_PROPRIETOR reclassification. |
| `sub_category` | note | As above. |
| `conformity_flag` | note | Policy conformity of the LEI record. |

**Legal Entity Events**, by type and status (LEI-CDF 3.1). A terminal type
— `DISSOLUTION`, `LIQUIDATION`, `BANKRUPTCY`, `INSOLVENCY`,
`MERGERS_AND_ACQUISITIONS`, `ABSORPTION`, `BREAKUP` (the Phase 305
`TERMINAL_TYPES`) — is **escalate**; every other type the CDF defines
(`CHANGE_LEGAL_NAME`, `CHANGE_LEGAL_FORM`, `VOLUNTARY_ARRANGEMENT`, the
three `TRANSFORMATION_*`, `SPINOFF`, `DEMERGER`, `REVERSE_TAKEOVER`,
`ACQUISITION_BRANCH`, `TRANSFER_BRANCH`, `CLOSURE_BRANCH`) is **recheck**;
an unknown type is recheck. A `COMPLETED` event carries its type's tier,
`IN_PROGRESS` one step lower (a terminal event in progress is recheck), and
`WITHDRAWN_CANCELLED` is note — the event did not happen. An unknown status
reads as in progress, never as completed. Address and other-name changes
never reach the feed. A `corporate_events` change takes its worst event.

### Risk-signal codes (`signal_new`)

Drawn at two lines: **escalate** is a listing or control finding against the
subject or a party in its chain, or a black-list jurisdiction; **recheck** is
a linkage, a PEP, a debarment or an EU high-risk jurisdiction; **note** is
structure, state ownership, the grey and tax lists, leaks, counter-sanctions,
and every `kind="context"` code. `OFFSHORE_LEAKS` is note because the leaks
data is often years out of date; `STATE_CONTROLLED` because state control is
notable, not a risk.

| Code | Tier |
|---|---|
| `SANCTIONED` | escalate |
| `RELATED_SANCTIONED` | escalate |
| `SANCTIONS_CONTROLLED` | escalate |
| `RELATED_SANCTIONS_CONTROLLED` | escalate |
| `SANCTIONS_OWNERSHIP_TEST` | escalate |
| `SANCTIONED_SECURITY` | escalate |
| `EXPORT_CONTROLLED` | escalate |
| `RELATED_EXPORT_CONTROLLED` | escalate |
| `FATF_BLACK_LIST` | escalate |
| `SANCTIONS_LINKED` | recheck |
| `RELATED_SANCTIONS_LINKED` | recheck |
| `EXPORT_CONTROL_LINKED` | recheck |
| `RELATED_EXPORT_CONTROL_LINKED` | recheck |
| `DEBARMENT` | recheck |
| `RELATED_DEBARMENT` | recheck |
| `PEP` | recheck |
| `RELATED_PEP` | recheck |
| `EU_HIGH_RISK_THIRD_COUNTRY` | recheck |
| `COUNTER_SANCTIONED` | note |
| `RELATED_COUNTER_SANCTIONED` | note |
| `EXPORT_RISK` | note |
| `RELATED_EXPORT_RISK` | note |
| `FATF_GREY_LIST` | note |
| `EU_TAX_NON_COOPERATIVE` | note |
| `TRUST_OR_ARRANGEMENT` | note |
| `COMPLEX_OWNERSHIP_LAYERS` | note |
| `POSSIBLE_OBFUSCATION` | note |
| `NOMINEE` | note |
| `OPAQUE_OWNERSHIP` | note |
| `STATE_CONTROLLED` | note |
| `OFFSHORE_LEAKS` | note |
| `NON_EU_JURISDICTION` | note |
| `SUBSIDIARY_LISTED_JURISDICTION` | note |
| `GLEIF_REPORTING_EXCEPTION` | note |
| `RELATED_PEP_SUBJECT_ROLE` | note |
| `RECORD_CONSISTENCY` | note |
| `SANCTIONS_OWNERSHIP_TEST_POSSIBLE` | note |
| `COMPLEX_CORPORATE_STRUCTURE` | note |

(`COMPLEX_CORPORATE_STRUCTURE` was retired in Phase 272 and is never diffed;
it is listed so the table names every code the backend declares.)

### The register tier's own trigger (Tier 3)

A register-tier entry is written even when the re-run's snapshot shows
nothing new, so its trigger is graded too. The company leaving the
register's open data is escalate (deleted companies are not published); a
shareholder or beneficial-owner list moving, or the name, status or legal
form changing, is recheck; the registry-card persons or supervisory board
moving, or the company appearing in the open data, is note. A terminal
status the re-run then reads is escalated by the snapshot's
`register_status` rule above.

| Key | Tier |
|---|---|
| `removed` | escalate |
| `shareholders` | recheck |
| `beneficial_owners` | recheck |
| `name` | recheck |
| `status` | recheck |
| `legal_form` | recheck |
| `officers` | note |
| `supervisory_board` | note |
| `added` | note |

### What it does not do

No new change kind, no new detection, no email, no case. No per-list
customisation of the table — part B's watcher context is the place for
"your watch expected X"; a per-list override would be workflow. No
re-grading of the baseline or the verdict: materiality grades *changes*,
not companies. No automatic re-run on recheck: the tier says what is
proportionate, and `/recheck` exists for a reader who agrees. One
consequence worth stating: a watched company whose `SANCTIONED` signal goes
unchecked because OpenSanctions did not answer is a note-only entry, hidden
by "Material only"; the entry still says "could not re-check" in full. If
`entries_by_materiality` shows unchecked sanctions-family codes arriving
often, revisit as a `MATERIALITY_RULES` bump.

## What the watcher expected

Moody's "reuse onboarding context" (pKYC stage B). The materiality tier
says whether a change matters in general; this lets the watcher say what
they *expected*, so an entry can say whether a change contradicts it:
"GLEIF ultimate parent: X → Y — your watch expected X." Module:
`opencheck/watchlist_context.py`.

A watch carries four optional fields, written by whoever holds the list
token and read by the same (migration 4, all nullable; a NULL is "nothing
said"):

| Field | Holds | Rule at write |
|---|---|---|
| `expected_jurisdiction` | ISO 3166-1 alpha-2 or 3166-2 (`GB`, `US-DE`) | an ISO code, upper-cased; anything else is refused (422 naming the field) |
| `expected_parent_lei` | the LEI the watcher expects as GLEIF's **ultimate** parent | LEI shape and check digits; not resolved against GLEIF — a lapsed or unknown LEI is still the watcher's claim |
| `rationale` | one line on why the company is watched | at most 280 characters, one line; refused when it carries an e-mail address, a link other than an OpenCheck saved-report link, or a run of eleven or more digits (a personal code's shape). The form says "about the company, not a person" |
| `onboarding_report_id` | the saved report that was the onboarding record | must exist on this instance, be unexpired, and be a report **of the same LEI**. Only the id is stored; the report's own expiry governs it |

Two expectations and no more (Stephen, 17 Oct 2026): jurisdiction and one
parent. The comparison is made at entry-write time in `_write_rerun` and
kept on the change as `expectation: {field, expected, met}` — the entry is
the record of the day, and editing the context afterwards rewrites nothing.
A `gleif_field` / `ultimate_parent_lei` change is compared always; a
`direct_parent_lei` change only when the expected LEI is on either side of
it (a one-layer group's direct parent *is* its ultimate parent); a
jurisdiction change from either diff (GLEIF's field or the merged record's)
is compared with the expected code, and a country expectation (`US`) is met
by any of its subdivisions (`US-DE`). A change with nothing to compare
carries no key.

**Setting context writes no entry.** A watcher's belief changing is not a
change to the company. When the baseline already contradicts a freshly set
expectation (GLEIF's ultimate parent is Y; the watcher just said X), the
watch row says so at once — "Baseline ultimate parent is Y (Name); your
watch expected X" — computed on read from the stored GLEIF facts, never
stored.

**A contradicted expectation does not move the materiality tier.** The
tier is the closed table's (no per-list customisation — the stage A rule);
the contradiction is said beside the chip. A met expectation is said too:
"as your watch expected".

Where it shows: `/watch/{token}` carries `context` on each watch (the four
fields, `set_at`, the expected parent's current name resolved from the
GLEIF mirror on read, the pinned report's date, and `baseline_disagrees`)
and `expectation` on each change that has one; the Atom feed folds the
clause into the change's sentence and adds
`<category scheme="https://opencheck.world/watchlist/expectation" term="contradicted"/>`
to an entry with at least one `met: false` — the title is unchanged; the
`/watchlist` page shows the context under each watch with an edit form, and
the sentence in the log beside the materiality chip; the report page's
"Watch for changes" pins the saved report on the page (opened, or just
saved from this run) as the onboarding record; `/watchstats` counts
`watches_with_context` and `expectations_contradicted`, never the text.

Privacy: the context is capability-scoped like the list and backed up
encrypted (Phase 260). A saved report id is itself a read capability, so
pinning one to a watch lets anyone with the list token open the report —
the same reader in practice. No names are stored: the expected parent is an
LEI and its name is read from the mirror each time.

## Periodic review: when OpenCheck last looked

Moody's recasts periodic review as *confirmation that monitoring caught
what it should*. The watchlist never polls, so "nothing happened" to a
watched company can mean two things: nothing changed, or no delta named it.
Since pKYC stage C every watch carries a **review line**, computed on read
by `opencheck/watchlist_review.py` from what the store already holds and
never stored:

- **The clock is the last full re-run** (`last_checked_at`) — the moment
  every source was asked. A delta naming the company moves it; a GLEIF fact
  upgrade does not.
- **How fresh that run was**: the snapshot's `checked[]` carries each
  source's liveness and `retrieved_at`, so a source that answered from a
  cache or a bulk index older than the run is named with the copy's date
  ("2 of 9 sources answered from an older copy (UK Companies House 3 Jun
  2026 …)"), and the run's `degraded_sources` are named as not reached.
- **Due a review** when the run is `OPENCHECK_WATCHLIST_REVIEW_DAYS` (90)
  old — one instance-wide threshold, never tied to what the list has found
  (a review period that depends on findings is risk-rating drift by another
  name). A watch never re-run since it was added says so: "Baseline taken
  112 days ago and never re-run — no delta has named this company since."
- **The line never says the company is unchanged.** It says when OpenCheck
  last looked and how much of that look was fresh, and ends with the one
  action on offer — *Re-check by hand* — which is `/recheck`, as before. No
  automatic re-run: that would be a schedule, the one thing the watchlist is
  built not to be.

Where it shows: `review` on every watch in `/watch/{token}` and a
list-level `review` block ("3 of 12 companies have not been fully re-run in
90 days"); on `/watchlist` the sentence under each watch, a `warn`-tone
"Review due" chip and the list-level line; in the Atom feed the count in the
`<subtitle>` only — due-ness is a state, not a change, so no entry is written
per watch; on `/watchstats` `watches_review_due`, `lists_expiring` and
`lists_deleted_total`, counts only. The words are `REVIEW_WORDS`, mirrored in
`lib/watchlist.ts` and pinned by `test_watchlist_review.py`. "Mark reviewed"
is stage D's disposition, not this.

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
