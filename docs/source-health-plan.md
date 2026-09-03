# Weekly source-health sweep — plan and status

**Prompted by:** PR #153 (Ariregister provenance) · **Phases A + part of B/C built:** 2026-08-21/22
**PR:** [#154](https://github.com/StephenAbbott/opencheck/pull/154) — every phase of this work lands on `feat/source-health-sweep` and ships as Phase 121
**Ticket:** [Regularly check health of OpenCheck data sources](https://app.notion.com/p/3c37f3dc29288056ac43d03d2f7e627a)

## Why the obvious version of this wouldn't have caught Estonia

The Ariregister bug defeats the naive design. `AriregisterAdapter` was working: it hit
`ariregister.rik.ee`, parsed the response, returned real officers and beneficial owners. A weekly
job that asked "does every adapter return a non-empty result?" would have gone **green every
week**.

What was broken was the *provenance*. The adapter builds its own `httpx.AsyncClient` (it needs a
bare HTML `Accept` header and a longer timeout), so it bypassed `http.build_client()` and with it
the implicit `provenance.record_live()`. With no observation recorded, `provenance.resolve()`
falls back to `stub`, and the UI stamped "Placeholder data" on genuine live results.

OpenCheck's output is a claim about provenance as much as about content, so a source can be fully
functional and still produce misleading output. Every probe therefore asserts **five** things:

| # | Assertion | Failure it catches |
|---|-----------|--------------------|
| 1 | Call completes inside a timeout | outage, DNS/TLS change, endpoint moved |
| 2 | Result is non-empty (unless the probe declares `allow_empty`) | API still 200s, shape drifted, parser yields nothing |
| 3 | **Resolved liveness is the expected value** | **the Estonia class** — right data, wrong provenance |
| 4 | Declared fields present and truthy | partial parse, silent degradation to a hollow answer |
| 5 | BODS statement counts, week over week | data still flows but ownership edges vanished |

Assertion 3 costs nothing to add and is the reason to build this at all. Assertion 4 turned out to
matter more than expected — see the Lithuania finding below.

---

## Phase A — built (2026-08-21)

| File | What it is |
|---|---|
| `backend/opencheck/sources/probes.py` | `PROBES` — one `SourceProbe` per registry id: tier, entry method + args, expected liveness, expected fields, required credentials and artifacts, anchor LEI, mapper name |
| `backend/tests/test_source_probes.py` | The two offline guards, plus metadata checks. Runs in `tests.yml`, gating every PR |
| `backend/scripts/source_health.py` | The sweep: concurrent probes, per-source timeout, retry-once, JSON + Markdown report, non-zero exit on any fail/degraded |
| `.github/workflows/source-health.yml` | Mondays 07:30 UTC, artifact upload, rolling GitHub issue |

### The probe table

Most adapters are not name-searchable — per `docs/sources.md` they're entered via an identifier
derived from the GLEIF anchor (`ee_registry_code`, `cz_ico`, `dk_cvr` …), so a probe is a
known-good **local identifier**, not a query. Subjects are large, old and boring — state utilities
and listed incumbents — so a fixture doesn't rot because a company was struck off.

Three fields carry more weight than their size suggests:

- **`expect_liveness`** — the Estonia assertion. Each call runs inside `provenance.recording()`
  and the resolved liveness must be in this set.
- **`expect_fields`** — top-level keys that must be present *and truthy*. Deliberately sparse:
  populated only where a shape has been verified against a real response. The sweep records the
  observed field names for every source in its JSON artifact, which is how the rest get filled in.
- **`known_gap`** — for an adapter whose provenance is knowingly wrong and not yet fixed, this
  asserts today's behaviour while the report prints the defect, so it stays visible instead of
  being asserted away. Both gaps it originally carried were closed on 2026-08-22 (below).

### The two offline guards (in `tests.yml`, not the weekly job)

Both are cheaper than the weekly job and prevent the two ways this goes quietly wrong.

**a. Probe coverage.** `set(PROBES) == set(REGISTRY)`. Adding an adapter without a probe fails CI.
Without this, "green" drifts into meaning "green for the sources someone remembered". The same
test also checks that each probe's method exists on its adapter, its mapper name resolves, and its
`requires_env` names are real `Settings` aliases — a typo there would skip a source forever,
silently.

**b. Provenance recording.** An AST check over `opencheck/sources/*.py`: any function that
constructs an `httpx` client directly must also record a provenance observation in the same
function. Exemptions are explicit, carry a reason, and a companion test fails if an exemption goes
stale — so every hole in the guard stays visible.

### Clean-cache guarantee

The sweep runs against a scratch data root (everything symlinked except `cache/live`) **and**
with cache reads stubbed out, so every probe genuinely contacts the upstream. Two things go wrong
without it: run the sweep twice locally and half the sources come back `cached`, having contacted
nobody; and the single retry poisons itself, reading back the failed attempt's own cache write.
CI never sees the first symptom — `data/cache/live` is gitignored, so a fresh checkout starts
empty — which is exactly why it would have gone unnoticed until someone trusted a local run.

### Reporting hygiene

The report carries ids, statuses, latencies, liveness values, result sizes and observed field
*names* only — no payloads, no personal names. Error text is truncated to 200 characters and
credential-shaped query parameters are redacted before anything is written. Same contract
`degraded_sources` and `/signalstats` already keep, and it matters because several sources are
CC BY-NC or carry personal data. Nothing is recorded to `data/` or committed.

### First run — 21 ok, 1 failed, 17 skipped (39 of 39 probed)

**1. Lithuania (`jar_lithuania`) is returning a hollow answer.** The JAR public interface replies
**HTTP 403** to OpenCheck's requests. The adapter tolerates this by design — it logs a warning and
returns a bundle carrying the GLEIF legal name with every register field null, marked
`is_stub: False`. From the outside that looks like a successful lookup. This is why the probe
asserts `status`, a register-only field: it separates "the register replied" from "we fell back to
what GLEIF already told us".

*Caveat before acting:* the 403 was observed from a cloud datacentre IP, and JAR may be blocking
by IP or user-agent rather than having changed. Worth reproducing from a residential connection
and from a GitHub runner before concluding the source is broken. Either way the silent degradation
is real, and the fallback bundle would be better surfaced in `degraded_sources` than passed off as
an answer.

**2. A second, latent instance of the PR #153 bug — fixed in this branch.** The AST guard found
`ariregister.fetch_timeline_data`'s X-Road SOAP path building its own client with no
`record_live()`. Nothing opens a provenance scope around the Time Machine path today, so it was
latent rather than user-visible, but it is the same defect on the same adapter and would have
become visible the moment that path was wired into a scope. One line, same semantics as #153.

**3. ClimateTRACE's bulk artifacts are absent from a fresh checkout** (`data/gem/` is gitignored),
so the probe reports `skipped — required local artifact absent` rather than failing. Honest, but
it means that source is never exercised in CI; worth deciding whether the weekly job should fetch
them.

---

## Adopted additions (agreed 2026-08-21)

Both were proposed as optional and are now part of the plan. The Phase A probe table already
carries the fields each needs (`anchor_lei`, `bods_mapper`), so both are small additions rather
than redesigns.

### GLEIF dispatch-drift check

For each adapter carrying a `LookupDeriver`, take the probe's `anchor_lei`, fetch the GLEIF anchor
record, and assert that `registeredAt.id` is still in the adapter's `ra_codes` and that
`normalise(registeredAs)` doesn't raise.

**Why it earns its place:** every national register is entered via a key *derived from GLEIF*. If
GLEIF renames a registration-authority code, or a registrar changes its number formatting, the
source stops being dispatched at all — while the adapter itself tests perfectly green, because its
own endpoint is fine. No per-source probe can see this: it is a failure of the join, not of either
side.

### Week-over-week BODS statement-count diff

Store per-source BODS statement-type counts in the JSON artifact; on the next run, diff against
the previous artifact and report a source whose counts collapse.

**Why it earns its place:** a BO-carrying source that drops from three person statements to zero
*for the same entity* is the earliest machine-detectable signal of an access change. That is
exactly the shape of Estonia's postponed legitimate-interest switch, which `docs/sources.md`
tracks as "no date announced" — there is no date to schedule a check against, so the data has to
be the alarm. It also catches the regression the five assertions miss: a source that still
answers, still resolves `live`, still has all its fields, but has quietly stopped carrying
ownership edges.

Needs care in two places: the previous artifact has to be fetched (last successful run's artifact,
or a committed baseline), and legitimate variation — a company genuinely filing a new PSC — must
read as a change, not an alarm. Report a *collapse* (non-zero → zero, or a drop past a threshold),
not any movement.

---

## Snapshot and curated sources need a *staleness* check, not a liveness check

For `cac_nigeria`, `eiti_bo`, `eiti_soe` and the bulk parquet adapters the failure mode isn't a
500 — it's the snapshot silently ageing out, or the upstream bulk file moving. Their probe should
also assert an upstream **freshness HEAD**: if the bulk file's `Last-Modified` is more than N days
newer than our snapshot, report `degraded: refresh due`. That turns the "curated snapshots go
stale silently" trap into an actual alarm. Phase C.

---

## Second pass — 2026-08-22

### Both provenance gaps closed

- **`eiti_soe` was over-claiming, not under-claiming.** The original note said it resolved `stub`;
  in fact, when payment rows are fetched live it resolved **`live`** — on the strength of the
  payments alone, while the bundle's central assertion (*this company is state-owned*) comes from
  a committed index built on `meta.source_snapshot`. It now records a snapshot observation on the
  index match, so the worst-liveness rule reports the bundle as only as fresh as its stalest part.
  The mirror image of the Ariregister bug: same missing observation, opposite direction of error.
- **`bce_belgium` recorded nothing at all** and would have resolved `stub` the day the source was
  switched on. It now records a snapshot on a DB hit, in both `search()` and `fetch()`. No
  retrieval time is claimed: `scripts/extract_bce.py` treats KBO's `meta.csv` as informational and
  doesn't persist the extract date, and the file's mtime says nothing about the register.
  *Follow-up: persist KBO's own extract date in a meta table so the snapshot can carry a real one.*

### GLEIF dispatch-drift check — built

`check_dispatch_drift()` fetches each probe's `anchor_lei` from GLEIF and asserts the anchor's
`registeredAt.id` is still in the adapter's `ra_codes`, then runs the adapter's own
`normalise(registeredAs)` and compares the result with the probe's identifier. Anchors were
resolved live against GLEIF, so each pairing is verified rather than assumed.

That the normalisers are load-bearing is not hypothetical: GLEIF returns Norway's registration
number as `'923 609 016'` with spaces and Croatia's zero-padded to nine digits (`'080000604'`
against the adapter's eight). The check runs them for real rather than assuming they still fit.

**9 of 23 identifier-dispatched sources are covered.** The rest have no anchor LEI yet and are
listed in the report as *not covered* rather than counted as passing — same honesty as the
credential skips. Adding an anchor is a one-line change per source.

### `expect_fields` filled — and the first choice was wrong

Filling them from the first run's observed field names immediately produced three failures that
were *my* error, and the error is instructive: `legal_name` is present in many bundles because the
adapter **echoes the GLEIF name in as a display fallback**, not because the register supplied it.
Asserting it proves nothing — it is exactly what makes the Lithuania 403 read as a successful
lookup. Every `expect_fields` entry is now a field only the source itself can supply.

### Two more bad probe subjects found by asserting fields

- **`ur_latvia`** — regcode `40003009556`, taken from a test fixture, is not in the register. It
  returned an empty entity, no officers and `is_stub: False`: the hollow-answer shape again, this
  time from a dead fixture rather than a dead source. Latvia itself is fine (Latvenergo returns 22
  entity fields and 5 officers). Subject moved to Latvenergo.
- **`rpo_slovakia`** — IČO `31320155` resolves to a *dissolved branch* of VÚB banka rather than the
  live parent, which is worth investigating on its own. And `address` is `None` for every active
  entity tried (SPP, Slovenská pošta, Slovnaft, Slovak Telekom), so it is either absent upstream or
  unparsed and cannot be asserted. Subject moved to SPP.

**The lesson worth keeping: a probe identifier lifted from a test file is not a verified subject.**
Both of these, and the original Lithuanian code, came from test fixtures that were fabricated or
stale. Probe subjects need checking against the live register once.

### Current state — 21 ok · 1 failed · 17 skipped

The one failure is Lithuania, still genuine and still unresolved (see above).

## Third pass — 2026-08-22 (Phase C)

### Week-over-week BODS statement counts

Each probe's answer is mapped through its BODS mapper and counted by record type, plus a histogram
of relationship interest types. Counts, not content: the artifact carries integers, never
statements, so nothing licence-restricted or personal is written down.

**The interest histogram replaced a single "beneficial ownership" bucket**, because the first
version was wrong in a way worth recording: which interest type carries beneficial ownership
varies by source. Estonia's ten beneficial owners map to `otherInfluenceOrControl`, not
`beneficialOwnershipOrControl`, so a bucket watching only the latter reported zero BO for a source
that carries plenty. Counting every type is both more honest and more informative.

The diff reports **collapses**, not movement — a drop to zero or past half the previous count. A
company genuinely filing a new PSC has to read as ordinary variation, or the check becomes noise
and stops being read. Verified against a synthetic previous run: an interest type going 2 → 0 is
reported, while person statements going 3 → 14 is correctly ignored.

Last week's counts come from **the previous successful run's artifact**, downloaded through the
Actions API. That can legitimately come back empty — 90-day retention, or a failed previous run —
and the report then says *"no comparison available"* explicitly, because a missing baseline must
never read as "nothing changed".

### Snapshot ageing

Snapshot and curated sources don't fail with a 500; they age out in silence. Each now declares a
`snapshot_max_age_days`, checked against the date the index itself declares (`built`, `harvested`,
`source_snapshot`) — never a file mtime, which records when git wrote the file locally and says
nothing about the register. Past the limit the source reports `degraded: refresh due`. Limits are
deliberately generous (180–240 days): a prompt to re-run the builder, not an SLA.

A `freshness_url` field exists for HEADing an upstream bulk file's `Last-Modified`, but none of the
three committed indexes has a stable file URL to point at — they're built from portals and APIs —
so they rely on the age check.

### Dispatch-drift coverage: 9 → 20 of 23

Eleven more anchors resolved against GLEIF, each verified by running the adapter's own
`normalise(registeredAs)` and checking it produces the probe's identifier. Where the probe subject
was still a generic test-fixture identifier it was realigned to the anchor (ČEZ Energy, Proximus,
Canadian National Railway, Ryanair Finance, Ørsted Services, OMV Austria E&P, Ignitis grupė, Neste,
Nestlé).

Three stay uncovered, deliberately, with the reason recorded on the probe: `abr_australia` (the
only ASIC-registered GLEIF records matching CBA are aircraft-leasing SPVs — a worse subject than
none), `mca_india` (the Indian records carrying an MCA number are small private companies, not
Infosys), and `malta_mbr` (no GLEIF record found at RA000443). Trading a good probe subject for
drift coverage would be a bad bargain.

## Phasing

| Phase | Scope | Status |
|-------|-------|--------|
| **A** | Probe table, both offline guards, sweep script, weekly workflow, rolling issue. | **Built 2026-08-21** |
| **B** | Add the credentials as repository secrets; lower `MAX_SKIPPED_FOR_CREDENTIALS` in the same commit as each. Decide the Lithuania 403. Fill `expect_fields` for the 15 credential-gated sources — they have never run, so there are no observed shapes to fill from until the secrets exist. | **The only phase left.** Both provenance gaps it listed are closed |
| **C** | Dispatch-drift check, `expect_fields`, week-over-week statement diff, snapshot ageing, 20 of 23 anchors. | **Complete 2026-08-22** |
| **D** | Publish `source-health.json` as a public data-source status page. | **Built 2026-09-03 (Phase 161)** — the workflow uploads `source-health.json`, `.md` and a rolling `source-health-history.json` (26 runs) to the `source-health-latest` release; `GET /source-health` reads them back hourly; `/sources` shows a sweep summary strip and, on every card, the sweep's word, when it was checked, and a *Health details* disclosure (last eight sweeps, freshness, latency, statement count, reason, known gap) that opens unasked on anything not plainly healthy. |

## Credentials to add (Phase B)

`ABN_GUID` · `BOLAGSVERKET_API_KEY` + `BOLAGSVERKET_CLIENT_SECRET` · `COMPANIES_HOUSE_API_KEY` ·
`CORPORATIONS_CANADA_API_KEY` · `CVR_DENMARK_API_KEY` · `DATA_GOV_IN_API_KEY` ·
`FIRMENBUCH_API_KEY` · `INPI_USERNAME` + `INPI_PASSWORD` · `NZBN_API_KEY` ·
`OPENCORPORATES_API_KEY` · `OPENSANCTIONS_API_KEY` (covers `everypolitician` too) ·
`SUDREG_CLIENT_ID` + `SUDREG_CLIENT_SECRET` · `WIKIRATE_API_KEY` · `ZEFIX_USERNAME` +
`ZEFIX_PASSWORD`

The workflow already maps all of them; each one added lifts its source out of
`skipped — not configured` with no code change. A missing secret is `skipped`; a 401/403 is
`fail` — key expiry is a real failure mode this catches rather than swallows.

## Relationship to `live-smoke.yml`

Keep it. `test_live_smoke.py` is the **deep tier** — full search → fetch → BODS-validate on five
open sources. The sweep is the **broad tier** — all 39, shallower, provenance-aware. Don't merge
the assertions: the deep tier's value is that it's deep.

## Silent green, case 2 and 3: EITI and EITI SOE — closed 2026-08-23

Both 403 from GitHub runners, and both still reported **ok**: they answer from their committed
indexes, and their probes declare `allow_empty=True`, so a payments list emptied by a refusal was
indistinguishable from a company that genuinely reported none. Visible only in the run log.

Both adapters now record a degradation when the live enrichment call fails. The classification
still stands — that is what the index is for — but the report and `degraded_sources` say plainly
that the payment rows are missing rather than absent.

**The sweep now reads the degradation channel, not a per-adapter bundle key.** Each probe runs
inside `degradation.recording()` as well as `provenance.recording()`, so *any* adapter that records
a degradation is reported amber automatically, with no per-source wiring in the sweep and no marker
for it to know about. The two channels answer different questions: provenance says **how current**
the payload is, degradation says **whether the source actually answered**.

Identical observations now collapse in the recorder. EITI fetches revenue per matched
organisation, so one 403 recorded the same sentence four times — a `degraded_sources` list with
four identical rows reads as four problems.

## What the sweep cannot see: fresh-process blindness

Found the hard way on 2026-08-23. INPI 401'd on every French lookup in production while the weekly
sweep reported it healthy — and both were right.

The adapter fetches a Bearer token once per process and holds it. Its refresh-on-401 path was dead
code (a double-checked-locking guard handed the expired token straight back), so INPI worked from
deploy until the first token expiry and failed on every lookup after that until the service
restarted. **The sweep starts a fresh process every run**, so it always takes the first-login path
and could never reach the expiry path at all.

The general shape: **the sweep validates adapter ↔ upstream, in a process seconds old, using CI's
credentials.** It cannot see anything whose cause is elapsed uptime, a stale in-process cache, or
the deployed service's own configuration. Those are real production failure modes and the weekly
green tick says nothing about them.

Two mitigations, neither of which the sweep can provide on its own:

1. **Unit tests for expiry paths** — where an adapter caches a credential, the refresh path needs
   an offline test that seeds a stale value. Now in place for INPI
   (`tests/test_inpi_token_refresh.py`); Bolagsverket and Sudreg were checked and are fine, both
   tracking `expires_at` from `expires_in` with a safety margin.
2. **A thin probe of the deployed API** — the option declined at the start of this work, worth
   revisiting now there is concrete evidence. Hitting `/lookup` on the live service exercises the
   long-lived process and its real environment, which is precisely the gap above. It answers a
   different question from the sweep rather than replacing it.

## Cadence — settled

**Weekly.** Considered running the key-free tier daily (it's ~21 round-trips, well under a minute)
and decided against: up to seven days of silent breakage is acceptable for these sources, and a
daily job on upstreams that publish weekly or monthly is noise for its own sake. Revisit only if a
real incident goes unnoticed for a week.
