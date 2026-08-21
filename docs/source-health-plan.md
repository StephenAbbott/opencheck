# Weekly source-health sweep — plan and status

**Prompted by:** PR #153 (Ariregister provenance) · **Phase A built:** 2026-08-21
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
| 5 | *(Phase C)* BODS statement counts by type | data still flows but ownership edges vanished |

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
- **`known_gap`** — several adapters record no provenance observation at all on their normal path,
  so they resolve `stub` even when answering correctly. Rather than assert the correct-but-false
  expectation (permanently red) or the current-but-wrong one silently, those probes match today's
  behaviour and the report prints the gap so it stays visible. Closing a gap means fixing the
  adapter and tightening `expect_liveness` in the same commit.

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

## Phasing

| Phase | Scope | Status |
|-------|-------|--------|
| **A** | Probe table, both offline guards, sweep script, weekly workflow, rolling issue. Key-free sources exercised (21 of 39 green). | **Built 2026-08-21** |
| **B** | Add the credentials as repository secrets; lower `MAX_SKIPPED_FOR_CREDENTIALS` in the same commit as each. Fix the two `known_gap` provenance defects (`eiti_soe`, `bce_belgium`). Decide the Lithuania 403. | Next |
| **C** | GLEIF dispatch-drift check; week-over-week statement-count diff; snapshot freshness HEADs; fill `expect_fields` from the first green run's observed fields. | Planned |
| **D** | *(optional)* Publish `source-health.json` as a public data-source status page — fits the transparency positioning. | Optional |

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

## One thing still to decide

Weekly means up to seven days of silent breakage. The key-free tier is cheap enough (~21 HTTP
round-trips, well under a minute of wall-clock) to run **daily** at no meaningful cost or upstream
burden, with the key-gated tier staying weekly to conserve quota. Worth considering for
BO-carrying registers, where a week of "Placeholder data" badges is a week of misleading output.
