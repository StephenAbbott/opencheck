# BODS quality sweep (Phase 316)

A weekly run that checks the **BODS statements production publishes**: whether
they validate against BODS v0.4, and whether their dates and provenance say
what OpenCheck's rules (see [dates.md](dates.md)) require. It is the third
sibling of the source-health sweep ([source-health-plan.md](source-health-plan.md)),
which asks whether every adapter answers, and the findings regression
([findings-regression.md](findings-regression.md)), which asks what the risk
engine concludes.

Why it exists: the BODS dates audit of 9 Oct 2026 found that for 41 sources
every deepened statement had been dated today with no `retrievedAt`
(Phase 314), and that PRH and CRO had answered HTTP 500 on `/deepen` for days
(Phase 315). Both other sweeps were green throughout: neither reads the
statements. The first run of this one, before it merged, found four more
defects in production, fixed in the same phase (below).

## Moving parts

| Part | Where |
|---|---|
| Checks, diff, report | `backend/opencheck/bods_quality.py` |
| Runner (CLI) | `backend/scripts/bods_quality.py` |
| Weekly workflow | `.github/workflows/bods-quality.yml` (Mondays 09:17 UTC + Run workflow) |
| Published report | `bods-quality-latest` release: `bods-quality.json`, `bods-quality.md`, `bods-quality-history.json` (26 runs of per-class counts) |
| Triage | the weekly Claude scheduled task "OpenCheck BODS quality: weekly triage of the sweep" (Tuesdays, below) |
| Offline tests | `backend/tests/test_bods_quality.py` |

## What it reads

- `GET /lookup?refresh=true` for every LEI in the findings golden set
  (`backend/findings_golden/`), one a minute: the statements of every deepened
  source, with `source_liveness` beside them, so the rules that depend on how
  a source was read can be applied.
- `GET /deepen` for every source-health probe subject `/deepen` can address
  (`probes.PROBES` entries with a one-argument `fetch` or `fetch_by_lei`, minus
  inactive sources and OpenAleph, whose `fetch` takes an entity id). This is
  what exercises each register. A deepen carries no liveness, so a probe's
  single `expect_liveness` value stands in for it.

## The checks

**Failures** — the statement is invalid or asserts something false:

| Class | Meaning |
|---|---|
| `schema` | A lib-cove-bods JSON Schema error. |
| `bods_additional` | A lib-cove-bods additional check, except the unknown-identifier-scheme advisory (the exclusion `test_bods_libcovebods.py` makes). |
| `date_in_future` | `statementDate`, a founding/dissolution/birth date or an interest date after the day of the run. |
| `statement_after_publication`, `retrieved_after_publication` | The claim, or OpenCheck's download, dated after the statement was published. |
| `start_after_end` | An interest that ends before it starts. |
| `bulk_dated_today` | A statement from a `snapshot` or `curated` source dated the day of the run — the Phase 314 conftest rule, applied to production. |
| `lookup_failed`, `deepen_failed` | The API returned an error for a subject that should map — the Phase 315 PRH/CRO class. |

**Warnings** — honest but weaker than it should be, or a known backlog:

| Class | Meaning |
|---|---|
| `cut_as_retrieval` | `retrievedAt` at midnight on the statement's own date, for a snapshot source that declared no separate `source_as_of`: the pre-Phase-314 conflation. Lookups only. |
| `no_retrieved_at` | A statement from a source that was read, with no `source.retrievedAt`. `meip` (publisher verbatim) is exempt. |
| `no_source_id` | A `source` block without `opencheckSourceId`. On the first run, all from the stored Open Ownership bundles (dates-audit Phase D). |
| `source_type` | `source.type` disagrees with `OFFICIAL_REGISTER_SOURCES`. |
| `ended_not_closed` | Every interest of a relationship has ended, but `recordStatus` is not `closed` (dates-audit Phase D). |

The report also carries, per source, how many statements it published, how
many carry `retrievedAt`, how many are dated by the retrieval day, and how many
distinct `statementDate` values they use. A source with every statement dated
by its retrieval day is the signature the dates audit removed; it is a number
to watch, not a rule to fail.

## Week over week

The diff is per `check|source` against the last **published** report: new,
resolved and changed counts. No published report means "no comparison
available", never "nothing changed". The job reports first and fails last:
any failure reds the run, with the report attached.

## Cadence

GitHub fires this repository's Monday schedules hours late. The Actions run
list, read on 9 Oct 2026, shows the 07:30 source-health cron starting between
13:37 and 16:17 UTC since August, and the findings regression's 08:30 one at
17:16. So the triage task runs on **Tuesday morning**, and treats a report
whose `generated_at` is more than three days old as "the sweep did not publish
this week" — which is itself the finding.

## The weekly triage task

A Claude scheduled task reads the published report every Tuesday and:

1. checks the run published this week (else reports that and stops);
2. reads the diff, and for every **new** or **grown** `check|source` pair
   reproduces one example against production (`/deepen`, or the statement in
   the report) and identifies the mapper or adapter at fault in the repo;
3. raises or updates one ticket per defect in the OpenCheck Tasks database in
   Notion, with the evidence and a proposed fix — never a duplicate of an open
   ticket for the same pair;
4. for a defect with a clear, contained fix, builds it on a branch with a
   test, runs the backend suite, and leaves it as a git bundle for review — it
   never pushes or merges;
5. writes a short dated note to the project (`claude/bods-quality-check-<date>.md`)
   with what was new, what was resolved and what it did.

Known backlog classes (`ended_not_closed`, `no_source_id` on the Open
Ownership bundles) are tracked as counts against the dates-audit Phase D
ticket rather than ticketed one by one.

## First run (9 Oct 2026, before merge)

45 deepen subjects, 927 statements. Fixed in this phase:

- **ANAF Romania** and **Firmenbuch** published `address.country` as a bare
  code (`"RO"`, `"AT"`); BODS v0.4 wants a jurisdiction object. Both now go
  through `_addr`, as do ONRC's representative addresses.
- **Firmenbuch** dates of birth arrive as `YYYYMMDD` and were passed through
  verbatim into `birthDate`, which the schema rejects. `_at_date_iso` now
  reads `YYYYMMDD` and `MM.YYYY`, and returns no date rather than an invalid one.
- **ΓΕΜΗ** published a sitting director's scheduled term expiry (2028) as
  `endDate`, asserting the interest had ended. A future `dtTo` now goes into
  the interest's details ("term runs to …"); only a past one is an `endDate`.
- **EITI SOE** asserted `beneficialOwnershipOrControl: true` with a state body
  as the interested party (lib-cove-bods
  `interest_beneficial_ownership_interested_party_not_person`). It is now
  `false`; `STATE_CONTROLLED` reads the `controlByLegalFramework` shape, not
  the flag.

Left as warnings for dates-audit Phase D: `ended_not_closed` on NZ Companies
(42), RPVS (14), SEC EDGAR (1) and Wikidata, and `no_source_id` on the stored
Open Ownership GLEIF and UK PSC bundles (519).
