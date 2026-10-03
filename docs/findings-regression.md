# Findings regression (Phase 277)

A weekly run that asserts what the risk engine **finds** for a golden set of
LEIs, against production. It is the sibling of the source-health sweep
([source-health-plan.md](source-health-plan.md)). The sweep asks whether
every adapter is alive; this asks what the engine concludes from what the
adapters return.

Why it exists: Phases 126, 127, 132 and 139 all began "Stephen found it in
production", and the three defects of 2 Sept 2026 (verdict wording, MCP
shaping, per-card chip duplication) sat on the Shell report for weeks.
Nothing asserted engine output. The first run against production, before
this phase merged, found three things nobody had reported (below).

## Moving parts

| Part | Where |
|---|---|
| Golden files, one per LEI | `backend/findings_golden/*.json` |
| Assertions, diff, report | `backend/opencheck/findings_regression.py` |
| Runner (CLI) | `backend/scripts/findings_regression.py` |
| Weekly workflow | `.github/workflows/findings-regression.yml` (Mondays 08:30 UTC + Run workflow) |
| Published report | `findings-regression-latest` release: `findings-regression.json`, `findings-regression.md` |
| Rolling issue | label `findings-regression` |
| Offline tests | `backend/tests/test_findings_regression.py` |

## The golden set

| Role | Subjects | Why |
|---|---|---|
| curated | BP, Rosneft, Taqa Bratani, Eesti Energia, Ørsted, Eli Lilly | The homepage example cards. Their `EXAMPLE_LEIS` claims are checked here (see card drift below). |
| anchor | Shell | The report where the 2 Sept defects sat. A large group whose subsidiaries must not drive a list chip. |
| anchor | Bank Saderat PLC (`2138008KTNTDICZU8L25`) | Phase 273: FATF black + EU high-risk from Bank Saderat Iran **above** the subject. The upstream path must keep firing; black never also fires grey. |
| anchor | Maersk (`549300D2K6PKKKXVNN73`) | Phase 273: its only listed-jurisdiction exposure is a BVI subsidiary. No list chip, no watch-list verdict clause, only the `SUBSIDIARY_LISTED_JURISDICTION` context note. |
| anchor | ASDA Stores (`549300IVCS91O4IUFA35`) | Phase 272: a real layered chain (7 intermediate layers on 1 Oct 2026). The verdict counts intermediate layers; the retired composite never returns. |
| anchor | Regulatory DataCorp Limited (`894500LD30X1VN839203`) | Phase 282: a former director, "Mr. Robert Frederick Smith", matched Panama Papers officer "Mr. Robert Frederick White" until persons got a token gate. `OFFSHORE_LEAKS` must stay absent; the verdict must not mention offshore leaks. |
| control | Birtley Investment Limited (`254900RT9QQBQZVH8O89`) | Small GB Ltd, no findings. Its only signal is the GLEIF reporting exception, which is context. Lapsed LEI. |
| control | American Foreign Policy Council (`549300W96W2VKSMVDF81`) | Non-EU (US-DC, DLCP register), reporting exception, LEI lapsed since 2017, no findings. |

A curated golden file and an `EXAMPLE_LEIS` card exist together or not at
all: `test_curated_golden_files_are_exactly_the_homepage_cards` fails offline
when one is added or removed without the other.

## A golden file

```json
{
  "lei": "2138008KTNTDICZU8L25",
  "name": "Bank Saderat PLC",
  "role": "anchor",
  "why": "…",
  "example_card": false,
  "no_risk": false,
  "risk": {"FATF_BLACK_LIST": "high", "EU_HIGH_RISK_THIRD_COUNTRY": "high", "SANCTIONED": "high"},
  "context": ["NON_EU_JURISDICTION"],
  "absent": ["FATF_GREY_LIST"],
  "verdict": {"matches": ["sanctions findings on the company itself"], "not_matches": []},
  "sources_found": ["gleif", "companies_house", "opensanctions"],
  "liveness": {},
  "lei_confirmed_min": 2,
  "intermediate_layers_min": null
}
```

Every field is a **lower bound or a shape**, never a snapshot:

- `risk` lists codes that must appear as risk findings, each at or above a
  confidence floor. Other risk codes may appear too; they show up in the
  week-over-week diff, not as failures.
- `context` lists codes that must appear as context.
- `absent` lists codes that must not appear.
- `no_risk` (controls and Maersk) means no risk-kind code at all.
- `verdict` holds regular expressions the sentence must match and must not match.
- `sources_found` lists sources that must return a record.
- `liveness` holds the allowed liveness values per source, where it matters
  (MEIP is always `snapshot`).
- `lei_confirmed_min` is the floor for the SubjectCard's "LEI confirmed by N
  sources", counted the way the badge counts it (independent origins via
  `sources.lineage`).
- `intermediate_layers_min` is the floor for `graph_shape.intermediate_layers`.

Upstream data legitimately changes. Lithuania's PEP drop of 17 Aug 2026 would
have failed a snapshot every week. A genuine change is fixed by editing the
subject's file in a reviewed PR, and that PR is the audit trail.

## What fails a run

Each finding names its failure class, because the class says where to look.

| Class | Fires when | Earlier instance |
|---|---|---|
| `kind_mismatch` | An expected risk code arrives as context (or the reverse); a signal carries no `kind`; one code carries both kinds in one lookup | Phase 111 (context read as risk) |
| `missing_signal` | An expected code is absent, or below its confidence floor | — |
| `unexpected_signal` | A ruled-out code is present; a `no_risk` subject has a risk finding; a retired code (`risk.RETIRED_SIGNAL_CODES`) reappears | Phase 273's subsidiary-driven chips |
| `structural_repeated` | A code the lookup collapses to one per run (`_STRUCTURAL_SIGNAL_CODES`) appears twice | 2 Sept chip duplication; DQ-14 |
| `degraded_reads_clean` | Sources or screens degraded, no risk finding was made, and the verdict omits the caveat | Phase 146 |
| `placeholder_badge` | A source returned records but resolves `liveness: stub` ("Placeholder data") | Ariregister (PR #153), ONRC (PR #275) |
| `card_drift` | `EXAMPLE_LEIS` differs from the live risk codes or their highest confidence | The open drift item |
| `verdict` | The sentence misses a pattern, or matches a ruled-out one | 2 Sept verdict wording |
| `source_not_found`, `liveness`, `lei_confirmation`, `layers`, `lei_registration` | The remaining shapes in the file | — |
| `mcp_mismatch` | The MCP `opencheck_lookup` result disagrees with `/lookup` about the same run (codes by kind, verdict, degraded count, counts) | Phase 153 (MCP dropped `kind`) |
| `lookup_failed` | `/lookup` did not answer after retries | — |

An MCP call that fails outright is a **warning**, not a failure: the MCP
surface being down is the source-health sweep's business.

### What it deliberately does not assert

- **The label maps.** The 2 Sept ticket listed "a code in the UI/PDF/MCP label
  maps that the engine no longer emits, or the reverse".
  `tests/test_signal_label_coverage.py` already checks every emittable code
  against og_image, RiskChip, graphStyle and the narrative packet, in both
  directions, on every PR. Offline is the better place for that.
- **Per-card chips in the UI.** Phase 245 took structural chips off source
  cards. The API-level form of that class is `structural_repeated`.
- **The verdict's caveat when a finding was made.** `verdict.build_verdict`
  omits the completeness caveat when the sentence states a finding ("we found
  X" stays true whatever else failed), so `degraded_reads_clean` only checks
  sentences that state an absence.

## How a run reads production

For each subject, in file-name order:

1. `GET /lookup?lei=…&refresh=true` on the deployed API. This is a fresh run,
   charged to the runner's per-IP lookup budget (Phase 234). A 429 or 503
   waits out its `Retry-After` (capped at 120 s), up to two retries.
2. The MCP `opencheck_lookup` tool for the same LEI. It has no `refresh`
   argument and replays the run from step 1 out of the server's 15-minute
   cache, so it costs nothing and describes the same run.
3. Wait `--pace` seconds (60 by default) before the next subject.

One fresh lookup a minute stays inside the per-IP lookup budget (10/min) and
leaves the process-wide GLEIF throttle (50/min) to readers. Twelve subjects
take about fifteen minutes. Twelve lookups a week is well inside non-commercial
use of OpenSanctions; keep it there.

## Week over week

Pass/fail is the alarm; the diff is the interesting output. Each report stores
every subject's actual signals (codes, kinds, highest confidence, sources;
never summaries or party names). The next run lists what appeared,
disappeared, changed confidence or changed source. Example line:
"Rosneft: +RELATED_EXPORT_CONTROLLED (openaleph)".

The comparison is against the last **published** report (the
`findings-regression-latest` release), not the last successful run. The
source-health sweep diffs against its last success, so a legitimate change
there stays red every week until something else moves.

Rule changes are labelled, not reported as drift. The report records the
watchlist's stamps, `VERDICT_TEMPLATE` (Phase 245) and `SIGNAL_RULES` /
`SIGNAL_RULES_CHANGED` (Phase 273). A code whose rule version moved between
two reports is tagged "(rule change)", and a verdict change across a template
bump is tagged "(verdict template changed)". The stamps are read from the
checked-out code, so they describe production once the commit is deployed;
the report names the commit.

## Running it by hand

```bash
cd backend
uv run python scripts/findings_regression.py --out /tmp/findings
uv run python scripts/findings_regression.py --only 253400JT3MQWNDKMJE44 --no-mcp --pace 0 --out /tmp/rosneft
```

Exit status: 0 when every subject met its expectations, 1 when any did not, 2
when the run could not start. The workflow publishes the report and updates
the rolling issue before it fails the job, so a red run always has its report.

## Scheduling caveat

As of 2 Oct 2026, scheduled workflows in this repository do not appear to fire
on their own; every published source-health sweep came from a manual dispatch
(Notion ticket "Scheduled GitHub workflows are not running on their own").
Until that is fixed, start this one with **Run workflow** as well.

## First run (2 Oct 2026, before merge)

Run with this branch's code against production at 17:36 UTC (pace 20 s,
twelve subjects, every lookup answered first time, MCP comparison on all
twelve): **10 of 12 subjects as expected.** No `mcp_mismatch`, no
`structural_repeated`, no `placeholder_badge`, no `degraded_reads_clean`.

- **Rosneft, `kind_mismatch`.** `SANCTIONED_SECURITY` carried no `kind`:
  `securities.sanctioned_securities_signal` builds its dict by hand and
  omitted the field `RiskSignal.to_dict` always carries. Every consumer
  defaulted it to risk, so nothing visible was wrong, but the contract was
  broken. Fixed in this phase; clears once deployed.
- **Rosneft and Eli Lilly, `card_drift`.** `RELATED_PEP` now reaches `high`
  in production (OpenAleph for Rosneft, OpenSanctions for Eli Lilly); both
  `EXAMPLE_LEIS` cards say `medium`.
- Wikirate was degraded (`source_read`) on Eli Lilly and Shell. Both verdicts
  state findings, so no caveat is due and nothing fails; the degradation is
  recorded in the report.
