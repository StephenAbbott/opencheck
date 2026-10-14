# Institutional shareholders (SEC Form 13F)

Phase 333 builds the index and Phase 334 reads it into a frozen lookup event
next to the primary listing. Phase C shows it, as a collapsed "Institutional
shareholders" section under Securities on QuickCheck: the largest manager
by shares, and the next nineteen when opened. The
Notion ticket is *Institutional holders from SEC Form 13F*; the decisions
below are Stephen's, of 14 Oct 2026.

## What a 13F is, and what it is not

An institutional investment manager with investment discretion over at least
$100 million of "Section 13(f) securities" files Form 13F within 45 days of
each calendar quarter's end, listing its long positions at that date. **A 13F
filer is not a beneficial owner.** "Vanguard Capital Management LLC" reports
shares it has discretion over, on behalf of funds and clients.

- **Never in the ownership graph and never counted as beneficial ownership.**
  The index never becomes a BODS statement, is never a registered source,
  never counts towards coverage, and no risk signal reads it.
- **Long positions only**, as at the quarter end, filed up to 45 days later.
- **Never totalled.** Positions overlap: a manager's combination report can
  include another manager that also filed the same position itself (171
  filings did so for Moody's in June 2026). On Moody's the 1,442 reported
  positions add up to 424.5 million shares, 2.5 times the 173.2 million
  outstanding.
- **Confidential treatment.** A manager can be allowed to leave a position
  out (84 filings say they did, in June–August 2026). A manager missing from
  the list is not evidence of anything.
- A manager's 13F figure and its Schedule 13G figure can differ: they are
  dated differently and filed under different rules (Vanguard Capital
  Management: 5.63% on the 30 June 2026 13F, 6.41% on its 13G).

## The index (Phase 333)

`backend/scripts/build_form13f_index.py`, standard library only.
`.github/workflows/refresh-form13f-index.yml` checks weekly and rebuilds when
the SEC lists a newer data set, publishing `form13f.sqlite.gz` and
`form13f-meta.json` to the `form13f-index` release.

| Table | Holds |
|---|---|
| `meta` | `period` (the quarter end), `filed_through` (the last filing day the newest data set covers), `data_sets`, `built_at`, counts, `license` (`US-PD`) |
| `managers` | one row per manager (CIK): name, the report's accession number, last filing day, report type, whether amended, managers included, confidential-treatment flag |
| `securities` | one row per CUSIP: the issuer name and class most filers use, positions kept, positions stored, positions left out by each check, the median implied price |
| `positions` | the top 100 positions per CUSIP by shares: manager, shares, value, `value_check`, investment discretion |
| `shares_outstanding` | per issuer CIK, the `dei:EntityCommonStockSharesOutstanding` fact dated nearest the period, with its date and accession number |

**30 June 2026** (built 14 Oct 2026 from the June–August and March–May data
sets): 8,857 managers, 23,363 securities, 2,391,325 positions (683,945
stored), 4,623 shares-outstanding facts; 30 MB, 11 MB gzipped, 1.5 minutes.

### How each position is chosen

- **The period** is the one with the most holdings reports in the newest data
  set. Each set is cut by filing date, so it also holds late filings for
  older quarters (back to 2001), and the previous set is read too for the
  period's early filers.
- **One report per manager**: the latest original or RESTATEMENT, plus every
  NEW HOLDINGS amendment filed after it. An amendment with no stated type is
  read as a restatement. Notices (`13F-NT`) carry no holdings.
- **Shares only, no options**: `SSHPRNAMTTYPE` `SH` (principal amounts are
  bonds), no `PUTCALL` rows, summed per manager across the report's rows
  (Berkshire's 24,669,778 Moody's shares are three rows).

### The checks — nothing is repaired

| `value_check` | Meaning |
|---|---|
| `ok` | value ÷ shares within a factor of two of the security's median implied price |
| `thousands` | within 25% of a thousandth of it (`THOUSANDS_BAND`, narrowed in Phase 334): the value is still reported in thousands, as before 3 January 2023. 254 managers, T. Rowe Price among them, report nine in ten or more of their stored positions this way (206 report all of them); for 466 securities the largest position is one |
| `unchecked` | fewer than five positions report a value to compare |

Left out and counted (`excluded_price`, `excluded_name`):

- a position whose price is in neither band — CalSTRS reported 100,442,257
  "shares" of Moody's worth $221,766 (shares and value transposed);
- a position whose issuer name carries none of the security's common name
  tokens *and* whose price does not vouch for it — "Axon Enterprise" under
  Berkshire A's CUSIP prices at a thousandth of it. A short or former name at
  the issuer's own price ("IBM", "RAYTHEON", "SCHLUMBERGER") is kept.

## Reading it (Phase 334)

`backend/opencheck/form13f.py`. The backend downloads the release asset at
boot (`warm_index`, the ariregister arrangement: replaced when the asset
changed, kept otherwise; `OPENCHECK_FORM13F_DB_FILE` / `_DB_URL` — on Render
`OPENCHECK_FORM13F_DB_FILE=/var/data/form13f.sqlite`, the persistent disk, so
a restart keeps the file rather than downloading it again; the warm-up is the
last step of the boot sequence, so `/signalstats` says `available: false` for
the first few minutes after a deploy). One lookup
event, `form13f`, is built after the sources settle and before the subject
profile, folded into `LookupResponse.form13f` as recorded — so a saved report
shows the quarter of its own day, and the PDF, Markdown and MCP read the
frozen payload. No index, or no security of the company's in it: no event.
A failed read is logged and the lookup carries on; it is never a degraded
source. `/signalstats` → `form13f` says whether the index is loaded and for
which quarter.

**Never ownership, by construction.** No module that decides or describes
risk, ownership or the verdict imports `form13f` (`risk`, `cross_check`,
`icij_check`, `openaleph_check`, `verdict`, `pep_merge`, `related_targets`,
`degradation`, `coverage`, `subject_profile`, `subject_identity`,
`reconcile`, `watchlist`, and everything under `bods/`, `narrative/`,
`sources/`, `timeline/` — `tests/test_form13f.py` parses them). A pipeline
test feeds a payload naming a sanctioned bank and checks the signals, the
verdict, the degraded sources and the statements are unchanged. Every
sentence is held to `BANNED_VOCABULARY` (owner, owns, owned, beneficial,
stake, control) except `DISCLAIMER`, the one sentence that says what 13F is
not.

### Matching a company to its securities

13F rows carry a CUSIP, the issuer's name and nothing else — no LEI, no CIK.

- **GLEIF's ISIN file** (the Phase 258 table, read with
  `isin_index.isins_with_prefix(lei, "US")`, which bisects to the run of US
  ISINs): a US ISIN is `US` + CUSIP + a check digit. This reaches 638 of the
  801 most widely held company CUSIPs. ADR ISINs map to the foreign issuer
  (Novo Nordisk, Shell).
- **The SEC EDGAR source's own statements**: its mapper puts the issuer's
  CUSIP, read off the company's Schedule 13D/13G filings, on the issuer's
  entity statement (scheme `CUSIP`). This reaches Chevron, which GLEIF lists
  no ISIN for, and issuers incorporated outside the US whose CUSIP starts
  with a letter (Accenture, Medtronic).
- Rejected: the six-character CUSIP issuer prefix, and issuer names.

### What is listed

- **Equity CUSIPs reported by at least `MIN_MANAGERS` (10) managers.** A
  CUSIP's issue number (characters 7–8) is digits for an equity issue and
  carries a letter for debt. JPMorgan matches 21 securities: its common
  stock, and twenty preferreds and notes one to nine filers entered as
  shares — those are counted in `not_listed_note`, never silently dropped.
- **Twenty positions per security** (`TOP`), the largest by shares, with the
  number of the rest. The section shows the first and opens to twenty.
- **A value in thousands** shows its shares and not its value, with
  `THOUSANDS_NOTE` (Stephen, 14 Oct 2026).
- **ADRs and depositary shares** (title of class ADR / ADS / DEP /
  depositary / sponsored) are labelled, with `ADR_NOTE`, and never get a
  percentage (Stephen, 14 Oct 2026).
- A position larger than the shares outstanding is left out and counted.

### The percentage

Only when the security is not an ADR, the company has one listed class of
shares, and the SEC frames carry the issuer's undimensioned
`dei:EntityCommonStockSharesOutstanding` dated within 100 days of the
period; the issuer's CIK is the one the SEC EDGAR source resolved —
from OpenCorporates or, for most issuers, from the legal name. Phase 334
passed only the first, so production showed no percentage for Moody's until
Phase 335 passed the name-resolved CIK too (kept out of the response's
`derived_identifiers`, which never carried it). Labelled
as computed: "Of 173.2 million shares outstanding on Moody’s Corporation's
10-Q/10-K cover, 30 June 2026 — computed by OpenCheck." Berkshire's 24,669,778
Moody's shares are 14.24%, the figure commercial vendors show. Berkshire and
Alphabet (two listed classes) get none; JPMorgan's BlackRock position is 7.76%.

### Measured on the 30 June 2026 index

| Company | Listed | Largest position |
|---|---|---|
| Moody's | COM, 1,401 managers | Berkshire Hathaway, 24,669,778 shares, 14.24% |
| Berkshire Hathaway | CL B (4,887), CL A (1,404); 1 more not listed | BlackRock, 121,183,112 B shares |
| Alphabet | CL A, CL C, two depositary shares | BlackRock, 460,972,803 A shares |
| JPMorgan Chase | COM (5,115); 20 more not listed | BlackRock, 206,290,537 shares, 7.76% |
| Shell | SPON ADS (1,520), ADR | FMR, 87,332,346 ADSs |

The builder's thousands band narrowed in this phase (`THOUSANDS_BAND` 1.25
inside `PRICE_BAND` 2): Berkshire B shares filed at the B price under the A
CUSIP sit at about 1/1,500 of A's price and read as "value in thousands"
before. Rebuild the asset after merging (Actions → Refresh Form 13F index →
Run workflow, *force*).

## Attribution

"Contains data from the SEC's Form 13F data sets (U.S. Securities and
Exchange Commission), public domain." Every rendering also states the SEC's
caveat: reported by the filers, not verified by the SEC, and no substitute
for the filings.
