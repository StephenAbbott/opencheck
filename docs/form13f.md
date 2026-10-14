# Institutional shareholders (SEC Form 13F)

Phase 333 builds the index. Phase B reads it (it becomes a frozen lookup event
next to the primary listing) and Phase C shows it, as a collapsed
"Institutional shareholders" section under Securities on QuickCheck. The
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
| `thousands` | within that band of a thousandth of it: the value is still reported in thousands, as before 3 January 2023. 254 managers, T. Rowe Price among them, report nine in ten or more of their stored positions this way (206 report all of them); for 466 securities the largest position is one |
| `unchecked` | fewer than five positions report a value to compare |

Left out and counted (`excluded_price`, `excluded_name`):

- a position whose price is in neither band — CalSTRS reported 100,442,257
  "shares" of Moody's worth $221,766 (shares and value transposed);
- a position whose issuer name carries none of the security's common name
  tokens *and* whose price does not vouch for it — "Axon Enterprise" under
  Berkshire A's CUSIP prices at a thousandth of it. A short or former name at
  the issuer's own price ("IBM", "RAYTHEON", "SCHLUMBERGER") is kept.

## Matching a company to its securities (Phase B)

13F rows carry a CUSIP, the issuer's name and nothing else — no LEI, no CIK.

- **GLEIF's ISIN file** (the Phase 258 table): a US ISIN is `US` + CUSIP + a
  check digit. This reaches 638 of the 801 most widely held company CUSIPs.
  ADR ISINs map to the foreign issuer (Novo Nordisk, Shell).
- **Schedule 13D/13G filings**: the SEC EDGAR adapter already reads each
  filing's `issuerCusipNumber`. This reaches what GLEIF misses — Chevron, and
  issuers incorporated outside the US whose CUSIP starts with a letter
  (Accenture, Medtronic).
- Rejected: the six-character CUSIP issuer prefix, and issuer names.

## The percentage (Phase B)

Computed only when the issuer has one class, an undimensioned shares-outstanding
fact dated within about 100 days of the period, and the security is not an
ADR — and labelled as computed: "of 173.2 million shares outstanding on
Moody's 10-Q cover, 30 June 2026 — computed by OpenCheck". Berkshire's
24,669,778 shares are 14.24%, the figure MarketScreener's vendors show.

## Attribution

"Contains data from the SEC's Form 13F data sets (U.S. Securities and
Exchange Commission), public domain." Every rendering also states the SEC's
caveat: reported by the filers, not verified by the SEC, and no substitute
for the filings.
