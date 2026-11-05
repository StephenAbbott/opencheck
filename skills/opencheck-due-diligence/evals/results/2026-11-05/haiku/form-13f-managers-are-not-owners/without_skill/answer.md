# Eli Lilly: who holds its shares

**Short answer:** The OpenCheck record does not identify Eli Lilly's beneficial owners. What it does show is the largest institutional investment managers that reported positions in Eli Lilly on Form 13F. Those are positions managers have discretion over, not owners, so read the list below as "who holds the most shares through funds and clients," not as "who owns the company."

Checked: LEI `FRDRIPF3EKNDJ2CQJL29` (ELI LILLY AND COMPANY, Indiana, US). This came from a recorded lookup fixture, not a live call.

## Largest 13F-reporting managers (as at 30 June 2026)

Shares outstanding used for the percentages: 941,357,065 (10-Q/10-K cover, 3 Aug 2026, computed by OpenCheck).

| # | Manager | Shares | % of shares outstanding (OpenCheck arithmetic) |
|---|---|---|---|
| 1 | Lilly Endowment Inc | 90,376,978 | 9.60% |
| 2 | BlackRock, Inc. | 67,540,400 | 7.17% |
| 3 | Vanguard Capital Management LLC | 53,528,868 | 5.69% |
| 4 | PNC Financial Services Group, Inc. | 51,322,720 | 5.45% |
| 5 | State Street Corp | 36,215,754 | 3.85% |
| 6 | FMR LLC | 28,462,475 | 3.02% |
| 7 | Capital Research Global Investors | 20,873,140 | 2.22% |
| 8 | Vanguard Portfolio Management LLC | 20,859,217 | 2.22% |
| 9 | Geode Capital Management, LLC | 20,310,858 | 2.16% |
| 10 | Morgan Stanley | 16,226,398 | 1.72% |

4,604 managers reported a position in total. Only the top ten are in the record.

## Why these are not the owners

- **Positions overlap.** One manager's report can include shares another manager also reported, so the figures must not be added together. The top ten do not sum to a total holding.
- **13F is not beneficial ownership.** The record says so directly: it "is not a record of beneficial ownership" and never enters the ownership graph.
- **Dates differ.** These are quarter-end long positions, filed up to 45 days later. A manager's 13F figure can differ from its Schedule 13D/13G figure.
- **Confidential treatment.** A manager missing from the list says nothing about whether it holds shares.

## What the rest of the record shows

- **Integrated ownership (shareholdings):** no holders returned (0).
- **SEC EDGAR (Schedule 13D/13G):** declares 3 shareholders. The fixture does not name them, so I cannot tell you who they are from this record.
- **GLEIF (consolidating parent):** exempt. Eli Lilly is reported as controlled by natural persons with no intermediate consolidating parent. GLEIF Level 2 records accounting consolidation, not ownership.
- **Wikidata (shareholders):** none on record.
- **Register (DLCP, Indiana):** no beneficial-owner register exists in the US. Ownership has to be inferred from securities filings, not a register.

## Caveats on the wider screen

- The screen is **incomplete**. The related-party sanctions and PEP check covered 25 of 48 related parties, and the offshore-leaks check covered 30 of 48. The "possible debarment" and "PEP" signals below come from that partial screen. Absence of other signals is not a clean result.
- These signals are **possible name matches only**, not confirmed identities:
  - PEP (high confidence): Katherine Baicker, matched on name and birth year.
  - PEP (medium): Christopher Anderson, Kimberly Johnson, Patrik Jonsson. Name matches only.
  - Debarment (low): Kimberly Johnson. Name match only.
  - Offshore leaks (low): Christopher Anderson (Panama and Paradise Papers); Jon Moeller (Paradise Papers).
- Context, not risk findings: no EU/EEA parent (US jurisdiction), and five sources agree on the company's status and jurisdiction.

## Licensing

- The bundle is **not for commercial use**. It includes EveryPolitician (CC-BY-NC-4.0). Remove those statements before any commercial use.
- Attribution: SEC Form 13F data sets (public domain, reported by filers and not verified by the SEC); LSEG PermID (CC-BY 4.0) for the NYSE listing (LLY); SEC EDGAR (public domain).
- OpenCorporates data (OC-Terms) carries share-alike obligations.

## To get the actual shareholders

1. Read the named Schedule 13D/13G filers on SEC EDGAR for the three declared shareholders. Those filings show beneficial ownership above the reporting threshold.
2. Check the Lilly Endowment position against its own filings, since it is the largest 13F figure and is a related-foundation holding.
3. Re-run the screen once the truncated related-party checks can complete, before relying on any PEP or debarment result.
