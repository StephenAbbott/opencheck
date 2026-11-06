# Who owns Eli Lilly? (OpenCheck lookup, 5 Nov 2026)

**Eli Lilly and Company**, LEI `FRDRIPF3EKNDJ2CQJL29`, an Indiana for-profit corporation (Indiana SoS 183025-143), listed on the NYSE as **LLY**. The company is live (GLEIF and OpenCorporates both say ACTIVE). Its LEI is separately recorded as issued, with renewal due 12 Jul 2027.

## Short answer

OpenCheck **names no declared or calculated owners** for Eli Lilly. The largest holders it shows come from **Form 13F**, and those are investment managers reporting positions they manage. They are not owners or beneficial owners.

## 1. Declared owners: three Schedule 13D/13G filers, not named here

- **SEC EDGAR (Schedule 13D/13G)** shows that shareholders are *declared (3)*. This means three parties have filed 13D/13G beneficial-ownership reports for 5%+ holdings. This lookup gives the count only. It does not give the names or percentages.
- **Integrated (calculated) ownership:** no holders were computed.
- **GLEIF:** no parent is reported. The stated reason is the `NATURAL_PERSONS` exception, which is allowed under the LEI rules. It is common for widely held companies and is not a refusal to disclose.
- **Wikidata:** no shareholders on record.
- **No central register exists to fill the gap.** The US keeps no central register of beneficial owners. OpenCheck reads beneficial owners only from DC, NY and Florida registers, and those do not cover an Indiana listed company. The thin result reflects what the registers publish. It does not mean Lilly has no owners.

**Next step to get the names:** run `opencheck_owners` on the LEI, or read the 13D/13G filings on EDGAR. I worked only from the recorded lookup, so I have not done either.

## 2. Largest Form 13F positions (managers, not owners)

At 30 June 2026, **4,604 managers** reported LLY positions. Percentages are OpenCheck's arithmetic against 941.4M shares outstanding (10-Q cover, 3 Aug 2026).

| Manager (Form 13F filer) | Shares | % computed |
|---|---:|---:|
| Lilly Endowment Inc | 90,376,978 | 9.60% |
| BlackRock, Inc. | 67,540,400 | 7.17% |
| Vanguard Capital Management LLC | 53,528,868 | 5.69% |
| PNC Financial Services Group, Inc. | 51,322,720 | 5.45% |
| State Street Corp | 36,215,754 | 3.85% |
| FMR LLC | 28,462,475 | 3.02% |
| Capital Research Global Investors | 20,873,140 | 2.22% |
| Vanguard Portfolio Management LLC | 20,859,217 | 2.22% |
| Geode Capital Management, LLC | 20,310,858 | 2.16% |
| Morgan Stanley | 16,226,398 | 1.72% |

How to read this table:
- **These are not owners.** Form 13F lists positions a manager has *investment discretion* over for funds and clients. In most cases the economic owners are the funds' and clients' investors. In OpenCheck's words, it "is not a record of beneficial ownership: it never enters the ownership graph and no risk signal reads it."
- **Do not add the rows together.** Positions overlap, because one manager's report can include shares another manager also reports. For example, the two Vanguard entities must not be combined into one "Vanguard 7.9%".
- **The data is a snapshot.** It shows long positions at the quarter end, filed up to 45 days later. Some positions may be left out under confidential treatment. A manager's 13F figure can also differ from its 13D/13G figure.
- **Lilly Endowment** appears at the top. This table cannot tell you whether it holds those shares for its own account or manages them, so treat it as a lead and confirm it against its 13D/13G filing.

## 3. Other items from the same lookup (not about ownership)

**Risk signals** concern officers or other related parties, not shareholders:
- **Katherine Baicker:** a PEP match, corroborated by birth year (high confidence).
- **Christopher Anderson, Kimberly Johnson and Patrik Jonsson:** *possible name matches only* to PEP records, and for Johnson also a debarment record. No birth date or nationality was shared, so these need confirming before they mean anything.
- **Christopher Anderson and Jon Moeller:** low-confidence name matches in the Panama/Paradise Papers.

**Context** (`GLEIF_REPORTING_EXCEPTION`, `NON_EU_JURISDICTION`, `RECORD_CONSISTENCY`) describes how the company is set up. These are not findings.

## What was not read

- **The related-party screening is incomplete.** The sanctions/PEP screen covered 25 of 48 related parties, and the Offshore Leaks screen covered 30 of 48. Parties that were not screened carry no signal, but that does not mean they are clear.
- All 11 sources answered. Seven had records and four (OpenSanctions on the company itself, OpenAleph, Climate TRACE, EITI) had none.

## Licence

This result is **not for commercial use**: it includes EveryPolitician data (CC-BY-NC-4.0). OpenCorporates data carries share-alike terms. The Form 13F data is SEC public domain, is reported by the filers and is not verified by the SEC.
