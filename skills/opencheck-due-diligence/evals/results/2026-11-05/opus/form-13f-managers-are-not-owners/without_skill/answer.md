# Eli Lilly and Company: who holds the shares

**Eli Lilly and Company** · LEI `FRDRIPF3EKNDJ2CQJL29` · Indiana, US · NYSE: LLY
Source: OpenCheck lookup, recorded 5 Nov 2026. All 11 sources answered.

## Short answer

Eli Lilly is a widely held listed company, and no parent company stands above it. The lookup doesn't actually name its owners. It shows two things that are easy to mistake for an owner list:

1. **SEC EDGAR Schedule 13D/13G:** three shareholders have declared a stake above 5%. These are ownership filings. However, this lookup gives only the count, not the names or percentages, and the integrated-ownership table is empty.
2. **Form 13F (30 June 2026):** the largest institutional positions. These are **not ownership**. Form 13F lists the shares an investment manager has discretion over on behalf of its funds and clients. It does not show who beneficially owns them.

So below are the largest *reported positions*. Please don't read them as a ranked list of owners.

## Largest Form 13F positions, 30 June 2026

The percentages are OpenCheck's own calculation against 941,357,065 shares outstanding (from Lilly's 10-Q cover, 3 Aug 2026). 4,604 managers reported a position. The top 10 are shown.

| Manager | Shares | % (computed) |
|---|---:|---:|
| Lilly Endowment Inc | 90,376,978 | 9.60% |
| BlackRock, Inc. | 67,540,400 | 7.17% |
| Vanguard Capital Management LLC | 53,528,868 | 5.69% |
| PNC Financial Services Group, Inc. | 51,322,720 | 5.45% |
| State Street Corp | 36,215,754 | 3.85% |
| FMR LLC (Fidelity) | 28,462,475 | 3.02% |
| Capital Research Global Investors | 20,873,140 | 2.22% |
| Vanguard Portfolio Management LLC | 20,859,217 | 2.22% |
| Geode Capital Management, LLC | 20,310,858 | 2.16% |
| Morgan Stanley | 16,226,398 | 1.72% |

How to read this table:
- **Don't add the rows together**, including the two Vanguard entities. A manager's report can include shares that another manager also reports.
- **BlackRock, Vanguard, State Street, Fidelity, Geode and Capital Research are asset managers.** They mostly hold shares for index funds and clients. The economic owners are those funds' investors.
- **PNC's figure is probably trust and custody business.** From outside the data: PNC has acted as trustee for Lilly Endowment, so part of its position may be the same shares the Endowment reports. Confirm this before relying on it.
- **Lilly Endowment is the one row that is very likely a real owner.** From outside the data: it is the private philanthropic foundation set up by the Lilly family in 1937, and it holds Lilly stock in its own name. The 13F filing alone doesn't prove that. On these figures it would be the largest single holder, at about 9.6%.
- The figures are long positions at quarter-end, filed up to 45 days later. Managers may leave positions out under confidential treatment.

## What the registers say about control

- **GLEIF:** Lilly reports no accounting-consolidation parent. The reason given is "NATURAL_PERSONS", meaning no intermediate company consolidates it. This is a permitted exception and is normal for a widely held company. It is context, not a risk finding.
- **US:** there is no central register of beneficial owners. Every owner figure above comes from SEC securities filings, not from a beneficial-ownership register.

## Other signals

These are not about ownership. All of them concern officers and directors, and most are weak:
- **PEP (politically exposed person) matches:** "Katherine Baicker" matches on name *and* birth year, so this is a high-confidence match. "Christopher Anderson", "Kimberly Johnson" and "Patrik Jonsson" match on **name only**. The "Kimberly Johnson" match also carries a debarment record. A name-only match is something to check, not a confirmed identity.
- **Offshore Leaks:** "Christopher Anderson" (Panama Papers, Paradise Papers) and "Jon Moeller" (Paradise Papers) appear in the leaked records. These are low-confidence name matches.
- **This screen is incomplete.** Sanctions and PEP screening covered only 25 of the 48 related parties, and Offshore Leaks screening covered 30. Finding no other hits is **not** a clean result.

## Next step

To get the named 5%+ holders and their filed percentages, run `opencheck_owners` or `opencheck_export_bods` on `FRDRIPF3EKNDJ2CQJL29`. Those would name the three Schedule 13D/13G filers. Those filings are the basis for an ownership answer; the 13F table is not.

---
*Licensing: this result includes EveryPolitician data (CC-BY-NC-4.0), so it is **not for commercial use** unless those statements are removed. OpenCorporates data carries share-alike terms. Form 13F and 13D/13G data come from the SEC and are public domain. Listing data is from LSEG PermID (CC-BY 4.0).*
