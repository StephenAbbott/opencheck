# Eli Lilly and Company: who owns it

**Short answer:** OpenCheck does not identify Eli Lilly's beneficial owners. The largest names in the record are investment managers that reported holdings on SEC Form 13F. Those are positions the managers control for their funds and clients. They are not beneficial ownership, and the figures overlap, so they must not be added together.

Source: OpenCheck lookup of LEI FRDRIPF3EKNDJ2CQJL29, recorded 2026-11-05T13:34:04Z (from the saved fixture; no live call was made).

## The company

- **ELI LILLY AND COMPANY**, LEI FRDRIPF3EKNDJ2CQJL29, Indiana (US-IN), Indiana Secretary of State ID 183025-143.
- **Company status:** GLEIF reports the company as ACTIVE, and the OpenCorporates record agrees.
- **LEI status (separate from company status):** ISSUED, last updated 15 May 2026, next renewal due 12 Jul 2027.
- **Listing:** NYSE, ticker LLY (PermID).

## Largest reported holders (Form 13F, not beneficial owners)

Data as at 30 June 2026. Percentages are OpenCheck's arithmetic on 941,357,065 shares outstanding (3 Aug 2026 cover page). The record lists the top 10 of 4,604 managers reporting a position.

| Manager | Shares | % (OpenCheck arithmetic) |
|---|---:|---:|
| Lilly Endowment Inc | 90,376,978 | 9.60 |
| BlackRock, Inc. | 67,540,400 | 7.17 |
| Vanguard Capital Management LLC | 53,528,868 | 5.69 |
| PNC Financial Services Group, Inc. | 51,322,720 | 5.45 |
| State Street Corp | 36,215,754 | 3.85 |
| FMR LLC | 28,462,475 | 3.02 |
| Capital Research Global Investors | 20,873,140 | 2.22 |
| Vanguard Portfolio Management LLC | 20,859,217 | 2.22 |
| Geode Capital Management, LLC | 20,310,858 | 2.16 |
| Morgan Stanley | 16,226,398 | 1.72 |

Why these are not an owner list:

- Form 13F covers positions a manager has discretion over. It is not a record of who owns the shares.
- Positions overlap. One manager's report can include shares another manager also reported, so the rows are never totalled.
- Two Vanguard entities appear as separate rows. Their positions may overlap.
- A 13F figure is a quarter-end snapshot filed by the manager. It has not been verified by the SEC.

So 9.60% is the largest manager-reported position, held by Lilly Endowment Inc. It is not established from this record that Lilly Endowment is the beneficial owner of those shares.

## What the other sources say about owners

- **Calculated ownership (integrated ownership):** No holders were computed. `holders_total` is 0, so OpenCheck has no calculated owner figures for this company.
- **SEC EDGAR (Schedule 13D/13G):** Three declared shareholder filings. The record gives the count only, not the names or percentages, so those filers are not identified here.
- **GLEIF:** No parent is reported. The exemption is "controlled by natural person(s) without any intermediate legal entity meeting the definition of accounting consolidating parent." This is a permitted filing status, not a statement about who holds shares. GLEIF consolidation is not ownership.
- **Wikidata:** No shareholders on record.
- **Beneficial owners in the US:** The knowability record says the United States keeps no central register of beneficial owners. OpenCheck reads beneficial owners from DLCP (Washington, DC), NY DOS and Sunbiz Florida. The Indiana company record in this lookup contains no owner or officer data.

## What was not read

- **The related-party screen is incomplete.** Sanctions and PEP screening covered 25 of 48 related parties. Offshore Leaks screening covered 30 of 48. Treat the absence of signals for the other parties as unknown, not clean.
- **No record found:** OpenSanctions, OpenAleph, Climate TRACE and EITI each returned no record for this company.

## Related-party flags (named individuals, not owners)

These are related parties (officers, controllers and similar roles). The record does not say how they relate to the 13F holders. Each is a match to be checked, not a confirmed identity.

- **Katherine Baicker:** PEP match on OpenSanctions. The birth year agrees, so this is the most corroborated match. High confidence.
- **Christopher Anderson, Kimberly Johnson, Patrik Jonsson:** PEP name matches only. Neither a birth date nor a nationality is in common, so each is a possible match. Medium confidence.
- **Kimberly Johnson:** Possible debarment name match only, with no birth date or nationality in common. Low confidence.
- **Christopher Anderson and Jon Moeller:** Offshore Leaks matches (ICIJ, Panama and Paradise Papers). Low confidence.

## Licence

This result is **not for commercial use**. It includes EveryPolitician and OpenSanctions (CC-BY-NC-4.0). Remove their statements before any commercial use. Attribution is required, and share-alike applies to OpenCorporates-derived material. The 13F data is public domain (SEC), with attribution.

## To go further

- To name the declared Schedule 13D/13G holders, run `opencheck_owners` for this LEI. It spends the shared lookup budget.
- To see the full ownership graph, run `opencheck_export_bods`.
- To check the related-party matches, confirm each against a birth date, nationality or role before relying on it.
