---
title: "What changed today? A daily list of company records that moved"
subtitle: "Every day, OpenCheck now publishes a list of companies whose GLEIF or OpenSanctions record changed, with a BODS file to download. Here is what it shows, and why stale data is a risk of its own"
published: "2026-11-27T11:58:10.000Z"
medium_url: "https://medium.com/@stephenabbottpugh/what-changed-today-a-daily-list-of-company-records-that-moved-6db90cf3e251"
medium_id: "6db90cf3e251"
topics:
  - "Open Standards"
  - "Open Data"
  - "Beneficial Ownership Data"
  - "Beneficial Ownership"
image: "/blog/media/what-changed-today-a-daily-list-of-company-records-that-moved/01.webp"
image_alt: ""
---

Every check on [OpenCheck](https://opencheck.world/) is a snapshot. You look up a company and you see what each source said at that moment. The [watchlist](https://opencheck.world/features#watchlist) asked the next question for the companies you care about: has anything changed since I last looked?

Now there is a second way to ask it, for everyone. [opencheck.world/changes](https://opencheck.world/changes) lists, day by day, the companies with a Legal Entity Identifier (LEI) whose record at the [Global Legal Entity Identifier Foundation](https://www.gleif.org/en) (GLEIF) or [OpenSanctions](https://www.opensanctions.org/) changed. Each row says what changed: before and after, the source and the time of the update. It ran for two weeks as a shadow experiment first.

## Most change is not news

GLEIF publishes the changes to its [Golden Copy](https://www.gleif.org/en/lei-data/gleif-golden-copy) three times a day. On 26 November, the delta files named 24,768 LEIs. That is too many to read, and most of them are not changes to the company at all:

- 18,295 were renewals and other routine churn.
- 3,632 changed only a detail: a registration status, a category, a reporting exception moving from one reason to another, a register number.
- 1,449 were new LEIs.
- 582 changed something about the company: its legal name, legal form, jurisdiction or address country.
- 98 recorded an ending, and 63 of those were LEIs that had already lapsed.
- 117 changed ownership or sanctions: a parent changed, a merger or successor was recorded, or OpenSanctions added or changed a listing.

![](/blog/media/what-changed-today-a-daily-list-of-company-records-that-moved/02.webp)

The [page](https://opencheck.world/changes) leads with those 117. Everything else is a click away, and the downloadable files hold it all.

Over the fortnight, the ownership and sanctions list averaged about 70 companies a day, from 3 on a quiet Sunday to 124. On 24 November, for example, it showed [Bank Melli Iran](https://opencheck.world/?lei=391200Y7BOFEZOE3X484) added to Canada’s sanctions list under the Special Economic Measures Act. On 26 November, the direct and ultimate parents of [Slovakia’s national lottery company](https://api.opencheck.world/share/097900BHML0000100392) changed.

![](/blog/media/what-changed-today-a-daily-list-of-company-records-that-moved/03.webp)

## Nothing here is a lookup

This list is not OpenCheck’s judgment of these companies. OpenCheck runs no check on any of them. It reads the two delta files [GLEIF](https://www.gleif.org/en/lei-data/gleif-golden-copy) and [OpenSanctions](https://www.opensanctions.org/docs/bulk/delta/) already publish, compares each record before and after, and sorts the change by a published rule. A full OpenCheck report on any company is one click away.

That matters for cost and honesty: a full check on 117 companies a day would mean hundreds of calls to rate-limited registers, and “findings” OpenCheck never made.

Two caveats are on the page itself. OpenSanctions is matched only on the LEI it holds, and about 10,800 organisations in its default dataset carry one, so the sanctions side under-counts. And a GLEIF “parent” means accounting consolidation, not ownership.

## Up-to-date is a property of data, not a promise

[FATF Recommendations 24 and 25](https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Fatf-recommendations.html) ask for beneficial ownership information that is “adequate, accurate and up-to-date”. The EU’s Anti-Money Laundering Regulation expects firms to verify owners using “other reliable sources, including public registers” (Article 22(7)). AMLA’s final draft rules on customer due diligence judge a source partly on how current it is and whether it is “consistent with other sources”.

All three tests assume you know when a record last changed. Most ownership data does not tell you. A PDF extract or last quarter’s spreadsheet is silent on whether it is still true.

The fortnight showed how big the gap can be. Two in three endings recorded in the GLEIF deltas were on LEIs that had already lapsed. The company had stopped renewing, the record sat unchanged and the ending reached the data later. Anyone relying on that record in the meantime was relying on something already false.

That is the case for publishing change rather than state. Collecting data isn’t the same as keeping it current, and a record that cannot say when it was last true is a weak basis for a decision.

## A file you can load without running anything

Each day also comes as a file of [Beneficial Ownership Data Standard](https://standard.openownership.org/) (BODS) v0.4 statements, built from GLEIF’s Golden Copy as it stands after the day’s deltas:

```
curl -OJL https://opencheck.world/changes/latest.jsonl.gz
```

Each line is one statement: the company and its parent relationships. A company keeps the same recordId from day to day, so you replace what you hold under that id with the new statement. A parent GLEIF removed arrives as a closed relationship record, not a deletion, because a former parent is history worth keeping. GLEIF's delta says that the link went, not when it ended, so the closed record carries no end date. As I wrote in Where did this come from, and is it still true?, OpenCheck would rather leave a date out than invent one.

A CSV covers both sources and every band, for spreadsheet users. OpenSanctions rows carry only a record ID, topic codes and a link, because its data is licensed CC BY-NC, and it is not in the BODS file. GLEIF data is CC0. Files are kept for thirty days.

![](/blog/media/what-changed-today-a-daily-list-of-company-records-that-moved/04.webp)

## Changes and the watchlist

The two answer different questions. The [watchlist](https://opencheck.world/features#watchlist) follows the companies you chose, re-runs the full check when a source names one, and records what you decided. The changes page shows everything that moved, for anyone, without a check. A journalist can scan the day's parent changes; a data team can load the file each morning.

[See today’s changes on OpenCheck](https://opencheck.world/changes).

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
