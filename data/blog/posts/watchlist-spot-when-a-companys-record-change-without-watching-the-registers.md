---
title: "Watchlist: spot when a company’s records change, without watching the registers"
subtitle: "Most beneficial ownership and corporate ownership data is published as a snapshot. You download a file or query an API and you get the…"
published: "2026-09-16T12:38:04.074Z"
medium_url: "https://medium.com/@stephenabbottpugh/watchlist-spot-when-a-companys-record-change-without-watching-the-registers-6b4222d78b9a"
medium_id: "6b4222d78b9a"
topics:
  - "Open Data"
  - "Open Standards"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
image: "/blog/media/watchlist-spot-when-a-companys-record-change-without-watching-the-registers/01.webp"
image_alt: ""
---

Most beneficial ownership and corporate ownership data is published as a snapshot. You download a file or query an API and you get the information as the register understood it at that moment.

What you do not get is the thing an investigator, a compliance officer or a journalist often needs next: has anything changed since I last looked?

This week I added a small feature to [OpenCheck](https://opencheck.world) that tries to answer that question honestly. It is called [Watchlist](https://opencheck.world/features#watchlist), and it is a first version.

But building it forced me to think about things I have been working on for years: the importance of [capturing dates](https://standard.openownership.org/en/main/standard/modelling/dates-guidance.html), [standardising data](https://standard.openownership.org/), [using robust identifiers](https://godin.gleif.org/) and being able to see [change over time](https://github.com/openownership/data-standard/issues/392).

Because the feature only works when all four are present in the source data. Where they are missing, it cannot work.

## Why change over time matters

The Financial Action Task Force requires countries to obtain “adequate, accurate and up-to-date beneficial ownership information” on [legal persons](https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Guidance-Beneficial-Ownership-Legal-Persons.html) and [legal arrangements](https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Guidance-Beneficial-Ownership-Transparency-Legal-Arrangements.html). This means not only holding current information but also holding historical records for years to understand when changes occurred and why.

Open Ownership’s principles emphasis the importance of [up-to-date and historical records](https://www.openownership.org/en/principles/up-to-date-and-historical-records/). This recommends that changes are reported within a short, defined window after they happen, and that the historical record is kept so that anyone can see what was declared at a given point in time.

The reasoning is simple. A register that only shows the present lets an entity obscure itself through a name change, a re-incorporation or a quiet swap of owners. A register with [auditable records](https://www.openownership.org/en/publications/building-an-auditable-record-of-beneficial-ownership/) does not.

## What the data standard does about it

[Version 0.4 of the Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS) was designed with this problem as its [primary focus](https://www.openownership.org/en/blog/an-open-solution-for-interoperability-the-launch-of-version-04-of-the-beneficial-ownership-data-standard/).

The answer in 0.4 is that every statement is a dated, immutable record of something a source said at a point in time. Each statement carries a `recordId` for the thing it describes and a `recordStatus` of `new`, `updated` or `closed`; `statementDate` became mandatory; and the old `replacesStatements` mechanism was removed.

The result is an append-only log. You do not overwrite an owner; you publish a new statement that closes the old record and opens the next.

Once the data has that shape, tooling can follow. In December 2024 Open Ownership added a timeslider to its visualisation library so that users can [see how a beneficial ownership network changes over time](https://www.openownership.org/en/news/helping-data-users-visualise-and-understand-how-beneficial-ownership-networks-change-over-time/) — pick a date, and the diagram shows the structure as it was then.

And earlier this year I built [bods.stream](https://bods.stream/), which turns the Companies House PSC streaming API into a live BODS v0.4 feed so you can [watch UK beneficial ownership information change in real time](https://stephenabbottpugh.medium.com/watch-uk-beneficial-ownership-information-change-in-real-time-as-open-standardised-data-e7db4001c5e3): owners appearing, being re-filed and ceasing, each one a statement with a date and a status.

bods.stream shows the whole firehose. Watchlist asks the narrower question: *of the companies I care about, which ones changed?*

## What Watchlist does

On any company page in OpenCheck, there is now a **Watch for changes** button. Press it and the company joins a list held in your browser. The list has its own page and its own Atom feed, so you can subscribe in any feed reader. When something changes, an entry appears in the feed. That is the whole user-facing feature.

The interesting part is what “when something changes” means, because the obvious design is the wrong one.

[OpenCheck](https://opencheck.world/) checks a company by anchoring on its [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei), pulling the [GLEIF](https://www.gleif.org/en) record and then fetching information in parallel from [dozens of open sources](https://opencheck.world/sources) including national registers, sanctions lists, procurement data, leaks databases.

The [batch screening feature](https://opencheck.world/features#batch-screening) I shipped earlier this month is a thin loop over that same pipeline. A watchlist looks like a saved batch that re-runs on a schedule, and that is exactly what it must not be. Re-running every watched company every day would be a crawl: four to six calls to GLEIF’s rate-limited API per company, then ten or more upstream fetches, several of them against registers that throttle hard and one of them ([OpenSanctions](https://www.opensanctions.org/)) licensed CC BY-NC. “OpenCheck monitors the registers for you” would feel true and be false.

So the Watchlist is **delta-driven, not poll-driven**. It re-checks a company only when one of two organisations publishes a change to that company’s record.

**GLEIF**: Every day GLEIF publishes not just the full Golden Copy of the LEI system (about 3.4 million records) but a [*LastDay delta*](https://www.gleif.org/en/lei-data/gleif-golden-copy)which includes only the records that changed. On the day I measured it, the delta held 15,920 LEI records, 3,765 relationship records and 6,029 reporting-exception records — roughly half a percent of the whole. OpenCheck already applies that delta to its own mirror of the LEI data every hour. The Watchlist simply intersects the delta with the list of watched LEIs. If a watched company appears, its material fields are digested and compared — legal name, status, jurisdiction, legal form, parents, reporting exceptions, dates — and only a real change triggers a re-check. Most delta rows are renewal churn (a `NextRenewalDate` moving forward a year); those are filtered out deliberately, because a renewal is not a change to the company.

**OpenSanctions**: OpenSanctions also publishes an [entity-level delta](https://www.opensanctions.org/docs/bulk/delta/) with every release, several times a day: a JSON-lines file of every entity added, modified or removed, with full properties. The Watchlist reads each new delta and matches the changed organisations against the names and LEIs on the watchlist, locally. No API calls, no re-screening, nothing redistributed. In this first version it matches only the watched company’s own names; people connected to it are out of scope for now, for reasons I will come to.

When either delta names a watched company, OpenCheck runs a full fresh check and diffs the result against the last one it holds: register status, risk signals, structural signals, source coverage, the verdict sentence. The feed entry says which delta fired and when, what the re-run found, and how many sources were actually reached as a result.

If a source could not be reached during the re-run, a signal it previously reported is worded as “could not re-check”, never as “gone”. Absence is only a finding when the producer answered.

There is no third tier. National registers are never polled on a clock. They are fetched only as a consequence of a GLEIF or OpenSanctions change, which is what makes the cost of the feature proportional to how much the world changes rather than to how many companies are watched. At 200 watched companies and a half-percent daily change rate, that is roughly one re-check a day.

## What this depends on

None of this is possible without the things I mentioned at the start, and it is worth being precise about which organisation supplies which:

**Robust identifiers**: The whole design keys on the LEI. A delta is only useful if you can intersect it with your list, and you can only intersect on an identifier that both sides use consistently. OpenSanctions carries `leiCode` on the entities where it has one, which lets the second tier match exactly rather than by name where possible.

**Standardised data**: GLEIF’s Golden Copy and OpenSanctions’ FollowTheMoney entities both have stable, documented schemas. That is what lets a program decide which fields are material and which are churn. A register that publishes PDFs or changes its column names between releases cannot be watched this way.

**Good dates**: Every GLEIF delta carries the publish date it belongs to; every OpenSanctions release has a version. The feed entry can therefore say “GLEIF published a change to this record in its 16 September delta”, which is a claim about the source, not a guess about the company. Without dated publications, there is nothing to anchor a change to.

**Deltas**: This is the one that is easy to take for granted. GLEIF and OpenSanctions both go to the trouble of publishing *what changed* alongside *what is*. Very few registers do. Companies House has its [streaming API](https://developer-specs.company-information.service.gov.uk/streaming-api/reference), which is why [bods.stream](https://bods.stream/) exists; almost nobody else offers anything comparable. The first version of the Watchlist covers GLEIF and OpenSanctions precisely because they are the two sources OpenCheck uses that release daily change files and the feature page says so.

## Where BODS fits

OpenCheck transforms all the data it fetches into [BODS v0.4](https://standard.openownership.org/en/0.4.0/) on the fly, so every check is a set of dated statements. What the Watchlist adds is the *comparison* between two such sets, and the diff vocabulary it uses - a status changed, a name changed, a signal appeared, coverage fell - is the kind of thing 0.4’s `recordStatus` model exists to carry.

If a register published its own history as `new` / `updated` / `closed` statements with reliable dates, a watchlist would not need to re-check anything; it could read the register's own changelog. That is the direction the standard points in and the reason I think the up-to-date-and-historical-records principle is not a nice-to-have but the thing that makes every other use of the data trustworthy.

## What is deliberately not in Watchlist v1

Email delivery is not in v1. The feed is enough to prove the architecture, and email means holding addresses, which means consent, a privacy notice, an unsubscribe path and a data-processing agreement with a sending provider. I would rather ship the mechanism first and add the mailing list as a separate, properly designed step.

Person watching is not in v1 either, for the related reason that a watchlist of people is a database of people. OpenSanctions’ deltas include individuals, and matching them against the beneficial owners of a watched company is an obvious next step. It is also the step at which the state store starts holding personal data, so it waits for the same GDPR work as email.

And the Watchlist does not claim to watch the registers. It watches two well-behaved sources that tell the world what changed, and it goes to the registers only when they do.

## Try it

Search for a company on [opencheck.world](https://opencheck.world), press **Watch for changes**, and subscribe to the feed from your watchlist page. The design notes are in the [documentation](https://github.com/StephenAbbott/opencheck/blob/main/docs/watchlist.md), and the code is open.

If you run a register and you are wondering whether publishing a daily delta is worth the effort, this is one of the things it makes possible.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information, and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via :*[*.co.uk*](https://www.beneficialownership.co.uk/)*.*
