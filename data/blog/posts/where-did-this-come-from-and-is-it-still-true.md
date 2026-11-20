---
title: "Where did this come from, and is it still true?"
subtitle: "Why OpenCheck records identifiers, dates, publishers and annotations on every Beneficial Ownership Data Standard (BODS) statement"
published: "2026-11-02T15:51:59.475Z"
medium_url: "https://medium.com/@stephenabbottpugh/where-did-this-come-from-and-is-it-still-true-ff3c1f53535b"
medium_id: "ff3c1f53535b"
topics:
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
  - "Open Standards"
  - "Open Data"
image: "/blog/media/where-did-this-come-from-and-is-it-still-true/01.webp"
image_alt: ""
---

I listened to a webinar on customer due diligence recently. The speakers kept coming back to two questions about any piece of ownership information. Where did it come from? And how fresh is it?

Those questions are the job. An analyst who can’t answer them can’t defend a decision to a supervisor. Most ownership data arrives as a PDF extract, a screenshot or a vendor’s merged record, and none of them answers either question well.

The [Beneficial Ownership Data Standard](https://standard.openownership.org/) (BODS) answers both, for every statement. This post explains how OpenCheck uses that.

## Adequate, accurate and up-to-date

[FATF Recommendations 24 and 25](https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Fatf-recommendations.html) require countries to ensure that authorities can get **“adequate, accurate and up-to-date”** information on the beneficial ownership of [legal persons](https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Guidance-Beneficial-Ownership-Legal-Persons.html) and [legal arrangements](https://www.fatf-gafi.org/en/publications/Fatfrecommendations/Guidance-Beneficial-Ownership-Transparency-Legal-Arrangements.html).

Each of those three words is a question about data:

- **Adequate:** is there enough to identify each party, and to tell them apart?
- **Accurate:** can each claim be checked against the source that made it?
- **Up-to-date:** do we know when each claim was true, and when it was last confirmed?

FATF’s evaluations now ask [how effectively that information is actually *used*](https://www.fatf-gafi.org/content/fatf-gafi/en/publications/Mutualevaluations/5th-Round-Procedures.html), not just collected. But FATF does not say how the data should be shaped. It recommends no data standard. BODS fills that gap. It is an open standard built for ownership data, and it puts the evidence for each claim on the claim itself.

![](/blog/media/where-did-this-come-from-and-is-it-still-true/02.webp)

## OpenCheck is a republisher

[OpenCheck](https://opencheck.world/) reads more than [50 sources](https://opencheck.world/sources): national company and beneficial ownership registers, Global Legal Entity Identifier Foundation, OpenSanctions, OpenCorporates, Wikidata, SEC EDGAR, the OECD-UNSD MEIP register and others. It publishes everything it gets back as [BODS v0.4](https://standard.openownership.org/en/0.4.0/).

It doesn’t merge sources into one statement about a company. Companies House’s record of a company and GLEIF’s record of the same company become two statements. OpenCheck joins them on screen using shared identifiers, but never claims in the data that they are one record. That way you can always see which source said what.

## Identifiers: two kinds, both essential

**Statement-level identifiers**: Every OpenCheck statement carries three, and [each answers a different question](https://github.com/StephenAbbott/opencheck/blob/main/docs/identifiers.md):

- `recordId` says *which record of which party* this is. It is built from the source, the record kind and the source's own identifier, so it stays the same on every run.
- `statementId` says *which version* of that record this is. It is computed from the statement's content. If the register changes a name, address or holding, the next run publishes a new statement under the same `recordId`.
- `declarationSubject` says *whose ownership network* the statement belongs to: that source's own record of the company you looked up.

For a compliance team, this means change detection comes free. Compare two exports on `recordId`. If the `statementId` differs, the source changed something. If it doesn't, nothing changed, even if OpenCheck read it on a different day.

**Real-world identifiers**: None of this connects across sources without Legal Entity Identifiers (LEIs) and register numbers, each labelled with the right scheme. That labelling is easy to get wrong. GLEIF files each company number against a registration authority code. Until August, OpenCheck mapped Scottish companies to Northern Ireland’s code, so the Companies House to LEI bridge silently found nothing. A mislabelled identifier doesn’t throw an error. It makes a company look as if it has no record. Nothing connects without an identifier, and nothing connects with a wrong one.

## Dates: four clocks, not one

BODS has [four date-shaped fields](https://github.com/StephenAbbott/opencheck/blob/main/docs/dates.md), and they answer different questions:

- `startDate` / `endDate`: when was the interest true? Is it still active?
- `statementDate`: when did the source declare it?
- `source.retrievedAt`: when did OpenCheck fetch it?
- `publicationDate`: when did OpenCheck publish this statement?

![](/blog/media/where-did-this-come-from-and-is-it-still-true/03.webp)

OpenCheck used to set all four to today. [Fixing that](https://github.com/StephenAbbott/opencheck/commit/185048f) showed how easy each one is to get wrong. A PSC notified in 2016 claimed OpenCheck had published it in 2016. A register extract cut in May claimed OpenCheck had downloaded it in May. Bulk datasets have two clocks, the register’s cut and our download, and both now go in their own fields.

Ended relationships stay in. A shareholding that ended is published as a closed record with its end date, not dropped. As I wrote about [Estonia’s e-Business Register](/blog/how-opencheck-uses-estonias-e-business-register-s-bulk-files-and-apis), Bolt Technology OÜ’s record alone carries 172 ended shareholdings. Former owners are part of a company’s ownership picture and important to store in case required for future investigations.

## Publishers: whose claim is this?

Every statement names two parties. The source block names the register that made the claim. The publication block names OpenCheck, which republished it. They are never the same.

Where another organisation already publishes BODS, such as the [OECD-UNSD MEIP register](https://stephenabbottpugh.medium.com/ownership-data-on-the-worlds-largest-corporate-groups-in-beneficial-ownership-data-standard-format-db43210ff2d8) or [Open Ownership’s bulk data](https://bods-data.openownership.org/), OpenCheck passes its statements on unchanged. Their ids, dates and publisher stay as they were. Two independent BODS publishers end up in one graph without any remapping.

## Annotations: the register’s own words

When OpenCheck replaces a register’s vocabulary with a BODS code, rounds a date or uses a register’s entry date as a start date, the statement carries the usable value. A `transformation` annotation carries what the register actually said. In the OpenCheck interface, an "as filed" toggle switches every source card to the register's own wording.

That is the “accurate” test in practice. You can always trace OpenCheck’s reading back to the source’s words.

## What the standard could still do

None of this is finished. Open Ownership has an open feature request on [republishing, provenance and transformation](https://github.com/openownership/data-standard/issues/464). I’ve [added OpenCheck’s experience there](https://github.com/openownership/data-standard/issues/464#issuecomment-6448689129). The short version:

- **Name the source by id.** OpenCheck matched `source.description` against source names, and it broke whenever a name changed. It now adds an extension field, `opencheckSourceId`. A standard machine-readable source identifier would remove the need.
- **Record the source of the source.** When OpenCheck reads OpenCorporates or Wikidata, *their* retrieval date matters, and BODS has no field for it. For now it goes in an annotation.
- **Settle which way round transformations go.** A `YYYY-MM-DD` field can't hold "01 November 2018", so the annotation holds the original. The definition currently suggests the opposite.
- **One record per party.** In bulk releases a person can sit on many boards. OpenCheck keeps one record per person rather than one per network, so they stay one node in the graph.

## Why it matters

These rules are checked by tests and by a weekly [BODS quality sweep](https://github.com/StephenAbbott/opencheck/blob/main/docs/bods-quality.md) of production output. That’s possible only because the standard is open and the [code and data are open too](/blog/open-all-the-way-down-why-opencheck-is-built-on-open-source-open-data-and-open-standards).

Rules tell you what ownership data has to be: adequate, accurate and up-to-date. A standard decides whether you can prove it, one statement at a time.

Look up a company on [opencheck.world](https://opencheck.world/), open a source card and switch on “as filed”. Or export the BODS and read the source and date fields yourself.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
