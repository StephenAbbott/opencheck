---
title: "Open all the way down: why OpenCheck is built on open source, open data and open standards"
subtitle: "A country that collects ownership data should be able to understand it, keep it, download it and take it elsewhere. Open standards are what…"
published: "2026-10-09T13:01:13.541Z"
medium_url: "https://medium.com/@stephenabbottpugh/open-all-the-way-down-why-opencheck-is-built-on-open-source-open-data-and-open-standards-8725f2c5325e"
medium_id: "8725f2c5325e"
topics:
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
  - "Corporate Structures"
  - "Open Data"
image: "/blog/media/open-all-the-way-down-why-opencheck-is-built-on-open-source-open-data-and-open-standards/01.webp"
image_alt: ""
---

*A country that collects ownership data should be able to understand it, keep it, download it and take it elsewhere. Open standards are what make that possible*

[OpenCheck](https://opencheck.world) checks who owns and controls a company by fanning out across [52 open data sources](https://opencheck.world/sources) - 33 national company registers, [global legal ownership information](http://gleif.org), [sanctions lists](https://www.opensanctions.org/), procurement notices, [leaks databases](https://openaleph.org/) - and returning one ownership graph.

I built it to show what can be done with high-quality ownership data. But the thing I want to write about here is not what it finds. It is the three ways in which it is open, and why I think the third of those matters most for governments.

## Open source

The [code is on GitHub](https://github.com/StephenAbbott/opencheck) under an MIT licence. Anyone can read how a risk signal is calculated, run the whole stack on a laptop or fork it. Every source adapter, every mapper and every test is there, alongside every phase of development history. Nothing happens inside a box that only I can open.

## Open data

OpenCheck holds no data of its own. Every record it shows was published by a register, a standards body or a civil society project, and every record carries the licence it arrived under.

Where a source is restricted - OpenSanctions is CC BY-NC, Companies House is OGL - the report says so. Where a register publishes nothing about owners, the report says that too, in a dated sentence, rather than leaving a blank that reads as “nothing to see”.

## Open standards

This is the one that does the work. Everything OpenCheck reads is mapped in line with the [Beneficial Ownership Data Standard](https://standard.openownership.org/) (BODS), and everything it exports - JSON, RDF, Neo4J, Senzing, FollowTheMoney - is derived from that one representation.

Because the standard is open and documented, a Norwegian register entry, a Hong Kong filing and a Global Legal Entity Identifier Foundation relationship can sit in the same graph and be compared on equal terms.

That is a technical convenience for me. For a country, it is something larger.

## Keeping the value of what you collect

Governments are collecting more ownership data than ever: beneficial ownership registers, procurement declarations, real estate filings. A great deal of it is collected through commercial platforms, in formats those platforms define. The data sits in the vendor’s schema, is queried through the vendor’s interface and is analysed by the vendor’s models.

Dan Newland of GraphAware put the problem precisely in a recent piece on [who controls your intelligence analysis platform](https://neo4j.com/blog/government/who-controls-your-intelligence-analysis-platform/). An organisation can “technically own its data while no longer really owning the intelligence capability built around it”, because every workflow, every pipeline and every piece of institutional knowledge lives on the vendor’s side of the line. “That is how dependency becomes lock-in.”

Open standards are a solution to this issue as recently highlighted by the European Commission. Its [Open Source Strategy](https://digital-strategy.ec.europa.eu/en/policies/open-source-strategy) lists what public administrations gain from a buoyant open source ecosystem: “increased choice, greater control over digital infrastructure, improved interoperability, reduced lock-in and more efficient reuse of solutions”. Its fourth objective is about standards: integrating open source communities into standardisation, and writing procurement guidelines for open standards. Open source without open standards delivers only half of that list. You can swap the software and still be stuck with the schema.

I argued in June that [Europe’s tech sovereignty strategy needs an open standard for corporate ownership data](https://stephenabbottpugh.medium.com/europes-tech-sovereignty-strategy-needs-an-open-standard-for-corporate-ownership-data-e11d8cbee800), and that data standards are the plumbing of digital sovereignty.

Since then the argument has moved [from proposal to production](https://stephenabbottpugh.medium.com/proposal-to-production-the-beneficial-ownership-data-standard-inside-the-european-business-wallets-a57d58a7f6c9): the European Business Wallet rulebooks now point at BODS for their ownership and control semantics. A wallet, a register and a due diligence tool can all speak the same language, and none of them has to buy it from anyone.

OpenCheck is a small demonstration of the same principle. Fifty-two sources, dozens of formats, one open standard in the middle - and the whole thing is reproducible by a government team with the standard’s documentation and a copy of the code.

## Five questions

So here is the test I would put to any country, regulator or register running ownership data through a commercial system. Ask it of your own setup:

- **Can you understand the data?** Is the schema documented somewhere you can read, in a standard someone other than your vendor maintains?
- **Can you keep the data?** If the contract ends, does the history stay with you, with its dates and its provenance intact?
- **Can you download the data?** All of it, in a machine-readable form, without a professional services engagement?
- **Can you take the data away?** Would a second system be able to read the export without a translation project?
- **Can you control the data?** Do you decide who sees it, what it is joined to and what is inferred from it?

If the answer to any of those is no, the data is not really yours. An open standard is the cheapest way to turn every one of them into a yes.

![](/blog/media/open-all-the-way-down-why-opencheck-is-built-on-open-source-open-data-and-open-standards/02.webp)

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
