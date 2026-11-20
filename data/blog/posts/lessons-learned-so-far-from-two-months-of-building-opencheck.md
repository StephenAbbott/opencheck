---
title: "Lessons learned so far from two months of building OpenCheck"
subtitle: "For the past two months, I’ve been building a new tool called OpenCheck and I want to share some of the lessons I’ve learned so far."
published: "2026-06-24T15:38:10.017Z"
medium_url: "https://medium.com/@stephenabbottpugh/lessons-learned-so-far-from-two-months-of-building-opencheck-9553bad1b750"
medium_id: "9553bad1b750"
topics:
  - "Beneficial Ownership"
  - "Open Data"
  - "Open Standards"
  - "Interoperability"
image: "/blog/media/lessons-learned-so-far-from-two-months-of-building-opencheck/01.webp"
image_alt: ""
---

## What is OpenCheck?

[OpenCheck](https://opencheck.world/) is an open source project to show how to do live due diligence risk checks powered by the [Legal Entity Identifier](https://www.gleif.org/en/organizational-identity/lei-vlei/the-legal-entity-identifier-lei) (LEI), open data and open standards including the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) (BODS).

You give [OpenCheck](https://opencheck.world/) an LEI and, thanks to [LEI mappings](https://www.gleif.org/en/newsroom/blog/transforming-data-into-opportunities-metric-of-the-month-mapping-network), it uses this global identifier to look up corporate information in dozens of [open datasets](https://opencheck.world/sources) from national registries, the [Global Legal Entity Identifier Foundation](https://www.gleif.org/en), [OpenSanctions](https://www.opensanctions.org/), [OpenCorporates](https://opencorporates.com/), [OpenAleph](https://openaleph.org/), [EveryPolitician](https://everypolitician.org/) and [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) before returning a free, detailed open-source intelligence report on any of the three million entities with an LEI.

> The more datasets can be [linked using the LEI](https://godin.gleif.org/) and the more corporate information is structured in line with [BODS](https://standard.openownership.org/), the more detailed these reports can be.

Common risk checks can easily be run on the data once it has been standardised into BODS format.

With access to high-quality open company data, I’ve been able to quickly add [new features](https://opencheck.world/changelog) including AI summaries, historical timelines, PDF export, interactive graph visualisations, an OpenCheck API, an MCP server and full data exports.

## Key lessons learned so far

### Data quality and predictability help AI do the right thing

![](/blog/media/lessons-learned-so-far-from-two-months-of-building-opencheck/02.webp)

It almost goes without saying that the higher the quality and predictability of open datasets, the easier they are to use.

But this has proved to be especially true in the new world of developing tools such as OpenCheck where generative AI coding assistants such as Claude Code, Google Antigravity or ChatGPT can be harnessed to quickly develop and iterate — but only if they can understand the information they are working with.

I’ve mainly used Claude Code when building [OpenCheck](https://opencheck.world/) and have been careful to read, store and create full documentation of [all the decisions and choices made](https://github.com/StephenAbbott/opencheck/commits/main/). But there have been many dead-ends or wasted explorations due to some sources lacking detailed information about how to work with their data.

As a counterpoint, the open data published by the [Global Legal Entity Identifier Foundation](https://www.gleif.org/en) (GLEIF) has been a joy to work with and I’ve harnessed their API, golden copy files, [MCP servers](https://ai.gleif.org/connect-ai) and [AI skills](https://ai.gleif.org/connect-ai) to help me along the way.

As recommended in this [recent paper on connecting ownership data](https://www.openownership.org/en/publications/connecting-ownership-data-practical-pathways-to-tackle-cross-border-financial-crime/), “well-established foundations for interoperability - including consistent use of identifiers, well-documented APIs or bulk-data access mechanisms, and use of metadata to assist interpretation” should be adopted by more registers.

### API access and documentation is crucial

![](/blog/media/lessons-learned-so-far-from-two-months-of-building-opencheck/03.webp)

Many of the [sources](https://opencheck.world/sources) in [OpenCheck](https://opencheck.world/) were added in minutes where they made API access swift and had detailed documentation about how to write API requests and what information to expect in response.

This was because I gave Claude Code access to a wealth of existing information about the open standards maintained by GLEIF as well as the open documentation for BODS.

Coupled with the [initial project plan](https://github.com/StephenAbbott/opencheck/blob/main/docs/plan.md), this set the direction of travel meaning that potential sources could quickly be assessed to see if they could [have adapters set up for them](https://github.com/StephenAbbott/opencheck/tree/main/backend/opencheck/sources) or otherwise discounted.

On the flip side, several registries ban access to API documentation sites by AI chatbots which made it more challenging to capture and share all the information required for mapping to take place.

Kudos to the [New Zealand Companies Office](https://companies-register.companiesoffice.govt.nz/) for their approach where they provide detailed resources accessible to chatbots while restricting access to full documentation to logged in, approved API users.

### Exposing the need for entity-resolved knowledge graphs

![](/blog/media/lessons-learned-so-far-from-two-months-of-building-opencheck/04.webp)

Once the data from each source in OpenCheck has been transformed and standardised in line with BODS, I use it to power all of the features from risk signals to interactive graph visualisations to historical timelines.

But these are currently dealt with on a source-by-source basis to show what information is coming in from each source and linking it back to original, verifiable records.

To understand all of the information that OpenCheck can pull together on a company in one go, it would be better in future if all this information can be brought together in a single graph to be examined and investigated.

I’ve already started down this path by - for example - offering all the collected BODS data for an entity in a single downloadable bundle as well as experimenting with [exporting data in the Neo4J format](https://github.com/StephenAbbott/bods-neo4j).

In the coming weeks, I will be doing more work in this space to see how OpenCheck could take advantage of approaches around [entity-resolved knowledge graphs](https://github.com/ikwattro/operationalising-entity-resolved-graphs).

### Lack of open company ownership information from some European Union countries

![](/blog/media/lessons-learned-so-far-from-two-months-of-building-opencheck/05.webp)

Under the [Open Data Directive](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32019L1024), European Union member states were all supposed to have released [open, machine-readable data on companies and company ownership](https://data.europa.eu/data/datasets?is_hvd=true&locale=en&hvdCategory=c_a9135398&hvdCategory=c_56a1bf47&hvdCategory=c_8f0fac04&page=1) by mid-2024. This does not cover beneficial ownership data where access is more restricted.

The EU wanted this because such data was seen as particularly [high-value](https://digital-strategy.ec.europa.eu/en/news/commission-defines-high-value-datasets-be-made-available-re-use) with officials [stating](https://digital-strategy.ec.europa.eu/en/news/commission-defines-high-value-datasets-be-made-available-re-use) that “making high-value datasets available will benefit both the economy and society”.

But eight EU countries have still not released this information or fail to make it available via API, based on my experience so far with [OpenCheck](https://opencheck.world/).

A 2025 OECD study [found](https://www.oecd.org/en/publications/2025/06/government-at-a-glance-2025_70e14c6c/full-report/open-access-to-high-value-datasets_6a8944a1.html) that the “categories of [high-value data] least likely to be available as open data are companies and company ownership”.

I will continue to monitor a variety of sources to see if there are further rich sources of open company data which can be included in [OpenCheck](https://opencheck.world/), so please let me know if you know of any good candidates missing from the [list of sources](https://opencheck.world/sources).

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
