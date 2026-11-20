---
title: "OpenCheck: customer due diligence checks powered by the Legal Entity Identifier and open data"
subtitle: "OpenCheck shows how to do customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards"
published: "2026-04-29T14:24:14.668Z"
medium_url: "https://medium.com/@stephenabbottpugh/introducing-opencheck-customer-due-diligence-checks-powered-by-the-legal-entity-identifier-open-9e27fdaafbd3"
medium_id: "9e27fdaafbd3"
topics:
  - "Open Data"
  - "Open Standards"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
image: "/blog/media/introducing-opencheck-customer-due-diligence-checks-powered-by-the-legal-entity-identifier-open/01.webp"
image_alt: ""
---

[OpenCheck](https://opencheck.world/) is a project to show how to do customer due diligence risk checks powered by the [Legal Entity Identifier](https://www.gleif.org/en/organizational-identity/lei-vlei/the-legal-entity-identifier-lei) (LEI), open data and open standards including the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) (BODS).

Try the beta version at: [**https://opencheck.world**](https://opencheck.world/)

## How it works

You give [OpenCheck](https://opencheck.world/) an LEI and, thanks to [LEI mappings](https://www.gleif.org/en/newsroom/blog/transforming-data-into-opportunities-metric-of-the-month-mapping-network), it uses the global identifier to look up details in a [curated set of open datasets](https://github.com/StephenAbbott/opencheck#sources) from national registries, the [Global Legal Entity Identifier Foundation](https://www.gleif.org/en), [OpenSanctions](https://www.opensanctions.org/), [OpenCorporates](https://opencorporates.com/), [OpenAleph](https://openaleph.org/), [EveryPolitician](https://everypolitician.org/) and [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) before returning a useful intelligence report.

This shows which sources have records on the entity in question or people connected to that entity; what [risk signals](https://github.com/StephenAbbott/opencheck#risk-signals) (sanctions, politically exposed person status, complex ownership structures) are present; and how all the ownership/control relationships look when drawn as a graph.

Every hit is explicitly attributed to its [source and license](https://opencheck.world/sources). Users can go deeper on any finding — expanding a single source’s record in full, and seeing how that record maps and links to equivalent records in the other datasets.

[OpenCheck](https://opencheck.world/) converts every ownership or control relationship it discovers into [BODS version 0.4](https://standard.openownership.org/en/0.4.0/), then uses the existing BODS adapter ecosystem ([bods-ftm](https://github.com/StephenAbbott/bods-ftm), [bods-neo4j](https://github.com/StephenAbbott/bods-neo4j), [bods-xml](https://github.com/StephenAbbott/bods-xml), [bods-gql](https://github.com/StephenAbbott/bods-gql), [bods-aml-ai](https://github.com/StephenAbbott/bods-aml-ai)) to let users take their results into [other tools and formats](https://stephenabbottpugh.medium.com/transforming-standardised-beneficial-ownership-data-into-usable-formats-3dcc8c7712c4).

## Spotting risks

The risk layer in [OpenCheck](https://opencheck.world/) mirrors the [draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) from the EU’s Anti-Money Laundering Authority (AMLA) where risks can be spotted using beneficial ownership or legal ownership data.

These [risk signals](https://github.com/StephenAbbott/opencheck#risk-signals) check if:

1. An entity has a complex corporate ownership structure with three or more layers of ownership
2. Trusts or legal arrangements appear in the corporate structure
3. Nominee arrangements feature in the corporate structure
4. Non-EU jurisdictions appear in the corporate structure

Flags are displayed to show where risk signals have been spotted. Hovering over the signal explains which source surfaced the risk.

Beyond these AMLA-aligned checks, [OpenCheck](https://opencheck.world/) also looks for whether entities or people connected to them are sanctioned or politically exposed.

## BODS as the central data spine

BODS is not just a feature of [OpenCheck](https://opencheck.world/). It is the internal data model that every source adapter writes into and the export format that every other format converts from.

Ownership and control information lies scattered across a wide range of registers and sources — and information often needs to be combined to understand international or complex corporate networks.

This is exactly why the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) was created and why it offers free expert guidance to increase data quality and interoperability.

Having more data formatted in line with a common standard like BODS helps solve data problems by making understanding and using ownership data a lot easier, including giving users the ability to query for risks in the same way across different standardised datasets.

## The Global Open Data Integration Network (GODIN)

![](/blog/media/introducing-opencheck-customer-due-diligence-checks-powered-by-the-legal-entity-identifier-open/02.webp)

[OpenCheck](https://opencheck.world/) has been developed as part of my work on the [Global Open Data Integration Network](https://godin.gleif.org/) (GODIN).

Linking more data sources using the LEI as the catalyst for connection is one of the key goals of GODIN, a collaborative effort to enhance global data interoperability and accessibility by connecting organisations that publish open data or create open data standards and aligning data to a global framework like the Global Legal Entity Identifier (LEI) System.

Find out more at [godin.gleif.org](https://godin.gleif.org/).

## Open-source code

Check out the open-source code to learn more about [OpenCheck](https://opencheck.world/) and please report any issues you spot:

[GitHub - StephenAbbott/opencheck: Customer due diligence checks powered by the Legal Entity Identifier, open data and open standards](https://github.com/StephenAbbott/opencheck)

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
