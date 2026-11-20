---
title: "Showcasing Nigeria’s public beneficial ownership data in OpenCheck"
subtitle: "Africa’s first national public beneficial ownership register has become the first African register to have data incorporated into…"
published: "2026-08-12T14:48:28.511Z"
medium_url: "https://medium.com/@stephenabbottpugh/showcasing-nigerias-public-beneficial-ownership-data-in-opencheck-3e20e216639b"
medium_id: "3e20e216639b"
topics:
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
  - "Open Standards"
  - "Nigeria"
image: "/blog/media/showcasing-nigerias-public-beneficial-ownership-data-in-opencheck/01.webp"
image_alt: ""
---

Africa’s first [national public beneficial ownership register](https://bor.cac.gov.ng/) has become the first African register to have data incorporated into [OpenCheck](https://opencheck.world/).

Nigeria’s Corporate Affairs Commission has made the data from its [Persons with Significant Control register](https://bor.cac.gov.ng/) publicly available to search since its [launch in 2023](https://www.openownership.org/en/news/nigeria-adopts-beneficial-ownership-data-standard-for-its-new-national-register/).

![](/blog/media/showcasing-nigerias-public-beneficial-ownership-data-in-opencheck/02.webp)

However the register’s official API is currently restricted to Nigerian government agencies.

To demonstrate how Nigerian beneficial ownership data for all local companies registered with a Legal Entity Identifier could be used widely in future if API access was opened up to more people, I’ve collected public data for 10 example companies, mapped it to the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) and uploaded it into [OpenCheck](https://opencheck.world/).

You can see data on these companies by clicking the links below where the Corporate Affairs Commission data is displayed:

1. [Dangote Cement PLC](https://opencheck.world/?lei=029200697A0R1BH0A835)
2. [Dangote Sugar Refinery PLC](https://opencheck.world/?lei=029200564G7N0Y86H603)
3. [BUA Cement PLC](https://opencheck.world/?lei=0292005368B9N9B7I135)
4. [Guinness Nigeria PLC](https://opencheck.world/?lei=0292002192H7C7H2H732)
5. [Presco PLC](https://opencheck.world/?lei=0292006198H4N0UI7E51)
6. [Access Holdings PLC](https://opencheck.world/?lei=0292005048H3P6V8I753)
7. [Transcorp Hotels PLC](https://opencheck.world/?lei=0292007167E0P6YH4A58)
8. [Zenith Bank PLC](https://opencheck.world/?lei=029200268F8M5YI5I629)
9. [MTN Nigeria Communications PLC](https://opencheck.world/?lei=0292003626J3K6UG9D04)
10. [Nestle Nigeria PLC](https://opencheck.world/?lei=0292003626J3K6UG9D04)

## What is OpenCheck?

[OpenCheck](https://opencheck.world/) is an open source project to show how to do live due diligence risk checks powered by the [Legal Entity Identifier](https://www.gleif.org/en/organizational-identity/lei-vlei/the-legal-entity-identifier-lei) (LEI), open data and open standards including the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) (BODS).

You give [OpenCheck](https://opencheck.world/) an LEI and, thanks to [LEI mappings](https://www.gleif.org/en/newsroom/blog/transforming-data-into-opportunities-metric-of-the-month-mapping-network), it uses this global identifier to look up corporate information in dozens of [open datasets](https://opencheck.world/sources) from national registries, the [Global Legal Entity Identifier Foundation](https://www.gleif.org/en), [OpenSanctions](https://www.opensanctions.org/), [OpenCorporates](https://opencorporates.com/), [OpenAleph](https://openaleph.org/), [EveryPolitician](https://everypolitician.org/) and [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) before returning a free, detailed open-source intelligence report on any of the three million entities with an LEI.

> *The more datasets can be* [*linked using the LEI*](https://godin.gleif.org/) *and the more corporate information is structured in line with* [*BODS*](https://standard.openownership.org/)*, the more detailed these reports can be.*

[Lessons learned so far from two months of building OpenCheck](/blog/lessons-learned-so-far-from-two-months-of-building-opencheck)

## Why is BODS relevant in Nigeria?

When the [Persons with Significant Control register](https://bor.cac.gov.ng/) was first unveiled in 2023, the Corporate Affairs Commission worked with my former team at [Open Ownership](https://www.openownership.org/en/) and we helped to export data from the register in BODS JSON format.

The goal was to set the template for countries to use a common data standard to make it easier for them to compare and exchange beneficial ownership information. However the data was not regularly made available in line with BODS.

In November 2026, the Corporate Affairs Commission will host a [Global Conference on Beneficial Ownership and Asset Recovery](https://boconference.cac.gov.ng/) where this idea will again be picked up as there is a theme focused on making ownership data searchable across borders via use of BODS and API-based data sharing.

![](/blog/media/showcasing-nigerias-public-beneficial-ownership-data-in-opencheck/03.webp)

I hope that [OpenCheck](https://opencheck.world/) — which converts [all data sources](https://opencheck.world/sources) in line with BODS and is [powered](https://opencheck.world/about) by live API connections — will be of interest to attendees.

As Stanley Achonu and [Favour Ime](https://www.openownership.org/en/about/team/favour-ime/) of Open Ownership noted in their recent report [Enhancing beneficial ownership data collection and use in Nigeria](https://www.openownership.org/en/publications/enhancing-beneficial-ownership-data-collection-and-use-in-nigeria/), there is a need for Nigeria to maximise the impact of its beneficial ownership register and strengthen its corporate transparency ecosystem via enhancing data quality, driving data use and standardising beneficial ownership data.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
