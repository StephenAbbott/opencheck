---
title: "QuickCheck: live customer due diligence checks in seconds, powered by open data"
subtitle: "Every business relationship starts with the same question: who am I actually dealing with? Answering it is a legal obligation for banks…"
published: "2026-08-31T06:34:58.688Z"
medium_url: "https://medium.com/@stephenabbottpugh/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data-d13dccb3c365"
medium_id: "d13dccb3c365"
topics:
  - "Open Data"
  - "Open Standards"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
image: "/blog/media/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data/01.webp"
image_alt: ""
---

Every business relationship starts with the same question: who am I actually dealing with? Answering it is a legal obligation for banks, payment firms, lawyers, accountants and many other businesses.

Customer due diligence - identifying a customer, verifying who owns and controls them, and assessing the risks of working with them - is the foundation of anti-money laundering rules worldwide. Yet the information needed to do it sits scattered across national company registers, sanctions lists and corporate databases, many of them behind paywalls.

QuickCheck is the default mode of [OpenCheck](https://opencheck.world), and it exists to show that a credible first check can be fast, free and fully transparent about its sources.

You start from a Legal Entity Identifier (LEI), a company name or a national registration number. OpenCheck resolves the company against the Global Legal Entity Identifier Foundation (GLEIF) registry, derives every identifier it can - a UK Companies House number, a Dutch KvK number, a Danish CVR number and more - and then queries [40 open data sources](https://opencheck.world/sources) in parallel. Results stream onto the page live as each source responds, so you watch the report assemble itself rather than waiting for a batch job.

![](/blog/media/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data/02.webp)

QuickCheck screens the company on its own: sanctions, control and structure - the fastest answer. The risk signals it raises are modelled on what the EU’s new Anti-Money Laundering Authority [expects obliged entities to look for](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en): trusts and legal arrangements in the ownership structure, three or more ownership layers, links to jurisdictions on the [Financial Action Task Force lists](https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html) and matches against [sanctions data](https://www.opensanctions.org/). Every finding names the source it came from, and when a source could not be checked, the report says so rather than implying a clean screen.

The word live matters. Due diligence is not a one-off event: obligations to keep customer information up to date mean checks are repeated at onboarding, at periodic review and when circumstances change. Because a QuickCheck runs against the sources at the moment you ask, and because every report is addressable by a shareable link, re-checking a customer is as simple as opening a URL again.

[OpenCheck](https://opencheck.world/) as a whole is my working demonstration that customer due diligence can be built on open data and open standards: the LEI acts as the connector between sources, and everything is mapped into the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) so the results are structured, comparable and exportable. I introduced the project in [my first post about OpenCheck](/blog/introducing-opencheck-customer-due-diligence-checks-powered-by-the-legal-entity-identifier-open) and reflected on building it in [lessons learned from two months of OpenCheck](/blog/lessons-learned-so-far-from-two-months-of-building-opencheck).

QuickCheck is one of three check modes, alongside [FullCheck](/blog/fullcheck-enhanced-due-diligence-by-following-the-ownership-chain) for enhanced due diligence on ownership networks and [BackgroundCheck](/blog/backgroundcheck-screening-the-people-behind-a-company) for screening the people behind a company. I will cover both in separate posts.

In the meantime, try QuickCheck on a company you know at [opencheck.world](https://opencheck.world) — it is free and requires no registration — and let me know what you find.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information, and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
