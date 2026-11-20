---
title: "FullCheck: enhanced due diligence by following the ownership chain"
subtitle: "Some risks do not live at the company you are onboarding. A firm can look entirely clean on its own - correctly registered, no sanctions…"
published: "2026-09-01T08:40:08.100Z"
medium_url: "https://medium.com/@stephenabbottpugh/fullcheck-enhanced-due-diligence-by-following-the-ownership-chain-d34b73da2910"
medium_id: "d34b73da2910"
topics:
  - "Open Data"
  - "Open Standards"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
image: "/blog/media/fullcheck-enhanced-due-diligence-by-following-the-ownership-chain/01.webp"
image_alt: ""
---

Some risks do not live at the company you are onboarding. A firm can look entirely clean on its own - correctly registered, no sanctions matches, plausible directors - while the reason for caution sits two or three layers up its ownership chain, or in a sister company controlled by the same parent.

Anti-money laundering rules recognise this: when a customer presents higher risk, obliged entities must go beyond standard checks and apply enhanced due diligence, which means understanding the full ownership and control structure rather than just the counterparty in front of you.

FullCheck is [OpenCheck](https://opencheck.world)’s mode for exactly this. Where [QuickCheck](/blog/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data) screens the subject company on its own, FullCheck follows the ownership chain outwards and then screens everything it reaches.

Starting from the picture [QuickCheck](/blog/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data) has already assembled from 40 open data sources, it expands owners and controllers layer by layer and draws the result as a single unified network graph, using the visual conventions of Open Ownership’s [Beneficial Ownership Visualisation System](https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/).

This ensures that people, companies, trusts and the interests connecting them are immediately readable. The subject’s subsidiary network, drawn from the GLEIF registry, appears alongside it, so you see both directions: who sits above this company, and what sits below it.

![](/blog/media/fullcheck-enhanced-due-diligence-by-following-the-ownership-chain/02.webp)

Screening the whole network is what turns a picture into due diligence. FullCheck carries related-party risk signals onto the graph itself: a [sanctioned entity](https://www.opensanctions.org/) upstream, a [politically exposed person](https://everypolitician.org/) connected through an intermediate company, or an ownership path long enough to trigger the [complex-structure conditions](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) set out in the EU Anti-Money Laundering Authority’s draft technical standards.

Complexity is itself a signal - decades of investigations, from the [World Bank’s Puppet Masters study](https://documents.worldbank.org/en/publication/documents-reports/documentdetail/784961468152973030) to the [Panama Papers](https://www.icij.org/investigations/panama-papers/), show that chains of companies across multiple jurisdictions are the vehicle of choice for hiding who really owns what.

FullCheck makes those chains visible and tells you which node on the diagram each warning attaches to, with the evidence and the source alongside.

[OpenCheck](https://opencheck.world/) as a whole is my working demonstration that due diligence can run on open data and open standards. The [Legal Entity Identifier](https://www.gleif.org/en/organizational-identity/lei-vlei/the-legal-entity-identifier-lei#) (LEI) connects the sources; the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) is the data model throughout, which is what makes it possible to merge ownership information from dozens of registers into one coherent, exportable graph. I introduced the project in [my first post about OpenCheck](/blog/introducing-opencheck-customer-due-diligence-checks-powered-by-the-legal-entity-identifier-open) and reflected on building it in [lessons learned from two months of OpenCheck](/blog/lessons-learned-so-far-from-two-months-of-building-opencheck).

FullCheck is one of three check modes, alongside [QuickCheck](/blog/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data) for a fast first screen and [BackgroundCheck](/blog/backgroundcheck-screening-the-people-behind-a-company) for the people named in a company’s records, each covered in its own post.

To see FullCheck in action, run any lookup at [opencheck.world](https://opencheck.world) and switch to the FullCheck tab - and if you work in compliance and have views on what an ownership network report should show, I would like to hear them.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information, and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
