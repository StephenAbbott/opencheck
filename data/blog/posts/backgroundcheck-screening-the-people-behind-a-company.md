---
title: "BackgroundCheck: screening the people behind a company"
subtitle: "Companies do not launder money; people do. Behind every legal entity sits a set of natural persons - directors, officers, shareholders…"
published: "2026-09-02T07:54:05.668Z"
medium_url: "https://medium.com/@stephenabbottpugh/backgroundcheck-screening-the-people-behind-a-company-56867223d3c6"
medium_id: "56867223d3c6"
topics:
  - "Open Data"
  - "Open Standards"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
image: "/blog/media/backgroundcheck-screening-the-people-behind-a-company/01.webp"
image_alt: ""
---

Companies do not launder money; people do. Behind every legal entity sits a set of natural persons - directors, officers, shareholders, beneficial owners - and much of what anti-money laundering rules actually require is knowing who those people are and whether any of them is sanctioned or politically exposed.

Screening beneficial owners is a core component of customer due diligence, and identifying politically exposed persons (PEPs) triggers additional obligations of its own.

BackgroundCheck is [OpenCheck](https://opencheck.world)’s mode for this. Where QuickCheck screens a company and FullCheck maps its ownership network, BackgroundCheck brings the people connected to the entity to the fore.

It gathers every person named in the records OpenCheck has assembled - officers from national company registers, persons with significant control from UK Companies House, beneficial owners from registers that publish them - and lets you run an on-demand screen for each person across the open sources that hold information about individuals: [OpenSanctions](https://www.opensanctions.org/) for sanctions and [PEP lists](https://everypolitician.org/), [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page), [Companies House officer records](https://developer-specs.company-information.service.gov.uk/) and the [OpenAleph](https://openaleph.org/) archive of investigative documents. Each screened person gets their own report, with their roles and appointments shown in context.

![](/blog/media/backgroundcheck-screening-the-people-behind-a-company/02.webp)

The hardest part of screening people is honesty about uncertainty. Person matching is name-based, and names are shared, transliterated and misspelled. BackgroundCheck therefore frames every result as a potential match, never a confirmed identity: each one is shown with its similarity score and, where available, birth-year corroboration and risk chips only appear on strong matches.

Just as importantly, the report states which sources were checked and found nothing, and never presents a clean screen as proof of absence. If a source could not be reached, that gap is reported rather than glossed over. In my view this evidence discipline matters more than the matching itself, because a false sense of certainty is worse in compliance work than a declared limitation.

[OpenCheck](https://opencheck.world/) as a whole is my working demonstration that customer due diligence can be built on open data and open standards. The [Legal Entity Identifier](https://www.gleif.org/en/organizational-identity/lei-vlei/the-legal-entity-identifier-lei) (LEI) connects records about companies across 40 open sources, and the [Beneficial Ownership Data Standard](https://www.beneficialownership.co.uk/data-standard) structures everything including the people and the interests linking them to companies, which is exactly the data BackgroundCheck screens. I introduced the project in [my first post about OpenCheck](/blog/introducing-opencheck-customer-due-diligence-checks-powered-by-the-legal-entity-identifier-open) and reflected on building it in [lessons learned from two months of OpenCheck](/blog/lessons-learned-so-far-from-two-months-of-building-opencheck).

BackgroundCheck completes the set of three check modes alongside [QuickCheck](/blog/quickcheck-live-customer-due-diligence-checks-in-seconds-powered-by-open-data) and [FullCheck](/blog/fullcheck-enhanced-due-diligence-by-following-the-ownership-chain), each covered in its own post.

To try it, look up a company at [opencheck.world](https://opencheck.world), switch to the BackgroundCheck tab and screen the people you find there. It is free and open source.

If you know of further open datasets about company officers and beneficial owners that would strengthen person screening, please let me know.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information, and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
