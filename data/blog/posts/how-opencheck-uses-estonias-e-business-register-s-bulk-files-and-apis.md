---
title: "How OpenCheck uses Estonia’s e-Business Register’s bulk files and APIs"
subtitle: "Estonia is one of the best places to test what open company data can do"
published: "2026-10-18T15:13:01.679Z"
medium_url: "https://medium.com/@stephenabbottpugh/how-opencheck-uses-estonias-e-business-register-s-bulk-files-and-apis-76888cbf9a35"
medium_id: "76888cbf9a35"
topics:
  - "Estonia"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
  - "Open Data"
image: "/blog/media/how-opencheck-uses-estonias-e-business-register-s-bulk-files-and-apis/01.webp"
image_alt: ""
---

Like the UK, where Companies House offers data via [APIs](https://developer-specs.company-information.service.gov.uk/) and [bulk downloads](https://www.gov.uk/guidance/companies-house-data-products), Estonia publishes its company, shareholder and beneficial ownership data openly and in more than one way.

Anyone can search the [e-Business Register](https://ariregister.rik.ee/eng) (e-äriregister) online. The Centre of Registers and Information Systems (RIK) publishes nine datasets as [daily bulk downloads](https://avaandmed.ariregister.rik.ee/en/downloading-open-data) in JSON, XML or CSV under a CC BY 4.0 licence. And under a free open data contract, RIK offers live APIs: an [X-Road service](https://avaandmed.ariregister.rik.ee/en/open-data-api/introduction-api-services) and a newer [REST API](https://avaandmed.ariregister.rik.ee/en/node/35).

This week I pushed [OpenCheck](https://opencheck.world/) as far as I could on that data.

## Why multiple formats matter

Open Ownership’s [principle on structured data](https://www.openownership.org/en/principles/structured-data/) says beneficial ownership data should be made available “in a range of ways”: browsable, in bulk, per record and through an API. Estonia shows why: each format answers a different question.

![](/blog/media/how-opencheck-uses-estonias-e-business-register-s-bulk-files-and-apis/02.webp)

## What OpenCheck already did with Estonian data

[Eesti Energia](https://opencheck.world/?lei=5493005044RTLQ5RZU70) has been one of the curated examples on [OpenCheck’s homepage](https://opencheck.world/) since August. Lookups read the register’s public company page for shareholders with exact percentages, beneficial owners and board members. The [History tab](https://opencheck.world/?lei=5493005044RTLQ5RZU70&mode=history) used the X-Road service to show how a company’s registry information and beneficial owners changed over time. And because Estonia publishes exact percentages, I could [test the beneficial ownership register against its own shareholder lists](https://stephenabbottpugh.medium.com/testing-estonias-beneficial-ownership-register-against-its-dbfb54c51702).

## What the bulk files now power

OpenCheck now builds a daily index from the nine files, covering almost 380,000 companies. From it, users get the following information:

- **Wider ownership networks -** Supervisory boards, the shareholders of public limited companies (AS) and founders as ownership history. Lookups follow Estonian corporate shareholders up the chain, up to six steps. Where a foreign shareholder’s code has the shape of a number on its own country’s register, FullCheck can follow it across the border
- **Subsidiaries -** The [Subsidiaries tab](https://opencheck.world/features#subsidiaries) lists what an Estonian company holds, read from the other side of the shareholder lists
- **A person’s other companies -** Instead of personal identification codes, the files carry a pseudonymous key for each person. OpenCheck lists a person’s other roles on that key, never by matching names
- **Context -** Share capital, main activity, the latest annual report, mergers and foreign branches, and whether a company must file beneficial owners, so a missing filing reads “beneficial owners due, none filed”. Compliance rulings in force, such as a deletion warning for an unfiled annual report, and commercial pledges appear as notes in plain English, never as risk signals
- **History and monitoring -** Every entry on every registry card appears on the History tab. The [watchlist](https://opencheck.world/features#watchlist) feature re-runs an Estonian company when the daily index shows its people, name, status or legal form changed

## What the APIs add: the full record

Open Ownership’s [principle on up-to-date and historical records](https://www.openownership.org/en/principles/up-to-date-and-historical-records/) says “it is important to keep historical information”, “including for dormant and dissolved corporate vehicles”. The bulk files show the register today. The APIs show what it used to say:

- **Former owners -** Every ended shareholding and beneficial ownership is drawn as a closed relationship
- **Deleted companies -** These are absent from every bulk file. A company the index does not hold is now read from the REST API, so [Eesti Energia Õlitööstus AS](https://opencheck.world/?lei=5493007RNMXROOIK2M94) opens with the date it was deleted from the register: 10 January 2017
- **Every company a company was named on -** One click lists every company a legal person has held a role in, deleted ones included. For Eesti Energia that is 25 companies, 13 of them deleted

RIK’s contract allows one query at a time and 50,000 answers a day, so every call goes through one queue with a daily budget. A busy queue is reported as a gap, never as “no record”.

## Open data, handled with care

The files and APIs include personal identification codes. OpenCheck uses them only to join records, then drops them.

## What other registers can learn

Estonia lets users choose bulk data or an API, today’s record or the full history. That choice is why Estonian companies now get the fullest record and the widest ownership network OpenCheck produces anywhere.

It depends on the data staying open. Estonia [postponed](https://www.err.ee/1610074771/tegelike-kasusaajate-andmete-varjamine-lukkub-edasi) a planned move to legitimate-interest access in July 2026. I hope it stays that way.

If you want to use the data yourself, RIK’s guides are a good place to start:

- [Downloading open data](https://avaandmed.ariregister.rik.ee/en/downloading-open-data)
- [Introduction to the open data API services](https://avaandmed.ariregister.rik.ee/en/open-data-api/introduction-api-services)
- [Introduction to the REST API services](https://avaandmed.ariregister.rik.ee/en/node/35)
- [Help with open data](https://abiinfo.rik.ee/en/e-business-register-queries/open-data-e-business-register) and [frequently asked questions](https://abiinfo.rik.ee/en/e-business-register-queries/open-data-e-business-register/frequently-asked-questions-about-open)

Or search for [Eesti Energia](https://opencheck.world/?lei=5493005044RTLQ5RZU70) on [opencheck.world](https://opencheck.world/) and open the [Subsidiaries](https://opencheck.world/?lei=5493005044RTLQ5RZU70&mode=subsidiaries) and [History](https://opencheck.world/?lei=5493005044RTLQ5RZU70&mode=history) tabs.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
