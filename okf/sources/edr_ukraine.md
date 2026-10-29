---
type: "Data Source"
title: "\u0404\u0414\u0420 \u2014 Unified State Register of Legal Entities (Ukraine)"
description: "Ukrainian company register published as open data by the Ministry of Justice on data.gov.ua, read from a local index of the weekly export. Carries founders with their hryvnia holdings, heads and signatories, governing-body members, and beneficial owners \u2014 or the register's stated reason for their absence. Addresses and activity codes are withheld at source under martial law."
resource: "https://usr.minjust.gov.ua/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-10-29"
source_id: "edr_ukraine"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Ukrainian company register published as open data by the Ministry of Justice on data.gov.ua, read from a local index of the weekly export. Carries founders with their hryvnia holdings, heads and signatories, governing-body members, and beneficial owners — or the register's stated reason for their absence. Addresses and activity codes are withheld at source under martial law. Official national company / beneficial-ownership register.

- **Source id:** `edr_ukraine`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `ua_edrpou`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains information from the Unified State Register of Legal Entities, Individual Entrepreneurs and Public Organisations, published by the Ministry of Justice of Ukraine on data.gov.ua under a Creative Commons Attribution 4.0 licence.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_edr_ukraine`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://usr.minjust.gov.ua/
