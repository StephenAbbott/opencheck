---
type: "Data Source"
title: "Bolagsverket \u2014 Swedish Companies Registration Office"
description: "Swedish company data from Bolagsverket's open data API (v\u00e4rdefulla datam\u00e4ngder), including entity details and registered address."
resource: "https://www.bolagsverket.se/"
tags: ["cdd", "national-register", "SE-PSI", "commercial-yes"]
timestamp: "2026-06-23"
source_id: "bolagsverket"
license: "SE-PSI"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Swedish company data from Bolagsverket's open data API (värdefulla datamängder), including entity details and registered address. Official national company / beneficial-ownership register.

- **Source id:** `bolagsverket`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `se_org_number`

# Licensing

- **Licence:** `SE-PSI` — SE-PSI
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from Bolagsverket (Swedish Companies Registration Office), published as open PSI data.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_bolagsverket`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.bolagsverket.se/
