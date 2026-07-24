---
type: "Data Source"
title: "New Zealand Companies Register (NZBN)"
description: "New Zealand company data from the NZBN API (Companies Office / MBIE): entity details, directors, shareholders with share allocations, and the ultimate holding company."
resource: "https://companies-register.companiesoffice.govt.nz/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "nz_companies"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

New Zealand company data from the NZBN API (Companies Office / MBIE): entity details, directors, shareholders with share allocations, and the ultimate holding company. Official national company / beneficial-ownership register.

- **Source id:** `nz_companies`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `nz_company_number`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the New Zealand Companies Register / NZBN (Ministry of Business, Innovation and Employment) via the NZBN API, licensed CC BY 4.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_nz_companies`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://companies-register.companiesoffice.govt.nz/
