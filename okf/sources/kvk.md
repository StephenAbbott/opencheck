---
type: "Data Source"
title: "KvK \u2014 Netherlands Chamber of Commerce"
description: "Dutch company data from the Netherlands Chamber of Commerce (KvK) open-data API, sourced via the KvK registration number."
resource: "https://www.kvk.nl/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "kvk"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Dutch company data from the Netherlands Chamber of Commerce (KvK) open-data API, sourced via the KvK registration number. Official national company / beneficial-ownership register.

- **Source id:** `kvk`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `kvk_number`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Netherlands Chamber of Commerce (KvK) via the KvK Open Data API (CC BY 4.0).
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_kvk`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.kvk.nl/
