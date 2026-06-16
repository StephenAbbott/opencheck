---
type: "Data Source"
title: "Malta Business Registry (MBR)"
description: "Maltese company data from the Malta Business Registry (MBR) Open Data API (CC BY 4.0). Provides core entity details \u2014 name, status, legal form, registered office and registration date \u2014 for companies on the Maltese register."
resource: "https://mbr.mt/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-06-16"
source_id: "malta_mbr"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Maltese company data from the Malta Business Registry (MBR) Open Data API (CC BY 4.0). Provides core entity details — name, status, legal form, registered office and registration date — for companies on the Maltese register. Official national company / beneficial-ownership register.

- **Source id:** `malta_mbr`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `mt_crn`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Malta Business Registry, available under CC BY 4.0 via the MBR Open Data API (openapi.baros.mbr.mt).
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_malta_mbr`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://mbr.mt/
