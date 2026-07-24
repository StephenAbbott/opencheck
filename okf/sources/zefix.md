---
type: "Data Source"
title: "Zefix \u2014 Swiss Commercial Registry"
description: "Swiss company data from the Federal Commercial Registry (Zefix / FCRO), sourced via the Swiss UID."
resource: "https://www.zefix.ch/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "zefix"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Swiss company data from the Federal Commercial Registry (Zefix / FCRO), sourced via the Swiss UID. Official national company / beneficial-ownership register.

- **Source id:** `zefix`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `che_uid`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Swiss Federal Commercial Registry Office (FCRO / EHRA) via Zefix, available under CC BY 4.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_zefix`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.zefix.ch/
