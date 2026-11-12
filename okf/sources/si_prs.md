---
type: "Data Source"
title: "AJPES \u2014 Slovenian Business Register (PRS)"
description: "Slovenia's Business Register (Poslovni register Slovenije), kept by AJPES and read from an index of its open file on podatki.gov.si: name, ten-digit registration number, legal form, the registering authority and the registered address (a sole trader's settlement and postcode only). The file publishes no status, dates, officers, shareholders or beneficial owners."
resource: "https://www.ajpes.si/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-11-12"
source_id: "si_prs"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Slovenia's Business Register (Poslovni register Slovenije), kept by AJPES and read from an index of its open file on podatki.gov.si: name, ten-digit registration number, legal form, the registering authority and the registered address (a sole trader's settlement and postcode only). The file publishes no status, dates, officers, shareholders or beneficial owners. Official national company / beneficial-ownership register.

- **Source id:** `si_prs`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `si_maticna`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains information from the Slovenian Business Register (Poslovni register Slovenije), AJPES, published on podatki.gov.si under CC BY 4.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_si_prs`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.ajpes.si/
