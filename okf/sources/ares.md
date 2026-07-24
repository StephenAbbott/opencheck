---
type: "Data Source"
title: "ARES (Czechia)"
description: "Czech ARES business register (Administrativn\u00ed registr ekonomick\u00fdch subjekt\u016f), aggregating data from the commercial register (Obchodn\u00ed rejst\u0159\u00edk), trade licence register, and other sub-registers.  Published by the Ministry of Finance under CC BY 4.0."
resource: "https://ares.gov.cz/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "ares"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Czech ARES business register (Administrativní registr ekonomických subjektů), aggregating data from the commercial register (Obchodní rejstřík), trade licence register, and other sub-registers.  Published by the Ministry of Finance under CC BY 4.0. Official national company / beneficial-ownership register.

- **Source id:** `ares`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `cz_ico`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from ARES (Administrativní registr ekonomických subjektů), published by the Ministry of Finance of the Czech Czechia (Ministerstvo financí ČR) under CC BY 4.0. Source: ares.gov.cz.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_ares`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://ares.gov.cz/
