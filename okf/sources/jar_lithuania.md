---
type: "Data Source"
title: "JAR \u2014 Lithuanian Register of Legal Entities"
description: "Lithuanian company data from the Register of Legal Entities (JAR), maintained by Registr\u0173 centras. Provides entity name, code, address, legal form, and registration status for all entities registered in Lithuania."
resource: "https://www.registrucentras.lt/jar/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "jar_lithuania"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Lithuanian company data from the Register of Legal Entities (JAR), maintained by Registrų centras. Provides entity name, code, address, legal form, and registration status for all entities registered in Lithuania. Official national company / beneficial-ownership register.

- **Source id:** `jar_lithuania`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `lt_code`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Lithuanian Register of Legal Entities (JAR), published by Registrų centras, available under CC BY 4.0. Source: registrucentras.lt.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_jar_lithuania`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.registrucentras.lt/jar/
