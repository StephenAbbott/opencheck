---
type: "Data Source"
title: "UR \u2014 Latvian Register of Enterprises"
description: "Latvian company data from the Register of Enterprises (UR), sourced via Latvia's open-data portal (data.gov.lv). Provides entity profiles, beneficial owners, officers, and shareholders for companies registered in Latvia."
resource: "https://www.ur.gov.lv/en/"
tags: ["cdd", "national-register", "Open Government Data (PSI Directive)", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "ur_latvia"
license: "Open Government Data (PSI Directive)"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Latvian company data from the Register of Enterprises (UR), sourced via Latvia's open-data portal (data.gov.lv). Provides entity profiles, beneficial owners, officers, and shareholders for companies registered in Latvia. Official national company / beneficial-ownership register.

- **Source id:** `ur_latvia`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `lv_regcode`

# Licensing

- **Licence:** `Open Government Data (PSI Directive)` — Open Government Data (PSI Directive)
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Latvian Register of Enterprises (UR), open data published on data.gov.lv.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_ur_latvia`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.ur.gov.lv/en/
