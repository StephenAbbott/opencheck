---
type: "Data Source"
title: "UK Companies House"
description: "Legal and beneficial ownership information from the UK corporate registry."
resource: "https://find-and-update.company-information.service.gov.uk/"
tags: ["cdd", "national-register", "OGL-3.0", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "companies_house"
license: "OGL-3.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Legal and beneficial ownership information from the UK corporate registry. Official national company / beneficial-ownership register.

- **Source id:** `companies_house`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity, person
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `gb_coh`

# Licensing

- **Licence:** `OGL-3.0` — UK Open Government Licence v3.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains public sector information licensed under the Open Government Licence v3.0 (Companies House).
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_companies_house`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://find-and-update.company-information.service.gov.uk/
