---
type: "Data Source"
title: "Corporations Canada \u2014 ISED federal register"
description: "Federal Canadian company data from Corporations Canada (Innovation, Science and Economic Development Canada), covering companies incorporated under federal statutes including the Canada Business Corporations Act."
resource: "https://ised-isde.canada.ca/site/corporations-canada/en"
tags: ["cdd", "national-register", "OGL-Canada-2.0", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "corporations_canada"
license: "OGL-Canada-2.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Federal Canadian company data from Corporations Canada (Innovation, Science and Economic Development Canada), covering companies incorporated under federal statutes including the Canada Business Corporations Act. Official national company / beneficial-ownership register.

- **Source id:** `corporations_canada`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `ca_corp_id`

# Licensing

- **Licence:** `OGL-Canada-2.0` — Open Government Licence – Canada 2.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains information licensed under the Open Government Licence – Canada. Source: Corporations Canada, Innovation, Science and Economic Development Canada.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_corporations_canada`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://ised-isde.canada.ca/site/corporations-canada/en
