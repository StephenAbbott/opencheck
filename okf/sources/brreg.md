---
type: "Data Source"
title: "Br\u00f8nn\u00f8ysundregistrene \u2014 Norwegian Register Centre"
description: "Norwegian company data from the Enhetsregisteret (Central Coordinating Register for Legal Entities), including entity details and role-holders."
resource: "https://www.brreg.no/en/"
tags: ["cdd", "national-register", "NLOD-2.0", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "brreg"
license: "NLOD-2.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Norwegian company data from the Enhetsregisteret (Central Coordinating Register for Legal Entities), including entity details and role-holders. Official national company / beneficial-ownership register.

- **Source id:** `brreg`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `no_orgnr`

# Licensing

- **Licence:** `NLOD-2.0` — Norwegian Licence for Open Government Data 2.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from Brønnøysundregistrene via the Enhetsregisteret, licensed under NLOD 2.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_brreg`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.brreg.no/en/
