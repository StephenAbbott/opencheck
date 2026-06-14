---
type: "Data Source"
title: "PRH \u2014 Finnish Patent and Registration Office"
description: "Finnish company data from the Patentti- ja rekisterihallitus (PRH) via the YTJ Open Data API, including entity details for all organisations registered in Finland. Officer data is not publicly available."
resource: "https://www.prh.fi/en/index.html"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "prh"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Finnish company data from the Patentti- ja rekisterihallitus (PRH) via the YTJ Open Data API, including entity details for all organisations registered in Finland. Officer data is not publicly available. Official national company / beneficial-ownership register.

- **Source id:** `prh`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `fi_ytunnus`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from Patentti- ja rekisterihallitus (PRH) / Finnish Patent and Registration Office, via the YTJ Open Data API (avoindata.prh.fi), licensed under CC BY 4.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_prh`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.prh.fi/en/index.html
