---
type: "Data Source"
title: "Estonian e-Business Register (e-\u00c4riregister)"
description: "Estonian company data including entity details, shareholders (with ownership percentages), board members, and beneficial owners, from the public e-Business Register portal (RIK)."
resource: "https://ariregister.rik.ee/eng"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-06-23"
source_id: "ariregister"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Estonian company data including entity details, shareholders (with ownership percentages), board members, and beneficial owners, from the public e-Business Register portal (RIK). Official national company / beneficial-ownership register.

- **Source id:** `ariregister`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `ee_registry_code`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Data from the Estonian e-Business Register (e-Äriregister), published by the Centre of Registers and Information Systems (RIK), CC BY 4.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_ariregister`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://ariregister.rik.ee/eng
