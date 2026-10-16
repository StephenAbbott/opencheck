---
type: "Data Source"
title: "Estonian e-Business Register (e-\u00c4riregister)"
description: "Estonian company data \u2014 entity details, the persons on the registry card (board members, procurists, liquidators, partners), the register of shareholders with ownership percentages, declared beneficial owners with their manner of control, and the persons outside the registry card (supervisory board members, the shareholders of a public limited company the register records, founders) \u2014 read from a daily index of RIK's open-data files, with the public e-Business Register page as the fallback for a company the files do not hold. An Estonian corporate owner's own record is read from the same index up the chain (six hops, twenty-five companies), and a foreign owner's register code becomes a register-hop identifier where it has the shape of that country's company number. Former shareholders and former beneficial owners come from the register's own history (the credentialed X-Road service, cached a day), each a closed record dated by the register."
resource: "https://ariregister.rik.ee/eng"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-10-16"
source_id: "ariregister"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Estonian company data — entity details, the persons on the registry card (board members, procurists, liquidators, partners), the register of shareholders with ownership percentages, declared beneficial owners with their manner of control, and the persons outside the registry card (supervisory board members, the shareholders of a public limited company the register records, founders) — read from a daily index of RIK's open-data files, with the public e-Business Register page as the fallback for a company the files do not hold. An Estonian corporate owner's own record is read from the same index up the chain (six hops, twenty-five companies), and a foreign owner's register code becomes a register-hop identifier where it has the shape of that country's company number. Former shareholders and former beneficial owners come from the register's own history (the credentialed X-Road service, cached a day), each a closed record dated by the register. Official national company / beneficial-ownership register.

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
