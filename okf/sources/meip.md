---
type: "Data Source"
title: "OECD-UNSD Multinational Enterprise Information Platform (MEIP)"
description: "Group membership from the OECD-UNSD Global Register of the 500 largest multinational enterprise groups and their subsidiaries, as the OECD publishes it in BODS v0.4 (register of 2024-12-31). One edge per membership, from a subsidiary to its group head, with the OECD's hierarchy classification; no shares, no dates, no natural persons. A group head's subsidiaries are listed on the Subsidiaries tab. Annual release; served from a local copy."
resource: "https://www.oecd.org/en/data/dashboards/oecd-unsd-multinational-enterprise-information-platform.html"
tags: ["cdd", "aggregator", "OECD-Terms", "commercial-yes"]
timestamp: "2026-09-15"
source_id: "meip"
license: "OECD-Terms"
commercial_use: "yes"
category: "cdd"
national_register: false
---

# Overview

Group membership from the OECD-UNSD Global Register of the 500 largest multinational enterprise groups and their subsidiaries, as the OECD publishes it in BODS v0.4 (register of 2024-12-31). One edge per membership, from a subsidiary to its group head, with the OECD's hierarchy classification; no shares, no dates, no natural persons. A group head's subsidiaries are listed on the Subsidiaries tab. Annual release; served from a local copy. Aggregator, cross-border database or ESG source.

- **Source id:** `meip`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** no


# Licensing

- **Licence:** `OECD-Terms` — OECD Terms and Conditions
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** OECD-UNSD Multinational Enterprise Information Platform (MEIP), Global Register — © OECD
- OECD data may be reused and redistributed for any purpose with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_meip`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.oecd.org/en/data/dashboards/oecd-unsd-multinational-enterprise-information-platform.html
